use bytes::Bytes;
use parquet::{
    errors::ParquetError,
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};

/// Number of bytes initially fetched from the tail of a parquet object when
/// attempting to read its footer.
///
/// The Parquet footer is usually small, so this is enough to parse the metadata
/// in a single range request in the vast majority of cases.
pub const INITIAL_FOOTER_FETCH_SIZE: u64 = 64 * 1024;

/// Result of attempting to parse the Parquet footer from a (possibly partial)
/// view of the tail of a file.
#[derive(Debug)]
pub enum FooterParseResult {
    /// The metadata was fully parsed.
    Parsed(Box<ParquetMetaData>),
    /// More bytes are required from the tail of the file. The value is the total
    /// number of bytes needed counted from the end of the file (not additional
    /// bytes).
    NeedMoreBytes(usize),
    /// Parsing failed (e.g. the object is not a valid parquet file).
    Failed(String),
}

/// Attempts to parse the Parquet footer from `tail_bytes`, which must be the
/// last `tail_bytes.len()` bytes of a file whose total size is `file_size`.
///
/// If `tail_bytes` does not contain enough data to fully parse the footer,
/// [`FooterParseResult::NeedMoreBytes`] is returned indicating how many bytes
/// from the tail are required. The caller is expected to fetch that many bytes
/// and retry.
pub fn parse_footer(tail_bytes: &[u8], file_size: u64) -> FooterParseResult {
    let bytes = Bytes::copy_from_slice(tail_bytes);
    let mut reader = ParquetMetaDataReader::new();
    match reader.try_parse_sized(&bytes, file_size) {
        Ok(()) => match reader.finish() {
            Ok(metadata) => FooterParseResult::Parsed(Box::new(metadata)),
            Err(e) => FooterParseResult::Failed(e.to_string()),
        },
        Err(ParquetError::NeedMoreData(needed)) => FooterParseResult::NeedMoreBytes(needed),
        Err(e) => FooterParseResult::Failed(e.to_string()),
    }
}

/// Renders the parsed Parquet metadata into a list of plain text lines suitable
/// for display in a scrollable preview.
pub fn format_metadata(metadata: &ParquetMetaData, file_size: u64) -> Vec<String> {
    let mut lines = Vec::new();
    let file_meta = metadata.file_metadata();
    let schema = file_meta.schema_descr();

    lines.push("[File]".to_string());
    lines.push(format!("  Size:           {file_size} bytes"));
    lines.push(format!("  Format version: {}", file_meta.version()));
    lines.push(format!("  Num rows:       {}", file_meta.num_rows()));
    lines.push(format!("  Num row groups: {}", metadata.num_row_groups()));
    lines.push(format!("  Num columns:    {}", schema.num_columns()));
    lines.push(format!(
        "  Created by:     {}",
        file_meta.created_by().unwrap_or("-")
    ));
    lines.push(String::new());

    lines.push("[Schema]".to_string());
    for (i, col) in schema.columns().iter().enumerate() {
        lines.push(format!("  {}. {}", i + 1, col.path().string()));
        lines.push(format!("     Physical type:  {:?}", col.physical_type()));
        let logical = col
            .logical_type_ref()
            .map(|lt| format!("{lt:?}"))
            .unwrap_or_else(|| "-".to_string());
        lines.push(format!("     Logical type:   {logical}"));
        lines.push(format!("     Converted type: {:?}", col.converted_type()));
        lines.push(format!("     Max def level:  {}", col.max_def_level()));
        lines.push(format!("     Max rep level:  {}", col.max_rep_level()));
    }
    lines.push(String::new());

    lines.push("[Key/Value Metadata]".to_string());
    match file_meta.key_value_metadata() {
        Some(kvs) if !kvs.is_empty() => {
            for kv in kvs {
                let value = kv.value.as_deref().unwrap_or("-");
                lines.push(format!("  {} = {}", kv.key, value));
            }
        }
        _ => lines.push("  (none)".to_string()),
    }
    lines.push(String::new());

    for (rg_idx, rg) in metadata.row_groups().iter().enumerate() {
        lines.push(format!("[Row Group {rg_idx}]"));
        lines.push(format!("  Num rows:          {}", rg.num_rows()));
        lines.push(format!("  Total byte size:   {}", rg.total_byte_size()));
        lines.push(format!("  Compressed size:   {}", rg.compressed_size()));
        lines.push(format!("  Num columns:       {}", rg.num_columns()));
        for col in rg.columns() {
            lines.push(format!("  - Column: {}", col.column_path().string()));
            lines.push(format!("      Physical type:     {:?}", col.column_type()));
            lines.push(format!("      Compression:       {:?}", col.compression()));
            let encodings = col
                .encodings()
                .map(|e| format!("{e:?}"))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!("      Encodings:         {encodings}"));
            lines.push(format!("      Num values:        {}", col.num_values()));
            lines.push(format!(
                "      Uncompressed size: {}",
                col.uncompressed_size()
            ));
            lines.push(format!(
                "      Compressed size:   {}",
                col.compressed_size()
            ));
            if let Some(stats) = col.statistics() {
                lines.push(format!("      Statistics:        {stats:?}"));
            }
        }
        lines.push(String::new());
    }

    // drop trailing empty separator lines
    while lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }

    lines
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parquet::{
        basic::Compression,
        data_type::{ByteArray, ByteArrayType, Int32Type},
        file::{properties::WriterProperties, writer::SerializedFileWriter},
        schema::parser::parse_message_type,
    };

    use super::*;

    fn sample_parquet_bytes() -> Vec<u8> {
        let message_type = "
            message schema {
                REQUIRED INT32 id;
                OPTIONAL BYTE_ARRAY name (UTF8);
            }
        ";
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::UNCOMPRESSED)
                .set_created_by("stu-test".to_string())
                .build(),
        );

        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = SerializedFileWriter::new(&mut buf, schema, props).unwrap();
            let mut row_group = writer.next_row_group().unwrap();

            let mut id_col = row_group.next_column().unwrap().unwrap();
            id_col
                .typed::<Int32Type>()
                .write_batch(&[1, 2, 3], None, None)
                .unwrap();
            id_col.close().unwrap();

            let mut name_col = row_group.next_column().unwrap().unwrap();
            let names = vec![
                ByteArray::from("alice"),
                ByteArray::from("bob"),
                ByteArray::from("carol"),
            ];
            name_col
                .typed::<ByteArrayType>()
                .write_batch(&names, Some(&[1, 1, 1]), None)
                .unwrap();
            name_col.close().unwrap();

            row_group.close().unwrap();
            writer.close().unwrap();
        }
        buf
    }

    #[test]
    fn test_parse_footer_full_buffer() {
        let bytes = sample_parquet_bytes();
        let file_size = bytes.len() as u64;

        let result = parse_footer(&bytes, file_size);
        let metadata = match result {
            FooterParseResult::Parsed(metadata) => metadata,
            other => panic!("expected Parsed, got {other:?}"),
        };

        let file_meta = metadata.file_metadata();
        assert_eq!(file_meta.num_rows(), 3);
        assert_eq!(metadata.num_row_groups(), 1);
        assert_eq!(file_meta.schema_descr().num_columns(), 2);
        assert_eq!(file_meta.created_by(), Some("stu-test"));
    }

    #[test]
    fn test_format_metadata_contains_expected_content() {
        let bytes = sample_parquet_bytes();
        let file_size = bytes.len() as u64;

        let metadata = match parse_footer(&bytes, file_size) {
            FooterParseResult::Parsed(metadata) => metadata,
            other => panic!("expected Parsed, got {other:?}"),
        };

        let lines = format_metadata(&metadata, file_size);
        let text = lines.join("\n");

        assert!(text.contains("[File]"));
        assert!(text.contains("Num rows:       3"));
        assert!(text.contains("Num row groups: 1"));
        assert!(text.contains("Num columns:    2"));
        assert!(text.contains("Created by:     stu-test"));
        assert!(text.contains("[Schema]"));
        assert!(text.contains("id"));
        assert!(text.contains("name"));
        assert!(text.contains("[Row Group 0]"));
        // no trailing empty line
        assert!(!lines.last().unwrap().is_empty());
    }

    #[test]
    fn test_parse_footer_partial_then_retry() {
        let bytes = sample_parquet_bytes();
        let file_size = bytes.len() as u64;

        // Provide only the final 8 bytes (footer length + magic), which is not
        // enough to parse the metadata itself.
        let tail = &bytes[bytes.len() - 8..];
        let needed = match parse_footer(tail, file_size) {
            FooterParseResult::NeedMoreBytes(needed) => needed,
            other => panic!("expected NeedMoreBytes, got {other:?}"),
        };
        assert!(needed > 8);
        assert!(needed as u64 <= file_size);

        // Retry with the requested number of tail bytes.
        let tail = &bytes[bytes.len() - needed..];
        let result = parse_footer(tail, file_size);
        assert!(matches!(result, FooterParseResult::Parsed(_)));
    }

    #[test]
    fn test_parse_footer_invalid_bytes() {
        let bytes = vec![0u8; 64];
        let result = parse_footer(&bytes, bytes.len() as u64);
        assert!(matches!(result, FooterParseResult::Failed(_)));
    }
}
