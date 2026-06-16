use ratatui::{
    buffer::Buffer,
    layout::Rect,
    text::Line,
    widgets::{Block, StatefulWidget},
};

use crate::{
    color::Theme,
    environment::Environment,
    format::format_version,
    widget::{ScrollLines, ScrollLinesOptions, ScrollLinesState},
};

#[derive(Debug)]
pub struct ParquetPreviewState {
    pub scroll_lines_state: ScrollLinesState,
    text: String,
}

impl ParquetPreviewState {
    pub fn new(lines: Vec<String>) -> Self {
        let text = lines.join("\n");
        let lines: Vec<Line<'static>> = lines.into_iter().map(Line::raw).collect();
        // Footer metadata is structured, so default to no wrapping to keep the
        // alignment readable; the user can toggle wrapping if desired.
        let options = ScrollLinesOptions::new(true, false);
        Self {
            scroll_lines_state: ScrollLinesState::new(lines, options),
            text,
        }
    }

    pub fn text(&self) -> &str {
        &self.text
    }
}

#[derive(Debug)]
pub struct ParquetPreview<'a> {
    file_name: &'a str,
    file_version_id: Option<&'a str>,

    env: &'a Environment,
    theme: &'a Theme,
}

impl<'a> ParquetPreview<'a> {
    pub fn new(
        file_name: &'a str,
        file_version_id: Option<&'a str>,
        env: &'a Environment,
        theme: &'a Theme,
    ) -> Self {
        Self {
            file_name,
            file_version_id,
            env,
            theme,
        }
    }
}

impl StatefulWidget for ParquetPreview<'_> {
    type State = ParquetPreviewState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        let title = if let Some(version_id) = self.file_version_id {
            format!(
                "Footer [{} (Version ID: {})]",
                self.file_name,
                format_version(Some(version_id), self.env.fix_dynamic_values)
            )
        } else {
            format!("Footer [{}]", self.file_name)
        };
        ScrollLines::default()
            .block(Block::bordered().title(title))
            .theme(self.theme)
            .render(area, buf, &mut state.scroll_lines_state);
    }
}
