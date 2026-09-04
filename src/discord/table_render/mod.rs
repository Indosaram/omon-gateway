#[derive(Debug, Clone)]
pub struct RenderedTable {
    pub original_text: String,
    pub png_bytes: Vec<u8>,
    pub filename: String,
}

fn parse_table_row(line: &str) -> Vec<String> {
    let trimmed = line.trim();
    let inner = trimmed
        .strip_prefix('|')
        .unwrap_or(trimmed)
        .strip_suffix('|')
        .unwrap_or(trimmed);
    inner
        .split('|')
        .map(|col| col.trim().replace("<br>", "\n").replace("<br/>", "\n"))
        .collect()
}

fn is_table_separator(line: &str) -> bool {
    let trimmed = line.trim();
    if !trimmed.contains('|') {
        return false;
    }
    let inner = trimmed.strip_prefix('|').unwrap_or(trimmed);
    let inner = inner.strip_suffix('|').unwrap_or(inner);
    inner.split('|').all(|col| {
        let col = col.trim();
        !col.is_empty() && col.chars().all(|c| c == '-' || c == ':' || c == ' ')
    })
}

#[derive(Debug, Clone)]
pub struct MarkdownTable {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub raw: String,
}

pub fn extract_markdown_tables(text: &str) -> Vec<MarkdownTable> {
    let mut tables = Vec::new();
    let lines: Vec<&str> = text.lines().collect();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i].trim();
        if line.starts_with('|') && line.ends_with('|') && i + 1 < lines.len() {
            let next_line = lines[i + 1].trim();
            if is_table_separator(next_line) {
                let headers = parse_table_row(line);
                let mut rows = Vec::new();
                let mut raw_lines = vec![lines[i], lines[i + 1]];
                let mut j = i + 2;

                while j < lines.len() {
                    let row_line = lines[j].trim();
                    if row_line.starts_with('|') && row_line.ends_with('|') {
                        rows.push(parse_table_row(row_line));
                        raw_lines.push(lines[j]);
                        j += 1;
                    } else {
                        break;
                    }
                }

                if !rows.is_empty() {
                    tables.push(MarkdownTable {
                        headers,
                        rows,
                        raw: raw_lines.join("\n"),
                    });
                    i = j;
                    continue;
                }
            }
        }
        i += 1;
    }

    tables
}

pub fn render_table_to_svg(table: &MarkdownTable) -> String {
    let font_family = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Apple SD Gothic Neo', 'Noto Sans KR', sans-serif";
    let num_cols = table.headers.len().max(1);

    let cell_padding_x = 16.0;
    let cell_padding_y = 12.0;
    let line_height = 20.0;
    let font_size = 14.0;
    let header_font_size = 14.0;

    let mut max_chars_per_col = vec![0usize; num_cols];
    for (c, h) in table.headers.iter().enumerate() {
        if c < num_cols {
            let len = h.lines().map(|l| l.chars().count()).max().unwrap_or(0);
            max_chars_per_col[c] = max_chars_per_col[c].max(len);
        }
    }
    for row in &table.rows {
        for (c, cell) in row.iter().enumerate() {
            if c < num_cols {
                let len = cell.lines().map(|l| l.chars().count()).max().unwrap_or(0);
                max_chars_per_col[c] = max_chars_per_col[c].max(len);
            }
        }
    }

    let total_weights: usize = max_chars_per_col.iter().sum::<usize>().max(1);
    let target_total_width = 860.0f64;
    let col_widths: Vec<f64> = max_chars_per_col
        .iter()
        .map(|&w| {
            let ratio = (w as f64) / (total_weights as f64);
            (ratio * target_total_width).clamp(160.0, 480.0)
        })
        .collect();
    let actual_total_width: f64 = col_widths.iter().sum();

    let wrap_text = |text: &str, width: f64| -> Vec<String> {
        let mut wrapped = Vec::new();
        for paragraph in text.split('\n') {
            let paragraph = paragraph.trim();
            if paragraph.is_empty() {
                continue;
            }
            let max_line_chars = ((width - cell_padding_x * 2.0) / 9.0).max(10.0) as usize;
            let mut current = String::new();
            for word in paragraph.split_whitespace() {
                if current.chars().count() + word.chars().count() + 1 > max_line_chars {
                    if !current.is_empty() {
                        wrapped.push(current.clone());
                        current.clear();
                    }
                }
                if !current.is_empty() {
                    current.push(' ');
                }
                current.push_str(word);
            }
            if !current.is_empty() {
                wrapped.push(current);
            }
        }
        if wrapped.is_empty() {
            wrapped.push(String::new());
        }
        wrapped
    };

    let header_lines: Vec<Vec<String>> = table
        .headers
        .iter()
        .enumerate()
        .map(|(c, h)| wrap_text(h, col_widths[c]))
        .collect();
    let header_height = (header_lines.iter().map(|l| l.len()).max().unwrap_or(1) as f64
        * line_height)
        + cell_padding_y * 2.0;

    let mut row_data = Vec::new();
    let mut total_height = header_height + 1.0;

    for row in &table.rows {
        let cells: Vec<Vec<String>> = (0..num_cols)
            .map(|c| {
                let text = row.get(c).map(String::as_str).unwrap_or("");
                wrap_text(text, col_widths[c])
            })
            .collect();
        let height = (cells.iter().map(|l| l.len()).max().unwrap_or(1) as f64 * line_height)
            + cell_padding_y * 2.0;
        total_height += height;
        row_data.push((cells, height));
    }

    let mut svg = String::new();
    svg.push_str(&format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{:.0}\" height=\"{:.0}\" viewBox=\"0 0 {:.0} {:.0}\">\n",
        actual_total_width, total_height, actual_total_width, total_height
    ));
    svg.push_str("<rect width=\"100%\" height=\"100%\" fill=\"#2b2d31\" rx=\"8\" ry=\"8\"/>\n");

    let mut curr_x = 0.0;
    for (c, &col_w) in col_widths.iter().enumerate() {
        svg.push_str(&format!(
            "<rect x=\"{:.1}\" y=\"0\" width=\"{:.1}\" height=\"{:.1}\" fill=\"#1e1f22\"/>\n",
            curr_x, col_w, header_height
        ));
        let lines = &header_lines[c];
        let mut curr_y = cell_padding_y + 14.0;
        for line in lines {
            svg.push_str(&format!(
                "<text x=\"{:.1}\" y=\"{:.1}\" font-family=\"{}\" font-size=\"{:.1}\" font-weight=\"bold\" fill=\"#f2f3f5\">{}</text>\n",
                curr_x + cell_padding_x,
                curr_y,
                font_family,
                header_font_size,
                html_escape(line)
            ));
            curr_y += line_height;
        }
        curr_x += col_w;
    }

    svg.push_str(&format!(
        "<line x1=\"0\" y1=\"{:.1}\" x2=\"{:.1}\" y2=\"{:.1}\" stroke=\"#3f4147\" stroke-width=\"1\"/>\n",
        header_height, actual_total_width, header_height
    ));

    let mut curr_y = header_height;
    for (r, (cells, row_h)) in row_data.iter().enumerate() {
        let bg_color = if r % 2 == 0 { "#313338" } else { "#2b2d31" };
        let mut cell_x = 0.0;

        for (c, &col_w) in col_widths.iter().enumerate() {
            svg.push_str(&format!(
                "<rect x=\"{:.1}\" y=\"{:.1}\" width=\"{:.1}\" height=\"{:.1}\" fill=\"{}\"/>\n",
                cell_x, curr_y, col_w, row_h, bg_color
            ));

            let lines = &cells[c];
            let mut text_y = curr_y + cell_padding_y + 14.0;
            for line in lines {
                svg.push_str(&format!(
                    "<text x=\"{:.1}\" y=\"{:.1}\" font-family=\"{}\" font-size=\"{:.1}\" fill=\"#dbdee1\">{}</text>\n",
                    cell_x + cell_padding_x,
                    text_y,
                    font_family,
                    font_size,
                    html_escape(line)
                ));
                text_y += line_height;
            }
            cell_x += col_w;
        }

        svg.push_str(&format!(
            "<line x1=\"0\" y1=\"{:.1}\" x2=\"{:.1}\" y2=\"{:.1}\" stroke=\"#383a40\" stroke-width=\"1\"/>\n",
            curr_y + row_h,
            actual_total_width,
            curr_y + row_h
        ));
        curr_y += row_h;
    }

    svg.push_str("</svg>");
    svg
}

fn html_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

pub fn svg_to_png(svg_str: &str, scale: f32) -> Result<Vec<u8>, String> {
    let mut opt = usvg::Options::default();
    opt.font_family = "sans-serif".to_string();
    opt.fontdb_mut().load_system_fonts();

    let tree = usvg::Tree::from_str(svg_str, &opt).map_err(|e| e.to_string())?;

    let pixmap_size = tree
        .size()
        .to_int_size()
        .scale_by(scale)
        .ok_or_else(|| "invalid scale size".to_string())?;
    let mut pixmap = resvg::tiny_skia::Pixmap::new(pixmap_size.width(), pixmap_size.height())
        .ok_or_else(|| "failed to allocate pixmap".to_string())?;

    let transform = resvg::tiny_skia::Transform::from_scale(scale, scale);
    resvg::render(&tree, transform, &mut pixmap.as_mut());

    pixmap.encode_png().map_err(|e| e.to_string())
}

pub fn transform_markdown_tables_to_images(content: &str) -> (String, Vec<RenderedTable>) {
    let tables = extract_markdown_tables(content);
    if tables.is_empty() {
        return (content.to_string(), Vec::new());
    }

    let mut rendered = Vec::new();
    let mut modified = content.to_string();

    for (idx, table) in tables.into_iter().enumerate() {
        let svg = render_table_to_svg(&table);
        match svg_to_png(&svg, 2.0) {
            Ok(png_bytes) => {
                let filename = format!("table_{}.png", idx + 1);
                let placeholder = format!("\n*(📊 아래 첨부된 표 [{filename}] 참조)*\n");
                modified = modified.replace(&table.raw, &placeholder);

                rendered.push(RenderedTable {
                    original_text: table.raw,
                    png_bytes,
                    filename,
                });
            }
            Err(err) => {
                tracing::warn!(%err, "Failed to rasterize table SVG to PNG; keeping raw text");
            }
        }
    }

    (modified, rendered)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_markdown_tables() {
        let sample = "Header\n\n| Col A | Col B |\n|---|---|\n| Val 1 | Val 2 |\n\nFooter";
        let tables = extract_markdown_tables(sample);
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].headers, vec!["Col A", "Col B"]);
        assert_eq!(tables[0].rows.len(), 1);
        assert_eq!(tables[0].rows[0], vec!["Val 1", "Val 2"]);
    }

    #[test]
    fn test_render_table_to_svg_and_png() {
        let sample = "| Col A | Col B |\n|---|---|\n| Val 1 | Val 2<br>Line 2 |";
        let tables = extract_markdown_tables(sample);
        assert_eq!(tables.len(), 1);
        let svg = render_table_to_svg(&tables[0]);
        assert!(svg.contains("<svg"));
        assert!(svg.contains("Val 1"));

        let png = svg_to_png(&svg, 1.0).expect("should render png");
        assert!(!png.is_empty());
        assert_eq!(&png[0..8], b"\x89PNG\r\n\x1a\n");
    }
}
