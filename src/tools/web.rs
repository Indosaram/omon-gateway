use async_trait::async_trait;
use serde_json::{json, Value};

use super::Tool;
use crate::OmonError;

#[derive(Clone, Default)]
pub struct WebSearchTool;

#[async_trait]
impl Tool for WebSearchTool {
    fn name(&self) -> &str {
        "web_search"
    }

    fn description(&self) -> &str {
        "Search the live web for current information, documentation, news, or facts using DuckDuckGo search API."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The search query to execute."
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of search results to return (default 5)."
                }
            },
            "required": ["query"]
        })
    }

    async fn execute(&self, args: Value) -> Result<Value, OmonError> {
        let query = args
            .get("query")
            .and_then(Value::as_str)
            .ok_or_else(|| OmonError::ToolExecution("missing 'query'".into()))?;
        let max_results = args
            .get("max_results")
            .and_then(Value::as_u64)
            .unwrap_or(5)
            .clamp(1, 20) as usize;

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
            .build()
            .map_err(|e| OmonError::ToolExecution(e.to_string()))?;

        let url = format!(
            "https://html.duckduckgo.com/html/?q={}",
            urlencoding::encode(query)
        );

        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(|e| OmonError::ToolExecution(format!("search request failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(OmonError::ToolExecution(format!(
                "search provider returned {}",
                resp.status()
            )));
        }
        let html = resp
            .text()
            .await
            .map_err(|e| OmonError::ToolExecution(format!("failed to read response: {e}")))?;

        let mut results = Vec::new();
        for snippet in html
            .split("<div class=\"result__body\">")
            .skip(1)
            .take(max_results)
        {
            let title = extract_tag_content(snippet, "<a class=\"result__url\"", "</a>")
                .or_else(|| extract_tag_content(snippet, "<a class=\"result__snippet\"", "</a>"))
                .unwrap_or_default();
            let body = extract_tag_content(snippet, "<a class=\"result__snippet\"", "</a>")
                .unwrap_or_default();
            let href = extract_attribute(snippet, "href=\"", "\"").unwrap_or_default();

            if !title.is_empty() || !body.is_empty() {
                results.push(json!({
                    "title": clean_html(&title),
                    "snippet": clean_html(&body),
                    "url": href
                }));
            }
        }

        if results.is_empty() {
            return Err(OmonError::ToolExecution(
                "search provider response contained no parseable results".into(),
            ));
        }

        Ok(json!({
            "query": query,
            "count": results.len(),
            "results": results
        }))
    }
}

#[derive(Clone, Default)]
pub struct WebFetchTool;

#[async_trait]
impl Tool for WebFetchTool {
    fn name(&self) -> &str {
        "web_fetch"
    }

    fn description(&self) -> &str {
        "Fetch URL content and extract clean readable text/markdown from any webpage."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "The URL to fetch."
                },
                "max_chars": {
                    "type": "integer",
                    "description": "Maximum characters to return (default 8000)."
                }
            },
            "required": ["url"]
        })
    }

    async fn execute(&self, args: Value) -> Result<Value, OmonError> {
        let url = args
            .get("url")
            .and_then(Value::as_str)
            .ok_or_else(|| OmonError::ToolExecution("missing 'url'".into()))?;
        let max_chars = args
            .get("max_chars")
            .and_then(Value::as_u64)
            .unwrap_or(8000)
            .clamp(1, 100_000) as usize;
        let parsed = reqwest::Url::parse(url)
            .map_err(|error| OmonError::ToolExecution(format!("invalid URL: {error}")))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(OmonError::ToolExecution(
                "web fetch only supports http and https URLs".into(),
            ));
        }
        let fetch_url = format!("https://r.jina.ai/{}", parsed);

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
            .build()
            .map_err(|e| OmonError::ToolExecution(e.to_string()))?;

        let resp = client
            .get(&fetch_url)
            .send()
            .await
            .map_err(|e| OmonError::ToolExecution(format!("fetch request failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(OmonError::ToolExecution(format!(
                "fetch provider returned {}",
                resp.status()
            )));
        }
        let text = resp
            .text()
            .await
            .map_err(|e| OmonError::ToolExecution(format!("failed to read response: {e}")))?;

        let truncated: String = text.chars().take(max_chars).collect();

        Ok(json!({
            "url": url,
            "length": truncated.chars().count(),
            "content": truncated
        }))
    }
}

fn extract_tag_content(html: &str, start_tag: &str, end_tag: &str) -> Option<String> {
    let start = html.find(start_tag)?;
    let rest = &html[start..];
    let content_start = rest.find('>')? + 1;
    let end = rest[content_start..].find(end_tag)?;
    Some(rest[content_start..content_start + end].to_string())
}

fn extract_attribute(html: &str, attr_prefix: &str, end_char: &str) -> Option<String> {
    let start = html.find(attr_prefix)? + attr_prefix.len();
    let rest = &html[start..];
    let end = rest.find(end_char)?;
    Some(rest[..end].to_string())
}

fn clean_html(input: &str) -> String {
    let mut out = String::new();
    let mut inside = false;
    for c in input.chars() {
        if c == '<' {
            inside = true;
        } else if c == '>' {
            inside = false;
        } else if !inside {
            out.push(c);
        }
    }
    out.replace("&quot;", "\"")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
        .trim()
        .to_string()
}
