use regex::Regex;
use std::sync::LazyLock;

static ANSI_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:\x1B[@-Z\\-_]|[\x80-\x9A\x9C-\x9F]|(?:\x1B\[|\x9B)[0-?]*[ -/]*[@-~])").unwrap()
});

static LINE_CONTINUATION_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\\\r?\n").unwrap());

static BACKSLASH_ESCAPE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\\([^\n])").unwrap());

static EMPTY_QUOTES_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"''|""""#).unwrap());

static IFS_EXPANSION_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{IFS\b[^}]*\}|\$IFS\b").unwrap());

static SIMPLE_SHELL_LITERAL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z0-9_./:@%+=,-]+$").unwrap());

static PARAM_REPLACEMENT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{[^}/\\s]+/[^}/]*/(?P<replacement>[^}]*)\}").unwrap());

static PARAM_DEFAULT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{[^}:}\s]+:-(?P<default>[^}]*)\}").unwrap());

static ENV_ASSIGNMENT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*=.*").unwrap());

const MAX_DETECTION_COMMAND_CHARS: usize = 128_000;
const MAX_SEPARATOR_FREE_COMMAND_CHARS: usize = 4_096;
const MAX_DETECTION_SEGMENTS: usize = 25_000;

pub fn command_parser_limit_exceeded(command: &str) -> bool {
    if command.len() > MAX_DETECTION_COMMAND_CHARS {
        return true;
    }
    if command.len() > MAX_SEPARATOR_FREE_COMMAND_CHARS
        && !command.chars().any(|c| matches!(c, ';' | '&' | '|' | '\n'))
    {
        return true;
    }
    let mut separators = 0;
    for c in command.chars() {
        if matches!(c, ';' | '&' | '|' | '\n') {
            separators += 1;
            if separators >= MAX_DETECTION_SEGMENTS {
                return true;
            }
        }
    }
    false
}

pub fn strip_ansi(input: &str) -> String {
    ANSI_RE.replace_all(input, "").into_owned()
}

pub fn normalize_unicode_nfkc(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for c in input.chars() {
        match c {
            '\u{3000}' => out.push(' '),
            '\u{FF01}'..='\u{FF5E}' => {
                let ascii = ((c as u32) - 0xFF00 + 0x20) as u8;
                out.push(ascii as char);
            }
            other => out.push(other),
        }
    }
    out
}

pub fn rewrite_home_prefixes(mut command: String) -> String {
    let mut candidates = Vec::new();
    if let Ok(home) = std::env::var("HOME") {
        if !home.trim().is_empty() && home.trim() != "/" {
            candidates.push(home.trim().to_string());
        }
    }
    if let Ok(userprofile) = std::env::var("USERPROFILE") {
        if !userprofile.trim().is_empty() {
            candidates.push(userprofile.trim().to_string());
        }
    }
    candidates.sort_by_key(|b| std::cmp::Reverse(b.len()));

    for home in candidates {
        let home_norm = home.replace('\\', "/");
        let home_trimmed = home_norm.trim_end_matches('/');
        if home_trimmed.is_empty() || home_trimmed == "/" {
            continue;
        }
        let patterns = [
            format!("{home_trimmed}/"),
            format!("{}/", home.trim_end_matches('\\')),
        ];
        for pat in patterns {
            if command.contains(&pat) {
                command = command.replace(&pat, "~/");
            }
        }
    }
    command
}

pub fn normalize_command_for_detection(command: &str) -> String {
    let stripped = strip_ansi(command);
    let no_nulls = stripped.replace('\0', "");
    let normalized_unicode = normalize_unicode_nfkc(&no_nulls);
    let collapsed_lines = LINE_CONTINUATION_RE.replace_all(&normalized_unicode, "");
    let rewritten_home = rewrite_home_prefixes(collapsed_lines.into_owned());
    let stripped_escapes = BACKSLASH_ESCAPE_RE.replace_all(&rewritten_home, "$1");
    let stripped_quotes = EMPTY_QUOTES_RE.replace_all(&stripped_escapes, "");
    let collapsed_ifs = IFS_EXPANSION_RE.replace_all(&stripped_quotes, " ");
    collapsed_ifs.into_owned()
}

pub fn iter_top_level_shell_segments(command: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut start = 0;
    let mut quote: Option<char> = None;
    let mut escaped = false;
    let chars: Vec<char> = command.chars().collect();
    let mut index = 0;

    while index < chars.len() {
        let ch = chars[index];
        if escaped {
            escaped = false;
        } else if ch == '\\' && quote != Some('\'') {
            escaped = true;
        } else if let Some(q) = quote {
            if ch == q {
                quote = None;
            }
        } else if ch == '\'' || ch == '"' {
            quote = Some(ch);
        } else if matches!(ch, ';' | '&' | '|' | '\n') {
            if start < index {
                let seg: String = chars[start..index].iter().collect();
                segments.push(seg);
            }
            if (ch == '&' || ch == '|') && index + 1 < chars.len() && chars[index + 1] == ch {
                index += 1;
            }
            start = index + 1;
        }
        index += 1;
    }
    if start < chars.len() {
        let seg: String = chars[start..].iter().collect();
        segments.push(seg);
    }
    segments
}

pub fn iter_shell_command_starts(command: &str) -> Vec<usize> {
    let mut starts = vec![0];
    let mut quote: Option<char> = None;
    let bytes = command.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let ch = bytes[i];
        if quote == Some('\'') {
            if ch == b'\'' {
                quote = None;
            }
            i += 1;
            continue;
        }
        if quote == Some('"') {
            if ch == b'\\' && i + 1 < bytes.len() {
                i += 2;
                continue;
            }
            if ch == b'"' {
                quote = None;
                i += 1;
                continue;
            }
            if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'(' {
                starts.push(i + 2);
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        if ch == b'\'' || ch == b'"' {
            quote = Some(ch as char);
            i += 1;
            continue;
        }
        if ch == b'\\' && i + 1 < bytes.len() {
            i += 2;
            continue;
        }
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'(' {
            starts.push(i + 2);
            i += 2;
            continue;
        }
        if matches!(ch, b'(' | b'{' | b';') {
            starts.push(i + 1);
            i += 1;
            continue;
        }
        if ch == b'&' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'&' {
                starts.push(i + 2);
                i += 2;
            } else {
                starts.push(i + 1);
                i += 1;
            }
            continue;
        }
        if ch == b'|' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'|' {
                starts.push(i + 2);
                i += 2;
            } else {
                starts.push(i + 1);
                i += 1;
            }
            continue;
        }
        if ch == b'\n' {
            starts.push(i + 1);
        }
        i += 1;
    }

    let mut seen = std::collections::HashSet::new();
    let mut result = Vec::new();
    for start in starts {
        let mut pos = start;
        while pos < command.len() && command.as_bytes()[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos < command.len() && seen.insert(pos) {
            result.push(pos);
        }
    }
    result
}

pub fn mark_command_starts(command: &str) -> String {
    let mut offsets: Vec<usize> = iter_shell_command_starts(command)
        .into_iter()
        .filter(|&o| o > 0)
        .collect();
    if offsets.is_empty() {
        return command.to_string();
    }
    offsets.sort_unstable();

    let mut parts = Vec::new();
    let mut prev = 0;
    for offset in offsets {
        if offset > prev && offset <= command.len() {
            parts.push(&command[prev..offset]);
            parts.push("\n");
            prev = offset;
        }
    }
    if prev < command.len() {
        parts.push(&command[prev..]);
    }
    parts.join("")
}

fn scan_dollar_paren_end(command: &str, start: usize) -> Option<usize> {
    let mut depth = 1;
    let mut quote: Option<char> = None;
    let bytes = command.as_bytes();
    let mut i = start + 2;
    while i < bytes.len() {
        let ch = bytes[i];
        if let Some(q) = quote {
            if ch == b'\\' && q == '"' && i + 1 < bytes.len() {
                i += 2;
                continue;
            }
            if ch as char == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        if ch == b'\'' || ch == b'"' {
            quote = Some(ch as char);
            i += 1;
            continue;
        }
        if ch == b'\\' && i + 1 < bytes.len() {
            i += 2;
            continue;
        }
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'(' {
            depth += 1;
            i += 2;
            continue;
        }
        if ch == b')' {
            depth -= 1;
            i += 1;
            if depth == 0 {
                return Some(i);
            }
            continue;
        }
        i += 1;
    }
    None
}

fn scan_backtick_end(command: &str, start: usize) -> Option<usize> {
    let bytes = command.as_bytes();
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            i += 2;
            continue;
        }
        if bytes[i] == b'`' {
            return Some(i + 1);
        }
        i += 1;
    }
    None
}

pub fn read_shell_word(command: &str, pos: usize) -> (usize, usize, String) {
    let bytes = command.as_bytes();
    let mut start = pos;
    while start < bytes.len() && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    let mut i = start;
    let mut quote: Option<char> = None;
    while i < bytes.len() {
        let ch = bytes[i];
        if let Some(q) = quote {
            if ch == b'\\' && q == '"' && i + 1 < bytes.len() {
                i += 2;
                continue;
            }
            if ch as char == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        if ch == b'\'' || ch == b'"' {
            quote = Some(ch as char);
            i += 1;
            continue;
        }
        if ch == b'\\' && i + 1 < bytes.len() {
            i += 2;
            continue;
        }
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'(' {
            if let Some(end) = scan_dollar_paren_end(command, i) {
                i = end;
            } else {
                i += 2;
            }
            continue;
        }
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            if let Some(end) = bytes[i + 2..].iter().position(|&b| b == b'}') {
                i += 2 + end + 1;
            } else {
                i += 2;
            }
            continue;
        }
        if ch == b'`' {
            if let Some(end) = scan_backtick_end(command, i) {
                i = end;
            } else {
                i += 1;
            }
            continue;
        }
        if ch.is_ascii_whitespace() || matches!(ch, b';' | b'&' | b'|') {
            break;
        }
        i += 1;
    }
    (
        start,
        i,
        String::from_utf8_lossy(&bytes[start..i]).into_owned(),
    )
}

fn replace_simple_command_substitutions(word: &str) -> String {
    let mut chars = Vec::new();
    let mut i = 0;
    while i < word.len() {
        if word[i..].starts_with("$(") {
            if let Some(end) = scan_dollar_paren_end(word, i) {
                let inner = &word[i + 2..end - 1];
                if let Some(rep) = literal_command_substitution_output(inner) {
                    chars.extend(rep.chars());
                    i = end;
                    continue;
                }
            }
        }
        if word.as_bytes()[i] == b'`' {
            if let Some(end) = scan_backtick_end(word, i) {
                let inner = &word[i + 1..end - 1];
                if let Some(rep) = literal_command_substitution_output(inner) {
                    chars.extend(rep.chars());
                    i = end;
                    continue;
                }
            }
        }
        chars.push(word[i..].chars().next().unwrap());
        i += word[i..].chars().next().unwrap().len_utf8();
    }
    chars.into_iter().collect()
}

fn literal_command_substitution_output(script: &str) -> Option<String> {
    let tokens: Vec<&str> = script.split_whitespace().collect();
    if tokens.is_empty() {
        return None;
    }
    let cmd = tokens[0].to_ascii_lowercase();
    let mut args = &tokens[1..];
    if cmd == "echo" {
        while !args.is_empty() && args[0].starts_with("-n") {
            args = &args[1..];
        }
        if args.len() == 1 && SIMPLE_SHELL_LITERAL_RE.is_match(args[0]) {
            return Some(args[0].to_string());
        }
        return None;
    }
    if cmd == "printf" {
        if args.len() == 1 && SIMPLE_SHELL_LITERAL_RE.is_match(args[0]) {
            return Some(args[0].to_string());
        }
        if args.len() == 2 && args[0] == "%s" && SIMPLE_SHELL_LITERAL_RE.is_match(args[1]) {
            return Some(args[1].to_string());
        }
    }
    None
}

fn replace_simple_shell_expansions(word: &str) -> String {
    let w = replace_simple_command_substitutions(word);
    let w = PARAM_REPLACEMENT_RE.replace_all(&w, "$replacement");
    PARAM_DEFAULT_RE.replace_all(&w, "$default").into_owned()
}

fn strip_shell_word_syntax(word: &str) -> String {
    let mut chars = Vec::new();
    let mut quote: Option<char> = None;
    let bytes = word.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i];
        if let Some(q) = quote {
            if ch == b'\\' && q == '"' && i + 1 < bytes.len() {
                chars.push(bytes[i + 1] as char);
                i += 2;
                continue;
            }
            if ch as char == q {
                quote = None;
                i += 1;
                continue;
            }
            chars.push(ch as char);
            i += 1;
            continue;
        }
        if ch == b'\'' || ch == b'"' {
            quote = Some(ch as char);
            i += 1;
            continue;
        }
        if ch == b'\\' && i + 1 < bytes.len() {
            chars.push(bytes[i + 1] as char);
            i += 2;
            continue;
        }
        chars.push(ch as char);
        i += 1;
    }
    chars.into_iter().collect()
}

pub fn deobfuscate_shell_word_for_detection(word: &str) -> String {
    let mut deobfuscated = word.to_string();
    for _ in 0..2 {
        let prev = deobfuscated.clone();
        deobfuscated = replace_simple_shell_expansions(&deobfuscated);
        deobfuscated = strip_shell_word_syntax(&deobfuscated);
        if deobfuscated == prev {
            break;
        }
    }
    deobfuscated
}

pub fn iter_shell_command_word_spans(command: &str) -> Vec<(usize, usize, String)> {
    let mut result = Vec::new();
    for start in iter_shell_command_starts(command) {
        let mut pos = start;
        let mut prefix_words = 0;
        let mut skip_wrapper_options = false;
        let mut skip_next_wrapper_arg = false;
        while prefix_words < 12 {
            let (word_start, word_end, word) = read_shell_word(command, pos);
            if word_start == word_end {
                break;
            }
            let deobfuscated = deobfuscate_shell_word_for_detection(&word);
            let lower_word = deobfuscated.to_ascii_lowercase();

            if skip_next_wrapper_arg {
                skip_next_wrapper_arg = false;
                pos = word_end;
                prefix_words += 1;
                continue;
            }

            if skip_wrapper_options && lower_word.starts_with('-') {
                let opt_name = lower_word.split('=').next().unwrap_or("");
                let sudo_opts = [
                    "-c",
                    "--close-from",
                    "-g",
                    "--group",
                    "-h",
                    "--host",
                    "-p",
                    "--prompt",
                    "-u",
                    "--user",
                ];
                skip_next_wrapper_arg = !lower_word.contains('=') && sudo_opts.contains(&opt_name);
                pos = word_end;
                prefix_words += 1;
                continue;
            }

            result.push((word_start, word_end, word));
            prefix_words += 1;

            if matches!(
                lower_word.as_str(),
                "sudo" | "env" | "exec" | "nohup" | "setsid" | "time" | "command" | "builtin"
            ) {
                skip_wrapper_options = matches!(lower_word.as_str(), "sudo" | "env");
                pos = word_end;
                continue;
            }
            if ENV_ASSIGNMENT_RE.is_match(&deobfuscated) {
                skip_wrapper_options = false;
                pos = word_end;
                continue;
            }
            break;
        }
    }
    result
}

/// Tokenize a shell segment preserving start and end spans and whether it was quoted.
#[derive(Debug, Clone)]
pub struct ShellToken {
    pub value: String,
    pub start: usize,
    pub end: usize,
    pub inert_single_quoted: bool,
}

pub fn shell_tokens_with_spans(segment: &str, start: usize) -> Option<Vec<ShellToken>> {
    let bytes = segment.as_bytes();
    let mut tokens = Vec::new();
    let mut i = start;

    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let token_start = i;
        let mut value = Vec::new();
        let mut quote: Option<char> = None;

        while i < bytes.len() && (quote.is_some() || !bytes[i].is_ascii_whitespace()) {
            let char = bytes[i] as char;
            if let Some(q) = quote {
                if char == q {
                    quote = None;
                    i += 1;
                } else if char == '\\' && q == '"' && i + 1 < bytes.len() {
                    value.push(bytes[i + 1] as char);
                    i += 2;
                } else {
                    value.push(char);
                    i += 1;
                }
            } else if char == '\'' || char == '"' {
                quote = Some(char);
                i += 1;
            } else if char == '\\' {
                if i + 1 >= bytes.len() {
                    return None;
                }
                value.push(bytes[i + 1] as char);
                i += 2;
            } else {
                value.push(char);
                i += 1;
            }
        }
        if quote.is_some() {
            return None;
        }
        let raw = &segment[token_start..i];
        let inert_single_quoted = (raw.starts_with('\'') && raw.ends_with('\''))
            || (raw.contains("='") && raw.ends_with('\''));
        tokens.push(ShellToken {
            value: value.into_iter().collect(),
            start: token_start,
            end: i,
            inert_single_quoted,
        });
    }
    Some(tokens)
}

pub fn quoted_grep_pattern_spans(command: &str) -> (Vec<(usize, usize)>, bool) {
    let mut spans = Vec::new();
    let mut offset = 0;

    for segment in iter_top_level_shell_segments(command) {
        let segment_at = match command[offset..].find(&segment) {
            Some(idx) => offset + idx,
            None => {
                offset += segment.len();
                continue;
            }
        };
        offset = segment_at + segment.len();

        for (start, _, word) in iter_shell_command_word_spans(&segment) {
            let deobf = deobfuscate_shell_word_for_detection(&word).to_ascii_lowercase();
            let base_name = std::path::Path::new(&deobf)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or(&deobf);
            let is_grep = matches!(base_name, "grep" | "egrep" | "rg");
            let is_echo_or_printf = matches!(base_name, "echo" | "printf");
            if !is_grep && !is_echo_or_printf {
                continue;
            }
            if is_echo_or_printf && (segment.contains('|') || segment.contains('>')) {
                continue;
            }
            let tokens = match shell_tokens_with_spans(&segment, start) {
                Some(toks) => toks,
                None => return (Vec::new(), true),
            };
            if tokens.is_empty() {
                continue;
            }
            let args = &tokens[1..];

            if is_echo_or_printf {
                for tok in args {
                    if !tok.value.starts_with('-') {
                        spans.push((segment_at + tok.start, segment_at + tok.end));
                    }
                }
                continue;
            }
            let mut pattern_indexes: Vec<usize> = Vec::new();
            let mut explicit_patterns = false;
            let mut operand_index: Option<usize> = None;
            let mut i = 0;
            let mut options = true;

            let grep_opts_with_arg = [
                "--after-context",
                "--before-context",
                "--binary-files",
                "--context",
                "--directories",
                "--devices",
                "--exclude",
                "--exclude-dir",
                "--exclude-from",
                "--include",
                "--label",
                "--max-count",
                "--regexp",
                "--file",
            ];
            let grep_short_opts_with_arg = ['A', 'B', 'C', 'D', 'd', 'e', 'f', 'm'];

            while i < args.len() {
                let token = &args[i].value;
                if options && token == "--" {
                    options = false;
                    i += 1;
                    continue;
                }
                if options && token.starts_with("--") {
                    let (option, equals_val) = match token.split_once('=') {
                        Some((opt, val)) => (opt, Some(val)),
                        None => (token.as_str(), None),
                    };
                    if option == "--regexp" || option == "--file" {
                        explicit_patterns = true;
                    }
                    if grep_opts_with_arg.contains(&option) && equals_val.is_none() {
                        if i + 1 >= args.len() {
                            return (Vec::new(), true);
                        }
                        if option == "--regexp" {
                            pattern_indexes.push(i + 1);
                        }
                        i += 2;
                        continue;
                    }
                    if option == "--regexp" && equals_val.is_some() {
                        pattern_indexes.push(i);
                    }
                    i += 1;
                    continue;
                }
                if options && token.starts_with('-') && token != "-" {
                    let chars: Vec<char> = token[1..].chars().collect();
                    let mut j = 0;
                    while j < chars.len() {
                        let c = chars[j];
                        if c == 'e' || c == 'f' {
                            explicit_patterns = true;
                        }
                        if grep_short_opts_with_arg.contains(&c) {
                            if j + 1 < chars.len() {
                                if c == 'e' {
                                    pattern_indexes.push(i);
                                }
                            } else {
                                if i + 1 >= args.len() {
                                    return (Vec::new(), true);
                                }
                                if c == 'e' {
                                    pattern_indexes.push(i + 1);
                                }
                                i += 1;
                            }
                            break;
                        }
                        j += 1;
                    }
                    i += 1;
                    continue;
                }
                if operand_index.is_none() {
                    operand_index = Some(i);
                }
                i += 1;
            }

            if !explicit_patterns {
                if let Some(op) = operand_index {
                    pattern_indexes.push(op);
                }
            }

            for idx in pattern_indexes {
                if idx < args.len() {
                    let tok = &args[idx];
                    spans.push((segment_at + tok.start, segment_at + tok.end));
                }
            }
        }
    }
    (spans, false)
}

pub fn grep_safe_detection_variant(command: &str) -> (String, bool) {
    let (spans, malformed) = quoted_grep_pattern_spans(command);
    if malformed || spans.is_empty() {
        return (command.to_string(), malformed);
    }
    let mut out = String::with_capacity(command.len());
    let mut prev = 0;
    for (start, end) in spans {
        if start >= prev && start <= command.len() && end <= command.len() {
            out.push_str(&command[prev..start]);
            for _ in 0..(end - start) {
                out.push(' ');
            }
            prev = end;
        }
    }
    if prev < command.len() {
        out.push_str(&command[prev..]);
    }
    (out, false)
}

fn interpreter_family(executable: &str) -> Option<&'static str> {
    let lower = executable.to_ascii_lowercase();
    let name = std::path::Path::new(&lower)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(&lower);
    if name.starts_with("python") || name.starts_with("py") {
        return Some("python");
    }
    if name.starts_with("node") {
        return Some("node");
    }
    if name.starts_with("perl") {
        return Some("perl");
    }
    if name.starts_with("ruby") {
        return Some("ruby");
    }
    if name.starts_with("php") {
        return Some("php");
    }
    if name.starts_with("powershell") || name.starts_with("pwsh") {
        return Some("powershell");
    }
    None
}

pub fn execution_flag_findings(command: &str) -> Vec<(String, Option<String>)> {
    let mut findings = Vec::new();
    for segment in iter_top_level_shell_segments(command) {
        for (start, _, word) in iter_shell_command_word_spans(&segment) {
            let executable = deobfuscate_shell_word_for_detection(&word);
            let lower_exec = executable.to_ascii_lowercase();
            let base_name = std::path::Path::new(&lower_exec)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or(&lower_exec)
                .to_string();
            let family = interpreter_family(&base_name);
            let tokens = match shell_tokens_with_spans(&segment, start) {
                Some(t) => t,
                None => {
                    if family.is_some()
                        || matches!(base_name.as_str(), "sort" | "rg" | "ag" | "man")
                    {
                        findings.push((
                            "command parser limit or malformed executable payload".into(),
                            None,
                        ));
                    }
                    continue;
                }
            };
            if tokens.is_empty() {
                continue;
            }
            let args: Vec<String> = tokens[1..].iter().map(|t| t.value.clone()).collect();

            if let Some(fam) = family {
                // Check heredoc
                if args.iter().any(|a| a.starts_with("<<")) {
                    findings.push(("script execution via heredoc".into(), None));
                    continue;
                }
                // Check -c / -e flags
                match fam {
                    "python" => {
                        let mut i = 0;
                        while i < args.len() {
                            if args[i] == "-c" {
                                let payload = args.get(i + 1).cloned();
                                findings.push(("script execution via -e/-c flag".into(), payload));
                                break;
                            }
                            i += 1;
                        }
                    }
                    "node" => {
                        let mut i = 0;
                        while i < args.len() {
                            if matches!(args[i].as_str(), "-e" | "--eval" | "-p" | "--print") {
                                let payload = args.get(i + 1).cloned();
                                findings.push(("script execution via -e/-c flag".into(), payload));
                                break;
                            }
                            i += 1;
                        }
                    }
                    "perl" | "ruby" => {
                        let mut i = 0;
                        while i < args.len() {
                            if matches!(args[i].as_str(), "-e" | "--eval")
                                || args[i].starts_with("-e")
                            {
                                let payload = if args[i] == "-e" || args[i] == "--eval" {
                                    args.get(i + 1).cloned()
                                } else {
                                    Some(args[i][2..].to_string())
                                };
                                findings.push(("script execution via -e/-c flag".into(), payload));
                                break;
                            }
                            i += 1;
                        }
                    }
                    "php" => {
                        let mut i = 0;
                        while i < args.len() {
                            if args[i] == "-r" {
                                let payload = args.get(i + 1).cloned();
                                findings.push(("script execution via -e/-c flag".into(), payload));
                                break;
                            }
                            i += 1;
                        }
                    }
                    "powershell" => {
                        let mut i = 0;
                        while i < args.len() {
                            let lower = args[i].to_ascii_lowercase();
                            if matches!(lower.as_str(), "-command" | "-c") {
                                let payload = args.get(i + 1).cloned();
                                findings.push(("script execution via -e/-c flag".into(), payload));
                                break;
                            }
                            i += 1;
                        }
                    }
                    _ => {}
                }
            }

            if matches!(base_name.as_str(), "bash" | "sh" | "zsh" | "ksh") {
                let mut i = 0;
                while i < args.len() {
                    let arg = &args[i];
                    if arg.starts_with('-') && arg.contains('c') {
                        let payload = args.get(i + 1).cloned();
                        findings.push(("shell command via -c/-lc flag".into(), payload));
                        break;
                    }
                    i += 1;
                }
            }
        }
    }
    findings
}

pub fn command_detection_variants(command: &str) -> Vec<String> {
    let normalized = normalize_command_for_detection(command);
    let (grep_safe, _) = grep_safe_detection_variant(&normalized);
    let mut variants = Vec::new();
    let mut seen = std::collections::HashSet::new();

    seen.insert(grep_safe.clone());
    variants.push(grep_safe.clone());

    let mut pending = vec![normalized.clone()];
    while let Some(variant) = pending.pop() {
        for (_, payload) in execution_flag_findings(&variant) {
            if let Some(p) = payload {
                if seen.insert(p.clone()) {
                    variants.push(p.clone());
                    let marked_payload = mark_command_starts(&p);
                    if marked_payload != p && seen.insert(marked_payload.clone()) {
                        variants.push(marked_payload);
                    }
                    pending.push(p);
                }
            }
        }
    }

    let marked = mark_command_starts(&grep_safe);
    if marked != grep_safe && seen.insert(marked.clone()) {
        variants.push(marked);
    }

    for (word_start, word_end, word) in iter_shell_command_word_spans(&normalized) {
        let deobf = deobfuscate_shell_word_for_detection(&word);
        if !deobf.is_empty() && deobf != word {
            let mut var = normalized[..word_start].to_string();
            var.push_str(&deobf);
            var.push_str(&normalized[word_end..]);
            if seen.insert(var.clone()) {
                variants.push(var);
            }
        }
    }

    variants
}
