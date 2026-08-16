use regex::Regex;
use std::sync::LazyLock;

use super::normalize::{
    command_detection_variants, command_parser_limit_exceeded, grep_safe_detection_variant,
    normalize_command_for_detection,
};

struct HardlineRule {
    regex: Regex,
    description: &'static str,
}

static SUDO_STDIN_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(?:^|[;&|`\n]|&&|\|\||[$]\()\s*sudo\s+-S\b").unwrap());

fn build_hardline_rm_path(path_alt: &str) -> String {
    let tail = r"(?:\s|$|[)`;|&])";
    format!(r#"(?:['\x22](?:{path_alt})['\x22]|(?:{path_alt}){tail})"#)
}

static HARDLINE_PATTERNS: LazyLock<Vec<HardlineRule>> = LazyLock::new(|| {
    let cmdpos = r"(?:^|[\n`]|[$]\()\s*(?:sudo\s+(?:-[^\s]+\s+)*)?(?:env\s+(?:\w+=\S*\s+)*)?(?:(?:exec|nohup|setsid|time)\s+)*\s*";
    let rm_prefix = format!(r"{cmdpos}rm\s+(-[^\s]*\s+)*");
    let hardline_system_dirs = r"/home|/home/\*|/root|/root/\*|/etc|/etc/\*|/usr|/usr/\*|/var|/var/\*|/bin|/bin/\*|/sbin|/sbin/\*|/boot|/boot/\*|/lib|/lib/\*|/System|/System/\*";

    let root_rm_path = build_hardline_rm_path(r"/(?:(?:[.]/|[.][.]/)*)*(?:[.]|[.][.])?\**|/ \*");
    let sys_rm_path = build_hardline_rm_path(hardline_system_dirs);
    let home_rm_path = build_hardline_rm_path(r"(?:~|\$\{?HOME\}?)(?:/?|/\*)?");

    let raw_rules = vec![
        // rm root / system / home
        (
            format!(r"{rm_prefix}{root_rm_path}"),
            "recursive delete of root filesystem",
        ),
        (
            format!(r"{rm_prefix}{sys_rm_path}"),
            "recursive delete of system directory",
        ),
        (
            format!(r"{rm_prefix}{home_rm_path}"),
            "recursive delete of home directory",
        ),
        // Filesystem format
        (
            r"\bmkfs(\.[a-z0-9]+)?\b".to_string(),
            "format filesystem (mkfs)",
        ),
        // Raw block device overwrites
        (
            r"\bdd\b[^\n]*\bof=/dev/(sd|nvme|hd|mmcblk|vd|xvd|disk)[a-z0-9]*".to_string(),
            "dd to raw block device",
        ),
        (
            r">\s*/dev/(sd|nvme|hd|mmcblk|vd|xvd|disk)[a-z0-9]*\b".to_string(),
            "redirect to raw block device",
        ),
        // Fork bomb
        (
            r":\(\)\s*\{\s*:\s*\|\s*:\s*&\s*\}\s*;\s*:".to_string(),
            "fork bomb",
        ),
        // Kill all processes
        (
            r"\bkill\s+(-[^\s]+\s+)*-1\b".to_string(),
            "kill all processes",
        ),
        (r"\bkill\s+-9\s+-1\b".to_string(), "kill all processes"),
        // Shutdown / reboot / halt
        (
            format!(r"{cmdpos}(shutdown|reboot|halt|poweroff)\b"),
            "system shutdown/reboot",
        ),
        (
            format!(r"{cmdpos}init\s+[06]\b"),
            "init 0/6 (shutdown/reboot)",
        ),
        (
            format!(r"{cmdpos}systemctl\s+(poweroff|reboot|halt|kexec)\b"),
            "systemctl poweroff/reboot",
        ),
        (
            format!(r"{cmdpos}telinit\s+[06]\b"),
            "telinit 0/6 (shutdown/reboot)",
        ),
    ];

    raw_rules
        .into_iter()
        .map(|(pat, desc)| {
            let regex = regex::RegexBuilder::new(&pat)
                .case_insensitive(true)
                .dot_matches_new_line(true)
                .build()
                .unwrap_or_else(|e| panic!("invalid hardline regex {pat}: {e}"));
            HardlineRule {
                regex,
                description: desc,
            }
        })
        .collect()
});

pub fn check_sudo_stdin_guard(command: &str) -> Option<String> {
    if std::env::var("SUDO_PASSWORD").is_ok() {
        return None;
    }
    let normalized = normalize_command_for_detection(command);
    if SUDO_STDIN_RE.is_match(&normalized) {
        return Some("sudo password guessing via stdin (sudo -S)".to_string());
    }
    None
}

pub fn detect_hardline_command(command: &str) -> Option<String> {
    if command_parser_limit_exceeded(command) {
        return Some("command parser limit exceeded".to_string());
    }
    let normalized = normalize_command_for_detection(command);
    let (_, malformed_grep) = grep_safe_detection_variant(&normalized);
    if malformed_grep {
        return Some("command parser limit or malformed executable payload".to_string());
    }
    for variant in command_detection_variants(command) {
        let variant_lower = variant.to_ascii_lowercase();
        for rule in HARDLINE_PATTERNS.iter() {
            if rule.regex.is_match(&variant_lower) {
                return Some(rule.description.to_string());
            }
        }
    }
    if let Some(reason) = check_sudo_stdin_guard(command) {
        return Some(reason);
    }
    None
}
