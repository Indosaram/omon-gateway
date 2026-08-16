use regex::Regex;
use std::sync::LazyLock;

use super::normalize::{
    command_detection_variants, command_parser_limit_exceeded, execution_flag_findings,
    normalize_command_for_detection,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DangerousFinding {
    pub pattern_key: String,
    pub description: String,
}

struct PatternRule {
    regex: Regex,
    description: &'static str,
}

static PATTERNS: LazyLock<Vec<PatternRule>> = LazyLock::new(|| {
    let raw_patterns: Vec<(&str, &'static str)> = vec![
        // rm
        (r"\brm\s+(-[^\s]*\s+)*/", "delete in root path"),
        (r"\brm\s+-[^\s]*r", "recursive delete"),
        (r"\brm\s+--recursive\b", "recursive delete (long flag)"),
        // Windows
        (
            r"\bcmd(?:\.exe)?\s+/(?:c|k)\s+.*\b(?:del|erase|rd|rmdir)\b",
            "Windows cmd destructive delete",
        ),
        (
            r"\b(?:powershell|pwsh)(?:\.exe)?\b(?:\s+-\S+)*\s+(?:-(?:command|c)\s+)?['\x22]?(?:remove-item|rmdir|erase|del|rd|ri|rm)\b",
            "Windows PowerShell destructive delete",
        ),
        (
            r"\b(?:powershell|pwsh)(?:\.exe)?\b.*\s-(?:encodedcommand|enc|e)\b",
            "PowerShell encoded command execution",
        ),
        // chmod / chown
        (
            r"\bchmod\s+(-[^\s]*\s+)*(777|666|o\+[rwx]*w|a\+[rwx]*w)\b",
            "world/other-writable permissions",
        ),
        (
            r"\bchmod\s+--recursive\b.*(777|666|o\+[rwx]*w|a\+[rwx]*w)",
            "recursive world/other-writable (long flag)",
        ),
        (r"\bchown\s+(-[^\s]*)?R\s+root", "recursive chown to root"),
        (
            r"\bchown\s+--recur[a-z]*\b.*root",
            "recursive chown to root (long flag)",
        ),
        // fs & device
        (r"\bmkfs(\.[a-z0-9]+)?\b", "format filesystem"),
        (
            r"\bdd\b[^\n]*\bof=/dev/(sd|nvme|hd|mmcblk|vd|xvd|disk)[a-z0-9]*",
            "dd to raw block device",
        ),
        (r"\bdd\s+.*if=", "disk copy"),
        (
            r">\s*/dev/(sd|nvme|hd|mmcblk|vd|xvd|disk)[a-z0-9]*",
            "write to block device",
        ),
        // SQL
        (r"\bDROP\s+(TABLE|DATABASE)\b", "SQL DROP"),
        (r"\bTRUNCATE\s+(TABLE)?\s*\w", "SQL TRUNCATE"),
        // systemctl
        (
            r"\bsystemctl\s+(-[^\s]+\s+)*(stop|restart|disable|mask)\b",
            "stop/restart system service",
        ),
        // kill & process
        (r"\bkill\s+-9\s+-1\b", "kill all processes"),
        (r"\bpkill\s+-9\b", "force kill processes"),
        (
            r"\bkillall\s+(-[^\s]*\s+)*-(9|KILL|SIGKILL)\b",
            "force kill processes (killall -KILL)",
        ),
        (
            r"\bkillall\s+(-[^\s]*\s+)*-s\s+(KILL|SIGKILL|9)\b",
            "force kill processes (killall -s KILL)",
        ),
        (
            r"\bkillall\s+(-[^\s]*\s+)*-r\b",
            "kill processes by regex (killall -r)",
        ),
        (r":\(\)\s*\{\s*:\s*\|\s*:\s*&\s*\}\s*;\s*:", "fork bomb"),
        // remote pipe to shell
        (
            r"\b(curl|wget)\b.*\|\s*(?:[/\w]*/)?(?:ba)?sh(?:\s|$|-c)",
            "pipe remote content to shell",
        ),
        (
            r"\b(bash|sh|zsh|ksh)\s+<\s*<?\s*\(\s*(curl|wget)\b",
            "execute remote script via process substitution",
        ),
        (
            r"(?:\beval\b|\bsource\b|\.)\s*(?:\$\(\s*|`\s*)(?:curl|wget)\b",
            "execute remote content via command substitution",
        ),
        // decode and execute
        (
            r"\b(base64|base32|base16)\s+(?:-[dD]|--decode)\b.*\|\s*\b(bash|sh|zsh|ksh|dash)\b",
            "pipe decoded content to shell (possible command obfuscation)",
        ),
        (
            r"\bxxd\s+-r\b.*\|\s*\b(bash|sh|zsh|ksh|dash)\b",
            "pipe xxd-decoded content to shell (possible command obfuscation)",
        ),
        (
            r"\becho\b[^|]*\|\s*\btr\b[^|]*\|\s*\b(bash|sh|zsh|ksh|dash)\b",
            "pipe tr-transformed output to shell (possible command obfuscation)",
        ),
        (
            r"\bopenssl\b.*\b(?:base64|enc)\b[^|]*\s+-[dD]\b[^|]*\|\s*\b(bash|sh|zsh|ksh|dash)\b",
            "pipe openssl-decoded content to shell (possible command obfuscation)",
        ),
        // xargs & find
        (r"\bxargs\s+.*\brm\b", "xargs with rm"),
        (
            r"\bfind\b.*-exec(?:dir)?\s+(/\S*/)?rm\b",
            "find -exec/-execdir rm",
        ),
        (r"\bfind\b.*-delete\b", "find -delete"),
        // gateway & process protection
        (
            r"\bhermes\s+(?:-{1,2}\S+(?:\s+\S+)?\s+)*gateway\s+(stop|restart)\b",
            "stop/restart hermes gateway (kills running agents)",
        ),
        (
            r"\bhermes\s+update\b",
            "hermes update (restarts gateway, kills running agents)",
        ),
        (
            r"\bdocker\s+compose\s+(restart|stop|kill|down)\b",
            "docker compose restart/stop/kill/down (container lifecycle)",
        ),
        (
            r"\bdocker\s+(restart|stop|kill)\b",
            "docker restart/stop/kill (container lifecycle)",
        ),
        (
            r"gateway\s+run\b.*(&\s*$|&\s*;|\bdisown\b|\bsetsid\b)",
            "start gateway outside systemd (use 'systemctl --user restart hermes-gateway')",
        ),
        (
            r"\bnohup\b.*gateway\s+run\b",
            "start gateway outside systemd (use 'systemctl --user restart hermes-gateway')",
        ),
        (
            r"\b(pkill|killall)\b.*\b(hermes|gateway|cli\.py)\b",
            "kill hermes/gateway process (self-termination)",
        ),
        (
            r"\bkill\b.*\$\(\s*(pgrep|pidof)\b",
            "kill process via pgrep/pidof expansion (self-termination)",
        ),
        (
            r"\bkill\b.*`\s*(pgrep|pidof)\b",
            "kill process via backtick pgrep/pidof expansion (self-termination)",
        ),
        (
            r"\blaunchctl\s+(stop|kickstart|bootout|unload|kill|disable|remove)\b.*\b(hermes|ai\.hermes)\b",
            "stop/restart hermes launchd service (kills running agents)",
        ),
        // sensitive path writes / edits / cp / mv
        (
            r">\s*(?:/etc/|/private/(?:etc|var|tmp|home)/)",
            "overwrite system config",
        ),
        (
            r"\btee\b.*['\x22]?(?:/etc/|/private/(?:etc|var|tmp|home)/|/dev/sd|(?:~|\$home|\$\{home\})/\.ssh(?:/|$)|(?:~/\.hermes/|(?:\$home|\$\{home\})/\.hermes/|(?:\$hermes_home|\$\{hermes_home\})/)\.env\b|(?:~/\.hermes/|(?:\$home|\$\{home\})/\.hermes/|(?:\$hermes_home|\$\{hermes_home\})/)(?:config\.yaml|config\.yml)\b|(?:~|\$home|\$\{home\})/\.(?:bashrc|zshrc|profile|bash_profile|zprofile)\b|(?:~|\$home|\$\{home\})/\.(?:netrc|pgpass|npmrc|pypirc)\b)",
            "overwrite system file via tee",
        ),
        (
            r">>?\s*['\x22]?(?:/etc/|/private/(?:etc|var|tmp|home)/|/dev/sd|(?:~|\$home|\$\{home\})/\.ssh(?:/|$)|(?:~/\.hermes/|(?:\$home|\$\{home\})/\.hermes/|(?:\$hermes_home|\$\{hermes_home\})/)\.env\b|(?:~/\.hermes/|(?:\$home|\$\{home\})/\.hermes/|(?:\$hermes_home|\$\{hermes_home\})/)(?:config\.yaml|config\.yml)\b|(?:~|\$home|\$\{home\})/\.(?:bashrc|zshrc|profile|bash_profile|zprofile)\b|(?:~|\$home|\$\{home\})/\.(?:netrc|pgpass|npmrc|pypirc)\b)",
            "overwrite system file via redirection",
        ),
        (
            r"\btee\b.*['\x22]?(?:(?:(?:/|\.{1,2}/)?(?:[^\s/'\x22`]+/)*\.env(?:\.[^/\s'\x22`]+)*)|(?:(?:/|\.{1,2}/)?(?:[^\s/'\x22`]+/)*(?:config\.yaml|config\.yml)))['\x22]?(?:[\s;&|<>\x22']|$)",
            "overwrite project env/config via tee",
        ),
        (
            r">>?\s*['\x22]?(?:(?:(?:/|\.{1,2}/)?(?:[^\s/'\x22`]+/)*\.env(?:\.[^/\s'\x22`]+)*)|(?:(?:/|\.{1,2}/)?(?:[^\s/'\x22`]+/)*(?:config\.yaml|config\.yml)))['\x22]?(?:[\s;&|<>\x22']|$)",
            "overwrite project env/config via redirection",
        ),
        (
            r"\b(cp|mv|install)\b.*\s(?:/etc/|/private/(?:etc|var|tmp|home)/)",
            "copy/move file into system config path",
        ),
        (
            r"\b(cp|mv|install)\b.*\s['\x22]?(?:(?:(?:/|\.{1,2}/)?(?:[^\s/'\x22`]+/)*\.env(?:\.[^/\s'\x22`]+)*)|(?:(?:/|\.{1,2}/)?(?:[^\s/'\x22`]+/)*(?:config\.yaml|config\.yml)))['\x22]?(?:\s*(?:&&|\|\||;).*)?$",
            "overwrite project env/config file",
        ),
        (
            r"\b(cp|mv|install)\b.*\s['\x22]?(?:/etc/|/private/(?:etc|var|tmp|home)/|/dev/sd|(?:~|\$home|\$\{home\})/\.ssh(?:/|$)|(?:~/\.hermes/|(?:\$home|\$\{home\})/\.hermes/|(?:\$hermes_home|\$\{hermes_home\})/)\.env\b|(?:~/\.hermes/|(?:\$home|\$\{home\})/\.hermes/|(?:\$hermes_home|\$\{hermes_home\})/)(?:config\.yaml|config\.yml)\b|(?:~|\$home|\$\{home\})/\.(?:bashrc|zshrc|profile|bash_profile|zprofile)\b|(?:~|\$home|\$\{home\})/\.(?:netrc|pgpass|npmrc|pypirc)\b)[^\s'\x22]*['\x22]?(?:\s*(?:&&|\|\||;).*)?$",
            "copy/move file into sensitive credential/SSH/shell-rc path",
        ),
        (
            r"\bsed\s+-[^\s]*i.*(?:(?:~|\$home|\$\{home\})/\.ssh(?:/|$)|(?:~|\$home|\$\{home\})/\.(?:bashrc|zshrc|profile|bash_profile|zprofile)\b|(?:~|\$home|\$\{home\})/\.(?:netrc|pgpass|npmrc|pypirc)\b)[^\s'\x22]*",
            "in-place edit of sensitive credential/SSH/shell-rc path",
        ),
        (
            r"\bsed\s+--in-place\b.*(?:(?:~|\$home|\$\{home\})/\.ssh(?:/|$)|(?:~|\$home|\$\{home\})/\.(?:bashrc|zshrc|profile|bash_profile|zprofile)\b|(?:~|\$home|\$\{home\})/\.(?:netrc|pgpass|npmrc|pypirc)\b)[^\s'\x22]*",
            "in-place edit of sensitive credential/SSH/shell-rc path (long flag)",
        ),
        (
            r"\b(?:perl|ruby)\b.*(?:^|\s)-[^\s]*i\b.*(?:(?:~|\$home|\$\{home\})/\.ssh(?:/|$)|(?:~|\$home|\$\{home\})/\.(?:bashrc|zshrc|profile|bash_profile|zprofile)\b|(?:~|\$home|\$\{home\})/\.(?:netrc|pgpass|npmrc|pypirc)\b)[^\s'\x22]*",
            "in-place edit of sensitive credential/SSH/shell-rc path (perl/ruby)",
        ),
        (
            r"\bsed\s+-[^\s]*i.*\s(?:/etc/|/private/(?:etc|var|tmp|home)/)",
            "in-place edit of system config",
        ),
        (
            r"\bsed\s+--in-place\b.*\s(?:/etc/|/private/(?:etc|var|tmp|home)/)",
            "in-place edit of system config (long flag)",
        ),
        (
            r"\bsed\s+-[^\s]*i.*(?:(?:~/\.hermes/|(?:\$home|\$\{home\})/\.hermes/|(?:\$hermes_home|\$\{hermes_home\})/)(?:config\.yaml|config\.yml)\b|(?:~/\.hermes/|(?:\$home|\$\{home\})/\.hermes/|(?:\$hermes_home|\$\{hermes_home\})/)\.env\b)",
            "in-place edit of Hermes config/env",
        ),
        (
            r"\bsed\s+--in-place\b.*(?:(?:~/\.hermes/|(?:\$home|\$\{home\})/\.hermes/|(?:\$hermes_home|\$\{hermes_home\})/)(?:config\.yaml|config\.yml)\b|(?:~/\.hermes/|(?:\$home|\$\{home\})/\.hermes/|(?:\$hermes_home|\$\{hermes_home\})/)\.env\b)",
            "in-place edit of Hermes config/env (long flag)",
        ),
        (
            r"\b(?:perl|ruby)\b.*(?:^|\s)-[^\s]*i\b.*(?:(?:~/\.hermes/|(?:\$home|\$\{home\})/\.hermes/|(?:\$hermes_home|\$\{hermes_home\})/)(?:config\.yaml|config\.yml)\b|(?:~/\.hermes/|(?:\$home|\$\{home\})/\.hermes/|(?:\$hermes_home|\$\{hermes_home\})/)\.env\b)",
            "in-place edit of Hermes config/env (perl/ruby)",
        ),
        // heredoc
        (r"\b(bash|sh|zsh|ksh)\s+<<", "shell execution via heredoc"),
        // git destructive
        (
            r"\bgit\s+reset\s+--h(?:a(?:r(?:d)?)?)?\b",
            "git reset --hard (destroys uncommitted changes)",
        ),
        (
            r"\bgit\s+push\b.*--forc[a-z]*\b",
            "git force push (rewrites remote history)",
        ),
        (
            r"\bgit\s+push\b.*-f\b",
            "git force push short flag (rewrites remote history)",
        ),
        (
            r"\bgit\s+clean\s+-[^\s]*f",
            "git clean with force (deletes untracked files)",
        ),
        (r"\bgit\s+branch\s+-D\b", "git branch force delete"),
        (
            r"\bgit\s+branch\b[^;|&\\n]*?(?:-d\b|--delete\b)[^;|&\\n]*?(?:-f\b|--force\b)",
            "git branch force delete (long flags)",
        ),
        (
            r"\bgit\s+branch\b[^;|&\\n]*?(?:-f\b|--force\b)[^;|&\\n]*?(?:-d\b|--delete\b)",
            "git branch force delete (long flags, force-first)",
        ),
        // chmod +x and run
        (
            r"\bchmod\s+\+x\b.*[;&|]+\s*\./",
            "chmod +x followed by immediate execution",
        ),
        // sudo privilege flags
        (
            r"\bsudo\b[^;|&\\n]*?\s+(?:-s\b|--st[a-z]*\b|-a\b|--a[a-z]*\b)",
            "sudo with privilege flag (stdin/askpass/shell/list)",
        ),
        (
            r"\bsudo\b[^;|&\\n]*?\s+-[a-z]*[sa][a-z]*\b",
            "sudo with combined-flag privilege escalation",
        ),
    ];

    raw_patterns
        .into_iter()
        .map(|(pat, desc)| {
            let regex = regex::RegexBuilder::new(pat)
                .case_insensitive(true)
                .dot_matches_new_line(true)
                .build()
                .unwrap_or_else(|e| panic!("invalid regex {pat}: {e}"));
            PatternRule {
                regex,
                description: desc,
            }
        })
        .collect()
});

static SQL_DELETE_FROM_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bDELETE\s+FROM\b").unwrap());

static SQL_WHERE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\bWHERE\b").unwrap());

fn check_sql_delete_without_where(command: &str) -> bool {
    for line in command.lines() {
        if SQL_DELETE_FROM_RE.is_match(line) && !SQL_WHERE_RE.is_match(line) {
            return true;
        }
    }
    false
}

pub fn is_verification_artifact_cleanup(command: &str) -> bool {
    let parts: Vec<&str> = command.split_whitespace().collect();
    if parts.len() != 3 || parts[0] != "rm" || parts[1] != "-f" {
        return false;
    }
    let target = parts[2];
    let path = std::path::Path::new(target);
    let temp_dir = std::env::temp_dir();
    let filename = match path.file_name().and_then(|s| s.to_str()) {
        Some(f) => f,
        None => return false,
    };
    if !path.starts_with(&temp_dir)
        && !target.starts_with("/tmp/")
        && !target.starts_with("/var/folders/")
    {
        return false;
    }
    filename.starts_with("hermes-verify-") || filename.starts_with("hermes-ad-hoc-")
}

pub fn detect_dangerous_command(command: &str) -> Option<DangerousFinding> {
    if command_parser_limit_exceeded(command) {
        return Some(DangerousFinding {
            pattern_key: "command parser limit exceeded".to_string(),
            description: "command parser limit exceeded".to_string(),
        });
    }
    if is_verification_artifact_cleanup(command) {
        return None;
    }

    for variant in command_detection_variants(command) {
        if check_sql_delete_without_where(&variant) {
            return Some(DangerousFinding {
                pattern_key: "SQL DELETE without WHERE".to_string(),
                description: "SQL DELETE without WHERE".to_string(),
            });
        }
        for rule in PATTERNS.iter() {
            if rule.regex.is_match(&variant) {
                return Some(DangerousFinding {
                    pattern_key: rule.description.to_string(),
                    description: rule.description.to_string(),
                });
            }
        }
    }

    let normalized = normalize_command_for_detection(command);
    if let Some((desc, _)) = execution_flag_findings(&normalized).into_iter().next() {
        return Some(DangerousFinding {
            pattern_key: desc.clone(),
            description: desc,
        });
    }

    None
}

pub fn is_dangerous(command: &str) -> bool {
    detect_dangerous_command(command).is_some()
}
