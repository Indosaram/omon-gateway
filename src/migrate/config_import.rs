use crate::migrate::sys::MigrationEnv;
use crate::{OmonError, Result};
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

const SCALAR_ENV_KEYS: [&str; 4] = [
    "DISCORD_ALLOWED_USERS",
    "DISCORD_FREE_RESPONSE_CHANNELS",
    "DISCORD_HOME_CHANNEL",
    "APPROVAL_MODE",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigImportResult {
    pub values: BTreeMap<String, String>,
    pub diff: String,
    pub backup_path: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
struct HermesConfig {
    #[serde(default)]
    model: HermesModel,
    #[serde(default)]
    approvals: HermesApprovals,
    #[serde(default)]
    discord: HermesDiscord,
}

#[derive(Debug, Default, Deserialize)]
struct HermesModel {
    default: Option<String>,
    #[serde(rename = "provider")]
    _provider: Option<String>,
    base_url: Option<String>,
    api_key: Option<String>,
    #[serde(rename = "api_mode")]
    _api_mode: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct HermesApprovals {
    mode: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct HermesDiscord {
    bot_token: Option<String>,
    token: Option<String>,
}

pub fn import_config(
    env: &dyn MigrationEnv,
    hermes_root: &Path,
    target_env: &Path,
    dry_run: bool,
) -> Result<ConfigImportResult> {
    validate_root(env, hermes_root)?;

    let root_config_path = hermes_root.join("config.yaml");
    let root_env_path = hermes_root.join(".env");
    let root_config = read_yaml(env, &root_config_path)?;
    let root_env = read_optional_env(env, &root_env_path)?;
    let profiles = read_profiles(env, hermes_root)?;

    // Parse and validate every source before touching the target. This keeps malformed input from
    // producing a partially migrated authoritative file.
    let mut profile_sources = Vec::with_capacity(profiles.len());
    for profile in profiles {
        let profile_env = read_optional_env(env, &profile.join(".env"))?;
        let profile_config_path = profile.join("config.yaml");
        let profile_config = if env.exists(&profile_config_path) {
            Some(read_yaml(env, &profile_config_path)?)
        } else {
            None
        };
        profile_sources.push((profile_env, profile_config));
    }

    let values = map_values(&root_config, &root_env, &profile_sources);
    validate_output_values(&values)?;

    let old_values = if env.exists(target_env) {
        Some(parse_env(target_env, &env.read_to_string(target_env)?)?)
    } else {
        None
    };
    let diff = masked_diff(old_values.as_ref(), &values);

    if dry_run {
        return Ok(ConfigImportResult {
            values,
            diff,
            backup_path: None,
        });
    }

    let backup_path = if env.exists(target_env) {
        let backup_path = backup_path(target_env, env.now());
        let current = env.read(target_env)?;
        env.write(&backup_path, &current)?;
        Some(backup_path)
    } else {
        None
    };

    env.write(target_env, render_env(&values).as_bytes())?;

    Ok(ConfigImportResult {
        values,
        diff,
        backup_path,
    })
}

fn validate_root(env: &dyn MigrationEnv, hermes_root: &Path) -> Result<()> {
    if !env.exists(hermes_root) {
        return Err(OmonError::Config(format!(
            "Hermes home does not exist: {}",
            hermes_root.display()
        )));
    }
    if !env.is_dir(hermes_root) {
        return Err(OmonError::Config(format!(
            "Hermes home is not a directory: {}",
            hermes_root.display()
        )));
    }
    Ok(())
}

fn read_yaml(env: &dyn MigrationEnv, path: &Path) -> Result<HermesConfig> {
    if !env.is_file(path) {
        return Err(OmonError::Config(format!(
            "Hermes config is not a file: {}",
            path.display()
        )));
    }
    let contents = env.read_to_string(path)?;
    serde_yaml::from_str(&contents).map_err(|error| {
        OmonError::Config(format!(
            "failed to parse Hermes config {}: {error}",
            path.display()
        ))
    })
}

fn read_optional_env(env: &dyn MigrationEnv, path: &Path) -> Result<BTreeMap<String, String>> {
    if !env.exists(path) {
        return Ok(BTreeMap::new());
    }
    if !env.is_file(path) {
        return Err(OmonError::Config(format!(
            "Hermes environment path is not a file: {}",
            path.display()
        )));
    }
    parse_env(path, &env.read_to_string(path)?)
}

fn read_profiles(env: &dyn MigrationEnv, hermes_root: &Path) -> Result<Vec<PathBuf>> {
    let profiles_root = hermes_root.join("profiles");
    if !env.exists(&profiles_root) {
        return Ok(Vec::new());
    }
    if !env.is_dir(&profiles_root) {
        return Err(OmonError::Config(format!(
            "Hermes profiles path is not a directory: {}",
            profiles_root.display()
        )));
    }
    let mut profiles = env
        .read_dir(&profiles_root)?
        .into_iter()
        .filter(|path| env.is_dir(path))
        .collect::<Vec<_>>();
    profiles.sort();
    Ok(profiles)
}

fn parse_env(path: &Path, contents: &str) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    for (index, raw_line) in contents.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = line.split_once('=').ok_or_else(|| {
            OmonError::Config(format!(
                "failed to parse {} line {}: expected KEY=VALUE",
                path.display(),
                index + 1
            ))
        })?;
        let key = key.trim();
        if key.is_empty() {
            return Err(OmonError::Config(format!(
                "failed to parse {} line {}: empty key",
                path.display(),
                index + 1
            )));
        }
        values.insert(
            key.to_string(),
            strip_matching_quotes(value.trim()).to_string(),
        );
    }
    Ok(values)
}

fn strip_matching_quotes(value: &str) -> &str {
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        if (bytes[0] == b'\'' && bytes[value.len() - 1] == b'\'')
            || (bytes[0] == b'"' && bytes[value.len() - 1] == b'"')
        {
            return &value[1..value.len() - 1];
        }
    }
    value
}

fn map_values(
    root_config: &HermesConfig,
    root_env: &BTreeMap<String, String>,
    profiles: &[(BTreeMap<String, String>, Option<HermesConfig>)],
) -> BTreeMap<String, String> {
    let mut output = BTreeMap::new();

    insert_nonempty(
        &mut output,
        "DEFAULT_MODEL",
        root_config.model.default.as_deref(),
    );
    let claude = root_config
        .model
        .default
        .as_deref()
        .is_some_and(|model| model.to_ascii_lowercase().starts_with("claude"));
    if claude {
        insert_nonempty(
            &mut output,
            "ANTHROPIC_BASE_URL",
            root_config.model.base_url.as_deref(),
        );
        insert_nonempty(
            &mut output,
            "ANTHROPIC_API_KEY",
            root_config.model.api_key.as_deref(),
        );
    } else {
        insert_nonempty(
            &mut output,
            "OPENAI_API_BASE",
            root_config.model.base_url.as_deref(),
        );
        insert_nonempty(
            &mut output,
            "OPENAI_API_KEY",
            root_config.model.api_key.as_deref(),
        );
    }

    let primary = root_env
        .get("DISCORD_BOT_TOKEN")
        .map(String::as_str)
        .filter(|value| !value.is_empty());
    insert_nonempty(&mut output, "DISCORD_BOT_TOKEN", primary);

    let mut seen_tokens = HashSet::new();
    if let Some(primary) = primary {
        seen_tokens.insert(primary.to_string());
    }
    let mut extra_tokens = Vec::new();
    for (profile_env, profile_config) in profiles {
        let token = profile_env
            .get("DISCORD_BOT_TOKEN")
            .map(String::as_str)
            .or_else(|| {
                profile_config.as_ref().and_then(|config| {
                    config
                        .discord
                        .bot_token
                        .as_deref()
                        .or(config.discord.token.as_deref())
                })
            });
        if let Some(token) = token.filter(|value| !value.is_empty()) {
            if seen_tokens.insert(token.to_string()) {
                extra_tokens.push(token.to_string());
            }
        }
    }
    if !extra_tokens.is_empty() {
        output.insert("DISCORD_BOT_TOKENS".into(), extra_tokens.join(","));
    }

    for key in SCALAR_ENV_KEYS {
        let root_value = root_env.get(key).map(String::as_str);
        let profile_value = profiles
            .iter()
            .find_map(|(profile_env, _)| profile_env.get(key).map(String::as_str));
        insert_nonempty(&mut output, key, root_value.or(profile_value));
    }
    if !output.contains_key("APPROVAL_MODE") {
        insert_nonempty(
            &mut output,
            "APPROVAL_MODE",
            root_config.approvals.mode.as_deref(),
        );
    }

    output
}

fn insert_nonempty(output: &mut BTreeMap<String, String>, key: &str, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        output.insert(key.to_string(), value.to_string());
    }
}

fn validate_output_values(values: &BTreeMap<String, String>) -> Result<()> {
    if let Some((key, _)) = values
        .iter()
        .find(|(_, value)| value.contains(['\n', '\r']))
    {
        return Err(OmonError::Config(format!(
            "cannot write migrated environment key {key}: value contains a newline"
        )));
    }
    Ok(())
}

fn render_env(values: &BTreeMap<String, String>) -> String {
    let mut rendered = String::new();
    for (key, value) in values {
        rendered.push_str(key);
        rendered.push('=');
        rendered.push_str(value);
        rendered.push('\n');
    }
    rendered
}

fn masked_diff(
    old_values: Option<&BTreeMap<String, String>>,
    new_values: &BTreeMap<String, String>,
) -> String {
    let empty = BTreeMap::new();
    let old_values = old_values.unwrap_or(&empty);
    let keys = old_values
        .keys()
        .chain(new_values.keys())
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let mut lines = Vec::new();
    for key in keys {
        match (old_values.get(&key), new_values.get(&key)) {
            (None, Some(value)) => lines.push(format!("+ {key}=*** ({} chars)", value.len())),
            (Some(value), None) => lines.push(format!("- {key}=*** ({} chars)", value.len())),
            (Some(old), Some(new)) if old != new => lines.push(format!(
                "~ {key}=*** ({} -> {} chars)",
                old.len(),
                new.len()
            )),
            (Some(value), Some(_)) => {
                lines.push(format!("= {key}=*** ({} chars, unchanged)", value.len()))
            }
            (None, None) => {}
        }
    }
    lines.join("\n")
}

fn backup_path(target_env: &Path, now: chrono::DateTime<chrono::Utc>) -> PathBuf {
    let mut path = target_env.as_os_str().to_os_string();
    path.push(format!(".bak-{}", now.format("%Y%m%dT%H%M%SZ")));
    PathBuf::from(path)
}

#[cfg(test)]
mod tests {
    use super::import_config;
    use crate::migrate::sys::{FakeMigrationEnv, MigrationEnv};
    use crate::OmonError;
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};

    struct RuntimeConfigProjection {
        discord_bot_tokens: Vec<String>,
        default_model: String,
        openai_api_base: Option<String>,
        openai_api_key: Option<String>,
    }

    impl RuntimeConfigProjection {
        // `Config` and `Config::from_env` live privately in the binary crate (`main.rs`), so lib
        // unit tests cannot name them without widening the production API or booting the binary.
        // Parse the importer's key/value map with the same token, required-key, and optional-key
        // semantics instead. This avoids process-global environment mutation in parallel tests.
        fn from_values(values: &BTreeMap<String, String>) -> crate::Result<Self> {
            let mut discord_bot_tokens = Vec::new();
            for key in ["DISCORD_BOT_TOKEN", "DISCORD_BOT_TOKENS"] {
                if let Some(tokens) = values.get(key) {
                    for token in tokens.split(',') {
                        let token = token.trim().trim_matches('"').trim_matches('\'');
                        if !token.is_empty() && !discord_bot_tokens.iter().any(|item| item == token)
                        {
                            discord_bot_tokens.push(token.to_owned());
                        }
                    }
                }
            }
            if discord_bot_tokens.is_empty() {
                return Err(OmonError::Config(
                    "missing required environment variable DISCORD_BOT_TOKEN".into(),
                ));
            }
            let default_model = values.get("DEFAULT_MODEL").cloned().ok_or_else(|| {
                OmonError::Config("missing required environment variable DEFAULT_MODEL".into())
            })?;
            Ok(Self {
                discord_bot_tokens,
                default_model,
                openai_api_base: values.get("OPENAI_API_BASE").cloned(),
                openai_api_key: values.get("OPENAI_API_KEY").cloned(),
            })
        }
    }

    fn fixture() -> FakeMigrationEnv {
        FakeMigrationEnv::new(Utc.with_ymd_and_hms(2026, 8, 15, 14, 30, 45).unwrap())
    }

    fn write(env: &FakeMigrationEnv, path: &str, contents: &str) {
        env.write(Path::new(path), contents.as_bytes()).unwrap();
    }

    #[test]
    fn routes_claude_model_to_anthropic_and_maps_runtime_keys() {
        let env = fixture();
        write(
            &env,
            "/hermes/config.yaml",
            "model:\n  default: Claude-3-7-Sonnet\n  provider: custom:quotio\n  base_url: https://anthropic.example/v1\n  api_key: anthropic-secret\n  api_mode: messages\napprovals:\n  mode: smart\n",
        );
        write(
            &env,
            "/hermes/.env",
            "DISCORD_BOT_TOKEN=primary\nDISCORD_ALLOWED_USERS=1,2\nDISCORD_FREE_RESPONSE_CHANNELS=3\nDISCORD_HOME_CHANNEL=4\n",
        );

        let result = import_config(
            &env,
            Path::new("/hermes"),
            Path::new("/gateway/.env"),
            false,
        )
        .unwrap();

        assert_eq!(
            result.values.get("DEFAULT_MODEL").map(String::as_str),
            Some("Claude-3-7-Sonnet")
        );
        assert_eq!(
            result.values.get("ANTHROPIC_BASE_URL").map(String::as_str),
            Some("https://anthropic.example/v1")
        );
        assert_eq!(
            result.values.get("ANTHROPIC_API_KEY").map(String::as_str),
            Some("anthropic-secret")
        );
        assert!(!result.values.contains_key("OPENAI_API_BASE"));
        assert!(!result.values.contains_key("OPENAI_API_KEY"));
        for key in [
            "DEFAULT_MODEL",
            "DISCORD_BOT_TOKEN",
            "DISCORD_ALLOWED_USERS",
            "DISCORD_FREE_RESPONSE_CHANNELS",
            "DISCORD_HOME_CHANNEL",
            "APPROVAL_MODE",
        ] {
            assert!(
                result.values.contains_key(key),
                "Config::from_env key missing: {key}"
            );
        }
        assert!(!result.diff.contains("anthropic-secret"));
        assert!(!result.diff.contains("primary"));
        println!(
            "claude mapping keys={:?}",
            result.values.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn imported_values_round_trip_through_runtime_environment_parsing() {
        let env = fixture();
        write(
            &env,
            "/hermes/config.yaml",
            "model:\n  default: gpt-5.6-luna\n  provider: custom:quotio\n  base_url: https://quotio.example/v1\n  api_key: quotio-secret\n",
        );
        write(&env, "/hermes/.env", "DISCORD_BOT_TOKEN=primary\n");

        let result = import_config(
            &env,
            Path::new("/hermes"),
            Path::new("/gateway/.env"),
            false,
        )
        .unwrap();
        let parsed = RuntimeConfigProjection::from_values(&result.values).unwrap();
        assert_eq!(parsed.discord_bot_tokens, ["primary"]);
        assert_eq!(parsed.default_model, "gpt-5.6-luna");
        assert_eq!(
            parsed.openai_api_base.as_deref(),
            Some("https://quotio.example/v1")
        );
        assert_eq!(parsed.openai_api_key.as_deref(), Some("quotio-secret"));
        assert_eq!(
            result.values.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                "DEFAULT_MODEL",
                "DISCORD_BOT_TOKEN",
                "OPENAI_API_BASE",
                "OPENAI_API_KEY"
            ]
        );
    }

    #[test]
    fn routes_non_claude_custom_provider_to_openai_compatible_keys() {
        let env = fixture();
        write(
            &env,
            "/hermes/config.yaml",
            "model:\n  default: gpt-5.6-luna\n  provider: custom:quotio\n  base_url: https://quotio.example/v1\n  api_key: quotio-secret\n  api_mode: chat_completions\n",
        );
        write(&env, "/hermes/.env", "DISCORD_BOT_TOKEN=primary\n");

        let result = import_config(
            &env,
            Path::new("/hermes"),
            Path::new("/gateway/.env"),
            false,
        )
        .unwrap();

        assert_eq!(
            result.values.get("OPENAI_API_BASE").map(String::as_str),
            Some("https://quotio.example/v1")
        );
        assert_eq!(
            result.values.get("OPENAI_API_KEY").map(String::as_str),
            Some("quotio-secret")
        );
        assert!(!result.values.contains_key("ANTHROPIC_BASE_URL"));
        println!("custom provider tolerated; openai-compatible keys emitted");
    }

    #[test]
    fn dedupes_profile_tokens_in_stable_order_and_strips_quotes() {
        let env = fixture();
        write(&env, "/hermes/config.yaml", "model:\n  default: gpt-4o\n");
        write(
            &env,
            "/hermes/.env",
            "DISCORD_BOT_TOKEN='primary'\nAPPROVAL_MODE=\"always\"\n",
        );
        write(
            &env,
            "/hermes/profiles/zeta/.env",
            "DISCORD_BOT_TOKEN=third\n",
        );
        write(
            &env,
            "/hermes/profiles/alpha/.env",
            "DISCORD_BOT_TOKEN=\"second\"\n",
        );
        write(
            &env,
            "/hermes/profiles/beta/.env",
            "DISCORD_BOT_TOKEN='primary'\n",
        );

        let result = import_config(
            &env,
            Path::new("/hermes"),
            Path::new("/gateway/.env"),
            false,
        )
        .unwrap();

        assert_eq!(
            result.values.get("DISCORD_BOT_TOKEN").map(String::as_str),
            Some("primary")
        );
        assert_eq!(
            result.values.get("DISCORD_BOT_TOKENS").map(String::as_str),
            Some("second,third")
        );
        assert_eq!(
            result.values.get("APPROVAL_MODE").map(String::as_str),
            Some("always")
        );
        println!("profile token union order=primary,second,third (values masked in importer diff)");
    }

    #[test]
    fn env_parser_ignores_comments_and_uses_last_value_in_a_file() {
        let env = fixture();
        write(&env, "/hermes/config.yaml", "model:\n  default: gpt-4o\n");
        write(
            &env,
            "/hermes/.env",
            "# old token below is superseded\nDISCORD_BOT_TOKEN=old\n\nDISCORD_BOT_TOKEN=\"primary\"\n",
        );

        let result = import_config(
            &env,
            Path::new("/hermes"),
            Path::new("/gateway/.env"),
            false,
        )
        .unwrap();

        assert_eq!(
            result.values.get("DISCORD_BOT_TOKEN").map(String::as_str),
            Some("primary")
        );
    }

    #[test]
    fn omits_missing_and_empty_values() {
        let env = fixture();
        write(
            &env,
            "/hermes/config.yaml",
            "model:\n  default: gpt-4o\n  base_url: ''\n  api_key: null\n",
        );
        write(
            &env,
            "/hermes/.env",
            "DISCORD_BOT_TOKEN=primary\nDISCORD_ALLOWED_USERS=\n",
        );

        let result = import_config(
            &env,
            Path::new("/hermes"),
            Path::new("/gateway/.env"),
            false,
        )
        .unwrap();

        assert_eq!(result.values.len(), 2);
        assert!(result.values.contains_key("DEFAULT_MODEL"));
        assert!(result.values.contains_key("DISCORD_BOT_TOKEN"));
        assert!(!result.values.contains_key("DISCORD_BOT_TOKENS"));
        assert!(!result.values.contains_key("OPENAI_API_BASE"));
        assert!(!result.values.contains_key("DISCORD_ALLOWED_USERS"));
    }

    #[test]
    fn backs_up_existing_env_before_authoritative_overwrite() {
        let env = fixture();
        write(&env, "/hermes/config.yaml", "model:\n  default: gpt-4o\n");
        write(&env, "/hermes/.env", "DISCORD_BOT_TOKEN=primary\n");
        write(&env, "/gateway/.env", "OLD=value\n");
        let writes_before = env.write_calls().len();

        let result = import_config(
            &env,
            Path::new("/hermes"),
            Path::new("/gateway/.env"),
            false,
        )
        .unwrap();

        let backup = PathBuf::from("/gateway/.env.bak-20260815T143045Z");
        assert_eq!(result.backup_path.as_deref(), Some(backup.as_path()));
        assert!(env.rename_calls().is_empty());
        assert_eq!(env.read_to_string(&backup).unwrap(), "OLD=value\n");
        let writes = &env.write_calls()[writes_before..];
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[0].0, backup);
        assert_eq!(writes[0].1, b"OLD=value\n");
        assert_eq!(writes[1].0, PathBuf::from("/gateway/.env"));
        assert_eq!(
            String::from_utf8(writes[1].1.clone()).unwrap(),
            "DEFAULT_MODEL=gpt-4o\nDISCORD_BOT_TOKEN=primary\n"
        );
        println!(
            "backup={} then authoritative write={}",
            writes[0].0.display(),
            writes[1].0.display()
        );
    }

    #[test]
    fn dry_run_returns_masked_diff_and_performs_zero_writes() {
        let env = fixture();
        write(
            &env,
            "/hermes/config.yaml",
            "model:\n  default: gpt-4o\n  api_key: api-secret\n",
        );
        write(&env, "/hermes/.env", "DISCORD_BOT_TOKEN=bot-secret\n");
        write(&env, "/gateway/.env", "DEFAULT_MODEL=old\n");
        let writes_before = env.write_calls().len();
        let renames_before = env.rename_calls().len();

        let result =
            import_config(&env, Path::new("/hermes"), Path::new("/gateway/.env"), true).unwrap();

        assert_eq!(env.write_calls().len(), writes_before);
        assert_eq!(env.rename_calls().len(), renames_before);
        assert!(result.diff.contains("DEFAULT_MODEL"));
        assert!(result.diff.contains("OPENAI_API_KEY"));
        assert!(!result.diff.contains("api-secret"));
        assert!(!result.diff.contains("bot-secret"));
        assert!(result.backup_path.is_none());
        println!("dry-run diff:\n{}", result.diff);
    }

    #[test]
    fn malformed_yaml_is_typed_and_never_writes_target() {
        let env = fixture();
        write(&env, "/hermes/config.yaml", "model: [unterminated\n");
        write(&env, "/hermes/.env", "DISCORD_BOT_TOKEN=primary\n");
        let writes_before = env.write_calls().len();

        let error = import_config(
            &env,
            Path::new("/hermes"),
            Path::new("/gateway/.env"),
            false,
        )
        .unwrap_err();

        assert!(matches!(error, OmonError::Config(_)));
        assert_eq!(env.write_calls().len(), writes_before);
        assert!(env.rename_calls().is_empty());
        println!("malformed yaml rejected before target write: {error}");
    }

    #[test]
    fn malformed_hermes_root_path_is_typed_and_never_writes_target() {
        let env = fixture();
        write(&env, "/hermes", "not a directory");
        let writes_before = env.write_calls().len();

        let error = import_config(
            &env,
            Path::new("/hermes"),
            Path::new("/gateway/.env"),
            false,
        )
        .unwrap_err();

        assert!(matches!(error, OmonError::Config(_)));
        assert!(error.to_string().contains("not a directory"));
        assert_eq!(env.write_calls().len(), writes_before);
    }

    #[test]
    fn root_scalar_values_win_over_profile_fallbacks() {
        let env = fixture();
        write(&env, "/hermes/config.yaml", "model:\n  default: gpt-4o\n");
        write(
            &env,
            "/hermes/.env",
            "DISCORD_BOT_TOKEN=primary\nAPPROVAL_MODE=never\n",
        );
        write(
            &env,
            "/hermes/profiles/alpha/.env",
            "DISCORD_BOT_TOKEN=second\nAPPROVAL_MODE=always\nDISCORD_ALLOWED_USERS=99\n",
        );

        let result = import_config(
            &env,
            Path::new("/hermes"),
            Path::new("/gateway/.env"),
            false,
        )
        .unwrap();

        assert_eq!(
            result.values.get("APPROVAL_MODE").map(String::as_str),
            Some("never")
        );
        assert_eq!(
            result
                .values
                .get("DISCORD_ALLOWED_USERS")
                .map(String::as_str),
            Some("99")
        );
    }
}
