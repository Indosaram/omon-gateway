use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use omon_gateway::{
    parse_profile_routes, AgentRunner, Database, InboundEvent, MultiplexerConfig, ProfileRoute,
    ProfileRouter, Result, SessionContext, SessionKey, SessionMultiplexer, SessionState,
};
use tokio::sync::Mutex;

struct ContextCapturingRunner {
    captured: Mutex<Vec<SessionContext>>,
}

#[async_trait]
impl AgentRunner for ContextCapturingRunner {
    async fn run(&self, session: &mut SessionContext, _event: InboundEvent) -> Result<()> {
        self.captured.lock().await.push(session.clone());
        Ok(())
    }
}

#[tokio::test]
async fn test_profile_router_hierarchical_precedence() {
    let json_config = r#"[
        {
            "guild": 100,
            "model": "guild-model",
            "system_prompt": "guild prompt"
        },
        {
            "guild": 100,
            "channel": 200,
            "model": "channel-model",
            "system_prompt": "channel prompt",
            "toolsets": ["terminal"]
        },
        {
            "guild": 100,
            "channel": 200,
            "thread": 300,
            "model": "thread-model",
            "system_prompt": "thread prompt",
            "toolsets": ["web"]
        }
    ]"#;

    let router = ProfileRouter::from_json(json_config);

    // 1. Thread level: thread_id 300 in channel 200 of guild 100 -> matches thread route
    let matched = router.match_route(Some(100), 200, Some(300)).unwrap();
    assert_eq!(matched.model.as_deref(), Some("thread-model"));
    assert_eq!(matched.system_prompt.as_deref(), Some("thread prompt"));
    assert_eq!(
        matched.enabled_toolsets.as_deref(),
        Some(&["web".to_string()][..])
    );

    // 2. Channel level: other thread 999 in channel 200 of guild 100 -> falls back to channel route
    let matched = router.match_route(Some(100), 200, Some(999)).unwrap();
    assert_eq!(matched.model.as_deref(), Some("channel-model"));
    assert_eq!(matched.system_prompt.as_deref(), Some("channel prompt"));
    assert_eq!(
        matched.enabled_toolsets.as_deref(),
        Some(&["terminal".to_string()][..])
    );

    // 3. Direct channel message: channel 200 in guild 100 (no thread) -> matches channel route
    let matched = router.match_route(Some(100), 200, None).unwrap();
    assert_eq!(matched.model.as_deref(), Some("channel-model"));

    // 4. Guild level: channel 555 in guild 100 (no thread) -> matches guild route
    let matched = router.match_route(Some(100), 555, None).unwrap();
    assert_eq!(matched.model.as_deref(), Some("guild-model"));
    assert_eq!(matched.system_prompt.as_deref(), Some("guild prompt"));

    // 5. Unrelated guild -> returns None
    let matched = router.match_route(Some(999), 555, None);
    assert!(matched.is_none());
}

#[tokio::test]
async fn test_multiplexer_routes_inbound_with_profile_defaults() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let runner = Arc::new(ContextCapturingRunner {
        captured: Mutex::new(Vec::new()),
    });

    let routes = vec![
        ProfileRoute {
            name: Some("dev-channel".into()),
            guild: Some(10),
            channel: Some(20),
            thread: None,
            enabled: true,
            model: Some("dev-llm-model".into()),
            system_prompt: Some("You are a dev assistant".into()),
            enabled_toolsets: Some(vec!["terminal".into(), "file".into()]),
        },
        ProfileRoute {
            name: Some("special-thread".into()),
            guild: Some(10),
            channel: Some(20),
            thread: Some(30),
            enabled: true,
            model: Some("thread-llm-model".into()),
            system_prompt: Some("You are a thread assistant".into()),
            enabled_toolsets: Some(vec!["web".into()]),
        },
    ];
    let profile_router = ProfileRouter::new(routes);

    let multiplexer = SessionMultiplexer::with_profile_router(
        db.pool().clone(),
        runner.clone(),
        None,
        MultiplexerConfig::default(),
        profile_router,
    );

    // Send event in channel 20 (no thread) -> should get dev-channel profile
    let session_key_channel =
        SessionKey::new("discord", Some("10"), "20", None::<String>, "user-channel");
    let event1 = InboundEvent::message(session_key_channel.clone(), "msg-1", "Hello channel");
    multiplexer.route(event1).await.unwrap();

    // Send event in thread 30 -> should get special-thread profile
    let session_key_thread =
        SessionKey::new("discord", Some("10"), "20", Some("30"), "user-thread");
    let event2 = InboundEvent::message(session_key_thread.clone(), "msg-2", "Hello thread");
    multiplexer.route(event2).await.unwrap();

    // Allow background turns to run
    tokio::time::sleep(Duration::from_millis(50)).await;

    let captured = runner.captured.lock().await.clone();
    assert_eq!(captured.len(), 2);

    let cap_channel = captured
        .iter()
        .find(|c| c.key == session_key_channel)
        .unwrap();
    assert_eq!(
        cap_channel.state.active_model.as_deref(),
        Some("dev-llm-model")
    );
    assert_eq!(
        cap_channel.state.system_prompt.as_deref(),
        Some("You are a dev assistant")
    );
    assert_eq!(
        cap_channel.state.enabled_toolsets.as_deref(),
        Some(&["terminal".to_string(), "file".to_string()][..])
    );

    let cap_thread = captured
        .iter()
        .find(|c| c.key == session_key_thread)
        .unwrap();
    assert_eq!(
        cap_thread.state.active_model.as_deref(),
        Some("thread-llm-model")
    );
    assert_eq!(
        cap_thread.state.system_prompt.as_deref(),
        Some("You are a thread assistant")
    );
    assert_eq!(
        cap_thread.state.enabled_toolsets.as_deref(),
        Some(&["web".to_string()][..])
    );
}

#[tokio::test]
async fn test_multiplexer_does_not_clobber_existing_explicit_model() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let runner = Arc::new(ContextCapturingRunner {
        captured: Mutex::new(Vec::new()),
    });

    let routes = vec![ProfileRoute {
        name: Some("default-guild".into()),
        guild: Some(50),
        channel: None,
        thread: None,
        enabled: true,
        model: Some("profile-default-model".into()),
        system_prompt: Some("Profile system prompt".into()),
        enabled_toolsets: None,
    }];
    let profile_router = ProfileRouter::new(routes);

    let multiplexer = SessionMultiplexer::with_profile_router(
        db.pool().clone(),
        runner.clone(),
        None,
        MultiplexerConfig::default(),
        profile_router,
    );

    // Pre-populate session in DB with an explicit model (e.g. set by /model)
    let session_key = SessionKey::new("discord", Some("50"), "60", None::<String>, "user-explicit");
    let explicit_state = SessionState {
        active_model: Some("explicit-user-chosen-model".into()),
        ..Default::default()
    };
    let explicit_json = serde_json::to_string(&explicit_state).unwrap();

    sqlx::query(
        "INSERT INTO sessions (session_key, platform, guild_id, channel_id, user_id, state_json) VALUES (?, 'discord', '50', '60', 'user-explicit', ?)"
    )
    .bind(session_key.storage_key())
    .bind(&explicit_json)
    .execute(db.pool())
    .await
    .unwrap();

    let event = InboundEvent::message(
        session_key.clone(),
        "msg-explicit",
        "Hello with explicit model",
    );
    multiplexer.route(event).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let captured = runner.captured.lock().await.clone();
    assert_eq!(captured.len(), 1);
    let session = &captured[0];
    // Explicit model is preserved, NOT clobbered by profile default
    assert_eq!(
        session.state.active_model.as_deref(),
        Some("explicit-user-chosen-model")
    );
    // Unset system prompt gets populated from profile
    assert_eq!(
        session.state.system_prompt.as_deref(),
        Some("Profile system prompt")
    );
}

#[test]
fn test_json_parsing_edge_cases() {
    assert_eq!(parse_profile_routes("").len(), 0);
    assert_eq!(parse_profile_routes("   \n\t  ").len(), 0);
    assert_eq!(parse_profile_routes("null").len(), 0);
    assert_eq!(parse_profile_routes("invalid json").len(), 0);
    assert_eq!(parse_profile_routes("{\"not_an_array\": 1}").len(), 0);

    let json = r#"[{"guild":123,"channel":456,"thread":null,"model":"gpt-x","system_prompt":"...","toolsets":["terminal","web"]}]"#;
    let routes = parse_profile_routes(json);
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].guild, Some(123));
    assert_eq!(routes[0].channel, Some(456));
    assert_eq!(routes[0].thread, None);
    assert_eq!(routes[0].model.as_deref(), Some("gpt-x"));
    assert_eq!(routes[0].system_prompt.as_deref(), Some("..."));
    assert_eq!(
        routes[0].enabled_toolsets.as_deref(),
        Some(&["terminal".to_string(), "web".to_string()][..])
    );
}
