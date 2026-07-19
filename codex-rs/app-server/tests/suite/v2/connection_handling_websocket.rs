use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use app_test_support::DISABLE_PLUGIN_STARTUP_TASKS_ARG;
use app_test_support::create_final_assistant_message_sse_response;
use app_test_support::create_mock_responses_server_sequence_unchecked;
use app_test_support::create_shell_command_sse_response;
use app_test_support::to_response;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use codex_app_server_protocol::ClientInfo;
use codex_app_server_protocol::ConfigWarningNotification;
use codex_app_server_protocol::DynamicToolCallOutputContentItem;
use codex_app_server_protocol::DynamicToolCallResponse;
use codex_app_server_protocol::InitializeCapabilities;
use codex_app_server_protocol::InitializeParams;
use codex_app_server_protocol::ItemStartedNotification;
use codex_app_server_protocol::JSONRPCError;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::JSONRPCNotification;
use codex_app_server_protocol::JSONRPCRequest;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::SandboxPolicy;
use codex_app_server_protocol::ServerConnectionListResponse;
use codex_app_server_protocol::ThreadForkResponse;
use codex_app_server_protocol::ThreadListResponse;
use codex_app_server_protocol::ThreadLoadedListParams;
use codex_app_server_protocol::ThreadLoadedListResponse;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::TurnStartedNotification;
use codex_app_server_protocol::TurnSteerParams;
use codex_app_server_protocol::TurnSteerResponse;
use codex_app_server_protocol::UserInput;
use codex_core::config::set_project_trust_level;
use codex_login::default_client::CODEX_INTERNAL_ORIGINATOR_OVERRIDE_ENV_VAR;
use codex_protocol::config_types::TrustLevel;
use core_test_support::responses;
use futures::SinkExt;
use futures::StreamExt;
use hmac::Hmac;
use hmac::Mac;
use reqwest::StatusCode;
use serde_json::json;
use sha2::Sha256;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Stdio;
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::process::Child;
use tokio::process::Command;
use tokio::time::Duration;
use tokio::time::Instant;
use tokio::time::sleep;
use tokio::time::timeout;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::header::ORIGIN;

// macOS and Windows CI can spend tens of seconds starting the app-server test
// binary under Bazel before it accepts JSON-RPC or reports its websocket bind
// address.
#[cfg(any(target_os = "macos", windows))]
pub(super) const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(60);
#[cfg(not(any(target_os = "macos", windows)))]
pub(super) const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) type WsClient = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
type HmacSha256 = Hmac<Sha256>;

#[tokio::test]
async fn websocket_transport_routes_per_connection_handshake_and_responses() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;

    let mut ws1 = connect_websocket(bind_addr).await?;
    let mut ws2 = connect_websocket(bind_addr).await?;

    send_initialize_request(&mut ws1, /*id*/ 1, "ws_client_one").await?;
    let first_init = read_response_for_id(&mut ws1, /*id*/ 1).await?;
    assert_eq!(first_init.id, RequestId::Integer(1));

    // Initialize responses are request-scoped and must not leak to other
    // connections.
    assert_no_message(&mut ws2, Duration::from_millis(250)).await?;

    send_config_read_request(&mut ws2, /*id*/ 2).await?;
    let not_initialized = read_error_for_id(&mut ws2, /*id*/ 2).await?;
    assert_eq!(not_initialized.error.message, "Not initialized");

    send_initialize_request(&mut ws2, /*id*/ 3, "ws_client_two").await?;
    let second_init = read_response_for_id(&mut ws2, /*id*/ 3).await?;
    assert_eq!(second_init.id, RequestId::Integer(3));

    // Same request-id on different connections must route independently.
    send_config_read_request(&mut ws1, /*id*/ 77).await?;
    send_config_read_request(&mut ws2, /*id*/ 77).await?;
    let ws1_config = read_response_for_id(&mut ws1, /*id*/ 77).await?;
    let ws2_config = read_response_for_id(&mut ws2, /*id*/ 77).await?;

    assert_eq!(ws1_config.id, RequestId::Integer(77));
    assert_eq!(ws2_config.id, RequestId::Integer(77));
    assert!(ws1_config.result.get("config").is_some());
    assert!(ws2_config.result.get("config").is_some());

    process
        .kill()
        .await
        .context("failed to stop websocket app-server process")?;
    Ok(())
}

#[tokio::test]
async fn websocket_shared_server_preserves_connection_originators_per_thread_and_turn() -> Result<()>
{
    let server = create_mock_responses_server_sequence_unchecked(vec![
        create_final_assistant_message_sse_response("desktop done")?,
        create_final_assistant_message_sse_response("manager done")?,
    ])
    .await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut desktop = connect_websocket(bind_addr).await?;
    let mut manager = connect_websocket(bind_addr).await?;

    send_initialize_request(&mut desktop, /*id*/ 1, "Codex Desktop").await?;
    read_response_for_id(&mut desktop, /*id*/ 1).await?;
    send_initialize_request(&mut manager, /*id*/ 2, "belie_manager").await?;
    read_response_for_id(&mut manager, /*id*/ 2).await?;

    let desktop_thread =
        start_thread_with_service_name(&mut desktop, /*id*/ 3, Some("codex_work_desktop")).await?;
    let manager_thread = start_thread(&mut manager, /*id*/ 4).await?;

    let desktop_origins = start_turn_and_read_origins(
        &mut desktop,
        /*id*/ 5,
        &desktop_thread,
        "desktop input",
    )
    .await?;
    let manager_origins = start_turn_and_read_origins(
        &mut manager,
        /*id*/ 6,
        &manager_thread,
        "manager input",
    )
    .await?;

    assert_eq!(
        desktop_origins,
        ("Codex Desktop".to_string(), "Codex Desktop".to_string())
    );
    assert_eq!(
        manager_origins,
        ("belie_manager".to_string(), "belie_manager".to_string())
    );

    let requests = server
        .received_requests()
        .await
        .context("failed to fetch model requests")?;
    let origins = requests
        .iter()
        .filter(|request| request.url.path().ends_with("/responses"))
        .map(|request| {
            request
                .headers
                .get("originator")
                .context("model request should include originator")?
                .to_str()
                .context("originator should be valid UTF-8")
                .map(str::to_string)
        })
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        origins,
        vec![
            "codex_work_desktop".to_string(),
            "belie_manager".to_string()
        ]
    );

    process
        .kill()
        .await
        .context("failed to stop websocket app-server process")?;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn websocket_turn_notifications_keep_winner_client_after_losing_start_and_steer_requests()
-> Result<()> {
    let workdir = TempDir::new()?;
    let server = create_mock_responses_server_sequence_unchecked(vec![
        create_shell_command_sse_response(
            vec!["sleep".to_string(), "1".to_string()],
            Some(workdir.path()),
            Some(10_000),
            "call_origin_race_sleep",
        )?,
        create_final_assistant_message_sse_response("winner done")?,
    ])
    .await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut manager = connect_websocket(bind_addr).await?;
    let mut desktop = connect_websocket(bind_addr).await?;
    send_initialize_request_with_experimental_api(&mut manager, 1, "belie_manager").await?;
    read_response_for_id(&mut manager, 1)
        .await
        .context("manager initialize response")?;
    send_initialize_request_with_experimental_api(&mut desktop, 2, "Codex Desktop").await?;
    read_response_for_id(&mut desktop, 2)
        .await
        .context("desktop initialize response")?;
    let thread_id = start_thread(&mut manager, 3)
        .await
        .context("manager thread/start response")?;

    send_request(
        &mut manager,
        "turn/start",
        4,
        Some(serde_json::to_value(TurnStartParams {
            thread_id: thread_id.clone(),
            input: vec![UserInput::Text {
                text: "run the winner".to_string(),
                text_elements: Vec::new(),
            }],
            cwd: Some(workdir.path().to_path_buf()),
            ..Default::default()
        })?),
    )
    .await?;
    let (response, started_notification) =
        read_response_and_notification_for_method(&mut manager, 4, "turn/started")
            .await
            .context("manager winning turn/start response and notification")?;
    let started: TurnStartResponse = to_response(response)?;
    let started_params: TurnStartedNotification =
        serde_json::from_value(started_notification.params.context("turn/started params")?)?;
    assert_eq!(started_params.client_name.as_deref(), Some("belie_manager"));

    // A losing second start and an accepted Desktop steer both used to overwrite
    // the thread-global client name while the Manager turn was still emitting.
    send_request(
        &mut desktop,
        "turn/start",
        5,
        Some(serde_json::to_value(TurnStartParams {
            thread_id: thread_id.clone(),
            input: vec![UserInput::Text {
                text: "must lose".to_string(),
                text_elements: Vec::new(),
            }],
            sandbox_policy: Some(SandboxPolicy::DangerFullAccess),
            permissions: Some(":workspace".to_string()),
            ..Default::default()
        })?),
    )
    .await?;
    let _ = read_error_for_id(&mut desktop, 5)
        .await
        .context("desktop losing turn/start error")?;
    send_request(
        &mut desktop,
        "turn/steer",
        6,
        Some(serde_json::to_value(TurnSteerParams {
            thread_id: thread_id.clone(),
            expected_turn_id: started.turn.id.clone(),
            input: vec![UserInput::Text {
                text: "desktop accepted steer".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })?),
    )
    .await?;
    let _: TurnSteerResponse = to_response(
        read_response_for_id(&mut desktop, 6)
            .await
            .context("desktop accepted turn/steer response")?,
    )?;
    sleep(Duration::from_secs(3)).await;
    process.kill().await.context("kill original app-server")?;

    let (mut restarted_process, restarted_bind_addr) =
        spawn_websocket_server(codex_home.path()).await?;
    let mut restarted_reader = connect_websocket(restarted_bind_addr).await?;
    send_initialize_request_with_experimental_api(&mut restarted_reader, 9, "belie_manager")
        .await?;
    read_response_for_id(&mut restarted_reader, 9)
        .await
        .context("restarted initialize response")?;
    send_request(
        &mut restarted_reader,
        "thread/resume",
        10,
        Some(json!({"threadId": thread_id})),
    )
    .await?;
    let ThreadResumeResponse {
        thread: resumed_thread,
        ..
    } = to_response(
        read_response_for_id(&mut restarted_reader, 10)
            .await
            .context("restarted thread/resume response")?,
    )?;
    let resumed_clients = resumed_thread
        .turns
        .iter()
        .flat_map(|turn| turn.items.iter())
        .filter_map(|item| match item {
            codex_app_server_protocol::ThreadItem::UserMessage { client_name, .. } => {
                client_name.as_deref()
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(resumed_clients, vec!["belie_manager", "Codex Desktop"]);

    send_request(
        &mut restarted_reader,
        "thread/read",
        11,
        Some(json!({"threadId": thread_id, "includeTurns": true})),
    )
    .await?;
    let ThreadReadResponse {
        thread: restarted_thread,
    } = to_response(
        read_response_for_id(&mut restarted_reader, 11)
            .await
            .context("restarted thread/read response")?,
    )?;
    let restarted_clients = restarted_thread
        .turns
        .iter()
        .flat_map(|turn| turn.items.iter())
        .filter_map(|item| match item {
            codex_app_server_protocol::ThreadItem::UserMessage { client_name, .. } => {
                client_name.as_deref()
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(restarted_clients, vec!["belie_manager", "Codex Desktop"]);
    restarted_process.kill().await?;
    Ok(())
}

#[tokio::test]
async fn websocket_thread_originator_survives_restart_and_all_thread_reads() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(vec![
        create_final_assistant_message_sse_response("persisted")?,
    ])
    .await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut manager = connect_websocket(bind_addr).await?;
    send_initialize_request(&mut manager, /*id*/ 1, "belie_manager").await?;
    read_response_for_id(&mut manager, /*id*/ 1).await?;

    send_request(
        &mut manager,
        "thread/start",
        /*id*/ 2,
        Some(serde_json::to_value(ThreadStartParams {
            model: Some("mock-model".to_string()),
            ..Default::default()
        })?),
    )
    .await?;
    let start_response = read_response_for_id(&mut manager, /*id*/ 2).await?;
    let ThreadStartResponse { thread, .. } = to_response(start_response)?;
    assert_eq!(thread.originator.as_deref(), Some("belie_manager"));

    let origins = start_turn_and_read_origins(
        &mut manager,
        /*id*/ 3,
        thread.id.as_str(),
        "persist this thread",
    )
    .await?;
    assert_eq!(
        origins,
        ("belie_manager".to_string(), "belie_manager".to_string())
    );
    process
        .kill()
        .await
        .context("failed to stop first websocket app-server process")?;

    let (mut restarted_process, restarted_bind_addr) =
        spawn_websocket_server(codex_home.path()).await?;
    let mut reconnected_manager = connect_websocket(restarted_bind_addr).await?;
    send_initialize_request(&mut reconnected_manager, /*id*/ 4, "belie_manager").await?;
    read_response_for_id(&mut reconnected_manager, /*id*/ 4).await?;

    send_request(
        &mut reconnected_manager,
        "thread/resume",
        /*id*/ 5,
        Some(json!({"threadId": thread.id})),
    )
    .await?;
    let resume_response = read_response_for_id(&mut reconnected_manager, /*id*/ 5).await?;
    let ThreadResumeResponse {
        thread: resumed_thread,
        ..
    } = to_response(resume_response)?;
    assert_eq!(resumed_thread.originator.as_deref(), Some("belie_manager"));

    send_request(
        &mut reconnected_manager,
        "thread/read",
        /*id*/ 6,
        Some(json!({"threadId": thread.id, "includeTurns": false})),
    )
    .await?;
    let read_response = read_response_for_id(&mut reconnected_manager, /*id*/ 6).await?;
    let ThreadReadResponse {
        thread: read_thread,
    } = to_response(read_response)?;
    assert_eq!(read_thread.originator.as_deref(), Some("belie_manager"));

    send_request(
        &mut reconnected_manager,
        "thread/list",
        /*id*/ 7,
        Some(json!({"limit": 100})),
    )
    .await?;
    let list_response = read_response_for_id(&mut reconnected_manager, /*id*/ 7).await?;
    let ThreadListResponse { data, .. } = to_response(list_response)?;
    let listed_thread = data
        .iter()
        .find(|candidate| candidate.id == thread.id)
        .context("thread/list should return the persisted thread")?;
    assert_eq!(listed_thread.originator.as_deref(), Some("belie_manager"));

    restarted_process
        .kill()
        .await
        .context("failed to stop restarted websocket app-server process")?;
    Ok(())
}

#[tokio::test]
async fn websocket_thread_start_idempotency_survives_restart_archive_and_origin_isolation()
-> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    let workspace = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    set_project_trust_level(codex_home.path(), workspace.path(), TrustLevel::Trusted)?;

    let key = "belie-manager:loop-115:run-7";
    let start_params = json!({
        "model": "mock-model",
        "cwd": workspace.path(),
        "threadSource": "appServer",
        "idempotencyKey": key
    });

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut manager = connect_websocket(bind_addr).await?;
    send_initialize_request_with_experimental_api(&mut manager, /*id*/ 1, "belie_manager").await?;
    read_response_for_id(&mut manager, /*id*/ 1).await?;
    send_request(
        &mut manager,
        "thread/start",
        /*id*/ 2,
        Some(start_params.clone()),
    )
    .await?;
    let first: ThreadStartResponse = to_response(read_response_for_id(&mut manager, 2).await?)?;
    assert_eq!(first.thread.originator.as_deref(), Some("belie_manager"));

    // Simulate the client losing the response association and the daemon itself restarting.
    process.kill().await?;
    let (mut restarted_process, restarted_bind_addr) =
        spawn_websocket_server(codex_home.path()).await?;
    let mut reconnected_manager = connect_websocket(restarted_bind_addr).await?;
    send_initialize_request_with_experimental_api(
        &mut reconnected_manager,
        /*id*/ 3,
        "belie_manager",
    )
    .await?;
    read_response_for_id(&mut reconnected_manager, /*id*/ 3).await?;
    send_request(
        &mut reconnected_manager,
        "thread/start",
        /*id*/ 4,
        Some(start_params.clone()),
    )
    .await?;
    // A resumed response is intentionally wire-compatible with thread/start's response shape.
    let retried: ThreadStartResponse =
        to_response(read_response_for_id(&mut reconnected_manager, 4).await?)?;
    assert_eq!(retried.thread.id, first.thread.id);
    assert_eq!(retried.thread.originator.as_deref(), Some("belie_manager"));

    // Operation kind is payload, not namespace: the same key cannot be reused
    // for a fork even though the original binding came from thread/start.
    send_request(
        &mut reconnected_manager,
        "thread/fork",
        /*id*/ 40,
        Some(json!({
            "threadId": first.thread.id,
            "excludeTurns": true,
            "idempotencyKey": key
        })),
    )
    .await?;
    let cross_operation_conflict = read_error_for_id(&mut reconnected_manager, /*id*/ 40).await?;
    assert!(
        cross_operation_conflict
            .error
            .message
            .contains("already bound")
    );

    // Reconciliation may archive the just-created task before the manager can bind it. The key
    // remains global: retry unarchives and resumes the same durable thread instead of duplicating.
    send_request(
        &mut reconnected_manager,
        "thread/archive",
        /*id*/ 5,
        Some(json!({"threadId": first.thread.id})),
    )
    .await?;
    read_response_for_id(&mut reconnected_manager, /*id*/ 5).await?;
    send_request(
        &mut reconnected_manager,
        "thread/start",
        /*id*/ 6,
        Some(start_params.clone()),
    )
    .await?;
    let unarchived: ThreadStartResponse =
        to_response(read_response_for_id(&mut reconnected_manager, 6).await?)?;
    assert_eq!(unarchived.thread.id, first.thread.id);

    // A key cannot be silently rebound to a different immutable start identity.
    let mut conflicting = start_params.clone();
    conflicting["cwd"] = json!(codex_home.path());
    send_request(
        &mut reconnected_manager,
        "thread/start",
        /*id*/ 7,
        Some(conflicting),
    )
    .await?;
    let conflict = read_error_for_id(&mut reconnected_manager, /*id*/ 7).await?;
    assert!(conflict.error.message.contains("already bound"));

    // Desktop has a different effective originator namespace and cannot claim the manager task.
    let mut desktop = connect_websocket(restarted_bind_addr).await?;
    send_initialize_request_with_experimental_api(&mut desktop, /*id*/ 8, "Codex Desktop").await?;
    read_response_for_id(&mut desktop, /*id*/ 8).await?;
    let desktop_params = json!({
        "model": "mock-model",
        "cwd": workspace.path(),
        "serviceName": "codex_work_desktop",
        "threadSource": "appServer",
        "idempotencyKey": key
    });
    send_request(
        &mut desktop,
        "thread/start",
        /*id*/ 9,
        Some(desktop_params),
    )
    .await?;
    let desktop_thread: ThreadStartResponse =
        to_response(read_response_for_id(&mut desktop, 9).await?)?;
    assert_ne!(desktop_thread.thread.id, first.thread.id);
    assert_eq!(
        desktop_thread.thread.originator.as_deref(),
        Some("codex_work_desktop")
    );

    restarted_process.kill().await?;
    Ok(())
}

#[tokio::test]
async fn websocket_thread_fork_idempotency_is_concurrent_durable_and_owner_scoped() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(vec![
        create_final_assistant_message_sse_response("source ready")?,
    ])
    .await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut manager_one = connect_websocket(bind_addr).await?;
    let mut manager_two = connect_websocket(bind_addr).await?;
    send_initialize_request_with_experimental_api(&mut manager_one, 1, "belie_manager").await?;
    read_response_for_id(&mut manager_one, 1).await?;
    send_initialize_request_with_experimental_api(&mut manager_two, 2, "belie_manager").await?;
    read_response_for_id(&mut manager_two, 2).await?;

    let source_id = start_thread(&mut manager_one, /*id*/ 3).await?;
    start_turn_and_read_origins(
        &mut manager_one,
        /*id*/ 4,
        &source_id,
        "materialize fork source",
    )
    .await?;
    let key = "belie-manager:action-115:successor-fork";
    let fork_params = json!({
        "threadId": source_id,
        "threadSource": "appServer",
        "excludeTurns": true,
        "idempotencyKey": key
    });

    // Two Manager connections racing the same durable action must converge on one fork.
    send_request(
        &mut manager_one,
        "thread/fork",
        /*id*/ 5,
        Some(fork_params.clone()),
    )
    .await?;
    send_request(
        &mut manager_two,
        "thread/fork",
        /*id*/ 6,
        Some(fork_params.clone()),
    )
    .await?;
    let fork_one: ThreadForkResponse =
        to_response(read_response_for_id(&mut manager_one, 5).await?)?;
    let fork_two: ThreadForkResponse =
        to_response(read_response_for_id(&mut manager_two, 6).await?)?;
    assert_eq!(fork_one.thread.id, fork_two.thread.id);
    assert_eq!(fork_one.thread.originator.as_deref(), Some("belie_manager"));

    process.kill().await?;
    let (mut restarted_process, restarted_bind_addr) =
        spawn_websocket_server(codex_home.path()).await?;
    let mut manager = connect_websocket(restarted_bind_addr).await?;
    send_initialize_request_with_experimental_api(&mut manager, 7, "belie_manager").await?;
    read_response_for_id(&mut manager, 7).await?;
    send_request(
        &mut manager,
        "thread/fork",
        /*id*/ 8,
        Some(fork_params.clone()),
    )
    .await?;
    let after_restart: ThreadForkResponse =
        to_response(read_response_for_id(&mut manager, 8).await?)?;
    assert_eq!(after_restart.thread.id, fork_one.thread.id);

    send_request(
        &mut manager,
        "thread/archive",
        /*id*/ 9,
        Some(json!({"threadId": fork_one.thread.id})),
    )
    .await?;
    read_response_for_id(&mut manager, 9).await?;
    send_request(
        &mut manager,
        "thread/fork",
        /*id*/ 10,
        Some(fork_params.clone()),
    )
    .await?;
    let after_archive: ThreadForkResponse =
        to_response(read_response_for_id(&mut manager, 10).await?)?;
    assert_eq!(after_archive.thread.id, fork_one.thread.id);

    let mut conflicting = fork_params.clone();
    conflicting["lastTurnId"] = json!("different-boundary");
    send_request(
        &mut manager,
        "thread/fork",
        /*id*/ 11,
        Some(conflicting),
    )
    .await?;
    let conflict = read_error_for_id(&mut manager, 11).await?;
    assert!(conflict.error.message.contains("different fork source"));

    let mut desktop = connect_websocket(restarted_bind_addr).await?;
    send_initialize_request_with_experimental_api(&mut desktop, 12, "Codex Desktop").await?;
    read_response_for_id(&mut desktop, 12).await?;
    send_request(
        &mut desktop,
        "thread/fork",
        /*id*/ 13,
        Some(fork_params.clone()),
    )
    .await?;
    let ownership_error = read_error_for_id(&mut desktop, 13).await?;
    assert!(
        ownership_error
            .error
            .message
            .contains("cannot idempotently fork")
    );

    send_request(
        &mut manager,
        "thread/list",
        /*id*/ 14,
        Some(json!({"limit": 100})),
    )
    .await?;
    let listed: ThreadListResponse = to_response(read_response_for_id(&mut manager, 14).await?)?;
    let forks = listed
        .data
        .iter()
        .filter(|thread| thread.id == fork_one.thread.id)
        .collect::<Vec<_>>();
    assert_eq!(forks.len(), 1);
    assert_eq!(forks[0].id, fork_one.thread.id);

    restarted_process.kill().await?;
    Ok(())
}

#[tokio::test]
async fn websocket_turn_start_idempotency_is_concurrent_and_survives_restart() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(vec![
        create_final_assistant_message_sse_response("exactly once")?,
    ])
    .await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut manager_one = connect_websocket(bind_addr).await?;
    let mut manager_two = connect_websocket(bind_addr).await?;
    send_initialize_request_with_experimental_api(&mut manager_one, 1, "belie_manager").await?;
    read_response_for_id(&mut manager_one, 1).await?;
    send_initialize_request_with_experimental_api(&mut manager_two, 2, "belie_manager").await?;
    read_response_for_id(&mut manager_two, 2).await?;

    let thread_id = start_thread(&mut manager_one, 3).await?;

    let key = "belie-manager:run-115:turn-9";
    let turn_params = json!({
        "threadId": thread_id,
        "input": [{"type": "text", "text": "execute this action", "textElements": []}],
        "idempotencyKey": key
    });

    // Two live connections for the same originator racing the same action converge on one turn.
    send_request(&mut manager_one, "turn/start", 5, Some(turn_params.clone())).await?;
    send_request(&mut manager_two, "turn/start", 6, Some(turn_params.clone())).await?;
    let first: TurnStartResponse = to_response(read_response_for_id(&mut manager_one, 5).await?)?;
    let concurrent: TurnStartResponse =
        to_response(read_response_for_id(&mut manager_two, 6).await?)?;
    assert_eq!(concurrent.turn.id, first.turn.id);

    // Wait for the accepted action to become terminal before simulating daemon loss.
    timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            let notification = read_jsonrpc_message(&mut manager_one).await?;
            if let JSONRPCMessage::Notification(notification) = notification
                && notification.method == "turn/completed"
                && notification
                    .params
                    .as_ref()
                    .and_then(|params| params.get("turn"))
                    .and_then(|turn| turn.get("id"))
                    .and_then(serde_json::Value::as_str)
                    == Some(first.turn.id.as_str())
            {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;

    process.kill().await?;
    let (mut restarted_process, restarted_bind_addr) =
        spawn_websocket_server(codex_home.path()).await?;
    let mut manager = connect_websocket(restarted_bind_addr).await?;
    send_initialize_request_with_experimental_api(&mut manager, 7, "belie_manager").await?;
    read_response_for_id(&mut manager, 7).await?;
    send_request(&mut manager, "turn/start", 8, Some(turn_params.clone())).await?;
    let retried: TurnStartResponse = to_response(read_response_for_id(&mut manager, 8).await?)?;
    assert_eq!(retried.turn.id, first.turn.id);

    let mut conflicting = turn_params;
    conflicting["input"][0]["text"] = json!("different action");
    send_request(&mut manager, "turn/start", 9, Some(conflicting)).await?;
    let conflict = read_error_for_id(&mut manager, 9).await?;
    assert!(conflict.error.message.contains("already bound"));

    let requests = server
        .received_requests()
        .await
        .context("failed to fetch idempotent turn model requests")?;
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.url.path().ends_with("/responses"))
            .count(),
        1,
        "concurrent and post-restart retries must not invoke the model twice"
    );

    restarted_process.kill().await?;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn websocket_turn_steer_idempotency_is_concurrent_and_survives_restart() -> Result<()> {
    let codex_home = TempDir::new()?;
    let workdir = TempDir::new()?;
    let server = create_mock_responses_server_sequence_unchecked(vec![
        create_shell_command_sse_response(
            vec!["sleep".to_string(), "2".to_string()],
            Some(workdir.path()),
            Some(10_000),
            "call_idempotent_steer_sleep",
        )?,
        create_final_assistant_message_sse_response("steered once")?,
    ])
    .await;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut manager_one = connect_websocket(bind_addr).await?;
    let mut manager_two = connect_websocket(bind_addr).await?;
    send_initialize_request_with_experimental_api(&mut manager_one, 1, "belie_manager").await?;
    read_response_for_id(&mut manager_one, 1).await?;
    send_initialize_request_with_experimental_api(&mut manager_two, 2, "belie_manager").await?;
    read_response_for_id(&mut manager_two, 2).await?;

    let thread_id = start_thread(&mut manager_one, 3).await?;
    send_request(
        &mut manager_one,
        "turn/start",
        4,
        Some(serde_json::to_value(TurnStartParams {
            idempotency_key: None,
            thread_id: thread_id.clone(),
            input: vec![UserInput::Text {
                text: "run sleep".to_string(),
                text_elements: Vec::new(),
            }],
            cwd: Some(workdir.path().to_path_buf()),
            ..Default::default()
        })?),
    )
    .await?;
    let (turn_response, _) =
        read_response_and_notification_for_method(&mut manager_one, 4, "turn/started").await?;
    let started: TurnStartResponse = to_response(turn_response)?;

    let key = "belie-manager:run-115:steer-3";
    let steer_params = json!({
        "threadId": thread_id,
        "expectedTurnId": started.turn.id,
        "clientUserMessageId": "idempotent-steer-message",
        "input": [{"type": "text", "text": "steer exactly once", "textElements": []}],
        "idempotencyKey": key
    });
    send_request(
        &mut manager_one,
        "turn/steer",
        5,
        Some(steer_params.clone()),
    )
    .await?;
    send_request(
        &mut manager_two,
        "turn/steer",
        6,
        Some(steer_params.clone()),
    )
    .await?;
    let first: TurnSteerResponse = to_response(read_response_for_id(&mut manager_one, 5).await?)?;
    let concurrent: TurnSteerResponse =
        to_response(read_response_for_id(&mut manager_two, 6).await?)?;
    assert_eq!(first.turn_id, started.turn.id);
    assert_eq!(concurrent.turn_id, first.turn_id);

    timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            if let JSONRPCMessage::Notification(notification) =
                read_jsonrpc_message(&mut manager_one).await?
                && notification.method == "turn/completed"
            {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;

    process.kill().await?;
    let (mut restarted_process, restarted_bind_addr) =
        spawn_websocket_server(codex_home.path()).await?;
    let mut manager = connect_websocket(restarted_bind_addr).await?;
    send_initialize_request_with_experimental_api(&mut manager, 7, "belie_manager").await?;
    read_response_for_id(&mut manager, 7).await?;
    send_request(&mut manager, "turn/steer", 8, Some(steer_params.clone())).await?;
    let retried: TurnSteerResponse = to_response(read_response_for_id(&mut manager, 8).await?)?;
    assert_eq!(retried.turn_id, first.turn_id);

    let mut conflicting = steer_params;
    conflicting["input"][0]["text"] = json!("changed steering input");
    send_request(&mut manager, "turn/steer", 9, Some(conflicting)).await?;
    let conflict = read_error_for_id(&mut manager, 9).await?;
    assert!(conflict.error.message.contains("already bound"));

    let requests = server
        .received_requests()
        .await
        .context("failed to fetch idempotent steer model requests")?;
    let responses_requests = requests
        .iter()
        .filter(|request| request.url.path().ends_with("/responses"))
        .collect::<Vec<_>>();
    assert_eq!(responses_requests.len(), 2);
    let second_request_body: serde_json::Value = responses_requests[1].body_json()?;
    assert_eq!(
        second_request_body
            .to_string()
            .matches("steer exactly once")
            .count(),
        1,
        "concurrent and post-restart retries must enqueue steering input once"
    );

    restarted_process.kill().await?;
    Ok(())
}

#[tokio::test]
async fn websocket_connection_list_reports_initialized_live_client_identity() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut manager = connect_websocket(bind_addr).await?;
    let mut desktop = connect_websocket(bind_addr).await?;
    send_initialize_request_with_experimental_api(&mut manager, 1, "belie_manager").await?;
    read_response_for_id(&mut manager, 1).await?;
    send_initialize_request_with_experimental_api(&mut desktop, 2, "Codex Desktop").await?;
    read_response_for_id(&mut desktop, 2).await?;

    send_request(&mut manager, "server/connection/list", 3, Some(json!({}))).await?;
    let listed: ServerConnectionListResponse =
        to_response(read_response_for_id(&mut manager, 3).await?)?;
    assert_eq!(listed.data.len(), 2);
    assert_eq!(
        listed
            .data
            .iter()
            .filter_map(|connection| connection.client_name.as_deref())
            .collect::<Vec<_>>(),
        vec!["Codex Desktop", "belie_manager"]
    );
    assert_eq!(
        listed
            .data
            .iter()
            .filter(|connection| connection.is_current)
            .filter_map(|connection| connection.client_name.as_deref())
            .collect::<Vec<_>>(),
        vec!["belie_manager"]
    );

    desktop.close(None).await?;
    send_request(&mut manager, "server/connection/list", 4, Some(json!({}))).await?;
    let after_disconnect: ServerConnectionListResponse =
        to_response(read_response_for_id(&mut manager, 4).await?)?;
    assert_eq!(after_disconnect.data.len(), 1);
    assert_eq!(
        after_disconnect.data[0].client_name.as_deref(),
        Some("belie_manager")
    );

    process.kill().await?;
    Ok(())
}

#[tokio::test]
async fn websocket_shared_server_routes_dynamic_tools_only_to_the_registering_client() -> Result<()>
{
    let call_id = "manager-callback-1";
    let tool_name = "report_progress";
    let tool_arguments = json!({"message": "working"});
    let server = create_mock_responses_server_sequence_unchecked(vec![
        responses::sse(vec![
            responses::ev_response_created("resp-1"),
            responses::ev_function_call(
                call_id,
                tool_name,
                &serde_json::to_string(&tool_arguments)?,
            ),
            responses::ev_completed("resp-1"),
        ]),
        create_final_assistant_message_sse_response("done")?,
    ])
    .await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut manager = connect_websocket(bind_addr).await?;
    let mut desktop = connect_websocket(bind_addr).await?;
    send_initialize_request_with_experimental_api(&mut manager, /*id*/ 1, "belie_manager").await?;
    read_response_for_id(&mut manager, /*id*/ 1).await?;
    send_initialize_request_with_experimental_api(&mut desktop, /*id*/ 2, "Codex Desktop").await?;
    read_response_for_id(&mut desktop, /*id*/ 2).await?;

    send_request(
        &mut manager,
        "thread/start",
        /*id*/ 3,
        Some(json!({
            "model": "mock-model",
            "dynamicTools": [{
                "name": tool_name,
                "description": "Report progress to the run owner",
                "inputSchema": {
                    "type": "object",
                    "properties": {"message": {"type": "string"}},
                    "required": ["message"],
                    "additionalProperties": false
                }
            }]
        })),
    )
    .await?;
    let thread_response = read_response_for_id(&mut manager, /*id*/ 3).await?;
    let ThreadStartResponse { thread, .. } = to_response::<ThreadStartResponse>(thread_response)?;

    send_request(
        &mut desktop,
        "turn/start",
        /*id*/ 5,
        Some(serde_json::to_value(TurnStartParams {
            idempotency_key: None,
            thread_id: thread.id.clone(),
            input: vec![UserInput::Text {
                text: "report progress".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })?),
    )
    .await?;
    read_response_for_id(&mut desktop, /*id*/ 5).await?;

    let dynamic_request = timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            let message = read_jsonrpc_message(&mut manager).await?;
            if let JSONRPCMessage::Request(request) = message
                && request.method == "item/tool/call"
            {
                return Ok::<_, anyhow::Error>(request);
            }
        }
    })
    .await??;
    let dynamic_params = dynamic_request
        .params
        .as_ref()
        .context("item/tool/call should include params")?;
    assert_eq!(dynamic_params["threadId"], thread.id);
    assert_eq!(dynamic_params["callId"], call_id);
    assert_eq!(dynamic_params["tool"], tool_name);
    assert_eq!(dynamic_params["arguments"], tool_arguments);

    assert_no_request(&mut desktop, Duration::from_millis(250)).await?;

    send_jsonrpc(
        &mut manager,
        JSONRPCMessage::Response(JSONRPCResponse {
            id: dynamic_request.id,
            result: serde_json::to_value(DynamicToolCallResponse {
                content_items: vec![DynamicToolCallOutputContentItem::InputText {
                    text: "progress accepted".to_string(),
                }],
                success: true,
            })?,
        }),
    )
    .await?;

    timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            let message = read_jsonrpc_message(&mut manager).await?;
            if let JSONRPCMessage::Notification(notification) = message
                && notification.method == "turn/completed"
                && notification
                    .params
                    .as_ref()
                    .and_then(|params| params.get("threadId"))
                    .and_then(serde_json::Value::as_str)
                    == Some(thread.id.as_str())
            {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;

    process
        .kill()
        .await
        .context("failed to stop websocket app-server process")?;
    Ok(())
}

#[tokio::test]
async fn websocket_dynamic_tool_owner_reconnects_mid_turn_without_desktop_leak() -> Result<()> {
    let call_id = "manager-reconnect-callback";
    let server = create_mock_responses_server_sequence_unchecked(vec![
        responses::sse(vec![
            responses::ev_response_created("resp-reconnect-1"),
            responses::ev_function_call(
                call_id,
                "report_progress",
                &serde_json::to_string(&json!({"message": "still working"}))?,
            ),
            responses::ev_completed("resp-reconnect-1"),
        ]),
        create_final_assistant_message_sse_response("done after reconnect")?,
    ])
    .await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut manager = connect_websocket(bind_addr).await?;
    let mut desktop = connect_websocket(bind_addr).await?;
    send_initialize_request_with_experimental_api(&mut manager, 1, "belie_manager").await?;
    read_response_for_id(&mut manager, 1).await?;
    send_initialize_request_with_experimental_api(&mut desktop, 2, "Codex Desktop").await?;
    read_response_for_id(&mut desktop, 2).await?;

    send_request(
        &mut manager,
        "thread/start",
        /*id*/ 3,
        Some(json!({
            "model": "mock-model",
            "dynamicTools": [{
                "name": "report_progress",
                "description": "Report progress to the run owner",
                "inputSchema": {
                    "type": "object",
                    "properties": {"message": {"type": "string"}},
                    "required": ["message"],
                    "additionalProperties": false
                }
            }]
        })),
    )
    .await?;
    let started: ThreadStartResponse = to_response(read_response_for_id(&mut manager, 3).await?)?;

    // Desktop may observe/start the turn, but it never owns the dynamic capability.
    send_request(
        &mut desktop,
        "turn/start",
        /*id*/ 4,
        Some(serde_json::to_value(TurnStartParams {
            idempotency_key: None,
            thread_id: started.thread.id.clone(),
            input: vec![UserInput::Text {
                text: "report progress".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })?),
    )
    .await?;
    read_response_for_id(&mut desktop, 4).await?;
    let first_dynamic_request = timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            if let JSONRPCMessage::Request(request) = read_jsonrpc_message(&mut manager).await?
                && request.method == "item/tool/call"
            {
                return Ok::<_, anyhow::Error>(request);
            }
        }
    })
    .await??;
    assert_eq!(
        first_dynamic_request.params.as_ref().unwrap()["callId"],
        call_id
    );
    assert_no_request(&mut desktop, Duration::from_millis(250)).await?;

    // Drop the owner before answering. The pending request stays parked in the app-server.
    manager.close(None).await?;
    sleep(Duration::from_millis(100)).await;
    let mut replacement_manager = connect_websocket(bind_addr).await?;
    send_initialize_request_with_experimental_api(&mut replacement_manager, 5, "belie_manager")
        .await?;
    read_response_for_id(&mut replacement_manager, 5).await?;
    send_request(
        &mut replacement_manager,
        "thread/resume",
        /*id*/ 6,
        Some(json!({"threadId": started.thread.id})),
    )
    .await?;

    let mut resume_response = None;
    let mut replayed_request = None;
    while resume_response.is_none() || replayed_request.is_none() {
        match read_jsonrpc_message(&mut replacement_manager).await? {
            JSONRPCMessage::Response(response) if response.id == RequestId::Integer(6) => {
                resume_response = Some(response);
            }
            JSONRPCMessage::Request(request) if request.method == "item/tool/call" => {
                replayed_request = Some(request);
            }
            _ => {}
        }
    }
    let resumed: ThreadResumeResponse = to_response(resume_response.unwrap())?;
    assert_eq!(resumed.thread.id, started.thread.id);
    assert_eq!(resumed.thread.turns.len(), 1);
    let replayed_request = replayed_request.unwrap();
    assert_eq!(replayed_request.params.as_ref().unwrap()["callId"], call_id);
    assert_no_request(&mut desktop, Duration::from_millis(250)).await?;

    send_jsonrpc(
        &mut replacement_manager,
        JSONRPCMessage::Response(JSONRPCResponse {
            id: replayed_request.id,
            result: serde_json::to_value(DynamicToolCallResponse {
                content_items: vec![DynamicToolCallOutputContentItem::InputText {
                    text: "replacement manager accepted".to_string(),
                }],
                success: true,
            })?,
        }),
    )
    .await?;
    timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            if let JSONRPCMessage::Notification(notification) =
                read_jsonrpc_message(&mut replacement_manager).await?
                && notification.method == "turn/completed"
            {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;
    assert_no_request(&mut desktop, Duration::from_millis(250)).await?;

    process.kill().await?;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn websocket_shared_server_identifies_the_client_that_steers_a_turn() -> Result<()> {
    let codex_home = TempDir::new()?;
    let workdir = TempDir::new()?;
    let server = create_mock_responses_server_sequence_unchecked(vec![
        create_shell_command_sse_response(
            vec!["sleep".to_string(), "2".to_string()],
            Some(workdir.path()),
            Some(10_000),
            "call_sleep",
        )?,
        create_final_assistant_message_sse_response("done")?,
    ])
    .await;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut desktop = connect_websocket(bind_addr).await?;
    let mut manager = connect_websocket(bind_addr).await?;

    send_initialize_request(&mut desktop, /*id*/ 1, "Codex Desktop").await?;
    read_response_for_id(&mut desktop, /*id*/ 1).await?;
    send_initialize_request(&mut manager, /*id*/ 2, "belie_manager").await?;
    read_response_for_id(&mut manager, /*id*/ 2).await?;

    let thread_id =
        start_thread_with_service_name(&mut desktop, /*id*/ 3, Some("codex_work_desktop")).await?;
    send_request(
        &mut desktop,
        "turn/start",
        /*id*/ 4,
        Some(serde_json::to_value(TurnStartParams {
            idempotency_key: None,
            thread_id: thread_id.clone(),
            input: vec![UserInput::Text {
                text: "run sleep".to_string(),
                text_elements: Vec::new(),
            }],
            cwd: Some(workdir.path().to_path_buf()),
            ..Default::default()
        })?),
    )
    .await?;
    let (turn_response, _) =
        read_response_and_notification_for_method(&mut desktop, /*id*/ 4, "turn/started").await?;
    let TurnStartResponse { turn } = to_response::<TurnStartResponse>(turn_response)?;

    send_request(
        &mut manager,
        "turn/steer",
        /*id*/ 5,
        Some(serde_json::to_value(TurnSteerParams {
            idempotency_key: None,
            thread_id: thread_id.clone(),
            client_user_message_id: Some("manager-steer".to_string()),
            input: vec![UserInput::Text {
                text: "manager input".to_string(),
                text_elements: Vec::new(),
            }],
            responsesapi_client_metadata: None,
            additional_context: None,
            expected_turn_id: turn.id.clone(),
        })?),
    )
    .await?;
    let steer_response = read_response_for_id(&mut manager, /*id*/ 5).await?;
    let steer = to_response::<TurnSteerResponse>(steer_response)?;
    assert_eq!(steer.turn_id, turn.id);

    let steered_input = timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            let message = read_jsonrpc_message(&mut desktop).await?;
            let JSONRPCMessage::Notification(notification) = message else {
                continue;
            };
            if notification.method != "item/started" {
                continue;
            }
            let params: ItemStartedNotification = serde_json::from_value(
                notification
                    .params
                    .context("item/started should include params")?,
            )?;
            let codex_app_server_protocol::ThreadItem::UserMessage { client_id, .. } = &params.item
            else {
                continue;
            };
            if client_id.as_deref() == Some("manager-steer") {
                return Ok::<_, anyhow::Error>(params);
            }
        }
    })
    .await??;
    assert_eq!(steered_input.client_name.as_deref(), Some("belie_manager"));

    timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            let message = read_jsonrpc_message(&mut desktop).await?;
            if let JSONRPCMessage::Notification(notification) = message
                && notification.method == "turn/completed"
                && notification
                    .params
                    .as_ref()
                    .and_then(|params| params.get("threadId"))
                    .and_then(serde_json::Value::as_str)
                    == Some(thread_id.as_str())
            {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;

    let requests = server
        .received_requests()
        .await
        .context("failed to fetch model requests")?;
    let origins = requests
        .iter()
        .filter(|request| request.url.path().ends_with("/responses"))
        .map(|request| {
            request
                .headers
                .get("originator")
                .context("model request should include originator")?
                .to_str()
                .context("originator should be valid UTF-8")
                .map(str::to_string)
        })
        .collect::<Result<Vec<_>>>()?;
    assert!(origins.iter().all(|origin| origin == "codex_work_desktop"));

    process
        .kill()
        .await
        .context("failed to stop websocket app-server process")?;
    Ok(())
}

#[tokio::test]
async fn thread_start_routes_project_exec_policy_warning_to_requester() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;

    let project = TempDir::new()?;
    std::fs::create_dir(project.path().join(".git"))?;
    let rules_dir = project.path().join(".codex/rules");
    std::fs::create_dir_all(&rules_dir)?;
    let rules_path = rules_dir.join("broken.rules");
    std::fs::write(&rules_path, "prefix_rule(")?;
    set_project_trust_level(codex_home.path(), project.path(), TrustLevel::Trusted)?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let mut requester = connect_websocket(bind_addr).await?;
    let mut other_client = connect_websocket(bind_addr).await?;

    send_initialize_request(&mut requester, /*id*/ 1, "requester").await?;
    read_response_for_id(&mut requester, /*id*/ 1).await?;
    send_initialize_request(&mut other_client, /*id*/ 2, "other_client").await?;
    read_response_for_id(&mut other_client, /*id*/ 2).await?;

    send_request(
        &mut requester,
        "thread/start",
        /*id*/ 3,
        Some(serde_json::to_value(ThreadStartParams {
            cwd: Some(project.path().display().to_string()),
            model: Some("mock-model".to_string()),
            ..Default::default()
        })?),
    )
    .await?;

    let target_id = RequestId::Integer(3);
    let warning_summary = "Error parsing rules; custom rules not applied.";
    let is_exec_policy_warning = |notification: &JSONRPCNotification| {
        notification.method == "configWarning"
            && notification
                .params
                .as_ref()
                .and_then(|params| params.get("summary"))
                .and_then(serde_json::Value::as_str)
                == Some(warning_summary)
    };
    let mut response = None;
    let mut warning = None;
    while response.is_none() || warning.is_none() {
        match read_jsonrpc_message(&mut requester).await? {
            JSONRPCMessage::Response(candidate) if candidate.id == target_id => {
                response = Some(candidate);
            }
            JSONRPCMessage::Notification(candidate) if is_exec_policy_warning(&candidate) => {
                warning = Some(candidate);
            }
            _ => {}
        }
    }

    let _: ThreadStartResponse = to_response(response.context("missing thread/start response")?)?;
    let warning: ConfigWarningNotification = serde_json::from_value(
        warning
            .context("missing exec-policy configWarning")?
            .params
            .context("configWarning should include params")?,
    )?;
    assert_eq!(
        warning
            .path
            .as_deref()
            .map(Path::new)
            .and_then(Path::file_name),
        Some(std::ffi::OsStr::new("broken.rules"))
    );

    match timeout(Duration::from_millis(250), async {
        loop {
            let message = read_jsonrpc_message(&mut other_client).await?;
            if let JSONRPCMessage::Notification(notification) = message
                && is_exec_policy_warning(&notification)
            {
                return Ok::<_, anyhow::Error>(notification);
            }
        }
    })
    .await
    {
        Ok(Ok(_)) => bail!("exec-policy configWarning leaked to another connection"),
        Ok(Err(err)) => return Err(err),
        Err(_) => {}
    }

    process
        .kill()
        .await
        .context("failed to stop websocket app-server process")?;
    Ok(())
}

#[tokio::test]
async fn websocket_transport_serves_health_endpoints_on_same_listener() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;
    let client = reqwest::Client::new();

    let readyz = http_get(&client, bind_addr, "/readyz").await?;
    assert_eq!(readyz.status(), StatusCode::OK);

    let healthz = http_get(&client, bind_addr, "/healthz").await?;
    assert_eq!(healthz.status(), StatusCode::OK);

    let mut ws = connect_websocket(bind_addr).await?;
    send_initialize_request(&mut ws, /*id*/ 1, "ws_health_client").await?;
    let init = read_response_for_id(&mut ws, /*id*/ 1).await?;
    assert_eq!(init.id, RequestId::Integer(1));

    process
        .kill()
        .await
        .context("failed to stop websocket app-server process")?;
    Ok(())
}

#[tokio::test]
async fn websocket_transport_rejects_browser_origin_without_auth() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;

    let mut ws = connect_websocket(bind_addr).await?;
    send_initialize_request(&mut ws, /*id*/ 1, "ws_loopback_client").await?;
    let init = read_response_for_id(&mut ws, /*id*/ 1).await?;
    assert_eq!(init.id, RequestId::Integer(1));
    drop(ws);

    assert_websocket_connect_rejected_with_headers(
        bind_addr,
        /*bearer_token*/ None,
        Some("https://evil.example"),
        StatusCode::FORBIDDEN,
    )
    .await?;

    process
        .kill()
        .await
        .context("failed to stop websocket app-server process")?;
    Ok(())
}

#[tokio::test]
async fn websocket_transport_rejects_missing_and_invalid_capability_tokens() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    let token_file = codex_home.path().join("app-server-token");
    std::fs::write(&token_file, "super-secret-token\n")?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    let auth_args = vec![
        "--ws-auth".to_string(),
        "capability-token".to_string(),
        "--ws-token-file".to_string(),
        token_file.display().to_string(),
    ];

    let (mut process, bind_addr) =
        spawn_websocket_server_with_args(codex_home.path(), "ws://0.0.0.0:0", &auth_args).await?;

    assert_websocket_connect_rejected(bind_addr, /*bearer_token*/ None).await?;
    assert_websocket_connect_rejected(bind_addr, Some("wrong-token")).await?;

    let mut ws = connect_websocket_with_bearer(bind_addr, Some("super-secret-token")).await?;
    send_initialize_request(&mut ws, /*id*/ 1, "ws_auth_client").await?;
    let init = read_response_for_id(&mut ws, /*id*/ 1).await?;
    assert_eq!(init.id, RequestId::Integer(1));

    process
        .kill()
        .await
        .context("failed to stop websocket app-server process")?;
    Ok(())
}

#[tokio::test]
async fn websocket_transport_verifies_signed_short_lived_bearer_tokens() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    let shared_secret_file = codex_home.path().join("app-server-signing-secret");
    let shared_secret = "0123456789abcdef0123456789abcdef";
    std::fs::write(&shared_secret_file, format!("{shared_secret}\n"))?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;
    let auth_args = vec![
        "--ws-auth".to_string(),
        "signed-bearer-token".to_string(),
        "--ws-shared-secret-file".to_string(),
        shared_secret_file.display().to_string(),
        "--ws-issuer".to_string(),
        "codex-enroller".to_string(),
        "--ws-audience".to_string(),
        "codex-app-server".to_string(),
        "--ws-max-clock-skew-seconds".to_string(),
        "1".to_string(),
    ];

    let (mut process, bind_addr) =
        spawn_websocket_server_with_args(codex_home.path(), "ws://127.0.0.1:0", &auth_args).await?;
    let expired_token = signed_bearer_token(
        shared_secret.as_bytes(),
        json!({
            "exp": OffsetDateTime::now_utc().unix_timestamp() - 30,
            "iss": "codex-enroller",
            "aud": "codex-app-server",
        }),
    )?;
    assert_websocket_connect_rejected(bind_addr, Some(expired_token.as_str())).await?;

    let malformed_token = "not-a-jwt";
    assert_websocket_connect_rejected(bind_addr, Some(malformed_token)).await?;

    let not_yet_valid_token = signed_bearer_token(
        shared_secret.as_bytes(),
        json!({
            "exp": OffsetDateTime::now_utc().unix_timestamp() + 60,
            "nbf": OffsetDateTime::now_utc().unix_timestamp() + 30,
            "iss": "codex-enroller",
            "aud": "codex-app-server",
        }),
    )?;
    assert_websocket_connect_rejected(bind_addr, Some(not_yet_valid_token.as_str())).await?;

    let wrong_issuer_token = signed_bearer_token(
        shared_secret.as_bytes(),
        json!({
            "exp": OffsetDateTime::now_utc().unix_timestamp() + 60,
            "iss": "someone-else",
            "aud": "codex-app-server",
        }),
    )?;
    assert_websocket_connect_rejected(bind_addr, Some(wrong_issuer_token.as_str())).await?;

    let wrong_audience_token = signed_bearer_token(
        shared_secret.as_bytes(),
        json!({
            "exp": OffsetDateTime::now_utc().unix_timestamp() + 60,
            "iss": "codex-enroller",
            "aud": "wrong-audience",
        }),
    )?;
    assert_websocket_connect_rejected(bind_addr, Some(wrong_audience_token.as_str())).await?;

    let wrong_signature_token = signed_bearer_token(
        b"fedcba9876543210fedcba9876543210",
        json!({
            "exp": OffsetDateTime::now_utc().unix_timestamp() + 60,
            "iss": "codex-enroller",
            "aud": "codex-app-server",
        }),
    )?;
    assert_websocket_connect_rejected(bind_addr, Some(wrong_signature_token.as_str())).await?;

    let valid_token = signed_bearer_token(
        shared_secret.as_bytes(),
        json!({
            "exp": OffsetDateTime::now_utc().unix_timestamp() + 60,
            "iss": "codex-enroller",
            "aud": "codex-app-server",
        }),
    )?;
    let mut ws = connect_websocket_with_bearer(bind_addr, Some(valid_token.as_str())).await?;
    send_initialize_request(&mut ws, /*id*/ 1, "ws_signed_auth_client").await?;
    let init = read_response_for_id(&mut ws, /*id*/ 1).await?;
    assert_eq!(init.id, RequestId::Integer(1));

    process
        .kill()
        .await
        .context("failed to stop websocket app-server process")?;
    Ok(())
}

#[tokio::test]
async fn websocket_transport_rejects_short_signed_bearer_secret_configuration() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    let shared_secret_file = codex_home.path().join("app-server-signing-secret");
    std::fs::write(&shared_secret_file, "too-short\n")?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;

    let output = run_websocket_server_to_completion_with_args(
        codex_home.path(),
        "ws://127.0.0.1:0",
        &[
            "--ws-auth".to_string(),
            "signed-bearer-token".to_string(),
            "--ws-shared-secret-file".to_string(),
            shared_secret_file.display().to_string(),
        ],
    )
    .await?;
    assert!(
        !output.status.success(),
        "short shared secret should fail websocket server startup"
    );
    let stderr = String::from_utf8(output.stderr).context("stderr should be valid utf-8")?;
    assert!(
        stderr.contains("must be at least 32 bytes"),
        "unexpected stderr: {stderr}"
    );

    Ok(())
}

#[tokio::test]
async fn websocket_transport_rejects_unauthenticated_non_loopback_startup() -> Result<()> {
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;

    let output =
        run_websocket_server_to_completion_with_args(codex_home.path(), "ws://0.0.0.0:0", &[])
            .await?;
    assert!(
        !output.status.success(),
        "unauthenticated non-loopback listener should fail websocket server startup"
    );
    let stderr = String::from_utf8(output.stderr).context("stderr should be valid utf-8")?;
    assert!(
        stderr.contains("refusing to start non-loopback websocket listener"),
        "unexpected stderr: {stderr}"
    );

    Ok(())
}

#[tokio::test]
async fn websocket_disconnect_keeps_last_subscribed_thread_loaded_until_idle_timeout() -> Result<()>
{
    let server = create_mock_responses_server_sequence_unchecked(Vec::new()).await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri(), "never")?;

    let (mut process, bind_addr) = spawn_websocket_server(codex_home.path()).await?;

    let mut ws1 = connect_websocket(bind_addr).await?;
    send_initialize_request(&mut ws1, /*id*/ 1, "ws_thread_owner").await?;
    read_response_for_id(&mut ws1, /*id*/ 1).await?;

    let thread_id = start_thread(&mut ws1, /*id*/ 2).await?;
    assert_loaded_threads(&mut ws1, /*id*/ 3, &[thread_id.as_str()]).await?;

    ws1.close(None).await.context("failed to close websocket")?;
    drop(ws1);

    let mut ws2 = connect_websocket(bind_addr).await?;
    send_initialize_request(&mut ws2, /*id*/ 4, "ws_reconnect_client").await?;
    read_response_for_id(&mut ws2, /*id*/ 4).await?;

    wait_for_loaded_threads(&mut ws2, /*first_id*/ 5, &[thread_id.as_str()]).await?;

    process
        .kill()
        .await
        .context("failed to stop websocket app-server process")?;
    Ok(())
}

pub(super) async fn spawn_websocket_server(codex_home: &Path) -> Result<(Child, SocketAddr)> {
    spawn_websocket_server_with_args(codex_home, "ws://127.0.0.1:0", &[]).await
}

pub(super) async fn spawn_websocket_server_with_args(
    codex_home: &Path,
    listen_url: &str,
    extra_args: &[String],
) -> Result<(Child, SocketAddr)> {
    let program = codex_utils_cargo_bin::cargo_bin("codex-app-server")
        .context("should find app-server binary")?;
    let mut cmd = Command::new(program);
    cmd.arg("--listen")
        .arg(listen_url)
        .arg(DISABLE_PLUGIN_STARTUP_TASKS_ARG)
        .args(extra_args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .env("CODEX_HOME", codex_home)
        .env_remove(CODEX_INTERNAL_ORIGINATOR_OVERRIDE_ENV_VAR)
        .env("RUST_LOG", "warn");
    let mut process = cmd
        .kill_on_drop(true)
        .spawn()
        .context("failed to spawn websocket app-server process")?;

    let stderr = process
        .stderr
        .take()
        .context("failed to capture websocket app-server stderr")?;
    let mut stderr_reader = BufReader::new(stderr).lines();
    let deadline = Instant::now() + DEFAULT_READ_TIMEOUT;
    let bind_addr = loop {
        let line = timeout(
            deadline.saturating_duration_since(Instant::now()),
            stderr_reader.next_line(),
        )
        .await
        .context("timed out waiting for websocket app-server to report bound websocket address")?
        .context("failed to read websocket app-server stderr")?
        .context("websocket app-server exited before reporting bound websocket address")?;
        eprintln!("[websocket app-server stderr] {line}");

        let stripped_line = {
            let mut stripped = String::with_capacity(line.len());
            let mut chars = line.chars().peekable();
            while let Some(ch) = chars.next() {
                if ch == '\u{1b}' && matches!(chars.peek(), Some(&'[')) {
                    chars.next();
                    for next in chars.by_ref() {
                        if ('@'..='~').contains(&next) {
                            break;
                        }
                    }
                    continue;
                }
                stripped.push(ch);
            }
            stripped
        };

        if let Some(bind_addr) = stripped_line
            .split_whitespace()
            .find_map(|token| token.strip_prefix("ws://"))
            .and_then(|addr| addr.parse::<SocketAddr>().ok())
        {
            break bind_addr;
        }
    };

    tokio::spawn(async move {
        while let Ok(Some(line)) = stderr_reader.next_line().await {
            eprintln!("[websocket app-server stderr] {line}");
        }
    });

    Ok((process, bind_addr))
}

pub(super) async fn connect_websocket(bind_addr: SocketAddr) -> Result<WsClient> {
    connect_websocket_with_bearer(bind_addr, /*bearer_token*/ None).await
}

pub(super) async fn connect_websocket_with_bearer(
    bind_addr: SocketAddr,
    bearer_token: Option<&str>,
) -> Result<WsClient> {
    let url = format!("ws://{}", connectable_bind_addr(bind_addr));
    let request = websocket_request(url.as_str(), bearer_token, /*origin*/ None)?;
    let deadline = Instant::now() + DEFAULT_READ_TIMEOUT;
    loop {
        match connect_async(request.clone()).await {
            Ok((stream, _response)) => return Ok(stream),
            Err(err) => {
                if Instant::now() >= deadline {
                    bail!("failed to connect websocket to {url}: {err}");
                }
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

async fn assert_websocket_connect_rejected(
    bind_addr: SocketAddr,
    bearer_token: Option<&str>,
) -> Result<()> {
    assert_websocket_connect_rejected_with_headers(
        bind_addr,
        bearer_token,
        /*origin*/ None,
        StatusCode::UNAUTHORIZED,
    )
    .await
}

async fn assert_websocket_connect_rejected_with_headers(
    bind_addr: SocketAddr,
    bearer_token: Option<&str>,
    origin: Option<&str>,
    expected_status: StatusCode,
) -> Result<()> {
    let url = format!("ws://{}", connectable_bind_addr(bind_addr));
    let request = websocket_request(url.as_str(), bearer_token, origin)?;

    match connect_async(request).await {
        Ok((_stream, response)) => {
            bail!(
                "expected websocket handshake rejection, got {}",
                response.status()
            )
        }
        Err(WsError::Http(response)) => {
            assert_eq!(response.status(), expected_status);
            Ok(())
        }
        Err(err) => bail!("expected http rejection during websocket handshake: {err}"),
    }
}

async fn run_websocket_server_to_completion_with_args(
    codex_home: &Path,
    listen_url: &str,
    extra_args: &[String],
) -> Result<std::process::Output> {
    let program = codex_utils_cargo_bin::cargo_bin("codex-app-server")
        .context("should find app-server binary")?;
    let mut cmd = Command::new(program);
    cmd.arg("--listen")
        .arg(listen_url)
        .arg(DISABLE_PLUGIN_STARTUP_TASKS_ARG)
        .args(extra_args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .env("CODEX_HOME", codex_home)
        .env("RUST_LOG", "warn");
    timeout(DEFAULT_READ_TIMEOUT, cmd.output())
        .await
        .context("timed out waiting for websocket app-server to exit")?
        .context("failed to run websocket app-server")
}

async fn http_get(
    client: &reqwest::Client,
    bind_addr: SocketAddr,
    path: &str,
) -> Result<reqwest::Response> {
    let connectable_bind_addr = connectable_bind_addr(bind_addr);
    let deadline = Instant::now() + DEFAULT_READ_TIMEOUT;
    loop {
        match client
            .get(format!("http://{connectable_bind_addr}{path}"))
            .send()
            .await
            .with_context(|| format!("failed to GET http://{connectable_bind_addr}{path}"))
        {
            Ok(response) => return Ok(response),
            Err(err) => {
                if Instant::now() >= deadline {
                    bail!("failed to GET http://{connectable_bind_addr}{path}: {err}");
                }
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

fn websocket_request(
    url: &str,
    bearer_token: Option<&str>,
    origin: Option<&str>,
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>> {
    let mut request = url
        .into_client_request()
        .context("failed to create websocket request")?;
    if let Some(bearer_token) = bearer_token {
        request.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {bearer_token}"))
                .context("invalid bearer token header")?,
        );
    }
    if let Some(origin) = origin {
        request.headers_mut().insert(
            ORIGIN,
            HeaderValue::from_str(origin).context("invalid origin header")?,
        );
    }
    Ok(request)
}

pub(super) async fn send_initialize_request(
    stream: &mut WsClient,
    id: i64,
    client_name: &str,
) -> Result<()> {
    let params = InitializeParams {
        client_info: ClientInfo {
            name: client_name.to_string(),
            title: Some("WebSocket Test Client".to_string()),
            version: "0.1.0".to_string(),
        },
        capabilities: None,
    };
    send_request(
        stream,
        "initialize",
        id,
        Some(serde_json::to_value(params)?),
    )
    .await
}

async fn send_initialize_request_with_experimental_api(
    stream: &mut WsClient,
    id: i64,
    client_name: &str,
) -> Result<()> {
    let params = InitializeParams {
        client_info: ClientInfo {
            name: client_name.to_string(),
            title: Some("WebSocket Test Client".to_string()),
            version: "0.1.0".to_string(),
        },
        capabilities: Some(InitializeCapabilities {
            experimental_api: true,
            ..Default::default()
        }),
    };
    send_request(
        stream,
        "initialize",
        id,
        Some(serde_json::to_value(params)?),
    )
    .await
}

async fn start_thread(stream: &mut WsClient, id: i64) -> Result<String> {
    start_thread_with_service_name(stream, id, None).await
}

async fn start_thread_with_service_name(
    stream: &mut WsClient,
    id: i64,
    service_name: Option<&str>,
) -> Result<String> {
    send_request(
        stream,
        "thread/start",
        id,
        Some(serde_json::to_value(ThreadStartParams {
            model: Some("mock-model".to_string()),
            service_name: service_name.map(str::to_string),
            ..Default::default()
        })?),
    )
    .await?;
    let response = read_response_for_id(stream, id).await?;
    let ThreadStartResponse { thread, .. } = to_response::<ThreadStartResponse>(response)?;
    Ok(thread.id)
}

async fn start_turn_and_read_origins(
    stream: &mut WsClient,
    id: i64,
    thread_id: &str,
    text: &str,
) -> Result<(String, String)> {
    send_request(
        stream,
        "turn/start",
        id,
        Some(serde_json::to_value(TurnStartParams {
            idempotency_key: None,
            thread_id: thread_id.to_string(),
            input: vec![UserInput::Text {
                text: text.to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })?),
    )
    .await?;

    let request_id = RequestId::Integer(id);
    let mut response_seen = false;
    let mut turn_client_name = None;
    let mut input_client_name = None;
    let mut completed = false;
    while !response_seen || turn_client_name.is_none() || input_client_name.is_none() || !completed
    {
        match read_jsonrpc_message(stream).await? {
            JSONRPCMessage::Response(response) if response.id == request_id => {
                let _: TurnStartResponse = to_response(response)?;
                response_seen = true;
            }
            JSONRPCMessage::Notification(notification) if notification.method == "turn/started" => {
                let params: TurnStartedNotification = serde_json::from_value(
                    notification
                        .params
                        .context("turn/started should include params")?,
                )?;
                if params.thread_id == thread_id {
                    turn_client_name = params.client_name;
                }
            }
            JSONRPCMessage::Notification(notification) if notification.method == "item/started" => {
                let params: ItemStartedNotification = serde_json::from_value(
                    notification
                        .params
                        .context("item/started should include params")?,
                )?;
                if params.thread_id == thread_id
                    && matches!(
                        params.item,
                        codex_app_server_protocol::ThreadItem::UserMessage { .. }
                    )
                {
                    input_client_name = params.client_name;
                }
            }
            JSONRPCMessage::Notification(notification)
                if notification.method == "turn/completed"
                    && notification
                        .params
                        .as_ref()
                        .and_then(|params| params.get("threadId"))
                        .and_then(serde_json::Value::as_str)
                        == Some(thread_id) =>
            {
                completed = true;
            }
            _ => {}
        }
    }

    Ok((
        turn_client_name.context("turn/started should identify its connection client")?,
        input_client_name.context("user input should identify its connection client")?,
    ))
}

async fn assert_loaded_threads(stream: &mut WsClient, id: i64, expected: &[&str]) -> Result<()> {
    let response = request_loaded_threads(stream, id).await?;
    let mut actual = response.data;
    actual.sort();
    let mut expected = expected
        .iter()
        .map(|thread_id| (*thread_id).to_string())
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(actual, expected);
    assert_eq!(response.next_cursor, None);
    Ok(())
}

async fn wait_for_loaded_threads(
    stream: &mut WsClient,
    first_id: i64,
    expected: &[&str],
) -> Result<()> {
    let mut next_id = first_id;
    let expected = expected
        .iter()
        .map(|thread_id| (*thread_id).to_string())
        .collect::<Vec<_>>();
    timeout(DEFAULT_READ_TIMEOUT, async {
        loop {
            let response = request_loaded_threads(stream, next_id).await?;
            next_id += 1;
            let mut actual = response.data;
            actual.sort();
            if actual == expected {
                return Ok::<(), anyhow::Error>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("timed out waiting for loaded thread list")??;
    Ok(())
}

async fn request_loaded_threads(
    stream: &mut WsClient,
    id: i64,
) -> Result<ThreadLoadedListResponse> {
    send_request(
        stream,
        "thread/loaded/list",
        id,
        Some(serde_json::to_value(ThreadLoadedListParams::default())?),
    )
    .await?;
    let response = read_response_for_id(stream, id).await?;
    to_response::<ThreadLoadedListResponse>(response)
}

async fn send_config_read_request(stream: &mut WsClient, id: i64) -> Result<()> {
    send_request(
        stream,
        "config/read",
        id,
        Some(json!({ "includeLayers": false })),
    )
    .await
}

pub(super) async fn send_request(
    stream: &mut WsClient,
    method: &str,
    id: i64,
    params: Option<serde_json::Value>,
) -> Result<()> {
    let message = JSONRPCMessage::Request(JSONRPCRequest {
        id: RequestId::Integer(id),
        method: method.to_string(),
        params,
        trace: None,
    });
    send_jsonrpc(stream, message).await
}

pub(super) async fn send_jsonrpc(stream: &mut WsClient, message: JSONRPCMessage) -> Result<()> {
    let payload = serde_json::to_string(&message)?;
    stream
        .send(WebSocketMessage::Text(payload.into()))
        .await
        .context("failed to send websocket frame")
}

pub(super) async fn read_response_for_id(
    stream: &mut WsClient,
    id: i64,
) -> Result<JSONRPCResponse> {
    let target_id = RequestId::Integer(id);
    loop {
        let message = read_jsonrpc_message(stream).await?;
        match message {
            JSONRPCMessage::Response(response) if response.id == target_id => {
                return Ok(response);
            }
            JSONRPCMessage::Error(error) if error.id == target_id => {
                bail!("request {id} failed: {}", error.error.message);
            }
            _ => {}
        }
    }
}

pub(super) async fn read_notification_for_method(
    stream: &mut WsClient,
    method: &str,
) -> Result<JSONRPCNotification> {
    loop {
        let message = read_jsonrpc_message(stream).await?;
        if let JSONRPCMessage::Notification(notification) = message
            && notification.method == method
        {
            return Ok(notification);
        }
    }
}

pub(super) async fn read_response_and_notification_for_method(
    stream: &mut WsClient,
    id: i64,
    method: &str,
) -> Result<(JSONRPCResponse, JSONRPCNotification)> {
    let target_id = RequestId::Integer(id);
    let mut response = None;
    let mut notification = None;

    while response.is_none() || notification.is_none() {
        let message = read_jsonrpc_message(stream).await?;
        match message {
            JSONRPCMessage::Response(candidate) if candidate.id == target_id => {
                response = Some(candidate);
            }
            JSONRPCMessage::Notification(candidate)
                if candidate.method == method && notification.is_some() =>
            {
                bail!(
                    "received duplicate notification for method `{method}` before completing paired read"
                );
            }
            JSONRPCMessage::Notification(candidate) if candidate.method == method => {
                notification = Some(candidate);
            }
            _ => {}
        }
    }

    let Some(response) = response else {
        bail!("response must be set before returning");
    };
    let Some(notification) = notification else {
        bail!("notification must be set before returning");
    };

    Ok((response, notification))
}

pub(super) async fn read_error_for_id(stream: &mut WsClient, id: i64) -> Result<JSONRPCError> {
    let target_id = RequestId::Integer(id);
    loop {
        let message = read_jsonrpc_message(stream).await?;
        match message {
            JSONRPCMessage::Error(err) if err.id == target_id => return Ok(err),
            JSONRPCMessage::Response(response) if response.id == target_id => {
                bail!("request {id} unexpectedly succeeded while waiting for an error")
            }
            _ => {}
        }
    }
}

pub(super) async fn read_jsonrpc_message(stream: &mut WsClient) -> Result<JSONRPCMessage> {
    loop {
        let frame = timeout(DEFAULT_READ_TIMEOUT, stream.next())
            .await
            .context("timed out waiting for websocket frame")?
            .context("websocket stream ended unexpectedly")?
            .context("failed to read websocket frame")?;

        match frame {
            WebSocketMessage::Text(text) => return Ok(serde_json::from_str(text.as_ref())?),
            WebSocketMessage::Ping(payload) => {
                stream.send(WebSocketMessage::Pong(payload)).await?;
            }
            WebSocketMessage::Pong(_) => {}
            WebSocketMessage::Close(frame) => {
                bail!("websocket closed unexpectedly: {frame:?}")
            }
            WebSocketMessage::Binary(_) => bail!("unexpected binary websocket frame"),
            WebSocketMessage::Frame(_) => {}
        }
    }
}

pub(super) async fn assert_no_message(stream: &mut WsClient, wait_for: Duration) -> Result<()> {
    match timeout(wait_for, stream.next()).await {
        Ok(Some(Ok(frame))) => bail!("unexpected frame while waiting for silence: {frame:?}"),
        Ok(Some(Err(err))) => bail!("unexpected websocket read error: {err}"),
        Ok(None) => bail!("websocket closed unexpectedly while waiting for silence"),
        Err(_) => Ok(()),
    }
}

async fn assert_no_request(stream: &mut WsClient, wait_for: Duration) -> Result<()> {
    match timeout(wait_for, async {
        loop {
            if let JSONRPCMessage::Request(request) = read_jsonrpc_message(stream).await? {
                return Ok::<_, anyhow::Error>(request);
            }
        }
    })
    .await
    {
        Ok(Ok(request)) => bail!("unexpected server request: {request:?}"),
        Ok(Err(err)) => Err(err),
        Err(_) => Ok(()),
    }
}

pub(super) fn create_config_toml(
    codex_home: &Path,
    server_uri: &str,
    approval_policy: &str,
) -> std::io::Result<()> {
    let config_toml = codex_home.join("config.toml");
    std::fs::write(
        config_toml,
        format!(
            r#"
model = "mock-model"
approval_policy = "{approval_policy}"
sandbox_mode = "read-only"

model_provider = "mock_provider"

[model_providers.mock_provider]
name = "Mock provider for test"
base_url = "{server_uri}/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
"#
        ),
    )
}

fn connectable_bind_addr(bind_addr: SocketAddr) -> SocketAddr {
    match bind_addr {
        SocketAddr::V4(addr) if addr.ip().is_unspecified() => {
            SocketAddr::from(([127, 0, 0, 1], addr.port()))
        }
        SocketAddr::V6(addr) if addr.ip().is_unspecified() => {
            SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], addr.port()))
        }
        _ => bind_addr,
    }
}

fn signed_bearer_token(shared_secret: &[u8], claims: serde_json::Value) -> Result<String> {
    let header_segment = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let claims_segment = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
    let payload = format!("{header_segment}.{claims_segment}");
    let mut mac = HmacSha256::new_from_slice(shared_secret).context("failed to create hmac")?;
    mac.update(payload.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(format!("{payload}.{signature}"))
}
