use crate::web_client::authentication::SessionTokenHash;
use crate::web_client::control_message::{
    SetConfigPayload, WebClientToWebServerControlMessage,
    WebClientToWebServerControlMessagePayload, WebServerToWebClientControlMessage,
};
use crate::web_client::message_handlers::{
    parse_stdin, render_to_client, send_control_messages_to_client,
};
use crate::web_client::server_listener::zellij_server_listener;
use crate::web_client::types::{AppState, TerminalParams};

use crate::keyboard_parser::KittyKeyboardParser;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path as AxumPath, Query, State,
    },
    response::IntoResponse,
};
use futures::StreamExt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;
use zellij_utils::{
    input::mouse::MouseEvent, ipc::ClientToServerMsg, vendored::termwiz::input::InputParser,
};

const HEARTBEAT_INTERVAL_SECS: u64 = 30;
const DEFAULT_HEARTBEAT_TIMEOUT_SECS: u64 = 45;
/// If the gap between heartbeat ticks exceeds this multiple of the interval,
/// the system likely just woke from sleep. Reset the timer instead of killing
/// the connection.
const SLEEP_DETECTION_MULTIPLIER: u64 = 3;

fn heartbeat_timed_out(heartbeat_timeout_secs: Option<u64>, now: u64, last_response: u64) -> bool {
    match heartbeat_timeout_secs {
        Some(0) => false,
        Some(timeout_secs) => now.saturating_sub(last_response) > timeout_secs,
        None => now.saturating_sub(last_response) > DEFAULT_HEARTBEAT_TIMEOUT_SECS,
    }
}

pub async fn ws_handler_control(
    ws: WebSocketUpgrade,
    _path: Option<AxumPath<String>>,
    State(state): State<AppState>,
    axum::Extension(session_token_hash): axum::Extension<SessionTokenHash>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws_control(socket, state, session_token_hash))
}

pub async fn ws_handler_terminal(
    ws: WebSocketUpgrade,
    session_name: Option<AxumPath<String>>,
    Query(params): Query<TerminalParams>,
    State(state): State<AppState>,
    axum::Extension(session_token_hash): axum::Extension<SessionTokenHash>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| {
        handle_ws_terminal(socket, session_name, params, state, session_token_hash)
    })
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn handle_ws_control(
    socket: WebSocket,
    state: AppState,
    session_token_hash: SessionTokenHash,
) {
    let payload = SetConfigPayload::from(&*state.config.lock().unwrap());
    let set_config_msg = WebServerToWebClientControlMessage::SetConfig(payload);

    let (control_socket_tx, mut control_socket_rx) = socket.split();

    let (control_channel_tx, control_channel_rx) = tokio::sync::mpsc::unbounded_channel();
    send_control_messages_to_client(control_channel_rx, control_socket_tx);

    let _ = control_channel_tx.send(Message::Text(
        serde_json::to_string(&set_config_msg).unwrap().into(),
    ));

    // Track last heartbeat response time (shared with heartbeat task)
    let last_heartbeat_response = Arc::new(AtomicU64::new(current_timestamp()));
    let heartbeat_timeout_secs = state
        .config
        .lock()
        .unwrap()
        .options
        .web_heartbeat_timeout_secs;
    let heartbeat_cancellation = CancellationToken::new();

    // Spawn heartbeat sender task
    let heartbeat_tx = control_channel_tx.clone();
    let heartbeat_last_response = last_heartbeat_response.clone();
    let heartbeat_cancel = heartbeat_cancellation.clone();
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
        loop {
            tokio::select! {
                _ = heartbeat_cancel.cancelled() => {
                    break;
                }
                _ = interval.tick() => {
                    let now = current_timestamp();
                    let last_response = heartbeat_last_response.load(Ordering::Relaxed);
                    let elapsed = now.saturating_sub(last_response);

                    // Detect system sleep/wake: if the elapsed time since
                    // the last heartbeat response is much larger than the
                    // heartbeat interval, the machine was likely asleep.
                    // Reset the timer and probe the client instead of
                    // immediately killing the connection.
                    if elapsed > HEARTBEAT_INTERVAL_SECS * SLEEP_DETECTION_MULTIPLIER {
                        log::info!(
                            "Detected possible system wake ({}s since last heartbeat response, expected ~{}s) - resetting heartbeat timer",
                            elapsed, HEARTBEAT_INTERVAL_SECS
                        );
                        heartbeat_last_response.store(now, Ordering::Relaxed);
                        // Send a heartbeat immediately to verify the client
                        // is still alive after wake.
                        let heartbeat_msg = WebServerToWebClientControlMessage::Heartbeat { timestamp: now };
                        if heartbeat_tx.send(Message::Text(
                            serde_json::to_string(&heartbeat_msg).unwrap().into(),
                        )).is_err() {
                            break;
                        }
                        continue;
                    }

                    // Check if client has timed out — drop tx to signal close to main loop
                    if heartbeat_timed_out(heartbeat_timeout_secs, now, last_response) {
                        let timeout_secs = heartbeat_timeout_secs.unwrap_or(DEFAULT_HEARTBEAT_TIMEOUT_SECS);
                        log::warn!(
                            "WebSocket control connection timed out for client - no heartbeat response received within {} seconds",
                            timeout_secs
                        );
                        drop(heartbeat_tx);
                        break;
                    }

                    // Send heartbeat
                    let heartbeat_msg = WebServerToWebClientControlMessage::Heartbeat { timestamp: now };
                    if heartbeat_tx.send(Message::Text(
                        serde_json::to_string(&heartbeat_msg).unwrap().into(),
                    )).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let send_message_to_server = |deserialized_msg: WebClientToWebServerControlMessage| {
        let Some(client_connection) = state
            .connection_table
            .lock()
            .unwrap()
            .get_client_os_api(&deserialized_msg.web_client_id)
            .cloned()
        else {
            log::error!("Control WebSocket: Unknown web_client_id '{}' - client may have been disconnected or session expired", deserialized_msg.web_client_id);
            return;
        };
        let client_msg = match &deserialized_msg.payload {
            WebClientToWebServerControlMessagePayload::TerminalResize(size) => {
                ClientToServerMsg::TerminalResize { new_size: *size }
            },
            WebClientToWebServerControlMessagePayload::HeartbeatResponse => {
                // Heartbeat responses are handled separately, not forwarded to server
                return;
            },
        };

        let _ = client_connection.send_to_server(client_msg);
    };

    let mut set_client_control_channel = false;
    // Throttle resize events to avoid overwhelming the server, matching the
    // native client's SIGWINCH_CB_THROTTLE_DURATION of 50ms.
    const RESIZE_THROTTLE_DURATION: Duration = Duration::from_millis(50);
    let mut last_resize_at: Option<Instant> = None;
    // Abort handle for the trailing-resize task scheduled during throttle bursts.
    let mut pending_resize_abort: Option<tokio::task::AbortHandle> = None;

    while let Some(Ok(msg)) = control_socket_rx.next().await {
        match msg {
            Message::Text(msg) => {
                let deserialized_msg: Result<WebClientToWebServerControlMessage, _> =
                    serde_json::from_str(&msg);
                match deserialized_msg {
                    Ok(deserialized_msg) => {
                        if !state
                            .connection_table
                            .lock()
                            .unwrap()
                            .verify_client_ownership(
                                &deserialized_msg.web_client_id,
                                &session_token_hash.0,
                            )
                        {
                            log::error!(
                                "Client attempted to use web_client_id {} that does not belong to their session",
                                deserialized_msg.web_client_id
                            );
                            return;
                        }
                        if !set_client_control_channel {
                            set_client_control_channel = true;
                            state
                                .connection_table
                                .lock()
                                .unwrap()
                                .add_client_control_tx(
                                    &deserialized_msg.web_client_id,
                                    control_channel_tx.clone(),
                                );
                        }
                        // Handle heartbeat response
                        if let WebClientToWebServerControlMessagePayload::HeartbeatResponse {
                            ..
                        } = &deserialized_msg.payload
                        {
                            last_heartbeat_response.store(current_timestamp(), Ordering::Relaxed);
                        }

                        // Debounce resize messages: send immediately on leading edge, then
                        // schedule a trailing send for the final resize in a burst so the
                        // terminal never stays stuck at a stale size.
                        if matches!(
                            deserialized_msg.payload,
                            WebClientToWebServerControlMessagePayload::TerminalResize(_)
                        ) {
                            // Cancel any previously scheduled trailing resize.
                            if let Some(handle) = pending_resize_abort.take() {
                                handle.abort();
                            }
                            if let Some(last) = last_resize_at {
                                if last.elapsed() < RESIZE_THROTTLE_DURATION {
                                    // Schedule a trailing send after the throttle window.
                                    let state_clone = state.clone();
                                    let msg_clone = deserialized_msg.clone();
                                    let task = tokio::spawn(async move {
                                        tokio::time::sleep(RESIZE_THROTTLE_DURATION).await;
                                        if let WebClientToWebServerControlMessagePayload::TerminalResize(size) = msg_clone.payload {
                                            if let Some(client_connection) = state_clone
                                                .connection_table
                                                .lock()
                                                .unwrap()
                                                .get_client_os_api(&msg_clone.web_client_id)
                                                .cloned()
                                            {
                                                let _ = client_connection.send_to_server(
                                                    ClientToServerMsg::TerminalResize { new_size: size },
                                                );
                                            }
                                        }
                                    });
                                    pending_resize_abort = Some(task.abort_handle());
                                    continue;
                                }
                            }
                            last_resize_at = Some(Instant::now());
                        }
                        send_message_to_server(deserialized_msg);
                    },
                    Err(e) => {
                        log::error!("Failed to deserialize control message from client: {:?} - message may be malformed or protocol version mismatch", e);
                    },
                }
            },
            Message::Ping(payload) => {
                let _ = control_channel_tx.send(Message::Pong(payload));
            },
            Message::Pong(_) => {},
            Message::Close(_) => {
                log::info!("Control WebSocket closed by client - connection terminated normally");
                heartbeat_cancellation.cancel();
                if let Some(handle) = pending_resize_abort.take() {
                    handle.abort();
                }
                return;
            },
            _ => {
                log::error!(
                    "Received unsupported WebSocket message type: {:?} - ignoring",
                    msg
                );
            },
        }
    }

    heartbeat_cancellation.cancel();
}

async fn handle_ws_terminal(
    socket: WebSocket,
    session_name: Option<AxumPath<String>>,
    params: TerminalParams,
    state: AppState,
    session_token_hash: SessionTokenHash,
) {
    let web_client_id = params.web_client_id;
    let is_cli_client = params.is_cli_client;

    // Verify the session token owns this web_client_id
    if !state
        .connection_table
        .lock()
        .unwrap()
        .verify_client_ownership(&web_client_id, &session_token_hash.0)
    {
        log::error!(
            "Terminal WebSocket: Authentication failed - client does not own web_client_id '{}' (possible session hijacking attempt)",
            web_client_id
        );
        return;
    }

    let Some(os_input) = state
        .connection_table
        .lock()
        .unwrap()
        .get_client_os_api(&web_client_id)
        .cloned()
    else {
        log::error!("Terminal WebSocket: Unknown web_client_id '{}' - session may have expired or client was disconnected", web_client_id);
        return;
    };

    let (client_terminal_channel_tx, mut client_terminal_channel_rx) = socket.split();
    let (stdout_channel_tx, stdout_channel_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .connection_table
        .lock()
        .unwrap()
        .add_client_terminal_tx(&web_client_id, stdout_channel_tx);

    let (attachment_complete_tx, attachment_complete_rx) = tokio::sync::oneshot::channel();

    zellij_server_listener(
        os_input.clone(),
        state.connection_table.clone(),
        session_name.map(|p| p.0),
        state.config.lock().unwrap().clone(),
        state.config_options.clone(),
        Some(state.config_file_path.clone()),
        web_client_id.clone(),
        state.session_manager.clone(),
        Some(attachment_complete_tx),
        is_cli_client,
    );

    let terminal_channel_cancellation_token = CancellationToken::new();
    let should_not_reconnect = state
        .connection_table
        .lock()
        .unwrap()
        .get_should_not_reconnect_flag(&web_client_id)
        .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
    let (terminal_protocol_tx, terminal_protocol_rx) =
        tokio::sync::mpsc::unbounded_channel::<Message>();
    render_to_client(
        stdout_channel_rx,
        client_terminal_channel_tx,
        terminal_channel_cancellation_token.clone(),
        should_not_reconnect,
        Some(terminal_protocol_rx),
    );
    state
        .connection_table
        .lock()
        .unwrap()
        .add_client_terminal_channel_cancellation_token(
            &web_client_id,
            terminal_channel_cancellation_token,
        );

    let explicitly_disable_kitty_keyboard_protocol = state
        .config
        .lock()
        .unwrap()
        .options
        .support_kitty_keyboard_protocol
        .map(|e| !e)
        .unwrap_or(false);

    let _ = attachment_complete_rx.await;

    let mut mouse_old_event = MouseEvent::new();
    let mut kitty_parser = KittyKeyboardParser::new();
    let mut input_parser = InputParser::new();
    while let Some(Ok(msg)) = client_terminal_channel_rx.next().await {
        match msg {
            Message::Binary(buf) => {
                let Some(client_connection) = state
                    .connection_table
                    .lock()
                    .unwrap()
                    .get_client_os_api(&web_client_id)
                    .cloned()
                else {
                    log::error!("Unknown web_client_id: {}", web_client_id);
                    continue;
                };
                parse_stdin(
                    &buf,
                    client_connection.clone(),
                    &mut mouse_old_event,
                    explicitly_disable_kitty_keyboard_protocol,
                    &mut kitty_parser,
                    &mut input_parser,
                );
            },
            Message::Text(msg) => {
                let Some(client_connection) = state
                    .connection_table
                    .lock()
                    .unwrap()
                    .get_client_os_api(&web_client_id)
                    .cloned()
                else {
                    log::error!("Unknown web_client_id: {}", web_client_id);
                    continue;
                };
                parse_stdin(
                    msg.as_bytes(),
                    client_connection.clone(),
                    &mut mouse_old_event,
                    explicitly_disable_kitty_keyboard_protocol,
                    &mut kitty_parser,
                    &mut input_parser,
                );
            },
            Message::Ping(payload) => {
                let _ = terminal_protocol_tx.send(Message::Pong(payload));
            },
            Message::Pong(_) => {},
            Message::Close(_) => {
                log::info!(
                    "Terminal WebSocket closed for client '{}' - removing from connection table",
                    web_client_id
                );
                state
                    .connection_table
                    .lock()
                    .unwrap()
                    .remove_client(&web_client_id);
                break;
            },
        }
    }
    os_input.send_to_server(ClientToServerMsg::ClientExited);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_timeout_defaults_to_45_seconds() {
        assert!(!heartbeat_timed_out(None, 45, 1));
        assert!(heartbeat_timed_out(None, 47, 1));
    }

    #[test]
    fn heartbeat_timeout_can_be_disabled_with_zero() {
        assert!(!heartbeat_timed_out(Some(0), 10_000, 1));
    }

    #[test]
    fn heartbeat_timeout_uses_custom_value_when_configured() {
        assert!(!heartbeat_timed_out(Some(120), 200, 100));
        assert!(heartbeat_timed_out(Some(120), 221, 100));
    }

    #[test]
    fn sleep_detection_threshold_exceeds_normal_timeout() {
        // 90s gap (HEARTBEAT_INTERVAL_SECS * 3) should be detected as sleep,
        // not as a timeout. The sleep detection check runs before the timeout
        // check in the heartbeat loop.
        let gap = HEARTBEAT_INTERVAL_SECS * SLEEP_DETECTION_MULTIPLIER;
        assert!(
            gap > DEFAULT_HEARTBEAT_TIMEOUT_SECS,
            "sleep detection threshold ({gap}s) must exceed default timeout ({}s)",
            DEFAULT_HEARTBEAT_TIMEOUT_SECS
        );
    }

    #[test]
    fn normal_heartbeat_gap_is_not_detected_as_sleep() {
        // A gap of 1 interval (30s) is normal operation.
        let elapsed = HEARTBEAT_INTERVAL_SECS;
        assert!(
            elapsed <= HEARTBEAT_INTERVAL_SECS * SLEEP_DETECTION_MULTIPLIER,
            "normal heartbeat gap should not trigger sleep detection"
        );
    }

    #[test]
    fn large_gap_triggers_sleep_detection() {
        // 1800s gap (30 min sleep) should be detected.
        let elapsed: u64 = 1800;
        assert!(
            elapsed > HEARTBEAT_INTERVAL_SECS * SLEEP_DETECTION_MULTIPLIER,
            "30 minute gap should trigger sleep detection"
        );
    }
}
