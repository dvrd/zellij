use crate::os_input_output::ClientOsApi;
use crate::web_client::control_message::{SetConfigPayload, WebServerToWebClientControlMessage};
use crate::web_client::host_query_seed::build_host_query_seed_msgs;
use crate::web_client::session_management::{
    build_initial_connection, create_first_message, create_ipc_pipe,
};
use crate::web_client::types::{ClientConnectionBus, ConnectionTable, SessionManager};
use crate::web_client::utils::terminal_init_messages;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use zellij_utils::{
    cli::CliArgs,
    data::Style,
    input::{config::Config, options::Options},
    ipc::{ClientToServerMsg, ExitReason, ServerToClientMsg},
    sessions::generate_unique_session_name,
    setup::Setup,
};

pub fn zellij_server_listener(
    os_input: Box<dyn ClientOsApi>,
    connection_table: Arc<Mutex<ConnectionTable>>,
    session_name: Option<String>,
    mut config: Config,
    mut config_options: Options,
    config_file_path: Option<PathBuf>,
    web_client_id: String,
    session_manager: Arc<dyn SessionManager>,
    attachment_complete_tx: Option<tokio::sync::oneshot::Sender<()>>,
    is_cli_client: bool,
) {
    let _server_listener_thread = std::thread::Builder::new()
        .name("server_listener".to_string())
        .spawn({
            move || {
                let mut client_connection_bus =
                    ClientConnectionBus::new(&web_client_id, &connection_table);
                let mut reconnect_to_session =
                    match build_initial_connection(session_name, &config) {
                        Ok(initial_session_connection) => initial_session_connection,
                        Err(e) => {
                            log::error!("Failed to build initial connection: {} - disconnecting client", e);
                            return;
                        },
                    };
                let mut attachment_complete_tx = attachment_complete_tx;
                'reconnect_loop: loop {
                    let reconnect_info = reconnect_to_session.take();
                    let initial_layout = reconnect_info.as_ref().and_then(|r| r.layout.clone());
                    let path = {
                        let Some(session_name) = reconnect_info
                            .as_ref()
                            .and_then(|r| r.name.clone())
                            .or_else(generate_unique_session_name)
                        else {
                            log::error!("Failed to generate unique session name - disconnecting client");
                            client_connection_bus.send_stdout(format!(
                                "\u{1b}[2J\n\r\u{1b}[1;31mError: Failed to generate unique session name\u{1b}[0m\n"
                            ));
                            client_connection_bus.close_connection();
                            return;
                        };
                        let mut sock_dir = zellij_utils::consts::ZELLIJ_SOCK_DIR.clone();
                        if let Err(e) = zellij_utils::sessions::validate_session_name(&session_name) {
                            log::error!("Invalid session name '{}': {} - disconnecting client", session_name, e);
                            client_connection_bus.send_stdout(format!(
                                "\u{1b}[2J\n\r\u{1b}[1;31mError: Invalid session name '{}'\u{1b}[0m\n\n{}",
                                session_name, e
                            ));
                            client_connection_bus.close_connection();
                            return;
                        }
                        sock_dir.push(session_name.clone());
                        sock_dir.to_str().unwrap().to_owned()
                    };

                    reload_config_from_disk(&mut config, &mut config_options, &config_file_path);

                    let full_screen_ws = os_input.get_terminal_size();
                    let mut sent_init_messages = false;

                    let palette = config
                        .theme_config(config_options.theme.as_ref())
                        .unwrap_or_else(|| os_input.load_palette().into());
                    let client_attributes = zellij_utils::ipc::ClientAttributes {
                        size: full_screen_ws,
                        style: Style {
                            colors: palette,
                            rounded_corners: config.ui.pane_frames.rounded_corners,
                            hide_session_name: config.ui.pane_frames.hide_session_name,
                        },
                    };

                    let session_name = PathBuf::from(path.clone())
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_owned();

                    // Look up read-only status from connection table
                    let is_read_only = connection_table
                        .lock()
                        .unwrap()
                        .is_client_read_only(&web_client_id);


                    let session_exists = session_manager.session_exists(&session_name).unwrap_or(false);

                    if is_read_only && !session_exists {
                        log::error!("Read-only token attempted to create new session '{}' - disconnecting client", session_name);
                        client_connection_bus.send_stdout(format!(
                            "\u{1b}[2J\n\r\u{1b}[1;33m╔════════════════════════════════════════════════════════════════╗\u{1b}[0m\n\
                            \u{1b}[1;33m║\u{1b}[0m  \u{1b}[1mPermission Denied\u{1b}[0m                                              \u{1b}[1;33m║\u{1b}[0m\n\
                            \u{1b}[1;33m╠════════════════════════════════════════════════════════════════╣\u{1b}[0m\n\
                            \u{1b}[1;33m║\u{1b}[0m  Read-only tokens cannot create new sessions.                  \u{1b}[1;33m║\u{1b}[0m\n\
                            \u{1b}[1;33m║\u{1b}[0m  Please use a full access token or connect to an existing     \u{1b}[1;33m║\u{1b}[0m\n\
                            \u{1b}[1;33m║\u{1b}[0m  session.                                                      \u{1b}[1;33m║\u{1b}[0m\n\
                            \u{1b}[1;33m╚════════════════════════════════════════════════════════════════╝\u{1b}[0m\n"
                        ));
                        client_connection_bus.close_connection();
                        return;
                    }

                    let should_create_new_session = !session_exists;
                    let first_message = create_first_message(is_read_only, config_file_path.clone(), client_attributes.clone(), config_options.clone(), should_create_new_session, &session_name, initial_layout);
                    let zellij_ipc_pipe = create_ipc_pipe(&session_name);

                    if let Err(e) = session_manager.spawn_session_if_needed(
                        &session_name,
                        os_input.clone(),
                        session_exists,
                        &zellij_ipc_pipe,
                        first_message,
                    ) {
                        log::error!("Failed to start session '{}': {} - disconnecting client", session_name, e);
                        client_connection_bus.send_stdout(format!(
                            "\u{1b}[2J\n\r\u{1b}[1;31m╔════════════════════════════════════════════════════════════════╗\u{1b}[0m\n\
                            \u{1b}[1;31m║\u{1b}[0m  \u{1b}[1mFailed to Start Session\u{1b}[0m                                        \u{1b}[1;31m║\u{1b}[0m\n\
                            \u{1b}[1;31m╠════════════════════════════════════════════════════════════════╣\u{1b}[0m\n\
                            \u{1b}[1;31m║\u{1b}[0m  Could not start session '{}':                              \u{1b}[1;31m║\u{1b}[0m\n\
                            \u{1b}[1;31m║\u{1b}[0m  {}                                                            \u{1b}[1;31m║\u{1b}[0m\n\
                            \u{1b}[1;31m╚════════════════════════════════════════════════════════════════╝\u{1b}[0m\n",
                            session_name, e
                        ));
                        client_connection_bus.close_connection();
                        return;
                    }

                    // Seed the server's host-terminal-query cache with web
                    // client state derived from Config (fg/bg/palette).
                    // Without this, OSC 10/11/4 queries from apps
                    // stall on the 1s server forward timeout and then come
                    // back empty. Pixel dimensions are seeded separately
                    // via the TerminalMetrics control message once the
                    // browser has reported them.
                    for seed in build_host_query_seed_msgs(&config, &config_options) {
                        os_input.send_to_server(seed);
                    }

                    if let Some(tx) = attachment_complete_tx.take() {
                        let _ = tx.send(());
                    }

                    client_connection_bus.send_control(
                        WebServerToWebClientControlMessage::SwitchedSession {
                            new_session_name: session_name.clone(),
                        },
                    );

                    let mut unknown_message_count = 0;
                    loop {
                        let msg = os_input.recv_from_server();
                        if msg.is_some() {
                            unknown_message_count = 0;
                        } else {
                            unknown_message_count += 1;
                        }
                        match msg.map(|m| m.0) {
                            Some(ServerToClientMsg::UnblockInputThread) => {},
                            Some(ServerToClientMsg::Connected) => {},
                            Some(ServerToClientMsg::CliPipeOutput { .. } ) => {},
                            Some(ServerToClientMsg::UnblockCliPipeInput { .. } ) => {},
                            Some(ServerToClientMsg::StartWebServer { .. } ) => {},
                            Some(ServerToClientMsg::Exit{exit_reason}) => {
                                handle_exit_reason(&mut client_connection_bus, exit_reason);
                                os_input.send_to_server(ClientToServerMsg::ClientExited);
                                break;
                            },
                            Some(ServerToClientMsg::Render{content: bytes}) => {
                                if !sent_init_messages {
                                    // CLI clients (e.g. `zellij attach https://…`) set up
                                    // their own outer terminal in start_remote_client().
                                    // Sending browser-only init sequences (alternate screen
                                    // push, mouse-mode enable, Kitty mode push) would cause
                                    // double-initialisation and corrupt the terminal state,
                                    // leading to broken keyboard escape sequences.
                                    if !is_cli_client {
                                        for message in terminal_init_messages() {
                                            client_connection_bus.send_stdout(message.to_owned())
                                        }
                                    }
                                    sent_init_messages = true;
                                }
                                client_connection_bus.send_stdout(bytes);
                            },
                            Some(ServerToClientMsg::SwitchSession{connect_to_session}) => {
                                reconnect_to_session = Some(connect_to_session);
                                continue 'reconnect_loop;
                            },
                            Some(ServerToClientMsg::QueryTerminalSize) => {
                                client_connection_bus.send_control(
                                    WebServerToWebClientControlMessage::QueryTerminalSize,
                                );
                            },
                            Some(ServerToClientMsg::Log{lines}) => {
                                client_connection_bus.send_control(
                                    WebServerToWebClientControlMessage::Log { lines },
                                );
                            },
                            Some(ServerToClientMsg::LogError{lines}) => {
                                client_connection_bus.send_control(
                                    WebServerToWebClientControlMessage::LogError { lines },
                                );
                            },
                            Some(ServerToClientMsg::RenamedSession{name: new_session_name}) => {
                                client_connection_bus.send_control(
                                    WebServerToWebClientControlMessage::SwitchedSession {
                                        new_session_name,
                                    },
                                );
                            },
                            Some(ServerToClientMsg::ConfigFileUpdated) => {

                                if let Some(config_file_path) = &config_file_path {
                                    if let Ok(new_config) = Config::from_path(&config_file_path, Some(config.clone())) {
                                        // Re-seed host-query cache for this client
                                        // so OSC 10/11/4 replies follow the new theme.
                                        for seed in build_host_query_seed_msgs(&new_config, &config_options) {
                                            os_input.send_to_server(seed);
                                        }
                                        let set_config_payload = SetConfigPayload::from(&new_config);

                                        let client_ids: Vec<String> = {
                                            let connection_table_lock = connection_table.lock().unwrap();
                                            connection_table_lock
                                                .client_id_to_channels
                                                .keys()
                                                .cloned()
                                                .collect()
                                        };

                                        let config_message =
                                            WebServerToWebClientControlMessage::SetConfig(set_config_payload);
                                        let config_msg_json = match serde_json::to_string(&config_message) {
                                            Ok(json) => json,
                                            Err(e) => {
                                                log::error!("Failed to serialize config message: {}", e);
                                                continue;
                                            },
                                        };

                                        for client_id in client_ids {
                                            if let Some(control_tx) = connection_table
                                                .lock()
                                                .unwrap()
                                                .get_client_control_tx(&client_id)
                                            {
                                                let ws_message = config_msg_json.clone();
                                                match control_tx.send(ws_message.into()) {
                                                    Ok(_) => {}, // no-op
                                                    Err(e) => {
                                                        log::error!(
                                                            "Failed to send config update to client {}: {}",
                                                            client_id,
                                                            e
                                                        );
                                                    },
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            // Subscribe-only messages — not relevant for web clients
                            Some(ServerToClientMsg::PaneRenderUpdate { .. }) => {},
                            Some(ServerToClientMsg::SubscribedPaneClosed { .. }) => {},
                            Some(ServerToClientMsg::ForwardQueryToHost { token, .. }) => {
                                // Reply immediately with empty reply_bytes.
                                // This is the existing convention that signals
                                // "no host reply available — please synthesize
                                // from cached state". The server's
                                // synthesize_cached_reply path will use the
                                // pixel dimensions and colors we have already
                                // seeded from the browser/config, returning a
                                // real answer rather than waiting for the
                                // 1000ms forward timeout.
                                os_input.send_to_server(
                                    ClientToServerMsg::ForwardedReplyFromHost {
                                        token,
                                        reply_bytes: Vec::new(),
                                    },
                                );
                            },
                            None => {
                                if unknown_message_count >= 1000 {
                                    log::error!("Received more than 1000 consecutive unknown server messages from session '{}' - disconnecting client to prevent CPU spike", session_name);
                                    client_connection_bus.send_stdout(format!(
                                        "\u{1b}[2J\n\r\u{1b}[1;31m╔════════════════════════════════════════════════════════════════╗\u{1b}[0m\n\
                                        \u{1b}[1;31m║\u{1b}[0m  \u{1b}[1mConnection Error\u{1b}[0m                                               \u{1b}[1;31m║\u{1b}[0m\n\
                                        \u{1b}[1;31m╠════════════════════════════════════════════════════════════════╣\u{1b}[0m\n\
                                        \u{1b}[1;31m║\u{1b}[0m  Received invalid data from server.                           \u{1b}[1;31m║\u{1b}[0m\n\
                                        \u{1b}[1;31m║\u{1b}[0m  The connection has been closed to prevent system overload.   \u{1b}[1;31m║\u{1b}[0m\n\
                                        \u{1b}[1;31m╚════════════════════════════════════════════════════════════════╝\u{1b}[0m\n"
                                    ));
                                    // this probably means we're in an infinite loop, let's
                                    // disconnect so as not to cause 100% CPU
                                    break;
                                }
                            },
                        }
                    }
                    if reconnect_to_session.is_none() {
                        break;
                    }
                }
            }
        });
}

fn handle_exit_reason(client_connection_bus: &mut ClientConnectionBus, exit_reason: ExitReason) {
    match exit_reason {
        ExitReason::KickedByHost => {
            log::info!("Client disconnected: Kicked by host - another client forced disconnection");
            client_connection_bus.send_stdout(format!(
                "\u{1b}[2J\n\r\u{1b}[1;31m╔════════════════════════════════════════════════════════════════╗\u{1b}[0m\n\
                \u{1b}[1;31m║\u{1b}[0m  \u{1b}[1mDisconnected\u{1b}[0m                                                   \u{1b}[1;31m║\u{1b}[0m\n\
                \u{1b}[1;31m╠════════════════════════════════════════════════════════════════╣\u{1b}[0m\n\
                \u{1b}[1;31m║\u{1b}[0m  You were disconnected by another client (kicked by host).     \u{1b}[1;31m║\u{1b}[0m\n\
                \u{1b}[1;31m║\u{1b}[0m  Another user or process forced your disconnection.            \u{1b}[1;31m║\u{1b}[0m\n\
                \u{1b}[1;31m╚════════════════════════════════════════════════════════════════╝\u{1b}[0m\n"
            ));
            client_connection_bus.close_connection_kicked();
            return;
        },
        ExitReason::WebClientsForbidden => {
            log::info!("Client disconnected: Web clients are not allowed in this session");
            client_connection_bus.send_stdout(format!(
                "\u{1b}[2J\n\r\u{1b}[1;33m╔════════════════════════════════════════════════════════════════╗\u{1b}[0m\n\
                \u{1b}[1;33m║\u{1b}[0m  \u{1b}[1mAccess Denied\u{1b}[0m                                                  \u{1b}[1;33m║\u{1b}[0m\n\
                \u{1b}[1;33m╠════════════════════════════════════════════════════════════════╣\u{1b}[0m\n\
                \u{1b}[1;33m║\u{1b}[0m  Web clients are not allowed to attach to this session.        \u{1b}[1;33m║\u{1b}[0m\n\
                \u{1b}[1;33m║\u{1b}[0m  The session owner has disabled web access.                    \u{1b}[1;33m║\u{1b}[0m\n\
                \u{1b}[1;33m╚════════════════════════════════════════════════════════════════╝\u{1b}[0m\n"
            ));
        },
        ExitReason::Error(e) => {
            log::error!("Client disconnected due to server error: {}", e);
            let goto_start_of_last_line = format!("\u{1b}[{};{}H", 1, 1);
            let clear_client_terminal_attributes = "\u{1b}[?1l\u{1b}=\u{1b}[r\u{1b}[?1000l\u{1b}[?1002l\u{1b}[?1003l\u{1b}[?1005l\u{1b}[?1006l\u{1b}[?12l";
            let disable_mouse = "\u{1b}[?1006l\u{1b}[?1015l\u{1b}[?1003l\u{1b}[?1002l\u{1b}[?1000l";
            let error_message = format!(
                "\u{1b}[2J\n\r\u{1b}[1;31m╔════════════════════════════════════════════════════════════════╗\u{1b}[0m\n\
                \u{1b}[1;31m║\u{1b}[0m  \u{1b}[1mServer Error\u{1b}[0m                                                   \u{1b}[1;31m║\u{1b}[0m\n\
                \u{1b}[1;31m╠════════════════════════════════════════════════════════════════╣\u{1b}[0m\n\
                \u{1b}[1;31m║\u{1b}[0m  The server encountered an error and closed the connection.    \u{1b}[1;31m║\u{1b}[0m\n\
                \u{1b}[1;31m╚════════════════════════════════════════════════════════════════╝\u{1b}[0m\n\n\
                Error details:\n{}",
                e.to_string().replace("\n", "\n\r")
            );
            let error = format!(
                "{}{}\n\r{}\n",
                disable_mouse,
                clear_client_terminal_attributes,
                goto_start_of_last_line,
            );
            client_connection_bus.send_stdout(format!("{}{}", error, error_message));
        },
        ExitReason::Normal => {
            log::info!("Client disconnected: Session ended normally");
            client_connection_bus.send_stdout(format!(
                "\u{1b}[2J\n\r\u{1b}[1;32m╔════════════════════════════════════════════════════════════════╗\u{1b}[0m\n\
                \u{1b}[1;32m║\u{1b}[0m  \u{1b}[1mSession Ended\u{1b}[0m                                                  \u{1b}[1;32m║\u{1b}[0m\n\
                \u{1b}[1;32m╠════════════════════════════════════════════════════════════════╣\u{1b}[0m\n\
                \u{1b}[1;32m║\u{1b}[0m  The session has ended.                                       \u{1b}[1;32m║\u{1b}[0m\n\
                \u{1b}[1;32m║\u{1b}[0m  All panes and processes have been closed.                    \u{1b}[1;32m║\u{1b}[0m\n\
                \u{1b}[1;32m╚════════════════════════════════════════════════════════════════╝\u{1b}[0m\n"
            ));
        },
        ExitReason::NormalDetached => {
            log::info!("Client disconnected: Session detached normally");
            client_connection_bus.send_stdout(format!(
                "\u{1b}[2J\n\r\u{1b}[1;34m╔════════════════════════════════════════════════════════════════╗\u{1b}[0m\n\
                \u{1b}[1;34m║\u{1b}[0m  \u{1b}[1mSession Detached\u{1b}[0m                                               \u{1b}[1;34m║\u{1b}[0m\n\
                \u{1b}[1;34m╠════════════════════════════════════════════════════════════════╣\u{1b}[0m\n\
                \u{1b}[1;34m║\u{1b}[0m  You have been detached from the session.                     \u{1b}[1;34m║\u{1b}[0m\n\
                \u{1b}[1;34m║\u{1b}[0m  The session is still running and can be reattached.          \u{1b}[1;34m║\u{1b}[0m\n\
                \u{1b}[1;34m╚════════════════════════════════════════════════════════════════╝\u{1b}[0m\n"
            ));
        },
        ExitReason::ForceDetached => {
            log::info!("Client disconnected: Force detached by another client");
            client_connection_bus.send_stdout(format!(
                "\u{1b}[2J\n\r\u{1b}[1;33m╔════════════════════════════════════════════════════════════════╗\u{1b}[0m\n\
                \u{1b}[1;33m║\u{1b}[0m  \u{1b}[1mForce Disconnected\u{1b}[0m                                             \u{1b}[1;33m║\u{1b}[0m\n\
                \u{1b}[1;33m╠════════════════════════════════════════════════════════════════╣\u{1b}[0m\n\
                \u{1b}[1;33m║\u{1b}[0m  Your session was detached by another client.                 \u{1b}[1;33m║\u{1b}[0m\n\
                \u{1b}[1;33m║\u{1b}[0m  This can happen when another client connects with --force.   \u{1b}[1;33m║\u{1b}[0m\n\
                \u{1b}[1;33m╚════════════════════════════════════════════════════════════════╝\u{1b}[0m\n"
            ));
        },
        ExitReason::CannotAttach => {
            log::warn!("Client disconnected: Cannot attach - session already attached to another client");
            client_connection_bus.send_stdout(format!(
                "\u{1b}[2J\n\r\u{1b}[1;33m╔════════════════════════════════════════════════════════════════╗\u{1b}[0m\n\
                \u{1b}[1;33m║\u{1b}[0m  \u{1b}[1mCannot Attach\u{1b}[0m                                                  \u{1b}[1;33m║\u{1b}[0m\n\
                \u{1b}[1;33m╠════════════════════════════════════════════════════════════════╣\u{1b}[0m\n\
                \u{1b}[1;33m║\u{1b}[0m  This session is attached to another client.                  \u{1b}[1;33m║\u{1b}[0m\n\
                \u{1b}[1;33m║\u{1b}[0m  Use the --force flag to force connect.                       \u{1b}[1;33m║\u{1b}[0m\n\
                \u{1b}[1;33m╚════════════════════════════════════════════════════════════════╝\u{1b}[0m\n"
            ));
        },
        ExitReason::Disconnect => {
            log::warn!("Client disconnected: Buffer full - client processing too slow");
            client_connection_bus.send_stdout(format!(
                "\u{1b}[2J\n\r\u{1b}[1;31m╔════════════════════════════════════════════════════════════════╗\u{1b}[0m\n\
                \u{1b}[1;31m║\u{1b}[0m  \u{1b}[1mConnection Lost\u{1b}[0m                                                \u{1b}[1;31m║\u{1b}[0m\n\
                \u{1b}[1;31m╠════════════════════════════════════════════════════════════════╣\u{1b}[0m\n\
                \u{1b}[1;31m║\u{1b}[0m  Your client lost connection to the server.                   \u{1b}[1;31m║\u{1b}[0m\n\
                \u{1b}[1;31m║\u{1b}[0m  This usually happens when your terminal processes messages   \u{1b}[1;31m║\u{1b}[0m\n\
                \u{1b}[1;31m║\u{1b}[0m  too slowly (high system load or slow network).               \u{1b}[1;31m║\u{1b}[0m\n\
                \u{1b}[1;31m║\u{1b}[0m  Your session is still active. Try reconnecting.              \u{1b}[1;31m║\u{1b}[0m\n\
                \u{1b}[1;31m╚════════════════════════════════════════════════════════════════╝\u{1b}[0m\n"
            ));
        },
        ExitReason::CustomExitStatus(code) => {
            log::info!("Client disconnected with custom exit status: {}", code);
            client_connection_bus.send_stdout(format!(
                "\u{1b}[2J\n\r\u{1b}[1mSession ended with exit code: {}\u{1b}[0m\n", code
            ));
        },
    }
    client_connection_bus.close_connection();
}

fn reload_config_from_disk(
    config_without_layout: &mut Config,
    config_options_without_layout: &mut Options,
    config_file_path: &Option<PathBuf>,
) {
    let mut cli_args = CliArgs::default();
    cli_args.config = config_file_path.clone();
    match Setup::from_cli_args(&cli_args) {
        Ok((_, _, _, reloaded_config_without_layout, reloaded_config_options_without_layout)) => {
            *config_without_layout = reloaded_config_without_layout;
            *config_options_without_layout = reloaded_config_options_without_layout;
        },
        Err(e) => {
            log::error!("Failed to reload config: {}", e);
        },
    };
}
