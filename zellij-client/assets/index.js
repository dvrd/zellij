import { initConnectionHandlers } from './connection.js';
import { initAuthentication } from './auth.js';
import { initTerminal } from './terminal.js';
import { setupInputHandlers } from './input.js';
import { initWebSockets } from './websockets.js';

document.addEventListener("DOMContentLoaded", async (event) => {
    const webClientId = await initAuthentication();

    const { term, fitAddon } = initTerminal();
    const sessionName = location.pathname.split("/").pop();

    let sendAnsiKey = (ansiKey) => {
        // This will be replaced by the WebSocket module
    };

    setupInputHandlers(term, sendAnsiKey);

    document.title = sessionName;
    const ws = initWebSockets(webClientId, sessionName, term, fitAddon, sendAnsiKey);

    // Initialize connection handlers with access to live WebSocket
    // instances so that visibilitychange can check socket liveness.
    initConnectionHandlers(() => ({
        wsTerminal: ws.wsTerminal,
        wsControl: ws.getWsControl(),
    }));
});
