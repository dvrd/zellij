/**
 * Terminal initialization and management
 */

import { build_link_handler } from "./links.js";

/**
 * Initialize the terminal with all required addons and configuration
 * @returns {object} Object containing term and fitAddon instances
 */
export function initTerminal() {
    const term = new Terminal({
        fontFamily: "Monospace",
        allowProposedApi: true,
        scrollback: 0,
    });
    // for debugging
    window.term = term;
    const fitAddon = new FitAddon.FitAddon();
    const clipboardAddon = new ClipboardAddon.ClipboardAddon();

    const { linkHandler, activateLink } = build_link_handler();
    const webLinksAddon = new WebLinksAddon.WebLinksAddon(
        activateLink,
        linkHandler
    );
    term.options.linkHandler = linkHandler;

    let webglAddon = new WebglAddon.WebglAddon();
    term.loadAddon(fitAddon);
    term.loadAddon(clipboardAddon);
    term.loadAddon(webLinksAddon);
    webglAddon.onContextLoss((e) => {
        // WebGL context is frequently lost after system sleep or GPU
        // resource pressure.  Dispose the old addon and try to create a
        // fresh one so the terminal keeps GPU-accelerated rendering.
        webglAddon.dispose();
        try {
            webglAddon = new WebglAddon.WebglAddon();
            webglAddon.onContextLoss(() => {
                // Second loss — fall back to canvas renderer permanently.
                webglAddon.dispose();
            });
            term.loadAddon(webglAddon);
        } catch (_) {
            // WebGL unavailable — canvas renderer will be used.
        }
    });
    term.loadAddon(webglAddon);
    term.open(document.getElementById("terminal"));
    fitAddon.fit();
    term.focus();
    return { term, fitAddon };
}
