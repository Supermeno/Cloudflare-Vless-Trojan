// =========================================================================
// PURE STANDALONE VLESS OVER WEBSOCKET WORKER (ULTRA-LIGHTWEIGHT & STABLE)
// Compatible with Cloudflare Workers & Cloudflare Pages (Functions)
// Supported Ports: 443 (TLS), 80, 8080 (No-TLS)
// =========================================================================

import { connect } from "cloudflare:sockets";

// DEFAULT CONFIGURATION
const DEFAULT_UUID = "";
const DEFAULT_WS_PATH = "galaxy-vless";
const DEFAULT_PROXYIP = "cdn-b100.xn--b6gac.eu.org";

// Fast Static Fallback Clean IPs (No GitHub fetch overhead)
const BACKUP_PROXY_POOL = [
    "cdn-b100.xn--b6gac.eu.org",
    "cdn.xn--b6gac.eu.org",
    "cdn-all.xn--b6gac.eu.org",
    "bpb.yousef.isegaro.com",
    "icook.hk",
    "icook.tw",
    "www.visa.com.sg"
];

export default {
    async fetch(request, env, ctx) {
        const url = new URL(request.url);
        const host = request.headers.get("Host") || url.host;

        const userID = (env.UUID || DEFAULT_UUID).trim().toLowerCase();
        const wsPath = (env.WS_PATH || DEFAULT_WS_PATH).replace(/^\/+/, "");
        const proxyIP = env.PROXYIP || DEFAULT_PROXYIP;

        // 1. WebSocket Proxy Upgrade
        const upgradeHeader = request.headers.get("Upgrade");
        if (upgradeHeader === "websocket") {
            return await handleVlessWS(request, userID, proxyIP);
        }

        // 2. Health check endpoint
        if (url.pathname === "/api/health") {
            return new Response(JSON.stringify({ status: "ok", protocol: "VLESS", wsPath }), {
                headers: { "Content-Type": "application/json" }
            });
        }

        // 3. Simple Camouflage / Direct Node Info Page
        return new Response(renderVlessPage(host, userID, wsPath, proxyIP), {
            headers: { "Content-Type": "text/html; charset=utf-8" }
        });
    }
};

// =========================================================================
// CORE VLESS WEBSOCKET HANDLER
// =========================================================================
async function handleVlessWS(request, userID, fallbackProxy) {
    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);
    server.accept();

    const earlyDataHeader = request.headers.get("sec-websocket-protocol") || "";
    const clientStream = makeReadableWSStream(server, earlyDataHeader);

    let remoteSocketWrapper = { value: null };

    clientStream.pipeTo(new WritableStream({
        async write(chunk) {
            // Already connected to remote target
            if (remoteSocketWrapper.value) {
                const writer = remoteSocketWrapper.value.writable.getWriter();
                await writer.write(chunk);
                writer.releaseLock();
                return;
            }

            // Parse VLESS Header
            const parsed = parseVlessHeader(chunk, userID);
            if (parsed.hasError) {
                server.close();
                return;
            }

            const { addressRemote, portRemote, rawDataIndex, responseHeader } = parsed;
            const rawClientData = chunk.slice(rawDataIndex);

            // Establish TCP Outbound
            handleTCPOutbound(remoteSocketWrapper, addressRemote, portRemote, rawClientData, server, responseHeader, fallbackProxy);
        },
        close() {},
        abort() {}
    })).catch(() => safeCloseWS(server));

    return new Response(null, { status: 101, webSocket: client });
}

// =========================================================================
// VLESS PROTOCOL PARSER
// =========================================================================
function parseVlessHeader(buffer, expectedUUID) {
    if (!buffer || buffer.byteLength < 24) {
        return { hasError: true, message: "Buffer too short" };
    }

    const uint8 = new Uint8Array(buffer);
    const version = uint8[0];
    const clientUUID = unsafeStringify(uint8.slice(1, 17));

    if (clientUUID !== expectedUUID) {
        return { hasError: true, message: "Unauthorized UUID" };
    }

    const optLength = uint8[17];
    const command = uint8[18 + optLength]; // 1: TCP, 2: UDP
    const portIndex = 18 + optLength + 1;
    const portRemote = new DataView(uint8.buffer, uint8.byteOffset + portIndex, 2).getUint16(0);

    const addressIndex = portIndex + 2;
    const addressType = uint8[addressIndex];

    let addressLength = 0;
    let addressValueIndex = addressIndex + 1;
    let addressValue = "";

    switch (addressType) {
        case 1: // IPv4
            addressLength = 4;
            addressValue = new Uint8Array(uint8.slice(addressValueIndex, addressValueIndex + addressLength)).join(".");
            break;
        case 2: // Domain
            addressLength = uint8[addressValueIndex];
            addressValueIndex += 1;
            addressValue = new TextDecoder().decode(uint8.slice(addressValueIndex, addressValueIndex + addressLength));
            break;
        case 3: // IPv6
            addressLength = 16;
            const dv = new DataView(uint8.buffer, uint8.byteOffset + addressValueIndex, addressLength);
            const ipv6 = [];
            for (let i = 0; i < 8; i++) ipv6.push(dv.getUint16(i * 2).toString(16));
            addressValue = ipv6.join(":");
            break;
        default:
            return { hasError: true, message: `Unsupported address type: ${addressType}` };
    }

    return {
        hasError: false,
        addressRemote: addressValue,
        portRemote,
        rawDataIndex: addressValueIndex + addressLength,
        responseHeader: new Uint8Array([version, 0])
    };
}

// =========================================================================
// ZERO-COPY TCP OUTBOUND & STREAM PIPING
// =========================================================================
async function handleTCPOutbound(socketWrapper, targetHost, targetPort, rawData, wsClient, responseHeader, fallbackProxy) {
    async function directConnect(host, port) {
        const socket = connect({ hostname: host, port: Number(port) });
        socketWrapper.value = socket;
        const writer = socket.writable.getWriter();
        if (rawData && rawData.byteLength > 0) {
            await writer.write(rawData);
        }
        writer.releaseLock();
        return socket;
    }

    async function retryWithProxy() {
        const proxy = fallbackProxy || BACKUP_PROXY_POOL[Math.floor(Math.random() * BACKUP_PROXY_POOL.length)];
        try {
            const socket = await directConnect(proxy, targetPort);
            pipeRemoteToWS(socket, wsClient, responseHeader, null);
        } catch {
            safeCloseWS(wsClient);
        }
    }

    try {
        const socket = await directConnect(targetHost, targetPort);
        pipeRemoteToWS(socket, wsClient, responseHeader, retryWithProxy);
    } catch {
        await retryWithProxy();
    }
}

async function pipeRemoteToWS(remoteSocket, wsClient, responseHeader, retry) {
    let header = responseHeader;
    let hasData = false;

    await remoteSocket.readable.pipeTo(new WritableStream({
        write(chunk, controller) {
            hasData = true;
            if (wsClient.readyState !== 1) {
                controller.error("WebSocket closed");
                return;
            }
            if (header) {
                const chunkBytes = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
                const merged = new Uint8Array(header.length + chunkBytes.length);
                merged.set(header, 0);
                merged.set(chunkBytes, header.length);
                wsClient.send(merged.buffer);
                header = null;
            } else {
                wsClient.send(chunk);
            }
        },
        close() {},
        abort() {}
    })).catch(() => safeCloseWS(wsClient));

    if (!hasData && retry) {
        retry();
    }
}

function makeReadableWSStream(serverWS, earlyDataHeader) {
    return new ReadableStream({
        start(controller) {
            serverWS.addEventListener("message", (e) => controller.enqueue(e.data));
            serverWS.addEventListener("close", () => {
                safeCloseWS(serverWS);
                controller.close();
            });
            serverWS.addEventListener("error", (err) => controller.error(err));

            if (earlyDataHeader) {
                try {
                    const raw = atob(earlyDataHeader.replace(/-/g, "+").replace(/_/g, "/"));
                    const buf = Uint8Array.from(raw, c => c.charCodeAt(0));
                    controller.enqueue(buf.buffer);
                } catch {}
            }
        },
        cancel() { safeCloseWS(serverWS); }
    });
}

function safeCloseWS(ws) {
    try {
        if (ws && (ws.readyState === 1 || ws.readyState === 2)) ws.close();
    } catch {}
}

const byteToHex = Array.from({ length: 256 }, (_, i) => (i + 256).toString(16).slice(1));
function unsafeStringify(arr) {
    return (
        byteToHex[arr[0]] + byteToHex[arr[1]] + byteToHex[arr[2]] + byteToHex[arr[3]] + "-" +
        byteToHex[arr[4]] + byteToHex[arr[5]] + "-" +
        byteToHex[arr[6]] + byteToHex[arr[7]] + "-" +
        byteToHex[arr[8]] + byteToHex[arr[9]] + "-" +
        byteToHex[arr[10]] + byteToHex[arr[11]] + byteToHex[arr[12]] + byteToHex[arr[13]] + byteToHex[arr[14]] + byteToHex[arr[15]]
    ).toLowerCase();
}

function renderVlessPage(host, uuid, wsPath, proxyIP) {
    const vlessTls = `vless://${uuid}@${host}:443?encryption=none&security=tls&sni=${host}&type=ws&host=${host}&path=%2F${encodeURIComponent(wsPath)}%3Fed%3D2048#VLESS-TLS-443`;
    const vlessHttp80 = `vless://${uuid}@${host}:80?encryption=none&security=none&type=ws&host=${host}&path=%2F${encodeURIComponent(wsPath)}%3Fed%3D2048#VLESS-NoTLS-80`;
    const vlessHttp8080 = `vless://${uuid}@${host}:8080?encryption=none&security=none&type=ws&host=${host}&path=%2F${encodeURIComponent(wsPath)}%3Fed%3D2048#VLESS-NoTLS-8080`;

    return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><title>VLESS Standalone Node</title>
<style>
body{font-family:system-ui,sans-serif;background:#090d16;color:#e2e8f0;padding:24px;display:flex;justify-content:center;}
.box{max-width:640px;width:100%;background:#111827;border:1px solid #1f2937;border-radius:12px;padding:20px;}
h2{color:#38bdf8;font-size:18px;margin-bottom:14px;}
.node{background:#030712;border:1px solid #374151;border-radius:8px;padding:12px;margin-bottom:12px;font-family:monospace;font-size:12px;word-break:break-all;color:#94a3b8;}
button{background:#0284c7;color:#fff;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-weight:600;font-size:12px;margin-top:8px;}
button:hover{background:#0369a1;}
</style>
</head>
<body>
<div class="box">
  <h2>⚡ Standalone VLESS (TLS 443 & No-TLS 80/8080)</h2>
  <div style="font-size:13px;color:#64748b;margin-bottom:16px;">Pure VLESS Core Engine without background subrequests.</div>
  
  <div class="node">
    <strong>Port 443 (TLS):</strong><br>${vlessTls}<br>
    <button onclick="navigator.clipboard.writeText('${vlessTls}')">Copy TLS Node</button>
  </div>
  <div class="node">
    <strong>Port 80 (No-TLS):</strong><br>${vlessHttp80}<br>
    <button onclick="navigator.clipboard.writeText('${vlessHttp80}')">Copy Port 80 Node</button>
  </div>
  <div class="node">
    <strong>Port 8080 (No-TLS):</strong><br>${vlessHttp8080}<br>
    <button onclick="navigator.clipboard.writeText('${vlessHttp8080}')">Copy Port 8080 Node</button>
  </div>
</div>
</body>
</html>`;
}
