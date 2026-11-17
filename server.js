const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("RadioLink Global Audio Relay Running\n");
});

const wss = new WebSocketServer({ server });
const channels = new Map(); // channelId -> Set<WebSocket>

function broadcastPeerCount(ch) {
  const room = channels.get(ch);
  if (!room) return;
  const msg = JSON.stringify({ type: "peers", channel: ch, count: room.size });
  for (const client of room) {
    if (client.readyState === 1) client.send(msg);
  }
}

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  let currentChannel = parseInt(url.searchParams.get("ch") || "1", 10);
  ws.isAlive = true;

  const joinChannel = (ch) => {
    if (channels.has(currentChannel)) {
      channels.get(currentChannel).delete(ws);
      broadcastPeerCount(currentChannel);
    }
    currentChannel = ch;
    if (!channels.has(ch)) channels.set(ch, new Set());
    channels.get(ch).add(ws);
    broadcastPeerCount(ch);
  };

  joinChannel(currentChannel);

  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (data, isBinary) => {
    if (!isBinary) {
      try {
        const cmd = JSON.parse(data.toString());
        if (cmd.type === "switch" && cmd.channel) {
          joinChannel(Number(cmd.channel));
        }
      } catch (_) {}
      return;
    }
    // অডিও প্যাকেট একই চ্যানেলের বাকি সবার কাছে পাঠানো (নিজের কাছে ফেরত নয়)
    const room = channels.get(currentChannel);
    if (!room) return;
    for (const peer of room) {
      if (peer !== ws && peer.readyState === 1) {
        peer.send(data, { binary: true });
      }
    }
  });

  ws.on("close", () => {
    const room = channels.get(currentChannel);
    if (room) {
      room.delete(ws);
      broadcastPeerCount(currentChannel);
      if (room.size === 0) channels.delete(currentChannel);
    }
  });
});

// 4G/5G সংযোগ সচল রাখার জন্য হার্টবিট
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 20000);

server.listen(PORT, () => console.log(`RadioLink Relay listening on ${PORT}`));
