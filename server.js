const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT || 10000);
const CONFIG_PATH = path.join(__dirname, 'app-config.json');
const WebSocketServer = WebSocket.WebSocketServer || WebSocket.Server;

function readAppConfigRaw() {
  try {
    return fs.readFileSync(CONFIG_PATH, 'utf8');
  } catch {
    return null;
  }
}

function getMinVersionCode() {
  const envMin = Number(process.env.MIN_APP_VERSION || 0);
  let cfgMin = 1;
  try {
    const raw = readAppConfigRaw();
    if (raw) {
      cfgMin = Number(JSON.parse(raw).min_version_code || 1);
    }
  } catch {}
  return Math.max(1, Number.isFinite(envMin) ? envMin : 0, Number.isFinite(cfgMin) ? cfgMin : 1);
}

const server = http.createServer((req, res) => {
  const reqPath = (req.url || '/').split('?')[0];

  if (reqPath === '/app-config.json') {
    const raw = readAppConfigRaw();
    if (!raw) {
      res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
      return res.end(JSON.stringify({ error: 'app-config.json not found' }));
    }
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'no-store'
    });
    return res.end(raw);
  }

  if (reqPath === '/' || reqPath === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    return res.end(
      JSON.stringify({
        status: 'ok',
        service: 'RadioLink Relay Server',
        min_version_code: getMinVersionCode(),
        connected_clients: wss.clients.size
      })
    );
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not Found');
});

const wss = new WebSocketServer({ server });

function broadcastPeerCount(channelId) {
  let count = 0;
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN && client.channelId === channelId) {
      count++;
    }
  }
  const payload = JSON.stringify({
    type: 'peers',
    channel: channelId,
    count: Math.max(1, count)
  });
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN && client.channelId === channelId) {
      try {
        client.send(payload);
      } catch {}
    }
  }
}

function rejectOutdatedClient(ws, minVersion) {
  try {
    ws.send(
      JSON.stringify({
        type: 'upgrade_required',
        min_version_code: minVersion
      })
    );
  } catch {}
  setTimeout(() => {
    try {
      ws.close(4002, 'UPGRADE_REQUIRED');
    } catch {}
  }, 120);
}

wss.on('connection', (ws, req) => {
  let channelId = 1;
  let clientVersion = 0;

  try {
    const parsedUrl = new URL(req.url || '/', 'https://localhost');
    const ch = Number(parsedUrl.searchParams.get('ch') || 1);
    if (Number.isInteger(ch) && ch >= 1 && ch <= 999) {
      channelId = ch;
    }
    const queryVer = Number(parsedUrl.searchParams.get('v') || 0);
    const headerVer = Number(req.headers['x-radiolink-version'] || 0);
    clientVersion = Math.max(
      Number.isFinite(queryVer) ? queryVer : 0,
      Number.isFinite(headerVer) ? headerVer : 0
    );
  } catch {}

  const minVersion = getMinVersionCode();
  if (minVersion > 1 && clientVersion < minVersion) {
    rejectOutdatedClient(ws, minVersion);
    return;
  }

  ws.channelId = channelId;
  ws.clientVersion = clientVersion;
  ws.isAlive = true;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  broadcastPeerCount(ws.channelId);

  ws.on('message', (data, isBinary) => {
    const binaryFrame = typeof isBinary === 'boolean' ? isBinary : Buffer.isBuffer(data);

    if (!binaryFrame) {
      try {
        const msg = JSON.parse(data.toString('utf8'));
        if (msg && msg.type === 'switch') {
          const msgVer = Number(msg.version || 0);
          if (Number.isFinite(msgVer) && msgVer > 0) {
            ws.clientVersion = msgVer;
          }
          const currentMin = getMinVersionCode();
          if (currentMin > 1 && ws.clientVersion < currentMin) {
            rejectOutdatedClient(ws, currentMin);
            return;
          }
          const nextChannel = Number(msg.channel);
          if (Number.isInteger(nextChannel) && nextChannel >= 1 && nextChannel <= 999) {
            const prevChannel = ws.channelId;
            if (prevChannel !== nextChannel) {
              ws.channelId = nextChannel;
              broadcastPeerCount(prevChannel);
              broadcastPeerCount(nextChannel);
            }
          }
        }
      } catch {}
      return;
    }

    // বাইনারি ওয়াকি-টকি ভয়েস ও কন্ট্রোল প্যাকেট একই চ্যানেলের অন্য ইউজারদের কাছে পাঠানো
    for (const client of wss.clients) {
      if (
        client !== ws &&
        client.readyState === WebSocket.OPEN &&
        client.channelId === ws.channelId
      ) {
        try {
          client.send(data, { binary: true });
        } catch {}
      }
    }
  });

  ws.on('close', () => {
    broadcastPeerCount(ws.channelId);
  });

  ws.on('error', () => {
    broadcastPeerCount(ws.channelId);
  });
});

// নিষ্ক্রিয় বা নেট-বিচ্ছিন্ন কানেকশন পরিষ্কার রাখা
const heartbeatTimer = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      const ch = ws.channelId;
      try {
        ws.terminate();
      } catch {}
      broadcastPeerCount(ch);
      continue;
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch {}
  }
}, 25000);

wss.on('close', () => {
  clearInterval(heartbeatTimer);
});

server.listen(PORT, () => {
  console.log(`RadioLink Relay Server listening on port ${PORT}`);
});
