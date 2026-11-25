// OxideX game server - WebSocket relay for Render.
// Max 10 players. Clients connect with wss://my-game-d6ay.onrender.com

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = parseInt(process.env.PORT || "10000", 10);
const MAX_PLAYERS = 10;
const SERVER_NAME = "OxideX Official";

const players = new Map(); // id -> { ws, name, state }
let nextId = 1;

// ---------- HTTP (browser test + Render health check) ----------
const httpServer = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ok");
    return;
  }
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ name: SERVER_NAME, players: players.size, max: MAX_PLAYERS }));
});

// ---------- WebSocket ----------
const wss = new WebSocketServer({ server: httpServer, maxPayload: 4096 });

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function broadcast(obj, exceptId) {
  const text = JSON.stringify(obj);
  for (const [id, p] of players) {
    if (id !== exceptId && p.ws.readyState === 1) p.ws.send(text);
  }
}

function cleanName(raw) {
  let n = String(raw || "Player").replace(/[^\w \-]/g, "").trim();
  if (n.length === 0) n = "Player";
  return n.slice(0, 16);
}

function numArray(a, len) {
  if (!Array.isArray(a) || a.length !== len) return null;
  const out = [];
  for (const v of a) {
    if (typeof v !== "number" || !Number.isFinite(v)) return null;
    out.push(v);
  }
  return out;
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.playerId = 0;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch (e) { return; }
    if (typeof msg !== "object" || msg === null) return;

    if (msg.t === "join" && ws.playerId === 0) {
      if (players.size >= MAX_PLAYERS) {
        send(ws, { t: "full" });
        ws.close(1000, "full");
        return;
      }
      const id = nextId++;
      ws.playerId = id;
      const name = cleanName(msg.name);
      players.set(id, { ws, name, state: null });

      const others = [];
      for (const [pid, p] of players) {
        if (pid !== id) others.push({ id: pid, name: p.name, state: p.state });
      }
      send(ws, { t: "welcome", id, name: SERVER_NAME, max: MAX_PLAYERS, players: others });
      broadcast({ t: "joined", id, name }, id);
      console.log(`+ ${name} (#${id}) joined. Players: ${players.size}/${MAX_PLAYERS}`);
      return;
    }

    if (ws.playerId === 0) return; // must join first
    const me = players.get(ws.playerId);
    if (!me) return;

    if (msg.t === "state") {
      const pos = numArray(msg.p, 3);
      const rot = numArray(msg.r, 2);
      if (!pos || !rot) return;
      me.state = { p: pos, r: rot, a: typeof msg.a === "number" ? msg.a : 0 };
      broadcast({ t: "state", id: ws.playerId, p: pos, r: rot, a: me.state.a }, ws.playerId);
    }
  });

  ws.on("close", () => {
    const id = ws.playerId;
    if (id !== 0 && players.has(id)) {
      const name = players.get(id).name;
      players.delete(id);
      broadcast({ t: "left", id });
      console.log(`- ${name} (#${id}) left. Players: ${players.size}/${MAX_PLAYERS}`);
    }
  });

  ws.on("error", () => {});
});

// Keep connections alive (Render closes idle ones) and drop dead ones.
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

httpServer.listen(PORT, "0.0.0.0", () => {
  console.log(`OxideX server running on port ${PORT} (max ${MAX_PLAYERS} players)`);
});
