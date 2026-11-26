"""OxideX game server - WebSocket relay for Render (Python 3, standard library only).

One room per map, max 10 players per room. Player saves are kept in memory
(they are lost when the server restarts). Player names are unique and are stored
in a GitHub file, so they survive restarts. Environment variables for that:
  GITHUB_TOKEN  fine-grained token with "Contents: Read and write" on one repository
  GITHUB_REPO   owner/repository
  GITHUB_BRANCH (optional, default main)
Clients connect with wss://my-game-d6ay.onrender.com
Open the same address in a browser (https) to see the player count.
"""
import asyncio
import base64
import hashlib
import json
import math
import os
import re
import struct
import urllib.error
import urllib.request

PORT = int(os.environ.get("PORT", "10000"))
MAX_PLAYERS = 10
MAX_CONNECTIONS = 40
MAX_SAVES = 2000
SERVER_NAME = "OxideX Official"
MAX_PAYLOAD = 8192
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
GITHUB_REPO = os.environ.get("GITHUB_REPO", "").strip().strip("/")
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main").strip()
GITHUB_API = os.environ.get("GITHUB_API", "https://api.github.com")
NAMES_PATH = "data/names.json"
MAX_NAMES = 8000
WS_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

players = {}  # id -> Client
saves = {}    # (map, device id) -> save data
connections = 0
next_id = 1


# ---------------------------------------------------------------- unique names

names = {}          # lower case name -> {"name": shown name, "owner": hashed device id}
names_loaded = False
names_status = "memory only (GITHUB_TOKEN / GITHUB_REPO not set)"
names_sha = None
names_dirty = None  # asyncio.Event, created in main()


def github_enabled():
    return bool(GITHUB_TOKEN and GITHUB_REPO)


def github_request(method, url, body=None):
    req = urllib.request.Request(
        GITHUB_API + url, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": "Bearer " + GITHUB_TOKEN, "Accept": "application/vnd.github+json",
                 "User-Agent": "oxidex-server", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as res:
        return json.loads(res.read().decode("utf-8"))


def github_read():
    """returns (names, sha); ({}, None) if the file does not exist yet"""
    try:
        res = github_request("GET", f"/repos/{GITHUB_REPO}/contents/{NAMES_PATH}?ref={GITHUB_BRANCH}")
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
        # 404 can also mean a wrong repository name or token: check the repository and branch
        github_request("GET", f"/repos/{GITHUB_REPO}/branches/{GITHUB_BRANCH}")
        return {}, None
    data = json.loads(base64.b64decode(res["content"]).decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("names file is not valid")
    return data, res["sha"]


def github_write(text, sha):
    body = {"message": "[skip render] update player names", "branch": GITHUB_BRANCH,
            "content": base64.b64encode(text.encode("utf-8")).decode()}
    if sha:
        body["sha"] = sha
    res = github_request("PUT", f"/repos/{GITHUB_REPO}/contents/{NAMES_PATH}", body)
    return res["content"]["sha"]


def merge_names(remote):
    """keep our newer entries, add the remote ones we do not know"""
    owners = {v["owner"] for v in names.values()}
    for key, value in remote.items():
        if key not in names and value.get("owner") not in owners:
            names[key] = value


async def load_names():
    global names_loaded, names_sha, names_status
    try:
        remote, names_sha = await asyncio.to_thread(github_read)
        merge_names(remote)
        names_loaded = True
        names_status = "saved on GitHub"
        print(f"Names loaded from GitHub: {len(names)}", flush=True)
    except Exception as e:
        names_status = f"GitHub error: {e}"
        print("Could not load names:", e, flush=True)


async def names_worker():
    global names_sha, names_status
    while not names_loaded:
        await asyncio.sleep(15)
        await load_names()
    while True:
        await names_dirty.wait()
        await asyncio.sleep(2)
        names_dirty.clear()
        text = json.dumps(names, ensure_ascii=False, indent=1, sort_keys=True)
        try:
            names_sha = await asyncio.to_thread(github_write, text, names_sha)
            names_status = "saved on GitHub"
        except Exception as e:
            names_status = f"GitHub error: {e}"
            print("Could not save names:", e, flush=True)
            await asyncio.sleep(10)
            try:  # the file may have changed: read it again, then retry
                remote, names_sha = await asyncio.to_thread(github_read)
                merge_names(remote)
            except Exception:
                pass
            names_dirty.set()


def owner_of(device):
    return hashlib.sha256(device.encode()).hexdigest()


def claim_name(raw, device):
    """returns (ok, reason, name)"""
    name = " ".join(str(raw or "").split())
    if not re.fullmatch(r"[A-Za-z0-9_\- ]{3,16}", name):
        return False, "Use 3-16 English letters or numbers", name
    key = name.lower()
    if key == "player":
        return False, "Choose another name", name
    if not device:
        return False, "Update the game", name
    if github_enabled() and not names_loaded:
        return False, "Server is starting, try again", name
    owner = owner_of(device)
    old = names.get(key)
    if old and old["owner"] != owner:
        return False, "Name is already taken", name
    if old and old["name"] == name:
        return True, "", name
    for other in [k for k, v in names.items() if v["owner"] == owner]:
        del names[other]  # one name per phone: the old one becomes free
    if len(names) >= MAX_NAMES:
        return False, "Name list is full", name
    names[key] = {"name": name, "owner": owner}
    if names_dirty is not None:
        names_dirty.set()
    return True, "", name


class Client:
    def __init__(self, writer):
        self.writer = writer
        self.id = 0
        self.name = ""
        self.map = "world"
        self.device = ""
        self.state = None
        self.closed = False

    def send_text(self, text):
        if self.closed or self.writer.is_closing():
            return
        try:
            self.writer.write(make_frame(0x1, text.encode("utf-8")))
            if self.writer.transport.get_write_buffer_size() > 512 * 1024:
                self.close()
        except Exception:
            self.close()

    def send(self, obj):
        self.send_text(json.dumps(obj, separators=(",", ":")))

    def close(self):
        self.closed = True
        try:
            self.writer.close()
        except Exception:
            pass


def make_frame(opcode, payload):
    n = len(payload)
    if n < 126:
        head = bytes([0x80 | opcode, n])
    elif n < 65536:
        head = bytes([0x80 | opcode, 126]) + struct.pack("!H", n)
    else:
        head = bytes([0x80 | opcode, 127]) + struct.pack("!Q", n)
    return head + payload


async def read_frame(reader):
    b = await reader.readexactly(2)
    fin = bool(b[0] & 0x80)
    opcode = b[0] & 0x0F
    masked = bool(b[1] & 0x80)
    n = b[1] & 0x7F
    if n == 126:
        n = struct.unpack("!H", await reader.readexactly(2))[0]
    elif n == 127:
        n = struct.unpack("!Q", await reader.readexactly(8))[0]
    if n > MAX_PAYLOAD:
        raise ValueError("payload too big")
    mask = await reader.readexactly(4) if masked else None
    data = await reader.readexactly(n) if n else b""
    if mask and n:
        full = (mask * (n // 4 + 1))[:n]
        data = (int.from_bytes(data, "big") ^ int.from_bytes(full, "big")).to_bytes(n, "big")
    return fin, opcode, data


def broadcast(obj, room, except_id=0):
    text = json.dumps(obj, separators=(",", ":"))
    for pid, c in list(players.items()):
        if c.map == room and pid != except_id:
            c.send_text(text)


def clean_name(raw):
    n = re.sub(r"[^\w \-]", "", str(raw or "Player")).strip()
    return (n or "Player")[:16]


def num_list(a, length):
    if not isinstance(a, list) or len(a) != length:
        return None
    out = []
    for v in a:
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
            return None
        out.append(v)
    return out


def clean_key(raw, fallback):
    k = re.sub(r"[^A-Za-z0-9_\-]", "", str(raw or ""))[:64]
    return k or fallback


def remove_player(client):
    if client.id and players.get(client.id) is client:
        del players[client.id]
        broadcast({"t": "left", "id": client.id}, client.map)
        print(f"- {client.name} (#{client.id}) left {client.map}.", flush=True)


def store_save(client, data):
    if not client.device or not isinstance(data, dict):
        return
    key = (client.map, client.device)
    saves.pop(key, None)
    saves[key] = data
    while len(saves) > MAX_SAVES:
        del saves[next(iter(saves))]  # forget the oldest one


def handle_message(client, text):
    global next_id
    try:
        msg = json.loads(text)
    except (ValueError, RecursionError):
        return
    if not isinstance(msg, dict):
        return
    t = msg.get("t")

    if t == "register" and client.id == 0:
        ok, reason, name = claim_name(msg.get("name"), clean_key(msg.get("device"), ""))
        client.send({"t": "register_result", "ok": ok, "reason": reason, "name": name})
        client.close()
        return

    if t == "join" and client.id == 0:
        room = clean_key(msg.get("map"), "world")
        device = clean_key(msg.get("device"), "")
        wanted = " ".join(str(msg.get("name") or "").split())
        taken = names.get(wanted.lower())
        if taken and (not device or taken["owner"] != owner_of(device)):
            client.send({"t": "name_taken"})
            client.close()
            return
        if not taken and device:
            claim_name(wanted, device)
        # the same phone came back while its old connection is still open
        if device:
            for old in list(players.values()):
                if old.device == device and old.map == room:
                    remove_player(old)
                    old.send({"t": "replaced"})
                    old.close()
        if sum(1 for c in players.values() if c.map == room) >= MAX_PLAYERS:
            client.send({"t": "full"})
            client.close()
            return
        pid = next_id
        next_id += 1
        client.id = pid
        client.map = room
        client.device = device
        client.name = clean_name(msg.get("name"))
        others = [{"id": i, "name": c.name, "state": c.state} for i, c in players.items() if c.map == room]
        players[pid] = client
        client.send({"t": "welcome", "id": pid, "name": SERVER_NAME, "max": MAX_PLAYERS,
                     "players": others, "save": saves.get((room, device)) if device else None})
        broadcast({"t": "joined", "id": pid, "name": client.name}, room, pid)
        count = len(others) + 1
        print(f"+ {client.name} (#{pid}) joined {room}. Players: {count}/{MAX_PLAYERS}", flush=True)
        return

    if client.id == 0 or players.get(client.id) is not client:
        return

    if t == "save":
        store_save(client, msg.get("data"))
        return

    if t == "state":
        pos = num_list(msg.get("p"), 3)
        rot = num_list(msg.get("r"), 2)
        if pos is None or rot is None:
            return
        a = msg.get("a", 0)
        if isinstance(a, bool) or not isinstance(a, (int, float)) or not math.isfinite(a):
            a = 0
        client.state = {"p": pos, "r": rot, "a": a}
        broadcast({"t": "state", "id": client.id, "p": pos, "r": rot, "a": a}, client.map, client.id)


async def pinger(client):
    try:
        while not client.closed:
            await asyncio.sleep(25)
            client.writer.write(make_frame(0x9, b""))
    except Exception:
        pass


async def ws_session(reader, writer, key):
    accept = base64.b64encode(hashlib.sha1(key.encode() + WS_GUID).digest()).decode()
    writer.write(
        ("HTTP/1.1 101 Switching Protocols\r\n"
         "Upgrade: websocket\r\n"
         "Connection: Upgrade\r\n"
         f"Sec-WebSocket-Accept: {accept}\r\n\r\n").encode()
    )
    client = Client(writer)
    ping_task = asyncio.create_task(pinger(client))
    frag = b""
    try:
        while not client.closed:
            timeout = 70 if client.id else 10
            fin, opcode, data = await asyncio.wait_for(read_frame(reader), timeout)
            if opcode == 0x8:
                try:
                    writer.write(make_frame(0x8, data[:2]))
                except Exception:
                    pass
                break
            if opcode == 0x9:
                writer.write(make_frame(0xA, data))
                continue
            if opcode == 0xA:
                continue
            if opcode in (0x1, 0x0):
                frag += data
                if len(frag) > MAX_PAYLOAD:
                    break
                if fin:
                    text, frag = frag, b""
                    handle_message(client, text.decode("utf-8", errors="ignore"))
    except (asyncio.IncompleteReadError, ConnectionError, TimeoutError, OSError, ValueError):
        pass
    finally:
        ping_task.cancel()
        remove_player(client)
        client.close()


async def http_reply(writer, method, path):
    if path.split("?")[0] == "/health":
        body, ctype = b"ok", "text/plain"
    else:
        body = json.dumps({"name": SERVER_NAME, "players": len(players), "max": MAX_PLAYERS,
                           "names": len(names), "name_storage": names_status}).encode()
        ctype = "application/json"
    head = (f"HTTP/1.1 200 OK\r\nContent-Type: {ctype}\r\n"
            f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode()
    writer.write(head if method == "HEAD" else head + body)
    await writer.drain()


async def handle(reader, writer):
    global connections
    if connections >= MAX_CONNECTIONS:
        writer.close()
        return
    connections += 1
    try:
        raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
        lines = raw.decode("latin-1").split("\r\n")
        parts = lines[0].split(" ")
        method = parts[0]
        path = parts[1] if len(parts) > 1 else "/"
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        if headers.get("upgrade", "").lower() == "websocket" and "sec-websocket-key" in headers:
            await ws_session(reader, writer, headers["sec-websocket-key"])
        else:
            await http_reply(writer, method, path)
    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, ConnectionError, TimeoutError, OSError):
        pass
    finally:
        connections -= 1
        try:
            writer.close()
        except Exception:
            pass


async def main():
    global names_dirty
    names_dirty = asyncio.Event()
    worker = None
    if github_enabled():
        await load_names()
        worker = asyncio.create_task(names_worker())  # keep a reference to the task
    server = await asyncio.start_server(handle, "0.0.0.0", PORT)
    print(f"OxideX server running on port {PORT} (max {MAX_PLAYERS} players)", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
