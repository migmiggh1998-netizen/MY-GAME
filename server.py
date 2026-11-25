"""OxideX game server - WebSocket relay for Render (Python 3, standard library only).

Max 10 players. Clients connect with wss://my-game-d6ay.onrender.com
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

PORT = int(os.environ.get("PORT", "10000"))
MAX_PLAYERS = 10
MAX_CONNECTIONS = 40
SERVER_NAME = "OxideX Official"
MAX_PAYLOAD = 4096
WS_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

players = {}
connections = 0
next_id = 1


class Client:
    def __init__(self, writer):
        self.writer = writer
        self.id = 0
        self.name = ""
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


def broadcast(obj, except_id=0):
    text = json.dumps(obj, separators=(",", ":"))
    for pid, c in list(players.items()):
        if pid != except_id:
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


def handle_message(client, text):
    global next_id
    try:
        msg = json.loads(text)
    except ValueError:
        return
    if not isinstance(msg, dict):
        return
    t = msg.get("t")

    if t == "join" and client.id == 0:
        if len(players) >= MAX_PLAYERS:
            client.send({"t": "full"})
            client.close()
            return
        pid = next_id
        next_id += 1
        client.id = pid
        client.name = clean_name(msg.get("name"))
        players[pid] = client
        others = [{"id": i, "name": c.name, "state": c.state} for i, c in players.items() if i != pid]
        client.send({"t": "welcome", "id": pid, "name": SERVER_NAME, "max": MAX_PLAYERS, "players": others})
        broadcast({"t": "joined", "id": pid, "name": client.name}, pid)
        print(f"+ {client.name} (#{pid}) joined. Players: {len(players)}/{MAX_PLAYERS}", flush=True)
        return

    if client.id == 0:
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
        broadcast({"t": "state", "id": client.id, "p": pos, "r": rot, "a": a}, client.id)


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
        if client.id and players.get(client.id) is client:
            del players[client.id]
            broadcast({"t": "left", "id": client.id})
            print(f"- {client.name} (#{client.id}) left. Players: {len(players)}/{MAX_PLAYERS}", flush=True)
        client.close()


async def http_reply(writer, method, path):
    if path.split("?")[0] == "/health":
        body, ctype = b"ok", "text/plain"
    else:
        body = json.dumps({"name": SERVER_NAME, "players": len(players), "max": MAX_PLAYERS}).encode()
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
    server = await asyncio.start_server(handle, "0.0.0.0", PORT)
    print(f"OxideX server running on port {PORT} (max {MAX_PLAYERS} players)", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
