import asyncio
import json
import os
import websockets

CLIENTS = set()


async def broadcast(message: str, sender=None):
    dead = set()
    for client in list(CLIENTS):
        if client == sender:
            continue
        try:
            await client.send(message)
        except Exception:
            dead.add(client)
    CLIENTS.difference_update(dead)


async def handle_ws(websocket):
    CLIENTS.add(websocket)
    try:
        async for raw_msg in websocket:
            try:
                data = json.loads(raw_msg)
            except Exception:
                continue

            if data.get("type") == "ping":
                await websocket.send(json.dumps({"type": "pong"}))
                continue

            await broadcast(json.dumps(data), sender=websocket)
    except Exception:
        pass
    finally:
        CLIENTS.discard(websocket)


async def handle_http_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        request_line = await reader.readline()
        if not request_line:
            writer.close()
            await writer.wait_closed()
            return

        headers = {}
        while True:
            line = await reader.readline()
            if not line or line in (b"\r\n", b"\n"):
                break
            decoded = line.decode("utf-8", errors="ignore").strip()
            if ":" in decoded:
                k, v = decoded.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        upgrade = headers.get("upgrade", "").lower()
        if upgrade == "websocket":
            # به جای هندشیک دستی، از یک سرور داخلی websockets روی پورت جدا استفاده می‌کنیم
            writer.close()
            await writer.wait_closed()
            return

        body = b"OK - Game Server Running"
        response = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain; charset=utf-8\r\n"
            + f"Content-Length: {len(body)}\r\n".encode("ascii")
            + b"Connection: close\r\n\r\n"
            + body
        )
        writer.write(response)
        await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def main():
    host = "0.0.0.0"
    port = int(os.environ.get("PORT", "10000"))

    # برای جلوگیری از تداخل روی یک پورت: Render پورت اصلی را چک می‌کند
    # بیایید با aiohttp یا یک راه تمیزتر کار کنیم تا نیازی به تداخل نباشد!
    print(f"Starting on {host}:{port}", flush=True)
    async with websockets.serve(handle_ws, host, port):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
