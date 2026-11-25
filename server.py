import asyncio
import json
import os
import websockets

CLIENTS = set()


async def broadcast(message: str, sender=None):
    if not CLIENTS:
        return
    dead = set()
    for client in CLIENTS:
        if client == sender:
            continue
        try:
            await client.send(message)
        except Exception:
            dead.add(client)
    CLIENTS.difference_update(dead)


async def handler(websocket):
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
    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        print("Client error:", e)
    finally:
        CLIENTS.discard(websocket)


async def process_request(connection, request):
    upgrade = request.headers.get("Upgrade", "").lower()
    if upgrade != "websocket":
        return connection.respond(200, "OK - Game Server Running\n")
    return None


async def main():
    host = "0.0.0.0"
    port = int(os.environ.get("PORT", "10000"))
    print(f"Server listening on {host}:{port}")

    async with websockets.serve(
        handler,
        host,
        port,
        process_request=process_request,
        ping_interval=20,
        ping_timeout=20,
    ):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
