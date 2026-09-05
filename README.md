import asyncio
import json
import logging
import os
import websockets
from websockets.exceptions import ConnectionClosed

# Dumb room-code relay: pairs up to 2 sockets per code and forwards
# whatever one sends, verbatim, to the other. No game knowledge at all.
rooms = {}  # code -> list of websockets (max 2)

# Abrupt disconnects (process exit, crash, network drop — no clean WS close
# handshake) are completely routine for game clients and must never be
# treated as a server error, just a normal "peer left" event.
logging.getLogger("websockets").setLevel(logging.CRITICAL)


async def handler(websocket):
    room_code = None
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except Exception:
                continue

            msg_type = data.get("type")

            if msg_type == "join":
                room_code = str(data.get("code", ""))
                if not room_code:
                    continue
                bucket = rooms.setdefault(room_code, [])
                if websocket in bucket:
                    continue  # duplicate join for an already-registered socket, ignore
                if len(bucket) >= 2:
                    await websocket.send(json.dumps({"type": "room_full"}))
                    room_code = None
                    continue
                bucket.append(websocket)
                if len(bucket) == 2:
                    for i, ws in enumerate(bucket):
                        await ws.send(json.dumps({"type": "paired", "is_first": i == 0}))
                else:
                    await websocket.send(json.dumps({"type": "waiting"}))

            elif msg_type == "relay":
                if room_code and room_code in rooms:
                    for ws in rooms[room_code]:
                        if ws is not websocket:
                            await ws.send(json.dumps({"type": "relay", "payload": data.get("payload")}))

    except ConnectionClosed:
        # normal: the other side hung up without a clean close handshake
        # (process exit, crash, network drop) — nothing to do here, cleanup
        # happens in `finally` exactly the same as a graceful disconnect
        pass

    finally:
        if room_code and room_code in rooms:
            bucket = rooms[room_code]
            if websocket in bucket:
                bucket.remove(websocket)
                for ws in bucket:
                    try:
                        await ws.send(json.dumps({"type": "peer_left"}))
                    except ConnectionClosed:
                        pass
            if not bucket:
                del rooms[room_code]


async def main():
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"Relay listening on 0.0.0.0:{port}")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
