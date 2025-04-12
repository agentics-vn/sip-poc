import asyncio
import base64
import io
import json
import logging
from pathlib import Path
from typing import Set
from urllib.parse import parse_qs, urlparse

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydub.audio_segment import AudioSegment
from websockets.exceptions import ConnectionClosed

app = FastAPI()

# Configuration
PCM_FILE = "3.pcm"
REPEAT_FILE = "3.wav"
TEST_FILE = "3.mp3"
SAMPLE_RATE = 44100
BYTE_PER_SAMPLE = 2
CHANNELS = 2
BYTES_CHUNK = int(SAMPLE_RATE * BYTE_PER_SAMPLE * CHANNELS / 1000)

# Global state
clients: Set[WebSocket] = set()
servers: Set[WebSocket] = set()
pcm_data: bytes | None = None
offset: int = 0
send_task: asyncio.Task | None = None

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def try_parse_json(json_string: str) -> dict:
    """Attempt to parse a JSON string, return dict or None."""
    try:
        obj = json.loads(json_string)
        if isinstance(obj, dict):
            return obj
    except json.JSONDecodeError:
        pass
    return {}


def base64_to_bytes(base64_str: str) -> bytes:
    """Convert base64 string to bytes."""
    return base64.b64decode(base64_str)


async def send_payload_to_clients(payload: bytes | str):
    """Send payload to all connected clients."""
    for client in clients.copy():
        try:
            if isinstance(payload, bytes):
                await client.send_bytes(payload)
            else:
                await client.send_text(payload)
        except (WebSocketDisconnect, ConnectionClosed):
            clients.discard(client)
            logger.info("Client disconnected during send")


async def send_payload_to_servers(payload: str):
    """Send payload to all connected servers."""
    for server in servers.copy():
        try:
            await server.send_text(payload)
        except (WebSocketDisconnect, ConnectionClosed):
            servers.discard(server)
            logger.info("Server disconnected during send")


async def send_data():
    """Send chunks of PCM data to clients."""
    global offset, pcm_data, send_task
    if not pcm_data:
        return
    while offset < len(pcm_data):
        payload = pcm_data[offset : offset + BYTES_CHUNK]
        offset += BYTES_CHUNK
        await send_payload_to_clients(payload)
        await asyncio.sleep(1)  # Simulate streaming interval
    offset = 0  # Reset offset when done
    send_task = None


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("Socket connected. Processing...")

    # Parse client type from query parameters
    query_params = parse_qs(urlparse(websocket.scope["query_string"].decode()).query)
    client_type = query_params.get("clientType", ["player"])[0]
    logger.info(f"Client type: {client_type}")

    # Register client or server
    if client_type == "player":
        clients.add(websocket)
    elif client_type == "server":
        servers.add(websocket)

    try:
        while True:
            data = await websocket.receive()
            if "bytes" in data:
                return None

            message = data.get("text", "")
            if message == "hangup":
                payload = json.dumps({"event": "hangup"})
                await send_payload_to_servers(payload)
                continue

            msg = try_parse_json(message)
            if not msg:
                continue

            if msg.get("event") == "media" and msg.get("media", {}).get("payload"):
                await send_payload_to_clients(base64_to_bytes(msg["media"]["payload"]))
                continue

            if msg.get("event") == "connected":
                logger.info("Starting new call")
                audio_data = Path("3.wav").read_bytes()

                # Get total bytes
                total_bytes = len(audio_data)
                logger.info(f"Total bytes {total_bytes}, Chunk: {BYTES_CHUNK}")

                for i in range(0, total_bytes, 2048 * 2):
                    chunk = audio_data[i : i + 2048 * 2]
                    logger.info(len(chunk))

                    try:
                        audio_segment = AudioSegment(
                            chunk,  # dữ liệu thô dạng byte
                            sample_width=2,  # mỗi mẫu có 2 byte (tương đương 16-bit)
                            frame_rate=8000,  # tần số lấy mẫu 8kHz (phù hợp với âm thanh thoại) = sample rate
                            channels=1,  # mono (1 kênh)
                        )

                        buffer = io.BytesIO()

                        audio_segment.export(buffer, format="mp3", bitrate="8k")

                        message = {
                            "event": "chunk",
                            "media": {
                                "payload": base64.b64encode(buffer.getvalue()).decode(),
                                "is_sync": True,
                            },
                        }

                        await send_payload_to_clients(json.dumps(message))
                        await asyncio.sleep(1)
                    except Exception:
                        logger.error("Chunk size doesnt match")
                        break

    except (WebSocketDisconnect, ConnectionClosed):
        logger.info("Disconnected")
        if client_type == "server":
            payload = json.dumps({"event": "close"})
            await send_payload_to_clients(payload)
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        clients.discard(websocket)
        servers.discard(websocket)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
