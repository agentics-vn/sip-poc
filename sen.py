import asyncio
import base64
import io
import json
import logging
import traceback
from pathlib import Path
from typing import Dict, Set
from urllib.parse import parse_qs, urlparse

import numpy as np
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydub import AudioSegment
from pydub.playback import play
from websockets.exceptions import ConnectionClosed

app = FastAPI()

# Configuration
PCM_FILE = "audio/16bit-8000.pcm"
REPEAT_FILE = "audio/repeat.wav"
TEST_FILE = "audio/woman_4.wav"
TEST_FILE_2 = "audio/3.mp3"
TEST_WAV = "audio/3.wav"

SAMPLE_RATE = 44100
BYTE_PER_SAMPLE = 2
CHANNELS = 2
BYTES_CHUNK = SAMPLE_RATE * BYTE_PER_SAMPLE * CHANNELS

# Global state
clients: Set[WebSocket] = set()
servers: Set[WebSocket] = set()
pcm_data: bytes = None
offset: int = 0
send_task: asyncio.Task = None

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
    return None


def base64_to_bytes(base64_str: str) -> bytes:
    """Convert base64 string to bytes."""
    return base64.b64decode(base64_str)


async def send_payload_to_clients(payload: bytes | str | dict, data_type="text"):
    """Send payload to all connected clients."""
    for client in clients.copy():
        try:
            if data_type == "binary":
                await client.send_bytes(payload)
            if data_type == "json":
                await client.send_json(payload)
            if data_type == "text":
                await client.send_text(payload)
        except (WebSocketDisconnect, ConnectionClosed):
            clients.discard(client)
            logger.error("Client disconnected during send")
        finally:
            await asyncio.sleep(0.02)


async def play_audio(audio_data):
    segment = AudioSegment(
        audio_data,
        sample_width=2,  # mỗi mẫu có 2 byte (tương đương 16-bit)
        frame_rate=8000,  # tần số lấy mẫu 8kHz (phù hợp với âm thanh thoại) = sample rate
        channels=1,  # mono (1 kênh)
    )
    play(segment)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("Socket connected.")

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
            is_binary = "bytes" in data
            if is_binary:
                continue

            raw_message = data["bytes"] if is_binary else data["text"]

            msg = try_parse_json(raw_message)
            if msg:
                if msg.get("event") == "media" and msg.get("media", {}).get("payload"):
                    # await send_payload_to_clients(base64_to_bytes(msg["media"]["payload"]))

                    # await play_audio(base64_to_bytes(msg["media"]["payload"]))
                    continue
                elif msg.get("event") == "connected":
                    logger.info("Starting new call")
                    # try:
                    #     data = Path(TEST_FILE).read_bytes()
                    #     payload = json.dumps({
                    #         "event": "media",
                    #         "media": {
                    #             "payload": base64.b64encode(data).decode(),
                    #             "is_sync": True
                    #         }
                    #     })
                    #     await send_payload_to_clients(payload)
                    # except Exception as e:
                    #     logger.error(f"Error reading test file: {e}")
                    #     raise
                    await send_chunks()
                    continue

            # Default: send binary message to clients
            # await send_payload_to_clients(message)

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


# this send whole media file to clients
async def send_media(file_name="audio/woman_4.wav"):
    try:
        audio_data = Path(file_name).read_bytes()
        message = {
            "event": "media",
            "media": {
                "payload": base64.b64encode(audio_data).decode(),
                "is_sync": True,
            },
        }
        await send_payload_to_clients(message, "json")

    except Exception as e:
        logger.error(f"Error reading file: {e}")


async def send_data():
    """Send chunks of PCM data to clients."""
    pcm_data = Path(PCM_FILE).read_bytes()
    offset = 0
    while offset < len(pcm_data):
        payload = pcm_data[offset : offset + BYTES_CHUNK]
        offset += BYTES_CHUNK
        await send_payload_to_clients(payload, "binary")
        await asyncio.sleep(1)  # Simulate streaming interval
    offset = 0  # Reset offset when done


async def send_chunks(chunk_size=BYTES_CHUNK):
    try:
        audio_data = Path(TEST_WAV).read_bytes()

        # Get total bytes
        total_bytes = len(audio_data)

        for i in range(0, total_bytes, chunk_size):
            # Get the current chunk
            chunk = audio_data[i : i + chunk_size]

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
                    "payload": base64.b64encode(buffer.getvalue()).decode("utf-8"),
                    "is_sync": False,
                },
            }

            await send_payload_to_clients(message, "json")
            # await asyncio.sleep(0.02)

    except Exception as e:
        print(f"Error: {str(e)}")
        print(traceback.format_exc())
        return None


async def main():
    await send_chunks()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
    # asyncio.run(main())
