"""
Twilio <-> Gemini Live bridge for phone calls.

This service exposes:
- An HTTP webhook that returns TwiML for Twilio Voice
- A WebSocket endpoint for Twilio bidirectional Media Streams
- A health endpoint for Render

Deploy this file as the server process on Render.
"""

import asyncio
import base64
import contextlib
import json
import logging
import os
from datetime import datetime
from typing import Any
from urllib.parse import parse_qsl
from xml.sax.saxutils import escape

import numpy as np
import requests
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect

load_dotenv(override=True)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "models/gemini-3.1-flash-live-preview")
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
PORT = int(os.getenv("PORT", "10000"))

WS_ENDPOINT = (
    "wss://generativelanguage.googleapis.com/ws/"
    "google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent"
)

TWILIO_SAMPLE_RATE = 8000
GEMINI_INPUT_SAMPLE_RATE = 16000
GEMINI_OUTPUT_SAMPLE_RATE = 24000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("twilio-gemini-bridge")

app = FastAPI(title="TrueAI Lab Twilio Gemini Bridge")


SYSTEM_PROMPT = """You are Jake, a friendly and professional AI voice agent for TrueAI Lab — an AI engineering company that builds production-grade voice AI agents, workflow automation, and intelligent systems for businesses.

## YOUR ROLE
You are an inbound sales agent on a live phone call. When the call connects, YOU speak first with a warm, fast greeting. You're selling TrueAI Lab's voice AI agent building services.

## INITIAL GREETING
"Hey there! Thanks for calling TrueAI Lab. I'm Jake, and I help businesses like yours get set up with custom AI voice agents. Whether it's handling customer calls, booking appointments, or automating your front desk — we build it all. How can I help you today?"

## CONVERSATION FLOW
1. Start with the greeting above — naturally and quickly.
2. Listen carefully and understand what the caller needs.
3. Naturally collect:
   - Full name
   - Phone number
   - Email address
   - Their specific use case
4. Once you have all four pieces of information, call save_lead.
5. After saving, say: "Awesome, I've got everything noted down. One of our engineers will reach out to you within 24 hours to discuss your project in detail. Thanks for reaching out to TrueAI Lab!"

## STYLE
- Keep responses short because this is a phone call
- Ask for one thing at a time
- Be warm, confident, and natural
- Never repeat the full greeting if interrupted
- If asked about pricing, say pricing depends on complexity and an engineer will walk them through options
- If they are not interested, end politely and briefly
- Stay focused on the caller and their use case"""


def call_n8n_webhook(lead_data: dict[str, Any]) -> dict[str, Any]:
    """POST lead data to n8n webhook. Returns success/error status."""
    if not N8N_WEBHOOK_URL:
        log.warning("N8N_WEBHOOK_URL not set - skipping webhook call")
        return {"success": False, "error": "Webhook URL not configured"}

    try:
        payload = {
            "name": lead_data.get("name", ""),
            "phone": lead_data.get("phone", ""),
            "email": lead_data.get("email", ""),
            "use_case": lead_data.get("use_case", ""),
            "timestamp": datetime.now().isoformat(),
            "source": "twilio-gemini-voice-agent",
        }
        log.info("Calling n8n webhook with lead payload")

        resp = requests.post(
            N8N_WEBHOOK_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        return {"success": True, "message": "Lead saved successfully"}

    except requests.RequestException as exc:
        log.error(f"Webhook failed: {exc}")
        return {"success": False, "error": str(exc)}


def build_public_base_url(request: Request) -> str:
    if PUBLIC_BASE_URL:
        return PUBLIC_BASE_URL

    forwarded_proto = request.headers.get("x-forwarded-proto")
    forwarded_host = request.headers.get("x-forwarded-host")
    host = forwarded_host or request.headers.get("host") or request.url.netloc
    scheme = forwarded_proto or request.url.scheme or "https"
    return f"{scheme}://{host}".rstrip("/")


def http_base_to_ws_base(http_base: str) -> str:
    if http_base.startswith("https://"):
        return "wss://" + http_base[len("https://") :]
    if http_base.startswith("http://"):
        return "ws://" + http_base[len("http://") :]
    return http_base


def build_twiml(stream_url: str, status_callback_url: str) -> str:
    escaped_stream_url = escape(stream_url, {'"': "&quot;"})
    escaped_status_url = escape(status_callback_url, {'"': "&quot;"})
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<Response>"
        "<Connect>"
        f'<Stream url="{escaped_stream_url}" statusCallback="{escaped_status_url}" '
        'statusCallbackMethod="POST">'
        '<Parameter name="agent" value="trueai-gemini" />'
        "</Stream>"
        "</Connect>"
        "</Response>"
    )


def pcm16_bytes_to_numpy(pcm: bytes) -> np.ndarray:
    if not pcm:
        return np.array([], dtype=np.int16)
    return np.frombuffer(pcm, dtype=np.int16)


def resample_pcm16(pcm: bytes, input_rate: int, output_rate: int) -> bytes:
    if not pcm or input_rate == output_rate:
        return pcm

    samples = pcm16_bytes_to_numpy(pcm).astype(np.float32)
    if samples.size == 0:
        return b""

    target_length = max(1, int(round(samples.size * output_rate / input_rate)))
    x_old = np.linspace(0.0, samples.size - 1, num=samples.size, dtype=np.float32)
    x_new = np.linspace(0.0, samples.size - 1, num=target_length, dtype=np.float32)
    resampled = np.interp(x_new, x_old, samples)
    return np.clip(resampled, -32768, 32767).astype(np.int16).tobytes()


MU_LAW_BIAS = 0x84
MU_LAW_CLIP = 32635


def mulaw_to_pcm16(mulaw_bytes: bytes) -> bytes:
    if not mulaw_bytes:
        return b""

    mulaw = np.frombuffer(mulaw_bytes, dtype=np.uint8)
    u = np.bitwise_not(mulaw)
    sign = u & 0x80
    exponent = (u >> 4) & 0x07
    mantissa = u & 0x0F
    magnitude = ((mantissa.astype(np.int32) << 3) + MU_LAW_BIAS) << exponent
    pcm = magnitude - MU_LAW_BIAS
    pcm = np.where(sign != 0, -pcm, pcm)
    return pcm.astype(np.int16).tobytes()


def pcm16_to_mulaw(pcm_bytes: bytes) -> bytes:
    if not pcm_bytes:
        return b""

    pcm = pcm16_bytes_to_numpy(pcm_bytes).astype(np.int32)
    sign = np.where(pcm < 0, 0x80, 0).astype(np.uint8)
    magnitude = np.minimum(np.abs(pcm), MU_LAW_CLIP) + MU_LAW_BIAS

    exponent = np.zeros_like(magnitude, dtype=np.uint8)
    exp_mask = 0x4000
    for exp in range(7, -1, -1):
        mask = (magnitude & exp_mask) != 0
        exponent = np.where(mask & (exponent == 0), exp, exponent)
        exp_mask >>= 1

    mantissa = ((magnitude >> (exponent + 3)) & 0x0F).astype(np.uint8)
    ulaw = np.bitwise_not(sign | (exponent << 4) | mantissa)
    return ulaw.astype(np.uint8).tobytes()


def twilio_payload_to_gemini_pcm(payload_b64: str) -> bytes:
    mulaw_audio = base64.b64decode(payload_b64)
    pcm_8k = mulaw_to_pcm16(mulaw_audio)
    return resample_pcm16(pcm_8k, TWILIO_SAMPLE_RATE, GEMINI_INPUT_SAMPLE_RATE)


def gemini_pcm_to_twilio_payload(pcm_24k: bytes) -> str:
    pcm_8k = resample_pcm16(pcm_24k, GEMINI_OUTPUT_SAMPLE_RATE, TWILIO_SAMPLE_RATE)
    mulaw_audio = pcm16_to_mulaw(pcm_8k)
    return base64.b64encode(mulaw_audio).decode("ascii")


class TwilioGeminiBridge:
    def __init__(self, twilio_ws: WebSocket):
        self.twilio_ws = twilio_ws
        self.gemini_ws: websockets.WebSocketClientProtocol | None = None
        self.stream_sid: str | None = None
        self.call_sid: str | None = None
        self.running = True
        self.gemini_ready = asyncio.Event()
        self.incoming_audio_queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=100)
        self.mark_counter = 0

    def _build_setup_message(self) -> dict[str, Any]:
        return {
            "setup": {
                "model": GEMINI_MODEL,
                "generationConfig": {
                    "responseModalities": ["AUDIO"],
                    "speechConfig": {
                        "voiceConfig": {
                            "prebuiltVoiceConfig": {"voiceName": "Puck"}
                        }
                    },
                    "temperature": 0.7,
                    "maxOutputTokens": 4096,
                },
                "systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
                "tools": [
                    {
                        "functionDeclarations": [
                            {
                                "name": "save_lead",
                                "description": (
                                    "Save a qualified lead to the CRM after collecting "
                                    "name, phone, email, and use_case."
                                ),
                                "parameters": {
                                    "type": "OBJECT",
                                    "properties": {
                                        "name": {"type": "STRING"},
                                        "phone": {"type": "STRING"},
                                        "email": {"type": "STRING"},
                                        "use_case": {"type": "STRING"},
                                    },
                                    "required": ["name", "phone", "email", "use_case"],
                                },
                            }
                        ]
                    }
                ],
                "realtimeInputConfig": {
                    "automaticActivityDetection": {
                        "disabled": False,
                        "startOfSpeechSensitivity": "START_SENSITIVITY_HIGH",
                        "endOfSpeechSensitivity": "END_SENSITIVITY_HIGH",
                        "prefixPaddingMs": 80,
                        "silenceDurationMs": 600,
                    },
                    "activityHandling": "START_OF_ACTIVITY_INTERRUPTS",
                    "turnCoverage": "TURN_INCLUDES_ONLY_ACTIVITY",
                },
                "inputAudioTranscription": {},
                "outputAudioTranscription": {},
            }
        }

    async def connect_gemini(self) -> None:
        if not GEMINI_API_KEY:
            raise RuntimeError("GEMINI_API_KEY is not configured")

        uri = f"{WS_ENDPOINT}?key={GEMINI_API_KEY}"
        self.gemini_ws = await websockets.connect(
            uri,
            max_size=10 * 1024 * 1024,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        )
        await self.gemini_ws.send(json.dumps(self._build_setup_message()))
        log.info("Gemini setup message sent")

    async def trigger_initial_greeting(self) -> None:
        if not self.gemini_ws:
            return
        await self.gemini_ws.send(
            json.dumps(
                {
                    "realtimeInput": {
                        "text": "The phone call has connected. Deliver your greeting immediately."
                    }
                }
            )
        )

    async def send_twilio_clear(self) -> None:
        if not self.stream_sid:
            return
        await self.twilio_ws.send_json({"event": "clear", "streamSid": self.stream_sid})

    async def send_twilio_audio(self, payload_b64: str) -> None:
        if not self.stream_sid:
            return

        await self.twilio_ws.send_json(
            {
                "event": "media",
                "streamSid": self.stream_sid,
                "media": {"payload": payload_b64},
            }
        )

        self.mark_counter += 1
        await self.twilio_ws.send_json(
            {
                "event": "mark",
                "streamSid": self.stream_sid,
                "mark": {"name": f"chunk-{self.mark_counter}"},
            }
        )

    async def handle_tool_call(self, function_calls: list[dict[str, Any]]) -> None:
        if not self.gemini_ws:
            return

        responses = []
        for call in function_calls:
            fn_name = call.get("name")
            fn_args = call.get("args", {})
            fn_id = call.get("id")

            log.info(f"Tool call received: {fn_name}")
            if fn_name == "save_lead":
                result = call_n8n_webhook(fn_args)
            else:
                result = {"error": f"Unknown function: {fn_name}"}

            responses.append({"id": fn_id, "name": fn_name, "response": result})

        await self.gemini_ws.send(
            json.dumps({"toolResponse": {"functionResponses": responses}})
        )

    async def twilio_to_bridge(self) -> None:
        try:
            while self.running:
                raw_text = await self.twilio_ws.receive_text()
                msg = json.loads(raw_text)
                event = msg.get("event")

                if event == "connected":
                    log.info("Twilio media stream connected")
                    continue

                if event == "start":
                    start = msg.get("start", {})
                    self.stream_sid = msg.get("streamSid") or start.get("streamSid")
                    self.call_sid = start.get("callSid")
                    log.info(f"Twilio stream started: {self.stream_sid}")
                    continue

                if event == "media":
                    payload_b64 = msg.get("media", {}).get("payload")
                    if not payload_b64:
                        continue

                    pcm_16k = twilio_payload_to_gemini_pcm(payload_b64)
                    try:
                        self.incoming_audio_queue.put_nowait(pcm_16k)
                    except asyncio.QueueFull:
                        log.warning("Incoming audio queue full - dropping chunk")
                    continue

                if event == "dtmf":
                    digit = msg.get("dtmf", {}).get("digit")
                    log.info(f"DTMF received: {digit}")
                    continue

                if event == "mark":
                    continue

                if event == "stop":
                    log.info("Twilio stream stopped")
                    break

        except WebSocketDisconnect:
            log.info("Twilio WebSocket disconnected")
        finally:
            self.running = False
            with contextlib.suppress(asyncio.QueueFull):
                self.incoming_audio_queue.put_nowait(None)

    async def pump_audio_to_gemini(self) -> None:
        await self.gemini_ready.wait()

        while self.running and self.gemini_ws:
            chunk = await self.incoming_audio_queue.get()
            if chunk is None:
                break

            try:
                audio_b64 = base64.b64encode(chunk).decode("ascii")
                await self.gemini_ws.send(
                    json.dumps(
                        {
                            "realtimeInput": {
                                "audio": {
                                    "mimeType": "audio/pcm;rate=16000",
                                    "data": audio_b64,
                                }
                            }
                        }
                    )
                )
            except websockets.ConnectionClosed:
                break

    async def gemini_to_twilio(self) -> None:
        if not self.gemini_ws:
            return

        try:
            async for raw in self.gemini_ws:
                msg = json.loads(raw)

                if "setupComplete" in msg:
                    log.info("Gemini session ready")
                    self.gemini_ready.set()
                    await self.trigger_initial_greeting()
                    continue

                if "toolCall" in msg:
                    await self.handle_tool_call(msg["toolCall"]["functionCalls"])
                    continue

                if "serverContent" in msg:
                    sc = msg["serverContent"]

                    if sc.get("interrupted"):
                        log.info("Gemini interrupted - clearing Twilio audio buffer")
                        await self.send_twilio_clear()
                        continue

                    model_turn = sc.get("modelTurn")
                    if model_turn and model_turn.get("parts"):
                        for part in model_turn["parts"]:
                            if "inlineData" in part:
                                audio_b64 = part["inlineData"]["data"]
                                pcm_24k = base64.b64decode(audio_b64)
                                twilio_payload = gemini_pcm_to_twilio_payload(pcm_24k)
                                await self.send_twilio_audio(twilio_payload)

                    if "inputTranscription" in sc:
                        text = sc["inputTranscription"].get("text", "").strip()
                        if text:
                            log.info(f"Caller: {text}")

                    if "outputTranscription" in sc:
                        text = sc["outputTranscription"].get("text", "").strip()
                        if text:
                            log.info(f"Agent: {text}")

        except websockets.ConnectionClosed as exc:
            log.info(f"Gemini connection closed: {exc.code} - {exc.reason}")
        finally:
            self.running = False
            with contextlib.suppress(asyncio.QueueFull):
                self.incoming_audio_queue.put_nowait(None)

    async def close(self) -> None:
        self.running = False
        with contextlib.suppress(asyncio.QueueFull):
            self.incoming_audio_queue.put_nowait(None)

        if self.gemini_ws and not self.gemini_ws.closed:
            with contextlib.suppress(Exception):
                await self.gemini_ws.send(
                    json.dumps({"realtimeInput": {"audioStreamEnd": True}})
                )
            with contextlib.suppress(Exception):
                await self.gemini_ws.close()

        with contextlib.suppress(Exception):
            await self.twilio_ws.close()

    async def run(self) -> None:
        await self.connect_gemini()

        tasks = [
            asyncio.create_task(self.twilio_to_bridge()),
            asyncio.create_task(self.pump_audio_to_gemini()),
            asyncio.create_task(self.gemini_to_twilio()),
        ]

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        self.running = False
        for task in pending:
            task.cancel()

        await asyncio.gather(*pending, return_exceptions=True)
        await asyncio.gather(*done, return_exceptions=True)
        await self.close()


@app.get("/")
async def root() -> dict[str, Any]:
    return {
        "ok": True,
        "service": "trueai-twilio-gemini-bridge",
        "model": GEMINI_MODEL,
    }


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.api_route("/twilio/voice", methods=["GET", "POST"])
async def twilio_voice(request: Request) -> Response:
    base_url = build_public_base_url(request)
    stream_url = f"{http_base_to_ws_base(base_url)}/twilio/media"
    status_url = f"{base_url}/twilio/stream-status"
    twiml = build_twiml(stream_url=stream_url, status_callback_url=status_url)
    return Response(content=twiml, media_type="application/xml")


@app.post("/twilio/stream-status")
async def twilio_stream_status(request: Request) -> dict[str, Any]:
    raw_body = (await request.body()).decode("utf-8")
    payload = dict(parse_qsl(raw_body, keep_blank_values=True))
    log.info(f"Twilio stream status: {payload}")
    return {"ok": True}


@app.websocket("/twilio/media")
async def twilio_media_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    bridge = TwilioGeminiBridge(websocket)
    try:
        await bridge.run()
    except Exception as exc:
        log.error(f"Bridge error: {exc}", exc_info=True)
        await bridge.close()
