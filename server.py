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
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl
from xml.sax.saxutils import escape

import numpy as np
import requests
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect
from prompt_config import SERVER_PROMPT

load_dotenv(override=True)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "models/gemini-3.1-flash-live-preview")
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
PORT = int(os.getenv("PORT", "10000"))

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY", "")
SUPABASE_USER_ID = os.getenv("SUPABASE_USER_ID", "")
SUPABASE_AGENT_ID = os.getenv("SUPABASE_AGENT_ID", "")

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


SYSTEM_PROMPT = SERVER_PROMPT


def _log_startup_config() -> None:
    log.info("TrueAI Lab — Twilio Gemini Bridge starting up")
    log.info(f"  Model       : {GEMINI_MODEL}")
    log.info(f"  Gemini key  : {'set' if GEMINI_API_KEY else '*** MISSING ***'}")
    log.info(f"  n8n webhook : {N8N_WEBHOOK_URL or '(not configured)'}")
    log.info(
        f"  Supabase    : "
        f"{'configured' if SUPABASE_URL and SUPABASE_API_KEY else '(not configured — call logs will be skipped)'}"
    )
    log.info(f"  Public URL  : {PUBLIC_BASE_URL or '(auto-detect from request headers)'}")
    log.info(f"  Port        : {PORT}")


_log_startup_config()


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


def save_call_log_sync(
    call_sid: str,
    customer_phone: str | None,
    conversation: list[dict],
    call_start_time: datetime | None,
    call_end_time: datetime,
) -> None:
    """Save completed call conversation to Supabase call_logs table."""
    if not SUPABASE_URL or not SUPABASE_API_KEY:
        log.warning("Supabase not configured - skipping call log")
        return

    transcript_text = "\n".join(
        f"{turn['speaker']}: {turn['text']}" for turn in conversation
    )
    duration = 0
    if call_start_time:
        duration = max(0, int((call_end_time - call_start_time).total_seconds()))

    payload = {
        "user_id": SUPABASE_USER_ID,
        "agent_id": SUPABASE_AGENT_ID,
        "call_sid": call_sid or "",
        "customer_phone_number": customer_phone,
        "full_conversation": conversation,
        "transcript_text": transcript_text,
        "call_start_time": call_start_time.isoformat() if call_start_time else None,
        "call_end_time": call_end_time.isoformat(),
        "duration_seconds": duration,
    }

    try:
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/call_logs",
            json=payload,
            headers={
                "apikey": SUPABASE_API_KEY,
                "Authorization": f"Bearer {SUPABASE_API_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal",
            },
            timeout=10,
        )
        resp.raise_for_status()
        log.info(f"Call log saved to Supabase: call_sid={call_sid}, turns={len(conversation)}")
    except requests.RequestException as exc:
        log.error(f"Failed to save call log to Supabase: {exc}")


def _normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _extract_email(text: str) -> str | None:
    match = re.search(r"[\w.\-+%]+@[\w.\-]+\.\w+", text)
    return match.group(0) if match else None


def _extract_phone(text: str) -> str | None:
    digits = re.sub(r"\D", "", text)
    if len(digits) < 10:
        return None
    if len(digits) == 10:
        return digits
    return f"+{digits}"


def _clean_name(text: str) -> str:
    cleaned = re.sub(
        r"^(my name is|this is|i am|i'm|im|it is|it's|hi i'm|hello i'm|hey i'm)\s+",
        "",
        text.strip(),
        flags=re.IGNORECASE,
    )
    # Stop before email address (@ sign or "at gmail/yahoo/...") so email doesn't pollute name
    cleaned = re.split(r"\s*@|\s+at\s+\w+\.\w+", cleaned, maxsplit=1)[0]
    # Keep only letters, spaces, apostrophes, hyphens, dots — no digits
    cleaned = re.sub(r"\d+", "", cleaned)
    cleaned = re.sub(r"[^\w\s'.-]", "", cleaned)
    return _normalize_spaces(cleaned)


_GREETING_RE = re.compile(
    r"^(hi\b|hello\b|hey\b|yeah\s+(hi|hello|okay)|good\s+(morning|afternoon|evening)|how\s+are\s+you)",
    re.IGNORECASE,
)


def _is_greeting_or_question(text: str) -> bool:
    """Return True if the text is a greeting or short question — not a real use-case."""
    if _GREETING_RE.match(text):
        return True
    # Short questions like "So what about solutions you sell?" are not use-cases
    if text.endswith("?") and len(text.split()) <= 15:
        return True
    return False


def _clean_use_case(text: str) -> str:
    cleaned = re.sub(
        r"^(we need|i need|we want|i want|it's for|it is for|we are looking for)\s+",
        "",
        text.strip(),
        flags=re.IGNORECASE,
    )
    return _normalize_spaces(cleaned)


class LeadState:
    def __init__(self):
        self.name = ""
        self.phone = ""
        self.email = ""
        self.use_case = ""
        self.saved = False
        self.expected_field = "use_case"

    def update_expected_field(self, agent_text: str):
        text = agent_text.lower()
        if any(phrase in text for phrase in [
            "may i have your name", "your name", "who am i speaking with", "who's this",
            "can i grab your name", "what's your name", "full name", "first name",
            "get your name", "i have your name", "just to confirm, that's",
        ]):
            self.expected_field = "name"
        elif any(phrase in text for phrase in [
            "phone number", "best number", "reach you at", "contact number",
            "number to reach", "country code",
        ]):
            self.expected_field = "phone"
        elif any(phrase in text for phrase in ["email address", "email", "good email"]):
            self.expected_field = "email"
        elif any(phrase in text for phrase in [
            "use case", "what do you want", "what would you like",
            "what should the voice agent do",
        ]):
            self.expected_field = "use_case"

    def merge(self, data: dict[str, Any]):
        for key in ("name", "phone", "email", "use_case"):
            value = _normalize_spaces(str(data.get(key, "")))
            if value:
                setattr(self, key, value)

    def consume_caller_text(self, caller_text: str):
        text = _normalize_spaces(caller_text)
        if not text:
            return

        email = _extract_email(text)
        phone = _extract_phone(text)
        if email and not self.email:
            self.email = email
        if phone and not self.phone:
            self.phone = phone

        if self.expected_field == "name":
            candidate = _clean_name(text)
            # Accept candidate if it has at least one real letter and 2+ chars.
            # Always overwrite when expected_field is "name" so a later, accurate
            # utterance ("it's Saravana Iyyappan") replaces an earlier bad capture.
            if candidate and len(candidate) >= 2 and re.search(r"[a-zA-Z]", candidate):
                self.name = candidate
        elif self.expected_field == "phone" and not self.phone and phone:
            self.phone = phone
        elif self.expected_field == "email" and not self.email and email:
            self.email = email
        elif self.expected_field == "use_case" and not self.use_case:
            cleaned = _clean_use_case(text)
            # Don't store a greeting or a short question as the use-case
            if cleaned and not _is_greeting_or_question(cleaned):
                self.use_case = cleaned

    def has_all_fields(self) -> bool:
        return all([self.name, self.phone, self.email, self.use_case])

    def missing_fields(self) -> list[str]:
        missing = []
        if not self.name:
            missing.append("name")
        if not self.phone:
            missing.append("phone")
        if not self.email:
            missing.append("email")
        if not self.use_case:
            missing.append("use_case")
        return missing

    def as_payload(self) -> dict[str, str]:
        return {
            "name": self.name,
            "phone": self.phone,
            "email": self.email,
            "use_case": self.use_case,
        }


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


def build_twiml(stream_url: str, status_callback_url: str, caller_phone: str = "") -> str:
    escaped_stream_url = escape(stream_url, {'"': "&quot;"})
    escaped_status_url = escape(status_callback_url, {'"': "&quot;"})
    escaped_caller = escape(caller_phone, {'"': "&quot;"})
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<Response>"
        "<Connect>"
        f'<Stream url="{escaped_stream_url}" statusCallback="{escaped_status_url}" '
        'statusCallbackMethod="POST">'
        '<Parameter name="agent" value="trueai-gemini" />'
        f'<Parameter name="callerPhone" value="{escaped_caller}" />'
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
        self.lead_state = LeadState()
        self.session_handle: str | None = None
        # Conversation logging
        self.conversation: list[dict] = []
        self.call_start_time: datetime | None = None
        self.customer_phone_number: str | None = None
        self._user_buf: list[str] = []
        self._ai_buf: list[str] = []
        self._finalized = False

    def _build_setup_message(self) -> dict[str, Any]:
        return {
            "setup": {
                "model": GEMINI_MODEL,
                "generationConfig": {
                    "responseModalities": ["AUDIO"],
                    "speechConfig": {
                        "voiceConfig": {
                            "prebuiltVoiceConfig": {"voiceName": "Aoede"}
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
                                    "Save a qualified lead's information to the CRM. "
                                    "Call this ONLY when you have collected ALL four "
                                    "pieces of information: name, phone, email, and use_case."
                                ),
                                "parameters": {
                                    "type": "OBJECT",
                                    "properties": {
                                        "name": {
                                            "type": "STRING",
                                            "description": "The caller's full name",
                                        },
                                        "phone": {
                                            "type": "STRING",
                                            "description": "The caller's phone number with country code",
                                        },
                                        "email": {
                                            "type": "STRING",
                                            "description": "The caller's email address",
                                        },
                                        "use_case": {
                                            "type": "STRING",
                                            "description": (
                                                "What the caller wants to use the voice agent for - "
                                                "a brief summary of their use case"
                                            ),
                                        },
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
                        # HIGH sensitivity = faster detection (critical for sales calls)
                        "startOfSpeechSensitivity": "START_SENSITIVITY_HIGH",
                        "endOfSpeechSensitivity": "END_SENSITIVITY_HIGH",
                        # Low prefix padding = detect speech start faster
                        "prefixPaddingMs": 80,
                        # Moderate silence = don't cut off mid-thought
                        "silenceDurationMs": 600,
                    },
                    # Barge-in enabled - caller can interrupt the agent
                    "activityHandling": "START_OF_ACTIVITY_INTERRUPTS",
                    # Only include active speech, not silence
                    "turnCoverage": "TURN_INCLUDES_ONLY_ACTIVITY",
                },
                # Session resumption for reconnect resilience
                "sessionResumption": (
                    {"handle": self.session_handle}
                    if self.session_handle
                    else {}
                ),
                # Context compression for long calls
                "contextWindowCompression": {
                    "slidingWindow": {"targetTokens": 20000},
                    "triggerTokens": 40000,
                },
                # Enable transcription for logging
                "inputAudioTranscription": {},
                "outputAudioTranscription": {},
            }
        }

    def _flush_user_turn(self) -> None:
        text = "".join(self._user_buf).strip()
        if text:
            self.conversation.append({"text": text, "speaker": "user"})
        self._user_buf.clear()

    def _flush_ai_turn(self) -> None:
        text = "".join(self._ai_buf).strip()
        if text:
            self.conversation.append({"text": text, "speaker": "ai"})
        self._ai_buf.clear()

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
                        "text": "The phone call has connected. Greet the caller now as Maya from TrueAI Lab."
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
                self.lead_state.merge(fn_args)
                if self.lead_state.saved:
                    result = {"success": True, "message": "Lead already saved successfully"}
                elif self.lead_state.has_all_fields():
                    result = call_n8n_webhook(self.lead_state.as_payload())
                    if result.get("success"):
                        self.lead_state.saved = True
                else:
                    result = {
                        "success": False,
                        "error": f"Missing required fields: {', '.join(self.lead_state.missing_fields())}",
                    }
            else:
                result = {"error": f"Unknown function: {fn_name}"}

            responses.append({"id": fn_id, "name": fn_name, "response": result})

        await self.gemini_ws.send(
            json.dumps({"toolResponse": {"functionResponses": responses}})
        )

    async def maybe_save_lead_fallback(self) -> None:
        if self.lead_state.saved or not self.lead_state.has_all_fields() or not self.gemini_ws:
            return

        result = call_n8n_webhook(self.lead_state.as_payload())
        if not result.get("success"):
            return

        self.lead_state.saved = True
        log.info("Lead auto-saved by fallback webhook logic")
        await self.gemini_ws.send(
            json.dumps(
                {
                    "realtimeInput": {
                        "text": (
                            "System note: the lead has already been saved successfully. "
                            "Briefly confirm that an engineer will reach out within 24 hours, "
                            "and do not ask for the same contact details again."
                        )
                    }
                }
            )
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
                    self.call_start_time = datetime.now(timezone.utc)
                    custom_params = start.get("customParameters", {})
                    self.customer_phone_number = custom_params.get("callerPhone") or None
                    log.info(f"Twilio stream started: {self.stream_sid}, caller={self.customer_phone_number}")
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
                            self._user_buf.append(text)
                            self.lead_state.consume_caller_text(text)
                            await self.maybe_save_lead_fallback()

                    if "outputTranscription" in sc:
                        text = sc["outputTranscription"].get("text", "").strip()
                        if text:
                            if self._user_buf:
                                self._flush_user_turn()
                            log.info(f"Agent: {text}")
                            self._ai_buf.append(text)
                            self.lead_state.update_expected_field(text)

                    if sc.get("turnComplete"):
                        if self._user_buf:
                            self._flush_user_turn()
                        if self._ai_buf:
                            self._flush_ai_turn()

                if "sessionResumptionUpdate" in msg:
                    update = msg["sessionResumptionUpdate"]
                    if update.get("resumable"):
                        self.session_handle = update["newHandle"]
                        log.debug("Session handle cached for reconnect")

                if "goAway" in msg:
                    log.warning(f"Server GoAway: {msg['goAway'].get('timeLeft')}")

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

    async def _finalize_and_save(self) -> None:
        """Flush conversation buffers, log call summary, and save to Supabase.
        Idempotent — safe to call from both the normal path and the error handler."""
        if self._finalized:
            return
        self._finalized = True

        # Flush any remaining partial turns before saving
        if self._user_buf:
            self._flush_user_turn()
        if self._ai_buf:
            self._flush_ai_turn()

        call_end_time = datetime.now(timezone.utc)
        duration = 0
        if self.call_start_time:
            duration = max(0, int((call_end_time - self.call_start_time).total_seconds()))

        ls = self.lead_state
        log.info("─" * 54)
        log.info(f"CALL ENDED  sid={self.call_sid}  duration={duration}s  turns={len(self.conversation)}")
        log.info(f"  caller : {self.customer_phone_number or '(unknown)'}")
        log.info(f"  name   : {ls.name!r}")
        log.info(f"  phone  : {ls.phone!r}")
        log.info(f"  email  : {ls.email!r}")
        log.info(f"  case   : {ls.use_case!r}")
        log.info(f"  saved  : {ls.saved}")
        log.info("─" * 54)

        if self.conversation:
            await asyncio.to_thread(
                save_call_log_sync,
                self.call_sid or "",
                self.customer_phone_number,
                self.conversation,
                self.call_start_time,
                call_end_time,
            )
        else:
            log.info(f"No conversation to log for call_sid={self.call_sid}")

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
        await self._finalize_and_save()


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

    # Safely extract caller phone — form() only works on POST with multipart/urlencoded
    caller_phone = request.query_params.get("From", "")
    if request.method == "POST":
        try:
            form = await request.form()
            caller_phone = str(form.get("From") or caller_phone)
        except Exception:
            pass

    twiml = build_twiml(stream_url=stream_url, status_callback_url=status_url, caller_phone=caller_phone)
    return Response(content=twiml, media_type="application/xml")


@app.post("/twilio/stream-status")
async def twilio_stream_status(request: Request) -> dict[str, Any]:
    raw_body = (await request.body()).decode("utf-8")
    payload = dict(parse_qsl(raw_body, keep_blank_values=True))
    stream_status = payload.get("StreamStatus", "unknown")
    call_sid = payload.get("CallSid", "")
    log.info(f"Twilio stream [{stream_status}]  call_sid={call_sid}")
    if stream_status == "stopped":
        log.info(
            f"  CallStatus={payload.get('CallStatus', '')}  "
            f"ErrorCode={payload.get('ErrorCode', 'none')}  "
            f"Timestamp={payload.get('Timestamp', '')}"
        )
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
        await bridge._finalize_and_save()
