"""
TrueAI Lab — Voice Agent Sales Assistant
=========================================
Low-latency inbound voice agent using Gemini Live API (WebSocket).
Collects lead info (name, phone, email, use case) and saves via n8n webhook.

Usage:
    1. Copy .env.example to .env and fill in your keys
    2. pip install -r requirements.txt
    3. python agent.py

The agent speaks first — greeting is triggered immediately after connection.
Press Ctrl+C to stop.
"""

import asyncio
import base64
import json
import logging
import os
import queue
import re
import sys
import threading
from datetime import datetime

import requests
import sounddevice as sd
import websockets
from dotenv import load_dotenv

load_dotenv(override=True)

# ─── Configuration ────────────────────────────────────────────

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "")
MODEL = os.getenv("GEMINI_MODEL", "models/gemini-3.1-flash-live-preview")

# Audio config — PCM 16-bit 16kHz mono (what Gemini expects/returns)
SAMPLE_RATE = 16000
AUDIO_OUTPUT_SAMPLE_RATE = 24000
CHANNELS = 1
CHUNK_SIZE = 1600  # 100ms chunks — sweet spot for latency vs overhead
PCM_DTYPE = "int16"
BYTES_PER_SAMPLE = 2

# WebSocket endpoint
WS_ENDPOINT = (
    "wss://generativelanguage.googleapis.com/ws/"
    "google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent"
)

# ─── Logging ──────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("voice-agent")

# ─── System Prompt ────────────────────────────────────────────


SYSTEM_PROMPT = """You are Maya, the AI receptionist for TrueAI Lab. You sound like a real, experienced front-desk receptionist - warm, natural, confident, and never robotic or pushy.

HOW TO SPEAK
- Keep every response short and conversational. Prefer 1-2 short sentences, and only go longer if the caller asks for more.
- Never read out lists or sound scripted.
- Never say things like "I have noted your details" or "I will proceed." Just do it and confirm naturally.
- Use the caller's name occasionally, but not in every sentence.
- Never repeat yourself.
- If the caller asks you to speak in Tamil, switch to casual Chennai Tamil without any formal or ancient tamil usage.

STARTING THE CALL
- Always open with this exact short greeting: "Hi, this is Maya from TrueAI Lab. How can I help you today?"
- Do not ask for a name or phone number at the start. Just listen first.
- Never repeat the full greeting if interrupted.

ABOUT TRUEAI LAB
- TrueAI Lab builds production-grade AI voice agents, workflow automation, and intelligent systems for businesses.
- When someone asks if the company is good or trustworthy, answer warmly and confidently like a proud team member.
- When someone asks about services or pricing, explain conversationally using your knowledge of TrueAI Lab. Never answer as a list.
- If pricing comes up, say it depends on the complexity and an engineer will walk them through options that fit their needs.
- Stay focused on the caller and their use case. Do not drift into generic AI explanations.

COLLECTING DETAILS
- Only collect details when the caller clearly wants follow-up, a meeting, a callback, or serious project discussion.
- Collect in this order: full name, phone number, email address, then their specific use case.
- Ask for only one detail at a time.
- After the caller gives their name, confirm it naturally: "Got it - just to confirm, that's [Name], right?"
- If they correct the name, acknowledge it naturally and use the corrected version.
- After the caller gives their email, read it back naturally and confirm it once.
- Always collect the phone number with country code. If it is missing, ask once naturally: "Could you include your country code as well?"
- Never ask for the same detail again once it has been confirmed.

WEBHOOK AND TOOL RULES
- The only available tool is save_lead, which sends the existing webhook.
- Calling save_lead is mandatory once you have name, phone, email, and use_case.
- The moment you have all four fields, call save_lead before the final confirmation.
- Before calling the tool, say naturally: "Just a moment" or "Let me check that for you."
- Do not mention the tool or webhook to the caller.
- If the tool fails, say: "I'm sorry about that - let me try that again."

AFTER THE WEBHOOK IS DONE
- Keep it minimal and natural.
- After save_lead succeeds, say something like: "Perfect, you're all set. We'll be in touch soon. Do you need any other help today?"
- Keep that short. Do not give a long closing unless the caller is ready to end the call.

IMPORTANT RULES
- Never ask more than one question at a time.
- Never suggest booking a meeting unless the caller brings it up.
- If they are not interested, be gracious and brief.
"""

# ─── Audio Playback (non-blocking, interruptible) ────────────

class AudioPlayer:
    """Thread-safe audio playback with instant flush for barge-in."""

    def __init__(self):
        self.audio_queue: queue.Queue[bytes | None] = queue.Queue()
        self._pending = bytearray()
        self._lock = threading.Lock()
        self.stream = sd.RawOutputStream(
            samplerate=AUDIO_OUTPUT_SAMPLE_RATE,
            channels=CHANNELS,
            dtype=PCM_DTYPE,
            blocksize=2400,
            callback=self._callback,
        )
        self.stream.start()

    def _callback(self, outdata, frames, time_info, status):
        if status:
            log.warning(f"Playback status: {status}")

        needed_bytes = frames * CHANNELS * BYTES_PER_SAMPLE

        with self._lock:
            while len(self._pending) < needed_bytes:
                try:
                    chunk = self.audio_queue.get_nowait()
                except queue.Empty:
                    break

                if chunk:
                    self._pending.extend(chunk)

            chunk = bytes(self._pending[:needed_bytes])
            del self._pending[:needed_bytes]

        if len(chunk) < needed_bytes:
            chunk += b"\x00" * (needed_bytes - len(chunk))

        outdata[:] = chunk

    def enqueue(self, pcm_data: bytes):
        """Add audio chunk to playback queue — starts playing immediately."""
        self.audio_queue.put(pcm_data)

    def flush(self):
        """Instantly clear all queued audio — called on barge-in."""
        with self._lock:
            self._pending.clear()
            while True:
                try:
                    self.audio_queue.get_nowait()
                except queue.Empty:
                    break

    def stop(self):
        self.flush()
        self.stream.stop()
        self.stream.close()


# ─── Microphone Capture (non-blocking) ───────────────────────

class MicCapture:
    """Captures mic audio in a background thread, feeds chunks to an async queue."""

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.async_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._running = True
        self.stream = sd.RawInputStream(
            samplerate=SAMPLE_RATE,
            channels=CHANNELS,
            dtype=PCM_DTYPE,
            blocksize=CHUNK_SIZE,
            callback=self._callback,
        )
        self.stream.start()

    def _callback(self, in_data, frame_count, time_info, status):
        if status:
            log.warning(f"Mic status: {status}")
        if self._running and in_data:
            self.loop.call_soon_threadsafe(self.async_queue.put_nowait, bytes(in_data))

    def stop(self):
        self._running = False
        self.stream.stop()
        self.stream.close()


# ─── n8n Webhook ──────────────────────────────────────────────

def call_n8n_webhook(lead_data: dict) -> dict:
    """POST lead data to n8n webhook. Returns success/error status."""
    if not N8N_WEBHOOK_URL:
        log.warning("N8N_WEBHOOK_URL not set — skipping webhook call")
        return {"success": False, "error": "Webhook URL not configured"}

    try:
        payload = {
            "name": lead_data.get("name", ""),
            "phone": lead_data.get("phone", ""),
            "email": lead_data.get("email", ""),
            "use_case": lead_data.get("use_case", ""),
            "timestamp": datetime.now().isoformat(),
            "source": "gemini-voice-agent",
        }
        log.info(f"Calling n8n webhook with: {json.dumps(payload, indent=2)}")

        resp = requests.post(
            N8N_WEBHOOK_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        log.info(f"Webhook response: {resp.status_code}")
        return {"success": True, "message": "Lead saved successfully"}

    except requests.RequestException as e:
        log.error(f"Webhook failed: {e}")
        return {"success": False, "error": str(e)}


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
        r"^(my name is|this is|i am|i'm|im|it is|it's)\s+",
        "",
        text.strip(),
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"[^\w\s'.-]", "", cleaned)
    return _normalize_spaces(cleaned)


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
        if any(phrase in text for phrase in ["full name", "your name", "who am i speaking with", "who's this"]):
            self.expected_field = "name"
        elif any(phrase in text for phrase in ["phone number", "best number", "reach you at"]):
            self.expected_field = "phone"
        elif "email" in text:
            self.expected_field = "email"
        elif any(phrase in text for phrase in ["use case", "what do you want", "what would you like", "what should the voice agent do"]):
            self.expected_field = "use_case"

    def merge(self, data: dict):
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

        if self.expected_field == "name" and not self.name:
            self.name = _clean_name(text)
        elif self.expected_field == "phone" and not self.phone and phone:
            self.phone = phone
        elif self.expected_field == "email" and not self.email and email:
            self.email = email
        elif self.expected_field == "use_case" and not self.use_case:
            self.use_case = _clean_use_case(text)

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

    def as_payload(self) -> dict:
        return {
            "name": self.name,
            "phone": self.phone,
            "email": self.email,
            "use_case": self.use_case,
        }


# ─── Voice Agent ──────────────────────────────────────────────

class VoiceAgent:
    """
    Gemini Live API voice agent with:
    - Instant initial greeting (agent speaks first)
    - Low-latency barge-in support
    - Function calling for lead capture → n8n webhook
    - Real-time transcription logging
    """

    def __init__(self):
        self.ws = None
        self.player = AudioPlayer()
        self.mic = None
        self.is_ready = False
        self.session_handle = None
        self.lead_state = LeadState()
        self._running = True
        self._cleaned_up = False

    def _build_setup_message(self) -> dict:
        """Build the BidiGenerateContentSetup message with all low-latency optimizations."""
        return {
            "setup": {
                "model": MODEL,
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
                "systemInstruction": {
                    "parts": [{"text": SYSTEM_PROMPT}]
                },
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
                                            "description": "The caller's phone number",
                                        },
                                        "email": {
                                            "type": "STRING",
                                            "description": "The caller's email address",
                                        },
                                        "use_case": {
                                            "type": "STRING",
                                            "description": (
                                                "What the caller wants to use the voice agent for — "
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
                # ─── Low-latency realtime input config ────────────
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
                    # Barge-in enabled — caller can interrupt the agent
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

    async def _trigger_initial_greeting(self):
        """
        Force the agent to speak first by sending realtime text input.
        This is the key pattern for inbound agents — the model needs a
        trigger to start talking since there's no user audio yet.
        """
        log.info("Triggering initial greeting...")
        await self.ws.send(json.dumps({
            "realtimeInput": {
                "text": "The call has connected. Greet the caller now as Maya from TrueAI Lab."
            }
        }))

    async def _handle_tool_call(self, function_calls: list):
        """Execute function calls and send responses back to Gemini."""
        responses = []
        for call in function_calls:
            fn_name = call.get("name")
            fn_args = call.get("args", {})
            fn_id = call.get("id")

            log.info(f"Tool call: {fn_name}({json.dumps(fn_args)})")

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

            responses.append({
                "id": fn_id,
                "name": fn_name,
                "response": result,
            })

        await self.ws.send(json.dumps({
            "toolResponse": {"functionResponses": responses}
        }))
        log.info("Tool response sent")

    async def _maybe_save_lead_fallback(self):
        if self.lead_state.saved or not self.lead_state.has_all_fields():
            return

        result = call_n8n_webhook(self.lead_state.as_payload())
        if not result.get("success"):
            return

        self.lead_state.saved = True
        log.info("Lead auto-saved by fallback webhook logic")
        await self.ws.send(json.dumps({
            "realtimeInput": {
                "text": (
                    "System note: the lead has already been saved successfully. "
                    "Briefly confirm that an engineer will reach out within 24 hours, "
                    "and do not ask for the same contact details again."
                )
            }
        }))

    async def _receive_loop(self):
        """Core receive loop — handles all server messages with zero-latency patterns."""
        try:
            async for raw in self.ws:
                msg = json.loads(raw)

                # ─── Setup Complete ───────────────────────────
                if "setupComplete" in msg:
                    log.info("✓ Session ready")
                    self.is_ready = True
                    # Immediately trigger the greeting — no delay
                    await self._trigger_initial_greeting()
                    continue

                # ─── Server Content (model audio/text output) ─
                if "serverContent" in msg:
                    sc = msg["serverContent"]

                    # BARGE-IN: Flush playback instantly
                    if sc.get("interrupted"):
                        self.player.flush()
                        log.info("⚡ Interrupted — flushed playback")
                        continue

                    # Stream audio to speaker immediately (don't wait for turnComplete)
                    model_turn = sc.get("modelTurn")
                    if model_turn and model_turn.get("parts"):
                        for part in model_turn["parts"]:
                            if "inlineData" in part:
                                audio_b64 = part["inlineData"]["data"]
                                pcm = base64.b64decode(audio_b64)
                                self.player.enqueue(pcm)
                            if "text" in part:
                                sys.stdout.write(part["text"])
                                sys.stdout.flush()

                    # Transcription logging
                    if "inputTranscription" in sc:
                        text = sc["inputTranscription"]["text"]
                        if text.strip():
                            log.info(f"🎤 Caller: {text}")
                            self.lead_state.consume_caller_text(text)
                            await self._maybe_save_lead_fallback()

                    if "outputTranscription" in sc:
                        text = sc["outputTranscription"]["text"]
                        if text.strip():
                            log.info(f"🤖 Agent:  {text}")
                            self.lead_state.update_expected_field(text)

                    if sc.get("turnComplete"):
                        log.debug("Turn complete")

                # ─── Tool Calls ───────────────────────────────
                if "toolCall" in msg:
                    await self._handle_tool_call(msg["toolCall"]["functionCalls"])

                # ─── Tool Call Cancellation ───────────────────
                if "toolCallCancellation" in msg:
                    ids = msg["toolCallCancellation"]["ids"]
                    log.warning(f"Tool calls cancelled: {ids}")

                # ─── Session Resumption ───────────────────────
                if "sessionResumptionUpdate" in msg:
                    update = msg["sessionResumptionUpdate"]
                    if update.get("resumable"):
                        self.session_handle = update["newHandle"]
                        log.debug("Session handle cached")

                # ─── GoAway ───────────────────────────────────
                if "goAway" in msg:
                    log.warning(f"Server GoAway: {msg['goAway'].get('timeLeft')}")

                # ─── Usage Metadata ───────────────────────────
                if "usageMetadata" in msg:
                    um = msg["usageMetadata"]
                    total = um.get("totalTokenCount", 0)
                    if total > 0:
                        log.debug(f"Tokens: {total}")

        except websockets.ConnectionClosed as e:
            log.info(f"Connection closed: {e.code} — {e.reason}")
        except Exception as e:
            log.error(f"Receive error: {e}", exc_info=True)

    async def _send_audio_loop(self):
        """Stream mic audio to Gemini as realtimeInput (not clientContent)."""
        while self._running:
            if not self.is_ready or not self.mic or not self.ws:
                await asyncio.sleep(0.05)
                continue

            try:
                chunk = await asyncio.wait_for(
                    self.mic.async_queue.get(), timeout=0.2
                )
                audio_b64 = base64.b64encode(chunk).decode("ascii")
                await self.ws.send(json.dumps({
                    "realtimeInput": {
                        "audio": {
                            "mimeType": "audio/pcm;rate=16000",
                            "data": audio_b64,
                        }
                    }
                }))
            except asyncio.TimeoutError:
                continue
            except websockets.ConnectionClosed:
                break
            except Exception as e:
                log.error(f"Audio send error: {e}")
                break

    async def run(self):
        """Main entry point — connect, setup, stream, and handle everything."""
        if not GEMINI_API_KEY:
            log.error("Set GEMINI_API_KEY in your .env file")
            return

        uri = f"{WS_ENDPOINT}?key={GEMINI_API_KEY}"

        log.info("Connecting to Gemini Live API...")
        log.info(f"Model: {MODEL}")
        log.info(f"Webhook: {N8N_WEBHOOK_URL or '(not configured)'}")
        print()
        print("=" * 60)
        print("  TrueAI Lab — Voice Agent (Gemini Live API)")
        print("  Speak into your mic. Press Ctrl+C to stop.")
        print("=" * 60)
        print()

        try:
            async with websockets.connect(
                uri,
                max_size=10 * 1024 * 1024,  # 10MB max message
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                self.ws = ws

                # 1. Send setup (first message, exactly once)
                setup_msg = self._build_setup_message()
                await ws.send(json.dumps(setup_msg))
                log.info("Setup message sent, waiting for setupComplete...")

                # 2. Start mic capture
                loop = asyncio.get_event_loop()
                self.mic = MicCapture(loop)

                # 3. Run receive + send loops concurrently
                receive_task = asyncio.create_task(self._receive_loop())
                send_task = asyncio.create_task(self._send_audio_loop())

                await asyncio.gather(receive_task, send_task)

        except websockets.InvalidStatusCode as e:
            log.error(f"Connection rejected: HTTP {e.status_code}")
            if e.status_code == 403:
                log.error("Check your GEMINI_API_KEY")
        except ConnectionRefusedError:
            log.error("Connection refused — check your network")
        except KeyboardInterrupt:
            log.info("Shutting down...")
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean shutdown of audio resources."""
        if self._cleaned_up:
            return

        self._cleaned_up = True
        self._running = False
        log.info("Cleaning up...")
        if self.mic:
            try:
                self.mic.stop()
            except Exception as e:
                log.warning(f"Mic cleanup error: {e}")
        try:
            self.player.stop()
        except Exception as e:
            log.warning(f"Player cleanup error: {e}")
        log.info("Done.")


# ─── Entry Point ──────────────────────────────────────────────

if __name__ == "__main__":
    agent = VoiceAgent()
    try:
        asyncio.run(agent.run())
    except KeyboardInterrupt:
        agent.cleanup()
        print("\nBye!")
