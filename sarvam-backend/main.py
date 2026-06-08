"""
Sarvam Voice Agent — FastAPI Backend Proxy
==========================================
Proxies three Sarvam AI API routes:
  POST /api/sarvam/stt         → Saaras v3  (Speech-to-Text)
  POST /api/sarvam/llm/stream  → Sarvam LLM (SSE streaming)
  POST /api/sarvam/tts         → Bulbul v3  (Text-to-Speech)

Run:
    SARVAM_API_KEY=your_key uvicorn main:app --reload --port 3000

Latency optimisations applied
──────────────────────────────
• Persistent httpx.AsyncClient with connection pooling (lifespan) —
  eliminates TCP/TLS handshake overhead (~200-400 ms) per call.
• LLM streaming chunk size 512 → 128 bytes for lower TTFT perception.
• TTS default sample rate 22050 → 16000 Hz (27 % smaller payload).
"""

import os
import httpx
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, AsyncIterator

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SARVAM_API_KEY = os.environ.get("SARVAM_API_KEY")
if not SARVAM_API_KEY:
    raise RuntimeError("❌  SARVAM_API_KEY environment variable is required")

SARVAM_BASE    = "https://api.sarvam.ai"
SARVAM_HEADERS = {"api-subscription-key": SARVAM_API_KEY}

# ── Shared persistent HTTP clients (connection-pooled) ────────────────────────
# One client per logical timeout profile so keep-alive sockets are reused
# across requests instead of being torn down after every call.
_stt_client: httpx.AsyncClient | None = None
_llm_client: httpx.AsyncClient | None = None
_tts_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create long-lived HTTP clients at startup; close sthem at shutdown."""
    global _stt_client, _llm_client, _tts_client

    # Limits: up to 10 keep-alive connections per host
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)

    _stt_client = httpx.AsyncClient(
        timeout=httpx.Timeout(30.0, connect=5.0),
        limits=limits,
        http2=False,   # Sarvam doesn't need HTTP/2; keep False to avoid h2 dep
    )
    _llm_client = httpx.AsyncClient(
        timeout=httpx.Timeout(None, connect=5.0),   # no read timeout for streaming
        limits=limits,
        http2=False,
    )
    _tts_client = httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=5.0),
        limits=limits,
        http2=False,
    )

    log.info("HTTP clients initialised (connection pooling enabled)")
    yield  # ← application runs here

    await _stt_client.aclose()
    await _llm_client.aclose()
    await _tts_client.aclose()
    log.info("HTTP clients closed")


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Sarvam Voice Agent Proxy", version="2.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200", "http://127.0.0.1:4200"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Pydantic models ───────────────────────────────────────────────────────────
class LLMRequest(BaseModel):
    model:            str              = "sarvam-30b"
    messages:         list[dict]
    stream:           bool             = True
    max_tokens:       int              = 128           # 256 → 128: faster, tighter voice answers
    temperature:      float            = 0.3           # 0.7 → 0.3: more decisive, fewer tokens
    reasoning_effort: Optional[str]    = None


class TTSRequest(BaseModel):
    text:                 str
    target_language_code: str   = "hi-IN"
    speaker:              str   = "shubh"
    model:                str   = "bulbul:v3"
    speech_sample_rate:   int   = 16000     # 22050 → 16000: 27 % smaller payload
    response_format:      str   = "wav"
    pace:                 float = 1.0


# ── Route 1: STT — Saaras v3 ─────────────────────────────────────────────────
@app.post("/api/sarvam/stt")
async def speech_to_text(
    file:          UploadFile = File(...),
    model:         str        = Form("saaras:v3"),
    language_code: str        = Form("hi-IN"),
    mode:          str        = Form("transcribe"),
):
    """
    Receives audio from Angular (multipart/form-data),
    forwards to Sarvam Saaras v3 STT, returns transcript JSON.
    Uses the persistent _stt_client (connection-pooled).
    """
    audio_bytes = await file.read()
    log.info(f"STT | lang={language_code} | size={len(audio_bytes)}B | mode={mode}")

    # Normalize content type. The frontend now sends WAV (audio/wav).
    # Keep webm/ogg normalization as a safety net for future changes.
    content_type = file.content_type or "audio/wav"
    if "audio/webm" in content_type:
        content_type = "audio/webm"
    elif "audio/ogg" in content_type:
        content_type = "audio/ogg"
    elif "audio/wav" in content_type or "audio/wave" in content_type:
        content_type = "audio/wav"

    response = await _stt_client.post(
        f"{SARVAM_BASE}/speech-to-text",
        headers=SARVAM_HEADERS,
        files={"file": (file.filename, audio_bytes, content_type)},
        data={
            "model":           model,
            "mode":            mode,
            "with_timestamps": "false",
        },
    )

    log.info(f"STT | status={response.status_code}")

    if response.status_code != 200:
        log.error(f"STT API Error: {response.status_code} - {response.text}")
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()


# ── Route 2: LLM — Streaming SSE ─────────────────────────────────────────────
@app.post("/api/sarvam/llm/stream")
async def llm_stream(body: LLMRequest, request: Request):
    """
    Forwards chat completion request to Sarvam LLM with stream=True.
    Pipes the SSE response back to Angular in real-time.
    Client can abort (barge-in) — the generator checks for disconnect.
    Uses persistent _llm_client (connection-pooled).
    Chunk size reduced 512 → 128 bytes for faster token forwarding.
    """
    log.info(f"LLM | model={body.model} | turns={len(body.messages)}")

    payload = body.model_dump()
    payload["stream"] = True

    async def generate() -> AsyncIterator[bytes]:
        log.info("LLM | Stream generator started")
        async with _llm_client.stream(
            "POST",
            f"{SARVAM_BASE}/v1/chat/completions",
            headers={**SARVAM_HEADERS, "Content-Type": "application/json"},
            json=payload,
        ) as upstream:
            log.info(f"LLM | Upstream status code: {upstream.status_code}")
            if upstream.status_code != 200:
                error_content = await upstream.aread()
                log.error(f"LLM | Upstream error body: {error_content.decode('utf-8')}")
                yield error_content
                return

            # chunk_size 512 → 128: forward smaller chunks faster to reduce TTFT
            async for chunk in upstream.aiter_bytes(chunk_size=128):
                if await request.is_disconnected():
                    log.info("LLM | client disconnected (barge-in)")
                    break
                yield chunk

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",   # disable Nginx buffering if proxied
        },
    )


# ── Route 3: TTS — Bulbul v3 ─────────────────────────────────────────────────
@app.post("/api/sarvam/tts")
async def text_to_speech(body: TTSRequest):
    """
    Sends text to Sarvam Bulbul v3 TTS.
    Returns JSON with base64-encoded WAV in `audios[0]`.
    Called once per sentence (sentence-level dispatch for low latency).
    Uses persistent _tts_client (connection-pooled).
    Default sample rate is now 16000 Hz (smaller payload, faster decode).
    """
    log.info(f"TTS | lang={body.target_language_code} | speaker={body.speaker} | text_len={len(body.text)}")

    payload = {
        "text":                 body.text,
        "speaker":              body.speaker,
        "model":                body.model,
        "target_language_code": body.target_language_code,
        "speech_sample_rate":   body.speech_sample_rate,
        "response_format":      body.response_format,
        "properties": {
            "pace": body.pace
        }
    }

    response = await _tts_client.post(
        f"{SARVAM_BASE}/text-to-speech",
        headers={**SARVAM_HEADERS, "Content-Type": "application/json"},
        json=payload,
    )

    log.info(f"TTS | status={response.status_code}")

    if response.status_code != 200:
        log.error(f"TTS API Error: {response.status_code} - {response.text}")
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()


# ── Health check ──────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "sarvam_key_set": bool(SARVAM_API_KEY)}
