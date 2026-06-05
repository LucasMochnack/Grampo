"""
Audio transcription via OpenAI Whisper.

Usage:
    from app.services.transcription import transcribe_url
    text = transcribe_url(db, "https://cdn.zenvia.com/.../audio.ogg")

The result is cached in `audio_transcriptions` table — the same URL is only
transcribed once.  Caching key is SHA-256 of the raw URL (query-string
stripped) so pre-signed S3 URLs with different expiry tokens still hit the
same cache entry.
"""
from __future__ import annotations

import hashlib
import io
import logging
from urllib.parse import urlparse, urlunparse

import httpx
from sqlalchemy.orm import Session

from app.config import settings
from app.models import AudioTranscription

logger = logging.getLogger(__name__)

_DOWNLOAD_TIMEOUT = 30   # seconds — audio files can be large
_MAX_AUDIO_BYTES  = 25 * 1024 * 1024   # 25 MB — OpenAI hard limit


def _strip_query(url: str) -> str:
    """Remove query-string from URL so pre-signed expiry tokens don't bust the cache."""
    p = urlparse(url)
    return urlunparse(p._replace(query="", fragment=""))


def _url_hash(url: str) -> str:
    return hashlib.sha256(_strip_query(url).encode()).hexdigest()


def _guess_filename(url: str, mime: str) -> str:
    """Return a filename hint for the OpenAI API based on URL extension or MIME."""
    path = urlparse(url).path.lower()
    for ext in (".mp3", ".ogg", ".oga", ".opus", ".m4a", ".aac", ".wav", ".3gp", ".flac", ".mp4"):
        if path.endswith(ext):
            return f"audio{ext}"
    mime_ext = {
        "audio/ogg": ".ogg", "audio/opus": ".opus", "audio/mpeg": ".mp3",
        "audio/mp4": ".m4a", "audio/aac": ".aac", "audio/wav": ".wav",
        "audio/webm": ".webm", "audio/3gpp": ".3gp",
    }
    ext = mime_ext.get(mime, ".ogg")
    return f"audio{ext}"


def _transcriber():
    """Return (client, model, provider) for whichever Whisper key is set.

    Groq (free tier, no card) is preferred; OpenAI (paid) is the fallback.
    Both expose the identical OpenAI-style ``audio.transcriptions.create``
    API — only the base_url and model id differ, so the call site is the same.
    Returns (None, None, None) when no key is configured.
    """
    import openai as _openai
    if settings.GROQ_API_KEY:
        client = _openai.OpenAI(
            api_key=settings.GROQ_API_KEY,
            base_url="https://api.groq.com/openai/v1",
        )
        return client, "whisper-large-v3", "groq"
    if settings.OPENAI_API_KEY:
        return _openai.OpenAI(api_key=settings.OPENAI_API_KEY), "whisper-1", "openai"
    return None, None, None


def get_cached(db: Session, url: str) -> str | None:
    """Return cached transcription text, or None if not yet transcribed."""
    row = db.get(AudioTranscription, _url_hash(url))
    return row.transcription if row else None


def transcribe_url(db: Session, url: str, mime: str = "") -> str:
    """Download audio from `url` and transcribe via Whisper.

    Returns the transcription text.  Raises ValueError/RuntimeError on failure.
    Idempotent: if already transcribed, returns cached result immediately.
    """
    client, model, provider = _transcriber()
    if client is None:
        raise RuntimeError(
            "Transcrição indisponível — configure GROQ_API_KEY (grátis) ou "
            "OPENAI_API_KEY nas variáveis do Railway"
        )

    # Check cache first
    cached = get_cached(db, url)
    if cached is not None:
        return cached

    # SSRF guard: HTTPS only + exact host suffix on the Zenvia/S3/GCS CDNs, and
    # do NOT follow redirects (blocks fetching cloud metadata / internal hosts).
    from urllib.parse import urlparse as _urlparse
    _allowed = ("zenvia.com", "zenviamobile.com.br", "amazonaws.com", "storage.googleapis.com")
    _p = _urlparse(url)
    _h = (_p.hostname or "").lower()
    if _p.scheme != "https" or not any(_h == d or _h.endswith("." + d) for d in _allowed):
        raise ValueError("URL de áudio não permitida")

    # Download audio
    try:
        resp = httpx.get(url, follow_redirects=False, timeout=_DOWNLOAD_TIMEOUT)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("Audio download failed: %s", exc)
        raise RuntimeError("Falha ao baixar o áudio") from exc

    audio_bytes = resp.content
    if len(audio_bytes) > _MAX_AUDIO_BYTES:
        raise ValueError(f"Arquivo muito grande ({len(audio_bytes)//1024} KB > 25 MB)")
    if len(audio_bytes) < 512:
        raise ValueError("Arquivo de áudio vazio ou corrompido")

    # Transcribe (Groq or OpenAI — same API)
    try:
        filename = _guess_filename(url, mime)
        result = client.audio.transcriptions.create(
            model=model,
            file=(filename, io.BytesIO(audio_bytes)),
            language="pt",
            response_format="verbose_json",
        )
        text = (result.text or "").strip()
        duration_s = int(result.duration) if hasattr(result, "duration") and result.duration else None
    except Exception as exc:
        logger.error("Whisper API error (%s): %s", provider, exc)
        raise RuntimeError("Erro na API de transcrição") from exc

    if not text:
        text = "[áudio sem fala detectada]"

    # Store in cache
    try:
        row = AudioTranscription(
            url_hash=_url_hash(url),
            audio_url=url,
            transcription=text,
            duration_s=duration_s,
        )
        db.merge(row)   # upsert in case of race condition
        db.commit()
    except Exception as exc:
        logger.warning("Failed to cache transcription: %s", exc)
        db.rollback()

    return text
