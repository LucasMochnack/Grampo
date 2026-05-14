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


def get_cached(db: Session, url: str) -> str | None:
    """Return cached transcription text, or None if not yet transcribed."""
    row = db.get(AudioTranscription, _url_hash(url))
    return row.transcription if row else None


def transcribe_url(db: Session, url: str, mime: str = "") -> str:
    """Download audio from `url` and transcribe via Whisper.

    Returns the transcription text.  Raises ValueError/RuntimeError on failure.
    Idempotent: if already transcribed, returns cached result immediately.
    """
    if not settings.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY não configurada — transcrição indisponível")

    # Check cache first
    cached = get_cached(db, url)
    if cached is not None:
        return cached

    # Download audio
    try:
        resp = httpx.get(url, follow_redirects=True, timeout=_DOWNLOAD_TIMEOUT)
        resp.raise_for_status()
    except Exception as exc:
        raise RuntimeError(f"Falha ao baixar áudio: {exc}") from exc

    audio_bytes = resp.content
    if len(audio_bytes) > _MAX_AUDIO_BYTES:
        raise ValueError(f"Arquivo muito grande ({len(audio_bytes)//1024} KB > 25 MB)")
    if len(audio_bytes) < 512:
        raise ValueError("Arquivo de áudio vazio ou corrompido")

    # Transcribe
    try:
        import openai as _openai
        client = _openai.OpenAI(api_key=settings.OPENAI_API_KEY)
        filename = _guess_filename(url, mime)
        result = client.audio.transcriptions.create(
            model="whisper-1",
            file=(filename, io.BytesIO(audio_bytes)),
            language="pt",
            response_format="verbose_json",
        )
        text = (result.text or "").strip()
        duration_s = int(result.duration) if hasattr(result, "duration") and result.duration else None
    except Exception as exc:
        logger.error("Whisper API error: %s", exc)
        raise RuntimeError(f"Erro na API de transcrição: {exc}") from exc

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
