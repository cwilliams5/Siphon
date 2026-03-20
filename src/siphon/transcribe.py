"""Whisper transcription utility using faster-whisper."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def transcribe(
    audio_path: str,
    model_size: str = "base",
    device: str = "cpu",
) -> dict[str, Any]:
    """Transcribe an audio file using faster-whisper.

    Returns a dict with:
        {
            "segments": [
                {"start": 0.0, "end": 5.2, "text": "Hello world..."}
            ],
            "text": "Full transcript text..."
        }
    """
    from faster_whisper import WhisperModel

    logger.info("Loading Whisper model %s on %s", model_size, device)
    compute_type = "float16" if device == "cuda" else "int8"
    model = WhisperModel(model_size, device=device, compute_type=compute_type)

    logger.info("Transcribing %s", audio_path)
    segments_iter, info = model.transcribe(audio_path, beam_size=5)

    segments = []
    full_text_parts = []
    for seg in segments_iter:
        segments.append({
            "start": round(seg.start, 2),
            "end": round(seg.end, 2),
            "text": seg.text.strip(),
        })
        full_text_parts.append(seg.text.strip())

    full_text = " ".join(full_text_parts)
    logger.info(
        "Transcribed %s: %d segments, %.1f seconds, language=%s",
        audio_path, len(segments), info.duration, info.language,
    )

    return {
        "segments": segments,
        "text": full_text,
        "language": info.language,
        "duration": info.duration,
    }
