"""Whisper transcription utility using faster-whisper."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def transcribe(
    audio_path: str,
    model_size: str = "base",
    device: str = "cpu",
    word_timestamps: bool = True,
) -> dict[str, Any]:
    """Transcribe an audio file using faster-whisper.

    Returns a dict with:
        {
            "segments": [
                {"start": 0.0, "end": 5.2, "text": "Hello world..."}
            ],
            "words": [
                {"word": "Hello", "start": 0.0, "end": 0.3},
                {"word": "world", "start": 0.4, "end": 0.7},
            ],
            "text": "Full transcript text...",
            "language": "en",
            "duration": 300.0,
        }
    """
    from faster_whisper import WhisperModel

    logger.info("Loading Whisper model %s on %s", model_size, device)
    compute_type = "float16" if device == "cuda" else "int8"
    model = WhisperModel(model_size, device=device, compute_type=compute_type)

    logger.info("Transcribing %s (word_timestamps=%s)", audio_path, word_timestamps)
    segments_iter, info = model.transcribe(
        audio_path, beam_size=5, word_timestamps=word_timestamps,
    )

    segments = []
    words = []
    full_text_parts = []
    for seg in segments_iter:
        segments.append({
            "start": round(seg.start, 2),
            "end": round(seg.end, 2),
            "text": seg.text.strip(),
        })
        full_text_parts.append(seg.text.strip())

        # Extract word-level timestamps when available
        if word_timestamps and hasattr(seg, "words") and seg.words:
            for w in seg.words:
                words.append({
                    "word": w.word,
                    "start": round(w.start, 2),
                    "end": round(w.end, 2),
                })

    full_text = " ".join(full_text_parts)
    logger.info(
        "Transcribed %s: %d segments, %d words, %.1f seconds, language=%s",
        audio_path, len(segments), len(words), info.duration, info.language,
    )

    return {
        "segments": segments,
        "words": words,
        "text": full_text,
        "language": info.language,
        "duration": info.duration,
    }
