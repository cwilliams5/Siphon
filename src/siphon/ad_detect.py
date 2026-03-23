"""Ad segment detection using Claude CLI with structured output."""

from __future__ import annotations

import json
import logging
import subprocess
from typing import Any

from siphon.config import LLMConfig, ResolvedFeed

logger = logging.getLogger(__name__)

# JSON Schema for Claude's structured output
AD_SEGMENTS_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "segments": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "start": {"type": "number"},
                    "end": {"type": "number"},
                    "type": {"type": "string"},
                    "label": {"type": "string"},
                    "confidence": {"type": "number"},
                },
                "required": ["start", "end", "label", "confidence"],
            },
        },
    },
    "required": ["segments"],
})


def resolve_prompt(feed: ResolvedFeed, llm_config: LLMConfig) -> str:
    """Build the final prompt for ad detection.

    If claude_prompt_override is set, use it exclusively.
    Otherwise, use the global default_ad_prompt + any claude_prompt_extra.
    """
    if feed.claude_prompt_override:
        return feed.claude_prompt_override
    prompt = llm_config.default_ad_prompt
    if feed.claude_prompt_extra:
        prompt += "\n\nAdditional instructions:\n" + feed.claude_prompt_extra
    return prompt


def build_transcript_for_claude(
    transcript_text: str,
    segments: list,
    words: list | None,
) -> str:
    """Build a dual-format transcript for Claude with segments and word timestamps.

    When word timestamps are available, produces:
        SEGMENTS (for understanding content):
        [0:00-0:45] Welcome to the show...
        ...

        WORD TIMESTAMPS (for precise cut points):
        0.00 Welcome
        0.31 to
        ...

    When words are not available, falls back to segment-only format.
    """
    parts = []

    # Format segments as [M:SS-M:SS] text
    if segments:
        parts.append("SEGMENTS (for understanding content):")
        for seg in segments:
            start_m, start_s = divmod(int(seg["start"]), 60)
            end_m, end_s = divmod(int(seg["end"]), 60)
            parts.append(f"[{start_m}:{start_s:02d}-{end_m}:{end_s:02d}] {seg['text']}")
        parts.append("")

    # Format word-level timestamps if available
    if words:
        parts.append("WORD TIMESTAMPS (for precise cut points):")
        for w in words:
            parts.append(f"{w['start']:.2f} {w['word']}")
        parts.append("")

    if parts:
        return "\n".join(parts)

    # Ultimate fallback: just use the raw text
    return transcript_text


def detect_ads(
    transcript_text: str,
    prompt: str,
    model: str = "claude-sonnet-4-6",
    effort: str = "medium",
    words: list | None = None,
    segments: list | None = None,
) -> dict[str, Any]:
    """Invoke Claude CLI to detect ad segments in a transcript.

    Returns the structured output dict: {"segments": [...]}
    """
    formatted = build_transcript_for_claude(
        transcript_text, segments or [], words,
    )
    full_prompt = f"{prompt}\n\nTRANSCRIPT:\n{formatted}"

    cmd = [
        "claude",
        "-p",
        "--model", model,
        "--output-format", "json",
        "--json-schema", AD_SEGMENTS_SCHEMA,
        "--effort", effort,
    ]

    logger.info("Running Claude CLI for ad detection (model=%s, effort=%s, prompt_len=%d)",
                model, effort, len(full_prompt))

    # Launch Claude CLI at below-normal priority
    import sys
    creationflags = 0x00004000 if sys.platform == "win32" else 0  # BELOW_NORMAL_PRIORITY_CLASS

    result = subprocess.run(
        cmd,
        input=full_prompt,
        capture_output=True,
        text=True,
        timeout=300,  # 5 minute timeout
        creationflags=creationflags,
    )

    if result.returncode != 0:
        logger.error("Claude CLI failed (rc=%d): %s", result.returncode, result.stderr[:500])
        raise RuntimeError(f"Claude CLI failed with exit code {result.returncode}: {result.stderr[:200]}")

    # Parse the JSON output — claude --output-format json wraps in a result envelope
    try:
        output = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        logger.error("Claude CLI output is not valid JSON: %s", result.stdout[:500])
        raise RuntimeError(f"Claude CLI output is not valid JSON: {e}") from e

    # Extract structured_output from the envelope
    if "structured_output" in output:
        return output["structured_output"]

    # If the output is already the segments dict
    if "segments" in output:
        return output

    logger.warning("Unexpected Claude CLI output structure: %s", list(output.keys()))
    return {"segments": []}


def filter_segments(
    segments: list[dict],
    confidence_threshold: float = 0.75,
    min_duration: int = 7,
    max_duration: int = 300,
) -> tuple[list[dict], list[dict]]:
    """Filter detected segments by confidence and duration.

    Returns (high_confidence, marginal) where:
    - high_confidence: segments to cut (confidence >= threshold, valid duration)
    - marginal: segments between 0.5 and threshold (logged but not cut)
    """
    high_confidence = []
    marginal = []

    for seg in segments:
        duration = seg.get("end", 0) - seg.get("start", 0)
        confidence = seg.get("confidence", 0)

        # Skip segments outside duration bounds
        if duration < min_duration or duration > max_duration:
            logger.debug(
                "Skipping segment %.1f-%.1f (duration=%.1fs, outside bounds %d-%d)",
                seg.get("start", 0), seg.get("end", 0), duration, min_duration, max_duration,
            )
            continue

        if confidence >= confidence_threshold:
            high_confidence.append(seg)
        elif confidence >= 0.5:
            marginal.append(seg)
            logger.info(
                "Marginal detection: %.1f-%.1f '%s' (confidence=%.2f)",
                seg.get("start", 0), seg.get("end", 0),
                seg.get("label", ""), confidence,
            )

    return high_confidence, marginal
