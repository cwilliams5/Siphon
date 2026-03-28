"""ffmpeg-based segment cutting for ad removal."""

from __future__ import annotations

import logging
import os
import subprocess
import tempfile

logger = logging.getLogger(__name__)


def extract_audio(video_path: str, output_path: str) -> None:
    """Extract audio track from a video file for Whisper transcription."""
    cmd = [
        "ffmpeg", "-y",
        "-i", video_path,
        "-vn",                    # no video
        "-acodec", "pcm_s16le",   # WAV format for Whisper
        "-ar", "16000",           # 16kHz sample rate (Whisper's native rate)
        "-ac", "1",               # mono
        output_path,
    ]
    logger.info("Extracting audio from %s to %s", video_path, output_path)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg audio extraction failed: {result.stderr[:300]}")


def get_duration(file_path: str) -> float:
    """Get the duration of a media file in seconds."""
    cmd = [
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "csv=p=0",
        file_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {result.stderr[:300]}")
    return float(result.stdout.strip())


def validate_file(file_path: str) -> bool:
    """Validate a media file using ffprobe. Returns True if the file is valid."""
    if not os.path.exists(file_path):
        return False
    cmd = [
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "csv=p=0",
        file_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        logger.warning("File validation failed for %s: %s", file_path, result.stderr[:200])
        return False
    try:
        dur = float(result.stdout.strip())
        if dur <= 0:
            logger.warning("File validation failed for %s: duration is %s", file_path, dur)
            return False
    except (ValueError, TypeError):
        logger.warning("File validation failed for %s: could not parse duration", file_path)
        return False
    return True


def cut_segments(
    input_path: str,
    segments: list[dict],
    output_path: str | None = None,
) -> str:
    """Remove ad segments from a media file using ffmpeg concat demuxer.

    Segments are the parts to REMOVE. This function inverts them to get
    keep-ranges, then concatenates those ranges.

    Writes to a temp file first and validates before replacing the original.
    If validation fails, the original file is preserved.

    If output_path is None, overwrites the input file.
    Returns the final output path.
    """
    if not segments:
        logger.info("No segments to cut, skipping")
        return input_path

    # Get total duration
    total_duration = get_duration(input_path)

    # Sort segments by start time and merge overlaps
    sorted_segs = sorted(segments, key=lambda s: s["start"])
    merged = []
    for seg in sorted_segs:
        if merged and seg["start"] <= merged[-1]["end"]:
            merged[-1]["end"] = max(merged[-1]["end"], seg["end"])
        else:
            merged.append({"start": seg["start"], "end": seg["end"]})

    # Invert to get keep ranges
    keep_ranges = []
    prev_end = 0.0
    for seg in merged:
        if seg["start"] > prev_end:
            keep_ranges.append((prev_end, seg["start"]))
        prev_end = seg["end"]
    if prev_end < total_duration:
        keep_ranges.append((prev_end, total_duration))

    if not keep_ranges:
        logger.warning("All content would be cut — skipping")
        return input_path

    logger.info(
        "Cutting %d segments from %s, keeping %d ranges",
        len(merged), input_path, len(keep_ranges),
    )

    ext = os.path.splitext(input_path)[1]

    # Use temp dir for intermediate files
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create individual segment files
        segment_files = []
        for i, (start, end) in enumerate(keep_ranges):
            seg_path = os.path.join(tmpdir, f"seg_{i:04d}{ext}")
            cmd = [
                "ffmpeg", "-y",
                "-i", input_path,
                "-ss", str(start),
                "-to", str(end),
                "-c", "copy",
                seg_path,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                logger.error("ffmpeg segment extraction failed: %s", result.stderr[:300])
                raise RuntimeError(f"ffmpeg segment extraction failed for range {start}-{end}")
            segment_files.append(seg_path)

        # Create concat list file
        list_path = os.path.join(tmpdir, "concat.txt")
        with open(list_path, "w") as f:
            for seg_path in segment_files:
                # ffmpeg concat requires forward slashes and escaped single quotes
                escaped = seg_path.replace("\\", "/").replace("'", "'\\''")
                f.write(f"file '{escaped}'\n")

        # Concat to temp file (not directly to output)
        temp_output = os.path.join(tmpdir, f"output{ext}")
        cmd = [
            "ffmpeg", "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", list_path,
            "-c", "copy",
            temp_output,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            logger.error("ffmpeg concat failed: %s", result.stderr[:300])
            raise RuntimeError(f"ffmpeg concat failed: {result.stderr[:300]}")

        # Validate the output before replacing the original
        if not validate_file(temp_output):
            raise RuntimeError(f"Cut output failed validation for {input_path}")

        # Move validated output to final destination
        import shutil
        final = output_path or input_path
        shutil.move(temp_output, final)
        return final
