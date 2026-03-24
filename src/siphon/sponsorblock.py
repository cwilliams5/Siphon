import json

import httpx
import logging

logger = logging.getLogger(__name__)


def get_segment_info(video_id: str, categories: list[str] | None = None) -> tuple[int, float]:
    """Query SponsorBlock API for segments of a video.
    Returns (count, total_seconds_removed). Returns (0, 0.0) on error."""
    try:
        params: dict = {"videoID": video_id}
        if categories:
            params["categories"] = json.dumps(categories)
        resp = httpx.get(
            "https://sponsor.ajay.app/api/skipSegments",
            params=params,
            timeout=5,
        )
        if resp.status_code == 200:
            segments = resp.json()
            count = len(segments)
            total_secs = sum(
                seg["segment"][1] - seg["segment"][0]
                for seg in segments
                if "segment" in seg and len(seg["segment"]) == 2
            )
            return count, round(total_secs, 1)
        return 0, 0.0
    except Exception:
        return 0, 0.0


def get_segment_count(video_id: str, categories: list[str] | None = None) -> int:
    """Query SponsorBlock API for the number of segments for a video.
    Returns 0 if no segments found or API is unreachable."""
    count, _ = get_segment_info(video_id, categories)
    return count
