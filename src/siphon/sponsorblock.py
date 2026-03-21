import httpx
import logging

logger = logging.getLogger(__name__)


def get_segment_count(video_id: str, categories: list[str] | None = None) -> int:
    """Query SponsorBlock API for the number of segments for a video.
    Returns 0 if no segments found or API is unreachable."""
    try:
        params: dict = {"videoID": video_id}
        if categories:
            params["categories"] = str(categories)  # JSON array format
        resp = httpx.get(
            "https://sponsor.ajay.app/api/skipSegments",
            params=params,
            timeout=5,
        )
        if resp.status_code == 200:
            return len(resp.json())
        return 0
    except Exception:
        return 0
