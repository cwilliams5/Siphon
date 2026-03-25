"""YouTube Data API v3 integration for video discovery."""

from __future__ import annotations

import logging
import re
import threading
from datetime import datetime, timedelta, timezone

import httpx

logger = logging.getLogger(__name__)

CHANNELS_URL = "https://www.googleapis.com/youtube/v3/channels"
PLAYLIST_ITEMS_URL = "https://www.googleapis.com/youtube/v3/playlistItems"
VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"

# Cooldown state — shared across all API calls
_cooldown_until: datetime | None = None
_cooldown_lock = threading.Lock()


class YouTubeQuotaExceeded(Exception):
    """Raised when the YouTube API returns 403 (quota exceeded) or we're in cooldown."""
    pass


def _check_cooldown() -> None:
    """Raise if we're in cooldown period."""
    with _cooldown_lock:
        if _cooldown_until and datetime.now(timezone.utc) < _cooldown_until:
            remaining = (_cooldown_until - datetime.now(timezone.utc)).total_seconds() / 3600
            raise YouTubeQuotaExceeded(
                f"YouTube API cooling down ({remaining:.1f}h remaining)"
            )


def _set_cooldown(hours: int) -> None:
    """Enter cooldown for the specified number of hours."""
    global _cooldown_until
    with _cooldown_lock:
        _cooldown_until = datetime.now(timezone.utc) + timedelta(hours=hours)
    logger.warning("YouTube API cooldown activated for %d hours", hours)


def _api_get(url: str, params: dict, cooldown_hours: int = 4) -> dict:
    """Make an API GET request with cooldown handling."""
    _check_cooldown()
    resp = httpx.get(url, params=params, timeout=15)
    if resp.status_code == 403:
        _set_cooldown(cooldown_hours)
        raise YouTubeQuotaExceeded(f"YouTube API 403: {resp.text[:200]}")
    resp.raise_for_status()
    return resp.json()


def resolve_channel_id(url: str, api_key: str, cooldown_hours: int = 4) -> str | None:
    """Resolve a YouTube channel URL to a channel ID via the API."""
    # If URL already contains a channel ID
    match = re.search(r"youtube\.com/channel/(UC[\w-]+)", url)
    if match:
        return match.group(1)

    # Extract handle
    handle = None
    match = re.search(r"youtube\.com/@([^\s/]+)", url)
    if match:
        handle = match.group(1)
    if not handle:
        match = re.search(r"youtube\.com/c/([\w-]+)", url)
        if match:
            handle = match.group(1)

    if handle:
        from urllib.parse import unquote
        handle = unquote(handle)
        data = _api_get(CHANNELS_URL, {
            "key": api_key,
            "forHandle": handle,
            "part": "id",
        }, cooldown_hours)
        items = data.get("items", [])
        if items:
            return items[0]["id"]

    return None


def get_channel_metadata(channel_id: str, api_key: str, cooldown_hours: int = 4) -> dict:
    """Get channel title and thumbnail. Cost: 1 unit."""
    data = _api_get(CHANNELS_URL, {
        "key": api_key,
        "id": channel_id,
        "part": "snippet",
    }, cooldown_hours)
    items = data.get("items", [])
    if not items:
        return {}
    snippet = items[0]["snippet"]
    thumbnails = snippet.get("thumbnails", {})
    best_thumb = None
    for size in ("high", "medium", "default"):
        if size in thumbnails:
            best_thumb = thumbnails[size]["url"]
            break
    return {
        "title": snippet.get("title", ""),
        "image_url": best_thumb,
    }


def list_videos(
    channel_id: str,
    api_key: str,
    date_cutoff: str | None = None,
    known_ids: set[str] | None = None,
    max_pages: int = 200,
    cooldown_hours: int = 4,
    country: str = "US",
) -> list[dict]:
    """List videos from a channel using playlistItems (1 unit per 50 videos).

    Uses the channel's "uploads" playlist (UC... -> UU...) which contains
    all videos in reverse chronological order with dates.

    Paginates backwards, stopping at date_cutoff or a known video ID.
    """
    known_ids = known_ids or set()

    # Convert channel ID to uploads playlist ID: UC... -> UU...
    if channel_id.startswith("UC"):
        uploads_playlist = "UU" + channel_id[2:]
    else:
        uploads_playlist = channel_id

    cutoff_iso = None
    if date_cutoff:
        cutoff_iso = f"{date_cutoff[:4]}-{date_cutoff[4:6]}-{date_cutoff[6:8]}T00:00:00Z"

    videos = []
    page_token = None

    for page_num in range(max_pages):
        params = {
            "key": api_key,
            "playlistId": uploads_playlist,
            "part": "snippet",
            "maxResults": 50,
        }
        if page_token:
            params["pageToken"] = page_token

        data = _api_get(PLAYLIST_ITEMS_URL, params, cooldown_hours)

        items = data.get("items", [])
        if not items:
            break

        hit_known = False
        hit_cutoff = False

        for item in items:
            snippet = item.get("snippet", {})
            resource = snippet.get("resourceId", {})
            video_id = resource.get("videoId")
            if not video_id:
                continue

            published = snippet.get("publishedAt", "")
            upload_date = published[:10].replace("-", "") if published else None

            # Stop if we've seen this video before
            if video_id in known_ids:
                hit_known = True
                break

            # Stop if we've gone past the date cutoff
            if cutoff_iso and published and published < cutoff_iso:
                hit_cutoff = True
                break

            # Pick best thumbnail
            thumbnails = snippet.get("thumbnails", {})
            thumb_url = None
            for size in ("high", "medium", "default"):
                if size in thumbnails:
                    thumb_url = thumbnails[size]["url"]
                    break

            videos.append({
                "id": video_id,
                "title": snippet.get("title", ""),
                "description": snippet.get("description", ""),
                "upload_date": upload_date,
                "published_at": published,  # full ISO 8601 for precise delay calc
                "thumbnail": thumb_url,
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "channel": snippet.get("channelTitle", ""),
                "duration": None,
            })

        if hit_known or hit_cutoff:
            break

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    # Fetch durations + region restrictions via videos.list (1 unit per 50 videos)
    if videos:
        _enrich_video_details(videos, api_key, cooldown_hours, country)

    logger.info("YouTube API: %d videos from channel %s (%d pages)",
                len(videos), channel_id, page_num + 1)
    return videos


def _parse_iso8601_duration(s: str) -> int:
    """Parse ISO 8601 duration (e.g. PT4M13S) to seconds."""
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", s)
    if not m:
        return 0
    hours = int(m.group(1) or 0)
    mins = int(m.group(2) or 0)
    secs = int(m.group(3) or 0)
    return hours * 3600 + mins * 60 + secs


def _is_region_blocked(content_details: dict, country: str) -> bool:
    """Check if a video is blocked in the given country."""
    restriction = content_details.get("regionRestriction", {})
    blocked = restriction.get("blocked")
    allowed = restriction.get("allowed")
    if blocked and country.upper() in [c.upper() for c in blocked]:
        return True
    if allowed and country.upper() not in [c.upper() for c in allowed]:
        return True
    return False


def _enrich_video_details(videos: list[dict], api_key: str, cooldown_hours: int, country: str = "US") -> None:
    """Batch-fetch durations and region restrictions from videos.list."""
    video_ids = [v["id"] for v in videos]
    details: dict[str, dict] = {}

    # videos.list accepts up to 50 IDs per call
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        try:
            data = _api_get(VIDEOS_URL, {
                "key": api_key,
                "id": ",".join(batch),
                "part": "contentDetails",
            }, cooldown_hours)
            for item in data.get("items", []):
                details[item["id"]] = item.get("contentDetails", {})
        except Exception as exc:
            logger.warning("Failed to fetch video details: %s", exc)
            return

    # Update durations and mark region-blocked videos for removal
    blocked_ids = set()
    for v in videos:
        cd = details.get(v["id"])
        if cd is None:
            continue
        raw = cd.get("duration", "")
        v["duration"] = _parse_iso8601_duration(raw)
        if _is_region_blocked(cd, country):
            blocked_ids.add(v["id"])
            logger.info("Region-blocked in %s: %s (%s)", country, v["id"], v.get("title", ""))

    # Remove blocked videos from the list
    if blocked_ids:
        videos[:] = [v for v in videos if v["id"] not in blocked_ids]
        logger.info("Filtered %d region-blocked videos", len(blocked_ids))
