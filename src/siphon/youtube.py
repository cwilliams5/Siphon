"""YouTube Data API v3 integration for video discovery."""

from __future__ import annotations

import logging
import re

import httpx

logger = logging.getLogger(__name__)

SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
CHANNELS_URL = "https://www.googleapis.com/youtube/v3/channels"


def resolve_channel_id(url: str, api_key: str) -> str | None:
    """Resolve a YouTube channel URL to a channel ID via the API."""
    # If URL already contains a channel ID
    match = re.search(r"youtube\.com/channel/(UC[\w-]+)", url)
    if match:
        return match.group(1)

    # Extract handle or custom URL name
    handle = None
    match = re.search(r"youtube\.com/@([\w-]+)", url)
    if match:
        handle = match.group(1)
    if not handle:
        match = re.search(r"youtube\.com/c/([\w-]+)", url)
        if match:
            handle = match.group(1)

    if handle:
        resp = httpx.get(CHANNELS_URL, params={
            "key": api_key,
            "forHandle": handle,
            "part": "id",
        }, timeout=10)
        if resp.status_code == 200:
            items = resp.json().get("items", [])
            if items:
                return items[0]["id"]

    return None


def get_channel_metadata(channel_id: str, api_key: str) -> dict:
    """Get channel title and thumbnail."""
    resp = httpx.get(CHANNELS_URL, params={
        "key": api_key,
        "id": channel_id,
        "part": "snippet",
    }, timeout=10)
    resp.raise_for_status()
    items = resp.json().get("items", [])
    if not items:
        return {}
    snippet = items[0]["snippet"]
    thumbnails = snippet.get("thumbnails", {})
    # Pick the best thumbnail
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
    max_pages: int = 100,
) -> list[dict]:
    """List videos from a channel, newest first, stopping at cutoff or known video.

    Args:
        channel_id: YouTube channel ID
        api_key: YouTube Data API key
        date_cutoff: YYYYMMDD string — stop paginating when we pass this date
        known_ids: set of video IDs already in our DB — stop when we hit one
        max_pages: safety limit on pagination

    Returns list of dicts with: id, title, upload_date (YYYYMMDD), description, thumbnail
    """
    known_ids = known_ids or set()
    cutoff_iso = None
    if date_cutoff:
        # Convert YYYYMMDD to ISO for comparison
        cutoff_iso = f"{date_cutoff[:4]}-{date_cutoff[4:6]}-{date_cutoff[6:8]}T00:00:00Z"

    videos = []
    page_token = None

    for page_num in range(max_pages):
        params = {
            "key": api_key,
            "channelId": channel_id,
            "part": "snippet",
            "order": "date",
            "type": "video",
            "maxResults": 50,
        }
        if page_token:
            params["pageToken"] = page_token

        resp = httpx.get(SEARCH_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        items = data.get("items", [])
        if not items:
            break

        hit_known = False
        hit_cutoff = False

        for item in items:
            video_id = item["id"]["videoId"]
            snippet = item["snippet"]
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
                "thumbnail": thumb_url,
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "channel": snippet.get("channelTitle", ""),
                "duration": None,  # Not available from search endpoint
            })

        if hit_known or hit_cutoff:
            break

        page_token = data.get("nextPageToken")
        if not page_token:
            break

        logger.debug("YouTube API page %d: %d videos so far", page_num + 1, len(videos))

    logger.info("YouTube API: %d videos from channel %s", len(videos), channel_id)
    return videos
