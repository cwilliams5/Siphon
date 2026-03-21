"""YouTube-specific utilities: RSS feed parsing and channel ID extraction."""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET

import httpx

logger = logging.getLogger(__name__)

YT_RSS_URL = "https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"


def fetch_youtube_rss(channel_id: str, timeout: int = 15) -> list[dict]:
    """Fetch the YouTube RSS feed for a channel. Returns ~15 most recent videos.

    Each entry has: id, title, upload_date (YYYYMMDD), url
    """
    url = YT_RSS_URL.format(channel_id=channel_id)
    resp = httpx.get(url, timeout=timeout, follow_redirects=True)
    resp.raise_for_status()

    ns = {"atom": "http://www.w3.org/2005/Atom", "yt": "http://www.youtube.com/xml/schemas/2015"}
    root = ET.fromstring(resp.content)

    entries = []
    for entry in root.findall("atom:entry", ns):
        video_id_el = entry.find("yt:videoId", ns)
        title_el = entry.find("atom:title", ns)
        published_el = entry.find("atom:published", ns)

        if video_id_el is None or video_id_el.text is None:
            continue

        video_id = video_id_el.text
        title = title_el.text if title_el is not None and title_el.text else ""

        upload_date = None
        if published_el is not None and published_el.text:
            # Format: 2026-03-20T12:00:00+00:00
            upload_date = published_el.text[:10].replace("-", "")

        entries.append({
            "id": video_id,
            "title": title,
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "upload_date": upload_date,
            "duration": None,
        })

    return entries


def extract_channel_id(metadata: dict) -> str | None:
    """Extract the channel ID from yt-dlp metadata."""
    # Try direct field first
    channel_id = metadata.get("channel_id")
    if channel_id:
        return channel_id
    # Try from uploader_id
    uploader_id = metadata.get("uploader_id")
    if uploader_id and uploader_id.startswith("UC"):
        return uploader_id
    return None
