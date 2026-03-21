"""Podcast RSS feed polling and audio downloading."""

from __future__ import annotations

import hashlib
import logging
import os
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)


def fetch_podcast_rss(url: str, timeout: int = 30) -> bytes:
    """Fetch a podcast RSS feed XML."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    resp = httpx.get(url, timeout=timeout, follow_redirects=True, headers=headers)
    resp.raise_for_status()
    return resp.content


def parse_podcast_feed(xml_bytes: bytes) -> dict[str, Any]:
    """Parse a podcast RSS feed and extract episodes.

    Returns:
        {
            "title": "Podcast Title",
            "description": "...",
            "image_url": "...",
            "episodes": [
                {
                    "guid": "unique-id",
                    "title": "Episode Title",
                    "description": "...",
                    "audio_url": "https://...",
                    "pub_date": "20250115",  # YYYYMMDD
                    "duration": 3600,  # seconds, or None
                    "thumbnail_url": "...",
                }
            ]
        }
    """
    root = ET.fromstring(xml_bytes)

    # Handle namespaces
    ns = {
        "itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd",
        "content": "http://purl.org/rss/1.0/modules/content/",
    }

    channel = root.find("channel")
    if channel is None:
        raise ValueError("No <channel> element found in RSS feed")

    feed_title = _text(channel, "title") or "Unknown Podcast"
    feed_desc = _text(channel, "description") or ""

    # Feed image
    image_url = None
    itunes_image = channel.find(f"{{{ns['itunes']}}}image")
    if itunes_image is not None:
        image_url = itunes_image.get("href")
    if not image_url:
        image_el = channel.find("image/url")
        if image_el is not None and image_el.text:
            image_url = image_el.text

    episodes = []
    for item in channel.findall("item"):
        # Get enclosure (audio file)
        enclosure = item.find("enclosure")
        if enclosure is None:
            continue

        audio_url = enclosure.get("url")
        if not audio_url:
            continue

        # Only include audio enclosures
        enc_type = (enclosure.get("type") or "").lower()
        if enc_type and not enc_type.startswith("audio"):
            continue

        # GUID
        guid_el = item.find("guid")
        if guid_el is not None and guid_el.text:
            guid = guid_el.text.strip()
        else:
            # Fallback: hash the audio URL
            guid = hashlib.sha256(audio_url.encode()).hexdigest()[:16]

        # Title
        title = _text(item, "title") or "Untitled"

        # Description
        description = _text(item, "description") or ""

        # Pub date
        pub_date = None
        pub_date_str = _text(item, "pubDate")
        if pub_date_str:
            pub_date = _parse_rfc2822_date(pub_date_str)

        # Duration
        duration = None
        dur_el = item.find(f"{{{ns['itunes']}}}duration")
        if dur_el is not None and dur_el.text:
            duration = _parse_duration(dur_el.text.strip())

        # Thumbnail
        thumbnail_url = None
        itunes_img = item.find(f"{{{ns['itunes']}}}image")
        if itunes_img is not None:
            thumbnail_url = itunes_img.get("href")
        if not thumbnail_url:
            thumbnail_url = image_url  # fall back to feed-level image

        episodes.append({
            "guid": guid,
            "title": title,
            "description": description,
            "audio_url": audio_url,
            "pub_date": pub_date,
            "duration": duration,
            "thumbnail_url": thumbnail_url,
        })

    return {
        "title": feed_title,
        "description": feed_desc,
        "image_url": image_url,
        "episodes": episodes,
    }


def download_podcast_audio(
    audio_url: str,
    output_path: str,
    timeout: int = 300,
) -> int:
    """Download a podcast audio file. Returns file size in bytes."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    with httpx.stream("GET", audio_url, timeout=timeout, follow_redirects=True, headers=headers) as resp:
        resp.raise_for_status()
        total = 0
        with open(output_path, "wb") as f:
            for chunk in resp.iter_bytes(chunk_size=65536):
                f.write(chunk)
                total += len(chunk)

    logger.info("Downloaded %s (%d bytes) to %s", audio_url, total, output_path)
    return total


def episode_filename(guid: str, audio_url: str) -> str:
    """Generate a filename for a podcast episode.

    Uses the GUID as a slug, with the extension from the audio URL.
    """
    # Get extension from URL
    path = urlparse(audio_url).path
    ext = os.path.splitext(path)[1].lower()
    if ext not in (".mp3", ".m4a", ".ogg", ".aac", ".wav"):
        ext = ".mp3"  # default

    # Slugify the GUID
    slug = re.sub(r"[^\w-]", "_", guid)[:80]
    return f"{slug}{ext}"


# ------------------------------------------------------------------ #
# Internal helpers
# ------------------------------------------------------------------ #

def _text(parent: ET.Element, tag: str) -> str | None:
    """Get text content of a child element, or None."""
    el = parent.find(tag)
    if el is not None and el.text:
        return el.text.strip()
    return None


def _parse_rfc2822_date(date_str: str) -> str | None:
    """Parse an RFC 2822 date to YYYYMMDD format. Returns None on failure."""
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.strftime("%Y%m%d")
    except (ValueError, TypeError):
        return None


def _parse_duration(text: str) -> int | None:
    """Parse a podcast duration string to seconds.

    Handles formats: "3600", "01:00:00", "60:00"
    """
    # Pure number = seconds
    try:
        return int(text)
    except ValueError:
        pass

    # HH:MM:SS or MM:SS
    parts = text.split(":")
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
    except (ValueError, IndexError):
        pass

    return None
