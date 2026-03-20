"""RSS feed generation with iTunes namespace for Pocket Casts compatibility."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import formatdate
from calendar import timegm


# ------------------------------------------------------------------ #
# iTunes namespace
# ------------------------------------------------------------------ #

ITUNES_NS = "http://www.itunes.com/dtds/podcast-1.0.dtd"
ET.register_namespace("itunes", ITUNES_NS)


# ------------------------------------------------------------------ #
# Helper functions
# ------------------------------------------------------------------ #


def format_duration(seconds: int | None) -> str:
    """Convert *seconds* to ``HH:MM:SS``.  Returns ``"00:00:00"`` for *None*."""
    if seconds is None:
        return "00:00:00"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def format_pubdate(upload_date: str | None) -> str:
    """Convert a ``YYYYMMDD`` string to RFC 2822.

    If *upload_date* is *None* or cannot be parsed, the current UTC time is
    used instead.
    """
    if upload_date is not None:
        try:
            dt = datetime.strptime(upload_date, "%Y%m%d").replace(tzinfo=timezone.utc)
            return formatdate(timegm(dt.timetuple()), usegmt=True)
        except ValueError:
            pass
    return formatdate(timegm(datetime.now(timezone.utc).timetuple()), usegmt=True)


def get_file_extension(mime_type: str) -> str:
    """Map a MIME type to a file extension.

    >>> get_file_extension("video/mp4")
    'mp4'
    >>> get_file_extension("audio/mpeg")
    'mp3'
    """
    mapping = {
        "video/mp4": "mp4",
        "audio/mpeg": "mp3",
        "audio/mp4": "m4a",
        "video/webm": "webm",
        "audio/ogg": "ogg",
    }
    return mapping.get(mime_type, "mp4")


# ------------------------------------------------------------------ #
# Feed generation
# ------------------------------------------------------------------ #


def generate_feed_xml(
    feed_name: str,
    episodes: list[dict],
    base_url: str,
    channel_name: str | None = None,
    *,
    display_name: str | None = None,
    image_url: str | None = None,
) -> str:
    """Return an RSS 2.0 XML string with iTunes extensions.

    Parameters
    ----------
    feed_name:
        Internal feed name, used as a fallback title.
    episodes:
        List of episode dicts (``status='done'``).  Expected keys:
        ``video_id``, ``feed_name``, ``title``, ``description``,
        ``thumbnail_url``, ``channel_name``, ``duration``,
        ``upload_date``, ``file_path``, ``file_size``, ``mime_type``.
    base_url:
        Server base URL for constructing media URLs.
    channel_name:
        Optional display name.  Falls back to the first episode's
        ``channel_name`` if available, then *feed_name*.
    display_name:
        Explicit display name from config (takes highest priority for title).
    image_url:
        Channel-level artwork URL (e.g. from podcast RSS).
    """

    # Resolve display name: display_name > channel_name > first episode > feed_name
    title = display_name
    if title is None:
        title = channel_name
    if title is None and episodes:
        title = episodes[0].get("channel_name") or feed_name
    if title is None:
        title = feed_name

    # Build the tree – register_namespace already handles the xmlns declaration
    rss = ET.Element("rss", version="2.0")

    channel = ET.SubElement(rss, "channel")

    _text(channel, "title", title)
    _text(channel, "link", base_url)
    _text(channel, "description", f"Siphon feed: {feed_name}")
    _text(channel, "language", "en")
    _text(channel, f"{{{ITUNES_NS}}}author", title)
    _text(channel, f"{{{ITUNES_NS}}}explicit", "false")

    # Channel-level artwork: explicit image_url > first episode thumbnail
    artwork_url = image_url
    if not artwork_url and episodes and episodes[0].get("thumbnail_url"):
        artwork_url = episodes[0]["thumbnail_url"]
    if artwork_url:
        ET.SubElement(
            channel,
            f"{{{ITUNES_NS}}}image",
            href=artwork_url,
        )

    # Episodes (already ordered by upload_date DESC from the DB)
    for ep in episodes:
        item = ET.SubElement(channel, "item")

        _text(item, "title", ep.get("title", ""))
        _text(item, "description", ep.get("description") or "")

        guid = ET.SubElement(item, "guid", isPermaLink="false")
        guid.text = ep.get("video_id", "")

        _text(item, "pubDate", format_pubdate(ep.get("upload_date")))

        mime = ep.get("mime_type", "video/mp4")
        ext = get_file_extension(mime)
        enc_url = f"{base_url}/media/{feed_name}/{ep.get('video_id', '')}.{ext}"
        ET.SubElement(
            item,
            "enclosure",
            url=enc_url,
            length=str(ep.get("file_size") or 0),
            type=mime,
        )

        _text(item, f"{{{ITUNES_NS}}}duration", format_duration(ep.get("duration")))
        _text(item, f"{{{ITUNES_NS}}}explicit", "false")

        if ep.get("thumbnail_url"):
            ET.SubElement(
                item,
                f"{{{ITUNES_NS}}}image",
                href=ep["thumbnail_url"],
            )

        _text(item, f"{{{ITUNES_NS}}}author", ep.get("channel_name") or channel_name)

    # Pretty-print (Python 3.9+)
    try:
        ET.indent(rss)
    except AttributeError:
        pass  # older Python – skip indentation

    tree = ET.ElementTree(rss)
    # Write to string with XML declaration
    from io import BytesIO
    buf = BytesIO()
    tree.write(buf, encoding="utf-8", xml_declaration=True)
    return buf.getvalue().decode("utf-8")


# ------------------------------------------------------------------ #
# Internal helpers
# ------------------------------------------------------------------ #


def _text(parent: ET.Element, tag: str, text: str) -> ET.Element:
    """Create a sub-element with the given *text* content."""
    el = ET.SubElement(parent, tag)
    el.text = text
    return el
