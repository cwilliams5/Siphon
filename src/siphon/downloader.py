"""Wraps yt-dlp as a Python module.

All yt-dlp interaction in Siphon goes through this module.
"""

from __future__ import annotations

import glob
import os

import yt_dlp

from siphon.config import CookiesConfig, ResolvedFeed


def build_extract_opts(cookies: CookiesConfig) -> dict:
    """Return yt-dlp options for flat playlist extraction."""
    return {
        "extract_flat": True,
        "quiet": True,
        "no_warnings": True,
        "cookiesfrombrowser": (cookies.browser,),
    }


def build_download_opts(
    feed: ResolvedFeed,
    cookies: CookiesConfig,
    download_dir: str,
) -> dict:
    """Build full yt-dlp download options based on feed config."""
    postprocessors: list[dict] = []

    if feed.mode == "video":
        if feed.quality == "max":
            fmt = "bestvideo+bestaudio/best"
        else:
            fmt = f"bestvideo[height<={feed.quality}]+bestaudio/best"
        opts: dict = {
            "format": fmt,
            "merge_output_format": "mp4",
        }
    else:
        opts = {
            "format": "bestaudio/best",
        }
        postprocessors.append(
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }
        )

    # Common metadata / thumbnail postprocessors
    postprocessors.append({"key": "FFmpegMetadata"})
    postprocessors.append({"key": "EmbedThumbnail"})

    # SponsorBlock postprocessors
    if feed.sponsorblock:
        postprocessors.append(
            {
                "key": "SponsorBlock",
                "categories": feed.sponsorblock_categories,
            }
        )
        postprocessors.append(
            {
                "key": "ModifyChapters",
                "remove_sponsor_segments": feed.sponsorblock_categories,
                "force_keyframes": feed.force_keyframes_at_cuts,
            }
        )

    opts.update(
        {
            "outtmpl": f"{download_dir}/{feed.name}/%(id)s.%(ext)s",
            "cookiesfrombrowser": (cookies.browser,),
            "quiet": True,
            "no_warnings": True,
            "writethumbnail": True,
            "postprocessors": postprocessors,
        }
    )

    return opts


def extract_feed_metadata(url: str, cookies: CookiesConfig) -> dict:
    """Extract flat playlist metadata without downloading."""
    opts = build_extract_opts(cookies)
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
        return info  # type: ignore[return-value]
    except yt_dlp.utils.DownloadError as exc:
        raise Exception(str(exc)) from exc


def download_video(
    video_url: str,
    feed: ResolvedFeed,
    cookies: CookiesConfig,
    download_dir: str,
) -> dict:
    """Download a single video and return its info dict."""
    opts = build_download_opts(feed, cookies, download_dir)
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(video_url, download=True)
        return info  # type: ignore[return-value]
    except yt_dlp.utils.DownloadError as exc:
        raise Exception(str(exc)) from exc


def test_youtube_cookies(cookies: CookiesConfig) -> dict:
    """Test if YouTube cookies are valid and the user is logged in.

    Returns a dict with:
        ok: bool — True if cookies work and user appears logged in
        message: str — human-readable status
        premium: bool | None — True if Premium detected, None if unknown
    """
    opts = {
        "quiet": True,
        "no_warnings": True,
        "cookiesfrombrowser": (cookies.browser,),
        "skip_download": True,
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            # Extract info from a known short public video
            info = ydl.extract_info(
                "https://www.youtube.com/watch?v=jNQXAC9IVRw",  # "Me at the zoo"
                download=False,
            )
    except Exception as exc:
        msg = str(exc)
        if "cookie" in msg.lower() or "locked" in msg.lower():
            return {
                "ok": False,
                "message": f"Cannot read {cookies.browser} cookies — is the browser open? Try closing it.",
                "premium": None,
            }
        return {
            "ok": False,
            "message": f"YouTube request failed: {msg[:200]}",
            "premium": None,
        }

    if info is None:
        return {"ok": False, "message": "No response from YouTube", "premium": None}

    # Check for signs of being logged in / premium
    # Premium users get formats with 'premium' in the note, or no ads
    formats = info.get("formats") or []
    has_premium_formats = any(
        f.get("format_note", "").lower().startswith("premium")
        or (f.get("height") or 0) >= 2160
        for f in formats
    )

    return {
        "ok": True,
        "message": "YouTube cookies working" + (" (Premium detected)" if has_premium_formats else ""),
        "premium": has_premium_formats if formats else None,
    }


def find_downloaded_file(
    download_dir: str,
    feed_name: str,
    video_id: str,
) -> tuple[str, int] | None:
    """Locate a downloaded file by video ID.

    Returns ``(file_path, file_size)`` or ``None`` when no matching file
    exists.
    """
    pattern = os.path.join(download_dir, feed_name, f"{video_id}.*")
    matches = glob.glob(pattern)
    if not matches:
        return None
    file_path = matches[0]
    file_size = os.path.getsize(file_path)
    return file_path, file_size
