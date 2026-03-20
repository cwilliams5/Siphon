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
