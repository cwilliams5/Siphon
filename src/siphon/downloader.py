"""Wraps yt-dlp as a Python module.

All yt-dlp interaction in Siphon goes through this module.
"""

from __future__ import annotations

import glob
import logging
import os
from collections.abc import Sequence

import yt_dlp

from siphon.config import CookiesConfig, ResolvedFeed

logger = logging.getLogger(__name__)

# yt-dlp player clients used for cookie-authenticated YouTube downloads.
#
# Left to its own defaults yt-dlp picks ``web_creator`` for YouTube Premium
# sessions and ``tv_downgraded`` for other logged-in sessions.  As of 2026-08
# YouTube answers ``web_creator`` stream URLs with HTTP 403 unless a PO token
# is supplied, and ``tv_downgraded`` fails with "The page needs to be reloaded"
# (yt-dlp/yt-dlp#17389).  The maintainers' interim guidance for cookie users is
# the ``web_embedded`` client, which serves the full DASH format set with a
# logged-in session.  ``web`` is deliberately left out: YouTube forces SABR
# streaming on it, leaving only the 360p progressive format 18, which would
# silently win the ``/best`` format fallback instead of failing over to the
# anonymous retry in :func:`download_video`.
DEFAULT_PLAYER_CLIENTS: tuple[str, ...] = ("web_embedded", "tv_downgraded")

# Files yt-dlp leaves next to the final download that must never be mistaken
# for it: partial/resume data and thumbnails (intermediate merge/keyframe files
# are matched by name below).
_NON_MEDIA_SUFFIXES = (".part", ".ytdl", ".webp", ".jpg", ".jpeg", ".png")


def build_extract_opts(cookies: CookiesConfig, max_entries: int | None = None) -> dict:
    """Return yt-dlp options for flat playlist extraction."""
    opts = {
        "extract_flat": True,
        "quiet": True,
        "no_warnings": True,
        "cookiesfrombrowser": (cookies.browser,),
    }
    if max_entries is not None:
        opts["playlistend"] = max_entries
    return opts


def build_download_opts(
    feed: ResolvedFeed,
    cookies: CookiesConfig,
    download_dir: str,
    player_clients: Sequence[str] | None = None,
    *,
    use_cookies: bool = True,
) -> dict:
    """Build full yt-dlp download options based on feed config.

    With ``use_cookies`` the browser cookie jar is attached and yt-dlp's
    player-client selection is pinned to ``player_clients`` (defaulting to
    :data:`DEFAULT_PLAYER_CLIENTS`).  Without cookies yt-dlp's own anonymous
    client defaults are left untouched.
    """
    postprocessors: list[dict] = []

    if feed.mode == "video":
        if feed.quality == "max":
            fmt = "bestvideo+bestaudio/best[vcodec!=none]"
        else:
            fmt = f"bestvideo[height<={feed.quality}]+bestaudio/best[height<={feed.quality}][vcodec!=none]"
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
                "force_keyframes": True,
            }
        )

    opts.update(
        {
            "outtmpl": f"{download_dir}/{feed.name}/%(id)s.%(ext)s",
            "quiet": True,
            "no_warnings": True,
            "writethumbnail": True,
            "postprocessors": postprocessors,
        }
    )

    if use_cookies:
        opts["cookiesfrombrowser"] = (cookies.browser,)
        clients = list(player_clients) if player_clients else list(DEFAULT_PLAYER_CLIENTS)
        opts["extractor_args"] = {"youtube": {"player_client": clients}}

    return opts


def extract_feed_metadata(
    url: str, cookies: CookiesConfig, max_entries: int | None = None
) -> dict:
    """Extract flat playlist metadata without downloading."""
    opts = build_extract_opts(cookies, max_entries=max_entries)
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
        return info  # type: ignore[return-value]
    except yt_dlp.utils.DownloadError as exc:
        raise Exception(str(exc)) from exc


def _run_download(video_url: str, opts: dict) -> dict:
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(video_url, download=True)
    return info  # type: ignore[return-value]


def _remove_intermediate_files(download_dir: str, feed_name: str) -> None:
    """Delete leftovers of a failed force_keyframes postprocessing run."""
    for pattern in ("*.temp.*", "*.keyframes.*"):
        for tmp in glob.glob(f"{download_dir}/{feed_name}/{pattern}"):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _download_attempt(
    video_url: str,
    feed: ResolvedFeed,
    cookies: CookiesConfig,
    download_dir: str,
    player_clients: Sequence[str] | None,
    *,
    use_cookies: bool,
) -> dict:
    """One download attempt, including the force_keyframes → stream-copy fallback.

    Raises ``yt_dlp.utils.DownloadError`` when the attempt fails.
    """
    opts = build_download_opts(feed, cookies, download_dir, player_clients, use_cookies=use_cookies)
    try:
        return _run_download(video_url, opts)
    except yt_dlp.utils.DownloadError as exc:
        # Only a postprocessing failure with force_keyframes on is worth a
        # stream-copy retry; anything else propagates to the caller.
        if "Postprocessing" not in str(exc) or not feed.sponsorblock:
            raise
        logger.warning("force_keyframes failed for %s, retrying with stream copy", video_url)
        _remove_intermediate_files(download_dir, feed.name)
        opts_fallback = build_download_opts(
            feed, cookies, download_dir, player_clients, use_cookies=use_cookies
        )
        for pp in opts_fallback.get("postprocessors", []):
            if pp.get("key") == "ModifyChapters":
                pp["force_keyframes"] = False
        return _run_download(video_url, opts_fallback)


def download_video(
    video_url: str,
    feed: ResolvedFeed,
    cookies: CookiesConfig,
    download_dir: str,
    player_clients: Sequence[str] | None = None,
) -> dict:
    """Download a single video and return its info dict.

    Two layers of fallback:

    * If SponsorBlock postprocessing fails with force_keyframes, the attempt
      is retried once with force_keyframes=False (stream copy).
    * If the cookie-authenticated download fails outright — YouTube rejecting
      the logged-in player clients with HTTP 403, a video whose owner disabled
      embedding, an expired browser session — the whole download is retried
      anonymously with yt-dlp's default clients.  That gives up Premium and
      members-only access for that one video rather than not downloading it.

    Raises a plain ``Exception`` carrying yt-dlp's message when every attempt
    fails.
    """
    try:
        return _download_attempt(
            video_url, feed, cookies, download_dir, player_clients, use_cookies=True
        )
    except yt_dlp.utils.DownloadError as exc:
        cookie_err = str(exc).strip()

    logger.warning(
        "Cookie-authenticated download failed for %s (%s) — retrying anonymously",
        video_url, cookie_err[:160],
    )
    try:
        return _download_attempt(
            video_url, feed, cookies, download_dir, player_clients, use_cookies=False
        )
    except yt_dlp.utils.DownloadError as exc:
        anon_err = str(exc).strip()
        if anon_err == cookie_err:
            raise Exception(anon_err) from exc
        raise Exception(f"{anon_err} (with cookies: {cookie_err})") from exc


def test_youtube_cookies(cookies: CookiesConfig) -> dict:
    """Test if YouTube cookies can be read from the browser and user is logged in.

    Returns a dict with:
        ok: bool — True if cookies were successfully extracted
        logged_in: bool — True if YouTube login cookies found
        message: str — human-readable status
        cookie_count: int — number of cookies extracted
    """
    LOGIN_COOKIE_NAMES = {"SID", "SSID", "HSID", "LOGIN_INFO", "__Secure-1PSID"}

    try:
        opts = {
            "quiet": True,
            "no_warnings": True,
            "cookiesfrombrowser": (cookies.browser,),
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            jar = ydl.cookiejar
            total = len(list(jar))
            login_cookies = [
                c for c in jar
                if c.name in LOGIN_COOKIE_NAMES
                and (".youtube.com" in c.domain or ".google.com" in c.domain)
            ]
    except Exception as exc:
        msg = str(exc)
        return {
            "ok": False,
            "logged_in": False,
            "message": f"Cannot read {cookies.browser} cookies — try closing the browser. ({msg[:100]})",
            "cookie_count": 0,
        }

    logged_in = len(login_cookies) > 0

    if total == 0:
        return {
            "ok": False,
            "logged_in": False,
            "message": f"No cookies found in {cookies.browser}.",
            "cookie_count": 0,
        }

    if logged_in:
        msg = f"Logged in ({total} cookies from {cookies.browser})"
    else:
        msg = f"Cookies loaded but not logged into YouTube ({total} cookies from {cookies.browser})"

    return {
        "ok": True,
        "logged_in": logged_in,
        "message": msg,
        "cookie_count": total,
    }


def _is_final_media_file(path: str) -> bool:
    name = os.path.basename(path).lower()
    if name.endswith(_NON_MEDIA_SUFFIXES):
        return False
    return ".temp." not in name and ".keyframes." not in name


def find_downloaded_file(
    download_dir: str,
    feed_name: str,
    video_id: str,
) -> tuple[str, int] | None:
    """Locate a downloaded file by video ID.

    Partial downloads, merge/keyframe intermediates and thumbnails that yt-dlp
    may leave next to the real file are ignored.  Returns
    ``(file_path, file_size)`` or ``None`` when no matching file exists.
    """
    pattern = os.path.join(download_dir, feed_name, f"{video_id}.*")
    matches = sorted(m for m in glob.glob(pattern) if _is_final_media_file(m))
    if not matches:
        return None
    file_path = matches[0]
    file_size = os.path.getsize(file_path)
    return file_path, file_size
