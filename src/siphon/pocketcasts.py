"""Pocket Casts API integration for auto-pruning completed episodes."""

from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
API_BASE = "https://api.pocketcasts.com"
CACHE_BASE = "https://cache.pocketcasts.com"

# Token cache (module-level, thread-safe)
_token: str | None = None
_token_lock = threading.Lock()


def _login(email: str, password: str) -> str:
    """Login to Pocket Casts and return a bearer token."""
    global _token
    with _token_lock:
        if _token:
            return _token
        resp = httpx.post(
            f"{API_BASE}/user/login",
            json={"email": email, "password": password, "scope": "webplayer"},
            headers={"User-Agent": UA},
            timeout=10,
        )
        resp.raise_for_status()
        _token = resp.json()["token"]
        return _token


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "User-Agent": UA}


def _extract_uuid_from_pc_url(pc_url: str) -> str | None:
    """Extract podcast UUID from a pca.st private URL."""
    # https://pca.st/private/e39abfa0-09f4-013f-2b05-0e956d54cc61
    if not pc_url:
        return None
    parts = pc_url.rstrip("/").split("/")
    if parts:
        candidate = parts[-1]
        # UUID format: 8-4-4-4-12
        if len(candidate) == 36 and candidate.count("-") == 4:
            return candidate
    return None


def get_episode_statuses(token: str, podcast_uuid: str) -> dict[str, dict]:
    """Get episode play statuses from Pocket Casts.

    Returns a dict of {pc_episode_uuid: {playingStatus, isDeleted, playedUpTo}}.
    """
    resp = httpx.post(
        f"{API_BASE}/user/podcast/episodes",
        json={"uuid": podcast_uuid},
        headers=_headers(token),
        timeout=10,
    )
    resp.raise_for_status()
    episodes = resp.json().get("episodes", [])
    return {ep["uuid"]: ep for ep in episodes}


def get_episode_mapping(podcast_uuid: str) -> dict[str, str]:
    """Get mapping of Pocket Casts episode UUID to Siphon video_id.

    Uses the cache endpoint which returns episode URLs containing the video_id.
    Returns {pc_episode_uuid: siphon_video_id}.
    """
    resp = httpx.get(
        f"{CACHE_BASE}/podcast/full/{podcast_uuid}/0/2/1000",
        headers={"User-Agent": UA},
        timeout=15,
        follow_redirects=True,
    )
    if resp.status_code != 200:
        return {}

    data = resp.json()
    episodes = data.get("podcast", {}).get("episodes", [])
    mapping = {}
    for ep in episodes:
        url = ep.get("url", "")
        uuid = ep.get("uuid", "")
        if "/media/" in url and uuid:
            # Extract video_id from URL: .../media/feed-name/video_id.ext
            path = url.split("/media/")[-1]
            parts = path.split("/")
            if len(parts) >= 2:
                video_id = os.path.splitext(parts[1])[0]
                mapping[uuid] = video_id
    return mapping


def get_completed_video_ids(
    email: str, password: str, podcast_uuid: str
) -> set[str]:
    """Get Siphon video_ids for episodes completed or archived in Pocket Casts."""
    token = _login(email, password)

    statuses = get_episode_statuses(token, podcast_uuid)
    mapping = get_episode_mapping(podcast_uuid)

    completed = set()
    for pc_uuid, status in statuses.items():
        # playingStatus: 0=unplayed, 2=in progress, 3=completed
        is_completed = status.get("playingStatus") == 3
        is_archived = status.get("isDeleted", False)
        if is_completed or is_archived:
            video_id = mapping.get(pc_uuid)
            if video_id:
                completed.add(video_id)

    return completed


def clear_token() -> None:
    """Clear cached token (e.g. on auth failure)."""
    global _token
    with _token_lock:
        _token = None
