"""Pipeline orchestration for Siphon.

Coordinates the two main scheduled jobs – feed checking and episode
downloading – wiring together the *db*, *downloader*, and *filters*
modules.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

from siphon.config import SiphonConfig, resolve_feed
from siphon.db import Database
from siphon.downloader import (
    download_video,
    extract_feed_metadata,
    find_downloaded_file,
)
from siphon.filters import apply_filters

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------- #
# Feed checking
# ---------------------------------------------------------------------- #


async def check_feeds(config: SiphonConfig, db: Database) -> None:
    """Discover new episodes from the next batch of feeds."""

    feeds_to_check = db.get_feeds_to_check(config.schedule.feeds_per_check)

    for feed_idx, feed_db in enumerate(feeds_to_check):
        # Throttle between feed checks to avoid rapid-fire requests
        if feed_idx > 0:
            await asyncio.sleep(5)

        # Find the matching FeedConfig by name
        feed_config = None
        for fc in config.feeds:
            if fc.name == feed_db["name"]:
                feed_config = fc
                break

        if feed_config is None:
            logger.warning("Feed %r not found in config, skipping", feed_db["name"])
            continue

        resolved = resolve_feed(feed_config, config.defaults)

        try:
            loop = asyncio.get_event_loop()
            metadata = await loop.run_in_executor(
                None, extract_feed_metadata, resolved.url, config.cookies
            )

            entries = metadata.get("entries") or []

            for entry in entries:
                video_id = entry["id"]

                # Skip episodes already in the database
                if db.get_episode(video_id, resolved.name) is not None:
                    continue

                reason = apply_filters(
                    entry,
                    resolved.block_shorts,
                    resolved.title_exclude,
                    resolved.min_duration_seconds,
                    resolved.date_cutoff,
                )

                if reason is not None:
                    logger.info(
                        "Filtered %s from %s: %s", video_id, resolved.name, reason
                    )
                    db.insert_episode(
                        video_id=video_id,
                        feed_name=resolved.name,
                        title=entry.get("title", ""),
                        description=entry.get("description"),
                        thumbnail_url=entry.get("thumbnail"),
                        channel_name=entry.get("channel"),
                        duration=entry.get("duration"),
                        upload_date=entry.get("upload_date"),
                        eligible_at=None,
                        status="filtered",
                    )
                else:
                    eligible_at = (
                        datetime.now(timezone.utc)
                        + timedelta(minutes=resolved.sponsorblock_delay_minutes)
                    ).isoformat()

                    db.insert_episode(
                        video_id=video_id,
                        feed_name=resolved.name,
                        title=entry.get("title", ""),
                        description=entry.get("description"),
                        thumbnail_url=entry.get("thumbnail"),
                        channel_name=entry.get("channel"),
                        duration=entry.get("duration"),
                        upload_date=entry.get("upload_date"),
                        eligible_at=eligible_at,
                        status="pending",
                    )

            db.update_feed_checked(feed_db["name"])
            logger.info("Checked feed %s: %d entries", resolved.name, len(entries))

        except Exception as exc:
            logger.error("Error checking feed %s: %s", feed_db["name"], exc)
            db.update_feed_checked(feed_db["name"], error=str(exc))


# ---------------------------------------------------------------------- #
# Download processing
# ---------------------------------------------------------------------- #


def _get_schedule_params(config: SiphonConfig, feed_type: str) -> tuple[int, int, int]:
    """Return (max_downloads_per_hour, download_workers, download_delay_seconds)
    for the given feed type."""
    sched = config.schedule
    if feed_type == "podcast":
        return (
            sched.podcast_max_downloads_per_hour,
            sched.podcast_download_workers,
            sched.podcast_download_delay_seconds,
        )
    return (
        sched.youtube_max_downloads_per_hour,
        sched.youtube_download_workers,
        sched.youtube_download_delay_seconds,
    )


def _get_keep_per_feed(config: SiphonConfig, feed_type: str) -> int:
    """Return the keep_per_feed limit for the given feed type."""
    if feed_type == "podcast":
        return config.storage.podcast_keep_per_feed
    return config.storage.youtube_keep_per_feed


async def process_downloads(config: SiphonConfig, db: Database) -> None:
    """Download eligible episodes and prune disk when needed."""

    db.promote_eligible_episodes()
    db.reset_stale_downloads()
    db.retry_failed_episodes()

    # Process YouTube and podcast downloads with separate limits
    for feed_type in ("youtube", "podcast"):
        max_per_hour, workers, delay = _get_schedule_params(config, feed_type)

        recent = db.get_recent_download_count(hours=1, feed_type=feed_type)
        remaining = max(0, max_per_hour - recent)
        limit = min(workers, remaining)

        if limit == 0:
            if remaining == 0:
                logger.info(
                    "Hourly %s download cap reached (%d/%d), skipping",
                    feed_type, recent, max_per_hour,
                )
            continue

        episodes = db.get_eligible_episodes(limit, feed_type=feed_type)

        for i, episode in enumerate(episodes):
            if i > 0 and delay > 0:
                logger.debug(
                    "Throttling %s: waiting %ds before next download",
                    feed_type, delay,
                )
                await asyncio.sleep(delay)

            video_id = episode["video_id"]
            feed_name = episode["feed_name"]

            feed_config = None
            for fc in config.feeds:
                if fc.name == feed_name:
                    feed_config = fc
                    break

            if feed_config is None:
                logger.warning(
                    "Feed config %r not found for episode %s, skipping",
                    feed_name, video_id,
                )
                continue

            resolved = resolve_feed(feed_config, config.defaults)

            db.update_episode_status(video_id, feed_name, "downloading")

            try:
                video_url = f"https://www.youtube.com/watch?v={video_id}"

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    download_video,
                    video_url,
                    resolved,
                    config.cookies,
                    config.storage.download_dir,
                )

                result = find_downloaded_file(
                    config.storage.download_dir, resolved.name, video_id
                )

                if result is not None:
                    path, size = result
                    mime = (
                        "audio/mpeg" if resolved.mode == "audio" else "video/mp4"
                    )
                    db.update_episode_status(
                        video_id,
                        feed_name,
                        "done",
                        file_path=path,
                        file_size=size,
                        mime_type=mime,
                    )
                    logger.info("Downloaded %s for feed %s", video_id, feed_name)
                else:
                    db.update_episode_status(
                        video_id,
                        feed_name,
                        "failed",
                        error="File not found after download",
                    )
                    logger.warning(
                        "File not found after download for %s/%s",
                        feed_name, video_id,
                    )

            except Exception as exc:
                logger.error("Download failed for %s/%s: %s", feed_name, video_id, exc)
                db.update_episode_status(
                    video_id,
                    feed_name,
                    "failed",
                    error=str(exc),
                    retry_count=episode["retry_count"] + 1,
                )

    await _prune_disk(config, db)


# ---------------------------------------------------------------------- #
# Disk pruning
# ---------------------------------------------------------------------- #


async def _prune_disk(config: SiphonConfig, db: Database) -> None:
    """Remove old episodes to stay within per-feed and global disk limits."""

    # --- Per-feed limits ---
    for feed in config.feeds:
        keep_limit = _get_keep_per_feed(config, feed.type)
        count = db.get_feed_episode_count(feed.name)
        if count > keep_limit:
            excess = count - keep_limit
            oldest = db.get_oldest_done_episodes(excess)
            # Only prune episodes belonging to this feed
            for ep in oldest:
                if ep["feed_name"] != feed.name:
                    continue
                if ep.get("file_path"):
                    try:
                        os.remove(ep["file_path"])
                    except OSError:
                        pass
                db.update_episode_status(
                    ep["video_id"], ep["feed_name"], "pruned"
                )
                logger.info(
                    "Pruned episode %s from feed %s (per-feed limit)",
                    ep["video_id"],
                    ep["feed_name"],
                )

    # --- Global disk limit ---
    max_bytes = config.storage.max_disk_gb * (1024 ** 3)
    usage = db.get_disk_usage()

    if usage > max_bytes:
        # Fetch a batch of the oldest episodes and delete until under limit
        oldest = db.get_oldest_done_episodes(50)
        for ep in oldest:
            if usage <= max_bytes:
                break
            file_size = ep.get("file_size") or 0
            if ep.get("file_path"):
                try:
                    os.remove(ep["file_path"])
                except OSError:
                    pass
            db.update_episode_status(
                ep["video_id"], ep["feed_name"], "pruned"
            )
            usage -= file_size
            logger.info(
                "Pruned episode %s from feed %s (disk limit)",
                ep["video_id"],
                ep["feed_name"],
            )
