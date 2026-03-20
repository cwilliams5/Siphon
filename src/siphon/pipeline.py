"""Pipeline orchestration for Siphon.

Coordinates the two main scheduled jobs – feed checking and episode
downloading – wiring together the *db*, *downloader*, *filters*,
*podcast*, and *llm_trim* modules.
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

    for feed_type, limit in (
        ("youtube", config.schedule.youtube_feeds_per_check),
        ("podcast", config.schedule.podcast_feeds_per_check),
    ):
        feeds_to_check = db.get_feeds_to_check(limit, feed_type=feed_type)

        for feed_idx, feed_db in enumerate(feeds_to_check):
            if feed_idx > 0:
                await asyncio.sleep(5 if feed_type == "youtube" else 1)

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
                if resolved.type == "podcast":
                    await _check_podcast_feed(resolved, db)
                else:
                    await _check_youtube_feed(resolved, config, db)

                db.update_feed_checked(feed_db["name"])

            except Exception as exc:
                logger.error("Error checking feed %s: %s", feed_db["name"], exc)
                db.update_feed_checked(feed_db["name"], error=str(exc))


async def _check_youtube_feed(resolved, config, db) -> None:
    """Check a YouTube feed for new episodes."""
    loop = asyncio.get_event_loop()
    metadata = await loop.run_in_executor(
        None, extract_feed_metadata, resolved.url, config.cookies
    )

    entries = metadata.get("entries") or []

    for entry in entries:
        video_id = entry["id"]

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
            logger.info("Filtered %s from %s: %s", video_id, resolved.name, reason)
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
            ).strftime("%Y-%m-%d %H:%M:%S")

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

    logger.info("Checked YouTube feed %s: %d entries", resolved.name, len(entries))


async def _check_podcast_feed(resolved, db) -> None:
    """Check a podcast feed for new episodes."""
    from siphon.podcast import fetch_podcast_rss, parse_podcast_feed

    loop = asyncio.get_event_loop()
    xml_bytes = await loop.run_in_executor(None, fetch_podcast_rss, resolved.url)
    feed_data = parse_podcast_feed(xml_bytes)

    # Store podcast artwork in DB
    feed_image_url = feed_data.get("image_url")
    if feed_image_url:
        db.update_feed_image(resolved.name, feed_image_url)

    episodes = feed_data.get("episodes") or []
    new_count = 0

    for ep in episodes:
        guid = ep["guid"]

        if db.get_episode(guid, resolved.name) is not None:
            continue

        # Build a filter-compatible entry dict
        entry = {
            "id": guid,
            "title": ep.get("title", ""),
            "url": ep.get("audio_url", ""),
            "duration": ep.get("duration"),
            "upload_date": ep.get("pub_date"),
        }

        reason = apply_filters(
            entry,
            block_shorts=False,  # No shorts concept for podcasts
            title_exclude=resolved.title_exclude,
            min_duration_seconds=resolved.min_duration_seconds,
            date_cutoff=resolved.date_cutoff,
        )

        if reason is not None:
            logger.info("Filtered %s from %s: %s", guid, resolved.name, reason)
            db.insert_episode(
                video_id=guid,
                feed_name=resolved.name,
                title=ep.get("title", ""),
                description=ep.get("description"),
                thumbnail_url=ep.get("thumbnail_url"),
                channel_name=feed_data.get("title"),
                duration=ep.get("duration"),
                upload_date=ep.get("pub_date"),
                eligible_at=None,
                status="filtered",
            )
        else:
            # Podcasts don't need SponsorBlock delay — eligible immediately
            eligible_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            db.insert_episode(
                video_id=guid,
                feed_name=resolved.name,
                title=ep.get("title", ""),
                description=ep.get("description"),
                thumbnail_url=ep.get("thumbnail_url"),
                channel_name=feed_data.get("title"),
                duration=ep.get("duration"),
                upload_date=ep.get("pub_date"),
                eligible_at=eligible_at,
                status="pending",
            )
            new_count += 1

    logger.info("Checked podcast feed %s: %d episodes, %d new", resolved.name, len(episodes), new_count)


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
                logger.debug("Throttling %s: waiting %ds", feed_type, delay)
                await asyncio.sleep(delay)

            video_id = episode["video_id"]
            feed_name = episode["feed_name"]

            feed_config = None
            for fc in config.feeds:
                if fc.name == feed_name:
                    feed_config = fc
                    break

            if feed_config is None:
                logger.warning("Feed config %r not found for %s, skipping", feed_name, video_id)
                continue

            resolved = resolve_feed(feed_config, config.defaults)
            db.update_episode_status(video_id, feed_name, "downloading")

            try:
                if resolved.type == "podcast":
                    await _download_podcast_episode(episode, resolved, config, db)
                else:
                    await _download_youtube_episode(episode, resolved, config, db)

                # LLM trim post-processing
                if resolved.llm_trim:
                    ep = db.get_episode(video_id, feed_name)
                    if ep and ep["status"] == "done" and ep.get("file_path"):
                        await _run_llm_trim(ep, resolved, config, db)

            except Exception as exc:
                logger.error("Download failed for %s/%s: %s", feed_name, video_id, exc)
                db.update_episode_status(
                    video_id, feed_name, "failed",
                    error=str(exc),
                    retry_count=episode["retry_count"] + 1,
                )

    await _prune_disk(config, db)


async def _download_youtube_episode(episode, resolved, config, db) -> None:
    """Download a YouTube episode."""
    video_id = episode["video_id"]
    feed_name = episode["feed_name"]
    video_url = f"https://www.youtube.com/watch?v={video_id}"

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        None, download_video, video_url, resolved,
        config.cookies, config.storage.download_dir,
    )

    result = find_downloaded_file(config.storage.download_dir, resolved.name, video_id)

    if result is not None:
        path, size = result
        mime = "audio/mpeg" if resolved.mode == "audio" else "video/mp4"
        db.update_episode_status(
            video_id, feed_name, "done",
            file_path=path, file_size=size, mime_type=mime,
        )
        logger.info("Downloaded YouTube %s for feed %s", video_id, feed_name)
    else:
        db.update_episode_status(
            video_id, feed_name, "failed",
            error="File not found after download",
        )


async def _download_podcast_episode(episode, resolved, config, db) -> None:
    """Download a podcast episode audio file."""
    from siphon.podcast import download_podcast_audio, episode_filename

    video_id = episode["video_id"]  # This is the GUID for podcasts
    feed_name = episode["feed_name"]

    # We need the audio URL — it was stored... actually it wasn't stored in
    # the episodes table. We need to re-fetch or store it. For now, let's
    # store the audio_url in the description or add a dedicated column.
    # Actually, the simplest approach: re-fetch the RSS and find the episode.
    # But that's expensive. Better: use the description field or a new approach.

    # Actually, let me check if the audio_url is in the episode somehow.
    # The video_id IS the GUID, and we can look up the audio URL from the
    # episode's description or from a re-fetch. But the simplest approach
    # for the DB is to store audio_url somewhere.

    # Let's use the 'error' field temporarily... no that's ugly.
    # Better: let's store the audio_url in a field. We already have
    # thumbnail_url. Let me add a column. But for now, let me use a simpler
    # approach: the video_id for podcasts could BE the audio URL if we
    # URL-encode the guid separately. OR we re-fetch the RSS.

    # Pragmatic approach: we'll store audio_url in the description field
    # as a prefix "AUDIO_URL:...\n" — ugly but works without schema change.
    # Actually, let me just re-fetch the RSS. It's cached by the HTTP layer
    # and podcast RSS feeds are tiny.

    from siphon.podcast import fetch_podcast_rss, parse_podcast_feed

    loop = asyncio.get_event_loop()
    xml_bytes = await loop.run_in_executor(None, fetch_podcast_rss, resolved.url)
    feed_data = parse_podcast_feed(xml_bytes)

    # Find the episode by GUID
    audio_url = None
    for ep in feed_data.get("episodes", []):
        if ep["guid"] == video_id:
            audio_url = ep["audio_url"]
            break

    if audio_url is None:
        db.update_episode_status(
            video_id, feed_name, "failed",
            error=f"Audio URL not found for GUID {video_id}",
        )
        return

    filename = episode_filename(video_id, audio_url)
    output_path = os.path.join(config.storage.download_dir, feed_name, filename)

    file_size = await loop.run_in_executor(
        None, download_podcast_audio, audio_url, output_path,
    )

    db.update_episode_status(
        video_id, feed_name, "done",
        file_path=output_path,
        file_size=file_size,
        mime_type="audio/mpeg",
    )
    logger.info("Downloaded podcast %s for feed %s", video_id, feed_name)


async def _run_llm_trim(episode, resolved, config, db) -> None:
    """Run LLM trim on a downloaded episode."""
    from siphon.llm_trim import run_llm_trim

    video_id = episode["video_id"]
    feed_name = episode["feed_name"]
    file_path = episode["file_path"]

    logger.info("Running LLM trim on %s/%s", feed_name, video_id)
    db.update_episode_status(video_id, feed_name, "done", llm_trim_status="pending")

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None, run_llm_trim, file_path, resolved, config.llm,
    )

    db.update_episode_status(
        video_id, feed_name, "done",
        llm_trim_status=result["llm_trim_status"],
        llm_segments_json=result["llm_segments_json"],
        llm_cuts_applied=result["llm_cuts_applied"],
    )

    if result["llm_trim_status"] == "done":
        logger.info(
            "LLM trim complete for %s/%s: %d cuts applied",
            feed_name, video_id, result["llm_cuts_applied"],
        )
    else:
        logger.warning(
            "LLM trim failed for %s/%s: %s",
            feed_name, video_id, result.get("error", "unknown"),
        )


# ---------------------------------------------------------------------- #
# Disk pruning
# ---------------------------------------------------------------------- #


async def _prune_disk(config: SiphonConfig, db: Database) -> None:
    """Remove old episodes to stay within per-feed and global disk limits."""

    for feed in config.feeds:
        keep_limit = _get_keep_per_feed(config, feed.type)
        count = db.get_feed_episode_count(feed.name)
        if count > keep_limit:
            excess = count - keep_limit
            oldest = db.get_oldest_done_episodes(excess)
            for ep in oldest:
                if ep["feed_name"] != feed.name:
                    continue
                if ep.get("file_path"):
                    try:
                        os.remove(ep["file_path"])
                    except OSError:
                        pass
                db.update_episode_status(ep["video_id"], ep["feed_name"], "pruned")
                logger.info("Pruned %s from %s (per-feed limit)", ep["video_id"], ep["feed_name"])

    max_bytes = config.storage.max_disk_gb * (1024 ** 3)
    usage = db.get_disk_usage()

    if usage > max_bytes:
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
            db.update_episode_status(ep["video_id"], ep["feed_name"], "pruned")
            usage -= file_size
            logger.info("Pruned %s from %s (disk limit)", ep["video_id"], ep["feed_name"])
