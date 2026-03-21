"""Pipeline orchestration for Siphon.

Coordinates the two main scheduled jobs -- feed checking and episode
downloading -- wiring together the *db*, *downloader*, *filters*,
*podcast*, and *llm_trim* modules.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

from siphon.activity import log_activity, set_status
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
    set_status("Checking feeds...")
    log_activity("Starting feed check")

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
                log_activity(f"Checking {feed_db['name']}", feed=feed_db["name"])
                if resolved.type == "podcast":
                    await _check_podcast_feed(resolved, db)
                else:
                    await _check_youtube_feed(resolved, config, db)

                db.update_feed_checked(feed_db["name"])

            except Exception as exc:
                logger.error("Error checking feed %s: %s", feed_db["name"], exc)
                db.update_feed_checked(feed_db["name"], error=str(exc))

    log_activity("Feed check complete")


def _normalize_youtube_url(url: str) -> str:
    """Ensure a YouTube channel URL points to the /videos tab."""
    # Strip trailing slash
    url = url.rstrip("/")
    # If it's a channel URL without a tab, add /videos
    if ("youtube.com/@" in url or "youtube.com/c/" in url or "youtube.com/channel/" in url):
        if not any(url.endswith(f"/{tab}") for tab in ("videos", "shorts", "streams", "playlists", "community")):
            url += "/videos"
    return url


async def _check_youtube_feed(resolved, config, db) -> None:
    """Check a YouTube feed for new episodes via YouTube Data API.

    First check: resolve channel ID, get metadata, paginate backwards
    until date_cutoff.
    Subsequent checks: paginate backwards until we find a known video
    or hit date_cutoff.
    """
    from siphon.youtube import get_channel_metadata, list_videos, resolve_channel_id

    api_key = config.youtube.api_key
    feed_db = db.get_feed(resolved.name)
    channel_id = feed_db.get("channel_id") if feed_db else None

    # Resolve channel ID if we don't have one
    if not channel_id:
        log_activity("Resolving channel ID...", feed=resolved.name)
        loop = asyncio.get_event_loop()
        channel_id = await loop.run_in_executor(
            None, resolve_channel_id, resolved.url, api_key,
        )
        if not channel_id:
            raise Exception(f"Could not resolve channel ID for {resolved.url}")
        db.update_feed_channel_id(resolved.name, channel_id)

        # Get channel metadata (thumbnail, title)
        meta = await loop.run_in_executor(
            None, get_channel_metadata, channel_id, api_key,
        )
        if meta.get("image_url"):
            db.update_feed_image(resolved.name, meta["image_url"])

    # Get known video IDs for this feed to detect where to stop
    existing = db.get_episodes_by_feed(resolved.name)
    known_ids = {ep["video_id"] for ep in existing}

    # Fetch videos from API — stops at cutoff date or known video
    loop = asyncio.get_event_loop()
    entries = await loop.run_in_executor(
        None, list_videos, channel_id, api_key,
        resolved.date_cutoff, known_ids,
    )

    new_count = _insert_youtube_entries(entries, resolved, db)

    if new_count > 0:
        log_activity(f"Found {new_count} new episodes", feed=resolved.name)


def _insert_youtube_entries(entries: list, resolved, db) -> int:
    """Insert YouTube entries into DB with filters. Returns new episode count."""
    new_count = 0
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
            new_count += 1

    return new_count


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

    if new_count > 0:
        log_activity(f"Found {new_count} new episodes", feed=resolved.name)
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


_download_lock = asyncio.Lock()


async def process_downloads(config: SiphonConfig, db: Database) -> None:
    """Download eligible episodes and prune disk when needed."""
    if _download_lock.locked():
        logger.info("process_downloads already running, skipping")
        return
    async with _download_lock:
        await _process_downloads_inner(config, db)


async def _process_downloads_inner(config: SiphonConfig, db: Database) -> None:
    """Inner download processing — runs under _download_lock."""
    set_status("Processing downloads...")

    _llm_processed: set = set()  # track episodes LLM-processed this cycle
    _downloaded: set = set()  # track episodes downloaded this cycle

    promoted = db.promote_eligible_episodes()
    if promoted and promoted > 0:
        log_activity(f"Promoted {promoted} episodes to eligible")
    db.reset_stale_downloads()
    db.retry_failed_episodes()

    llm_sem = asyncio.Semaphore(config.llm.claude_concurrency)
    llm_tasks: list[asyncio.Task] = []

    for feed_type in ("youtube", "podcast"):
        max_per_hour, workers, delay = _get_schedule_params(config, feed_type)

        while True:  # Keep downloading until queue empty or rate limited
            recent = db.get_recent_download_count(hours=1, feed_type=feed_type)
            remaining = max(0, max_per_hour - recent)
            limit = min(workers, remaining)

            if limit == 0:
                if remaining == 0:
                    log_activity(
                        f"Rate limit reached for {feed_type} ({recent}/{max_per_hour}/hr)",
                        level="warning",
                    )
                    logger.info(
                        "Hourly %s download cap reached (%d/%d), skipping",
                        feed_type, recent, max_per_hour,
                    )
                break

            episodes = db.get_eligible_episodes(limit, feed_type=feed_type)
            if not episodes:
                break

            for i, episode in enumerate(episodes):
                if i > 0 and delay > 0:
                    logger.debug("Throttling %s: waiting %ds", feed_type, delay)
                    await asyncio.sleep(delay)

                video_id = episode["video_id"]
                feed_name = episode["feed_name"]
                title = episode.get("title", video_id)

                if (video_id, feed_name) in _downloaded:
                    continue
                _downloaded.add((video_id, feed_name))

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
                set_status(f"Downloading {title[:40]}...")
                log_activity(f"Downloading {title[:50]}", feed=feed_name)

                try:
                    if resolved.type == "podcast":
                        await _download_podcast_episode(episode, resolved, config, db)
                    else:
                        await _download_youtube_episode(episode, resolved, config, db)

                    log_activity(f"Downloaded {title[:50]}", feed=feed_name)

                    # LLM trim post-processing — run concurrently
                    if resolved.llm_trim:
                        ep = db.get_episode(video_id, feed_name)
                        if ep and ep["status"] == "done" and ep.get("file_path"):
                            async def _do_llm(ep=ep, resolved=resolved):
                                async with llm_sem:
                                    await _run_llm_trim(ep, resolved, config, db)
                            llm_tasks.append(asyncio.create_task(_do_llm()))
                            _llm_processed.add((video_id, feed_name))

                except Exception as exc:
                    logger.error("Download failed for %s/%s: %s", feed_name, video_id, exc)
                    log_activity(f"Download failed: {str(exc)[:80]}", feed=feed_name, level="error")
                    db.update_episode_status(
                        video_id, feed_name, "failed",
                        error=str(exc),
                        retry_count=episode["retry_count"] + 1,
                    )

    # Re-process episodes that need LLM trim (reset from error or newly enabled)
    await _process_pending_llm(config, db, skip=_llm_processed, llm_sem=llm_sem, llm_tasks=llm_tasks)

    # Wait for all concurrent LLM tasks to finish
    if llm_tasks:
        set_status(f"LLM processing ({len(llm_tasks)} episodes)...")
        results = await asyncio.gather(*llm_tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                logger.error("LLM task failed: %s", r)

    await _prune_disk(config, db)


async def _process_pending_llm(
    config: SiphonConfig, db: Database, *, skip: set | None = None,
    llm_sem: asyncio.Semaphore | None = None,
    llm_tasks: list | None = None,
) -> None:
    """Re-process done episodes that need LLM trim (null llm_trim_status)."""
    skip = skip or set()
    episodes = db.get_episodes_needing_llm(limit=3)
    for ep in episodes:
        if (ep["video_id"], ep["feed_name"]) in skip:
            continue

        feed_config = None
        for fc in config.feeds:
            if fc.name == ep["feed_name"]:
                feed_config = fc
                break
        if feed_config is None:
            continue

        resolved = resolve_feed(feed_config, config.defaults)
        if not resolved.llm_trim:
            continue

        if ep.get("file_path"):
            if llm_sem is not None and llm_tasks is not None:
                async def _do_llm(ep=ep, resolved=resolved):
                    async with llm_sem:
                        await _run_llm_trim(ep, resolved, config, db)
                llm_tasks.append(asyncio.create_task(_do_llm()))
            else:
                await _run_llm_trim(ep, resolved, config, db)


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

        # Query SponsorBlock for segment count if enabled
        sb_cuts: int | None = None
        if resolved.sponsorblock:
            from siphon.sponsorblock import get_segment_count
            sb_cuts = await loop.run_in_executor(
                None, get_segment_count, video_id, resolved.sponsorblock_categories,
            )
            logger.info("SponsorBlock segments for %s: %d", video_id, sb_cuts)

        db.update_episode_status(
            video_id, feed_name, "done",
            file_path=path, file_size=size, mime_type=mime,
            **({"sb_cuts_applied": sb_cuts} if sb_cuts is not None else {}),
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
    title = episode.get("title", video_id)

    logger.info("Running LLM trim on %s/%s", feed_name, video_id)
    set_status(f"LLM processing {title[:40]}...")
    log_activity(f"LLM processing {title[:50]}", feed=feed_name)
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
        cuts = result["llm_cuts_applied"]
        log_activity(f"LLM: {cuts} cuts applied to {title[:50]}", feed=feed_name)
        logger.info(
            "LLM trim complete for %s/%s: %d cuts applied",
            feed_name, video_id, cuts,
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
