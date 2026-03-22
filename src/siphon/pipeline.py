"""Pipeline orchestration for Siphon.

Three independent workers form a queue-based pipeline:

    Download Queue -> Whisper Queue -> Claude Queue
       (5 min)         (30 sec)         (30 sec)

The download worker fetches media files and sets status to
pending_whisper (if llm_trim is enabled) or done (if not).

The Whisper worker transcribes one episode at a time, saves the
transcript JSON to disk, and moves the episode to pending_claude.

The Claude worker reads transcript JSON, runs ad detection with
Claude, applies ffmpeg cuts, and sets the episode to done.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from siphon.activity import check_paused, log_activity, set_status
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
# Interrupt recovery
# ---------------------------------------------------------------------- #


def recover_interrupted(config: SiphonConfig, db: Database) -> None:
    """Reset episodes stuck in intermediate pipeline states after a restart.

    - pending_whisper: leave as is (Whisper worker will pick them up)
    - pending_claude: check if transcript JSON exists on disk; if not,
      reset to pending_whisper so they get re-transcribed.
    - downloading: handled by existing reset_stale_downloads
    """
    pending_claude = db.get_pending_claude(limit=100)
    reset_count = 0
    for ep in pending_claude:
        transcript_path = _transcript_path(config, ep["feed_name"], ep["video_id"])
        if not os.path.exists(transcript_path):
            db.update_episode_status(ep["video_id"], ep["feed_name"], "pending_whisper")
            reset_count += 1
            logger.info(
                "Reset %s/%s from pending_claude to pending_whisper (transcript missing)",
                ep["feed_name"], ep["video_id"],
            )
    if reset_count:
        log_activity(f"Recovery: reset {reset_count} episodes to pending_whisper")


def _transcript_path(config: SiphonConfig, feed_name: str, video_id: str) -> str:
    """Return the expected transcript JSON path for an episode."""
    return os.path.join(
        config.storage.download_dir, feed_name, f"{video_id}_transcript.json"
    )


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
                from siphon.youtube import YouTubeQuotaExceeded
                if isinstance(exc, YouTubeQuotaExceeded):
                    log_activity(str(exc), level="warning")
                    break  # Stop checking YouTube feeds entirely
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
    cooldown_hours = config.youtube.quota_cooldown_hours
    feed_db = db.get_feed(resolved.name)
    channel_id = feed_db.get("channel_id") if feed_db else None

    # Resolve channel ID if we don't have one
    if not channel_id:
        log_activity("Resolving channel ID...", feed=resolved.name)
        loop = asyncio.get_event_loop()
        channel_id = await loop.run_in_executor(
            None, resolve_channel_id, resolved.url, api_key, cooldown_hours,
        )
        if not channel_id:
            raise Exception(f"Could not resolve channel ID for {resolved.url}")
        db.update_feed_channel_id(resolved.name, channel_id)

        # Get channel metadata (thumbnail, title)
        meta = await loop.run_in_executor(
            None, get_channel_metadata, channel_id, api_key, cooldown_hours,
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
        resolved.date_cutoff, known_ids, 200, cooldown_hours,
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
# Worker 1: Download processing
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

    _downloaded: set = set()  # track episodes downloaded this cycle

    promoted = db.promote_eligible_episodes()
    if promoted and promoted > 0:
        log_activity(f"Promoted {promoted} episodes to eligible")
    db.reset_stale_downloads()
    db.retry_failed_episodes()

    for feed_type in ("youtube", "podcast"):
        max_per_hour, workers, delay = _get_schedule_params(config, feed_type)

        while True:  # Keep downloading until queue empty or rate limited
            if check_paused():
                log_activity("Paused — stopping downloads")
                return

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

                    # Decide next status based on llm_trim setting
                    ep = db.get_episode(video_id, feed_name)
                    if ep and ep["status"] == "done" and resolved.llm_trim:
                        # Move to whisper queue
                        db.update_episode_status(video_id, feed_name, "pending_whisper")
                        logger.info("Queued %s/%s for Whisper", feed_name, video_id)

                except Exception as exc:
                    logger.error("Download failed for %s/%s: %s", feed_name, video_id, exc)
                    log_activity(f"Download failed: {str(exc)[:80]}", feed=feed_name, level="error")
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


# ---------------------------------------------------------------------- #
# Worker 2: Whisper transcription
# ---------------------------------------------------------------------- #

_whisper_lock = asyncio.Lock()


async def process_whisper(config: SiphonConfig, db: Database) -> None:
    """Process one episode through Whisper transcription."""
    if _whisper_lock.locked():
        logger.debug("process_whisper already running, skipping")
        return
    async with _whisper_lock:
        await _process_whisper_inner(config, db)


async def _process_whisper_inner(config: SiphonConfig, db: Database) -> None:
    """Inner Whisper processing — runs under _whisper_lock."""
    if check_paused():
        return

    episodes = db.get_pending_whisper(limit=1)
    if not episodes:
        return

    ep = episodes[0]
    video_id = ep["video_id"]
    feed_name = ep["feed_name"]
    file_path = ep.get("file_path")
    title = ep.get("title", video_id)

    if not file_path or not os.path.exists(file_path):
        logger.error("File missing for %s/%s, marking failed", feed_name, video_id)
        db.update_episode_status(
            video_id, feed_name, "failed",
            error="Media file missing for Whisper",
        )
        return

    set_status(f"Whisper: {title[:40]}...")
    log_activity(f"Whisper: transcribing {title[:50]}", feed=feed_name)

    t0 = time.time()
    loop = asyncio.get_event_loop()

    try:
        from siphon.transcribe import transcribe
        from siphon.cutter import extract_audio
        import tempfile

        # Step 1: Extract audio if needed (video files)
        is_audio = file_path.lower().endswith((".mp3", ".m4a", ".ogg", ".wav", ".flac", ".aac"))
        temp_audio = None

        if is_audio:
            whisper_input = file_path
        else:
            temp_audio = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
            temp_audio.close()
            try:
                await loop.run_in_executor(None, extract_audio, file_path, temp_audio.name)
                whisper_input = temp_audio.name
            except Exception:
                os.unlink(temp_audio.name)
                raise

        try:
            # Step 2: Transcribe
            transcript = await loop.run_in_executor(
                None, transcribe, whisper_input,
                config.llm.whisper_model, config.llm.whisper_device,
                config.llm.whisper_word_timestamps,
            )
        finally:
            if temp_audio is not None:
                try:
                    os.unlink(temp_audio.name)
                except OSError:
                    pass

        whisper_duration = time.time() - t0

        # Step 3: Save transcript JSON to disk
        transcript_file = _transcript_path(config, feed_name, video_id)
        os.makedirs(os.path.dirname(transcript_file), exist_ok=True)
        transcript_json = json.dumps(transcript)
        with open(transcript_file, "w", encoding="utf-8") as f:
            f.write(transcript_json)

        # Step 4: Record metrics and move to pending_claude
        word_count = len(transcript.get("words", []))
        segment_count = len(transcript.get("segments", []))

        db.update_episode_status(
            video_id, feed_name, "pending_claude",
            whisper_duration_seconds=round(whisper_duration, 2),
            whisper_word_count=word_count,
            whisper_segment_count=segment_count,
            transcript_size_bytes=len(transcript_json),
            whisper_model=config.llm.whisper_model,
        )

        _fmt_time = _format_duration(whisper_duration)
        log_activity(
            f"Whisper: {word_count} words ({_fmt_time})",
            feed=feed_name,
        )
        logger.info(
            "Whisper done for %s/%s: %d words, %d segments in %.1fs",
            feed_name, video_id, word_count, segment_count, whisper_duration,
        )

    except Exception as exc:
        logger.error("Whisper failed for %s/%s: %s", feed_name, video_id, exc)
        log_activity(f"Whisper failed: {str(exc)[:80]}", feed=feed_name, level="error")
        retry_count = (ep.get("llm_retry_count") or 0) + 1
        if retry_count >= 3:
            db.update_episode_status(
                video_id, feed_name, "done",
                llm_trim_status="skipped",
                llm_retry_count=retry_count,
            )
            log_activity(f"Whisper: skipped after {retry_count} failures", feed=feed_name, level="warning")
        else:
            db.update_episode_status(
                video_id, feed_name, "pending_whisper",
                llm_retry_count=retry_count,
                error=str(exc),
            )


# ---------------------------------------------------------------------- #
# Worker 3: Claude ad detection
# ---------------------------------------------------------------------- #

_claude_lock = asyncio.Lock()


async def process_claude(config: SiphonConfig, db: Database) -> None:
    """Process episodes through Claude ad detection, up to claude_concurrency."""
    if _claude_lock.locked():
        logger.debug("process_claude already running, skipping")
        return
    async with _claude_lock:
        await _process_claude_inner(config, db)


async def _process_claude_inner(config: SiphonConfig, db: Database) -> None:
    """Inner Claude processing — runs under _claude_lock."""
    if check_paused():
        return

    concurrency = config.llm.claude_concurrency
    episodes = db.get_pending_claude(limit=concurrency)
    if not episodes:
        return

    sem = asyncio.Semaphore(concurrency)
    tasks = []

    for ep in episodes:
        async def _do(ep=ep):
            async with sem:
                await _process_one_claude(ep, config, db)
        tasks.append(asyncio.create_task(_do()))

    if tasks:
        set_status(f"Claude: {len(tasks)} active...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                logger.error("Claude task failed: %s", r)


async def _process_one_claude(ep: dict, config: SiphonConfig, db: Database) -> None:
    """Run Claude ad detection + ffmpeg cut on a single episode."""
    from siphon.ad_detect import build_transcript_for_claude, detect_ads, filter_segments, resolve_prompt
    from siphon.cutter import cut_segments

    video_id = ep["video_id"]
    feed_name = ep["feed_name"]
    file_path = ep.get("file_path")
    title = ep.get("title", video_id)
    retry_count = ep.get("llm_retry_count") or 0

    # Load transcript from disk
    transcript_file = _transcript_path(config, feed_name, video_id)
    if not os.path.exists(transcript_file):
        logger.error("Transcript missing for %s/%s, resetting to pending_whisper", feed_name, video_id)
        db.update_episode_status(video_id, feed_name, "pending_whisper")
        return

    feed_config = None
    for fc in config.feeds:
        if fc.name == feed_name:
            feed_config = fc
            break
    if feed_config is None:
        logger.warning("Feed config %r not found for %s, skipping", feed_name, video_id)
        return

    resolved = resolve_feed(feed_config, config.defaults)

    log_activity(f"Claude: processing {title[:50]}", feed=feed_name)
    set_status(f"Claude: {title[:40]}...")
    db.update_episode_status(video_id, feed_name, "pending_claude", llm_trim_status="pending")

    try:
        with open(transcript_file, "r", encoding="utf-8") as f:
            transcript = json.load(f)

        transcript_text = transcript.get("text", "")

        if not transcript_text.strip():
            logger.info("Empty transcript for %s/%s, skipping ad detection", feed_name, video_id)
            # Clean up transcript file
            try:
                os.remove(transcript_file)
            except OSError:
                pass
            db.update_episode_status(
                video_id, feed_name, "done",
                llm_trim_status="done",
                llm_segments_json=json.dumps({"segments": []}),
                llm_cuts_applied=0,
            )
            return

        # Skip word timestamps for long episodes
        words = transcript.get("words")
        duration_minutes = transcript.get("duration", 0) / 60
        if duration_minutes > config.llm.word_timestamps_max_minutes:
            logger.info(
                "Episode is %.0f min (> %d min limit), using segment-only timestamps",
                duration_minutes, config.llm.word_timestamps_max_minutes,
            )
            words = None

        t0_claude = time.time()
        loop = asyncio.get_event_loop()

        prompt = resolve_prompt(resolved, config.llm)
        raw_result = await loop.run_in_executor(
            None, detect_ads,
            transcript_text, prompt, config.llm.claude_model,
            config.llm.claude_effort, words, transcript.get("segments"),
        )

        claude_duration = time.time() - t0_claude

        all_segments = raw_result.get("segments", [])
        logger.info("Claude detected %d potential ad segments", len(all_segments))

        # Filter
        high_confidence, marginal = filter_segments(
            all_segments,
            confidence_threshold=config.llm.confidence_threshold,
            min_duration=config.llm.min_segment_duration,
            max_duration=config.llm.max_segment_duration,
        )

        logger.info(
            "After filtering: %d to cut, %d marginal",
            len(high_confidence), len(marginal),
        )

        # Cut with ffmpeg
        ffmpeg_duration = 0.0
        if high_confidence and file_path:
            t0_ffmpeg = time.time()
            await loop.run_in_executor(None, cut_segments, file_path, high_confidence)
            ffmpeg_duration = time.time() - t0_ffmpeg
            logger.info("Applied %d cuts to %s in %.1fs", len(high_confidence), file_path, ffmpeg_duration)

        # Delete transcript file
        try:
            os.remove(transcript_file)
        except OSError:
            pass

        # Update file_size after cuts
        new_size = None
        if file_path and os.path.exists(file_path):
            new_size = os.path.getsize(file_path)

        # Build audit data
        audit = {
            "segments": all_segments,
            "high_confidence": [s.get("label", "") for s in high_confidence],
            "marginal": [s.get("label", "") for s in marginal],
        }

        update_kwargs = {
            "llm_trim_status": "done",
            "llm_segments_json": json.dumps(audit),
            "llm_cuts_applied": len(high_confidence),
            "claude_duration_seconds": round(claude_duration, 2),
            "ffmpeg_duration_seconds": round(ffmpeg_duration, 2),
        }
        if new_size is not None:
            update_kwargs["file_size"] = new_size

        db.update_episode_status(video_id, feed_name, "done", **update_kwargs)

        cuts = len(high_confidence)
        _fmt_claude = _format_duration(claude_duration)
        _fmt_ffmpeg = _format_duration(ffmpeg_duration) if ffmpeg_duration > 0 else "0:00"
        log_activity(f"Claude: {cuts} cuts ({_fmt_claude})", feed=feed_name)
        if ffmpeg_duration > 0:
            log_activity(f"ffmpeg: cut applied ({_fmt_ffmpeg})", feed=feed_name)

    except Exception as exc:
        logger.error("Claude failed for %s/%s: %s", feed_name, video_id, exc)
        log_activity(f"Claude failed: {str(exc)[:80]}", feed=feed_name, level="error")
        new_retry = retry_count + 1
        if new_retry >= 3:
            # Clean up transcript file on give-up
            try:
                os.remove(transcript_file)
            except OSError:
                pass
            db.update_episode_status(
                video_id, feed_name, "done",
                llm_trim_status="skipped",
                llm_retry_count=new_retry,
            )
            log_activity(f"Claude: skipped after {new_retry} failures", feed=feed_name, level="warning")
        else:
            db.update_episode_status(
                video_id, feed_name, "pending_claude",
                llm_trim_status="error",
                llm_segments_json=json.dumps({"error": str(exc)}),
                llm_retry_count=new_retry,
            )
            log_activity(f"Claude: failed (attempt {new_retry}/3)", feed=feed_name, level="error")


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


# ---------------------------------------------------------------------- #
# Helpers
# ---------------------------------------------------------------------- #


def _format_duration(seconds: float) -> str:
    """Format seconds as M:SS."""
    m, s = divmod(int(seconds), 60)
    return f"{m}:{s:02d}"
