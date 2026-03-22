"""Web UI routes for feed management."""

from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from fastapi import APIRouter, Form, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from siphon.config import FeedConfig, SiphonConfig, resolve_feed
from siphon.db import Database

router = APIRouter(prefix="/ui")

templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")
)


def _flash(request: Request, text: str, msg_type: str = "info") -> None:
    if not hasattr(request.state, "messages"):
        request.state.messages = []
    request.state.messages.append({"text": text, "type": msg_type})


def _get_messages(request: Request) -> list[dict]:
    return getattr(request.state, "messages", [])


def _get_background_tasks(app) -> set:
    """Return (and lazily create) the set of strong task references on app.state."""
    if not hasattr(app.state, "_background_tasks"):
        app.state._background_tasks = set()
    return app.state._background_tasks


def _slugify(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_]+", "-", s)
    return s.strip("-")[:60]


def _get_feed_display(request: Request) -> list[dict]:
    config = request.app.state.config
    db = request.app.state.db

    feeds_display = []
    for fc in config.feeds:
        resolved = resolve_feed(fc, config.defaults)
        db_feed = db.get_feed(fc.name) or {}
        episodes = db.get_episodes_by_feed(fc.name)

        status_counts = {}
        in_rss = 0
        whisper_pending = 0
        claude_pending = 0
        queued = 0
        sb_cuts_total = 0
        llm_cuts_total = 0
        for ep in episodes:
            s = ep["status"]
            status_counts[s] = status_counts.get(s, 0) + 1
            if s == "done":
                if resolved.llm_trim and ep.get("llm_trim_status") not in ("done", "skipped"):
                    claude_pending += 1  # legacy: done but llm not processed
                else:
                    in_rss += 1
            if s == "pending_whisper":
                whisper_pending += 1
            if s == "pending_claude":
                claude_pending += 1
            if s in ("pending", "eligible", "downloading"):
                queued += 1
            sb_cuts_total += ep.get("sb_cuts_applied") or 0
            llm_cuts_total += ep.get("llm_cuts_applied") or 0

        feeds_display.append({
            "name": fc.name,
            "url": fc.url,
            "feed_type": fc.type,
            "mode": resolved.mode,
            "quality": resolved.quality,
            "sponsorblock": resolved.sponsorblock,
            "sponsorblock_categories": resolved.sponsorblock_categories,
            "sponsorblock_delay_minutes": resolved.sponsorblock_delay_minutes,
            "block_shorts": resolved.block_shorts,
            "min_duration_seconds": resolved.min_duration_seconds,
            "date_cutoff": resolved.date_cutoff,
            "title_exclude": resolved.title_exclude,
            "llm_trim": resolved.llm_trim,
            "claude_prompt_extra": resolved.claude_prompt_extra,
            "claude_prompt_override": resolved.claude_prompt_override,
            "display_name": resolved.display_name,
            "pc_url": resolved.pc_url,
            "image_url": db_feed.get("image_url"),
            "last_checked_at": db_feed.get("last_checked_at"),
            "last_error": db_feed.get("last_error"),
            "episode_counts": status_counts,
            "in_rss": in_rss,
            "whisper_pending": whisper_pending,
            "claude_pending": claude_pending,
            "queued": queued,
            "sb_cuts_total": sb_cuts_total,
            "llm_cuts_total": llm_cuts_total,
        })

    return feeds_display


def _get_system_status(config: SiphonConfig, db: Database) -> dict:
    """Build a summary dict of system-wide status for the dashboard."""
    from siphon.activity import get_active_counts, get_pause_state

    sched = config.schedule
    yt_recent = db.get_recent_download_count(hours=1, feed_type="youtube")
    pod_recent = db.get_recent_download_count(hours=1, feed_type="podcast")
    active = get_active_counts()

    row = db.conn.execute(
        "SELECT "
        "  SUM(CASE WHEN status = 'eligible' THEN 1 ELSE 0 END) AS dl_queue, "
        "  SUM(CASE WHEN status = 'pending_whisper' THEN 1 ELSE 0 END) AS whisper_queue, "
        "  SUM(CASE WHEN status = 'pending_claude' THEN 1 ELSE 0 END) AS claude_queue "
        "FROM episodes"
    ).fetchone()

    return {
        "youtube_downloads_this_hour": yt_recent,
        "youtube_downloads_max": sched.youtube_max_downloads_per_hour,
        "podcast_downloads_this_hour": pod_recent,
        "podcast_downloads_max": sched.podcast_max_downloads_per_hour,
        "dl_queue": int(row["dl_queue"] or 0),
        "whisper_queue": int(row["whisper_queue"] or 0),
        "claude_queue": int(row["claude_queue"] or 0),
        "active_dl": active.get("download", 0),
        "active_whisper": active.get("whisper", 0),
        "active_claude": active.get("claude", 0),
        "pause_state": get_pause_state(),
    }


# ------------------------------------------------------------------ #
# Cookie test (localhost-only, no auth)
# ------------------------------------------------------------------ #

@router.get("/test-cookies")
async def test_cookies_ui(request: Request):
    import asyncio
    from siphon.downloader import test_youtube_cookies

    config = request.app.state.config
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, test_youtube_cookies, config.cookies)
    return JSONResponse(result)


# ------------------------------------------------------------------ #
# Activity log JSON endpoint
# ------------------------------------------------------------------ #

@router.get("/activity")
async def activity_log(request: Request):
    from siphon.activity import get_recent
    return JSONResponse(get_recent(50))


@router.get("/activity-log", response_class=HTMLResponse)
async def activity_log_page(request: Request):
    from siphon.activity import get_recent, get_status as get_activity_status
    config = request.app.state.config
    db = request.app.state.db
    status_info = get_activity_status()
    return templates.TemplateResponse("activity_log.html", {
        "request": request,
        "activity_log": get_recent(50),
        "status": _get_system_status(config, db),
        "current_status": status_info["text"],
    })


# ------------------------------------------------------------------ #
# Feed list
# ------------------------------------------------------------------ #

@router.get("/", response_class=HTMLResponse)
async def feeds_page(request: Request):
    _reload_config(request.app)
    config = request.app.state.config
    db = request.app.state.db

    feeds = _get_feed_display(request)
    disk_usage = db.get_disk_usage()
    total_episodes = sum(
        sum(f["episode_counts"].values()) for f in feeds
    )

    status = _get_system_status(config, db)

    # Build auth-embedded base URL for RSS links
    # https://user:pass@host/feed/name
    from urllib.parse import urlparse
    parsed = urlparse(config.server.base_url)
    auth_base_url = f"{parsed.scheme}://{config.auth.username}:{config.auth.password}@{parsed.netloc}"

    return templates.TemplateResponse("feeds.html", {
        "request": request,
        "active_page": "feeds",
        "feeds": feeds,
        "disk_usage_gb": round(disk_usage / (1024 ** 3), 2),
        "max_disk_gb": config.storage.max_disk_gb,
        "total_episodes": total_episodes,
        "status": status,
        "auth_base_url": auth_base_url,
        "messages": _get_messages(request),
    })


# ------------------------------------------------------------------ #
# Check feeds now
# ------------------------------------------------------------------ #

@router.post("/check-now")
async def check_now(request: Request):
    import asyncio
    import logging
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    from siphon.activity import set_status
    from siphon.pipeline import check_feeds, process_downloads

    logger = logging.getLogger(__name__)

    # Reload config from disk so any changes made via the UI are picked up
    _reload_config(request.app)
    config = request.app.state.config
    db = request.app.state.db

    logger.info("Manual feed check triggered: %d feeds", len(config.feeds))

    async def _check_with_logging():
        try:
            await check_feeds(config, db)
            logger.info("Manual feed check completed, starting downloads")
            await process_downloads(config, db)
            logger.info("Manual download processing completed")
        except Exception as e:
            logger.error("Manual feed check failed: %s", e, exc_info=True)
        finally:
            tz = ZoneInfo(config.server.timezone)
            next_time = (datetime.now(tz) + timedelta(minutes=config.schedule.check_interval_minutes)).strftime("%H:%M:%S")
            set_status(f"Idle \u2014 next check at {next_time}")

    # Keep a strong reference so the task isn't garbage-collected mid-flight.
    task = asyncio.create_task(_check_with_logging())
    _get_background_tasks(request.app).add(task)
    task.add_done_callback(_get_background_tasks(request.app).discard)

    return RedirectResponse("/ui/", status_code=303)


def _reload_config(app) -> None:
    """Reload config from disk into app.state so new/renamed feeds are picked up."""
    import logging
    from siphon.config import load_config

    logger = logging.getLogger(__name__)
    config = app.state.config
    config_path = getattr(config, "_config_path", None)
    if not config_path:
        return
    try:
        new_config = load_config(config_path)
        app.state.config = new_config
        # Sync any new feeds to DB
        db = app.state.db
        for feed in new_config.feeds:
            db.upsert_feed(feed.name, feed.url, feed.type)
        logger.info("Config reloaded: %d feeds", len(new_config.feeds))
    except Exception as e:
        logger.error("Config reload failed: %s", e)


# ------------------------------------------------------------------ #
# Add feed
# ------------------------------------------------------------------ #

@router.get("/add", response_class=HTMLResponse)
async def add_feed_page(request: Request):
    return templates.TemplateResponse("add_feed.html", {
        "request": request,
        "active_page": "add",
        "prefill": {},
        "messages": _get_messages(request),
    })


@router.post("/add")
async def add_feed_submit(
    request: Request,
    url: str = Form(...),
    name: str = Form(...),
    type: str = Form("youtube"),
    mode: str = Form(""),
    quality: str = Form(""),
    sponsorblock: str = Form(""),
    sponsorblock_categories: str = Form(""),
    sponsorblock_delay_minutes: str = Form(""),
    block_shorts: str = Form(""),
    min_duration_seconds: str = Form(""),
    llm_trim: str = Form(""),
    date_cutoff: str = Form(""),
    title_exclude: str = Form(""),
    claude_prompt_extra: str = Form(""),
    claude_prompt_override: str = Form(""),
    display_name: str = Form(""),
    pc_url: str = Form(""),
):
    config = request.app.state.config
    db = request.app.state.db

    # Sanitize inputs
    url = url.strip()
    name = _slugify(name) if name else _slugify(url.split("/")[-1])
    if not name:
        name = f"feed-{len(config.feeds)}"

    # Check for duplicate name
    for fc in config.feeds:
        if fc.name == name:
            _flash(request, f"Feed '{name}' already exists.", "error")
            return templates.TemplateResponse("add_feed.html", {
                "request": request,
                "active_page": "add",
                "prefill": {"url": url, "name": name, "type": type},
                "messages": _get_messages(request),
            })

    feed_data: dict = {"name": name, "url": url, "type": type}
    if mode:
        feed_data["mode"] = mode
    if quality:
        feed_data["quality"] = quality if quality == "max" else int(quality)
    if sponsorblock:
        feed_data["sponsorblock"] = sponsorblock == "true"
    if sponsorblock_categories:
        feed_data["sponsorblock_categories"] = [
            c.strip() for c in sponsorblock_categories.split(",") if c.strip()
        ]
    if sponsorblock_delay_minutes:
        feed_data["sponsorblock_delay_minutes"] = int(sponsorblock_delay_minutes)
    if block_shorts:
        feed_data["block_shorts"] = block_shorts == "true"
    if min_duration_seconds:
        feed_data["min_duration_seconds"] = int(min_duration_seconds)
    if llm_trim:
        feed_data["llm_trim"] = llm_trim == "true"
    if date_cutoff:
        feed_data["date_cutoff"] = date_cutoff
    if title_exclude:
        feed_data["title_exclude"] = [t.strip() for t in title_exclude.split(",") if t.strip()]
    if claude_prompt_extra:
        feed_data["claude_prompt_extra"] = claude_prompt_extra
    if claude_prompt_override:
        feed_data["claude_prompt_override"] = claude_prompt_override
    if display_name:
        feed_data["display_name"] = display_name
    if pc_url:
        feed_data["pc_url"] = pc_url

    new_feed = FeedConfig(**feed_data)
    config.feeds.append(new_feed)
    db.upsert_feed(name, url, type)

    _save_config(config)

    return RedirectResponse("/ui/", status_code=303)


# ------------------------------------------------------------------ #
# Feed actions — all use POST with feed_name in form body
# ------------------------------------------------------------------ #

@router.post("/feed-action")
async def feed_action(
    request: Request,
    feed_name: str = Form(...),
    action: str = Form(...),
    # Update fields
    mode: str = Form("video"),
    quality: str = Form("1440"),
    sponsorblock: str = Form("true"),
    sponsorblock_categories: str = Form(""),
    sponsorblock_delay_minutes: int = Form(4320),
    block_shorts: str = Form("true"),
    min_duration_seconds: int = Form(60),
    llm_trim: str = Form("false"),
    date_cutoff: str = Form(""),
    title_exclude: str = Form(""),
    claude_prompt_extra: str = Form(""),
    claude_prompt_override: str = Form(""),
    display_name: str = Form(""),
    pc_url: str = Form(""),
    new_name: str = Form(""),
):
    config = request.app.state.config
    db = request.app.state.db

    if action == "update":
        return _do_update(
            config, feed_name, mode, quality, sponsorblock,
            sponsorblock_categories, sponsorblock_delay_minutes,
            block_shorts, min_duration_seconds,
            llm_trim, date_cutoff, title_exclude, claude_prompt_extra,
            claude_prompt_override, display_name, pc_url,
        )
    elif action == "rename":
        return _do_rename(config, db, feed_name, new_name)
    elif action == "delete":
        return _do_delete(config, db, feed_name)
    elif action == "catchup":
        return _do_catchup(config, db, feed_name)
    else:
        return RedirectResponse("/ui/", status_code=303)


def _do_update(config, feed_name, mode, quality, sponsorblock,
               sponsorblock_categories, sponsorblock_delay_minutes,
               block_shorts, min_duration_seconds,
               llm_trim, date_cutoff, title_exclude, claude_prompt_extra,
               claude_prompt_override, display_name, pc_url=""):
    for i, fc in enumerate(config.feeds):
        if fc.name == feed_name:
            update = {
                "name": fc.name,
                "url": fc.url,
                "type": fc.type,
                "mode": mode,
                "quality": quality if quality == "max" else int(quality),
                "sponsorblock": sponsorblock == "true",
                "sponsorblock_categories": (
                    [c.strip() for c in sponsorblock_categories.split(",") if c.strip()]
                    if sponsorblock_categories else []
                ),
                "sponsorblock_delay_minutes": sponsorblock_delay_minutes,
                "block_shorts": block_shorts == "true",
                "min_duration_seconds": min_duration_seconds,
                "llm_trim": llm_trim == "true",
                "date_cutoff": date_cutoff if date_cutoff else None,
                "title_exclude": [t.strip() for t in title_exclude.split(",") if t.strip()],
                "claude_prompt_extra": claude_prompt_extra if claude_prompt_extra else None,
                "claude_prompt_override": claude_prompt_override if claude_prompt_override else None,
                "display_name": display_name if display_name else None,
                "pc_url": pc_url if pc_url else None,
            }
            config.feeds[i] = FeedConfig(**update)
            break
    _save_config(config)
    return RedirectResponse("/ui/", status_code=303)


def _do_rename(config, db, feed_name, new_name):
    new_name = _slugify(new_name)
    if not new_name or new_name == feed_name:
        return RedirectResponse("/ui/", status_code=303)

    # Check for duplicate
    if any(fc.name == new_name for fc in config.feeds):
        return RedirectResponse("/ui/", status_code=303)

    # Update config
    for i, fc in enumerate(config.feeds):
        if fc.name == feed_name:
            data = fc.model_dump()
            data["name"] = new_name
            config.feeds[i] = FeedConfig(**data)
            break

    # Update DB — delete old, insert new
    old_feed = db.get_feed(feed_name)
    if old_feed:
        db.upsert_feed(new_name, old_feed["url"], old_feed.get("feed_type", "youtube"))
        # Update episodes to point to new feed name
        db.conn.execute(
            "UPDATE episodes SET feed_name = ? WHERE feed_name = ?",
            (new_name, feed_name),
        )
        db.conn.commit()
        db.delete_feed(feed_name)

    # Rename media directory (skip if old name is invalid as a path component)
    download_dir = config.storage.download_dir
    old_dir = os.path.join(download_dir, feed_name)
    new_dir = os.path.join(download_dir, new_name)
    try:
        if os.path.isdir(old_dir) and not os.path.exists(new_dir):
            os.rename(old_dir, new_dir)
            # Update file paths in DB
            db.conn.execute(
                "UPDATE episodes SET file_path = REPLACE(file_path, ?, ?) WHERE feed_name = ?",
                (feed_name, new_name, new_name),
            )
            db.conn.commit()
    except OSError:
        pass  # Old dir didn't exist or had an invalid name — no files to move

    _save_config(config)
    return RedirectResponse("/ui/", status_code=303)


def _do_delete(config, db, feed_name):
    config.feeds = [f for f in config.feeds if f.name != feed_name]

    download_dir = config.storage.download_dir
    feed_dir = os.path.join(download_dir, feed_name)
    if os.path.isdir(feed_dir):
        for f in os.listdir(feed_dir):
            try:
                os.remove(os.path.join(feed_dir, f))
            except OSError:
                pass
        try:
            os.rmdir(feed_dir)
        except OSError:
            pass

    db.delete_episodes_by_feed(feed_name)
    db.delete_feed(feed_name)
    _save_config(config)
    return RedirectResponse("/ui/", status_code=303)


def _do_catchup(config, db, feed_name):
    # Find the most recent episode actually in RSS
    latest = db.conn.execute(
        """SELECT upload_date FROM episodes
           WHERE feed_name = ? AND status = 'done'
             AND llm_trim_status IN ('done', 'skipped')
           ORDER BY upload_date DESC LIMIT 1""",
        (feed_name,),
    ).fetchone()

    if not latest or not latest["upload_date"]:
        # No episodes in RSS — just set cutoff to today
        new_cutoff = datetime.now(timezone.utc).strftime("%Y%m%d")
    else:
        # Set cutoff to 1 day before the latest episode
        from datetime import datetime as dt
        latest_date = dt.strptime(latest["upload_date"], "%Y%m%d")
        new_cutoff = (latest_date - timedelta(days=1)).strftime("%Y%m%d")

    # Update config
    for i, fc in enumerate(config.feeds):
        if fc.name == feed_name:
            update = fc.model_dump()
            update["date_cutoff"] = new_cutoff
            config.feeds[i] = FeedConfig(**update)
            break

    # Delete files and prune episodes OLDER than the latest RSS episode
    download_dir = config.storage.download_dir
    all_episodes = db.get_episodes_by_feed(feed_name)
    keep_id = latest["upload_date"] if latest else None

    for ep in all_episodes:
        # Keep the latest RSS episode, prune everything else
        if ep["status"] == "done" and ep.get("llm_trim_status") in ("done", "skipped"):
            if ep.get("upload_date") == keep_id:
                # This is the one we're keeping (or one of them if same day)
                keep_id = None  # Only keep the first match
                continue

        # Skip episodes that aren't taking up disk space
        if ep["status"] in ("filtered", "pruned"):
            continue

        # Delete file if exists
        if ep.get("file_path"):
            try:
                os.remove(ep["file_path"])
            except OSError:
                pass

        # Also delete any transcript file
        transcript = os.path.join(download_dir, feed_name, f"{ep['video_id']}_transcript.json")
        try:
            os.remove(transcript)
        except OSError:
            pass

        db.update_episode_status(ep["video_id"], feed_name, "pruned")

    _save_config(config)
    return RedirectResponse("/ui/", status_code=303)


# ------------------------------------------------------------------ #
# OPML Import
# ------------------------------------------------------------------ #

@router.get("/import", response_class=HTMLResponse)
async def import_page(request: Request):
    return templates.TemplateResponse("import_opml.html", {
        "request": request,
        "active_page": "import",
        "feeds_to_review": None,
        "messages": _get_messages(request),
    })


@router.post("/import")
async def import_upload(request: Request, opml_file: UploadFile = File(...)):
    content = await opml_file.read()
    feeds = _parse_opml(content)

    if not feeds:
        _flash(request, "No feeds found in OPML file.", "error")
        return templates.TemplateResponse("import_opml.html", {
            "request": request,
            "active_page": "import",
            "feeds_to_review": None,
            "messages": _get_messages(request),
        })

    config = request.app.state.config
    existing_urls = {fc.url for fc in config.feeds}
    existing_names = {fc.name for fc in config.feeds}

    new_feeds = []
    for f in feeds:
        if f["url"] not in existing_urls:
            name = f["name"]
            counter = 2
            while name in existing_names:
                name = f"{f['name']}-{counter}"
                counter += 1
            f["name"] = name
            existing_names.add(name)
            new_feeds.append(f)

    if not new_feeds:
        _flash(request, "All feeds in the OPML file already exist.", "info")
        return templates.TemplateResponse("import_opml.html", {
            "request": request,
            "active_page": "import",
            "feeds_to_review": None,
            "messages": _get_messages(request),
        })

    return templates.TemplateResponse("import_opml.html", {
        "request": request,
        "active_page": "import",
        "feeds_to_review": new_feeds,
        "messages": _get_messages(request),
    })


@router.post("/import/confirm")
async def import_confirm(request: Request):
    config = request.app.state.config
    db = request.app.state.db
    form = await request.form()

    total = int(form.get("total", 0))
    imported = 0

    for i in range(total):
        if not form.get(f"import_{i}"):
            continue

        name = form.get(f"name_{i}", "").strip()
        url = form.get(f"url_{i}", "").strip()
        if not name or not url:
            continue

        name = _slugify(name)
        if not name:
            name = f"podcast-{i}"

        date_cutoff = form.get(f"date_cutoff_{i}", "").strip() or None
        llm_trim_str = form.get(f"llm_trim_{i}", "").strip()
        title_exclude_str = form.get(f"title_exclude_{i}", "").strip()

        feed_data: dict = {
            "name": name,
            "url": url,
            "type": "podcast",
            "mode": "audio",
        }
        if date_cutoff:
            feed_data["date_cutoff"] = date_cutoff
        if llm_trim_str:
            feed_data["llm_trim"] = llm_trim_str == "true"
        if title_exclude_str:
            feed_data["title_exclude"] = [t.strip() for t in title_exclude_str.split(",") if t.strip()]

        new_feed = FeedConfig(**feed_data)
        config.feeds.append(new_feed)
        db.upsert_feed(name, url, "podcast")
        imported += 1

    _save_config(config)
    return RedirectResponse(f"/ui/?imported={imported}", status_code=303)


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

def _parse_opml(content: bytes) -> list[dict]:
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return []

    feeds = []
    for outline in root.iter("outline"):
        xml_url = outline.get("xmlUrl") or outline.get("xmlurl") or outline.get("url")
        if not xml_url:
            continue

        title = outline.get("title") or outline.get("text") or "unknown"
        name = _slugify(title)
        if not name:
            name = f"feed-{len(feeds)}"

        feeds.append({
            "name": name,
            "url": xml_url,
            "title": title,
        })

    return feeds


def _save_config(config) -> None:
    import yaml
    from pathlib import Path

    data = config.model_dump()

    def _clean(obj):
        if isinstance(obj, dict):
            return {k: _clean(v) for k, v in obj.items() if v is not None or k in ("date_cutoff",)}
        if isinstance(obj, list):
            return [_clean(i) for i in obj]
        return obj

    data = _clean(data)

    config_path = Path(getattr(config, "_config_path", "config.yaml"))
    config_path.write_text(
        yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
