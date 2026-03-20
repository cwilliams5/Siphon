"""Web UI routes for feed management."""

from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from fastapi import APIRouter, Form, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from siphon.config import FeedConfig, resolve_feed

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
        for ep in episodes:
            s = ep["status"]
            status_counts[s] = status_counts.get(s, 0) + 1

        feeds_display.append({
            "name": fc.name,
            "url": fc.url,
            "feed_type": fc.type,
            "mode": resolved.mode,
            "quality": resolved.quality,
            "sponsorblock": resolved.sponsorblock,
            "sponsorblock_delay_minutes": resolved.sponsorblock_delay_minutes,
            "block_shorts": resolved.block_shorts,
            "min_duration_seconds": resolved.min_duration_seconds,
            "date_cutoff": resolved.date_cutoff,
            "title_exclude": resolved.title_exclude,
            "llm_trim": resolved.llm_trim,
            "claude_prompt_extra": resolved.claude_prompt_extra,
            "last_checked_at": db_feed.get("last_checked_at"),
            "last_error": db_feed.get("last_error"),
            "episode_counts": status_counts,
        })

    return feeds_display


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
# Feed list
# ------------------------------------------------------------------ #

@router.get("/", response_class=HTMLResponse)
async def feeds_page(request: Request):
    config = request.app.state.config
    db = request.app.state.db

    feeds = _get_feed_display(request)
    disk_usage = db.get_disk_usage()
    total_episodes = sum(
        sum(f["episode_counts"].values()) for f in feeds
    )

    return templates.TemplateResponse("feeds.html", {
        "request": request,
        "active_page": "feeds",
        "feeds": feeds,
        "disk_usage_gb": round(disk_usage / (1024 ** 3), 2),
        "max_disk_gb": config.storage.max_disk_gb,
        "total_episodes": total_episodes,
        "messages": _get_messages(request),
    })


# ------------------------------------------------------------------ #
# Check feeds now
# ------------------------------------------------------------------ #

@router.post("/check-now")
async def check_now(request: Request):
    import asyncio
    from siphon.pipeline import check_feeds

    config = request.app.state.config
    db = request.app.state.db
    asyncio.create_task(check_feeds(config, db))

    return RedirectResponse("/ui/", status_code=303)


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
    llm_trim: str = Form(""),
    date_cutoff: str = Form(""),
    sponsorblock_delay_minutes: str = Form(""),
    title_exclude: str = Form(""),
    claude_prompt_extra: str = Form(""),
):
    config = request.app.state.config
    db = request.app.state.db

    # Sanitize name
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
    if llm_trim:
        feed_data["llm_trim"] = llm_trim == "true"
    if date_cutoff:
        feed_data["date_cutoff"] = date_cutoff
    if sponsorblock_delay_minutes:
        feed_data["sponsorblock_delay_minutes"] = int(sponsorblock_delay_minutes)
    if title_exclude:
        feed_data["title_exclude"] = [t.strip() for t in title_exclude.split(",") if t.strip()]
    if claude_prompt_extra:
        feed_data["claude_prompt_extra"] = claude_prompt_extra

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
    llm_trim: str = Form("false"),
    block_shorts: str = Form("true"),
    min_duration_seconds: int = Form(60),
    date_cutoff: str = Form(""),
    sponsorblock_delay_minutes: int = Form(4320),
    title_exclude: str = Form(""),
    claude_prompt_extra: str = Form(""),
    new_name: str = Form(""),
):
    config = request.app.state.config
    db = request.app.state.db

    if action == "update":
        return _do_update(config, feed_name, mode, quality, sponsorblock,
                          llm_trim, block_shorts, min_duration_seconds,
                          date_cutoff, sponsorblock_delay_minutes,
                          title_exclude, claude_prompt_extra)
    elif action == "rename":
        return _do_rename(config, db, feed_name, new_name)
    elif action == "delete":
        return _do_delete(config, db, feed_name)
    elif action == "catchup":
        return _do_catchup(config, db, feed_name)
    else:
        return RedirectResponse("/ui/", status_code=303)


def _do_update(config, feed_name, mode, quality, sponsorblock, llm_trim,
               block_shorts, min_duration_seconds, date_cutoff,
               sponsorblock_delay_minutes, title_exclude, claude_prompt_extra):
    for i, fc in enumerate(config.feeds):
        if fc.name == feed_name:
            update = {
                "name": fc.name,
                "url": fc.url,
                "type": fc.type,
                "mode": mode,
                "quality": quality if quality == "max" else int(quality),
                "sponsorblock": sponsorblock == "true",
                "llm_trim": llm_trim == "true",
                "block_shorts": block_shorts == "true",
                "min_duration_seconds": min_duration_seconds,
                "sponsorblock_delay_minutes": sponsorblock_delay_minutes,
                "date_cutoff": date_cutoff if date_cutoff else None,
                "title_exclude": [t.strip() for t in title_exclude.split(",") if t.strip()],
                "claude_prompt_extra": claude_prompt_extra if claude_prompt_extra else None,
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

    # Rename media directory
    download_dir = config.storage.download_dir
    old_dir = os.path.join(download_dir, feed_name)
    new_dir = os.path.join(download_dir, new_name)
    if os.path.isdir(old_dir) and not os.path.exists(new_dir):
        os.rename(old_dir, new_dir)
        # Update file paths in DB
        db.conn.execute(
            "UPDATE episodes SET file_path = REPLACE(file_path, ?, ?) WHERE feed_name = ?",
            (feed_name, new_name, new_name),
        )
        db.conn.commit()

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
    today = datetime.now(timezone.utc).strftime("%Y%m%d")

    for i, fc in enumerate(config.feeds):
        if fc.name == feed_name:
            update = fc.model_dump()
            update["date_cutoff"] = today
            config.feeds[i] = FeedConfig(**update)
            break

    download_dir = config.storage.download_dir
    feed_dir = os.path.join(download_dir, feed_name)
    if os.path.isdir(feed_dir):
        for f in os.listdir(feed_dir):
            try:
                os.remove(os.path.join(feed_dir, f))
            except OSError:
                pass

    episodes = db.get_done_episodes_by_feed(feed_name)
    for ep in episodes:
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
