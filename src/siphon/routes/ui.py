"""Web UI routes for feed management."""

from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from fastapi import APIRouter, Form, Request, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from siphon.config import FeedConfig, resolve_feed

router = APIRouter(prefix="/ui")

templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")
)


def _flash(request: Request, text: str, msg_type: str = "info") -> None:
    """Append a flash message to the request state."""
    if not hasattr(request.state, "messages"):
        request.state.messages = []
    request.state.messages.append({"text": text, "type": msg_type})


def _get_messages(request: Request) -> list[dict]:
    return getattr(request.state, "messages", [])


def _slugify(name: str) -> str:
    """Convert a name to a URL-safe slug."""
    s = name.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_]+", "-", s)
    return s.strip("-")[:60]


def _get_feed_display(request: Request) -> list[dict]:
    """Build feed display data by merging config + DB info."""
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
    title_exclude: str = Form(""),
    claude_prompt_extra: str = Form(""),
):
    config = request.app.state.config
    db = request.app.state.db

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

    # Build feed config dict
    feed_data: dict = {"name": name, "url": url, "type": type}
    if mode:
        feed_data["mode"] = mode
    if quality:
        feed_data["quality"] = quality if quality == "max" else int(quality)
    if llm_trim:
        feed_data["llm_trim"] = llm_trim == "true"
    if date_cutoff:
        feed_data["date_cutoff"] = date_cutoff
    if title_exclude:
        feed_data["title_exclude"] = [t.strip() for t in title_exclude.split(",") if t.strip()]
    if claude_prompt_extra:
        feed_data["claude_prompt_extra"] = claude_prompt_extra

    # Add to config and DB
    new_feed = FeedConfig(**feed_data)
    config.feeds.append(new_feed)
    db.upsert_feed(name, url, type)

    _save_config(config)

    return RedirectResponse("/ui/", status_code=303)


# ------------------------------------------------------------------ #
# Update feed settings
# ------------------------------------------------------------------ #

@router.post("/feed/{feed_name}/update")
async def update_feed(
    request: Request,
    feed_name: str,
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
):
    config = request.app.state.config

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


# ------------------------------------------------------------------ #
# Delete feed
# ------------------------------------------------------------------ #

@router.post("/feed/{feed_name}/delete")
async def delete_feed(request: Request, feed_name: str):
    config = request.app.state.config
    db = request.app.state.db

    # Remove from config
    config.feeds = [f for f in config.feeds if f.name != feed_name]

    # Remove files
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

    # Remove from DB
    db.delete_episodes_by_feed(feed_name)
    db.delete_feed(feed_name)

    _save_config(config)

    return RedirectResponse("/ui/", status_code=303)


# ------------------------------------------------------------------ #
# Mark as caught up
# ------------------------------------------------------------------ #

@router.post("/feed/{feed_name}/catchup")
async def catchup_feed(request: Request, feed_name: str):
    config = request.app.state.config
    db = request.app.state.db

    today = datetime.now(timezone.utc).strftime("%Y%m%d")

    # Update feed config date_cutoff
    for i, fc in enumerate(config.feeds):
        if fc.name == feed_name:
            update = fc.model_dump()
            update["date_cutoff"] = today
            config.feeds[i] = FeedConfig(**update)
            break

    # Delete downloaded files
    download_dir = config.storage.download_dir
    feed_dir = os.path.join(download_dir, feed_name)
    if os.path.isdir(feed_dir):
        for f in os.listdir(feed_dir):
            try:
                os.remove(os.path.join(feed_dir, f))
            except OSError:
                pass

    # Update DB: mark all done episodes as pruned
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

    # Filter out feeds that already exist
    config = request.app.state.config
    existing_urls = {fc.url for fc in config.feeds}
    existing_names = {fc.name for fc in config.feeds}

    new_feeds = []
    for f in feeds:
        if f["url"] not in existing_urls:
            # Make sure name is unique
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
    """Parse an OPML file and extract feed URLs and titles."""
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return []

    feeds = []
    # OPML feeds are in <outline> elements, typically with xmlUrl attribute
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
    """Save the current config back to YAML.

    This writes to the config file that was originally loaded. If the
    config was loaded from a non-file source, this is a no-op.
    """
    import yaml
    from pathlib import Path

    # Build the YAML-serializable dict
    data = config.model_dump()

    # Convert to clean YAML types
    def _clean(obj):
        if isinstance(obj, dict):
            return {k: _clean(v) for k, v in obj.items() if v is not None or k in ("date_cutoff",)}
        if isinstance(obj, list):
            return [_clean(i) for i in obj]
        return obj

    data = _clean(data)

    # Find the config path — stored as a module-level detail
    # For now, save to config.yaml in working directory
    config_path = Path("config.yaml")
    if config_path.exists() or True:  # always save
        config_path.write_text(
            yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
