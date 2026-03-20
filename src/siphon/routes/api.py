"""Health and management API endpoints."""
import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter()

@router.get("/health")
async def health(request: Request):
    db = request.app.state.db
    config = request.app.state.config

    feeds = db.get_all_feeds()
    disk_usage = db.get_disk_usage()

    scheduler = getattr(request.app.state, "scheduler", None)
    scheduler_state = "unknown"
    if scheduler is not None:
        scheduler_state = "paused" if getattr(scheduler, "_paused", False) else "running"

    feed_status = []
    for feed in feeds:
        episodes = db.get_episodes_by_feed(feed["name"])
        status_counts = {}
        for ep in episodes:
            s = ep["status"]
            status_counts[s] = status_counts.get(s, 0) + 1
        feed_status.append({
            "name": feed["name"],
            "feed_type": feed.get("feed_type", "youtube"),
            "last_checked_at": feed.get("last_checked_at"),
            "last_error": feed.get("last_error"),
            "episodes": status_counts,
        })

    return JSONResponse({
        "status": "ok",
        "scheduler": scheduler_state,
        "feeds": feed_status,
        "disk_usage_bytes": disk_usage,
        "disk_usage_gb": round(disk_usage / (1024**3), 2),
        "max_disk_gb": config.storage.max_disk_gb,
    })

@router.post("/refresh")
async def refresh(request: Request):
    from siphon.pipeline import check_feeds
    config = request.app.state.config
    db = request.app.state.db

    asyncio.create_task(check_feeds(config, db))

    return JSONResponse({"status": "accepted"}, status_code=202)

@router.get("/test-cookies")
async def test_cookies(request: Request):
    from siphon.downloader import test_youtube_cookies
    config = request.app.state.config

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, test_youtube_cookies, config.cookies)

    return JSONResponse(result)

@router.post("/scheduler/pause")
async def pause_scheduler(request: Request):
    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler is None:
        return JSONResponse({"error": "No scheduler"}, status_code=503)
    scheduler.pause()
    return JSONResponse({"status": "paused"})

@router.post("/scheduler/resume")
async def resume_scheduler(request: Request):
    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler is None:
        return JSONResponse({"error": "No scheduler"}, status_code=503)
    scheduler.resume()
    return JSONResponse({"status": "running"})
