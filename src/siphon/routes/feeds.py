"""RSS feed endpoint."""
from fastapi import APIRouter, Request
from fastapi.responses import Response

from siphon.config import resolve_feed

router = APIRouter()

@router.get("/feed/{feed_name}")
async def get_feed(feed_name: str, request: Request):
    db = request.app.state.db
    config = request.app.state.config

    feed = db.get_feed(feed_name)
    if feed is None:
        return Response(status_code=404, content="Feed not found")

    episodes = db.get_done_episodes_by_feed(feed_name)

    # Get channel_name from first episode if available
    channel_name = None
    if episodes:
        channel_name = episodes[0].get("channel_name")

    # Get display_name from config if set
    display_name = None
    for fc in config.feeds:
        if fc.name == feed_name:
            resolved = resolve_feed(fc, config.defaults)
            display_name = resolved.display_name
            break

    # Get image_url from DB (stored from podcast RSS)
    image_url = feed.get("image_url")

    from siphon.feed import generate_feed_xml
    xml = generate_feed_xml(
        feed_name, episodes, config.server.base_url, channel_name,
        display_name=display_name, image_url=image_url,
    )

    return Response(content=xml, media_type="application/rss+xml; charset=utf-8")
