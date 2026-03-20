"""RSS feed endpoint."""
from fastapi import APIRouter, Request
from fastapi.responses import Response

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

    from siphon.feed import generate_feed_xml
    xml = generate_feed_xml(feed_name, episodes, config.server.base_url, channel_name)

    return Response(content=xml, media_type="application/rss+xml; charset=utf-8")
