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

    all_done = db.get_done_episodes_by_feed(feed_name)

    # Get feed config
    display_name = None
    sponsorblock_active = False
    llm_trim_active = False
    for fc in config.feeds:
        if fc.name == feed_name:
            resolved = resolve_feed(fc, config.defaults)
            display_name = resolved.display_name
            sponsorblock_active = resolved.sponsorblock
            llm_trim_active = resolved.llm_trim
            break

    # Filter episodes: hide until LLM done if llm_trim is enabled
    if llm_trim_active:
        episodes = [ep for ep in all_done if ep.get("llm_trim_status") == "done"]
    else:
        episodes = all_done

    # Get channel_name from first episode if available
    channel_name = None
    if episodes:
        channel_name = episodes[0].get("channel_name")

    # Get image_url from DB (stored from podcast RSS)
    image_url = feed.get("image_url")

    # Use media_base_url for enclosure URLs if configured
    media_base_url = config.server.media_base_url or None

    from siphon.feed import generate_feed_xml
    xml = generate_feed_xml(
        feed_name, episodes, config.server.base_url, channel_name,
        display_name=display_name, image_url=image_url,
        media_base_url=media_base_url,
        sponsorblock_active=sponsorblock_active,
    )

    return Response(content=xml, media_type="application/rss+xml; charset=utf-8")
