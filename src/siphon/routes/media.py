"""Media file serving endpoint."""
import os
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, Response

router = APIRouter()

@router.get("/media/{feed_name}/{filename}")
async def get_media(feed_name: str, filename: str, request: Request):
    config = request.app.state.config
    download_dir = config.storage.download_dir

    # Path traversal prevention
    safe_feed = Path(feed_name)
    safe_file = Path(filename)
    if ".." in safe_feed.parts or ".." in safe_file.parts:
        return Response(status_code=400, content="Invalid path")

    file_path = os.path.join(download_dir, feed_name, filename)

    if not os.path.isfile(file_path):
        return Response(status_code=404, content="File not found")

    # Determine media type from extension
    ext = os.path.splitext(filename)[1].lower()
    media_types = {
        ".mp4": "video/mp4",
        ".mp3": "audio/mpeg",
        ".m4a": "audio/mp4",
        ".webm": "video/webm",
    }
    media_type = media_types.get(ext, "application/octet-stream")

    # FileResponse handles range requests (HTTP 206) natively
    return FileResponse(file_path, media_type=media_type)
