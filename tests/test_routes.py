"""Tests for HTTP routes."""
import os
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from siphon.app import create_app
from siphon.config import (
    YouTubeConfig,
    AuthConfig,
    CookiesConfig,
    FeedConfig,
    FeedDefaults,
    ScheduleConfig,
    ServerConfig,
    SiphonConfig,
    StorageConfig,
)


@pytest.fixture
def config(tmp_path):
    return SiphonConfig(
        server=ServerConfig(host="127.0.0.1", port=8585, base_url="https://test.example.com"),
        auth=AuthConfig(username="testuser", password="testpass"),
        youtube=YouTubeConfig(api_key="test-key"),
        storage=StorageConfig(
            download_dir=str(tmp_path / "media"),
            database=str(tmp_path / "test.db"),
            max_disk_gb=10,
            youtube_keep_per_feed=5,
            podcast_keep_per_feed=20,
        ),
        schedule=ScheduleConfig(check_interval_minutes=30),
        cookies=CookiesConfig(),
        defaults=FeedDefaults(sponsorblock_delay_minutes=0),
        feeds=[
            FeedConfig(name="test-feed", url="https://www.youtube.com/@TestChannel"),
        ],
    )


@pytest.fixture
def auth_headers():
    import base64
    credentials = base64.b64encode(b"testuser:testpass").decode()
    return {"Authorization": f"Basic {credentials}"}


@pytest.fixture
def bad_auth_headers():
    import base64
    credentials = base64.b64encode(b"wrong:wrong").decode()
    return {"Authorization": f"Basic {credentials}"}


@pytest.fixture
async def client(config):
    # Patch APScheduler so no real scheduler starts during tests.
    with patch.dict("sys.modules", {"apscheduler": None, "apscheduler.schedulers": None, "apscheduler.schedulers.asyncio": None}):
        app = create_app(config)
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
                yield c


class TestAuth:
    async def test_unauthorized_without_credentials(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 401

    async def test_unauthorized_with_bad_credentials(self, client, bad_auth_headers):
        resp = await client.get("/health", headers=bad_auth_headers)
        assert resp.status_code == 401

    async def test_authorized_with_correct_credentials(self, client, auth_headers):
        resp = await client.get("/health", headers=auth_headers)
        assert resp.status_code == 200


class TestHealth:
    async def test_health_returns_json(self, client, auth_headers):
        resp = await client.get("/health", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "feeds" in data
        assert "disk_usage_bytes" in data
        assert "disk_usage_gb" in data


class TestFeedEndpoint:
    async def test_feed_not_found(self, client, auth_headers):
        resp = await client.get("/feed/nonexistent", headers=auth_headers)
        assert resp.status_code == 404

    async def test_feed_returns_rss(self, client, auth_headers):
        resp = await client.get("/feed/test-feed", headers=auth_headers)
        assert resp.status_code == 200
        assert "application/rss+xml" in resp.headers["content-type"]
        assert "<?xml" in resp.text

    async def test_feed_with_episodes(self, client, auth_headers, config):
        app = client._transport.app
        db = app.state.db
        db.insert_episode(
            video_id="test123",
            feed_name="test-feed",
            title="Test Episode",
            description="A test episode",
            thumbnail_url="https://example.com/thumb.jpg",
            channel_name="Test Channel",
            duration=300,
            upload_date="20240115",
            eligible_at="2024-01-15T00:00:00",
            status="done",
        )
        db.update_episode_status("test123", "test-feed", "done",
            file_path="/media/test-feed/test123.mp4",
            file_size=1000000,
            mime_type="video/mp4")

        resp = await client.get("/feed/test-feed", headers=auth_headers)
        assert resp.status_code == 200
        assert "test123" in resp.text
        assert "Test Episode" in resp.text


class TestMediaEndpoint:
    async def test_media_not_found(self, client, auth_headers):
        resp = await client.get("/media/test-feed/nonexistent.mp4", headers=auth_headers)
        assert resp.status_code == 404

    async def test_media_path_traversal_blocked(self, client, auth_headers):
        resp = await client.get("/media/../etc/passwd", headers=auth_headers)
        assert resp.status_code in (400, 404, 422)

    async def test_media_serves_file(self, client, auth_headers, config):
        media_dir = os.path.join(config.storage.download_dir, "test-feed")
        os.makedirs(media_dir, exist_ok=True)
        test_file = os.path.join(media_dir, "testvid.mp4")
        with open(test_file, "wb") as f:
            f.write(b"fake video content")

        resp = await client.get("/media/test-feed/testvid.mp4", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.content == b"fake video content"


class TestRefresh:
    async def test_refresh_returns_202(self, client, auth_headers):
        mock_check = AsyncMock()
        with patch("siphon.pipeline.check_feeds", mock_check):
            resp = await client.post("/refresh", headers=auth_headers)
            assert resp.status_code == 202
            data = resp.json()
            assert data["status"] == "accepted"
