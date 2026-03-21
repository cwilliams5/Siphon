"""Integration tests — real RSS parsing, real DB, real config round-trips.

These tests verify end-to-end flows without mocking, catching issues like
the CASCADE DELETE bug that unit tests with mocks missed.
"""

from __future__ import annotations

import os

import pytest

from siphon.config import FeedConfig, FeedDefaults, SiphonConfig, load_config, resolve_feed
from siphon.db import Database
from siphon.podcast import parse_podcast_feed


# ------------------------------------------------------------------ #
# Sample podcast RSS for testing
# ------------------------------------------------------------------ #

SAMPLE_PODCAST_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>Test Pod</title>
    <item>
      <title>Episode 1</title>
      <guid>ep-001</guid>
      <pubDate>Wed, 15 Jan 2025 12:00:00 +0000</pubDate>
      <enclosure url="https://cdn.example.com/ep001.mp3" type="audio/mpeg"/>
      <itunes:duration>3600</itunes:duration>
    </item>
    <item>
      <title>Episode 2</title>
      <guid>ep-002</guid>
      <pubDate>Wed, 22 Jan 2025 12:00:00 +0000</pubDate>
      <enclosure url="https://cdn.example.com/ep002.mp3" type="audio/mpeg"/>
      <itunes:duration>1800</itunes:duration>
    </item>
  </channel>
</rss>"""


# ------------------------------------------------------------------ #
# DB persistence across upserts (regression for CASCADE DELETE bug)
# ------------------------------------------------------------------ #

class TestDBPersistence:
    def test_episodes_survive_feed_upsert(self):
        """Episodes must not be deleted when a feed is re-upserted."""
        db = Database(":memory:")
        db.upsert_feed("pod", "http://pod", "podcast")

        # Insert episodes
        for i in range(10):
            db.insert_episode(
                video_id=f"ep-{i:03d}",
                feed_name="pod",
                title=f"Episode {i}",
                status="pending",
            )
        assert len(db.get_episodes_by_feed("pod")) == 10

        # Simulate what happens on config reload / app startup
        db.upsert_feed("pod", "http://pod", "podcast")
        db.upsert_feed("pod", "http://pod-updated", "podcast")

        eps = db.get_episodes_by_feed("pod")
        assert len(eps) == 10
        assert db.get_feed("pod")["url"] == "http://pod-updated"
        db.close()

    def test_episodes_survive_repeated_upserts(self):
        """Simulate many config reloads (page loads)."""
        db = Database(":memory:")
        db.upsert_feed("yt", "http://yt", "youtube")
        db.insert_episode(video_id="v1", feed_name="yt", title="Vid", status="done")
        db.update_episode_status("v1", "yt", "done", file_size=1000)

        # 50 reloads
        for _ in range(50):
            db.upsert_feed("yt", "http://yt", "youtube")

        ep = db.get_episode("v1", "yt")
        assert ep is not None
        assert ep["status"] == "done"
        assert ep["file_size"] == 1000
        db.close()


# ------------------------------------------------------------------ #
# Config round-trip (save and reload)
# ------------------------------------------------------------------ #

class TestConfigRoundTrip:
    def test_save_and_reload(self, tmp_path):
        """Config saved via the UI can be reloaded correctly."""
        import yaml

        config_path = str(tmp_path / "config.yaml")

        # Write a minimal config
        data = {
            "server": {"host": "0.0.0.0", "port": 8585, "base_url": "http://localhost"},
            "auth": {"username": "u", "password": "p"},
            "storage": {"download_dir": str(tmp_path / "media"), "database": ":memory:"}, "youtube": {"api_key": "test"},
            "feeds": [
                {"name": "my-yt", "url": "https://youtube.com/@test", "type": "youtube"},
                {"name": "my-pod", "url": "https://example.com/rss", "type": "podcast",
                 "mode": "audio", "llm_trim": True, "sponsorblock_delay_minutes": 0},
            ],
        }
        with open(config_path, "w") as f:
            yaml.dump(data, f)

        cfg = load_config(config_path)
        assert len(cfg.feeds) == 2
        assert cfg.feeds[0].name == "my-yt"
        assert cfg.feeds[0].type == "youtube"
        assert cfg.feeds[1].name == "my-pod"
        assert cfg.feeds[1].type == "podcast"
        assert cfg.feeds[1].llm_trim is True

        # Simulate UI adding a feed
        cfg.feeds.append(FeedConfig(name="new-feed", url="http://new", type="podcast"))

        # Save back (simulates _save_config)
        save_data = cfg.model_dump()
        def _clean(obj):
            if isinstance(obj, dict):
                return {k: _clean(v) for k, v in obj.items() if v is not None or k in ("date_cutoff",)}
            if isinstance(obj, list):
                return [_clean(i) for i in obj]
            return obj
        save_data = _clean(save_data)
        with open(config_path, "w") as f:
            yaml.dump(save_data, f, default_flow_style=False, sort_keys=False)

        # Reload
        cfg2 = load_config(config_path)
        assert len(cfg2.feeds) == 3
        assert cfg2.feeds[2].name == "new-feed"
        assert cfg2.feeds[2].type == "podcast"


# ------------------------------------------------------------------ #
# Podcast RSS parsing integration
# ------------------------------------------------------------------ #

class TestPodcastParsing:
    def test_parse_and_insert(self):
        """Parse RSS and insert episodes into DB, then verify they persist."""
        db = Database(":memory:")
        db.upsert_feed("test-pod", "http://test", "podcast")

        feed_data = parse_podcast_feed(SAMPLE_PODCAST_RSS)
        assert len(feed_data["episodes"]) == 2

        for ep in feed_data["episodes"]:
            db.insert_episode(
                video_id=ep["guid"],
                feed_name="test-pod",
                title=ep["title"],
                duration=ep["duration"],
                upload_date=ep["pub_date"],
                status="pending",
            )

        eps = db.get_episodes_by_feed("test-pod")
        assert len(eps) == 2
        assert eps[0]["title"] in ("Episode 1", "Episode 2")

        # Upsert feed again — episodes must survive
        db.upsert_feed("test-pod", "http://test", "podcast")
        assert len(db.get_episodes_by_feed("test-pod")) == 2

        db.close()


# ------------------------------------------------------------------ #
# Filter integration
# ------------------------------------------------------------------ #

class TestFilterIntegration:
    def test_date_cutoff_filters_old_episodes(self):
        """Episodes before date_cutoff should be filtered."""
        from siphon.filters import apply_filters

        old_entry = {"id": "old", "title": "Old Ep", "url": "", "duration": 3600, "upload_date": "20230101"}
        new_entry = {"id": "new", "title": "New Ep", "url": "", "duration": 3600, "upload_date": "20250601"}

        assert apply_filters(old_entry, False, [], 0, "20240101") == "too_old"
        assert apply_filters(new_entry, False, [], 0, "20240101") is None

    def test_min_duration_filters_short(self):
        from siphon.filters import apply_filters

        short = {"id": "s", "title": "Short", "url": "", "duration": 30, "upload_date": "20250101"}
        long = {"id": "l", "title": "Long", "url": "", "duration": 3600, "upload_date": "20250101"}

        assert apply_filters(short, False, [], 60, None) == "too_short"
        assert apply_filters(long, False, [], 60, None) is None
