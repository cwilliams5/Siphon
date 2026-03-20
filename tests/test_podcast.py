"""Tests for siphon.podcast — RSS parsing, audio downloading, GUID handling."""

from __future__ import annotations

import os
from unittest.mock import patch, MagicMock

import pytest

from siphon.podcast import (
    parse_podcast_feed,
    episode_filename,
    _parse_duration,
    _parse_rfc2822_date,
)


# ------------------------------------------------------------------ #
# Sample RSS XML
# ------------------------------------------------------------------ #

SAMPLE_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
  <channel>
    <title>Test Podcast</title>
    <description>A test podcast for unit tests</description>
    <itunes:image href="https://example.com/cover.jpg"/>
    <item>
      <title>Episode 1: Hello World</title>
      <description>The first episode</description>
      <guid>ep-001</guid>
      <pubDate>Wed, 15 Jan 2025 12:00:00 +0000</pubDate>
      <enclosure url="https://cdn.example.com/ep001.mp3" type="audio/mpeg" length="5000000"/>
      <itunes:duration>01:30:00</itunes:duration>
      <itunes:image href="https://example.com/ep001.jpg"/>
    </item>
    <item>
      <title>Episode 2: Deep Dive</title>
      <description>A deep dive episode</description>
      <guid>ep-002</guid>
      <pubDate>Wed, 22 Jan 2025 12:00:00 +0000</pubDate>
      <enclosure url="https://cdn.example.com/ep002.mp3" type="audio/mpeg" length="8000000"/>
      <itunes:duration>3600</itunes:duration>
    </item>
    <item>
      <title>Video Bonus</title>
      <guid>ep-003</guid>
      <enclosure url="https://cdn.example.com/bonus.mp4" type="video/mp4" length="50000000"/>
    </item>
    <item>
      <title>No Enclosure</title>
      <guid>ep-004</guid>
    </item>
  </channel>
</rss>"""


MINIMAL_RSS = b"""<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <title>Minimal</title>
    <item>
      <enclosure url="https://example.com/ep.mp3" type="audio/mpeg"/>
    </item>
  </channel>
</rss>"""


# ------------------------------------------------------------------ #
# parse_podcast_feed
# ------------------------------------------------------------------ #

class TestParsePodcastFeed:
    def test_basic_parsing(self):
        result = parse_podcast_feed(SAMPLE_RSS)
        assert result["title"] == "Test Podcast"
        assert result["description"] == "A test podcast for unit tests"
        assert result["image_url"] == "https://example.com/cover.jpg"

    def test_episode_count(self):
        """Only audio enclosures are included (video is skipped, no-enclosure is skipped)."""
        result = parse_podcast_feed(SAMPLE_RSS)
        assert len(result["episodes"]) == 2

    def test_episode_fields(self):
        result = parse_podcast_feed(SAMPLE_RSS)
        ep = result["episodes"][0]
        assert ep["guid"] == "ep-001"
        assert ep["title"] == "Episode 1: Hello World"
        assert ep["description"] == "The first episode"
        assert ep["audio_url"] == "https://cdn.example.com/ep001.mp3"
        assert ep["pub_date"] == "20250115"
        assert ep["duration"] == 5400  # 1:30:00
        assert ep["thumbnail_url"] == "https://example.com/ep001.jpg"

    def test_duration_as_seconds_string(self):
        result = parse_podcast_feed(SAMPLE_RSS)
        ep2 = result["episodes"][1]
        assert ep2["duration"] == 3600

    def test_fallback_thumbnail(self):
        """Episode without its own image falls back to feed-level image."""
        result = parse_podcast_feed(SAMPLE_RSS)
        ep2 = result["episodes"][1]
        assert ep2["thumbnail_url"] == "https://example.com/cover.jpg"

    def test_minimal_feed(self):
        result = parse_podcast_feed(MINIMAL_RSS)
        assert result["title"] == "Minimal"
        assert len(result["episodes"]) == 1
        # GUID is auto-generated from URL hash
        assert len(result["episodes"][0]["guid"]) > 0

    def test_no_channel_raises(self):
        with pytest.raises(ValueError, match="No <channel>"):
            parse_podcast_feed(b"<rss></rss>")


# ------------------------------------------------------------------ #
# Duration parsing
# ------------------------------------------------------------------ #

class TestParseDuration:
    def test_seconds(self):
        assert _parse_duration("3600") == 3600

    def test_mm_ss(self):
        assert _parse_duration("60:00") == 3600

    def test_hh_mm_ss(self):
        assert _parse_duration("01:30:00") == 5400

    def test_invalid(self):
        assert _parse_duration("not-a-duration") is None


# ------------------------------------------------------------------ #
# Date parsing
# ------------------------------------------------------------------ #

class TestParseDate:
    def test_rfc2822(self):
        assert _parse_rfc2822_date("Wed, 15 Jan 2025 12:00:00 +0000") == "20250115"

    def test_invalid_returns_none(self):
        assert _parse_rfc2822_date("not a date") is None


# ------------------------------------------------------------------ #
# Episode filename
# ------------------------------------------------------------------ #

class TestEpisodeFilename:
    def test_basic(self):
        name = episode_filename("ep-001", "https://cdn.example.com/ep001.mp3")
        assert name == "ep-001.mp3"

    def test_no_extension_defaults_mp3(self):
        name = episode_filename("ep-001", "https://cdn.example.com/stream")
        assert name == "ep-001.mp3"

    def test_m4a_extension(self):
        name = episode_filename("ep-001", "https://cdn.example.com/ep001.m4a")
        assert name == "ep-001.m4a"

    def test_special_chars_in_guid(self):
        name = episode_filename("https://example.com/ep/001", "https://cdn/ep.mp3")
        assert ".mp3" in name
        assert "/" not in name  # No slashes in filename
