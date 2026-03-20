"""Tests for siphon.feed – RSS feed generation."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import pytest
from freezegun import freeze_time

from siphon.feed import (
    format_duration,
    format_pubdate,
    generate_feed_xml,
    get_file_extension,
    ITUNES_NS,
)


# ------------------------------------------------------------------ #
# Sample data
# ------------------------------------------------------------------ #

def _make_episode(**overrides) -> dict:
    defaults = dict(
        video_id="abc123",
        feed_name="tech",
        title="Episode One",
        description="A great episode",
        thumbnail_url="https://img.example.com/thumb1.jpg",
        channel_name="TechChannel",
        duration=3661,
        upload_date="20240115",
        file_path="/media/tech/abc123.mp4",
        file_size=104857600,
        mime_type="video/mp4",
    )
    defaults.update(overrides)
    return defaults


def _sample_episodes() -> list[dict]:
    return [
        _make_episode(
            video_id="vid1",
            title="Newest Episode",
            upload_date="20240215",
            duration=5400,
            file_size=200000000,
        ),
        _make_episode(
            video_id="vid2",
            title="Older Episode",
            upload_date="20240115",
            duration=3600,
            file_size=150000000,
        ),
    ]


# ------------------------------------------------------------------ #
# format_duration
# ------------------------------------------------------------------ #

class TestFormatDuration:
    def test_none(self):
        assert format_duration(None) == "00:00:00"

    def test_zero(self):
        assert format_duration(0) == "00:00:00"

    def test_one_minute(self):
        assert format_duration(60) == "00:01:00"

    def test_one_hour(self):
        assert format_duration(3600) == "01:00:00"

    def test_ninety_minutes(self):
        assert format_duration(5400) == "01:30:00"

    def test_mixed(self):
        assert format_duration(3661) == "01:01:01"

    def test_large_value(self):
        assert format_duration(36000) == "10:00:00"


# ------------------------------------------------------------------ #
# format_pubdate
# ------------------------------------------------------------------ #

class TestFormatPubdate:
    def test_valid_date(self):
        result = format_pubdate("20240115")
        # Should be RFC 2822 format
        assert "15 Jan 2024" in result
        assert "+0000" in result or "GMT" in result

    def test_another_date(self):
        result = format_pubdate("20231225")
        assert "25 Dec 2023" in result

    @freeze_time("2025-06-01 12:00:00")
    def test_none_uses_current_time(self):
        result = format_pubdate(None)
        assert "01 Jun 2025" in result

    @freeze_time("2025-06-01 12:00:00")
    def test_invalid_date_uses_current_time(self):
        result = format_pubdate("not-a-date")
        assert "01 Jun 2025" in result


# ------------------------------------------------------------------ #
# get_file_extension
# ------------------------------------------------------------------ #

class TestGetFileExtension:
    def test_video_mp4(self):
        assert get_file_extension("video/mp4") == "mp4"

    def test_audio_mpeg(self):
        assert get_file_extension("audio/mpeg") == "mp3"

    def test_unknown_defaults_mp4(self):
        assert get_file_extension("application/octet-stream") == "mp4"

    def test_audio_mp4(self):
        assert get_file_extension("audio/mp4") == "m4a"


# ------------------------------------------------------------------ #
# generate_feed_xml – full integration
# ------------------------------------------------------------------ #

class TestGenerateFeedXml:
    def test_produces_valid_xml(self):
        episodes = _sample_episodes()
        xml_str = generate_feed_xml("tech", episodes, "http://localhost:8000")
        # Should not raise
        root = ET.fromstring(xml_str)
        assert root is not None

    def test_xml_declaration(self):
        xml_str = generate_feed_xml("tech", _sample_episodes(), "http://localhost:8000")
        assert xml_str.startswith("<?xml")
        assert "encoding='utf-8'" in xml_str or 'encoding="utf-8"' in xml_str

    def test_root_is_rss_v2(self):
        xml_str = generate_feed_xml("tech", _sample_episodes(), "http://localhost:8000")
        root = ET.fromstring(xml_str)
        assert root.tag == "rss"
        assert root.attrib["version"] == "2.0"

    def test_channel_metadata(self):
        xml_str = generate_feed_xml(
            "tech", _sample_episodes(), "http://localhost:8000", channel_name="My Tech Show"
        )
        root = ET.fromstring(xml_str)
        ch = root.find("channel")
        assert ch is not None
        assert ch.findtext("title") == "My Tech Show"
        assert ch.findtext("link") == "http://localhost:8000"
        assert ch.findtext("description") == "Siphon feed: tech"
        assert ch.findtext("language") == "en"

    def test_channel_itunes_elements(self):
        xml_str = generate_feed_xml(
            "tech", _sample_episodes(), "http://localhost:8000", channel_name="My Show"
        )
        root = ET.fromstring(xml_str)
        ch = root.find("channel")
        assert ch.findtext(f"{{{ITUNES_NS}}}author") == "My Show"
        assert ch.findtext(f"{{{ITUNES_NS}}}explicit") == "false"

    def test_channel_image_from_first_episode(self):
        episodes = _sample_episodes()
        xml_str = generate_feed_xml("tech", episodes, "http://localhost:8000")
        root = ET.fromstring(xml_str)
        ch = root.find("channel")
        img = ch.find(f"{{{ITUNES_NS}}}image")
        assert img is not None
        assert img.attrib["href"] == episodes[0]["thumbnail_url"]

    def test_correct_item_count(self):
        episodes = _sample_episodes()
        xml_str = generate_feed_xml("tech", episodes, "http://localhost:8000")
        root = ET.fromstring(xml_str)
        items = root.findall("channel/item")
        assert len(items) == 2

    def test_item_guid(self):
        episodes = _sample_episodes()
        xml_str = generate_feed_xml("tech", episodes, "http://localhost:8000")
        root = ET.fromstring(xml_str)
        items = root.findall("channel/item")
        guid = items[0].find("guid")
        assert guid.text == "vid1"
        assert guid.attrib["isPermaLink"] == "false"

    def test_item_title(self):
        episodes = _sample_episodes()
        xml_str = generate_feed_xml("tech", episodes, "http://localhost:8000")
        root = ET.fromstring(xml_str)
        items = root.findall("channel/item")
        assert items[0].findtext("title") == "Newest Episode"
        assert items[1].findtext("title") == "Older Episode"

    def test_item_enclosure(self):
        episodes = _sample_episodes()
        xml_str = generate_feed_xml("tech", episodes, "http://localhost:8000")
        root = ET.fromstring(xml_str)
        items = root.findall("channel/item")
        enc = items[0].find("enclosure")
        assert enc is not None
        assert enc.attrib["url"] == "http://localhost:8000/media/tech/vid1.mp4"
        assert enc.attrib["length"] == "200000000"
        assert enc.attrib["type"] == "video/mp4"

    def test_item_enclosure_mp3(self):
        episodes = [_make_episode(mime_type="audio/mpeg")]
        xml_str = generate_feed_xml("tech", episodes, "http://localhost:8000")
        root = ET.fromstring(xml_str)
        enc = root.find("channel/item/enclosure")
        assert enc.attrib["url"].endswith(".mp3")
        assert enc.attrib["type"] == "audio/mpeg"

    def test_item_pubdate_rfc2822(self):
        episodes = _sample_episodes()
        xml_str = generate_feed_xml("tech", episodes, "http://localhost:8000")
        root = ET.fromstring(xml_str)
        items = root.findall("channel/item")
        pubdate = items[0].findtext("pubDate")
        assert "15 Feb 2024" in pubdate

    def test_item_itunes_duration(self):
        episodes = _sample_episodes()
        xml_str = generate_feed_xml("tech", episodes, "http://localhost:8000")
        root = ET.fromstring(xml_str)
        items = root.findall("channel/item")
        dur = items[0].findtext(f"{{{ITUNES_NS}}}duration")
        assert dur == "01:30:00"  # 5400 seconds
        dur2 = items[1].findtext(f"{{{ITUNES_NS}}}duration")
        assert dur2 == "01:00:00"  # 3600 seconds

    def test_item_itunes_image(self):
        episodes = _sample_episodes()
        xml_str = generate_feed_xml("tech", episodes, "http://localhost:8000")
        root = ET.fromstring(xml_str)
        item = root.findall("channel/item")[0]
        img = item.find(f"{{{ITUNES_NS}}}image")
        assert img is not None
        assert img.attrib["href"] == episodes[0]["thumbnail_url"]

    def test_item_itunes_explicit(self):
        episodes = _sample_episodes()
        xml_str = generate_feed_xml("tech", episodes, "http://localhost:8000")
        root = ET.fromstring(xml_str)
        item = root.findall("channel/item")[0]
        assert item.findtext(f"{{{ITUNES_NS}}}explicit") == "false"


# ------------------------------------------------------------------ #
# Empty episode list
# ------------------------------------------------------------------ #

class TestEmptyFeed:
    def test_empty_produces_valid_xml(self):
        xml_str = generate_feed_xml("tech", [], "http://localhost:8000")
        root = ET.fromstring(xml_str)
        assert root.tag == "rss"

    def test_empty_has_no_items(self):
        xml_str = generate_feed_xml("tech", [], "http://localhost:8000")
        root = ET.fromstring(xml_str)
        items = root.findall("channel/item")
        assert len(items) == 0

    def test_empty_channel_uses_feed_name(self):
        xml_str = generate_feed_xml("tech", [], "http://localhost:8000")
        root = ET.fromstring(xml_str)
        ch = root.find("channel")
        assert ch.findtext("title") == "tech"

    def test_empty_no_channel_image(self):
        xml_str = generate_feed_xml("tech", [], "http://localhost:8000")
        root = ET.fromstring(xml_str)
        ch = root.find("channel")
        img = ch.find(f"{{{ITUNES_NS}}}image")
        assert img is None


# ------------------------------------------------------------------ #
# Channel name resolution
# ------------------------------------------------------------------ #

class TestChannelNameResolution:
    def test_explicit_channel_name(self):
        xml_str = generate_feed_xml(
            "tech", _sample_episodes(), "http://localhost:8000",
            channel_name="Custom Name",
        )
        root = ET.fromstring(xml_str)
        ch = root.find("channel")
        assert ch.findtext("title") == "Custom Name"
        assert ch.findtext(f"{{{ITUNES_NS}}}author") == "Custom Name"

    def test_channel_name_from_first_episode(self):
        """When channel_name param is None, use the first episode's channel_name."""
        episodes = _sample_episodes()
        xml_str = generate_feed_xml("tech", episodes, "http://localhost:8000")
        root = ET.fromstring(xml_str)
        ch = root.find("channel")
        # First episode has channel_name="TechChannel"
        assert ch.findtext("title") == "TechChannel"

    def test_channel_name_falls_back_to_feed_name(self):
        """If first episode has no channel_name, fall back to feed_name."""
        episodes = [_make_episode(channel_name=None)]
        xml_str = generate_feed_xml("myfeed", episodes, "http://localhost:8000")
        root = ET.fromstring(xml_str)
        ch = root.find("channel")
        assert ch.findtext("title") == "myfeed"
