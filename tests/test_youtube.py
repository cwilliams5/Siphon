"""Tests for YouTube API integration."""

from unittest.mock import patch, MagicMock

import pytest

from siphon.youtube import _parse_iso8601_duration, _enrich_durations


class TestParseISO8601Duration:
    def test_minutes_and_seconds(self):
        assert _parse_iso8601_duration("PT4M13S") == 253

    def test_hours_minutes_seconds(self):
        assert _parse_iso8601_duration("PT1H2M3S") == 3723

    def test_hours_only(self):
        assert _parse_iso8601_duration("PT1H") == 3600

    def test_minutes_only(self):
        assert _parse_iso8601_duration("PT30M") == 1800

    def test_seconds_only(self):
        assert _parse_iso8601_duration("PT45S") == 45

    def test_zero_duration(self):
        assert _parse_iso8601_duration("PT0S") == 0

    def test_empty_string(self):
        assert _parse_iso8601_duration("") == 0

    def test_invalid_format(self):
        assert _parse_iso8601_duration("not a duration") == 0

    def test_short_video(self):
        # YouTube Shorts are typically < 60s
        assert _parse_iso8601_duration("PT58S") == 58

    def test_long_podcast(self):
        assert _parse_iso8601_duration("PT3H45M12S") == 13512


class TestEnrichDurations:
    @patch("siphon.youtube._api_get")
    def test_updates_video_durations(self, mock_api):
        mock_api.return_value = {
            "items": [
                {"id": "vid1", "contentDetails": {"duration": "PT10M30S"}},
                {"id": "vid2", "contentDetails": {"duration": "PT5M"}},
            ]
        }
        videos = [
            {"id": "vid1", "duration": None},
            {"id": "vid2", "duration": None},
        ]
        _enrich_durations(videos, "fake-key", 4)
        assert videos[0]["duration"] == 630
        assert videos[1]["duration"] == 300

    @patch("siphon.youtube._api_get")
    def test_missing_video_left_as_none(self, mock_api):
        mock_api.return_value = {
            "items": [
                {"id": "vid1", "contentDetails": {"duration": "PT10M"}},
            ]
        }
        videos = [
            {"id": "vid1", "duration": None},
            {"id": "vid2", "duration": None},
        ]
        _enrich_durations(videos, "fake-key", 4)
        assert videos[0]["duration"] == 600
        assert videos[1]["duration"] is None

    @patch("siphon.youtube._api_get")
    def test_api_failure_leaves_durations_unchanged(self, mock_api):
        mock_api.side_effect = Exception("quota exceeded")
        videos = [{"id": "vid1", "duration": None}]
        _enrich_durations(videos, "fake-key", 4)
        assert videos[0]["duration"] is None

    @patch("siphon.youtube._api_get")
    def test_batches_over_50(self, mock_api):
        mock_api.return_value = {"items": []}
        videos = [{"id": f"vid{i}", "duration": None} for i in range(75)]
        _enrich_durations(videos, "fake-key", 4)
        # Should make 2 API calls (50 + 25)
        assert mock_api.call_count == 2
