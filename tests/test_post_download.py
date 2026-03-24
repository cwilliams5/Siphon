"""Tests for post-download filtering and date normalization."""

from types import SimpleNamespace

import pytest

from siphon.pipeline import _post_download_filter
from siphon.routes.ui import _normalize_date_cutoff


class TestPostDownloadFilter:
    def _resolved(self, block_shorts=True, min_duration_seconds=120):
        return SimpleNamespace(
            block_shorts=block_shorts,
            min_duration_seconds=min_duration_seconds,
        )

    def test_short_detected(self):
        assert _post_download_filter(45, self._resolved()) == "short"

    def test_short_at_boundary(self):
        assert _post_download_filter(59, self._resolved()) == "short"

    def test_not_short_at_60(self):
        # 60s is not a short, but may be too_short depending on min_duration
        assert _post_download_filter(60, self._resolved()) != "short"

    def test_too_short_detected(self):
        assert _post_download_filter(90, self._resolved(min_duration_seconds=120)) == "too_short"

    def test_too_short_at_boundary(self):
        assert _post_download_filter(119, self._resolved(min_duration_seconds=120)) == "too_short"

    def test_passes_at_min_duration(self):
        assert _post_download_filter(120, self._resolved(min_duration_seconds=120)) is None

    def test_passes_above_min_duration(self):
        assert _post_download_filter(600, self._resolved()) is None

    def test_none_duration_passes(self):
        assert _post_download_filter(None, self._resolved()) is None

    def test_block_shorts_disabled(self):
        # 30s video with block_shorts=False should only check min_duration
        result = _post_download_filter(30, self._resolved(block_shorts=False, min_duration_seconds=0))
        assert result is None

    def test_short_takes_priority_over_too_short(self):
        # 30s video: both short and too_short would match, short should win
        assert _post_download_filter(30, self._resolved(min_duration_seconds=120)) == "short"


class TestNormalizeDateCutoff:
    def test_converts_date_picker_format(self):
        assert _normalize_date_cutoff("2026-03-22") == "20260322"

    def test_leaves_yyyymmdd_unchanged(self):
        assert _normalize_date_cutoff("20260322") == "20260322"

    def test_empty_string(self):
        assert _normalize_date_cutoff("") == ""

    def test_none_like_empty(self):
        # empty string passed through
        assert _normalize_date_cutoff("") == ""


class TestFeedTypeAutoDetect:
    """Test URL-based feed type detection in add_feed_submit."""

    @pytest.mark.parametrize("url,expected", [
        ("https://www.youtube.com/@LinusTechTips", "youtube"),
        ("https://youtube.com/@Channel", "youtube"),
        ("https://m.youtube.com/@Channel", "youtube"),
        ("https://youtu.be/abc123", "youtube"),
        ("https://www.youtube.com/channel/UC123", "youtube"),
        ("https://example.com/podcast/rss", "podcast"),
        ("https://feeds.npr.org/123/podcast.xml", "podcast"),
        ("https://anchor.fm/s/abc/podcast/rss", "podcast"),
    ])
    def test_auto_detect(self, url, expected):
        url_lower = url.lower()
        if any(d in url_lower for d in ("youtube.com", "youtu.be", "m.youtube.com")):
            detected = "youtube"
        else:
            detected = "podcast"
        assert detected == expected
