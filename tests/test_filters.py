"""Tests for siphon.filters — pure filtering functions."""

from __future__ import annotations

import pytest

from siphon.filters import (
    apply_filters,
    is_short,
    title_excluded,
    too_old,
    too_short,
)


# ── helpers ──────────────────────────────────────────────────────────────

def _entry(
    *,
    video_id: str = "abc123",
    title: str = "Regular Video",
    url: str = "https://www.youtube.com/watch?v=abc123",
    duration: int | None = 600,
    upload_date: str | None = "20250301",
) -> dict:
    return {
        "id": video_id,
        "title": title,
        "url": url,
        "duration": duration,
        "upload_date": upload_date,
    }


# ── is_short ─────────────────────────────────────────────────────────────

class TestIsShort:
    def test_shorts_url(self):
        entry = _entry(url="https://www.youtube.com/shorts/xyz789")
        assert is_short(entry) == "short"

    def test_short_duration(self):
        entry = _entry(duration=30)
        assert is_short(entry) == "short"

    def test_duration_exactly_60_passes(self):
        entry = _entry(duration=60)
        assert is_short(entry) is None

    def test_normal_video_passes(self):
        entry = _entry(duration=300, url="https://www.youtube.com/watch?v=abc")
        assert is_short(entry) is None

    def test_none_duration_passes(self):
        entry = _entry(duration=None, url="https://www.youtube.com/watch?v=abc")
        assert is_short(entry) is None


# ── title_excluded ───────────────────────────────────────────────────────

class TestTitleExcluded:
    def test_match_case_insensitive(self):
        assert title_excluded("My LIVESTREAM today", ["livestream"]) == "title_match:livestream"

    def test_match_substring(self):
        assert title_excluded("Compilation of Fails #shorts", ["#shorts"]) == "title_match:#shorts"

    def test_multiple_patterns_first_match_wins(self):
        result = title_excluded("Live Q&A Stream", ["live", "stream"])
        assert result == "title_match:live"

    def test_no_match(self):
        assert title_excluded("How to bake bread", ["livestream", "#shorts"]) is None

    def test_empty_patterns(self):
        assert title_excluded("Any title", []) is None


# ── too_short ────────────────────────────────────────────────────────────

class TestTooShort:
    def test_none_duration_deferred(self):
        assert too_short(None, 120) is None

    def test_below_minimum(self):
        assert too_short(30, 60) == "too_short"

    def test_exactly_minimum_passes(self):
        assert too_short(60, 60) is None

    def test_above_minimum_passes(self):
        assert too_short(600, 60) is None


# ── too_old ──────────────────────────────────────────────────────────────

class TestTooOld:
    def test_before_cutoff(self):
        assert too_old("20240101", "20240601") == "too_old"

    def test_on_cutoff_passes(self):
        assert too_old("20240601", "20240601") is None

    def test_after_cutoff_passes(self):
        assert too_old("20250101", "20240601") is None

    def test_none_upload_date_passes(self):
        assert too_old(None, "20240601") is None

    def test_none_cutoff_passes(self):
        assert too_old("20240101", None) is None

    def test_both_none_passes(self):
        assert too_old(None, None) is None


# ── apply_filters ────────────────────────────────────────────────────────

class TestApplyFilters:
    def test_all_pass(self):
        entry = _entry(duration=600, upload_date="20250301")
        result = apply_filters(
            entry,
            block_shorts=True,
            title_exclude=[],
            min_duration_seconds=60,
            date_cutoff="20240101",
        )
        assert result is None

    def test_blocked_as_short(self):
        entry = _entry(url="https://www.youtube.com/shorts/xyz", duration=45)
        result = apply_filters(
            entry,
            block_shorts=True,
            title_exclude=[],
            min_duration_seconds=60,
            date_cutoff=None,
        )
        assert result == "short"

    def test_shorts_not_blocked_when_disabled(self):
        entry = _entry(url="https://www.youtube.com/shorts/xyz", duration=45)
        result = apply_filters(
            entry,
            block_shorts=False,
            title_exclude=[],
            min_duration_seconds=10,
            date_cutoff=None,
        )
        assert result is None

    def test_title_exclusion(self):
        entry = _entry(title="Weekly Livestream Q&A")
        result = apply_filters(
            entry,
            block_shorts=True,
            title_exclude=["livestream"],
            min_duration_seconds=60,
            date_cutoff=None,
        )
        assert result == "title_match:livestream"

    def test_too_short_filter(self):
        entry = _entry(duration=30)
        # block_shorts=False so is_short doesn't fire first
        result = apply_filters(
            entry,
            block_shorts=False,
            title_exclude=[],
            min_duration_seconds=60,
            date_cutoff=None,
        )
        assert result == "too_short"

    def test_too_old_filter(self):
        entry = _entry(upload_date="20230101")
        result = apply_filters(
            entry,
            block_shorts=False,
            title_exclude=[],
            min_duration_seconds=60,
            date_cutoff="20240101",
        )
        assert result == "too_old"

    def test_first_failing_filter_wins(self):
        """Shorts filter fires before title or duration filters."""
        entry = _entry(
            url="https://www.youtube.com/shorts/xyz",
            title="Livestream clip",
            duration=10,
            upload_date="20200101",
        )
        result = apply_filters(
            entry,
            block_shorts=True,
            title_exclude=["livestream"],
            min_duration_seconds=60,
            date_cutoff="20240101",
        )
        assert result == "short"

    def test_no_cutoff_skips_date_check(self):
        entry = _entry(upload_date="20200101")
        result = apply_filters(
            entry,
            block_shorts=False,
            title_exclude=[],
            min_duration_seconds=60,
            date_cutoff=None,
        )
        assert result is None
