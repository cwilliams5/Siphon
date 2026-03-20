"""Tests for siphon.ad_detect — Claude CLI ad detection."""

from __future__ import annotations

import json
from unittest.mock import patch, MagicMock

import pytest

from siphon.ad_detect import detect_ads, filter_segments, resolve_prompt
from siphon.config import FeedConfig, FeedDefaults, LLMConfig, resolve_feed


# ------------------------------------------------------------------ #
# Prompt resolution
# ------------------------------------------------------------------ #

class TestResolvePrompt:
    def test_default_prompt_used(self):
        llm = LLMConfig()
        defaults = FeedDefaults()
        feed_cfg = FeedConfig(name="f", url="http://x")
        feed = resolve_feed(feed_cfg, defaults)
        prompt = resolve_prompt(feed, llm)
        assert "sponsor" in prompt.lower()

    def test_prompt_extra_appended(self):
        llm = LLMConfig()
        defaults = FeedDefaults()
        feed_cfg = FeedConfig(name="f", url="http://x", claude_prompt_extra="Also remove Discord promos.")
        feed = resolve_feed(feed_cfg, defaults)
        prompt = resolve_prompt(feed, llm)
        assert "Discord promos" in prompt
        assert "sponsor" in prompt.lower()  # default is still there

    def test_prompt_override_replaces(self):
        llm = LLMConfig()
        defaults = FeedDefaults()
        feed_cfg = FeedConfig(name="f", url="http://x", claude_prompt_override="Only remove sponsor reads.")
        feed = resolve_feed(feed_cfg, defaults)
        prompt = resolve_prompt(feed, llm)
        assert prompt == "Only remove sponsor reads."


# ------------------------------------------------------------------ #
# Segment filtering
# ------------------------------------------------------------------ #

class TestFilterSegments:
    def test_high_confidence_passes(self):
        segments = [{"start": 0, "end": 30, "label": "ad", "confidence": 0.9}]
        high, marginal = filter_segments(segments, confidence_threshold=0.75)
        assert len(high) == 1
        assert len(marginal) == 0

    def test_marginal_confidence(self):
        segments = [{"start": 0, "end": 30, "label": "ad", "confidence": 0.6}]
        high, marginal = filter_segments(segments, confidence_threshold=0.75)
        assert len(high) == 0
        assert len(marginal) == 1

    def test_below_minimum_confidence_dropped(self):
        segments = [{"start": 0, "end": 30, "label": "ad", "confidence": 0.3}]
        high, marginal = filter_segments(segments, confidence_threshold=0.75)
        assert len(high) == 0
        assert len(marginal) == 0

    def test_too_short_filtered(self):
        segments = [{"start": 0, "end": 3, "label": "ad", "confidence": 0.9}]
        high, marginal = filter_segments(segments, min_duration=7)
        assert len(high) == 0

    def test_too_long_filtered(self):
        segments = [{"start": 0, "end": 600, "label": "ad", "confidence": 0.9}]
        high, marginal = filter_segments(segments, max_duration=300)
        assert len(high) == 0

    def test_mixed_segments(self):
        segments = [
            {"start": 0, "end": 30, "label": "sponsor", "confidence": 0.95},
            {"start": 100, "end": 120, "label": "self-promo", "confidence": 0.6},
            {"start": 200, "end": 202, "label": "tiny", "confidence": 0.99},
            {"start": 300, "end": 310, "label": "low-conf", "confidence": 0.3},
        ]
        high, marginal = filter_segments(
            segments, confidence_threshold=0.75, min_duration=7, max_duration=300
        )
        assert len(high) == 1  # sponsor
        assert high[0]["label"] == "sponsor"
        assert len(marginal) == 1  # self-promo
        assert marginal[0]["label"] == "self-promo"

    def test_empty_segments(self):
        high, marginal = filter_segments([])
        assert high == []
        assert marginal == []


# ------------------------------------------------------------------ #
# detect_ads (mocked CLI)
# ------------------------------------------------------------------ #

class TestDetectAds:
    @patch("siphon.ad_detect.subprocess.run")
    def test_detect_ads_parses_structured_output(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "structured_output": {
                    "segments": [
                        {"start": 10.0, "end": 45.0, "label": "sponsor", "confidence": 0.9}
                    ]
                }
            }),
            stderr="",
        )

        result = detect_ads("some transcript", "detect ads prompt")
        assert len(result["segments"]) == 1
        assert result["segments"][0]["label"] == "sponsor"

    @patch("siphon.ad_detect.subprocess.run")
    def test_detect_ads_handles_direct_output(self, mock_run):
        """If CLI returns the segments dict directly (no envelope)."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({
                "segments": [
                    {"start": 5.0, "end": 35.0, "label": "ad", "confidence": 0.8}
                ]
            }),
            stderr="",
        )

        result = detect_ads("some transcript", "detect ads prompt")
        assert len(result["segments"]) == 1

    @patch("siphon.ad_detect.subprocess.run")
    def test_detect_ads_cli_failure_raises(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Error: model not found",
        )

        with pytest.raises(RuntimeError, match="exit code 1"):
            detect_ads("transcript", "prompt")

    @patch("siphon.ad_detect.subprocess.run")
    def test_detect_ads_bad_json_raises(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="not json",
            stderr="",
        )

        with pytest.raises(RuntimeError, match="not valid JSON"):
            detect_ads("transcript", "prompt")

    @patch("siphon.ad_detect.subprocess.run")
    def test_detect_ads_empty_result(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"structured_output": {"segments": []}}),
            stderr="",
        )

        result = detect_ads("transcript", "prompt")
        assert result["segments"] == []

    @patch("siphon.ad_detect.subprocess.run")
    def test_detect_ads_passes_model_and_effort(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"structured_output": {"segments": []}}),
            stderr="",
        )

        detect_ads("transcript", "prompt", model="claude-haiku-4-5-20251001", effort="low")

        call_args = mock_run.call_args[0][0]
        assert "--model" in call_args
        model_idx = call_args.index("--model")
        assert call_args[model_idx + 1] == "claude-haiku-4-5-20251001"
        assert "--effort" in call_args
        effort_idx = call_args.index("--effort")
        assert call_args[effort_idx + 1] == "low"
