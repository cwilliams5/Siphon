"""Tests for SponsorBlock API integration."""

import json
from unittest.mock import patch, MagicMock

import pytest

from siphon.sponsorblock import get_segment_info, get_segment_count


def _mock_response(status_code, segments):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = segments
    return resp


class TestGetSegmentInfo:
    @patch("siphon.sponsorblock.httpx.get")
    def test_returns_count_and_seconds(self, mock_get):
        mock_get.return_value = _mock_response(200, [
            {"segment": [10.0, 40.0], "category": "sponsor"},
            {"segment": [100.0, 115.5], "category": "outro"},
        ])
        count, secs = get_segment_info("vid123")
        assert count == 2
        assert secs == 45.5

    @patch("siphon.sponsorblock.httpx.get")
    def test_categories_sent_as_json(self, mock_get):
        mock_get.return_value = _mock_response(200, [])
        get_segment_info("vid123", ["sponsor", "selfpromo"])
        call_args = mock_get.call_args
        params = call_args.kwargs.get("params") or call_args[1].get("params")
        # Must be proper JSON, not Python str() representation
        parsed = json.loads(params["categories"])
        assert parsed == ["sponsor", "selfpromo"]

    @patch("siphon.sponsorblock.httpx.get")
    def test_no_categories_omits_param(self, mock_get):
        mock_get.return_value = _mock_response(200, [])
        get_segment_info("vid123")
        call_args = mock_get.call_args
        params = call_args.kwargs.get("params") or call_args[1].get("params")
        assert "categories" not in params

    @patch("siphon.sponsorblock.httpx.get")
    def test_404_returns_zeros(self, mock_get):
        mock_get.return_value = _mock_response(404, None)
        count, secs = get_segment_info("vid_no_segments")
        assert count == 0
        assert secs == 0.0

    @patch("siphon.sponsorblock.httpx.get")
    def test_network_error_returns_zeros(self, mock_get):
        mock_get.side_effect = Exception("connection refused")
        count, secs = get_segment_info("vid123")
        assert count == 0
        assert secs == 0.0

    @patch("siphon.sponsorblock.httpx.get")
    def test_malformed_segment_skipped(self, mock_get):
        mock_get.return_value = _mock_response(200, [
            {"segment": [10.0, 40.0], "category": "sponsor"},
            {"category": "outro"},  # missing segment key
        ])
        count, secs = get_segment_info("vid123")
        assert count == 2  # both counted
        assert secs == 30.0  # only first has duration

    @patch("siphon.sponsorblock.httpx.get")
    def test_empty_segments_list(self, mock_get):
        mock_get.return_value = _mock_response(200, [])
        count, secs = get_segment_info("vid123")
        assert count == 0
        assert secs == 0.0


class TestGetSegmentCount:
    @patch("siphon.sponsorblock.httpx.get")
    def test_returns_count_only(self, mock_get):
        mock_get.return_value = _mock_response(200, [
            {"segment": [10.0, 40.0], "category": "sponsor"},
            {"segment": [100.0, 115.5], "category": "outro"},
        ])
        count = get_segment_count("vid123")
        assert count == 2

    @patch("siphon.sponsorblock.httpx.get")
    def test_error_returns_zero(self, mock_get):
        mock_get.side_effect = Exception("timeout")
        assert get_segment_count("vid123") == 0
