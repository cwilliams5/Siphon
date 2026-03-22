"""Tests for siphon.tray -- system tray icon."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from siphon.tray import SiphonTray, create_icon_image


class TestCreateIconImage:
    def test_creates_image(self):
        img = create_icon_image(64)
        assert img.size == (64, 64)
        assert img.mode == "RGBA"

    def test_custom_size(self):
        img = create_icon_image(128)
        assert img.size == (128, 128)


class TestSiphonTray:
    def test_init_defaults(self):
        tray = SiphonTray()
        assert tray.port == 8585
        assert tray.host == "127.0.0.1"
        assert tray.base_url == "http://127.0.0.1:8585"

    def test_custom_port(self):
        tray = SiphonTray(port=9090, host="0.0.0.0")
        assert tray.base_url == "http://0.0.0.0:9090"

    def test_set_scheduler(self):
        tray = SiphonTray()
        mock_scheduler = MagicMock()
        tray.set_scheduler(mock_scheduler)
        assert tray._scheduler is mock_scheduler

    def test_pause_toggles(self):
        from siphon.activity import get_pause_state, resume

        # Reset pause state before test
        resume()

        tray = SiphonTray()

        # First call: request pause
        tray._on_pause(None, None)
        assert get_pause_state() == "pending_pause"

        # Second call: resume (from pending_pause)
        tray._on_pause(None, None)
        assert get_pause_state() == "running"

    def test_pause_resume_from_paused(self):
        from siphon.activity import check_paused, get_pause_state, request_pause, resume

        # Reset state
        resume()

        tray = SiphonTray()

        # Request pause, then consume it to transition to "paused"
        request_pause()
        check_paused()  # transitions pending_pause -> paused
        assert get_pause_state() == "paused"

        # Now resume
        tray._on_pause(None, None)
        assert get_pause_state() == "running"

        # Clean up
        resume()

    def test_pause_without_scheduler(self):
        tray = SiphonTray()
        # Should not raise
        tray._on_pause(None, None)
        # Clean up
        from siphon.activity import resume
        resume()

    @patch("siphon.tray.webbrowser.open")
    def test_open_ui(self, mock_open):
        tray = SiphonTray(port=8585)
        tray._on_open_ui(None, None)
        mock_open.assert_called_once_with("http://127.0.0.1:8585/ui/")

    def test_build_menu(self):
        tray = SiphonTray()
        menu = tray._build_menu()
        assert menu is not None

    def test_menu_shows_paused_state(self):
        from siphon.activity import request_pause, check_paused, resume

        # Reset
        resume()

        tray = SiphonTray()

        # Request pause, transition to paused
        request_pause()
        check_paused()

        menu = tray._build_menu()
        assert menu is not None

        # Clean up
        resume()
