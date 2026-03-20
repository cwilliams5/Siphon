"""Tests for siphon.tray — system tray icon."""

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
        tray = SiphonTray()
        mock_scheduler = MagicMock()
        tray.set_scheduler(mock_scheduler)

        # First call: pause
        tray._on_pause(None, None)
        mock_scheduler.pause.assert_called_once()
        assert tray._paused is True

        # Second call: resume
        tray._on_pause(None, None)
        mock_scheduler.resume.assert_called_once()
        assert tray._paused is False

    def test_pause_without_scheduler(self):
        tray = SiphonTray()
        # Should not raise
        tray._on_pause(None, None)

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
        tray = SiphonTray()
        tray._paused = True
        menu = tray._build_menu()
        # Menu should exist — we can't easily inspect pystray MenuItem internals
        # but at least verify it doesn't crash
        assert menu is not None
