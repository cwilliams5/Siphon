"""System tray icon for Siphon — start/pause/stop/open config."""

from __future__ import annotations

import logging
import threading
import webbrowser
from typing import Any

from PIL import Image, ImageDraw

logger = logging.getLogger(__name__)


def create_icon_image(size: int = 64, color: str = "#7c83ff") -> Image.Image:
    """Generate a simple Siphon tray icon (funnel shape)."""
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Draw a simple "S" inspired funnel/siphon shape
    margin = size // 8
    # Top wide part
    draw.rectangle(
        [margin, margin, size - margin, size // 3],
        fill=color,
    )
    # Middle narrowing
    mid_margin = size // 4
    draw.rectangle(
        [mid_margin, size // 3, size - mid_margin, 2 * size // 3],
        fill=color,
    )
    # Bottom narrow spout
    spout_margin = size * 3 // 8
    draw.rectangle(
        [spout_margin, 2 * size // 3, size - spout_margin, size - margin],
        fill=color,
    )

    return img


class SiphonTray:
    """Manages the system tray icon and menu."""

    def __init__(self, port: int = 8585, host: str = "127.0.0.1"):
        self.port = port
        self.host = host
        self._icon = None
        self._paused = False
        self._scheduler = None
        self._server_thread = None

    def set_scheduler(self, scheduler: Any) -> None:
        """Set the APScheduler instance for pause/resume control."""
        self._scheduler = scheduler

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def _on_open_ui(self, icon, item):
        webbrowser.open(f"{self.base_url}/ui/")

    def _on_pause(self, icon, item):
        if self._scheduler is None:
            logger.warning("No scheduler set — cannot pause")
            return
        if self._paused:
            self._scheduler.resume()
            self._paused = False
            logger.info("Scheduler resumed")
        else:
            self._scheduler.pause()
            self._paused = True
            logger.info("Scheduler paused")
        # Update the menu to reflect the new state
        self._update_menu()

    def _on_quit(self, icon, item):
        logger.info("Quit requested from tray")
        icon.stop()
        # Signal the server to shut down
        import os
        os._exit(0)

    def _pause_label(self, item):
        return "Resume" if self._paused else "Pause"

    def _update_menu(self):
        """Rebuild the menu to reflect current state."""
        if self._icon is not None:
            self._icon.menu = self._build_menu()

    def _build_menu(self):
        import pystray
        pause_text = "Resume" if self._paused else "Pause"
        status_text = "Paused" if self._paused else "Running"
        return pystray.Menu(
            pystray.MenuItem(f"Siphon ({status_text})", None, enabled=False),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Open Config", self._on_open_ui, default=True),
            pystray.MenuItem(pause_text, self._on_pause),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Quit", self._on_quit),
        )

    def run(self) -> None:
        """Start the tray icon. Blocks the calling thread."""
        import pystray

        icon_image = create_icon_image()
        self._icon = pystray.Icon(
            "siphon",
            icon=icon_image,
            title="Siphon",
            menu=self._build_menu(),
        )

        logger.info("Starting system tray icon")
        self._icon.run()

    def run_in_background(self) -> threading.Thread:
        """Start the tray icon in a background thread. Returns the thread."""
        t = threading.Thread(target=self.run, daemon=True, name="siphon-tray")
        t.start()
        return t
