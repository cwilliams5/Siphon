"""In-memory activity log for the web UI."""
from __future__ import annotations
import threading
from datetime import datetime, timezone
from collections import deque

_log: deque[dict] = deque(maxlen=200)
_lock = threading.Lock()

_current_status = {"text": "Idle", "updated": ""}
_status_lock = threading.Lock()


def set_status(text: str) -> None:
    with _status_lock:
        _current_status["text"] = text
        _current_status["updated"] = datetime.now(timezone.utc).strftime("%H:%M:%S")


def get_status() -> dict:
    with _status_lock:
        return dict(_current_status)

def log_activity(message: str, feed: str = "", level: str = "info") -> None:
    """Add an activity entry."""
    with _lock:
        _log.appendleft({
            "time": datetime.now(timezone.utc).strftime("%H:%M:%S"),
            "message": message,
            "feed": feed,
            "level": level,
        })

def get_recent(limit: int = 50) -> list[dict]:
    """Get recent activity entries."""
    with _lock:
        return list(_log)[:limit]

def clear() -> None:
    with _lock:
        _log.clear()
