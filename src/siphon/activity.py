"""In-memory activity log for the web UI."""
from __future__ import annotations
import threading
from datetime import datetime, timezone
from collections import deque

_log: deque[dict] = deque(maxlen=200)
_lock = threading.Lock()

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
