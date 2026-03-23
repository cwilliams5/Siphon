"""In-memory activity log for the web UI."""
from __future__ import annotations
import threading
from datetime import datetime, timezone
from collections import deque
from zoneinfo import ZoneInfo

_log: deque[dict] = deque(maxlen=1000)
_lock = threading.Lock()

_current_status = {"text": "Idle", "updated": ""}
_status_lock = threading.Lock()

_tz_name: str = "America/Los_Angeles"


def set_timezone(tz: str) -> None:
    global _tz_name
    _tz_name = tz


def _now_local() -> str:
    return datetime.now(ZoneInfo(_tz_name)).strftime("%H:%M:%S")


def set_status(text: str) -> None:
    with _status_lock:
        _current_status["text"] = text
        _current_status["updated"] = _now_local()


def get_status() -> dict:
    with _status_lock:
        return dict(_current_status)

def log_activity(message: str, feed: str = "", level: str = "info") -> None:
    """Add an activity entry."""
    with _lock:
        _log.appendleft({
            "time": _now_local(),
            "message": message,
            "feed": feed,
            "level": level,
        })

def get_recent(limit: int = 200) -> list[dict]:
    """Get recent activity entries."""
    with _lock:
        return list(_log)[:limit]

def clear() -> None:
    with _lock:
        _log.clear()


# ------------------------------------------------------------------ #
# Pause / Resume system — queue-level control for all workers
# ------------------------------------------------------------------ #

_pause_state = "running"  # "running" | "pending_pause" | "paused"
_pause_lock = threading.Lock()


def request_pause() -> None:
    """Request a graceful pause. Workers will stop after finishing current item."""
    global _pause_state
    with _pause_lock:
        if _pause_state == "running":
            _pause_state = "pending_pause"


def resume() -> None:
    """Resume all workers."""
    global _pause_state
    with _pause_lock:
        _pause_state = "running"


def check_paused() -> bool:
    """Workers call this before starting next item. Returns True if should not proceed."""
    global _pause_state
    with _pause_lock:
        if _pause_state == "pending_pause":
            _pause_state = "paused"
            return True
        return _pause_state == "paused"


def get_pause_state() -> str:
    """Return current pause state: 'running', 'pending_pause', or 'paused'."""
    with _pause_lock:
        return _pause_state


# ------------------------------------------------------------------ #
# Active worker counters — for observability
# ------------------------------------------------------------------ #

_active_counts = {"download": 0, "whisper": 0, "claude": 0}
_active_lock = threading.Lock()


def worker_start(worker: str) -> None:
    """Increment active count for a worker type."""
    with _active_lock:
        _active_counts[worker] = _active_counts.get(worker, 0) + 1


def worker_done(worker: str) -> None:
    """Decrement active count for a worker type."""
    with _active_lock:
        _active_counts[worker] = max(0, _active_counts.get(worker, 0) - 1)


def get_active_counts() -> dict:
    """Return current active worker counts."""
    with _active_lock:
        return dict(_active_counts)
