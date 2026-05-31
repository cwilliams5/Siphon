"""Diagnostic logging: durable file log + periodic resource snapshots.

This module exists to hunt a slow-burn degradation: after several days of
uptime, every episode starts getting marked "LLM skipped" until the process is
restarted. A restart clearing it points at an exhausted process-level resource.
The leading suspects are GPU VRAM exhaustion in the Whisper worker and OS
handle/thread leaks from subprocess churn (Claude CLI + ffmpeg).

Everything here is always-on for now so we can catch the next occurrence in the
wild. It is intentionally cheap and fully best-effort — a missing optional
dependency (psutil, NVML) degrades the snapshot, it never raises. We can gate
it behind config once the cause is found.
"""

from __future__ import annotations

import gc
import logging
import logging.handlers
import os
import threading

logger = logging.getLogger(__name__)

# Dedicated logger so resource lines are easy to grep/filter in the file log.
_res_logger = logging.getLogger("siphon.diagnostics.resources")

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def setup_file_logging(log_path: str, level: int = logging.INFO) -> str:
    """Attach a rotating file handler to the root logger.

    Console (stderr) logging stays as-is; this adds a durable copy on disk so
    failures that happen days into a run survive the inevitable restart. Rotates
    at 20 MB and keeps 10 backups (~200 MB ceiling) — comfortably multiple weeks
    of INFO-level output, and rotation keeps the most recent (i.e. the run that
    degraded) rather than the oldest.

    Idempotent: calling twice will not double-attach. Returns the log path.
    """
    log_dir = os.path.dirname(log_path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    root = logging.getLogger()
    for h in root.handlers:
        if getattr(h, "_siphon_file_log", False):
            return log_path

    handler = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=20 * 1024 * 1024, backupCount=10, encoding="utf-8",
    )
    handler._siphon_file_log = True  # type: ignore[attr-defined]
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    root.addHandler(handler)
    if root.level == logging.NOTSET or root.level > level:
        root.setLevel(level)

    logger.info("File logging enabled at %s", log_path)
    return log_path


def _gpu_memory() -> str | None:
    """Best-effort GPU VRAM usage. Returns None if no GPU/driver/binding."""
    # Prefer NVML (nvidia-ml-py) — reports real device-wide usage, which is
    # what matters for a leak, and works regardless of which framework holds it.
    try:
        import pynvml  # type: ignore

        pynvml.nvmlInit()
        try:
            h = pynvml.nvmlDeviceGetHandleByIndex(0)
            mem = pynvml.nvmlDeviceGetMemoryInfo(h)
            return (
                f"vram_used={mem.used / 1e9:.2f}GB "
                f"vram_free={mem.free / 1e9:.2f}GB "
                f"vram_total={mem.total / 1e9:.2f}GB"
            )
        finally:
            pynvml.nvmlShutdown()
    except Exception:
        pass

    # Fall back to torch only if it's already importable (don't pull it in).
    try:
        import torch  # type: ignore

        if torch.cuda.is_available():
            free, total = torch.cuda.mem_get_info()
            return (
                f"vram_used={(total - free) / 1e9:.2f}GB "
                f"vram_free={free / 1e9:.2f}GB "
                f"vram_total={total / 1e9:.2f}GB"
            )
    except Exception:
        pass

    return None


def log_resource_snapshot() -> None:
    """Log a one-line snapshot of process + GPU resource usage.

    Called on a timer so we can correlate the onset of "everything skipped" with
    a climbing resource. Watch for a steady rise across snapshots in: handles
    (Windows handle leak), threads (orphaned/hung executor threads), rss (memory
    leak), or vram_used (Whisper/CTranslate2 VRAM leak).
    """
    parts: list[str] = [
        f"threads={threading.active_count()}",
        f"gc_objects={len(gc.get_objects())}",
    ]

    try:
        import psutil  # type: ignore

        p = psutil.Process()
        with p.oneshot():
            parts.append(f"rss={p.memory_info().rss / 1e6:.0f}MB")
            parts.append(f"cpu={p.cpu_percent(interval=None):.0f}%")
            try:
                parts.append(f"handles={p.num_handles()}")  # Windows
            except AttributeError:
                parts.append(f"fds={p.num_fds()}")  # POSIX
        try:
            parts.append(f"child_procs={len(p.children())}")
        except Exception:
            pass
    except Exception as exc:
        parts.append(f"psutil_unavailable={exc!r}")

    gpu = _gpu_memory()
    if gpu:
        parts.append(gpu)

    _res_logger.info("RESOURCES %s", " ".join(parts))
