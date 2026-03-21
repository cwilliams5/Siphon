"""Pure functions for filtering YouTube video entries.

Each function takes an entry dict (from yt-dlp metadata) and/or filter
config values, and returns a reason string if the entry should be filtered
out, or ``None`` if the entry passes.
"""

from __future__ import annotations


def is_short(entry: dict) -> str | None:
    """Return ``"short"`` if the entry looks like a YouTube Short."""
    url = entry.get("url") or ""
    if "/shorts/" in url:
        return "short"

    duration = entry.get("duration")
    if duration is not None and duration < 60:
        # A sub-60-second video is assumed to be a Short unless the title
        # hints that it is intentionally a regular (non-Short) upload.
        return "short"

    return None


def title_excluded(title: str, exclude_patterns: list[str]) -> str | None:
    """Return ``"title_match:<pattern>"`` if *title* matches any pattern.

    Matching is case-insensitive substring search.
    """
    lower_title = title.lower()
    for pattern in exclude_patterns:
        if pattern.lower() in lower_title:
            return f"title_match:{pattern}"
    return None


def too_short(duration: int | None, min_duration: int) -> str | None:
    """Return ``"too_short"`` if *duration* is below the minimum.

    If *duration* is ``None`` the check is deferred (we cannot know yet),
    so the entry passes.
    """
    if duration is None:
        return None
    if duration < min_duration:
        return "too_short"
    return None


def too_old(upload_date: str | None, date_cutoff: str | None) -> str | None:
    """Return ``"too_old"`` if *upload_date* is before *date_cutoff*.

    Both values are ``YYYYMMDD`` strings.  If either is ``None`` the check
    is skipped and the entry passes.
    """
    if date_cutoff is None or upload_date is None:
        return None
    if upload_date < date_cutoff:
        return "too_old"
    return None


def apply_filters(
    entry: dict,
    block_shorts: bool,
    title_exclude: list[str],
    min_duration_seconds: int,
    date_cutoff: str | None,
) -> str | None:
    """Run every filter in order and return the first rejection reason.

    Returns ``None`` when the entry passes all filters.

    Expected *entry* keys: ``id``, ``title``, ``url``, ``duration``,
    ``upload_date``.
    """
    if block_shorts:
        reason = is_short(entry)
        if reason is not None:
            return reason

    reason = title_excluded(entry.get("title", ""), title_exclude)
    if reason is not None:
        return reason

    reason = too_short(entry.get("duration"), min_duration_seconds)
    if reason is not None:
        return reason

    reason = too_old(entry.get("upload_date"), date_cutoff)
    if reason is not None:
        return reason

    return None
