# Siphon - Claude Code Guide

## What this is
Self-hosted podcast pipeline: YouTube channels + podcast RSS feeds → SponsorBlock cuts → Whisper transcription → Claude ad detection → clean RSS feeds served via Tailscale Funnel to Pocket Casts.

## Running & Testing
```bash
# Run the app (config lives outside repo — never commit secrets)
python -m siphon -c "/path/to/siphon-data/config.yaml"

# Run tests (380+, must all pass before committing)
python -m pytest tests/ -x -q
```

## Security
This is a public repo. NEVER commit:
- config.yaml (contains API keys, auth credentials, Tailscale hostname)
- Database files (.db)
- Media files
- Personal paths, hostnames, or feed URLs
- temp.txt or any scratch files with personal data

All personal config/data lives outside the repo. The .gitignore covers config.yaml, *.db, media/, and temp.txt.

## Architecture
Three-queue pipeline: Download → Whisper → Claude. Each runs independently on its own scheduler interval. Episodes flow: `pending` → `eligible` → `downloading` → `pending_whisper` → `pending_claude` → `done`. Feeds without LLM trim skip directly to `done`.

Key modules:
- `pipeline.py` — orchestrates all three workers, feed checking, download/whisper/claude processing
- `db.py` — SQLite with WAL mode, all episode state management, migrations via ALTER TABLE
- `config.py` — Pydantic v2 models, `resolve_feed()` merges per-feed overrides onto defaults
- `app.py` — FastAPI factory with lifespan, APScheduler, security middleware
- `routes/ui.py` — Web UI (htmx SPA), stats dashboard, feed management, OPML import
- `youtube.py` — YouTube Data API v3 (playlistItems + videos.list for duration/region)
- `ad_detect.py` — Claude CLI invocation with JSON schema, dual-format transcript
- `cutter.py` — ffmpeg segment cutting via inversion (keep-ranges, not sequential cuts)
- `sponsorblock.py` — SponsorBlock API for segment count + duration
- `filters.py` — Pure functions for short/duration/title/date filtering

## Key Patterns

### DB updates use kwargs
```python
db.update_episode_status(video_id, feed_name, "done", file_size=123, duration=456)
```
Any column can be set via kwargs. Watch for NULL vs missing — use `(ep.get("col") or 0)` not `ep.get("col", 0)` because SQLite columns can be NULL.

### Config reload
Scheduler reloads config from disk before each job. UI routes also reload on each request. The config object has `_config_path` for save-back.

### htmx integration
Templates use conditional extends: `{% if not is_htmx %}{% extends "base.html" %}{% endif %}`. Routes use `_render()` which passes `is_htmx` to template context. POST actions use `_redirect()` which returns `HX-Redirect` header for htmx, 303 for normal requests.

### Feed type detection
YouTube URLs auto-detected from URL (youtube.com, youtu.be, m.youtube.com). Everything else is podcast. No manual type picker in add form.

### SponsorBlock
Categories must be sent as `json.dumps(list)` not `str(list)` — the API silently returns 0 with Python's `str()` format (single quotes). SB cuts happen in yt-dlp postprocessors with `force_keyframes: True` for clean video transitions.

### Duration
YouTube Data API playlistItems.list does NOT return duration. We make a separate videos.list call (same 1 unit/50 videos cost) to get `contentDetails.duration` (ISO 8601) and `regionRestriction`. Post-download, ffprobe captures actual duration as safety net.

### Episode status transitions
Download functions set the correct next status atomically (`pending_whisper` if llm_trim, `done` if not). Do NOT set `done` then upgrade — that two-step pattern caused orphaned episodes on crash.

## Common Pitfalls
- `INSERT OR REPLACE` on feeds triggers CASCADE DELETE on episodes — use `ON CONFLICT DO UPDATE`
- Windows filenames can't have colons — sanitize video IDs with `re.sub(r'[^\w-]', '_', id)`
- `asyncio.create_task()` without saving reference → task gets garbage collected. Store in `app.state._background_tasks`
- YouTube API `search.list` costs 100 units/call vs `playlistItems.list` at 1 unit — never use search
- CUDA Whisper forced to 1 worker (CTranslate2 can't do concurrent GPU kernels)
- Whisper prompt piped via stdin, not CLI arg (Windows 32K char limit)
- Region-blocked videos filtered during discovery via `contentDetails.regionRestriction`

## Commit Convention
Always run tests before committing. Include `Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>` in commit messages. Push after each commit. User prefers autonomous milestone commits, not batching.
