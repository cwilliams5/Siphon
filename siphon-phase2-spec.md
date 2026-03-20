# Siphon — Phase 2 Spec: LLM Ad Trimming + Audio Podcast Path

## Context

Siphon is an existing personal media tool built in Python (yt-dlp, FastAPI, SQLite/SQLModel,
APScheduler, YAML config) that downloads YouTube channel feeds, applies SponsorBlock cuts,
and serves clean RSS via Tailscale Funnel. This spec extends it in two directions:

1. **LLM trim pass** — post-SponsorBlock Whisper+Claude cleanup for YouTube videos
2. **Audio podcast path** — a new RSS feed type for non-YouTube podcasts with full
   Whisper+Claude ad removal

---

## 1. Global Config Additions

Extend the existing global config YAML with a new `llm` block:

```yaml
llm:
  whisper_model: "base"           # tiny | base | small | medium | large
                                  # runs locally, no API cost
  whisper_device: "cpu"           # cpu | cuda
  claude_model: "claude-sonnet-4-6"  # invoked via `claude` CLI using Max subscription
  claude_concurrency: 3           # max parallel claude CLI invocations
  claude_effort: "medium"         # low | medium | high | max
                                  # Controls adaptive thinking depth for ad detection.
                                  # medium is a good starting point — watch API usage
                                  # and dial back to low if cost is a concern.
                                  # low is likely sufficient for pattern-matching tasks
                                  # like ad detection; high/max is overkill here.
  default_ad_prompt: |
    You are analyzing a podcast/video transcript to identify non-content segments
    that should be removed for personal use. Identify any of the following:
    - Paid sponsor reads or product promotions
    - Patreon, membership, or donation pitches
    - Merchandise promotion
    - Social media follow requests
    - Newsletter or mailing list signups
    - App review/subscribe/like requests
    - Live show or event ticket promotions
    - Cross-promotion of other shows or networks
    - The host's own book, course, or consulting pitch

    Return ONLY a JSON file written to the output path provided. No explanation,
    no markdown fences, just the raw JSON. Format:
    {
      "segments": [
        {
          "start": 0.0,
          "end": 0.0,
          "type": "ad",
          "label": "brief human-readable label",
          "confidence": 0.0
        }
      ]
    }
    Confidence is 0.0–1.0. Only include segments with confidence >= 0.5.
    If no ad segments found, return {"segments": []}.
  confidence_threshold: 0.75      # segments below this are flagged, not cut
  min_segment_duration: 7         # seconds — ignore detections shorter than this
  max_segment_duration: 300       # seconds — ignore detections longer than this (5 min)
```

---

## 2. Per-Feed Config Additions

Both YouTube feeds (existing) and podcast feeds (new, see section 4) support these
optional overrides. All are optional — omitting falls back to global defaults.

```yaml
feeds:
  - url: https://www.youtube.com/@SomeChannel
    type: youtube                   # existing — default type if omitted
    # ... existing fields unchanged ...
    llm_trim: true                  # opt-in to post-SponsorBlock LLM pass (default: false)
    claude_prompt_extra: |          # appended to global prompt, not replacing it
      Also remove segments where the host promotes their Discord server
      or references their own podcast network.

  - url: https://somepodcast.com/rss
    type: podcast                   # NEW — triggers audio podcast path
    # ... shared fields below ...
    claude_prompt_override: |       # replaces global prompt entirely for this feed
      This is a true crime podcast. Only remove third-party sponsor reads.
      Do NOT remove the host's own commentary, recaps, or chapter summaries
      even if they reference the show itself.
```

### Prompt resolution order (per feed):
1. If `claude_prompt_override` is set → use it exclusively
2. Else → global `default_ad_prompt` + appended `claude_prompt_extra` (if set)

---

## 3. YouTube LLM Trim Path (Addition to Existing Pipeline)

Triggered when a YouTube feed has `llm_trim: true`.

### Pipeline addition (runs after existing SponsorBlock step):

```
[existing] yt-dlp download → SponsorBlock cut → [NEW] extract audio →
Whisper transcribe → Claude detect → confidence filter →
ffmpeg final cut → serve RSS (unchanged)
```

### Implementation notes:

**Step: Extract audio for Whisper**
- Extract audio-only track from already-SponsorBlock-cut video using ffmpeg
- Use a temp file — discard after transcription
- Do not re-download; work from the file already on disk

**Step: Whisper transcription**
- Run local Whisper (openai-whisper Python package or faster-whisper)
- Output format: `word_timestamps=True` or at minimum segment-level timestamps
- Write transcript to `{job_id}_transcript.json` in a working directory
- Transcript JSON schema expected downstream:
  ```json
  {
    "segments": [
      { "start": 0.0, "end": 0.0, "text": "..." }
    ]
  }
  ```

**Step: Claude CLI invocation**
- Build prompt from resolved prompt (see section 2)
- Pass full transcript text inline in the prompt
- Provide an output path for Claude to write JSON to disk:
  `{job_id}_ad_segments.json`
- Invocation pattern:
  ```python
  import subprocess, json, tempfile, os

  def detect_ads(transcript_text: str, prompt: str, model: str,
                 effort: str, output_path: str) -> dict:
      full_prompt = (
          f"{prompt}\n\n"
          f"Write your response as JSON to this exact file path: {output_path}\n\n"
          f"TRANSCRIPT:\n{transcript_text}"
      )
      subprocess.run(
          ["claude", "--model", model, "--effort", effort, "-p", full_prompt],
          check=True
      )
      # Note: --effort flag enables adaptive thinking on Sonnet 4.6.
      # medium is the recommended default. Verify exact CLI flag name in
      # `claude --help` during implementation — may be --effort or --thinking-effort.
      with open(output_path) as f:
          return json.load(f)
  ```
- Respect `claude_concurrency` global setting via a semaphore

**Step: Confidence filter**
- Drop segments below `confidence_threshold`
- Drop segments shorter than `min_segment_duration`
- Drop segments longer than `max_segment_duration`
- Remaining segments → flagged in DB as `status = "pending_review"` if confidence
  is between 0.5 and threshold (marginal detections — logged but not cut)

**Step: ffmpeg final cut**
- Apply remaining high-confidence segments as additional cuts on top of
  already-SponsorBlock-processed file
- Use concat demuxer approach (same as SponsorBlock already does) — invert segment
  list to get keep-ranges, concat those ranges
- Overwrite file in place or write to new path and swap

**DB additions (episodes table):**
```sql
llm_trim_status   TEXT  -- null | pending | done | error
llm_segments_json TEXT  -- raw JSON of detected segments (for audit/review)
llm_cuts_applied  INTEGER  -- count of segments actually cut
```

---

## 4. Audio Podcast Path (New Feed Type)

Feeds with `type: podcast` follow a fully new pipeline. They are not YouTube sources —
they are standard RSS podcast feeds (MP3/AAC enclosures).

### Pipeline:

```
APScheduler poll → RSS fetch → new episode detection → DB insert →
download audio enclosure → Whisper transcribe → Claude detect →
confidence filter → ffmpeg cut → serve clean RSS via FastAPI
```

### RSS polling:
- Fetch feed XML on the same APScheduler interval as YouTube feeds
- Parse `<item>` entries, extract `<enclosure url>` for audio file
- Use `<guid>` as dedup key
- Respect existing `title_filter`, `min_length`, `date_cutoff` per-feed settings
  (these apply to podcasts too)

### Audio download:
- Simple HTTP download (requests or httpx) — no yt-dlp needed
- Store in same media directory structure as YouTube downloads:
  `media/{feed_slug}/{episode_slug}.mp3`

### Whisper + Claude:
- Identical to section 3 — no changes needed
- For podcasts, LLM trim is always on (it's the only ad-removal mechanism)
- No SponsorBlock step

### Clean RSS serving:
- FastAPI generates a synthetic RSS feed for each podcast feed
- Enclosure URLs point to Siphon's local/Tailscale-served audio files
  (same pattern as YouTube RSS serving)
- Feed metadata (title, description, artwork) proxied from original RSS
- Episodes not yet processed serve with original enclosure URL as fallback,
  or are withheld until processed (configurable per feed: `serve_unprocessed: true/false`,
  default `false`)

### DB additions (new `podcast_episodes` table or extend `episodes`):
```sql
feed_id           TEXT
guid              TEXT  -- from RSS <guid>
title             TEXT
pub_date          TEXT
duration_seconds  INTEGER
audio_url         TEXT  -- original enclosure URL
local_path        TEXT  -- path to downloaded + processed file
status            TEXT  -- pending_download | downloading | pending_llm |
                        -- processing_llm | done | error
llm_trim_status   TEXT
llm_segments_json TEXT
llm_cuts_applied  INTEGER
```

---

## 5. Shared Infrastructure Notes

### Whisper as a module:
- Install `openai-whisper` or `faster-whisper` (faster-whisper recommended —
  significantly faster on CPU, same quality)
- Wrap in a `transcribe(audio_path, model_size, device) -> dict` utility function
- Transcription is blocking and memory-intensive — run in a thread pool with
  concurrency 1 unless CUDA is available

### Processing queue:
- Both YouTube LLM trim jobs and podcast jobs share the same background worker
- APScheduler job checks DB for `status = 'pending_llm'` and processes sequentially
  (or up to `claude_concurrency` parallel Claude calls with serial Whisper)
- On startup, reset any `status = 'processing_*'` rows back to their pending state
  (crash recovery)

### Error handling:
- If Claude CLI exits non-zero or output JSON is missing/malformed → set
  `llm_trim_status = 'error'`, log, serve original uncut file
- If Whisper fails → same fallback
- Never block RSS serving due to LLM processing failure

### Temp file cleanup:
- Delete `_transcript.json` and `_ad_segments.json` after processing completes
- Keep `llm_segments_json` in DB for audit purposes

---

## 6. What Is NOT Changing

- Existing YouTube feed config fields (quality, title_filter, date_cutoff, short_blocking,
  min_length, sponsorblock_delay, etc.) — unchanged
- SponsorBlock integration — unchanged, still runs first for YouTube feeds
- FastAPI RSS serving for YouTube — unchanged (LLM trim is transparent, same output path)
- Tailscale Funnel serving — unchanged
- SQLite schema for existing tables — additive only (new columns, new table)
- YAML config structure for existing feeds — additive only (new optional fields)

---

## 7. Suggested Implementation Order for Claude Code

1. Add `llm` global config block + parsing
2. Add `faster-whisper` dependency + `transcribe()` utility
3. Add Claude CLI detection utility (`detect_ads()`) with JSON-to-disk pattern
4. Add YouTube LLM trim path (post-SponsorBlock, opt-in per feed)
5. Add `type: podcast` feed support — RSS polling + audio download
6. Wire podcast feeds through Whisper → Claude → ffmpeg → clean RSS
7. DB migrations (additive columns + new table)
8. Test with one feed of each type end to end

---

## 8. Open Questions / Decisions for Claude Code Session

- **faster-whisper vs openai-whisper**: Recommend faster-whisper unless there's a
  conflict with existing deps
- **Whisper model default**: `base` is fast and good enough for ad detection transcript
  quality; `small` if accuracy is poor on a specific feed. User can override per global config.
- **serve_unprocessed default**: Suggest `false` — better to wait for a clean episode
  than serve an ad-laden one. Make it explicit in config if user wants otherwise.
- **ffmpeg concat approach**: Confirm Siphon already has an ffmpeg wrapper from
  SponsorBlock integration — reuse that rather than reimplementing
