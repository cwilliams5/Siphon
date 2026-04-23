"""Tests for the split pipeline workers: Whisper, Claude, and interrupt recovery."""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from siphon.config import (
    AuthConfig,
    CookiesConfig,
    FeedConfig,
    FeedDefaults,
    LLMConfig,
    ScheduleConfig,
    ServerConfig,
    SiphonConfig,
    StorageConfig,
    YouTubeConfig,
)
from siphon.db import Database
from siphon.pipeline import (
    _transcript_path,
    process_claude,
    process_downloads,
    process_whisper,
    recover_interrupted,
)


# ------------------------------------------------------------------ #
# Fixtures
# ------------------------------------------------------------------ #


@pytest.fixture()
def db():
    """In-memory database with pre-registered feeds."""
    d = Database(":memory:")
    d.upsert_feed("test-feed", "https://www.youtube.com/@TestChannel", "youtube")
    d.upsert_feed("llm-feed", "https://www.youtube.com/@LLMChannel", "youtube")
    yield d
    d.close()


@pytest.fixture()
def download_dir(tmp_path):
    """Temporary download directory."""
    d = tmp_path / "media"
    d.mkdir()
    return str(d)


@pytest.fixture()
def config(download_dir):
    """SiphonConfig with llm_trim enabled on llm-feed."""
    return SiphonConfig(
        server=ServerConfig(host="127.0.0.1", port=8080, base_url="http://localhost"),
        auth=AuthConfig(username="user", password="pass"),
        youtube=YouTubeConfig(api_key="test-key"),
        storage=StorageConfig(
            download_dir=download_dir,
            database=":memory:",
            max_disk_gb=100,
            youtube_keep_per_feed=50,
            podcast_keep_per_feed=200,
        ),
        schedule=ScheduleConfig(
            youtube_feeds_per_check=10,
            podcast_feeds_per_check=30,
            youtube_download_workers=2,
            youtube_download_delay_seconds=0,
            youtube_max_downloads_per_hour=100,
            podcast_download_workers=10,
            podcast_download_delay_seconds=0,
            podcast_max_downloads_per_hour=100,
        ),
        cookies=CookiesConfig(source="browser", browser="firefox"),
        defaults=FeedDefaults(
            sponsorblock_delay_minutes=60,
            block_shorts=True,
            min_duration_seconds=60,
        ),
        llm=LLMConfig(
            whisper_model="base",
            whisper_device="cpu",
            claude_concurrency=3,
        ),
        feeds=[
            FeedConfig(name="test-feed", url="https://www.youtube.com/@TestChannel"),
            FeedConfig(name="llm-feed", url="https://www.youtube.com/@LLMChannel", llm_trim=True),
        ],
    )


def _create_media_file(download_dir, feed_name, video_id, ext=".mp3"):
    """Create a dummy media file and return its path."""
    feed_dir = os.path.join(download_dir, feed_name)
    os.makedirs(feed_dir, exist_ok=True)
    path = os.path.join(feed_dir, f"{video_id}{ext}")
    with open(path, "wb") as f:
        f.write(b"x" * 1000)
    return path


def _create_transcript(download_dir, feed_name, video_id, transcript=None):
    """Create a transcript JSON file and return its path."""
    if transcript is None:
        transcript = {
            "text": "Hello and welcome. This video is sponsored by Acme Corp.",
            "segments": [
                {"start": 0.0, "end": 10.0, "text": "Hello and welcome."},
                {"start": 10.0, "end": 30.0, "text": "This video is sponsored by Acme Corp."},
            ],
            "words": [
                {"word": "Hello", "start": 0.0, "end": 0.3},
                {"word": "and", "start": 0.4, "end": 0.5},
                {"word": "welcome", "start": 0.6, "end": 1.0},
            ],
            "language": "en",
            "duration": 300.0,
        }
    feed_dir = os.path.join(download_dir, feed_name)
    os.makedirs(feed_dir, exist_ok=True)
    path = os.path.join(feed_dir, f"{video_id}_transcript.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(transcript, f)
    return path


# ------------------------------------------------------------------ #
# Interrupt recovery
# ------------------------------------------------------------------ #


class TestRecoverInterrupted:
    def test_pending_whisper_left_alone(self, config, db, download_dir):
        """Episodes in pending_whisper should stay there."""
        path = _create_media_file(download_dir, "llm-feed", "vid001")
        db.insert_episode(video_id="vid001", feed_name="llm-feed", title="Ep 1", status="pending_whisper")
        db.update_episode_status("vid001", "llm-feed", "pending_whisper", file_path=path)

        recover_interrupted(config, db)

        ep = db.get_episode("vid001", "llm-feed")
        assert ep["status"] == "pending_whisper"

    def test_pending_claude_with_transcript_stays(self, config, db, download_dir):
        """pending_claude with a transcript file on disk should stay."""
        path = _create_media_file(download_dir, "llm-feed", "vid002")
        _create_transcript(download_dir, "llm-feed", "vid002")
        db.insert_episode(video_id="vid002", feed_name="llm-feed", title="Ep 2", status="pending_claude")
        db.update_episode_status("vid002", "llm-feed", "pending_claude", file_path=path)

        recover_interrupted(config, db)

        ep = db.get_episode("vid002", "llm-feed")
        assert ep["status"] == "pending_claude"

    def test_pending_claude_without_transcript_resets(self, config, db, download_dir):
        """pending_claude WITHOUT a transcript file should reset to pending_whisper."""
        path = _create_media_file(download_dir, "llm-feed", "vid003")
        db.insert_episode(video_id="vid003", feed_name="llm-feed", title="Ep 3", status="pending_claude")
        db.update_episode_status("vid003", "llm-feed", "pending_claude", file_path=path)

        recover_interrupted(config, db)

        ep = db.get_episode("vid003", "llm-feed")
        assert ep["status"] == "pending_whisper"


# ------------------------------------------------------------------ #
# Download worker — llm_trim routing
# ------------------------------------------------------------------ #


class TestDownloadWorkerLLMRouting:
    @patch("siphon.cutter.has_real_video_stream", return_value=True)
    @patch("siphon.cutter.validate_file", return_value=True)
    @patch("siphon.pipeline.find_downloaded_file")
    @patch("siphon.pipeline.download_video")
    async def test_download_sets_pending_whisper_when_llm_trim(
        self, mock_download, mock_find, mock_validate, mock_has_video, config, db, download_dir
    ):
        """When llm_trim is enabled, download should set status to pending_whisper."""
        db.insert_episode(
            video_id="vid001",
            feed_name="llm-feed",
            title="LLM Episode",
            status="eligible",
            eligible_at="2020-01-01T00:00:00",
        )

        path = os.path.join(download_dir, "llm-feed", "vid001.mp3")
        mock_download.return_value = {}
        mock_find.return_value = (path, 50_000)

        await process_downloads(config, db)

        ep = db.get_episode("vid001", "llm-feed")
        assert ep["status"] == "pending_whisper"

    @patch("siphon.cutter.has_real_video_stream", return_value=True)
    @patch("siphon.cutter.validate_file", return_value=True)
    @patch("siphon.pipeline.find_downloaded_file")
    @patch("siphon.pipeline.download_video")
    async def test_download_sets_done_when_no_llm_trim(
        self, mock_download, mock_find, mock_validate, mock_has_video, config, db, download_dir
    ):
        """When llm_trim is NOT enabled, download should set status to done."""
        db.insert_episode(
            video_id="vid001",
            feed_name="test-feed",
            title="Normal Episode",
            status="eligible",
            eligible_at="2020-01-01T00:00:00",
        )

        path = os.path.join(download_dir, "test-feed", "vid001.mp4")
        mock_download.return_value = {}
        mock_find.return_value = (path, 50_000_000)

        await process_downloads(config, db)

        ep = db.get_episode("vid001", "test-feed")
        assert ep["status"] == "done"


# ------------------------------------------------------------------ #
# Whisper worker
# ------------------------------------------------------------------ #


class TestWhisperWorker:
    @patch("siphon.transcribe.transcribe")
    async def test_whisper_processes_episode(
        self, mock_transcribe, config, db, download_dir
    ):
        """Whisper worker should transcribe and move to pending_claude."""
        path = _create_media_file(download_dir, "llm-feed", "vid001")
        db.insert_episode(video_id="vid001", feed_name="llm-feed", title="Ep 1", status="pending_whisper")
        db.update_episode_status("vid001", "llm-feed", "pending_whisper", file_path=path)

        mock_transcribe.return_value = {
            "text": "Hello world.",
            "segments": [{"start": 0.0, "end": 5.0, "text": "Hello world."}],
            "words": [
                {"word": "Hello", "start": 0.0, "end": 0.3},
                {"word": "world.", "start": 0.4, "end": 0.7},
            ],
            "language": "en",
            "duration": 5.0,
        }

        await process_whisper(config, db)

        ep = db.get_episode("vid001", "llm-feed")
        assert ep["status"] == "pending_claude"
        assert ep["whisper_word_count"] == 2
        assert ep["whisper_segment_count"] == 1
        assert ep["whisper_model"] == "base"
        assert ep["whisper_duration_seconds"] is not None

        # Transcript file should exist on disk
        transcript_file = _transcript_path(config, "llm-feed", "vid001")
        assert os.path.exists(transcript_file)

        # Verify content
        with open(transcript_file) as f:
            data = json.load(f)
        assert data["text"] == "Hello world."

    async def test_whisper_missing_file_marks_failed(self, config, db, download_dir):
        """Whisper should fail gracefully when media file is missing."""
        db.insert_episode(video_id="vid001", feed_name="llm-feed", title="Ep 1", status="pending_whisper")
        db.update_episode_status("vid001", "llm-feed", "pending_whisper", file_path="/nonexistent/file.mp3")

        await process_whisper(config, db)

        ep = db.get_episode("vid001", "llm-feed")
        assert ep["status"] == "failed"

    async def test_whisper_no_episodes_is_noop(self, config, db, download_dir):
        """Whisper worker with no pending episodes should not crash."""
        await process_whisper(config, db)  # Should not raise

    @patch("siphon.transcribe.transcribe")
    async def test_whisper_error_retries(
        self, mock_transcribe, config, db, download_dir
    ):
        """Whisper error should increment retry count and leave as pending_whisper."""
        path = _create_media_file(download_dir, "llm-feed", "vid001")
        db.insert_episode(video_id="vid001", feed_name="llm-feed", title="Ep 1", status="pending_whisper")
        db.update_episode_status("vid001", "llm-feed", "pending_whisper", file_path=path)

        mock_transcribe.side_effect = RuntimeError("Whisper crashed")

        await process_whisper(config, db)

        ep = db.get_episode("vid001", "llm-feed")
        assert ep["status"] == "pending_whisper"
        assert ep["llm_retry_count"] == 1


# ------------------------------------------------------------------ #
# Claude worker
# ------------------------------------------------------------------ #


class TestClaudeWorker:
    @patch("siphon.cutter.cut_segments")
    @patch("siphon.ad_detect.detect_ads")
    async def test_claude_processes_episode(
        self, mock_detect, mock_cut, config, db, download_dir
    ):
        """Claude worker should detect ads, cut, and set done."""
        path = _create_media_file(download_dir, "llm-feed", "vid001")
        _create_transcript(download_dir, "llm-feed", "vid001")
        db.insert_episode(video_id="vid001", feed_name="llm-feed", title="Ep 1", status="pending_claude")
        db.update_episode_status("vid001", "llm-feed", "pending_claude", file_path=path)

        mock_detect.return_value = {
            "segments": [
                {"start": 10.0, "end": 30.0, "type": "ad", "label": "sponsor", "confidence": 0.95}
            ]
        }

        await process_claude(config, db)

        ep = db.get_episode("vid001", "llm-feed")
        assert ep["status"] == "done"
        assert ep["llm_trim_status"] == "done"
        assert ep["llm_cuts_applied"] == 1
        assert ep["claude_duration_seconds"] is not None

        # Transcript file should be cleaned up
        transcript_file = _transcript_path(config, "llm-feed", "vid001")
        assert not os.path.exists(transcript_file)

    @patch("siphon.ad_detect.detect_ads")
    async def test_claude_no_ads_detected(self, mock_detect, config, db, download_dir):
        """Claude worker with no ads should still set done."""
        path = _create_media_file(download_dir, "llm-feed", "vid001")
        _create_transcript(download_dir, "llm-feed", "vid001")
        db.insert_episode(video_id="vid001", feed_name="llm-feed", title="Ep 1", status="pending_claude")
        db.update_episode_status("vid001", "llm-feed", "pending_claude", file_path=path)

        mock_detect.return_value = {"segments": []}

        await process_claude(config, db)

        ep = db.get_episode("vid001", "llm-feed")
        assert ep["status"] == "done"
        assert ep["llm_trim_status"] == "done"
        assert ep["llm_cuts_applied"] == 0

    async def test_claude_missing_transcript_resets(self, config, db, download_dir):
        """Claude worker with missing transcript should reset to pending_whisper."""
        path = _create_media_file(download_dir, "llm-feed", "vid001")
        db.insert_episode(video_id="vid001", feed_name="llm-feed", title="Ep 1", status="pending_claude")
        db.update_episode_status("vid001", "llm-feed", "pending_claude", file_path=path)
        # No transcript file created

        await process_claude(config, db)

        ep = db.get_episode("vid001", "llm-feed")
        assert ep["status"] == "pending_whisper"

    async def test_claude_no_episodes_is_noop(self, config, db, download_dir):
        """Claude worker with no pending episodes should not crash."""
        await process_claude(config, db)  # Should not raise

    @patch("siphon.ad_detect.detect_ads")
    async def test_claude_empty_transcript_sets_done(self, mock_detect, config, db, download_dir):
        """Claude worker with empty transcript should skip detection and set done."""
        path = _create_media_file(download_dir, "llm-feed", "vid001")
        _create_transcript(download_dir, "llm-feed", "vid001", transcript={
            "text": "",
            "segments": [],
            "words": [],
            "language": "en",
            "duration": 0.0,
        })
        db.insert_episode(video_id="vid001", feed_name="llm-feed", title="Ep 1", status="pending_claude")
        db.update_episode_status("vid001", "llm-feed", "pending_claude", file_path=path)

        await process_claude(config, db)

        ep = db.get_episode("vid001", "llm-feed")
        assert ep["status"] == "done"
        assert ep["llm_trim_status"] == "done"
        assert ep["llm_cuts_applied"] == 0
        mock_detect.assert_not_called()

    @patch("siphon.ad_detect.detect_ads")
    async def test_claude_error_retries(self, mock_detect, config, db, download_dir):
        """Claude error should increment retry count and leave as pending_claude."""
        path = _create_media_file(download_dir, "llm-feed", "vid001")
        _create_transcript(download_dir, "llm-feed", "vid001")
        db.insert_episode(video_id="vid001", feed_name="llm-feed", title="Ep 1", status="pending_claude")
        db.update_episode_status("vid001", "llm-feed", "pending_claude", file_path=path)

        mock_detect.side_effect = RuntimeError("Claude failed")

        await process_claude(config, db)

        ep = db.get_episode("vid001", "llm-feed")
        assert ep["status"] == "pending_claude"
        assert ep["llm_retry_count"] == 1

    @patch("siphon.ad_detect.detect_ads")
    async def test_claude_gives_up_after_max_retries(self, mock_detect, config, db, download_dir):
        """Claude should give up after 3 retries and set done/skipped."""
        path = _create_media_file(download_dir, "llm-feed", "vid001")
        _create_transcript(download_dir, "llm-feed", "vid001")
        db.insert_episode(video_id="vid001", feed_name="llm-feed", title="Ep 1", status="pending_claude")
        db.update_episode_status(
            "vid001", "llm-feed", "pending_claude",
            file_path=path, llm_retry_count=2,
        )

        mock_detect.side_effect = RuntimeError("Claude failed")

        await process_claude(config, db)

        ep = db.get_episode("vid001", "llm-feed")
        assert ep["status"] == "done"
        assert ep["llm_trim_status"] == "skipped"
        assert ep["llm_retry_count"] == 3


# ------------------------------------------------------------------ #
# Pause system
# ------------------------------------------------------------------ #


class TestPauseSystem:
    async def test_whisper_respects_pause(self, config, db, download_dir):
        """Whisper worker should skip processing when paused."""
        from siphon.activity import check_paused, request_pause, resume

        path = _create_media_file(download_dir, "llm-feed", "vid001")
        db.insert_episode(video_id="vid001", feed_name="llm-feed", title="Ep 1", status="pending_whisper")
        db.update_episode_status("vid001", "llm-feed", "pending_whisper", file_path=path)

        # Pause
        request_pause()
        check_paused()  # transitions to paused

        await process_whisper(config, db)

        # Episode should NOT have been processed
        ep = db.get_episode("vid001", "llm-feed")
        assert ep["status"] == "pending_whisper"

        # Clean up
        resume()

    async def test_claude_respects_pause(self, config, db, download_dir):
        """Claude worker should skip processing when paused."""
        from siphon.activity import check_paused, request_pause, resume

        path = _create_media_file(download_dir, "llm-feed", "vid001")
        _create_transcript(download_dir, "llm-feed", "vid001")
        db.insert_episode(video_id="vid001", feed_name="llm-feed", title="Ep 1", status="pending_claude")
        db.update_episode_status("vid001", "llm-feed", "pending_claude", file_path=path)

        # Pause
        request_pause()
        check_paused()

        await process_claude(config, db)

        # Episode should NOT have been processed
        ep = db.get_episode("vid001", "llm-feed")
        assert ep["status"] == "pending_claude"

        # Clean up
        resume()


# ------------------------------------------------------------------ #
# DB methods
# ------------------------------------------------------------------ #


class TestDBPipelineMethods:
    def test_get_pending_whisper(self, db):
        """get_pending_whisper returns episodes with pending_whisper status."""
        db.insert_episode(video_id="v1", feed_name="test-feed", title="Ep 1", status="pending_whisper")
        db.insert_episode(video_id="v2", feed_name="test-feed", title="Ep 2", status="done")
        db.insert_episode(video_id="v3", feed_name="test-feed", title="Ep 3", status="pending_whisper")

        result = db.get_pending_whisper(limit=5)
        assert len(result) == 2
        ids = {r["video_id"] for r in result}
        assert ids == {"v1", "v3"}

    def test_get_pending_whisper_limit(self, db):
        """get_pending_whisper respects limit."""
        for i in range(5):
            db.insert_episode(
                video_id=f"v{i}", feed_name="test-feed",
                title=f"Ep {i}", status="pending_whisper",
            )

        result = db.get_pending_whisper(limit=2)
        assert len(result) == 2

    def test_get_pending_claude(self, db):
        """get_pending_claude returns episodes with pending_claude status and file_path."""
        db.insert_episode(video_id="v1", feed_name="test-feed", title="Ep 1", status="pending_claude")
        db.update_episode_status("v1", "test-feed", "pending_claude", file_path="/some/file.mp3")

        db.insert_episode(video_id="v2", feed_name="test-feed", title="Ep 2", status="pending_claude")
        # No file_path set — should NOT be returned

        result = db.get_pending_claude(limit=5)
        assert len(result) == 1
        assert result[0]["video_id"] == "v1"

    def test_new_metric_columns_exist(self, db):
        """New metric columns should be accessible."""
        db.insert_episode(video_id="v1", feed_name="test-feed", title="Ep 1", status="done")
        db.update_episode_status(
            "v1", "test-feed", "done",
            whisper_duration_seconds=12.5,
            claude_duration_seconds=45.3,
            ffmpeg_duration_seconds=2.1,
            whisper_word_count=500,
            whisper_segment_count=25,
            transcript_size_bytes=12345,
            whisper_model="base",
        )

        ep = db.get_episode("v1", "test-feed")
        assert ep["whisper_duration_seconds"] == 12.5
        assert ep["claude_duration_seconds"] == 45.3
        assert ep["ffmpeg_duration_seconds"] == 2.1
        assert ep["whisper_word_count"] == 500
        assert ep["whisper_segment_count"] == 25
        assert ep["transcript_size_bytes"] == 12345
        assert ep["whisper_model"] == "base"
