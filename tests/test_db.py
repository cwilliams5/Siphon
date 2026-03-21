"""Tests for siphon.db – plain sqlite3 database layer."""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta

import pytest

from siphon.db import Database, SCHEMA


# ------------------------------------------------------------------ #
# Fixtures
# ------------------------------------------------------------------ #

@pytest.fixture()
def db(tmp_path):
    """Yield a Database backed by a temp file, closed after the test."""
    d = Database(str(tmp_path / "test.db"))
    yield d
    d.close()


def _add_feed(db: Database, name: str = "tech", url: str = "https://youtube.com/@tech",
              feed_type: str = "youtube") -> None:
    db.upsert_feed(name, url, feed_type)


def _add_episode(db: Database, video_id: str = "v1", feed_name: str = "tech", **overrides) -> None:
    defaults = dict(
        title="Episode 1",
        description="desc",
        thumbnail_url="https://img/1.jpg",
        channel_name="TechChan",
        duration=600,
        upload_date="2025-01-01",
        eligible_at="2025-01-01T00:00:00",
        status="pending",
    )
    defaults.update(overrides)
    db.insert_episode(video_id=video_id, feed_name=feed_name, **defaults)


# ------------------------------------------------------------------ #
# Schema
# ------------------------------------------------------------------ #

class TestSchema:
    def test_tables_exist(self, db: Database):
        tables = {
            row["name"]
            for row in db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "feeds" in tables
        assert "episodes" in tables

    def test_indexes_exist(self, db: Database):
        indexes = {
            row["name"]
            for row in db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
        assert "idx_episodes_feed_status" in indexes
        assert "idx_episodes_eligible" in indexes

    def test_wal_mode(self, db: Database):
        mode = db.conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode.lower() == "wal"

    def test_foreign_keys_enabled(self, db: Database):
        fk = db.conn.execute("PRAGMA foreign_keys").fetchone()[0]
        assert fk == 1

    def test_feed_type_column_exists(self, db: Database):
        _add_feed(db)
        feed = db.get_feed("tech")
        assert feed["feed_type"] == "youtube"

    def test_llm_columns_exist(self, db: Database):
        _add_feed(db)
        _add_episode(db)
        ep = db.get_episode("v1", "tech")
        assert ep["llm_trim_status"] is None
        assert ep["llm_segments_json"] is None
        assert ep["llm_cuts_applied"] is None

    def test_sb_cuts_applied_column_exists(self, db: Database):
        _add_feed(db)
        _add_episode(db)
        ep = db.get_episode("v1", "tech")
        assert ep["sb_cuts_applied"] is None


# ------------------------------------------------------------------ #
# Feeds
# ------------------------------------------------------------------ #

class TestFeeds:
    def test_upsert_and_get(self, db: Database):
        _add_feed(db)
        feed = db.get_feed("tech")
        assert feed is not None
        assert feed["name"] == "tech"
        assert feed["url"] == "https://youtube.com/@tech"
        assert feed["feed_type"] == "youtube"
        assert feed["created_at"] is not None

    def test_upsert_replaces_url(self, db: Database):
        _add_feed(db)
        db.upsert_feed("tech", "https://new-url.com")
        feed = db.get_feed("tech")
        assert feed["url"] == "https://new-url.com"

    def test_upsert_does_not_delete_episodes(self, db: Database):
        """Regression: INSERT OR REPLACE triggers CASCADE DELETE on episodes.
        upsert_feed must use ON CONFLICT DO UPDATE instead."""
        _add_feed(db)
        _add_episode(db, "v1")
        _add_episode(db, "v2", title="Ep 2")
        assert len(db.get_episodes_by_feed("tech")) == 2

        # Re-upsert the same feed (simulates config reload)
        db.upsert_feed("tech", "https://youtube.com/@tech")

        # Episodes must still exist
        eps = db.get_episodes_by_feed("tech")
        assert len(eps) == 2, f"Expected 2 episodes, got {len(eps)} — upsert cascaded a delete!"

    def test_upsert_podcast_feed(self, db: Database):
        _add_feed(db, "pod", "https://example.com/rss", "podcast")
        feed = db.get_feed("pod")
        assert feed["feed_type"] == "podcast"

    def test_get_feed_missing(self, db: Database):
        assert db.get_feed("nope") is None

    def test_get_all_feeds(self, db: Database):
        _add_feed(db, "a", "http://a")
        _add_feed(db, "b", "http://b")
        feeds = db.get_all_feeds()
        assert len(feeds) == 2
        names = {f["name"] for f in feeds}
        assert names == {"a", "b"}

    def test_get_feeds_to_check_nulls_first(self, db: Database):
        """Feeds that have never been checked (NULL last_checked_at) come first."""
        _add_feed(db, "checked", "http://checked")
        _add_feed(db, "unchecked", "http://unchecked")
        db.update_feed_checked("checked")

        result = db.get_feeds_to_check(10)
        assert result[0]["name"] == "unchecked"
        assert result[1]["name"] == "checked"

    def test_get_feeds_to_check_limit(self, db: Database):
        for i in range(5):
            _add_feed(db, f"f{i}", f"http://f{i}")
        result = db.get_feeds_to_check(2)
        assert len(result) == 2

    def test_update_feed_checked_sets_timestamp(self, db: Database):
        _add_feed(db)
        db.update_feed_checked("tech")
        feed = db.get_feed("tech")
        assert feed["last_checked_at"] is not None
        assert feed["last_error"] is None

    def test_update_feed_checked_with_error(self, db: Database):
        _add_feed(db)
        db.update_feed_checked("tech", error="timeout")
        feed = db.get_feed("tech")
        assert feed["last_error"] == "timeout"

    def test_returns_plain_dicts(self, db: Database):
        _add_feed(db)
        feed = db.get_feed("tech")
        assert type(feed) is dict

    def test_delete_feed(self, db: Database):
        _add_feed(db)
        _add_episode(db)
        db.delete_feed("tech")
        assert db.get_feed("tech") is None
        # Episodes should also be gone (cascade)
        assert db.get_episode("v1", "tech") is None


# ------------------------------------------------------------------ #
# Episodes – insert & get
# ------------------------------------------------------------------ #

class TestEpisodeInsert:
    def test_insert_and_get(self, db: Database):
        _add_feed(db)
        _add_episode(db)
        ep = db.get_episode("v1", "tech")
        assert ep is not None
        assert ep["video_id"] == "v1"
        assert ep["feed_name"] == "tech"
        assert ep["title"] == "Episode 1"
        assert ep["status"] == "pending"
        assert ep["retry_count"] == 0

    def test_insert_or_ignore_skips_duplicate(self, db: Database):
        _add_feed(db)
        _add_episode(db, title="Original")
        _add_episode(db, title="Duplicate")  # same video_id + feed_name
        ep = db.get_episode("v1", "tech")
        assert ep["title"] == "Original"

    def test_get_episode_missing(self, db: Database):
        _add_feed(db)
        assert db.get_episode("nope", "tech") is None

    def test_get_episodes_by_feed_all(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1")
        _add_episode(db, "v2", title="Ep 2")
        eps = db.get_episodes_by_feed("tech")
        assert len(eps) == 2

    def test_get_episodes_by_feed_filtered(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", status="pending")
        _add_episode(db, "v2", status="done", title="Ep 2")
        eps = db.get_episodes_by_feed("tech", status="done")
        assert len(eps) == 1
        assert eps[0]["video_id"] == "v2"


# ------------------------------------------------------------------ #
# Status transitions
# ------------------------------------------------------------------ #

class TestStatusTransitions:
    def test_update_status(self, db: Database):
        _add_feed(db)
        _add_episode(db)
        db.update_episode_status("v1", "tech", "downloading")
        ep = db.get_episode("v1", "tech")
        assert ep["status"] == "downloading"

    def test_update_status_with_kwargs(self, db: Database):
        _add_feed(db)
        _add_episode(db)
        db.update_episode_status(
            "v1", "tech", "done",
            file_path="/media/v1.mp4",
            file_size=123456,
        )
        ep = db.get_episode("v1", "tech")
        assert ep["status"] == "done"
        assert ep["file_path"] == "/media/v1.mp4"
        assert ep["file_size"] == 123456

    def test_update_status_sets_updated_at(self, db: Database):
        _add_feed(db)
        _add_episode(db)
        before = db.get_episode("v1", "tech")["updated_at"]
        db.conn.execute(
            "UPDATE episodes SET updated_at = datetime('now', '-1 minute') WHERE video_id = 'v1'"
        )
        db.conn.commit()
        db.update_episode_status("v1", "tech", "eligible")
        after = db.get_episode("v1", "tech")["updated_at"]
        assert after >= before

    def test_update_status_error_field(self, db: Database):
        _add_feed(db)
        _add_episode(db)
        db.update_episode_status("v1", "tech", "failed", error="download timeout")
        ep = db.get_episode("v1", "tech")
        assert ep["error"] == "download timeout"

    def test_update_llm_fields(self, db: Database):
        _add_feed(db)
        _add_episode(db)
        db.update_episode_status(
            "v1", "tech", "done",
            llm_trim_status="done",
            llm_segments_json='{"segments": []}',
            llm_cuts_applied=3,
        )
        ep = db.get_episode("v1", "tech")
        assert ep["llm_trim_status"] == "done"
        assert ep["llm_segments_json"] == '{"segments": []}'
        assert ep["llm_cuts_applied"] == 3


# ------------------------------------------------------------------ #
# Done episodes ordering
# ------------------------------------------------------------------ #

class TestDoneEpisodes:
    def test_get_done_episodes_ordered_desc(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", upload_date="2025-01-01", status="done")
        _add_episode(db, "v2", upload_date="2025-06-15", status="done", title="Ep 2")
        _add_episode(db, "v3", upload_date="2025-03-10", status="done", title="Ep 3")
        eps = db.get_done_episodes_by_feed("tech")
        dates = [e["upload_date"] for e in eps]
        assert dates == ["2025-06-15", "2025-03-10", "2025-01-01"]

    def test_get_done_episodes_excludes_other_statuses(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", status="done")
        _add_episode(db, "v2", status="pending", title="Ep 2")
        eps = db.get_done_episodes_by_feed("tech")
        assert len(eps) == 1
        assert eps[0]["video_id"] == "v1"


# ------------------------------------------------------------------ #
# Eligible episodes
# ------------------------------------------------------------------ #

class TestEligibleEpisodes:
    def test_get_eligible_pending_past(self, db: Database):
        """Pending episodes whose eligible_at is in the past are returned."""
        _add_feed(db)
        _add_episode(db, "v1", eligible_at="2020-01-01T00:00:00")
        eps = db.get_eligible_episodes(10)
        assert len(eps) == 1

    def test_get_eligible_pending_future_excluded(self, db: Database):
        """Pending episodes whose eligible_at is in the future are not returned."""
        _add_feed(db)
        _add_episode(db, "v1", eligible_at="2099-01-01T00:00:00")
        eps = db.get_eligible_episodes(10)
        assert len(eps) == 0

    def test_get_eligible_includes_eligible_status(self, db: Database):
        """Episodes already marked 'eligible' are always returned."""
        _add_feed(db)
        _add_episode(db, "v1", status="eligible", eligible_at="2099-12-31T00:00:00")
        eps = db.get_eligible_episodes(10)
        assert len(eps) == 1

    def test_get_eligible_limit(self, db: Database):
        _add_feed(db)
        for i in range(5):
            _add_episode(
                db, f"v{i}", eligible_at="2020-01-01T00:00:00", title=f"Ep {i}"
            )
        eps = db.get_eligible_episodes(2)
        assert len(eps) == 2

    def test_get_eligible_by_feed_type(self, db: Database):
        """Filter eligible episodes by feed type."""
        _add_feed(db, "yt", "http://yt", "youtube")
        _add_feed(db, "pod", "http://pod", "podcast")
        _add_episode(db, "v1", feed_name="yt", eligible_at="2020-01-01T00:00:00")
        _add_episode(db, "v2", feed_name="pod", eligible_at="2020-01-01T00:00:00", title="Pod Ep")

        yt_eps = db.get_eligible_episodes(10, feed_type="youtube")
        pod_eps = db.get_eligible_episodes(10, feed_type="podcast")
        all_eps = db.get_eligible_episodes(10)

        assert len(yt_eps) == 1
        assert yt_eps[0]["video_id"] == "v1"
        assert len(pod_eps) == 1
        assert pod_eps[0]["video_id"] == "v2"
        assert len(all_eps) == 2


# ------------------------------------------------------------------ #
# Promote eligible
# ------------------------------------------------------------------ #

class TestPromoteEligible:
    def test_promote_eligible_episodes(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", eligible_at="2020-01-01T00:00:00")
        _add_episode(db, "v2", eligible_at="2099-01-01T00:00:00", title="Ep 2")

        db.promote_eligible_episodes()

        ep1 = db.get_episode("v1", "tech")
        ep2 = db.get_episode("v2", "tech")
        assert ep1["status"] == "eligible"
        assert ep2["status"] == "pending"  # still in the future

    def test_promote_ignores_non_pending(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", eligible_at="2020-01-01T00:00:00", status="done")
        db.promote_eligible_episodes()
        ep = db.get_episode("v1", "tech")
        assert ep["status"] == "done"


# ------------------------------------------------------------------ #
# Disk usage
# ------------------------------------------------------------------ #

class TestRecentDownloadCount:
    def test_count_recent_done(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", status="done")
        db.update_episode_status("v1", "tech", "done", file_size=1000)
        # Just completed, should count
        assert db.get_recent_download_count(hours=1) >= 1

    def test_count_excludes_non_done(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", status="pending")
        assert db.get_recent_download_count(hours=1) == 0

    def test_count_empty(self, db: Database):
        assert db.get_recent_download_count(hours=1) == 0

    def test_count_by_feed_type(self, db: Database):
        _add_feed(db, "yt", "http://yt", "youtube")
        _add_feed(db, "pod", "http://pod", "podcast")
        _add_episode(db, "v1", feed_name="yt", status="done")
        _add_episode(db, "v2", feed_name="pod", status="done", title="Pod Ep")
        db.update_episode_status("v1", "yt", "done", file_size=1000)
        db.update_episode_status("v2", "pod", "done", file_size=500)

        yt_count = db.get_recent_download_count(hours=1, feed_type="youtube")
        pod_count = db.get_recent_download_count(hours=1, feed_type="podcast")
        total = db.get_recent_download_count(hours=1)

        assert yt_count >= 1
        assert pod_count >= 1
        assert total >= 2


class TestDiskUsage:
    def test_disk_usage_sums_done(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", status="done")
        _add_episode(db, "v2", status="done", title="Ep 2")
        db.update_episode_status("v1", "tech", "done", file_size=1000)
        db.update_episode_status("v2", "tech", "done", file_size=2000)
        assert db.get_disk_usage() == 3000

    def test_disk_usage_ignores_non_done(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", status="pending")
        db.update_episode_status("v1", "tech", "pending", file_size=9999)
        assert db.get_disk_usage() == 0

    def test_disk_usage_empty(self, db: Database):
        assert db.get_disk_usage() == 0


# ------------------------------------------------------------------ #
# Reset stale downloads
# ------------------------------------------------------------------ #

class TestResetStaleDownloads:
    def test_reset_stale(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", status="downloading")
        db.conn.execute(
            "UPDATE episodes SET updated_at = datetime('now', '-7 hours') WHERE video_id = 'v1'"
        )
        db.conn.commit()

        db.reset_stale_downloads(hours=6)
        ep = db.get_episode("v1", "tech")
        assert ep["status"] == "eligible"

    def test_reset_stale_leaves_recent(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", status="downloading")
        db.reset_stale_downloads(hours=6)
        ep = db.get_episode("v1", "tech")
        assert ep["status"] == "downloading"

    def test_reset_stale_ignores_other_statuses(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", status="pending")
        db.conn.execute(
            "UPDATE episodes SET updated_at = datetime('now', '-7 hours') WHERE video_id = 'v1'"
        )
        db.conn.commit()
        db.reset_stale_downloads(hours=6)
        ep = db.get_episode("v1", "tech")
        assert ep["status"] == "pending"


# ------------------------------------------------------------------ #
# Retry failed episodes
# ------------------------------------------------------------------ #

class TestRetryFailed:
    def test_retry_below_max(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", status="failed")
        db.update_episode_status("v1", "tech", "failed", retry_count=2)
        db.retry_failed_episodes(max_retries=3)
        ep = db.get_episode("v1", "tech")
        assert ep["status"] == "eligible"

    def test_retry_at_max_stays_failed(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", status="failed")
        db.update_episode_status("v1", "tech", "failed", retry_count=3)
        db.retry_failed_episodes(max_retries=3)
        ep = db.get_episode("v1", "tech")
        assert ep["status"] == "failed"

    def test_retry_ignores_non_failed(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", status="pending")
        db.retry_failed_episodes(max_retries=3)
        ep = db.get_episode("v1", "tech")
        assert ep["status"] == "pending"


# ------------------------------------------------------------------ #
# Oldest done episodes (for pruning)
# ------------------------------------------------------------------ #

class TestOldestDone:
    def test_oldest_done_ordered_asc(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", upload_date="2025-06-01", status="done")
        _add_episode(db, "v2", upload_date="2025-01-01", status="done", title="Ep 2")
        _add_episode(db, "v3", upload_date="2025-03-01", status="done", title="Ep 3")
        eps = db.get_oldest_done_episodes(10)
        dates = [e["upload_date"] for e in eps]
        assert dates == ["2025-01-01", "2025-03-01", "2025-06-01"]

    def test_oldest_done_limit(self, db: Database):
        _add_feed(db)
        for i in range(5):
            _add_episode(
                db, f"v{i}", upload_date=f"2025-0{i+1}-01", status="done", title=f"Ep {i}"
            )
        eps = db.get_oldest_done_episodes(2)
        assert len(eps) == 2


# ------------------------------------------------------------------ #
# Feed episode count
# ------------------------------------------------------------------ #

class TestFeedEpisodeCount:
    def test_count_done(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1", status="done")
        _add_episode(db, "v2", status="done", title="Ep 2")
        _add_episode(db, "v3", status="pending", title="Ep 3")
        assert db.get_feed_episode_count("tech") == 2

    def test_count_empty(self, db: Database):
        _add_feed(db)
        assert db.get_feed_episode_count("tech") == 0


# ------------------------------------------------------------------ #
# Delete episodes by feed
# ------------------------------------------------------------------ #

class TestDeleteEpisodes:
    def test_delete_episodes_by_feed(self, db: Database):
        _add_feed(db)
        _add_episode(db, "v1")
        _add_episode(db, "v2", title="Ep 2")
        count = db.delete_episodes_by_feed("tech")
        assert count == 2
        assert db.get_episodes_by_feed("tech") == []

    def test_delete_episodes_empty_feed(self, db: Database):
        _add_feed(db)
        count = db.delete_episodes_by_feed("tech")
        assert count == 0


# ------------------------------------------------------------------ #
# In-memory database
# ------------------------------------------------------------------ #

class TestInMemory:
    def test_memory_db(self):
        db = Database(":memory:")
        _add_feed(db, "mem", "http://mem")
        assert db.get_feed("mem") is not None
        db.close()
