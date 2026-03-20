"""SQLite database layer for Siphon – plain sqlite3, no ORM."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

SCHEMA = """\
CREATE TABLE IF NOT EXISTS feeds (
    name            TEXT PRIMARY KEY,
    url             TEXT NOT NULL,
    feed_type       TEXT NOT NULL DEFAULT 'youtube',
    last_checked_at TEXT,
    last_error      TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS episodes (
    video_id        TEXT NOT NULL,
    feed_name       TEXT NOT NULL REFERENCES feeds(name) ON DELETE CASCADE,
    title           TEXT NOT NULL,
    description     TEXT,
    thumbnail_url   TEXT,
    channel_name    TEXT,
    duration        INTEGER,
    upload_date     TEXT,
    discovered_at   TEXT NOT NULL DEFAULT (datetime('now')),
    eligible_at     TEXT,
    status          TEXT NOT NULL DEFAULT 'pending',
    file_path       TEXT,
    file_size       INTEGER,
    mime_type       TEXT DEFAULT 'video/mp4',
    error           TEXT,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    llm_trim_status TEXT,
    llm_segments_json TEXT,
    llm_cuts_applied INTEGER,
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (video_id, feed_name)
);

CREATE INDEX IF NOT EXISTS idx_episodes_feed_status ON episodes(feed_name, status);
CREATE INDEX IF NOT EXISTS idx_episodes_eligible ON episodes(status, eligible_at);
"""

# Migrations applied after initial schema creation
MIGRATIONS = [
    # Add feed_type column if upgrading from older schema
    "ALTER TABLE feeds ADD COLUMN feed_type TEXT NOT NULL DEFAULT 'youtube'",
    # Add LLM columns if upgrading from older schema
    "ALTER TABLE episodes ADD COLUMN llm_trim_status TEXT",
    "ALTER TABLE episodes ADD COLUMN llm_segments_json TEXT",
    "ALTER TABLE episodes ADD COLUMN llm_cuts_applied INTEGER",
]


class Database:
    """Thin wrapper around a single sqlite3 connection."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.conn = self._connect()
        self.conn.executescript(SCHEMA)
        self._apply_migrations()

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _apply_migrations(self) -> None:
        """Run ALTER TABLE statements, ignoring 'duplicate column' errors."""
        for sql in MIGRATIONS:
            try:
                self.conn.execute(sql)
                self.conn.commit()
            except sqlite3.OperationalError:
                pass  # column already exists

    def close(self) -> None:
        self.conn.close()

    # ------------------------------------------------------------------
    # Feed CRUD
    # ------------------------------------------------------------------

    def upsert_feed(self, name: str, url: str, feed_type: str = "youtube") -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO feeds (name, url, feed_type) VALUES (?, ?, ?)",
            (name, url, feed_type),
        )
        self.conn.commit()

    def get_feed(self, name: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM feeds WHERE name = ?", (name,)
        ).fetchone()
        return dict(row) if row else None

    def get_all_feeds(self) -> list[dict]:
        rows = self.conn.execute("SELECT * FROM feeds").fetchall()
        return [dict(r) for r in rows]

    def get_feeds_to_check(self, limit: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM feeds ORDER BY last_checked_at ASC NULLS FIRST LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def update_feed_checked(self, name: str, error: str | None = None) -> None:
        self.conn.execute(
            "UPDATE feeds SET last_checked_at = datetime('now'), last_error = ? WHERE name = ?",
            (error, name),
        )
        self.conn.commit()

    def delete_feed(self, name: str) -> None:
        """Delete a feed and all its episodes."""
        self.conn.execute("DELETE FROM feeds WHERE name = ?", (name,))
        self.conn.commit()

    # ------------------------------------------------------------------
    # Episode CRUD
    # ------------------------------------------------------------------

    def insert_episode(
        self,
        video_id: str,
        feed_name: str,
        title: str,
        description: str | None = None,
        thumbnail_url: str | None = None,
        channel_name: str | None = None,
        duration: int | None = None,
        upload_date: str | None = None,
        eligible_at: str | None = None,
        status: str = "pending",
    ) -> None:
        self.conn.execute(
            """INSERT OR IGNORE INTO episodes
               (video_id, feed_name, title, description, thumbnail_url,
                channel_name, duration, upload_date, eligible_at, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                video_id,
                feed_name,
                title,
                description,
                thumbnail_url,
                channel_name,
                duration,
                upload_date,
                eligible_at,
                status,
            ),
        )
        self.conn.commit()

    def get_episode(self, video_id: str, feed_name: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM episodes WHERE video_id = ? AND feed_name = ?",
            (video_id, feed_name),
        ).fetchone()
        return dict(row) if row else None

    def get_episodes_by_feed(
        self, feed_name: str, status: str | None = None
    ) -> list[dict]:
        if status is not None:
            rows = self.conn.execute(
                "SELECT * FROM episodes WHERE feed_name = ? AND status = ?",
                (feed_name, status),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM episodes WHERE feed_name = ?",
                (feed_name,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_done_episodes_by_feed(self, feed_name: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM episodes WHERE feed_name = ? AND status = 'done' ORDER BY upload_date DESC",
            (feed_name,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_eligible_episodes(
        self, limit: int, feed_type: str | None = None
    ) -> list[dict]:
        if feed_type is not None:
            rows = self.conn.execute(
                """SELECT e.* FROM episodes e
                   JOIN feeds f ON e.feed_name = f.name
                   WHERE ((e.status = 'pending' AND e.eligible_at <= datetime('now'))
                          OR e.status = 'eligible')
                     AND f.feed_type = ?
                   LIMIT ?""",
                (feed_type, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT * FROM episodes
                   WHERE (status = 'pending' AND eligible_at <= datetime('now'))
                      OR status = 'eligible'
                   LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def update_episode_status(
        self, video_id: str, feed_name: str, status: str, **kwargs: object
    ) -> None:
        sets = ["status = ?", "updated_at = datetime('now')"]
        params: list[object] = [status]

        for col, val in kwargs.items():
            sets.append(f"{col} = ?")
            params.append(val)

        params.extend([video_id, feed_name])
        sql = f"UPDATE episodes SET {', '.join(sets)} WHERE video_id = ? AND feed_name = ?"
        self.conn.execute(sql, params)
        self.conn.commit()

    def get_disk_usage(self) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(file_size), 0) AS total FROM episodes WHERE status = 'done'"
        ).fetchone()
        return int(row["total"])

    def get_oldest_done_episodes(self, limit: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM episodes WHERE status = 'done' ORDER BY upload_date ASC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_feed_episode_count(self, feed_name: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM episodes WHERE status = 'done' AND feed_name = ?",
            (feed_name,),
        ).fetchone()
        return int(row["cnt"])

    def get_recent_download_count(
        self, hours: int = 1, feed_type: str | None = None
    ) -> int:
        """Count episodes that reached 'done' status within the last *hours*."""
        if feed_type is not None:
            row = self.conn.execute(
                "SELECT COUNT(*) AS cnt FROM episodes e "
                "JOIN feeds f ON e.feed_name = f.name "
                "WHERE e.status = 'done' AND e.updated_at >= datetime('now', ?) "
                "AND f.feed_type = ?",
                (f"-{hours} hours", feed_type),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT COUNT(*) AS cnt FROM episodes "
                "WHERE status = 'done' AND updated_at >= datetime('now', ?)",
                (f"-{hours} hours",),
            ).fetchone()
        return int(row["cnt"])

    def promote_eligible_episodes(self) -> None:
        self.conn.execute(
            "UPDATE episodes SET status = 'eligible', updated_at = datetime('now') "
            "WHERE status = 'pending' AND eligible_at <= datetime('now')"
        )
        self.conn.commit()

    def reset_stale_downloads(self, hours: int = 6) -> None:
        self.conn.execute(
            "UPDATE episodes SET status = 'eligible', updated_at = datetime('now') "
            f"WHERE status = 'downloading' AND updated_at < datetime('now', '-{hours} hours')"
        )
        self.conn.commit()

    def retry_failed_episodes(self, max_retries: int = 3) -> None:
        self.conn.execute(
            "UPDATE episodes SET status = 'eligible', updated_at = datetime('now') "
            "WHERE status = 'failed' AND retry_count < ?",
            (max_retries,),
        )
        self.conn.commit()

    def delete_episodes_by_feed(self, feed_name: str) -> int:
        """Delete all episodes for a feed. Returns count deleted."""
        cursor = self.conn.execute(
            "DELETE FROM episodes WHERE feed_name = ?", (feed_name,)
        )
        self.conn.commit()
        return cursor.rowcount

    def update_feed_date_cutoff_in_episodes(
        self, feed_name: str, date_cutoff: str
    ) -> None:
        """Mark episodes older than date_cutoff as 'pruned' for a feed."""
        self.conn.execute(
            "UPDATE episodes SET status = 'pruned', updated_at = datetime('now') "
            "WHERE feed_name = ? AND status = 'done' AND upload_date < ?",
            (feed_name, date_cutoff),
        )
        self.conn.commit()
