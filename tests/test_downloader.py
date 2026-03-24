"""Tests for siphon.downloader — yt-dlp is always mocked."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from siphon.config import CookiesConfig, ResolvedFeed
from siphon.downloader import (
    build_download_opts,
    build_extract_opts,
    download_video,
    extract_feed_metadata,
    find_downloaded_file,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def cookies() -> CookiesConfig:
    return CookiesConfig(source="browser", browser="firefox")


@pytest.fixture()
def video_feed() -> ResolvedFeed:
    return ResolvedFeed(
        name="testfeed",
        url="https://www.youtube.com/feeds/videos.xml?channel_id=TEST",
        type="youtube",
        mode="video",
        quality=1080,
        sponsorblock=False,
        sponsorblock_categories=["sponsor"],
        sponsorblock_delay_minutes=1440,

        block_shorts=True,
        min_duration_seconds=60,
        date_cutoff=None,
        title_exclude=[],
        llm_trim=False,
    )


@pytest.fixture()
def audio_feed() -> ResolvedFeed:
    return ResolvedFeed(
        name="audiofeed",
        url="https://www.youtube.com/feeds/videos.xml?channel_id=AUDIO",
        type="youtube",
        mode="audio",
        quality=1440,
        sponsorblock=False,
        sponsorblock_categories=["sponsor"],
        sponsorblock_delay_minutes=1440,

        block_shorts=True,
        min_duration_seconds=60,
        date_cutoff=None,
        title_exclude=[],
        llm_trim=False,
    )


@pytest.fixture()
def sponsorblock_feed() -> ResolvedFeed:
    return ResolvedFeed(
        name="sbfeed",
        url="https://www.youtube.com/feeds/videos.xml?channel_id=SB",
        type="youtube",
        mode="video",
        quality=720,
        sponsorblock=True,
        sponsorblock_categories=["sponsor", "selfpromo"],
        sponsorblock_delay_minutes=1440,

        block_shorts=True,
        min_duration_seconds=60,
        date_cutoff=None,
        title_exclude=[],
        llm_trim=False,
    )


@pytest.fixture()
def max_quality_feed() -> ResolvedFeed:
    return ResolvedFeed(
        name="maxfeed",
        url="https://www.youtube.com/feeds/videos.xml?channel_id=MAX",
        type="youtube",
        mode="video",
        quality="max",
        sponsorblock=False,
        sponsorblock_categories=["sponsor"],
        sponsorblock_delay_minutes=1440,

        block_shorts=True,
        min_duration_seconds=60,
        date_cutoff=None,
        title_exclude=[],
        llm_trim=False,
    )


# ---------------------------------------------------------------------------
# build_extract_opts
# ---------------------------------------------------------------------------

class TestBuildExtractOpts:
    def test_returns_correct_structure(self, cookies: CookiesConfig) -> None:
        opts = build_extract_opts(cookies)
        assert opts["extract_flat"] is True
        assert opts["quiet"] is True
        assert opts["no_warnings"] is True
        assert opts["cookiesfrombrowser"] == ("firefox",)


# ---------------------------------------------------------------------------
# build_download_opts — video mode
# ---------------------------------------------------------------------------

class TestBuildDownloadOptsVideo:
    def test_format_string(
        self, video_feed: ResolvedFeed, cookies: CookiesConfig
    ) -> None:
        opts = build_download_opts(video_feed, cookies, "/tmp/dl")
        assert opts["format"] == "bestvideo[height<=1080]+bestaudio/best"

    def test_format_string_max_quality(
        self, max_quality_feed: ResolvedFeed, cookies: CookiesConfig
    ) -> None:
        opts = build_download_opts(max_quality_feed, cookies, "/tmp/dl")
        assert opts["format"] == "bestvideo+bestaudio/best"

    def test_merge_output_format(
        self, video_feed: ResolvedFeed, cookies: CookiesConfig
    ) -> None:
        opts = build_download_opts(video_feed, cookies, "/tmp/dl")
        assert opts["merge_output_format"] == "mp4"

    def test_outtmpl(
        self, video_feed: ResolvedFeed, cookies: CookiesConfig
    ) -> None:
        opts = build_download_opts(video_feed, cookies, "/tmp/dl")
        assert opts["outtmpl"] == "/tmp/dl/testfeed/%(id)s.%(ext)s"

    def test_common_options(
        self, video_feed: ResolvedFeed, cookies: CookiesConfig
    ) -> None:
        opts = build_download_opts(video_feed, cookies, "/tmp/dl")
        assert opts["quiet"] is True
        assert opts["no_warnings"] is True
        assert opts["writethumbnail"] is True
        assert opts["cookiesfrombrowser"] == ("firefox",)

    def test_metadata_and_thumbnail_postprocessors(
        self, video_feed: ResolvedFeed, cookies: CookiesConfig
    ) -> None:
        opts = build_download_opts(video_feed, cookies, "/tmp/dl")
        keys = [pp["key"] for pp in opts["postprocessors"]]
        assert "FFmpegMetadata" in keys
        assert "EmbedThumbnail" in keys


# ---------------------------------------------------------------------------
# build_download_opts — audio mode
# ---------------------------------------------------------------------------

class TestBuildDownloadOptsAudio:
    def test_format_string(
        self, audio_feed: ResolvedFeed, cookies: CookiesConfig
    ) -> None:
        opts = build_download_opts(audio_feed, cookies, "/tmp/dl")
        assert opts["format"] == "bestaudio/best"

    def test_no_merge_output_format(
        self, audio_feed: ResolvedFeed, cookies: CookiesConfig
    ) -> None:
        opts = build_download_opts(audio_feed, cookies, "/tmp/dl")
        assert "merge_output_format" not in opts

    def test_extract_audio_postprocessor(
        self, audio_feed: ResolvedFeed, cookies: CookiesConfig
    ) -> None:
        opts = build_download_opts(audio_feed, cookies, "/tmp/dl")
        pp_keys = [pp["key"] for pp in opts["postprocessors"]]
        assert "FFmpegExtractAudio" in pp_keys
        extract = next(
            pp for pp in opts["postprocessors"]
            if pp["key"] == "FFmpegExtractAudio"
        )
        assert extract["preferredcodec"] == "mp3"
        assert extract["preferredquality"] == "192"


# ---------------------------------------------------------------------------
# build_download_opts — sponsorblock
# ---------------------------------------------------------------------------

class TestBuildDownloadOptsSponsorblock:
    def test_sponsorblock_enabled(
        self, sponsorblock_feed: ResolvedFeed, cookies: CookiesConfig
    ) -> None:
        opts = build_download_opts(sponsorblock_feed, cookies, "/tmp/dl")
        pp_keys = [pp["key"] for pp in opts["postprocessors"]]
        assert "SponsorBlock" in pp_keys
        assert "ModifyChapters" in pp_keys

        sb = next(
            pp for pp in opts["postprocessors"] if pp["key"] == "SponsorBlock"
        )
        assert sb["categories"] == ["sponsor", "selfpromo"]

        mc = next(
            pp for pp in opts["postprocessors"]
            if pp["key"] == "ModifyChapters"
        )
        assert mc["remove_sponsor_segments"] == ["sponsor", "selfpromo"]
        assert mc["force_keyframes"] is True

    def test_sponsorblock_disabled(
        self, video_feed: ResolvedFeed, cookies: CookiesConfig
    ) -> None:
        opts = build_download_opts(video_feed, cookies, "/tmp/dl")
        pp_keys = [pp["key"] for pp in opts["postprocessors"]]
        assert "SponsorBlock" not in pp_keys
        assert "ModifyChapters" not in pp_keys


# ---------------------------------------------------------------------------
# extract_feed_metadata (mocked)
# ---------------------------------------------------------------------------

class TestExtractFeedMetadata:
    @patch("siphon.downloader.yt_dlp.YoutubeDL")
    def test_calls_extract_info(
        self, mock_ydl_cls: MagicMock, cookies: CookiesConfig
    ) -> None:
        mock_ydl = MagicMock()
        mock_ydl_cls.return_value.__enter__ = MagicMock(return_value=mock_ydl)
        mock_ydl_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_ydl.extract_info.return_value = {"title": "Test Playlist"}

        result = extract_feed_metadata("https://example.com/playlist", cookies)

        mock_ydl.extract_info.assert_called_once_with(
            "https://example.com/playlist", download=False
        )
        assert result == {"title": "Test Playlist"}

    @patch("siphon.downloader.yt_dlp.YoutubeDL")
    def test_wraps_download_error(
        self, mock_ydl_cls: MagicMock, cookies: CookiesConfig
    ) -> None:
        import yt_dlp.utils

        mock_ydl = MagicMock()
        mock_ydl_cls.return_value.__enter__ = MagicMock(return_value=mock_ydl)
        mock_ydl_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_ydl.extract_info.side_effect = yt_dlp.utils.DownloadError(
            "network error"
        )

        with pytest.raises(Exception, match="network error"):
            extract_feed_metadata("https://example.com/bad", cookies)


# ---------------------------------------------------------------------------
# download_video (mocked)
# ---------------------------------------------------------------------------

class TestDownloadVideo:
    @patch("siphon.downloader.yt_dlp.YoutubeDL")
    def test_calls_extract_info_with_download(
        self,
        mock_ydl_cls: MagicMock,
        video_feed: ResolvedFeed,
        cookies: CookiesConfig,
    ) -> None:
        mock_ydl = MagicMock()
        mock_ydl_cls.return_value.__enter__ = MagicMock(return_value=mock_ydl)
        mock_ydl_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_ydl.extract_info.return_value = {"id": "abc123", "title": "Vid"}

        result = download_video(
            "https://youtu.be/abc123", video_feed, cookies, "/tmp/dl"
        )

        mock_ydl.extract_info.assert_called_once_with(
            "https://youtu.be/abc123", download=True
        )
        assert result["id"] == "abc123"

    @patch("siphon.downloader.yt_dlp.YoutubeDL")
    def test_wraps_download_error(
        self,
        mock_ydl_cls: MagicMock,
        video_feed: ResolvedFeed,
        cookies: CookiesConfig,
    ) -> None:
        import yt_dlp.utils

        mock_ydl = MagicMock()
        mock_ydl_cls.return_value.__enter__ = MagicMock(return_value=mock_ydl)
        mock_ydl_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_ydl.extract_info.side_effect = yt_dlp.utils.DownloadError(
            "download failed"
        )

        with pytest.raises(Exception, match="download failed"):
            download_video(
                "https://youtu.be/bad", video_feed, cookies, "/tmp/dl"
            )


# ---------------------------------------------------------------------------
# find_downloaded_file
# ---------------------------------------------------------------------------

class TestFindDownloadedFile:
    def test_finds_existing_file(self, tmp_path) -> None:
        feed_dir = tmp_path / "myfeed"
        feed_dir.mkdir()
        video_file = feed_dir / "abc123.mp4"
        video_file.write_bytes(b"x" * 1024)

        result = find_downloaded_file(str(tmp_path), "myfeed", "abc123")

        assert result is not None
        path, size = result
        assert path == str(video_file)
        assert size == 1024

    def test_returns_none_when_no_file(self, tmp_path) -> None:
        feed_dir = tmp_path / "myfeed"
        feed_dir.mkdir()

        result = find_downloaded_file(str(tmp_path), "myfeed", "nonexistent")

        assert result is None

    def test_finds_mp3_file(self, tmp_path) -> None:
        feed_dir = tmp_path / "audiofeed"
        feed_dir.mkdir()
        audio_file = feed_dir / "xyz789.mp3"
        audio_file.write_bytes(b"a" * 512)

        result = find_downloaded_file(str(tmp_path), "audiofeed", "xyz789")

        assert result is not None
        path, size = result
        assert path == str(audio_file)
        assert size == 512
