"""FastAPI application factory with lifespan, auth, and scheduler."""
import asyncio
import logging
import secrets
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from siphon.config import SiphonConfig
from siphon.db import Database

logger = logging.getLogger(__name__)
security = HTTPBasic()


def verify_credentials(config: SiphonConfig):
    """Return a FastAPI dependency that checks HTTP Basic credentials."""
    def _check(credentials: HTTPBasicCredentials = Depends(security)):
        correct_user = secrets.compare_digest(credentials.username, config.auth.username)
        correct_pass = secrets.compare_digest(credentials.password, config.auth.password)
        if not (correct_user and correct_pass):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials",
                headers={"WWW-Authenticate": "Basic"},
            )
        return credentials
    return _check


def create_app(config: SiphonConfig) -> FastAPI:
    """Build and return the FastAPI application."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup
        db = Database(config.storage.database)
        app.state.db = db
        app.state.config = config

        # Sync feeds from config to DB
        for feed in config.feeds:
            db.upsert_feed(feed.name, feed.url, feed.type)

        # Start scheduler
        scheduler = None
        try:
            from apscheduler.schedulers.asyncio import AsyncIOScheduler
            from siphon.pipeline import check_feeds, process_downloads

            scheduler = AsyncIOScheduler()
            scheduler.add_job(
                check_feeds, 'interval',
                minutes=config.schedule.check_interval_minutes,
                args=[config, db],
                id='check_feeds',
                name='Check feeds',
            )
            dl_interval = min(
                config.schedule.youtube_download_interval_minutes,
                config.schedule.podcast_download_interval_minutes,
            )
            scheduler.add_job(
                process_downloads, 'interval',
                minutes=dl_interval,
                args=[config, db],
                id='process_downloads',
                name='Process downloads',
            )
            scheduler.start()
            app.state.scheduler = scheduler
            logger.info("Scheduler started")
        except Exception as e:
            logger.warning(f"Scheduler failed to start: {e}")

        yield

        # Shutdown
        if scheduler:
            scheduler.shutdown(wait=False)
        db.close()

    auth_dep = verify_credentials(config)

    app = FastAPI(
        title="Siphon",
        description="Self-hosted YouTube-to-podcast bridge",
        lifespan=lifespan,
    )

    from siphon.routes.feeds import router as feeds_router
    from siphon.routes.media import router as media_router
    from siphon.routes.api import router as api_router
    from siphon.routes.ui import router as ui_router

    # RSS, media, and API routes require auth
    app.include_router(feeds_router, dependencies=[Depends(auth_dep)])
    app.include_router(media_router, dependencies=[Depends(auth_dep)])
    app.include_router(api_router, dependencies=[Depends(auth_dep)])

    # Web UI does NOT require auth — local/Tailscale access only
    app.include_router(ui_router)

    return app
