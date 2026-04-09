from __future__ import absolute_import
"""FastAPI server for OB CASH 3.0."""

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

from obcash3.api.routers import router as api_router
from obcash3.api.services import OBCCashService
from obcash3.utils.logger import get_logger
from obcash3 import __version__

logger = get_logger(__name__)

# Global service
_service: Optional[OBCCashService] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - startup and shutdown."""
    global _service
    logger.info("Starting OB CASH 3.0 API...")
    _service = OBCCashService()
    logger.info("Service initialized")
    yield
    logger.info("Shutting down...")
    if _service:
        _service.shutdown()
    logger.info("Shutdown complete")


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    app = FastAPI(
        title="OB CASH 3.0 API",
        description="Trading signal generation API with Telegram bot integration",
        version=__version__,
        lifespan=lifespan
    )

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # TODO: Restrict in production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include API router
    app.include_router(api_router)

    # Root endpoint
    @app.get("/")
    async def root():
        return {
            "name": "OB CASH 3.0 API",
            "version": __version__,
            "status": "running",
            "docs": "/docs",
            "api": "/api/v1"
        }

    # Error handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error("Unhandled exception: %s", exc, exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error", "error": str(exc)}
        )

    return app


def run_server(host: str = "0.0.0.0", port: int = 8000, reload: bool = False):
    """Run the API server."""
    app = create_app()
    logger.info("Starting server on %s:%s", host, port)
    uvicorn.run(
        "obcash3.api.server:create_app" if reload else app,
        host=host,
        port=port,
        reload=reload,
        log_level="info"
    )


if __name__ == "__main__":
    run_server()
