from __future__ import absolute_import
"""
API route handlers.
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException

from obcash3.api.models import (
    SignalRequest, SignalResponse,
    ScanAllRequest, ScanAllResponse,
    BacktestRequest, BacktestResponse,
    StatsResponse, HealthResponse,
    ConfigUpdate, WebhookSignal
)
from obcash3.api.services import OBCCashService
from obcash3.utils.logger import get_logger

logger = get_logger(__name__)

# Global service instance (singleton)
_service: Optional[OBCCashService] = None


def get_service() -> OBCCashService:
    """Get or create service instance."""
    global _service
    if _service is None:
        _service = OBCCashService()
    return _service


# Create router
router = APIRouter(prefix="/api/v1", tags=["obcash3"])


@router.get("/health", response_model=HealthResponse)
async def health_check(service: OBCCashService = Depends(get_service)):
    """Health check endpoint."""
    from obcash3 import __version__

    # Check dependencies
    deps = {
        "numpy": True,
        "pandas": True,
        "customtkinter": True,
        "matplotlib": True,
        "pystray": True,
        "yfinance": True
    }

    try:
        import numpy
    except ImportError:
        deps["numpy"] = False

    try:
        import pandas
    except ImportError:
        deps["pandas"] = False

    # ... check other deps

    return HealthResponse(
        status="healthy" if all(deps.values()) else "degraded",
        timestamp=datetime.now(),
        version=__version__,
        dependencies=deps
    )


@router.post("/signal", response_model=SignalResponse)
async def analyze_signal(
    request: SignalRequest,
    service: OBCCashService = Depends(get_service)
):
    """
    Analyze a single pair and return trading signal.

    Returns signal with action, strength, score, and all indicators.
    """
    try:
        result = await service.analyze_pair(
            pair=request.pair,
            timeframe=request.timeframe,
            send_notification=False
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Error analyzing %s: %s", request.pair, e)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/scan-all", response_model=ScanAllResponse)
async def scan_all_pairs(
    request: ScanAllRequest,
    service: OBCCashService = Depends(get_service)
):
    """
    Scan all available pairs for signals.

    This can take several minutes depending on number of pairs.
    """
    try:
        result = await service.scan_all_pairs(
            timeframe=request.timeframe,
            send_notifications=request.send_telegram
        )
        return result
    except Exception as e:
        logger.error("Error scanning all pairs: %s", e)
        raise HTTPException(status_code=500, detail="Scan failed")


@router.post("/backtest", response_model=BacktestResponse)
async def run_backtest(
    request: BacktestRequest,
    service: OBCCashService = Depends(get_service)
):
    """
    Run backtest for a specific pair.

    Analyzes historical data to evaluate strategy performance.
    """
    try:
        result = await service.run_backtest(
            pair=request.pair,
            timeframe=request.timeframe,
            initial_balance=request.initial_balance,
            risk_percent=request.risk_percent
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Backtest error for %s: %s", request.pair, e)
        raise HTTPException(status_code=500, detail="Backtest failed")


@router.get("/stats", response_model=StatsResponse)
async def get_statistics(
    service: OBCCashService = Depends(get_service)
):
    """Get application statistics."""
    try:
        stats = service.get_stats()
        return stats
    except Exception as e:
        logger.error("Error getting stats: %s", e)
        raise HTTPException(status_code=500, detail="Failed to get stats")


@router.get("/config")
async def get_config(service: OBCCashService = Depends(get_service)):
    """Get current configuration (without sensitive keys)."""
    config = service.get_config_dict()
    # Mask sensitive keys
    if config.get('twelve_api_key'):
        config['twelve_api_key'] = '***' if config['twelve_api_key'] else ''
    if config.get('av_api_key'):
        config['av_api_key'] = '***' if config['av_api_key'] else ''
    if config.get('telegram_token'):
        config['telegram_token'] = '***' if config['telegram_token'] else ''
    if config.get('telegram_chat_id'):
        config['telegram_chat_id'] = '***' if config['telegram_chat_id'] else ''
    if config.get('free_telegram_chat_id'):
        config['free_telegram_chat_id'] = '***' if config['free_telegram_chat_id'] else ''
    if config.get('vip_telegram_chat_id'):
        config['vip_telegram_chat_id'] = '***' if config['vip_telegram_chat_id'] else ''
    return config


@router.post("/config")
async def update_config(
    update: ConfigUpdate,
    service: OBCCashService = Depends(get_service)
):
    """Update configuration."""
    update_dict = update.model_dump(exclude_unset=True)

    # Don't allow setting API keys via API if they're masked
    if 'twelve_api_key' in update_dict and update_dict['twelve_api_key'] == '***':
        update_dict.pop('twelve_api_key')
    if 'av_api_key' in update_dict and update_dict['av_api_key'] == '***':
        update_dict.pop('av_api_key')
    if 'telegram_token' in update_dict and update_dict['telegram_token'] == '***':
        update_dict.pop('telegram_token')
    if 'telegram_chat_id' in update_dict and update_dict['telegram_chat_id'] == '***':
        update_dict.pop('telegram_chat_id')
    if 'free_telegram_chat_id' in update_dict and update_dict['free_telegram_chat_id'] == '***':
        update_dict.pop('free_telegram_chat_id')
    if 'vip_telegram_chat_id' in update_dict and update_dict['vip_telegram_chat_id'] == '***':
        update_dict.pop('vip_telegram_chat_id')

    success = service.update_config(**update_dict)
    if not success:
        raise HTTPException(status_code=400, detail="Invalid configuration")

    return {"status": "ok", "message": "Configuration updated"}


@router.post("/webhook")
async def receive_webhook(
    payload: WebhookSignal,
    service: OBCCashService = Depends(get_service)
):
    """
    Receive signal via webhook from external system.

    Can be used to integrate with other platforms.
    """
    # Verify signature if secret provided
    # TODO: Implement signature verification

    # Process webhook (log, forward, etc.)
    logger.info("Webhook received: %s %s", payload.signal.action, payload.signal.asset)

    # Optionally send to external URL
    if payload.webhook_url:
        # Forward to external URL
        pass

    return {"status": "received"}


# Cleanup on shutdown would be handled by FastAPI lifespan events
