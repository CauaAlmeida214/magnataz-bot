from __future__ import absolute_import
"""
Bot-facing signal generation wrapper.

The trading engine only generates candidates. Telegram delivery stays in the
bot/orchestrator layer.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from obcash3.utils.logger import get_logger
from obcash3.utils.time import next_candle_start, now_br

logger = get_logger(__name__)

ENTRY_ACTIONS = {"COMPRA", "VENDA"}


@dataclass(frozen=True)
class FreeWindowSignalCandidate:
    signal: Any
    technical_score: float
    ml_score: float
    score_final: float


class BotSignalEngine:
    """Orchestrates the global scanner and returns one FREE-window candidate."""

    def __init__(self, service, timeframe: str = "1m"):
        self.service = service
        self.timeframe = str(timeframe or "1m")

    async def generate_signal_for_window(self, window: str) -> Optional[FreeWindowSignalCandidate]:
        """Scan the market and return the best qualified candidate for the current FREE window."""
        result = await self.service.scan_all_pairs(self.timeframe, send_notifications=False)
        signal = getattr(result, "best_signal", None)
        if signal is None:
            logger.info("FREE window %s: no qualified signal found", window)
            return None

        action = str(getattr(signal, "action", "") or "").upper()
        final_score = float(getattr(signal, "decision_score", 0.0) or getattr(signal, "score", 0.0) or 0.0)
        if action not in ENTRY_ACTIONS:
            logger.info("FREE window %s: candidate discarded because action=%s", window, action or "N/A")
            return None

        logger.info(
            "FREE window %s candidate -> %s %s tech=%.1f ml=%.1f final=%.1f",
            window,
            getattr(signal, "asset", "-"),
            action,
            float(getattr(signal, "technical_score", 0.0) or getattr(signal, "score", 0.0) or 0.0),
            float(getattr(signal, "ml_score", 0.0) or 0.0),
            final_score,
        )
        return FreeWindowSignalCandidate(
            signal=signal,
            technical_score=float(getattr(signal, "technical_score", 0.0) or getattr(signal, "score", 0.0) or 0.0),
            ml_score=float(getattr(signal, "ml_score", 0.0) or 0.0),
            score_final=final_score,
        )

    @staticmethod
    def build_entry_time(signal: Any) -> datetime:
        timestamp = getattr(signal, "timestamp", None)
        interval = getattr(signal, "interval", "1m")
        try:
            return next_candle_start(str(interval), base_time=timestamp)
        except Exception:
            return now_br()
