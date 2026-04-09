from __future__ import absolute_import
"""
Daily free-group VIP promo cadence tracking.
"""

from dataclasses import dataclass
from threading import Lock
from typing import Dict, Optional

from obcash3.bot.commercial import AUTO_VIP_PROMO_EVERY_SIGNALS, FREE_SIGNAL_WINDOWS
from obcash3.utils.logger import get_logger
from obcash3.utils.time import now_br

logger = get_logger(__name__)


def _window_minutes(clock_value: str) -> int:
    hour, minute = [int(part) for part in clock_value.split(":", 1)]
    return hour * 60 + minute


def _empty_window_bucket() -> Dict[str, object]:
    return {"signals": 0, "promo_sent": False}


@dataclass(frozen=True)
class PromoWindowDecision:
    window_key: Optional[str]
    signal_count: int
    should_send_promo: bool


class FreeGroupPromoTracker:
    """Track the free-group promotional cadence by daily window."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._state = self._new_state()

    def _new_state(self, current_time=None) -> Dict[str, object]:
        current = current_time or now_br()
        state: Dict[str, object] = {"date": current.strftime("%Y-%m-%d")}
        for window_key, _, _ in FREE_SIGNAL_WINDOWS:
            state[window_key] = _empty_window_bucket()
        return state

    def reset_free_window_state_if_needed(self, current_time=None) -> None:
        current = current_time or now_br()
        date_key = current.strftime("%Y-%m-%d")
        with self._lock:
            if self._state.get("date") != date_key:
                self._state = self._new_state(current)
                logger.info("Free promo state reset for new day: %s", date_key)

    def get_current_free_window(self, current_time=None) -> Optional[str]:
        current = current_time or now_br()
        self.reset_free_window_state_if_needed(current)
        current_minutes = current.hour * 60 + current.minute
        for window_key, start_str, end_str in FREE_SIGNAL_WINDOWS:
            start_minutes = _window_minutes(start_str)
            end_minutes = _window_minutes(end_str)
            if start_minutes <= current_minutes <= end_minutes:
                logger.debug("Free promo window identified: %s", window_key)
                return window_key
        logger.debug("No active free promo window at %s", current.strftime("%H:%M"))
        return None

    def increment_free_signal_counter(self, current_time=None) -> PromoWindowDecision:
        current = current_time or now_br()
        window_key = self.get_current_free_window(current)
        if window_key is None:
            return PromoWindowDecision(window_key=None, signal_count=0, should_send_promo=False)

        with self._lock:
            bucket = self._state[window_key]
            bucket["signals"] = int(bucket.get("signals", 0)) + 1
            signal_count = int(bucket["signals"])
            promo_sent = bool(bucket.get("promo_sent", False))

        should_send = signal_count >= AUTO_VIP_PROMO_EVERY_SIGNALS and not promo_sent
        logger.info(
            "Free promo counter updated: window=%s signals=%d promo_sent=%s",
            window_key,
            signal_count,
            promo_sent,
        )
        return PromoWindowDecision(
            window_key=window_key,
            signal_count=signal_count,
            should_send_promo=should_send,
        )

    def should_send_vip_promo(self, current_time=None) -> PromoWindowDecision:
        current = current_time or now_br()
        window_key = self.get_current_free_window(current)
        if window_key is None:
            return PromoWindowDecision(window_key=None, signal_count=0, should_send_promo=False)

        with self._lock:
            bucket = self._state[window_key]
            signal_count = int(bucket.get("signals", 0))
            promo_sent = bool(bucket.get("promo_sent", False))

        return PromoWindowDecision(
            window_key=window_key,
            signal_count=signal_count,
            should_send_promo=signal_count >= AUTO_VIP_PROMO_EVERY_SIGNALS and not promo_sent,
        )

    def mark_promo_sent(self, window_key: Optional[str]) -> None:
        if not window_key:
            return
        with self._lock:
            bucket = self._state.get(window_key)
            if not isinstance(bucket, dict):
                return
            bucket["promo_sent"] = True
        logger.info("Free VIP promo marked as sent for window=%s", window_key)

    def get_window_state(self, window_key: Optional[str]) -> Dict[str, object]:
        if not window_key:
            return _empty_window_bucket()
        with self._lock:
            bucket = self._state.get(window_key, _empty_window_bucket())
            return {
                "signals": int(bucket.get("signals", 0)),
                "promo_sent": bool(bucket.get("promo_sent", False)),
            }

    def snapshot(self) -> Dict[str, object]:
        with self._lock:
            state: Dict[str, object] = {"date": self._state.get("date")}
            for window_key, _, _ in FREE_SIGNAL_WINDOWS:
                bucket = self._state.get(window_key, _empty_window_bucket())
                state[window_key] = {
                    "signals": int(bucket.get("signals", 0)),
                    "promo_sent": bool(bucket.get("promo_sent", False)),
                }
            return state
