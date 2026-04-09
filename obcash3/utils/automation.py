from __future__ import absolute_import
"""
Automation helpers for premium notifications and operational protection.
"""

import threading
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional

from obcash3.data.signal_store import refresh_pending_signal_results
from obcash3.utils.dashboard import build_dashboard_metrics
from obcash3.utils.history import SignalHistoryStore, evaluate_operational_pause
from obcash3.utils.logger import get_logger
from obcash3.utils.time import now_br

logger = get_logger(__name__)


class PendingResultResolver:
    """Background resolver that closes pending signals while the app stays open."""

    def __init__(
        self,
        history_store: SignalHistoryStore,
        fetcher: Any,
        config_supplier: Callable[[], Any],
        interval_seconds: int = 30,
    ):
        self.history_store = history_store
        self.fetcher = fetcher
        self.config_supplier = config_supplier
        self.interval_seconds = max(15, int(interval_seconds))
        self._lock = threading.Lock()
        self._timer: Optional[threading.Timer] = None
        self._running = False

    def start(self) -> None:
        with self._lock:
            self._running = True
        self.reschedule()

    def stop(self) -> None:
        with self._lock:
            self._running = False
            if self._timer is not None:
                try:
                    self._timer.cancel()
                except Exception:
                    pass
                self._timer = None

    def reschedule(self) -> None:
        with self._lock:
            if not self._running:
                return
            if self._timer is not None:
                try:
                    self._timer.cancel()
                except Exception:
                    pass
            timer = threading.Timer(self.interval_seconds, self._run_cycle)
            timer.daemon = True
            self._timer = timer
            timer.start()

    def _run_cycle(self) -> None:
        try:
            config = self.config_supplier()
            refresh_pending_signal_results(
                fetcher=self.fetcher,
                config=config,
                store=self.history_store,
            )
        except Exception as exc:
            logger.error("Pending result resolver failed: %s", exc, exc_info=True)
        finally:
            self.reschedule()


class SignalResultMonitor:
    """
    Deprecated placeholder kept for compatibility.

    Automatic WIN/LOSS messaging is intentionally disabled. Results are updated
    manually through the history layer.
    """

    def __init__(self, *args, **kwargs):
        self.enabled = False

    def schedule(self, signal: Any) -> None:  # pragma: no cover - compatibility no-op
        return

    def stop(self) -> None:  # pragma: no cover - compatibility no-op
        return


class PremiumAutomationManager:
    """Manage pause protection and social proof based on persisted manual results."""

    def __init__(
        self,
        history_store: SignalHistoryStore,
        notifier: Any,
        config_supplier: Callable[[], Any],
    ):
        self.history_store = history_store
        self.notifier = notifier
        self.config_supplier = config_supplier
        self._lock = threading.Lock()
        self._recent_events: Dict[str, datetime] = {}
        self._last_pause_state: Dict[str, Any] = {"paused": False, "reason": ""}

    def can_dispatch_signals(self) -> bool:
        """Return False when operational protection has paused Telegram delivery."""
        state = self.evaluate_pause_state(notify=True)
        return not bool(state.get("paused"))

    def evaluate_pause_state(self, notify: bool = True) -> Dict[str, Any]:
        """Evaluate the current pause state from persisted results."""
        config = self.config_supplier()
        history = self.history_store.load_dataframe()
        state = evaluate_operational_pause(
            history,
            enabled=bool(getattr(config, "auto_pause_enabled", True)),
            max_consecutive_losses=int(getattr(config, "max_consecutive_losses", 3)),
            min_daily_win_rate=float(getattr(config, "min_daily_win_rate_pause", 0.0)),
        )

        if notify and state.get("paused"):
            pause_key = f"pause|{now_br().strftime('%Y-%m-%d')}|{state.get('reason', '')}"
            if self._mark_once(pause_key):
                self.notifier.send_pause_alert(state)

        self._last_pause_state = state
        return state

    def handle_history_update(self, updated_row: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """React to manual history updates with pause checks and social proof."""
        state = self.evaluate_pause_state(notify=True)
        self._send_social_proof(updated_row)
        return state

    def _send_social_proof(self, updated_row: Optional[Dict[str, Any]]) -> None:
        config = self.config_supplier()
        if not bool(getattr(config, "social_proof_enabled", True)):
            return

        history = self.history_store.load_dataframe()
        today = now_br().strftime("%d/%m/%Y")
        today_df = history[history["date"] == today].copy()
        metrics = build_dashboard_metrics(today_df)
        today_key = now_br().strftime("%Y-%m-%d")

        if int(metrics.get("current_win_streak", 0)) >= int(getattr(config, "social_proof_min_streak", 3)):
            streak = int(metrics["current_win_streak"])
            streak_key = f"social-proof|streak|{today_key}|{streak}"
            if self._mark_once(streak_key):
                self.notifier.send_social_proof(
                    kind="streak",
                    payload={
                        "streak": streak,
                        "brand": "MagnataZ VIP",
                    },
                    dedupe_key=streak_key,
                )

        decisive_total = int(metrics.get("wins", 0)) + int(metrics.get("losses", 0))
        if decisive_total >= int(getattr(config, "social_proof_min_decisive", 3)):
            win_rate = float(metrics.get("win_rate", 0.0))
            threshold = float(getattr(config, "social_proof_min_win_rate", 75.0))
            partial_key = (
                f"social-proof|partial|{today_key}|{int(metrics.get('wins', 0))}|"
                f"{int(metrics.get('losses', 0))}|{metrics.get('best_pair', '-')}"
            )
            if win_rate >= threshold and self._mark_once(partial_key):
                self.notifier.send_social_proof(
                    kind="partial_day",
                    payload={
                        "wins": int(metrics.get("wins", 0)),
                        "losses": int(metrics.get("losses", 0)),
                        "win_rate": win_rate,
                        "best_pair": metrics.get("best_pair", "-"),
                        "date": today,
                    },
                    dedupe_key=partial_key,
                )

    def _mark_once(self, key: str, ttl_minutes: int = 180) -> bool:
        now_value = datetime.now()
        with self._lock:
            cutoff = now_value - timedelta(minutes=ttl_minutes)
            self._recent_events = {
                item_key: item_time
                for item_key, item_time in self._recent_events.items()
                if item_time >= cutoff
            }
            if key in self._recent_events:
                return False
            self._recent_events[key] = now_value
            return True


class DailySummaryScheduler:
    """Schedule daily Telegram summary messages based on persisted manual results."""

    def __init__(
        self,
        history_store: SignalHistoryStore,
        notifier: Any,
        config_supplier: Callable[[], Any],
    ):
        self.history_store = history_store
        self.notifier = notifier
        self.config_supplier = config_supplier
        self._lock = threading.Lock()
        self._timer: Optional[threading.Timer] = None

    def start(self) -> None:
        self.reschedule()

    def stop(self) -> None:
        with self._lock:
            if self._timer is not None:
                try:
                    self._timer.cancel()
                except Exception:
                    pass
                self._timer = None

    def reschedule(self) -> None:
        self.stop()
        config = self.config_supplier()
        if not bool(getattr(config, "daily_summary_enabled", True)):
            return

        run_at = self._next_run(str(getattr(config, "daily_summary_time", "23:59")))
        wait_seconds = max(30.0, (run_at - now_br()).total_seconds())
        timer = threading.Timer(wait_seconds, self._run_summary)
        timer.daemon = True
        with self._lock:
            self._timer = timer
            timer.start()

    def _run_summary(self) -> None:
        try:
            config = self.config_supplier()
            if not bool(getattr(config, "daily_summary_enabled", True)):
                return

            history = self.history_store.load_dataframe()
            today = now_br().strftime("%d/%m/%Y")
            today_df = history[history["date"] == today].copy()
            metrics = build_dashboard_metrics(today_df)
            if int(metrics.get("wins", 0)) + int(metrics.get("losses", 0)) <= 0:
                return

            self.notifier.send_daily_summary(metrics, dedupe_key=f"daily-summary|{today}")
        except Exception as exc:
            logger.error("Daily summary failed: %s", exc, exc_info=True)
        finally:
            self.reschedule()

    def _next_run(self, schedule_time: str) -> datetime:
        now_value = now_br()
        try:
            hour_str, minute_str = schedule_time.split(":", 1)
            target = now_value.replace(
                hour=int(hour_str),
                minute=int(minute_str),
                second=0,
                microsecond=0,
            )
        except Exception:
            target = now_value.replace(hour=23, minute=59, second=0, microsecond=0)

        if target <= now_value:
            target += timedelta(days=1)
        return target
