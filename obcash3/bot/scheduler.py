from __future__ import absolute_import
"""
Async FREE-window scheduler owned by the cloud Telegram bot runtime.

This module centralizes:
- window detection
- signal generation
- Telegram sending
- result resolution
- window summary
- VIP CTA after the summary
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from obcash3.bot.commercial import FREE_SIGNAL_WINDOWS
from obcash3.bot.signal_engine import BotSignalEngine, FreeWindowSignalCandidate
from obcash3.bot.signal_store import GROUP_FREE, RESULT_LOSS, RESULT_PENDING, RESULT_WIN, WindowSignalStore
from obcash3.bot.telegram_sender import TelegramSender
from obcash3.bot.window_report import (
    build_window_cta_message,
    build_window_report_message,
    build_window_report_payload,
    should_send_positive_image,
)
from obcash3.config.settings import RESULT_POSITIVE_IMAGE_PATH
from obcash3.data.signal_store import save_signal_record
from obcash3.utils.history import signal_to_row
from obcash3.utils.logger import get_logger
from obcash3.utils.telegram import build_signal_message
from obcash3.utils.time import now_br

logger = get_logger(__name__)


@dataclass(frozen=True)
class FreeWindowConfig:
    key: str
    start: str
    end: str


WINDOWS = tuple(FreeWindowConfig(key=item[0], start=item[1], end=item[2]) for item in FREE_SIGNAL_WINDOWS)


def _minutes(clock_value: str) -> int:
    hour, minute = [int(part) for part in str(clock_value).split(":", 1)]
    return hour * 60 + minute


class FreeWindowScheduler:
    """Cloud-side scheduler for FREE window signals, reports and CTA cadence."""

    def __init__(
        self,
        bot,
        sender: TelegramSender,
        signal_engine: BotSignalEngine,
        results_engine,
        store: WindowSignalStore,
        poll_seconds: int = 30,
    ) -> None:
        self.bot = bot
        self.sender = sender
        self.signal_engine = signal_engine
        self.results_engine = results_engine
        self.store = store
        self.poll_seconds = max(10, int(poll_seconds))
        self.positive_result_image_path = Path(RESULT_POSITIVE_IMAGE_PATH)
        self._task: Optional[asyncio.Task] = None
        self._tick_lock = asyncio.Lock()

    # Scheduler lifecycle: starts the background FREE-window orchestration loop.
    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._run_loop(), name="magnataz-free-window-scheduler")
        logger.info("FREE window scheduler started")

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        logger.info("FREE window scheduler stopped")

    async def _run_loop(self) -> None:
        try:
            while True:
                await self.tick()
                await asyncio.sleep(self.poll_seconds)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("FREE window scheduler crashed")

    async def tick(self) -> None:
        if self._tick_lock.locked():
            return
        async with self._tick_lock:
            self.reset_daily_window_state()
            resolved = await self.results_engine.process_pending_results()
            if resolved:
                logger.info("FREE scheduler processed %d pending results", resolved)
            await self._dispatch_completed_window_reports()
            current_window = self.get_current_window()
            if current_window is None:
                return
            await self._maybe_send_signal_for_window(current_window)

    def get_current_window(self) -> Optional[FreeWindowConfig]:
        """Return the active FREE signal window for the current Brazil time."""
        current = now_br()
        current_minutes = current.hour * 60 + current.minute
        for window in WINDOWS:
            if _minutes(window.start) <= current_minutes <= _minutes(window.end):
                logger.debug("FREE scheduler active window: %s", window.key)
                return window
        return None

    async def generate_signal_for_window(self, window: FreeWindowConfig) -> Optional[FreeWindowSignalCandidate]:
        """Generate the best candidate for the active FREE window."""
        candidate = await self.signal_engine.generate_signal_for_window(window.key)
        if candidate is None:
            logger.info("FREE scheduler found no qualified signal for window=%s", window.key)
            return None
        return candidate

    def save_signal(self, signal_payload: dict[str, Any]) -> bool:
        """Persist one FREE signal in SQLite before it is sent."""
        return self.store.save_signal(signal_payload)

    async def send_signal_to_free_group(self, signal_payload: dict[str, Any], candidate: FreeWindowSignalCandidate):
        """Send one signal to the FREE group through the central Telegram sender."""
        target_chat_id = str(self.bot._configured_free_group_chat_id() or "").strip()
        if not target_chat_id:
            logger.warning("FREE scheduler skipped signal send without free group chat id")
            return None
        return await self.sender.send_text(
            chat_id=target_chat_id,
            text=build_signal_message(candidate.signal, message_mode=getattr(self.bot.service.config, "message_mode", "vip")),
            parse_mode="Markdown",
        )

    def update_signal_result(self, signal_id: str, result: str, profit_estimate: float) -> bool:
        """Compatibility wrapper for manual result updates through the SQLite store."""
        return self.store.update_signal_result(signal_id, result, profit_estimate)

    def get_window_signals(self, date: str, window: str) -> list[dict[str, Any]]:
        return self.store.get_window_signals(date, window, GROUP_FREE)

    def is_window_complete(self, date: str, window: str) -> bool:
        return self.store.is_window_complete(date, window, GROUP_FREE)

    def calculate_window_stats(self, date: str, window: str):
        return self.store.calculate_window_stats(date, window, GROUP_FREE)

    async def send_window_report(self, date: str, window: str):
        """Send the FREE-window performance report once."""
        state = self.store.get_window_state(date, window, GROUP_FREE)
        if int(state.get("report_sent", 0)):
            return None

        target_chat_id = str(self.bot._configured_free_group_chat_id() or "").strip()
        if not target_chat_id:
            return None

        stats = self.calculate_window_stats(date, window)
        payload = build_window_report_payload(stats)
        logger.info("FREE window report payload ready: %s", payload.to_dict())
        message = await self.sender.send_text(
            chat_id=target_chat_id,
            text=build_window_report_message(stats),
        )
        if message is not None:
            self.store.mark_window_report_sent(date, window, getattr(message, "message_id", None), GROUP_FREE)
            logger.info("FREE window report sent: date=%s window=%s", date, window)
        return message

    async def send_window_result_image(self, date: str, window: str):
        """Send the positive-result art once when the FREE window closes with at least one WIN."""
        state = self.store.get_window_state(date, window, GROUP_FREE)
        if int(state.get("image_sent", 0)):
            return None

        target_chat_id = str(self.bot._configured_free_group_chat_id() or "").strip()
        if not target_chat_id:
            return None

        if not self.positive_result_image_path.exists():
            logger.warning("FREE result image missing, skipping photo send: %s", self.positive_result_image_path)
            self.store.mark_window_image_sent(date, window, None, GROUP_FREE)
            return None

        message = await self.sender.send_photo(
            chat_id=target_chat_id,
            photo_path=self.positive_result_image_path,
        )
        if message is not None:
            self.store.mark_window_image_sent(date, window, getattr(message, "message_id", None), GROUP_FREE)
            logger.info("FREE window positive image sent: date=%s window=%s", date, window)
        else:
            # Fail safe: avoid retry spam if the asset exists but Telegram rejects once.
            self.store.mark_window_image_sent(date, window, None, GROUP_FREE)
        return message

    async def send_vip_cta_after_report(self, date: str, window: str):
        """Send the VIP CTA once after the FREE-window report."""
        state = self.store.get_window_state(date, window, GROUP_FREE)
        if int(state.get("cta_sent", 0)):
            return None

        target_chat_id = str(self.bot._configured_free_group_chat_id() or "").strip()
        if not target_chat_id:
            return None

        message = await self.sender.send_text(
            chat_id=target_chat_id,
            text=build_window_cta_message(self.bot.service.config),
        )
        if message is not None:
            self.store.mark_window_cta_sent(date, window, getattr(message, "message_id", None), GROUP_FREE)
            logger.info("FREE window CTA sent: date=%s window=%s", date, window)
        return message

    def reset_daily_window_state(self) -> None:
        """Keep SQLite lean; date-scoped state makes daily reset implicit."""
        self.store.prune_old_data()

    async def _maybe_send_signal_for_window(self, window: FreeWindowConfig) -> None:
        config = self.bot.service.config
        if not bool(getattr(config, "telegram_enabled", True)):
            return

        today = now_br().strftime("%Y-%m-%d")
        current_count = self.store.count_window_signals(today, window.start, GROUP_FREE)
        logger.info("FREE scheduler window check: window=%s signals=%d", window.key, current_count)
        if current_count >= 2:
            return

        if not self._respect_signal_spacing(today, window.start):
            return

        candidate = await self.generate_signal_for_window(window)
        if candidate is None:
            return

        entry_time = self.signal_engine.build_entry_time(candidate.signal)
        if self.store.signal_exists_for_window(
            date=today,
            window=window.start,
            asset=str(candidate.signal.asset),
            entry_time=entry_time.strftime("%H:%M"),
            action=str(candidate.signal.action),
        ):
            logger.info(
                "FREE scheduler discarded duplicate candidate: %s %s %s",
                candidate.signal.asset,
                entry_time.strftime("%H:%M"),
                candidate.signal.action,
            )
            return

        sequence = self.store.next_sequence(today, window.start, GROUP_FREE)
        signal_payload = self._build_signal_payload(today, window, sequence, candidate, entry_time)
        saved = self.save_signal(signal_payload)
        if not saved:
            logger.info("FREE scheduler skipped already saved signal id=%s", signal_payload["id"])
            return

        message = await self.send_signal_to_free_group(signal_payload, candidate)
        if message is None:
            self.store.delete_signal(signal_payload["id"])
            logger.warning("FREE scheduler rolled back unsent signal id=%s", signal_payload["id"])
            return

        self.store.attach_message_id(signal_payload["id"], getattr(message, "message_id", None))
        self._mirror_signal_to_shared_history(candidate)
        logger.info(
            "FREE signal sent: id=%s window=%s sequence=%d technical=%.1f ml=%.1f final=%.1f",
            signal_payload["id"],
            window.key,
            sequence,
            signal_payload["technical_score"],
            signal_payload["score_ml"],
            signal_payload["score_final"],
        )

    async def _dispatch_completed_window_reports(self) -> None:
        today = now_br().date()
        date_candidates = [today.isoformat(), (today - timedelta(days=1)).isoformat()]
        for date_key in date_candidates:
            for window in self.store.list_windows_for_date(date_key, GROUP_FREE):
                state = self.store.get_window_state(date_key, window, GROUP_FREE)
                signals = self.store.get_window_signals(date_key, window, GROUP_FREE)
                if len(signals) < 2:
                    continue
                if not self.is_window_complete(date_key, window):
                    continue
                stats = self.calculate_window_stats(date_key, window)
                if not int(state.get("report_sent", 0)):
                    await self.send_window_report(date_key, window)
                    state = self.store.get_window_state(date_key, window, GROUP_FREE)
                if (
                    int(state.get("report_sent", 0))
                    and should_send_positive_image(stats.total_wins, stats.total_losses)
                    and not int(state.get("image_sent", 0))
                ):
                    await self.send_window_result_image(date_key, window)
                    state = self.store.get_window_state(date_key, window, GROUP_FREE)
                if int(state.get("report_sent", 0)) and not int(state.get("cta_sent", 0)):
                    await self.send_vip_cta_after_report(date_key, window)

    def _respect_signal_spacing(self, date: str, window: str) -> bool:
        last_sent_at = self.store.last_signal_sent_at(date, window, GROUP_FREE)
        if not last_sent_at:
            return True
        try:
            last_dt = datetime.fromisoformat(last_sent_at)
        except Exception:
            return True
        min_gap = int(getattr(self.bot.service.config, "min_signal_interval_seconds", 180) or 180)
        elapsed = (now_br() - last_dt).total_seconds()
        if elapsed < min_gap:
            logger.info(
                "FREE scheduler spacing guard: window=%s elapsed=%.1f required=%d",
                window,
                elapsed,
                min_gap,
            )
            return False
        return True

    def _build_signal_payload(
        self,
        date_key: str,
        window: FreeWindowConfig,
        sequence: int,
        candidate: FreeWindowSignalCandidate,
        entry_time,
    ) -> dict[str, Any]:
        signal = candidate.signal
        history_row = signal_to_row(signal)
        signal_id = f"{date_key}-{window.start.replace(':', '-')}-{sequence:02d}"
        direction = "CALL" if str(signal.action).upper() == "COMPRA" else "PUT"
        selection_reason = str(getattr(signal, "selection_reason", "") or "").strip()
        if not selection_reason:
            selection_reason = f"technical={candidate.technical_score:.1f} ml={candidate.ml_score:.1f} final={candidate.score_final:.1f}"
        extra_payload = {
            "signal": getattr(signal, "model_dump", lambda: {})() or {},
            "technical_score": candidate.technical_score,
            "ml_score": candidate.ml_score,
            "score_final": candidate.score_final,
        }
        return {
            "id": signal_id,
            "history_signal_id": str(history_row.get("signal_id", "") or ""),
            "date": date_key,
            "window": window.start,
            "sequence": sequence,
            "group_name": GROUP_FREE,
            "asset": str(signal.asset),
            "direction": direction,
            "action": str(signal.action),
            "interval": str(signal.interval),
            "price": float(signal.price or 0.0),
            "entry_time": entry_time.strftime("%H:%M"),
            "entry_timestamp": entry_time.isoformat(),
            "created_at": now_br().isoformat(),
            "sent_at": now_br().isoformat(),
            "technical_score": float(candidate.technical_score),
            "score_ml": float(candidate.ml_score),
            "score_final": float(candidate.score_final),
            "status": "sent",
            "result": RESULT_PENDING,
            "profit_estimate": 0.0,
            "stake_estimate": self._stake_estimate(),
            "telegram_message_id": None,
            "source": str(getattr(signal, "source", "") or ""),
            "report_sent": 0,
            "cta_sent": 0,
            "result_updated_at": "",
            "selection_reason": selection_reason,
            "extra_payload": json.dumps(extra_payload, ensure_ascii=False, default=str),
        }

    def _mirror_signal_to_shared_history(self, candidate: FreeWindowSignalCandidate) -> None:
        if save_signal_record(candidate.signal, store=self.bot.service.history_store):
            logger.info("FREE signal mirrored to shared history: %s", candidate.signal.asset)
        else:
            logger.warning("FREE signal could not be mirrored to shared history: %s", candidate.signal.asset)

    def _stake_estimate(self) -> float:
        config = self.bot.service.config
        balance = float(getattr(config, "account_balance", 1000.0) or 1000.0)
        risk_pct = float(getattr(config, "risk_pct", 1.0) or 1.0)
        return round(balance * (risk_pct / 100.0), 2)
