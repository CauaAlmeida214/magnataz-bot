from __future__ import absolute_import
"""
Private welcome funnel persistence and scheduling for Telegram leads.
"""

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

from obcash3.config.settings import APP_DATA_DIR
from obcash3.utils.logger import get_logger
from obcash3.utils.time import now_br

logger = get_logger(__name__)

FUNNEL_STATE_PATH = APP_DATA_DIR / "telegram_funnel_state.json"

FOLLOWUP_STEPS = {
    "followup_1_sent": timedelta(hours=1),
    "followup_2_sent": timedelta(hours=6),
    "followup_3_sent": timedelta(hours=24),
}


def _iso_now() -> str:
    return now_br().isoformat()


def _parse_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


class LeadFunnelManager:
    """Persist private-funnel state and schedule follow-up tasks."""

    def __init__(self, bot, state_path: Optional[Path] = None):
        self.bot = bot
        self.state_path = Path(state_path or FUNNEL_STATE_PATH)
        self._lock = Lock()
        self._tasks: Dict[str, Dict[str, asyncio.Task]] = {}
        self._state = self.load_funnel_state()

    # Funnel persistence: load JSON state for all users.
    def load_funnel_state(self) -> Dict[str, Dict[str, Any]]:
        try:
            if not self.state_path.exists():
                return {}
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return {str(key): self._normalize_state(value) for key, value in data.items()}
        except Exception:
            logger.exception("Failed to load Telegram funnel state")
        return {}

    # Funnel persistence: save JSON state after each stage update.
    def save_funnel_state(self) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            with self._lock:
                snapshot = dict(self._state)
            self.state_path.write_text(
                json.dumps(snapshot, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            logger.exception("Failed to save Telegram funnel state")

    def start(self) -> None:
        """Restore pending follow-up schedules after bot startup."""
        for user_id in list(self._state.keys()):
            self.schedule_followups(user_id)

    async def stop(self) -> None:
        """Cancel scheduled tasks when the bot stops."""
        tasks = []
        for user_tasks in self._tasks.values():
            tasks.extend(user_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()

    def get_user_state(self, user_id: int | str) -> Dict[str, Any]:
        key = str(user_id)
        with self._lock:
            state = self._state.get(key)
            if state is None:
                state = self._new_user_state()
                self._state[key] = state
        return state

    def register_welcome(self, user_id: int | str, first_name: str, private_chat_id: int | str) -> bool:
        """Register the private welcome only once per user cycle."""
        key = str(user_id)
        with self._lock:
            state = self._state.get(key, self._new_user_state())
            if bool(state.get("welcome_sent")):
                self._state[key] = state
                return False
            state["first_name"] = str(first_name or "")
            state["private_chat_id"] = str(private_chat_id or "")
            state["welcome_sent"] = True
            state["welcome_sent_at"] = _iso_now()
            state["updated_at"] = _iso_now()
            self._state[key] = state
        self.save_funnel_state()
        return True

    def mark_followup_sent(self, user_id: int | str, step_key: str) -> None:
        key = str(user_id)
        with self._lock:
            state = self._state.get(key, self._new_user_state())
            state[step_key] = True
            state[f"{step_key}_at"] = _iso_now()
            state["updated_at"] = _iso_now()
            if step_key == "followup_3_sent":
                state["completed"] = True
                state["completed_at"] = _iso_now()
            self._state[key] = state
        self.save_funnel_state()

    def schedule_followups(self, user_id: int | str) -> None:
        """Schedule only the pending follow-ups for a given user."""
        key = str(user_id)
        state = self.get_user_state(key)
        welcome_sent_at = _parse_datetime(str(state.get("welcome_sent_at", "")))
        if not bool(state.get("welcome_sent")) or welcome_sent_at is None:
            return

        user_tasks = self._tasks.setdefault(key, {})
        for step_key, offset in FOLLOWUP_STEPS.items():
            if bool(state.get(step_key)):
                continue
            task_name = f"{key}:{step_key}"
            task = user_tasks.get(step_key)
            if task is not None and not task.done():
                continue
            delay = max(0.0, (welcome_sent_at + offset - now_br()).total_seconds())
            user_tasks[step_key] = asyncio.create_task(
                self._run_followup_after_delay(key, step_key, delay),
                name=f"telegram-funnel-{task_name}",
            )
            logger.info("Telegram funnel scheduled: user_id=%s step=%s delay_seconds=%.1f", key, step_key, delay)

    async def _run_followup_after_delay(self, user_id: str, step_key: str, delay_seconds: float) -> None:
        try:
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
            await self._dispatch_followup(user_id, step_key)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Telegram funnel follow-up failed: user_id=%s step=%s", user_id, step_key)
        finally:
            user_tasks = self._tasks.get(user_id, {})
            user_tasks.pop(step_key, None)
            if not user_tasks and user_id in self._tasks:
                self._tasks.pop(user_id, None)

    async def _dispatch_followup(self, user_id: str, step_key: str) -> None:
        state = self.get_user_state(user_id)
        if bool(state.get(step_key)) or not bool(state.get("welcome_sent")):
            return
        private_chat_id = str(state.get("private_chat_id", "") or "").strip()
        if not private_chat_id:
            logger.warning("Telegram funnel skipped follow-up without private chat id: user_id=%s", user_id)
            return

        if step_key == "followup_1_sent":
            sent = await self.bot.send_followup_1(private_chat_id)
        elif step_key == "followup_2_sent":
            sent = await self.bot.send_followup_2(private_chat_id)
        elif step_key == "followup_3_sent":
            sent = await self.bot.send_followup_3(private_chat_id)
        else:
            return

        if sent:
            self.mark_followup_sent(user_id, step_key)

    def _new_user_state(self) -> Dict[str, Any]:
        return self._normalize_state({})

    def _normalize_state(self, state: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(state or {})
        normalized.setdefault("first_name", "")
        normalized.setdefault("private_chat_id", "")
        normalized.setdefault("welcome_sent", False)
        normalized.setdefault("welcome_sent_at", "")
        normalized.setdefault("followup_1_sent", False)
        normalized.setdefault("followup_1_sent_at", "")
        normalized.setdefault("followup_2_sent", False)
        normalized.setdefault("followup_2_sent_at", "")
        normalized.setdefault("followup_3_sent", False)
        normalized.setdefault("followup_3_sent_at", "")
        normalized.setdefault("completed", False)
        normalized.setdefault("completed_at", "")
        normalized.setdefault("updated_at", "")
        return normalized
