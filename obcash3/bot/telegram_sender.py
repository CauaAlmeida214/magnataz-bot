from __future__ import absolute_import
"""
Centralized Telegram sender for all automated bot messages.

This keeps cloud automation sequential and avoids parallel send bursts.
"""

import asyncio
from pathlib import Path
from typing import Any, Optional

from obcash3.utils.logger import get_logger

logger = get_logger(__name__)


class TelegramSender:
    """Single sequential sender used by the Oracle bot runtime."""

    def __init__(self, bot, delay_seconds: float = 0.6):
        self.bot = bot
        self.delay_seconds = max(0.0, float(delay_seconds))
        self._lock = asyncio.Lock()

    async def send_text(
        self,
        chat_id: str,
        text: str,
        parse_mode: Optional[str] = None,
        reply_markup: Any = None,
        disable_web_page_preview: bool = True,
    ):
        """Send one message at a time through the live bot application."""
        async with self._lock:
            application = getattr(self.bot, "application", None)
            if application is None:
                logger.warning("Telegram sender called before application initialization")
                return None

            try:
                message = await application.bot.send_message(
                    chat_id=str(chat_id),
                    text=text,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup,
                    disable_web_page_preview=disable_web_page_preview,
                )
                if self.delay_seconds > 0:
                    await asyncio.sleep(self.delay_seconds)
                return message
            except Exception as exc:
                logger.error("Central Telegram sender failed: %s", exc, exc_info=True)
                return None

    async def send_photo(
        self,
        chat_id: str,
        photo_path: str | Path,
        caption: Optional[str] = None,
        parse_mode: Optional[str] = None,
        reply_markup: Any = None,
    ):
        """Send one local photo at a time through the live bot application."""
        async with self._lock:
            application = getattr(self.bot, "application", None)
            if application is None:
                logger.warning("Telegram sender photo called before application initialization")
                return None

            path = Path(photo_path)
            if not path.exists():
                logger.warning("Central Telegram sender photo missing: %s", path)
                return None

            try:
                with path.open("rb") as photo_file:
                    message = await application.bot.send_photo(
                        chat_id=str(chat_id),
                        photo=photo_file,
                        caption=caption,
                        parse_mode=parse_mode,
                        reply_markup=reply_markup,
                    )
                if self.delay_seconds > 0:
                    await asyncio.sleep(self.delay_seconds)
                return message
            except Exception as exc:
                logger.error("Central Telegram sender photo failed: %s", exc, exc_info=True)
                return None
