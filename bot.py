from __future__ import absolute_import
"""
Standalone Telegram bot launcher for GitHub/Render deployments.
"""

import asyncio
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from obcash3.api.services import OBCCashService
from obcash3.bot.handlers import start_bot


async def _main() -> None:
    service = OBCCashService(enable_background_tasks=False)
    config = service.config

    if not config.telegram_token:
        print("BOT_TOKEN/TELEGRAM_TOKEN não configurado.")
        return

    target_chat_id = (
        str(getattr(config, "free_telegram_chat_id", "") or "").strip()
        if str(getattr(config, "group_tier", "free") or "free").strip().lower() == "free"
        else str(getattr(config, "vip_telegram_chat_id", "") or "").strip()
    ) or str(getattr(config, "telegram_chat_id", "") or "").strip()

    bot = await start_bot(
        token=config.telegram_token,
        service=service,
        chat_id=target_chat_id or None,
    )

    print("Bot iniciado. Pressione Ctrl+C para encerrar.")
    try:
        await asyncio.Future()
    finally:
        await bot.stop()
        service.shutdown()


def main() -> int:
    try:
        asyncio.run(_main())
        return 0
    except KeyboardInterrupt:
        print("\nBot encerrado.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
