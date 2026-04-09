from __future__ import absolute_import
"""
Telegram bot handlers for OB CASH 3.0.
"""

from typing import Any, Dict, Optional

try:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
    from telegram.ext import (
        ApplicationBuilder,
        CallbackQueryHandler,
        CommandHandler,
        ContextTypes,
        MessageHandler,
        filters,
    )
    import telegram.ext as telegram_ext_module
    import telegram.ext._applicationbuilder as telegram_appbuilder_module
    from telegram.ext import Updater as TelegramUpdater

    HAS_TELEGRAM = True
except ImportError:  # pragma: no cover - optional dependency for bot mode
    InlineKeyboardButton = None
    InlineKeyboardMarkup = None
    Update = Any
    ApplicationBuilder = None
    CallbackQueryHandler = None
    CommandHandler = None
    MessageHandler = None
    filters = None

    class ContextTypes:  # type: ignore[override]
        DEFAULT_TYPE = Any

    HAS_TELEGRAM = False

from obcash3.api.services import OBCCashService
from obcash3.bot.commercial import (
    AUTO_VIP_PROMO_EVERY_SIGNALS,
    build_auto_vip_promo_message,
    build_free_welcome_message,
    build_group_free_join_message,
    build_post_payment_dm_message,
    build_private_welcome_vip_message,
    build_vip_offer_message,
    lovable_url,
    payment_link,
    private_welcome_link,
    PRIVATE_WELCOME_BUTTON_TEXT,
)
from obcash3.bot.funnel import LeadFunnelManager
from obcash3.bot.promo_tracker import FreeGroupPromoTracker
from obcash3.bot.results_engine import FreeResultsEngine
from obcash3.bot.scheduler import FreeWindowScheduler
from obcash3.bot.signal_engine import BotSignalEngine
from obcash3.bot.signal_store import WindowSignalStore
from obcash3.bot.telegram_sender import TelegramSender
from obcash3.utils.history import build_history_table, ensure_history_schema
from obcash3.utils.logger import get_logger
from obcash3.utils.telegram import build_dashboard_message, build_signal_message
from obcash3.utils.time import now_br
logger = get_logger(__name__)


def _patch_python_telegram_bot() -> None:
    """
    Work around python-telegram-bot 20.7 slot incompatibility on Python 3.14.
    """
    if not HAS_TELEGRAM:
        return

    slots = getattr(TelegramUpdater, "__slots__", ())
    if "_Updater__polling_cleanup_cb" in slots:
        return

    class PatchedUpdater(TelegramUpdater):
        __slots__ = ("_Updater__polling_cleanup_cb",)

    telegram_appbuilder_module.Updater = PatchedUpdater
    telegram_ext_module.Updater = PatchedUpdater


_patch_python_telegram_bot()


def _model_value(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    if hasattr(obj, key):
        return getattr(obj, key)
    model_dump = getattr(obj, "model_dump", None)
    if callable(model_dump):
        try:
            return model_dump().get(key, default)
        except Exception:
            return default
    return default


class OBCCashBot:
    """Telegram bot for OB CASH 3.0."""

    def __init__(self, token: str, service: OBCCashService):
        self.token = token
        self.service = service
        self.application = None
        self.chat_id: Optional[str] = None
        self.last_user_chat_id: Optional[str] = None
        self.free_promo_tracker = FreeGroupPromoTracker()
        self.funnel_manager = LeadFunnelManager(self)
        # Cloud orchestration: Telegram sending, SQLite state and FREE windows stay inside the bot.
        self.telegram_sender = TelegramSender(self)
        self.window_store = WindowSignalStore()
        self.window_signal_engine = BotSignalEngine(service)
        self.results_engine = FreeResultsEngine(
            store=self.window_store,
            fetcher=self.service.fetcher,
            config_supplier=lambda: self.service.config,
            history_store=self.service.history_store,
        )
        self.window_scheduler = FreeWindowScheduler(
            bot=self,
            sender=self.telegram_sender,
            signal_engine=self.window_signal_engine,
            results_engine=self.results_engine,
            store=self.window_store,
        )

    async def start(self, chat_id: Optional[str] = None):
        """Start the bot."""
        if not HAS_TELEGRAM:
            raise RuntimeError("python-telegram-bot nao esta instalado neste ambiente.")

        self.application = ApplicationBuilder().token(self.token).build()
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("scan", self.scan_command))
        self.application.add_handler(CommandHandler("status", self.status_command))
        self.application.add_handler(CommandHandler("stats", self.stats_command))
        self.application.add_handler(CommandHandler("placar", self.placar_command))
        self.application.add_handler(CommandHandler("resultados", self.resultados_command))
        self.application.add_handler(CommandHandler("vip", self.vip_command))
        self.application.add_handler(CommandHandler("plano", self.plano_command))
        self.application.add_handler(CommandHandler("entrar", self.entrar_command))
        self.application.add_handler(CommandHandler("comprar", self.comprar_command))
        self.application.add_handler(CommandHandler("dashboard", self.dashboard_command))
        self.application.add_handler(CommandHandler("history", self.history_command))
        self.application.add_handler(CommandHandler("config", self.config_command))
        # Funnel onboarding: detect new members entering the free group.
        self.application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, self.new_chat_members_handler))
        self.application.add_handler(CallbackQueryHandler(self.button_callback))

        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()

        if chat_id:
            self.chat_id = str(chat_id)
            await self.send_free_welcome()

        # Funnel restore: pending private follow-ups are rescheduled on bot startup.
        self.funnel_manager.start()
        # FREE windows: start the cloud scheduler after polling is alive.
        self.window_scheduler.start()
        logger.info("Telegram bot started")

    async def stop(self):
        """Stop the bot."""
        if self.application:
            updater = getattr(self.application, "updater", None)
            if updater is not None:
                try:
                    if getattr(updater, "running", False):
                        await updater.stop()
                except Exception as exc:
                    logger.warning("Updater stop failed: %s", exc)
            try:
                await self.application.stop()
            finally:
                await self.window_scheduler.stop()
                await self.funnel_manager.stop()
                await self.application.shutdown()
            logger.info("Telegram bot stopped")

    async def send_message(self, text: str, parse_mode: str = None, reply_markup=None, chat_id: Optional[str] = None):
        """Send a message to the configured chat or to an explicit target."""
        target_chat_id = str(chat_id or self.chat_id or "").strip()
        if not target_chat_id:
            logger.warning("No chat ID configured, cannot send message")
            return False

        # Central sender: all automated Telegram traffic goes through one sequential path.
        return await self.telegram_sender.send_text(
            chat_id=target_chat_id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )

    async def send_free_welcome(self, chat_id: Optional[str] = None) -> bool:
        """Send the fixed MagnataZ Free welcome message."""
        return bool(await self.send_message(build_free_welcome_message(self.service.config), chat_id=chat_id))

    async def send_vip_offer(self, chat_id: Optional[str] = None) -> bool:
        """Send the public VIP offer without exposing the premium group link."""
        return bool(await self.send_message(build_vip_offer_message(self.service.config), chat_id=chat_id))

    async def send_auto_vip_promo(self, chat_id: Optional[str] = None) -> bool:
        """Send the lightweight VIP CTA used after the FREE window summary."""
        return bool(await self.send_message(build_auto_vip_promo_message(self.service.config), chat_id=chat_id))

    async def send_post_payment_dm(self, user_id: int | str) -> bool:
        """Send the premium group link only through a private DM to the buyer."""
        if not user_id:
            return False
        return bool(await self.send_message(build_post_payment_dm_message(self.service.config), chat_id=str(user_id)))

    async def handle_payment_confirmation(self, user_id: int | str) -> bool:
        """
        Ready entry point for future Kiwify/webhook integration.
        """
        return await self.send_post_payment_dm(user_id)

    async def send_signal_alert(self, signal: Dict[str, Any]):
        """Send one signal manually without triggering FREE-window promo logic."""
        target_chat_id = self._configured_group_chat_id()
        if not target_chat_id:
            logger.warning(
                "Bot signal alert skipped: no configured Telegram route for tier=%s",
                getattr(self.service.config, "group_tier", "free"),
            )
            return

        text = build_signal_message(signal, message_mode=getattr(self.service.config, "message_mode", "vip"))
        keyboard = [
            [InlineKeyboardButton("📊 Dashboard", callback_data="dashboard")],
            [
                InlineKeyboardButton("🔄 Scan", callback_data="scan_manual"),
                InlineKeyboardButton("📈 Histórico", callback_data="history"),
            ],
            [InlineKeyboardButton("⭐ Ver plano premium vitalício", callback_data="vip")],
        ]

        sent = await self.send_message(
            text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
            chat_id=target_chat_id,
        )
        if sent:
            logger.info("Bot manual signal alert sent to chat_id=%s", target_chat_id)

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self._capture_chat_id(update)
        start_payload = str(context.args[0]).strip().lower() if getattr(context, "args", None) else ""
        # Deep-link onboarding: /start welcome opens the private VIP intro automatically.
        if start_payload == "welcome":
            await self.send_private_welcome(update)
            return
        await update.message.reply_text(build_free_welcome_message(self.service.config))

    async def new_chat_members_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Onboard new members in the free group with a deep-link to the private bot."""
        self._capture_chat_id(update)
        message = getattr(update, "message", None)
        if message is None or not getattr(message, "new_chat_members", None):
            return
        if not self._is_free_group_update(update):
            return

        for member in message.new_chat_members:
            if getattr(member, "is_bot", False):
                continue
            try:
                await self.send_group_welcome(message, member)
            except Exception as exc:
                logger.error("Failed to send free-group onboarding message: %s", exc)

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self._capture_chat_id(update)
        text = (
            "Comandos disponíveis\n\n"
            "/start\n"
            "/scan EUR/USD 5m\n"
            "/status\n"
            "/stats\n"
            "/placar\n"
            "/resultados\n"
            "/vip\n"
            "/plano\n"
            "/entrar\n"
            "/comprar\n"
            "/dashboard\n"
            "/history\n"
            "/config"
        )
        await update.message.reply_text(text)

    async def scan_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self._capture_chat_id(update)
        args = context.args
        pair = args[0] if len(args) >= 1 else None
        timeframe = args[1] if len(args) >= 2 else "5m"

        await update.message.reply_text("Analisando...")
        try:
            if pair:
                signal = await self.service.analyze_pair(pair, timeframe, send_notification=False)
                await self._send_signal_result(update.message, signal)
            else:
                result = await self.service.scan_all_pairs(timeframe, send_notifications=False)
                best_signal = _model_value(result, "best_signal", None)
                if best_signal is not None:
                    await self._send_signal_result(update.message, best_signal)
                else:
                    await update.message.reply_text(
                        (
                            "Nenhuma oportunidade qualificada agora.\n"
                            f"Pares analisados: {int(_model_value(result, 'total_pairs', 0))}\n"
                            f"Qualificados: {int(_model_value(result, 'qualified_candidates', 0))}\n"
                            f"Tempo: {float(_model_value(result, 'scan_duration_seconds', 0.0)):.1f}s"
                        )
                    )
        except Exception as exc:
            await update.message.reply_text(f"Erro: {exc}")

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self._capture_chat_id(update)
        stats = self.service.get_stats()
        pause_state = self.service.automation_manager.evaluate_pause_state(notify=False)
        uptime_seconds = int(_model_value(stats, "uptime_seconds", 0) or 0)
        telegram_routed = any(
            str(getattr(self.service.config, key, "") or "").strip()
            for key in ("telegram_chat_id", "free_telegram_chat_id", "vip_telegram_chat_id")
        )
        text = (
            "*Status do Sistema*\n\n"
            f"Uptime: {uptime_seconds // 3600}h {(uptime_seconds % 3600) // 60}m\n"
            f"Scans: {int(_model_value(stats, 'total_scans', 0))}\n"
            f"Sinais: {int(_model_value(stats, 'total_signals', 0))}\n"
            f"Sinais fortes hoje: {int(_model_value(stats, 'strong_signals_today', 0))}\n"
            f"Cache: {float(_model_value(stats, 'cache_hit_rate', 0.0)):.1f}%\n"
            f"Telegram: {'ON' if self.service.config.telegram_enabled and self.service.config.telegram_token and telegram_routed else 'OFF'}\n"
            f"Proteção operacional: {'PAUSADA' if pause_state.get('paused') else 'ATIVA'}"
        )
        await update.message.reply_text(text, parse_mode="Markdown")

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self._capture_chat_id(update)
        stats = self.service.get_stats()
        await update.message.reply_text(
            (
                f"Média de score: {float(_model_value(stats, 'average_score', 0.0)):.1f}%\n"
                f"Último scan: {_model_value(stats, 'last_scan', 'N/A') or 'N/A'}"
            )
        )

    async def placar_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self._capture_chat_id(update)
        metrics = self.service.get_dashboard_metrics()
        text = (
            "*PLACAR OPERACIONAL*\n\n"
            f"Wins: `{int(metrics.get('wins', 0))}`\n"
            f"Losses: `{int(metrics.get('losses', 0))}`\n"
            f"Win Rate: `{float(metrics.get('win_rate', 0.0)):.1f}%`\n"
            f"Sequência de wins: `{int(metrics.get('current_win_streak', 0))}`\n"
            f"Sequência de losses: `{int(metrics.get('current_loss_streak', 0))}`"
        )
        await update.message.reply_text(text, parse_mode="Markdown")

    async def resultados_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self._capture_chat_id(update)
        await update.message.reply_text(self._results_text(), parse_mode="Markdown")

    async def vip_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self._capture_chat_id(update)
        logger.info("Telegram /vip command served with current VIP offer copy")
        await update.message.reply_text(build_vip_offer_message(self.service.config))

    async def plano_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self._capture_chat_id(update)
        await update.message.reply_text(build_vip_offer_message(self.service.config))

    async def entrar_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self._capture_chat_id(update)
        await update.message.reply_text(build_vip_offer_message(self.service.config))

    async def comprar_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self._capture_chat_id(update)
        await update.message.reply_text(build_vip_offer_message(self.service.config))

    async def dashboard_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self._capture_chat_id(update)
        await update.message.reply_text(
            build_dashboard_message(self.service.get_dashboard_metrics()),
            parse_mode="Markdown",
        )

    async def history_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self._capture_chat_id(update)
        await update.message.reply_text(self._history_text(), parse_mode="Markdown")

    async def config_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self._capture_chat_id(update)
        config = self.service.get_config_dict()
        masked = dict(config)
        for key in ("twelve_api_key", "av_api_key", "telegram_token", "telegram_chat_id", "free_telegram_chat_id", "vip_telegram_chat_id"):
            if masked.get(key):
                masked[key] = "***"

        text = (
            "*Configuração Atual*\n\n"
            f"Conta: ${masked['account_balance']:,.2f}\n"
            f"Risco: {masked['risk_pct']}%\n"
            f"Filtro horas: {'ON' if masked['filter_hours'] else 'OFF'}\n"
            f"MTF: {'ON' if masked['mtf_confirm'] else 'OFF'}\n"
            f"Telegram: {'ON' if masked['telegram_enabled'] else 'OFF'}\n"
            f"Template: {masked['message_mode']}\n"
            f"Score mínimo: {masked['min_score']}\n"
            f"Auto pausa: {'ON' if masked.get('auto_pause_enabled') else 'OFF'}"
        )
        await update.message.reply_text(text, parse_mode="Markdown")

    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        if query.data == "dashboard":
            await query.edit_message_text(
                build_dashboard_message(self.service.get_dashboard_metrics()),
                parse_mode="Markdown",
            )
        elif query.data == "scan_manual":
            await query.edit_message_text("Executando scan manual...")
            result = await self.service.scan_all_pairs("5m", send_notifications=False)
            best_signal = _model_value(result, "best_signal", None)
            if best_signal is not None:
                await query.edit_message_text(
                    build_signal_message(best_signal, message_mode=getattr(self.service.config, "message_mode", "vip")),
                    parse_mode="Markdown",
                )
            else:
                await query.edit_message_text(
                    (
                        "Nenhuma oportunidade qualificada agora.\n"
                        f"Pares analisados: {int(_model_value(result, 'total_pairs', 0))}\n"
                        f"Qualificados: {int(_model_value(result, 'qualified_candidates', 0))}\n"
                        f"Tempo: {float(_model_value(result, 'scan_duration_seconds', 0.0)):.1f}s"
                    )
                )
        elif query.data == "history":
            await query.edit_message_text(self._history_text(), parse_mode="Markdown")
        elif query.data == "vip":
            logger.info("Telegram VIP callback served with current VIP offer copy")
            await query.edit_message_text(build_vip_offer_message(self.service.config))

    async def _send_signal_result(self, message, signal):
        text = build_signal_message(signal, message_mode=getattr(self.service.config, "message_mode", "vip"))
        await message.reply_text(text, parse_mode="Markdown")

    # Funnel onboarding: public welcome in the free group with a private deep-link CTA.
    async def send_group_welcome(self, group_message, member) -> bool:
        mention = member.mention_html(member.first_name or "Trader")
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton(PRIVATE_WELCOME_BUTTON_TEXT, url=private_welcome_link(self.service.config))]]
        )
        await group_message.reply_text(
            build_group_free_join_message(mention),
            parse_mode="HTML",
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
        logger.info("Free-group onboarding message sent for new member user_id=%s", getattr(member, "id", ""))
        return True

    # Funnel onboarding: private welcome is sent only once after /start welcome.
    async def send_private_welcome(self, update: Update) -> bool:
        user = getattr(update, "effective_user", None)
        chat = getattr(update, "effective_chat", None)
        message = getattr(update, "message", None)
        if user is None or chat is None or message is None:
            return False

        user_id = str(getattr(user, "id", "") or "").strip()
        if not user_id:
            return False

        existing_state = self.funnel_manager.get_user_state(user_id)
        if bool(existing_state.get("welcome_sent")):
            logger.info("Telegram funnel welcome skipped for existing lead user_id=%s", user_id)
            self.funnel_manager.schedule_followups(user_id)
            return False

        sent_message = await message.reply_text(
            build_private_welcome_vip_message(self.service.config),
            reply_markup=self._private_offer_keyboard("Ver página oficial", "Entrar no VIP agora"),
            disable_web_page_preview=True,
        )
        if sent_message is not None and self.funnel_manager.register_welcome(user_id, user.first_name or "", chat.id):
            self.funnel_manager.schedule_followups(user_id)
            logger.info("Telegram funnel welcome sent for user_id=%s", user_id)
            return True
        return False

    def _private_offer_keyboard(self, page_label: str, checkout_label: str):
        return InlineKeyboardMarkup(
            [[
                InlineKeyboardButton(page_label, url=lovable_url(self.service.config)),
                InlineKeyboardButton(checkout_label, url=payment_link(self.service.config)),
            ]]
        )

    # Funnel follow-up 1: reminder after 1 hour.
    async def send_followup_1(self, chat_id: str) -> bool:
        return bool(await self.send_message(
            (
                "⏰ Passando para te lembrar:\n\n"
                "No VIP você recebe sinais durante TODO o dia, com mais frequência e acompanhamento muito mais completo do que no free.\n\n"
                "Muitos membros entram justamente para não depender apenas das janelas gratuitas."
            ),
            reply_markup=self._private_offer_keyboard("Conhecer benefícios", "Garantir acesso"),
            chat_id=chat_id,
        ))

    # Funnel follow-up 2: urgency after 6 hours.
    async def send_followup_2(self, chat_id: str) -> bool:
        return bool(await self.send_message(
            (
                "🔥 Muitos traders entram no VIP no mesmo dia em que conhecem o grupo free.\n\n"
                "Quanto antes você entrar, mais sinais e oportunidades consegue aproveitar ainda hoje."
            ),
            reply_markup=self._private_offer_keyboard("Ver página oficial", "Entrar agora"),
            chat_id=chat_id,
        ))

    # Funnel follow-up 3: final reminder after 24 hours.
    async def send_followup_3(self, chat_id: str) -> bool:
        return bool(await self.send_message(
            (
                "🚨 Último lembrete da oferta vitalícia promocional do MAGNATAZ VIP.\n\n"
                "Você pode garantir seu acesso por apenas:\n\n"
                "⭐ R$ 49,90 vitalício\n\n"
                "Recebendo sinais, análises, lives e atualizações exclusivas da comunidade MagnataZ."
            ),
            reply_markup=self._private_offer_keyboard("Ver todos os detalhes", "Ir para o checkout"),
            chat_id=chat_id,
        ))

    def _capture_chat_id(self, update: Update) -> None:
        if update.effective_chat:
            current_chat_id = str(update.effective_chat.id)
            self.last_user_chat_id = current_chat_id
            chat_type = str(getattr(update.effective_chat, "type", "") or "").lower()
            if self.chat_id is None:
                self.chat_id = current_chat_id
                if chat_type in {"group", "supergroup", "channel"}:
                    self._persist_group_route(update.effective_chat)
                return
            if chat_type in {"group", "supergroup", "channel"}:
                self.chat_id = current_chat_id
                self._persist_group_route(update.effective_chat)
                return
            if current_chat_id.startswith("-"):
                self.chat_id = current_chat_id

    def _configured_group_chat_id(self) -> str:
        tier = str(getattr(self.service.config, "group_tier", "free") or "free").strip().lower()
        legacy_chat_id = str(getattr(self.service.config, "telegram_chat_id", "") or "").strip()
        free_chat_id = str(getattr(self.service.config, "free_telegram_chat_id", "") or "").strip()
        vip_chat_id = str(getattr(self.service.config, "vip_telegram_chat_id", "") or "").strip()

        if tier == "free" and free_chat_id:
            return free_chat_id
        if tier == "vip" and vip_chat_id:
            return vip_chat_id
        if tier == "free" and vip_chat_id and legacy_chat_id == vip_chat_id and not free_chat_id:
            return ""
        if tier == "vip" and free_chat_id and legacy_chat_id == free_chat_id and not vip_chat_id:
            return ""
        return legacy_chat_id

    def _configured_free_group_chat_id(self) -> str:
        free_chat_id = str(getattr(self.service.config, "free_telegram_chat_id", "") or "").strip()
        if free_chat_id:
            return free_chat_id
        if str(getattr(self.service.config, "group_tier", "free") or "free").strip().lower() == "free":
            return str(getattr(self.service.config, "telegram_chat_id", "") or "").strip()
        return ""

    def _is_free_group_update(self, update: Update) -> bool:
        chat = getattr(update, "effective_chat", None)
        if chat is None:
            return False
        chat_type = str(getattr(chat, "type", "") or "").lower()
        if chat_type not in {"group", "supergroup"}:
            return False
        configured_chat_id = self._configured_free_group_chat_id()
        if not configured_chat_id:
            return False
        return str(getattr(chat, "id", "") or "").strip() == configured_chat_id

    def _group_route_key(self, chat) -> str:
        title = str(getattr(chat, "title", "") or getattr(chat, "username", "") or "").strip().lower()
        if any(keyword in title for keyword in ("vip", "premium", "premuim")):
            return "vip_telegram_chat_id"
        if any(keyword in title for keyword in ("free", "gratis", "gratuito")):
            return "free_telegram_chat_id"
        return "free_telegram_chat_id"

    def _persist_group_route(self, chat) -> None:
        route_key = self._group_route_key(chat)
        current_chat_id = str(getattr(chat, "id", "") or "").strip()
        if not current_chat_id:
            return
        if str(getattr(self.service.config, route_key, "") or "").strip() == current_chat_id:
            return
        updates = {route_key: current_chat_id}
        if route_key == "free_telegram_chat_id" and not str(getattr(self.service.config, "telegram_chat_id", "") or "").strip():
            updates["telegram_chat_id"] = current_chat_id
        if self.service.update_config(**updates):
            logger.info("Telegram route captured: %s=%s", route_key, current_chat_id)
        else:
            logger.warning("Failed to persist Telegram route: %s=%s", route_key, current_chat_id)

    def reset_free_window_state_if_needed(self) -> None:
        if str(getattr(self.service.config, "group_tier", "free")).strip().lower() != "free":
            return
        self.free_promo_tracker.reset_free_window_state_if_needed()

    def get_current_free_window(self) -> Optional[str]:
        if str(getattr(self.service.config, "group_tier", "free")).strip().lower() != "free":
            return None
        current_window = self.window_scheduler.get_current_window()
        return current_window.start if current_window is not None else None

    def increment_free_signal_counter(self):
        if str(getattr(self.service.config, "group_tier", "free")).strip().lower() != "free":
            return self.free_promo_tracker.should_send_vip_promo()
        return self.free_promo_tracker.increment_free_signal_counter()

    def can_send_free_window_signal(self) -> tuple[bool, str]:
        if str(getattr(self.service.config, "group_tier", "free")).strip().lower() != "free":
            return True, "ok"
        current_window = self.get_current_free_window()
        if current_window is None:
            return False, "outside_free_window"
        date_key = now_br().date().isoformat()
        if self.window_store.count_window_signals(date_key, current_window) >= AUTO_VIP_PROMO_EVERY_SIGNALS:
            return False, "free_window_limit_reached"
        return True, "ok"

    def should_send_vip_promo(self):
        if str(getattr(self.service.config, "group_tier", "free")).strip().lower() != "free":
            return self.free_promo_tracker.should_send_vip_promo()
        return self.free_promo_tracker.should_send_vip_promo()

    def mark_vip_promo_sent(self, window_key: Optional[str]) -> None:
        if str(getattr(self.service.config, "group_tier", "free")).strip().lower() != "free":
            return
        self.free_promo_tracker.mark_promo_sent(window_key)

    def increment_signal_counter(self) -> bool:
        """Compatibility wrapper returning whether the promo threshold was reached."""
        return bool(self.increment_free_signal_counter().should_send_promo)

    def reset_signal_counter(self) -> None:
        """Compatibility wrapper kept for older callers."""
        self.reset_free_window_state_if_needed()

    def _history_text(self) -> str:
        history = self.service.history_store.load_dataframe()
        if history.empty:
            return "Nenhum histórico disponível."

        table = build_history_table(history, limit=10)
        lines = ["*Últimos 10 sinais*\n"]
        for _, row in table.iterrows():
            action = row["action"]
            emoji = "🟢" if action == "COMPRA" else "🔴" if action == "VENDA" else "🟡"
            lines.append(
                f"{emoji} `{row['date']} {row['time']}` | `{row['asset']}` | *{action}* | `{row['score']}` | *{row['result']}*"
            )
        return "\n".join(lines)

    def _results_text(self) -> str:
        history = ensure_history_schema(self.service.history_store.load_dataframe())
        decisive = history[history["result"].isin(["WIN", "LOSS"])].copy()
        if decisive.empty:
            return "Nenhum WIN/LOSS resolvido ainda."

        decisive["sort_ts"] = decisive["timestamp"]
        decisive = decisive.sort_values("sort_ts", ascending=False).head(10)
        lines = ["*ÚLTIMOS RESULTADOS*\n"]
        for _, row in decisive.iterrows():
            emoji = "✅" if row["result"] == "WIN" else "❌"
            lines.append(
                f"{emoji} `{row['date']} {row['time']}` | `{row['asset']}` | *{row['action']}* | *{row['result']}*"
            )
        return "\n".join(lines)

async def start_bot(token: str, service: OBCCashService, chat_id: Optional[str] = None):
    """Start the Telegram bot and return the instance."""
    bot = OBCCashBot(token, service)
    await bot.start(chat_id)
    return bot
