from __future__ import absolute_import
"""
Telegram notification helpers.
"""

import threading
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import requests

from obcash3.bot.commercial import (
    AUTO_VIP_PROMO_EVERY_SIGNALS,
    build_auto_vip_promo_message,
    build_free_welcome_message,
    build_post_payment_dm_message,
    build_vip_offer_message,
)
from obcash3.bot.promo_tracker import FreeGroupPromoTracker
from obcash3.utils.logger import get_logger
from obcash3.utils.time import next_candle_start, now_br, to_brt_datetime

logger = get_logger(__name__)


def _signal_attr(signal: Any, name: str, default: Any = "") -> Any:
    if isinstance(signal, dict):
        return signal.get(name, default)
    return getattr(signal, name, default)


def _safe_markdown_text(value: Any) -> str:
    return str(value or "").replace("`", "'").replace("*", "").replace("_", "-").strip()


def _interval_label(interval: str) -> str:
    return {
        "1m": "1 minuto",
        "5m": "5 minutos",
        "15m": "15 minutos",
        "30m": "30 minutos",
        "1h": "1 hora",
    }.get(str(interval), str(interval))


def _action_emoji(action: str) -> str:
    return {
        "COMPRA": "\U0001F7E2",
        "VENDA": "\U0001F534",
    }.get(str(action).upper(), "\u26AA")


def _strength_emoji(strength: str) -> str:
    return {
        "FORTE": "\U0001F525",
        "MODERADO": "\u26A1",
        "FRACO": "\u26AA",
    }.get(str(strength).upper(), "\u26A1")


def _confidence_emoji(label: str) -> str:
    return {
        "Alta confianca": "\U0001F6E1",
        "Media confianca": "\U0001F4A0",
        "Baixa confianca": "\U0001F50E",
        "Evitar": "\u26D4",
    }.get(str(label), "\U0001F6E1")


def bankroll_suggestions() -> Dict[str, str]:
    return {
        "conservadora": "2%",
        "moderada": "5%",
        "agressiva": "10%",
    }


def _entry_time_label(signal: Any) -> str:
    entry_time = _signal_attr(signal, "entry_time", "")
    if entry_time:
        try:
            return to_brt_datetime(entry_time).strftime("%H:%M")
        except Exception:
            pass

    signal_timestamp = _signal_attr(signal, "timestamp", None)
    interval = str(_signal_attr(signal, "interval", "") or "")
    try:
        if interval in {"1m", "5m", "15m", "30m", "1h"}:
            return next_candle_start(interval, base_time=signal_timestamp).strftime("%H:%M")
    except Exception:
        logger.debug("Failed to derive entry time from signal timestamp", exc_info=True)
    return now_br().strftime("%H:%M")


def build_signal_message(signal: Any, message_mode: str = "vip") -> str:
    """Build a signal message in free or vip mode."""
    mode = str(message_mode or "vip").strip().lower()
    action = str(_signal_attr(signal, "action", "AGUARDAR"))
    strength = str(_signal_attr(signal, "strength", "NEUTRO"))
    asset = _safe_markdown_text(_signal_attr(signal, "asset", "-"))
    interval = _signal_attr(signal, "interval", "-")
    score = float(_signal_attr(signal, "score", 0.0) or 0.0)
    sl = _signal_attr(signal, "sl", None)
    tp = _signal_attr(signal, "tp", None)
    confidence_label = _safe_markdown_text(_signal_attr(signal, "confidence_label", "Media confianca"))
    note = _safe_markdown_text(_signal_attr(signal, "note", "")) or _safe_markdown_text(_signal_attr(signal, "filters", [""])[0] if isinstance(_signal_attr(signal, "filters", []), list) and _signal_attr(signal, "filters", []) else "")
    entry_time = _entry_time_label(signal)

    if mode == "free":
        lines = [
            "\U0001F6A8 *NOVO SINAL FREE* \U0001F6A8",
            "",
            f"{_action_emoji(action)} *{action}* | `{asset}`",
            f"\u23F1 `Vela de duracao {_interval_label(str(interval))}`",
            f"{_strength_emoji(strength)} *{strength}*",
            f"{_confidence_emoji(confidence_label)} *{confidence_label}*",
            f"\U0001F4CA `{score:.1f}%`",
            "",
            f"\u23F0 *Entrar na vela das* `{entry_time}`",
            "_MagnataZ Free_",
        ]
        if note:
            lines.insert(-2, f"\U0001F4DD _Observacao:_ {note}")
        return "\n".join(lines)

    tp_str = f"{tp:.5f}" if isinstance(tp, (int, float)) else "---"
    sl_str = f"{sl:.5f}" if isinstance(sl, (int, float)) else "---"
    bankroll = bankroll_suggestions()
    lines = [
        "\U0001F6A8 *NOVO SINAL VIP* \U0001F6A8",
        "",
        f"{_action_emoji(action)} *{action}* | `{asset}`",
        f"\u23F1 `Vela de duracao {_interval_label(str(interval))}`",
        f"{_strength_emoji(strength)} *{strength}*",
        f"{_confidence_emoji(confidence_label)} *{confidence_label}*",
        f"\U0001F4CA `{score:.1f}%`",
        "",
        f"*TP:* `{tp_str}`",
        f"*SL:* `{sl_str}`",
        "",
        (
            f"\U0001F4BC *Entrada sugerida:* "
            f"`Cons. {bankroll['conservadora']} | Mod. {bankroll['moderada']} | Agr. {bankroll['agressiva']}`"
        ),
        f"\u23F0 *Entrar na vela das* `{entry_time}`",
        "_MagnataZ VIP_",
    ]
    if note:
        lines.insert(10, f"\U0001F4DD _Observacao:_ {note}")
    return "\n".join(lines)


def build_daily_summary_message(summary: Dict[str, Any], message_mode: str = "vip") -> str:
    """Build a daily summary message."""
    brand = "MagnataZ VIP" if str(message_mode).lower() == "vip" else "MagnataZ Free"
    best_pairs = summary.get("best_pairs", [])
    best_pairs_text = ", ".join(best_pairs[:3]) if best_pairs else "-"
    return "\n".join(
        [
            "\U0001F4CA *RESUMO DO DIA*",
            "",
            f"Wins: `{int(summary.get('wins', 0))}`",
            f"Losses: `{int(summary.get('losses', 0))}`",
            f"Assertividade: `{float(summary.get('win_rate', 0.0)):.1f}%`",
            f"Melhores pares: `{best_pairs_text}`",
            "",
            f"_{brand}_",
        ]
    )


def build_dashboard_message(metrics: Dict[str, Any]) -> str:
    """Build a compact dashboard text for bot commands."""
    return "\n".join(
        [
            "\U0001F4C8 *DASHBOARD*",
            "",
            f"Total: `{int(metrics.get('total_signals', 0))}`",
            f"Wins: `{int(metrics.get('wins', 0))}`",
            f"Losses: `{int(metrics.get('losses', 0))}`",
            f"Win Rate: `{float(metrics.get('win_rate', 0.0)):.1f}%`",
            f"Sinais do dia: `{int(metrics.get('signals_today', 0))}`",
            f"Melhor par: `{metrics.get('best_pair', '-')}`",
            f"Pior par: `{metrics.get('worst_pair', '-')}`",
            f"Melhor timeframe: `{metrics.get('best_timeframe', '-')}`",
            f"Sequencia wins: `{int(metrics.get('current_win_streak', 0))}`",
            f"Sequencia losses: `{int(metrics.get('current_loss_streak', 0))}`",
        ]
    )


def build_pause_message(state: Dict[str, Any]) -> str:
    return "\n".join(
        [
            "\u26A0\uFE0F *PAUSA AUTOMATICA ATIVADA*",
            "",
            f"Motivo: {state.get('reason', 'protecao operacional')}",
            "O sistema pausou temporariamente os sinais para protecao operacional.",
            "_MagnataZ VIP_",
        ]
    )


def build_social_proof_message(kind: str, payload: Dict[str, Any]) -> str:
    if kind == "streak":
        return "\n".join(
            [
                "\U0001F525 *SEQUENCIA POSITIVA*",
                "",
                f"`{int(payload.get('streak', 0))} WINS SEGUIDOS HOJE`",
                "_MagnataZ VIP_",
            ]
        )

    return "\n".join(
        [
            "\U0001F4CA *RESULTADO PARCIAL DO DIA*",
            "",
            f"Wins: `{int(payload.get('wins', 0))}`",
            f"Losses: `{int(payload.get('losses', 0))}`",
            f"Assertividade: `{float(payload.get('win_rate', 0.0)):.1f}%`",
            f"Melhor par: `{_safe_markdown_text(payload.get('best_pair', '-'))}`",
            "_MagnataZ VIP_",
        ]
    )


def build_welcome_message(config: Any) -> str:
    """Legacy wrapper kept for compatibility with old callers."""
    return build_free_welcome_message(config)


def build_vip_pitch_message(config: Any) -> str:
    """Legacy wrapper kept for compatibility with old callers."""
    return build_vip_offer_message(config)


def build_post_payment_message(config: Any) -> str:
    """Legacy wrapper kept for compatibility with old callers."""
    return build_post_payment_dm_message(config)


def _time_in_window(now_value: datetime, window: str) -> bool:
    """Check whether a BRT time falls inside a HH:MM-HH:MM window."""
    if not window:
        return True

    try:
        start_str, end_str = window.split("-", 1)
        start_hour, start_minute = [int(item) for item in start_str.split(":", 1)]
        end_hour, end_minute = [int(item) for item in end_str.split(":", 1)]
    except Exception:
        return True

    current_minutes = now_value.hour * 60 + now_value.minute
    start_minutes = start_hour * 60 + start_minute
    end_minutes = end_hour * 60 + end_minute

    if start_minutes <= end_minutes:
        return start_minutes <= current_minutes <= end_minutes
    return current_minutes >= start_minutes or current_minutes <= end_minutes


class TelegramNotifier:
    """Synchronous Telegram sender with templates, anti-spam and pause/social-proof messages."""

    STRENGTH_RANK = {
        "FRACO": 0,
        "MODERADO": 1,
        "FORTE": 2,
    }

    def __init__(
        self,
        token: str = "",
        chat_id: str = "",
        free_chat_id: str = "",
        vip_chat_id: str = "",
        enabled: bool = True,
        group_tier: str = "free",
        message_mode: str = "vip",
        timeout: int = 10,
        dedupe_window_seconds: int = 180,
        min_strength: str = "FORTE",
        min_confidence: float = 68.0,
        min_score: float = 70.0,
        send_only_strong: bool = True,
        min_signal_interval_seconds: int = 180,
        allowed_pairs: Optional[list[str]] = None,
        allowed_hours: str = "00:00-23:59",
    ):
        self.timeout = timeout
        self.dedupe_window = timedelta(seconds=dedupe_window_seconds)
        self._lock = threading.Lock()
        self._recent_messages: Dict[str, datetime] = {}
        self._recent_pairs: Dict[str, datetime] = {}
        self._pending_config_updates: Dict[str, str] = {}
        self.free_promo_tracker = FreeGroupPromoTracker()
        self.configure(
            token=token,
            chat_id=chat_id,
            free_chat_id=free_chat_id,
            vip_chat_id=vip_chat_id,
            enabled=enabled,
            group_tier=group_tier,
            message_mode=message_mode,
            min_strength=min_strength,
            min_confidence=min_confidence,
            min_score=min_score,
            send_only_strong=send_only_strong,
            min_signal_interval_seconds=min_signal_interval_seconds,
            allowed_pairs=allowed_pairs or [],
            allowed_hours=allowed_hours,
        )

    @classmethod
    def from_config(cls, config: Any) -> "TelegramNotifier":
        return cls(
            token=getattr(config, "telegram_token", "") if config is not None else "",
            chat_id=getattr(config, "telegram_chat_id", "") if config is not None else "",
            free_chat_id=getattr(config, "free_telegram_chat_id", "") if config is not None else "",
            vip_chat_id=getattr(config, "vip_telegram_chat_id", "") if config is not None else "",
            enabled=bool(getattr(config, "telegram_enabled", True)) if config is not None else True,
            group_tier=str(getattr(config, "group_tier", "free")) if config is not None else "free",
            message_mode=str(getattr(config, "message_mode", "vip")) if config is not None else "vip",
            min_strength=getattr(config, "telegram_min_strength", "FORTE") if config is not None else "FORTE",
            min_score=float(getattr(config, "min_score", 70.0)) if config is not None else 70.0,
            send_only_strong=bool(getattr(config, "send_only_strong", True)) if config is not None else True,
            min_signal_interval_seconds=int(getattr(config, "min_signal_interval_seconds", 180)) if config is not None else 180,
            allowed_pairs=list(getattr(config, "allowed_pairs", []) or []) if config is not None else [],
            allowed_hours=str(getattr(config, "allowed_hours", "00:00-23:59")) if config is not None else "00:00-23:59",
        )

    def configure(
        self,
        token: str = "",
        chat_id: str = "",
        free_chat_id: str = "",
        vip_chat_id: str = "",
        enabled: bool = True,
        group_tier: str = "free",
        message_mode: str = "vip",
        min_strength: str = "FORTE",
        min_confidence: float = 68.0,
        min_score: float = 70.0,
        send_only_strong: bool = True,
        min_signal_interval_seconds: int = 180,
        allowed_pairs: Optional[list[str]] = None,
        allowed_hours: str = "00:00-23:59",
    ) -> None:
        self.token = (token or "").strip()
        self.legacy_chat_id = str(chat_id or "").strip()
        self.free_chat_id = str(free_chat_id or "").strip()
        self.vip_chat_id = str(vip_chat_id or "").strip()
        self.enabled = bool(enabled)
        self.group_tier = str(group_tier or "free").strip().lower()
        self.message_mode = str(message_mode or "vip").strip().lower()
        normalized = str(min_strength or "FORTE").strip().upper()
        self.min_strength = normalized if normalized in self.STRENGTH_RANK else "FORTE"
        self.min_confidence = float(min_confidence or 68.0)
        self.min_score = float(min_score or 70.0)
        self.send_only_strong = bool(send_only_strong)
        self.min_signal_interval_seconds = int(min_signal_interval_seconds or 180)
        self.allowed_pairs = [str(item).strip().upper().replace("-", "/") for item in (allowed_pairs or []) if str(item).strip()]
        self.allowed_hours = str(allowed_hours or "00:00-23:59").strip() or "00:00-23:59"
        self.chat_id = self._resolve_default_chat_id()

    def _resolve_default_chat_id(self) -> str:
        tier = str(self.group_tier or "").strip().lower()
        if tier == "free" and self.free_chat_id:
            return self.free_chat_id
        if tier == "vip" and self.vip_chat_id:
            return self.vip_chat_id
        if tier == "free" and self.vip_chat_id and self.legacy_chat_id == self.vip_chat_id and not self.free_chat_id:
            return ""
        if tier == "vip" and self.free_chat_id and self.legacy_chat_id == self.free_chat_id and not self.vip_chat_id:
            return ""
        return self.legacy_chat_id

    def _active_chat_config_key(self) -> str:
        tier = str(self.group_tier or "").strip().lower()
        if tier == "free" and self.free_chat_id:
            return "free_telegram_chat_id"
        if tier == "vip" and self.vip_chat_id:
            return "vip_telegram_chat_id"
        if tier == "free" and not self.free_chat_id:
            return "free_telegram_chat_id"
        if tier == "vip" and not self.vip_chat_id:
            return "vip_telegram_chat_id"
        return "telegram_chat_id"

    def _apply_routed_chat_id(self, chat_id: str) -> None:
        normalized_chat_id = str(chat_id or "").strip()
        target_key = self._active_chat_config_key()
        if target_key == "free_telegram_chat_id":
            self.free_chat_id = normalized_chat_id
        elif target_key == "vip_telegram_chat_id":
            self.vip_chat_id = normalized_chat_id
        else:
            self.legacy_chat_id = normalized_chat_id
        self._pending_config_updates[target_key] = normalized_chat_id
        self.chat_id = self._resolve_default_chat_id()

    def consume_config_updates(self) -> Dict[str, str]:
        updates = dict(self._pending_config_updates)
        self._pending_config_updates.clear()
        return updates

    @property
    def is_configured(self) -> bool:
        return bool(self.enabled and self.token and self._resolve_default_chat_id())

    def send_text(self, text: str, dedupe_key: str = "", parse_mode: str = "Markdown") -> bool:
        """Send a raw text message to the configured chat."""
        if not self.is_configured:
            logger.warning(
                "Telegram not configured or disabled; skipping send (tier=%s legacy=%s free=%s vip=%s)",
                self.group_tier,
                bool(self.legacy_chat_id),
                bool(self.free_chat_id),
                bool(self.vip_chat_id),
            )
            return False

        if dedupe_key and self._was_sent_recently(dedupe_key):
            logger.info("Telegram duplicate suppressed: %s", dedupe_key)
            return False

        target_chat_id = self._resolve_default_chat_id()
        self.chat_id = target_chat_id
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": target_chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode

        try:
            response = requests.post(url, json=payload, timeout=self.timeout)
            ok, error_message, migrated_chat_id = self._parse_response(response)
            if not ok:
                if migrated_chat_id and migrated_chat_id != target_chat_id:
                    old_chat_id = target_chat_id
                    self._apply_routed_chat_id(migrated_chat_id)
                    logger.warning(
                        "Telegram group migrated from %s to %s; retrying send",
                        old_chat_id,
                        migrated_chat_id,
                    )
                    payload["chat_id"] = self.chat_id
                    response = requests.post(url, json=payload, timeout=self.timeout)
                    ok, error_message, migrated_chat_id = self._parse_response(response)

                if not ok:
                    logger.error("Telegram send failed for chat_id=%s: %s", target_chat_id, error_message)
                    return False

            if dedupe_key:
                self._mark_sent(dedupe_key)
            return True
        except Exception as exc:
            logger.error("Failed to send Telegram message: %s", exc, exc_info=True)
            return False

    def is_free_group_mode(self) -> bool:
        """Promotional cadence is only active for the free group flow."""
        return str(self.group_tier or "").strip().lower() == "free"

    def reset_free_window_state_if_needed(self) -> None:
        if not self.is_free_group_mode():
            return
        self.free_promo_tracker.reset_free_window_state_if_needed()

    def get_current_free_window(self) -> Optional[str]:
        if not self.is_free_group_mode():
            return None
        return self.free_promo_tracker.get_current_free_window()

    def increment_free_signal_counter(self):
        if not self.is_free_group_mode():
            return self.free_promo_tracker.should_send_vip_promo(current_time=now_br())
        return self.free_promo_tracker.increment_free_signal_counter()

    def should_send_vip_promo(self):
        if not self.is_free_group_mode():
            return self.free_promo_tracker.should_send_vip_promo(current_time=now_br())
        return self.free_promo_tracker.should_send_vip_promo()

    def mark_vip_promo_sent(self, window_key: Optional[str]) -> None:
        if not self.is_free_group_mode():
            return
        self.free_promo_tracker.mark_promo_sent(window_key)

    def increment_signal_counter(self) -> bool:
        """Compatibility wrapper returning whether the promo threshold was reached."""
        return bool(self.increment_free_signal_counter().should_send_promo)

    def reset_signal_counter(self) -> None:
        """Compatibility wrapper kept for older callers."""
        self.reset_free_window_state_if_needed()

    def send_vip_offer(self) -> bool:
        """Send the full VIP offer text to the current chat."""
        return self.send_text(build_vip_offer_message(getattr(self, "config", None)), parse_mode=None)

    def send_free_vip_promo(self) -> bool:
        """Send the free-group VIP CTA in the active promotional window."""
        if not self.is_free_group_mode():
            return False
        return self.send_text(
            build_auto_vip_promo_message(getattr(self, "config", None)),
            parse_mode=None,
        )

    def send_auto_vip_promo(self) -> bool:
        """Compatibility wrapper for the free-group VIP CTA."""
        return self.send_free_vip_promo()

    def can_send_signal(self, signal: Any) -> Tuple[bool, str]:
        """Check whether a signal clears the premium anti-spam and quality filters."""
        if not self.is_configured:
            return False, "not_configured"

        action = str(_signal_attr(signal, "action", "")).upper()
        strength = str(_signal_attr(signal, "strength", "")).upper()
        score = float(_signal_attr(signal, "score", 0.0) or 0.0)
        confidence_score = float(_signal_attr(signal, "confidence_score", 0.0) or 0.0)
        policy_state = str(_signal_attr(signal, "policy_state", "neutral") or "neutral")
        asset = str(_signal_attr(signal, "asset", "")).upper().replace("-", "/")

        if action not in ("COMPRA", "VENDA"):
            return False, "invalid_action"
        if self.send_only_strong and strength != "FORTE":
            return False, "only_strong"
        if strength not in self.STRENGTH_RANK:
            return False, "invalid_strength"
        if self.STRENGTH_RANK[strength] < self.STRENGTH_RANK.get(self.min_strength, 2):
            return False, "strength_filter"
        if score < self.min_score:
            return False, "score_filter"
        if confidence_score < self.min_confidence:
            return False, "confidence_filter"
        if policy_state == "blocked":
            return False, "policy_blocked"
        if self.allowed_pairs and asset not in self.allowed_pairs:
            return False, "pair_not_allowed"
        if not _time_in_window(now_br(), self.allowed_hours):
            return False, "outside_allowed_hours"
        if self.is_free_group_mode():
            current_window = self.get_current_free_window()
            if current_window is None:
                logger.info("Free signal blocked: outside configured free windows")
                return False, "outside_free_window"
            window_state = self.free_promo_tracker.get_window_state(current_window)
            if int(window_state.get("signals", 0)) >= AUTO_VIP_PROMO_EVERY_SIGNALS:
                logger.info(
                    "Free signal blocked: window=%s already reached limit=%d",
                    current_window,
                    int(window_state.get("signals", 0)),
                )
                return False, "free_window_limit_reached"
        if self._pair_on_cooldown(asset):
            return False, "pair_cooldown"
        return True, "ok"

    def send_signal(self, signal: Any, dedupe_key: str = "") -> bool:
        """Send a formatted signal alert only when the signal clears quality filters."""
        allowed, reason = self.can_send_signal(signal)
        if not allowed:
            logger.debug("Telegram signal suppressed: %s", reason)
            return False

        if not dedupe_key:
            dedupe_key = self._build_dedupe_key(signal)

        sent = self.send_text(
            build_signal_message(signal, message_mode=self.message_mode),
            dedupe_key=dedupe_key,
        )
        if sent:
            self._mark_pair_sent(str(_signal_attr(signal, "asset", "")))
            decision = self.increment_free_signal_counter()
            if decision.window_key:
                logger.info(
                    "Free window signal registered: window=%s count=%d",
                    decision.window_key,
                    decision.signal_count,
                )
            if decision.should_send_promo:
                promo_sent = self.send_free_vip_promo()
                if promo_sent:
                    self.mark_vip_promo_sent(decision.window_key)
                    logger.info(
                        "Free VIP promo sent: window=%s count=%d",
                        decision.window_key,
                        decision.signal_count,
                    )
        return sent

    def send_daily_summary(self, summary: Dict[str, Any], dedupe_key: str) -> bool:
        """Send a daily summary message."""
        return self.send_text(
            build_daily_summary_message(summary, message_mode=self.message_mode),
            dedupe_key=dedupe_key,
        )

    def send_pause_alert(self, state: Dict[str, Any]) -> bool:
        """Send the automatic pause warning."""
        dedupe_key = f"pause-alert|{state.get('reason', '')}"
        return self.send_text(build_pause_message(state), dedupe_key=dedupe_key)

    def send_social_proof(self, kind: str, payload: Dict[str, Any], dedupe_key: str) -> bool:
        """Send automated social proof messages."""
        return self.send_text(build_social_proof_message(kind, payload), dedupe_key=dedupe_key)

    def _pair_on_cooldown(self, asset: str) -> bool:
        now_value = datetime.now()
        with self._lock:
            cutoff = now_value - timedelta(seconds=self.min_signal_interval_seconds)
            self._recent_pairs = {
                key: sent_at
                for key, sent_at in self._recent_pairs.items()
                if sent_at >= cutoff
            }
            return asset in self._recent_pairs

    def _mark_pair_sent(self, asset: str) -> None:
        normalized = str(asset or "").upper().replace("-", "/")
        with self._lock:
            self._recent_pairs[normalized] = datetime.now()

    def _was_sent_recently(self, dedupe_key: str) -> bool:
        now_value = datetime.now()
        with self._lock:
            cutoff = now_value - self.dedupe_window
            self._recent_messages = {
                key: sent_at
                for key, sent_at in self._recent_messages.items()
                if sent_at >= cutoff
            }
            return dedupe_key in self._recent_messages

    def _mark_sent(self, dedupe_key: str) -> None:
        with self._lock:
            self._recent_messages[dedupe_key] = datetime.now()

    def _build_dedupe_key(self, signal: Any) -> str:
        price = _signal_attr(signal, "price", 0.0)
        return "|".join(
            [
                str(_signal_attr(signal, "asset", "")),
                str(_signal_attr(signal, "interval", "")),
                str(_signal_attr(signal, "action", "")),
                f"{float(_signal_attr(signal, 'score', 0.0) or 0.0):.1f}",
                f"{float(price or 0.0):.5f}",
            ]
        )

    def _parse_response(self, response: requests.Response) -> Tuple[bool, str, Optional[str]]:
        """Parse the Telegram API response without losing useful error details."""
        data: Dict[str, Any] = {}
        try:
            data = response.json()
        except Exception:
            data = {}

        if response.ok and data.get("ok", True):
            return True, "", None

        description = ""
        migrated_chat_id: Optional[str] = None
        if data:
            description = str(data.get("description") or data)
            parameters = data.get("parameters") or {}
            migrate_to_chat_id = parameters.get("migrate_to_chat_id")
            if migrate_to_chat_id:
                migrated_chat_id = str(migrate_to_chat_id)
        else:
            description = response.text.strip()

        error_message = f"HTTP {response.status_code}"
        if description:
            error_message = f"{error_message} - {description}"
        return False, error_message, migrated_chat_id
