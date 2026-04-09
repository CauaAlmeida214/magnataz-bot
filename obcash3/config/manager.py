from __future__ import absolute_import
"""
Configuration management for the standalone MagnataZ bot.

Supports optional local config files plus environment-variable overrides for
GitHub/Render deployments.
"""

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from obcash3.config.settings import CONFIG_PATH, DEFAULT_CONFIG
from obcash3.config.validator import ConfigValidator, ValidationResult
from obcash3.utils.logger import get_logger

logger = get_logger(__name__)


def _env_str(*names: str) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return None


def _env_bool(*names: str) -> Optional[bool]:
    value = _env_str(*names)
    if value is None:
        return None
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(*names: str) -> Optional[int]:
    value = _env_str(*names)
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _env_float(*names: str) -> Optional[float]:
    value = _env_str(*names)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _env_list(*names: str) -> Optional[list[str]]:
    value = _env_str(*names)
    if value is None:
        return None
    items = [item.strip().upper().replace("-", "/") for item in value.split(",") if item.strip()]
    return items


def load_environment_overrides() -> Dict[str, Any]:
    """Read optional production overrides from environment variables."""
    overrides: Dict[str, Any] = {}

    string_mappings = {
        "twelve_api_key": ("TWELVE_API_KEY", "TWELVE_DATA_API_KEY"),
        "av_api_key": ("ALPHA_VANTAGE_API_KEY", "AV_API_KEY"),
        "telegram_token": ("BOT_TOKEN", "TELEGRAM_TOKEN"),
        "telegram_chat_id": ("TELEGRAM_CHAT_ID",),
        "free_telegram_chat_id": ("FREE_GROUP_ID",),
        "vip_telegram_chat_id": ("VIP_GROUP_ID",),
        "group_tier": ("GROUP_TIER",),
        "message_mode": ("MESSAGE_MODE",),
        "allowed_hours": ("ALLOWED_HOURS",),
        "daily_summary_time": ("DAILY_SUMMARY_TIME",),
        "telegram_min_strength": ("TELEGRAM_MIN_STRENGTH",),
        "free_group_link": ("FREE_GROUP_LINK",),
        "private_welcome_link": ("PRIVATE_WELCOME_LINK",),
        "vip_payment_link": ("KIWIFY_URL", "VIP_PAYMENT_LINK", "PAYMENT_LINK"),
        "payment_link": ("KIWIFY_URL", "PAYMENT_LINK", "VIP_PAYMENT_LINK"),
        "lovable_url": ("LOVABLE_URL",),
        "vip_group_link": ("VIP_GROUP_LINK",),
    }
    for config_key, env_names in string_mappings.items():
        value = _env_str(*env_names)
        if value is not None:
            overrides[config_key] = value

    bool_mappings = {
        "telegram_enabled": ("TELEGRAM_ENABLED",),
        "send_only_strong": ("SEND_ONLY_STRONG",),
        "auto_pause_enabled": ("AUTO_PAUSE_ENABLED",),
        "social_proof_enabled": ("SOCIAL_PROOF_ENABLED",),
        "daily_summary_enabled": ("DAILY_SUMMARY_ENABLED",),
        "filter_hours": ("FILTER_HOURS",),
        "mtf_confirm": ("MTF_CONFIRM",),
        "divergence_detect": ("DIVERGENCE_DETECT",),
        "simple_mode": ("SIMPLE_MODE",),
        "show_advanced_panel": ("SHOW_ADVANCED_PANEL",),
        "adaptive_filtering": ("ADAPTIVE_FILTERING",),
        "global_scan_enabled": ("GLOBAL_SCAN_ENABLED",),
        "ml_enabled": ("ML_ENABLED",),
        "use_optimized": ("USE_OPTIMIZED",),
    }
    for config_key, env_names in bool_mappings.items():
        value = _env_bool(*env_names)
        if value is not None:
            overrides[config_key] = value

    int_mappings = {
        "min_signal_interval_seconds": ("MIN_SIGNAL_INTERVAL_SECONDS",),
        "max_consecutive_losses": ("MAX_CONSECUTIVE_LOSSES",),
        "social_proof_min_streak": ("SOCIAL_PROOF_MIN_STREAK",),
        "social_proof_min_decisive": ("SOCIAL_PROOF_MIN_DECISIVE",),
        "min_resolved_trades": ("MIN_RESOLVED_TRADES",),
        "ml_min_samples": ("ML_MIN_SAMPLES",),
    }
    for config_key, env_names in int_mappings.items():
        value = _env_int(*env_names)
        if value is not None:
            overrides[config_key] = value

    float_mappings = {
        "min_score": ("MIN_SCORE",),
        "min_daily_win_rate_pause": ("MIN_DAILY_WIN_RATE_PAUSE",),
        "social_proof_min_win_rate": ("SOCIAL_PROOF_MIN_WIN_RATE",),
        "account_balance": ("ACCOUNT_BALANCE",),
        "risk_pct": ("RISK_PCT", "RISK_PERCENT"),
        "min_win_rate": ("MIN_WIN_RATE",),
        "min_profit_factor": ("MIN_PROFIT_FACTOR",),
        "best_signal_min_score": ("BEST_SIGNAL_MIN_SCORE",),
        "ml_weight": ("ML_WEIGHT",),
    }
    for config_key, env_names in float_mappings.items():
        value = _env_float(*env_names)
        if value is not None:
            overrides[config_key] = value

    allowed_pairs = _env_list("ALLOWED_PAIRS")
    if allowed_pairs is not None:
        overrides["allowed_pairs"] = allowed_pairs

    favorite_pairs = _env_list("FAVORITE_PAIRS")
    if favorite_pairs is not None:
        overrides["favorite_pairs"] = favorite_pairs

    strategy_version = _env_str("STRATEGY_VERSION")
    if strategy_version is not None:
        overrides["strategy_version"] = strategy_version

    return overrides


@dataclass
class Config:
    """Typed configuration object."""

    twelve_api_key: str
    av_api_key: str
    telegram_token: str
    telegram_chat_id: str
    free_telegram_chat_id: str
    vip_telegram_chat_id: str
    telegram_enabled: bool
    group_tier: str
    message_mode: str
    min_score: float
    send_only_strong: bool
    min_signal_interval_seconds: int
    allowed_pairs: list[str]
    favorite_pairs: list[str]
    allowed_hours: str
    auto_pause_enabled: bool
    max_consecutive_losses: int
    min_daily_win_rate_pause: float
    social_proof_enabled: bool
    social_proof_min_streak: int
    social_proof_min_win_rate: float
    social_proof_min_decisive: int
    daily_summary_enabled: bool
    daily_summary_time: str
    account_balance: float
    risk_pct: float
    filter_hours: bool
    mtf_confirm: bool
    divergence_detect: bool
    simple_mode: bool = True
    show_advanced_panel: bool = False
    adaptive_filtering: bool = True
    telegram_min_strength: str = "FORTE"
    min_resolved_trades: int = 5
    min_win_rate: float = 0.52
    min_profit_factor: float = 1.05
    global_scan_enabled: bool = True
    best_signal_min_score: float = 70.0
    ml_enabled: bool = True
    ml_weight: float = 0.30
    ml_min_samples: int = 24
    free_group_link: str = ""
    private_welcome_link: str = ""
    vip_payment_link: str = ""
    payment_link: str = ""
    lovable_url: str = ""
    vip_group_link: str = ""
    post_purchase_message: str = ""
    post_payment_message: str = ""
    marketing_welcome_text: str = ""
    marketing_vip_pitch: str = ""
    marketing_testimonials: str = ""
    marketing_free_group_text: str = ""
    welcome_message: str = ""
    vip_pitch_message: str = ""
    use_optimized: bool = True
    strategy_version: str = "optimized"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Config":
        result = ConfigValidator.validate(data)
        if not result.is_valid:
            raise ValueError(f"Invalid configuration: {', '.join(result.errors)}")

        clean = ConfigValidator.sanitize(data)
        return cls(
            twelve_api_key=clean["twelve_api_key"],
            av_api_key=clean["av_api_key"],
            telegram_token=clean["telegram_token"],
            telegram_chat_id=clean["telegram_chat_id"],
            free_telegram_chat_id=str(clean.get("free_telegram_chat_id", "")),
            vip_telegram_chat_id=str(clean.get("vip_telegram_chat_id", "")),
            telegram_enabled=bool(clean.get("telegram_enabled", True)),
            group_tier=str(clean.get("group_tier", "free")),
            message_mode=str(clean.get("message_mode", "vip")),
            min_score=float(clean.get("min_score", 70.0)),
            send_only_strong=bool(clean.get("send_only_strong", True)),
            min_signal_interval_seconds=int(clean.get("min_signal_interval_seconds", 180)),
            allowed_pairs=list(clean.get("allowed_pairs", [])),
            favorite_pairs=list(clean.get("favorite_pairs", [])),
            allowed_hours=str(clean.get("allowed_hours", "00:00-23:59")),
            auto_pause_enabled=bool(clean.get("auto_pause_enabled", True)),
            max_consecutive_losses=int(clean.get("max_consecutive_losses", 3)),
            min_daily_win_rate_pause=float(clean.get("min_daily_win_rate_pause", 0.0)),
            social_proof_enabled=bool(clean.get("social_proof_enabled", True)),
            social_proof_min_streak=int(clean.get("social_proof_min_streak", 3)),
            social_proof_min_win_rate=float(clean.get("social_proof_min_win_rate", 75.0)),
            social_proof_min_decisive=int(clean.get("social_proof_min_decisive", 3)),
            daily_summary_enabled=bool(clean.get("daily_summary_enabled", True)),
            daily_summary_time=str(clean.get("daily_summary_time", "23:59")),
            account_balance=float(clean["account_balance"]),
            risk_pct=float(clean["risk_pct"]),
            filter_hours=bool(clean["filter_hours"]),
            mtf_confirm=bool(clean["mtf_confirm"]),
            divergence_detect=bool(clean["divergence_detect"]),
            simple_mode=bool(clean.get("simple_mode", True)),
            show_advanced_panel=bool(clean.get("show_advanced_panel", False)),
            adaptive_filtering=bool(clean.get("adaptive_filtering", True)),
            telegram_min_strength=str(clean.get("telegram_min_strength", "FORTE")),
            min_resolved_trades=int(clean.get("min_resolved_trades", 5)),
            min_win_rate=float(clean.get("min_win_rate", 0.52)),
            min_profit_factor=float(clean.get("min_profit_factor", 1.05)),
            global_scan_enabled=bool(clean.get("global_scan_enabled", True)),
            best_signal_min_score=float(clean.get("best_signal_min_score", clean.get("min_score", 70.0))),
            ml_enabled=bool(clean.get("ml_enabled", True)),
            ml_weight=float(clean.get("ml_weight", 0.30)),
            ml_min_samples=int(clean.get("ml_min_samples", 24)),
            free_group_link=str(clean.get("free_group_link", "")),
            private_welcome_link=str(clean.get("private_welcome_link", "")),
            vip_payment_link=str(clean.get("vip_payment_link", "")),
            payment_link=str(clean.get("payment_link", clean.get("vip_payment_link", ""))),
            lovable_url=str(clean.get("lovable_url", "")),
            vip_group_link=str(clean.get("vip_group_link", "")),
            post_purchase_message=str(clean.get("post_purchase_message", "")),
            post_payment_message=str(clean.get("post_payment_message", clean.get("post_purchase_message", ""))),
            marketing_welcome_text=str(clean.get("marketing_welcome_text", "")),
            marketing_vip_pitch=str(clean.get("marketing_vip_pitch", "")),
            marketing_testimonials=str(clean.get("marketing_testimonials", "")),
            marketing_free_group_text=str(clean.get("marketing_free_group_text", "")),
            welcome_message=str(clean.get("welcome_message", clean.get("marketing_welcome_text", ""))),
            vip_pitch_message=str(clean.get("vip_pitch_message", clean.get("marketing_vip_pitch", ""))),
            use_optimized=bool(clean.get("use_optimized", True)),
            strategy_version=str(clean.get("strategy_version", "optimized")),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "twelve_api_key": self.twelve_api_key,
            "av_api_key": self.av_api_key,
            "telegram_token": self.telegram_token,
            "telegram_chat_id": self.telegram_chat_id,
            "free_telegram_chat_id": self.free_telegram_chat_id,
            "vip_telegram_chat_id": self.vip_telegram_chat_id,
            "telegram_enabled": self.telegram_enabled,
            "group_tier": self.group_tier,
            "message_mode": self.message_mode,
            "min_score": self.min_score,
            "send_only_strong": self.send_only_strong,
            "min_signal_interval_seconds": self.min_signal_interval_seconds,
            "allowed_pairs": list(self.allowed_pairs),
            "favorite_pairs": list(self.favorite_pairs),
            "allowed_hours": self.allowed_hours,
            "auto_pause_enabled": self.auto_pause_enabled,
            "max_consecutive_losses": self.max_consecutive_losses,
            "min_daily_win_rate_pause": self.min_daily_win_rate_pause,
            "social_proof_enabled": self.social_proof_enabled,
            "social_proof_min_streak": self.social_proof_min_streak,
            "social_proof_min_win_rate": self.social_proof_min_win_rate,
            "social_proof_min_decisive": self.social_proof_min_decisive,
            "daily_summary_enabled": self.daily_summary_enabled,
            "daily_summary_time": self.daily_summary_time,
            "account_balance": self.account_balance,
            "risk_pct": self.risk_pct,
            "filter_hours": self.filter_hours,
            "mtf_confirm": self.mtf_confirm,
            "divergence_detect": self.divergence_detect,
            "simple_mode": self.simple_mode,
            "show_advanced_panel": self.show_advanced_panel,
            "adaptive_filtering": self.adaptive_filtering,
            "telegram_min_strength": self.telegram_min_strength,
            "min_resolved_trades": self.min_resolved_trades,
            "min_win_rate": self.min_win_rate,
            "min_profit_factor": self.min_profit_factor,
            "global_scan_enabled": self.global_scan_enabled,
            "best_signal_min_score": self.best_signal_min_score,
            "ml_enabled": self.ml_enabled,
            "ml_weight": self.ml_weight,
            "ml_min_samples": self.ml_min_samples,
            "free_group_link": self.free_group_link,
            "private_welcome_link": self.private_welcome_link,
            "vip_payment_link": self.vip_payment_link,
            "payment_link": self.payment_link,
            "lovable_url": self.lovable_url,
            "vip_group_link": self.vip_group_link,
            "post_purchase_message": self.post_purchase_message,
            "post_payment_message": self.post_payment_message,
            "marketing_welcome_text": self.marketing_welcome_text,
            "marketing_vip_pitch": self.marketing_vip_pitch,
            "marketing_testimonials": self.marketing_testimonials,
            "marketing_free_group_text": self.marketing_free_group_text,
            "welcome_message": self.welcome_message,
            "vip_pitch_message": self.vip_pitch_message,
            "use_optimized": self.use_optimized,
            "strategy_version": self.strategy_version,
        }

    def has_api_keys(self) -> bool:
        return bool(self.twelve_api_key or self.av_api_key)


class ConfigManager:
    """Application config manager with optional file persistence and env overrides."""

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = Path(config_path or CONFIG_PATH)
        self._config: Optional[Config] = None
        self._file_config: Dict[str, Any] = {}
        self._env_overrides: Dict[str, Any] = load_environment_overrides()
        self._load()

    def _compose_config(self, file_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        merged = DEFAULT_CONFIG.copy()
        if file_config:
            merged.update(file_config)
        if self._env_overrides:
            merged.update(self._env_overrides)
        return merged

    def _load(self) -> None:
        try:
            raw_config: Dict[str, Any] = {}
            if self.config_path.exists():
                with open(self.config_path, "r", encoding="utf-8") as handle:
                    raw_config = json.load(handle)
                logger.info("Configuration loaded from %s", self.config_path)
            else:
                logger.info("Config file not found; using defaults and environment variables only")

            self._file_config = dict(raw_config or {})
            self._config = Config.from_dict(self._compose_config(self._file_config))
            if self._env_overrides:
                logger.info("Environment overrides active: %s", ", ".join(sorted(self._env_overrides.keys())))
        except json.JSONDecodeError as exc:
            logger.error("Invalid JSON in config: %s", exc)
            self._file_config = {}
            self._config = Config.from_dict(self._compose_config({}))
        except Exception as exc:
            logger.error("Failed to load config: %s", exc)
            self._file_config = {}
            self._config = Config.from_dict(self._compose_config({}))

    def save(self) -> bool:
        """Persist only local file-backed overrides. Environment secrets stay out of disk."""
        try:
            if not self._file_config and not self.config_path.exists():
                logger.info("Skipping config file creation because no local overrides were provided")
                return True

            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config_path, "w", encoding="utf-8") as handle:
                json.dump(self._file_config, handle, indent=2, ensure_ascii=False)
            logger.info("Configuration saved to %s", self.config_path)
            return True
        except Exception as exc:
            logger.error("Failed to save config: %s", exc)
            return False

    def get(self) -> Config:
        if self._config is None:
            self._load()
        return self._config

    def update(self, **kwargs: Any) -> bool:
        local_config = dict(self._file_config)
        local_config.update(kwargs)

        composed = self._compose_config(local_config)
        result = ConfigValidator.validate(composed)
        if not result.is_valid:
            logger.error("Invalid config update: %s", result.errors)
            return False

        self._file_config = local_config
        self._config = Config.from_dict(composed)
        return self.save()

    def reset(self) -> bool:
        self._file_config = {}
        self._config = Config.from_dict(self._compose_config({}))
        if self.config_path.exists():
            try:
                self.config_path.unlink()
            except Exception as exc:
                logger.warning("Failed to remove config file during reset: %s", exc)
        return True

    def validate(self) -> ValidationResult:
        if self._config is None:
            return ValidationResult(False, ["Config not loaded"], [])
        return ConfigValidator.validate(self._config.to_dict())

    @property
    def is_valid(self) -> bool:
        return self.validate().is_valid
