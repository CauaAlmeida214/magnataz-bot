from __future__ import absolute_import
"""
Configuration validation for OB CASH 3.0.

Validates user-provided configuration values to ensure they are within
acceptable ranges and formats.
"""

from typing import Dict, Any, List, Tuple
from dataclasses import dataclass
from obcash3.config.settings import DEFAULT_CONFIG


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]

    def __bool__(self) -> bool:
        return self.is_valid


class ConfigValidator:
    """Validates configuration dictionaries."""

    # Valid ranges for numeric config values
    NUMERIC_RANGES = {
        "account_balance": (10.0, float('inf')),
        "risk_pct": (0.1, 100.0),  # 0.1% to 100%
        "min_score": (30.0, 100.0),
        "min_signal_interval_seconds": (15.0, 86400.0),
        "max_consecutive_losses": (1.0, 20.0),
        "min_resolved_trades": (1.0, 50.0),
        "min_win_rate": (0.30, 0.90),
        "min_profit_factor": (0.50, 3.00),
        "best_signal_min_score": (30.0, 100.0),
        "ml_weight": (0.0, 0.80),
        "ml_min_samples": (4.0, 500.0),
        "min_daily_win_rate_pause": (0.0, 100.0),
        "social_proof_min_streak": (1.0, 20.0),
        "social_proof_min_win_rate": (0.0, 100.0),
        "social_proof_min_decisive": (1.0, 50.0),
    }

    # API key format patterns (basic length check)
    API_KEY_MIN_LENGTH = 8

    # Telegram chat ID format (can be numeric or string)
    TELEGRAM_CHAT_ID_PATTERN = r'^[\d\-_]+$'

    @classmethod
    def validate(cls, config: Dict[str, Any]) -> ValidationResult:
        """
        Validate a configuration dictionary.

        Args:
            config: Configuration dictionary to validate

        Returns:
            ValidationResult with any errors or warnings
        """
        errors: List[str] = []
        warnings: List[str] = []

        normalized = DEFAULT_CONFIG.copy()
        normalized.update(config)

        # Validate numeric ranges
        for key, (min_val, max_val) in cls.NUMERIC_RANGES.items():
            if key in normalized:
                try:
                    val = float(normalized[key])
                    if not (min_val <= val <= max_val):
                        errors.append(
                            f"{key} must be between {min_val} and {max_val}, got {val}"
                        )
                except (ValueError, TypeError):
                    errors.append(f"{key} must be a valid number, got {type(normalized[key])}")

        # Validate API keys (if provided)
        for api_key in ["twelve_api_key", "av_api_key"]:
            if api_key in normalized and normalized[api_key]:
                if len(normalized[api_key]) < cls.API_KEY_MIN_LENGTH:
                    warnings.append(
                        f"{api_key} seems too short (min {cls.API_KEY_MIN_LENGTH} chars)"
                    )

        # Validate Telegram token (if provided)
        if normalized.get("telegram_token"):
            if len(normalized["telegram_token"]) < 10:
                warnings.append("Telegram token seems too short")

        # Validate Telegram chat ID (if provided)
        if normalized.get("telegram_chat_id"):
            import re
            if not re.match(cls.TELEGRAM_CHAT_ID_PATTERN, str(normalized["telegram_chat_id"])):
                warnings.append(
                    "Telegram chat ID should be numeric (use @getidsbot to get it)"
                )
        for route_key in ("free_telegram_chat_id", "vip_telegram_chat_id"):
            if normalized.get(route_key):
                import re
                if not re.match(cls.TELEGRAM_CHAT_ID_PATTERN, str(normalized[route_key])):
                    warnings.append(
                        f"{route_key} should be numeric (use @getidsbot to get it)"
                    )

        telegram_min_strength = str(normalized.get("telegram_min_strength", "FORTE")).upper()
        if telegram_min_strength not in {"FORTE", "MODERADO"}:
            errors.append("telegram_min_strength must be FORTE or MODERADO")

        message_mode = str(normalized.get("message_mode", "vip")).strip().lower()
        if message_mode not in {"free", "vip"}:
            errors.append("message_mode must be free or vip")

        group_tier = str(normalized.get("group_tier", "free")).strip().lower()
        if group_tier not in {"free", "vip"}:
            errors.append("group_tier must be free or vip")

        import re
        allowed_hours = str(normalized.get("allowed_hours", "00:00-23:59")).strip()
        if allowed_hours and not re.match(r"^\d{2}:\d{2}-\d{2}:\d{2}$", allowed_hours):
            errors.append("allowed_hours must be in HH:MM-HH:MM format")

        daily_summary_time = str(normalized.get("daily_summary_time", "23:59")).strip()
        if daily_summary_time and not re.match(r"^\d{2}:\d{2}$", daily_summary_time):
            errors.append("daily_summary_time must be in HH:MM format")

        # Check for deprecated/unknown keys
        known_keys = set(DEFAULT_CONFIG.keys())
        provided_keys = set(config.keys())
        unknown_keys = provided_keys - known_keys
        if unknown_keys:
            warnings.append(f"Unknown config keys: {', '.join(unknown_keys)}")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )

    @classmethod
    def sanitize(cls, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and normalize configuration values.

        Args:
            config: Raw configuration dictionary

        Returns:
            Sanitized configuration with defaults applied
        """
        result = DEFAULT_CONFIG.copy()

        # Update with provided values, with basic sanitization
        for key, value in config.items():
            if key in result:
                # Strip strings
                if isinstance(value, str):
                    result[key] = value.strip()
                else:
                    result[key] = value

        # Ensure boolean values are actually boolean
        for bool_key in [
            "telegram_enabled",
            "filter_hours",
            "mtf_confirm",
            "divergence_detect",
            "simple_mode",
            "show_advanced_panel",
            "adaptive_filtering",
            "send_only_strong",
            "daily_summary_enabled",
            "auto_pause_enabled",
            "social_proof_enabled",
            "global_scan_enabled",
            "ml_enabled",
        ]:
            if bool_key in result:
                result[bool_key] = bool(result[bool_key])

        for int_key in [
            "min_resolved_trades",
            "min_signal_interval_seconds",
            "max_consecutive_losses",
            "social_proof_min_streak",
            "social_proof_min_decisive",
            "ml_min_samples",
        ]:
            if int_key in result:
                try:
                    result[int_key] = int(float(result[int_key]))
                except (TypeError, ValueError):
                    result[int_key] = DEFAULT_CONFIG[int_key]

        for float_key in [
            "min_score",
            "min_win_rate",
            "min_profit_factor",
            "best_signal_min_score",
            "ml_weight",
            "min_daily_win_rate_pause",
            "social_proof_min_win_rate",
        ]:
            if float_key in result:
                try:
                    result[float_key] = float(result[float_key])
                except (TypeError, ValueError):
                    result[float_key] = DEFAULT_CONFIG[float_key]

        result["telegram_min_strength"] = str(result.get("telegram_min_strength", "FORTE")).strip().upper() or "FORTE"
        result["group_tier"] = str(result.get("group_tier", "free")).strip().lower() or "free"
        result["message_mode"] = str(result.get("message_mode", "vip")).strip().lower() or "vip"
        result["allowed_hours"] = str(result.get("allowed_hours", "00:00-23:59")).strip() or "00:00-23:59"
        result["daily_summary_time"] = str(result.get("daily_summary_time", "23:59")).strip() or "23:59"

        for list_key in ["allowed_pairs", "favorite_pairs"]:
            value = result.get(list_key, [])
            if isinstance(value, str):
                result[list_key] = [item.strip().upper().replace("-", "/") for item in value.split(",") if item.strip()]
            elif isinstance(value, (list, tuple, set)):
                result[list_key] = [str(item).strip().upper().replace("-", "/") for item in value if str(item).strip()]
            else:
                result[list_key] = list(DEFAULT_CONFIG.get(list_key, []))

        for text_key in [
            "free_telegram_chat_id",
            "vip_telegram_chat_id",
            "free_group_link",
            "private_welcome_link",
            "vip_payment_link",
            "payment_link",
            "lovable_url",
            "vip_group_link",
            "post_purchase_message",
            "post_payment_message",
            "marketing_welcome_text",
            "marketing_vip_pitch",
            "marketing_testimonials",
            "marketing_free_group_text",
            "welcome_message",
            "vip_pitch_message",
        ]:
            result[text_key] = str(result.get(text_key, "")).strip()

        if not result.get("payment_link"):
            result["payment_link"] = str(result.get("vip_payment_link", "")).strip()
        if not result.get("post_payment_message"):
            result["post_payment_message"] = str(result.get("post_purchase_message", "")).strip()
        if not result.get("welcome_message"):
            result["welcome_message"] = str(result.get("marketing_welcome_text", "")).strip()
        if not result.get("vip_pitch_message"):
            result["vip_pitch_message"] = str(result.get("marketing_vip_pitch", "")).strip()

        return result
