from __future__ import absolute_import
"""
Configuration settings and constants for the standalone MagnataZ bot.
"""

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

# Timezone
BRT = ZoneInfo("America/Sao_Paulo")

# File paths
SETTINGS_DIR = Path(__file__).resolve().parent
PKG_DIR = SETTINGS_DIR.parent
PROJECT_ROOT = PKG_DIR.parent


def _runtime_data_dir() -> Path:
    """Return a stable writable directory for logs, cache, ML and runtime state."""
    runtime_override = os.getenv("APP_RUNTIME_DIR", "").strip()
    if runtime_override:
        return Path(runtime_override).expanduser().resolve()

    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent

    return PROJECT_ROOT


RUNTIME_DIR = _runtime_data_dir()
APP_DATA_DIR = RUNTIME_DIR / "runtime"
CONFIG_PATH = Path(os.getenv("CONFIG_PATH", "").strip() or (RUNTIME_DIR / "config.json"))
ASSETS_DIR = PROJECT_ROOT / "assets"
RESULTS_ASSETS_DIR = ASSETS_DIR / "resultados"
RESULT_POSITIVE_IMAGE_PATH = Path(
    os.getenv("RESULT_POSITIVE_IMAGE_PATH", "").strip() or (RESULTS_ASSETS_DIR / "resultado_positivo.png")
)
HISTORY_PATH = APP_DATA_DIR / "signal_history.csv"
LOGS_DIR = APP_DATA_DIR / "logs"
CACHE_DIR = APP_DATA_DIR / "cache"
BOT_DB_PATH = APP_DATA_DIR / "bot_runtime.sqlite3"
ML_DIR = APP_DATA_DIR / "ml"
ML_MODELS_DIR = ML_DIR / "models"
ML_MODEL_PATH = ML_MODELS_DIR / "ml_model.pkl"
ML_ENCODERS_PATH = ML_MODELS_DIR / "encoders.pkl"
ML_METADATA_PATH = ML_MODELS_DIR / "metadata.json"

# Market pairs
PAIRS: Dict[str, Tuple[str, str, str]] = {
    "EUR/USD": ("EURUSD", "EURUSD", "EURUSD=X"),
    "GBP/USD": ("GBPUSD", "GBPUSD", "GBPUSD=X"),
    "USD/JPY": ("USDJPY", "USDJPY", "USDJPY=X"),
    "AUD/USD": ("AUDUSD", "AUDUSD", "AUDUSD=X"),
    "USD/CAD": ("USDCAD", "USDCAD", "USDCAD=X"),
    "EUR/GBP": ("EURGBP", "EURGBP", "EURGBP=X"),
    "EUR/JPY": ("EURJPY", "EURJPY", "EURJPY=X"),
    "GBP/JPY": ("GBPJPY", "GBPJPY", "GBPJPY=X"),
    "USD/CHF": ("USDCHF", "USDCHF", "USDCHF=X"),
    "NZD/USD": ("NZDUSD", "NZDUSD", "NZDUSD=X"),
    "EUR/CAD": ("EURCAD", "EURCAD", "EURCAD=X"),
    "AUD/CAD": ("AUDCAD", "AUDCAD", "AUDCAD=X"),
    "AUD/NZD": ("AUDNZD", "AUDNZD", "AUDNZD=X"),
    "CAD/JPY": ("CADJPY", "CADJPY", "CADJPY=X"),
    "CHF/JPY": ("CHFJPY", "CHFJPY", "CHFJPY=X"),
}


class StrategyConfig:
    """Strategy thresholds and indicator parameters."""

    STRONG_SIGNAL_MIN_SCORE = 70.0
    MODERATE_SIGNAL_MIN_SCORE = 45.0
    MIN_SCORE_FOR_SIGNAL = 30.0

    MTF_BONUS_POINTS = 3
    DIVERGENCE_BONUS_POINTS = 3

    STOP_LOSS_ATR_MULTIPLIER = 2.0
    TAKE_PROFIT_ATR_MULTIPLIER = 3.0
    DEFAULT_RISK_PCT = 1.0
    DEFAULT_BALANCE = 1000.0

    RSI_PERIOD = 14
    MACD_FAST = 12
    MACD_SLOW = 26
    MACD_SIGNAL = 9
    ADX_PERIOD = 14
    ATR_PERIOD = 14
    STOCH_PERIOD = 14
    BB_PERIOD = 20
    BB_STD = 2.0
    EMA_FAST = 21
    EMA_SLOW = 100
    SMA_FAST = 3
    SMA_SLOW = 50

    ADX_TREND_THRESHOLD = 25
    ADX_NORMAL_THRESHOLD = 15
    VOLUME_SPIKE_RATIO = 1.2
    BB_MIN_WIDTH = 0.0005
    RSI_OVERBOUGHT = 72
    RSI_OVERSOLD = 28
    STOCH_OVERBOUGHT = 85
    STOCH_OVERSOLD = 15
    MAX_EMA100_DISTANCE_PCT = 0.5
    SMA50_SLOPE_THRESHOLD = 0.00008


PERIODS: List[str] = ["1m", "5m", "15m", "30m", "1h"]
PDAYS: Dict[str, int] = {"1m": 2, "5m": 14, "15m": 30, "30m": 60, "1h": 60}
TW_I: Dict[str, str] = {"1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min", "1h": "1h"}
AV_I: Dict[str, str] = {"1m": "1min", "5m": "5min", "15m": "15min", "30m": "60min", "1h": "60min"}
YF_I: Dict[str, str] = {"1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m", "1h": "1h"}
HTF: Dict[str, str | None] = {"1m": "5m", "5m": "15m", "15m": "1h", "30m": "1h", "1h": None}

HDR: Dict[str, str] = {
    "User-Agent": "Mozilla/5.0 Chrome/131.0.0.0",
    "Accept-Language": "pt-BR,pt;q=0.9",
}


@dataclass
class SessionConfig:
    name: str
    label: str
    start_hour: int
    end_hour: int
    min_score: int


SESSIONS: List[SessionConfig] = [
    SessionConfig("ASIANA", "ASIANA (Tokyo)", 20, 4, 55),
    SessionConfig("LONDON_OPEN", "LONDON (early)", 4, 8, 50),
    SessionConfig("TRANSICAO", "TRANSICAO", 8, 9, 50),
    SessionConfig("LONDON_NY", "LONDON/NY OVERLAP", 9, 13, 45),
    SessionConfig("NEW_YORK", "NEW YORK", 13, 18, 50),
    SessionConfig("QUIET", "QUIET HOURS", 18, 20, 60),
]


DEFAULT_CONFIG = {
    "twelve_api_key": "",
    "av_api_key": "",
    "telegram_token": "",
    "telegram_chat_id": "",
    "free_telegram_chat_id": "",
    "vip_telegram_chat_id": "",
    "telegram_enabled": True,
    "group_tier": "free",
    "message_mode": "vip",
    "min_score": 70.0,
    "send_only_strong": True,
    "min_signal_interval_seconds": 180,
    "allowed_pairs": [],
    "favorite_pairs": ["GBP/JPY", "EUR/USD", "AUD/CAD"],
    "allowed_hours": "00:00-23:59",
    "auto_pause_enabled": True,
    "max_consecutive_losses": 3,
    "min_daily_win_rate_pause": 0.0,
    "social_proof_enabled": True,
    "social_proof_min_streak": 3,
    "social_proof_min_win_rate": 75.0,
    "social_proof_min_decisive": 3,
    "daily_summary_enabled": True,
    "daily_summary_time": "23:59",
    "account_balance": 1000.0,
    "risk_pct": 1.0,
    "filter_hours": True,
    "mtf_confirm": True,
    "divergence_detect": True,
    "simple_mode": True,
    "show_advanced_panel": False,
    "adaptive_filtering": True,
    "telegram_min_strength": "FORTE",
    "min_resolved_trades": 5,
    "min_win_rate": 0.52,
    "min_profit_factor": 1.05,
    "global_scan_enabled": True,
    "best_signal_min_score": 70.0,
    "ml_enabled": True,
    "ml_weight": 0.30,
    "ml_min_samples": 24,
    "free_group_link": "https://t.me/+2-sVI86sGzQ1MmEx",
    "private_welcome_link": "https://t.me/MagnataZ_Bot?start=welcome",
    "vip_payment_link": "https://pay.kiwify.com.br/Aty9Q61",
    "payment_link": "https://pay.kiwify.com.br/Aty9Q61",
    "lovable_url": "https://magnataz-vip-luxe.lovable.app",
    "vip_group_link": "https://t.me/+Olel0chcCJgwMTZh",
    "post_purchase_message": (
        "✅ Pagamento confirmado!\n\n"
        "Seja muito bem-vindo à família MagnataZ VIP.\n\n"
        "Seu acesso exclusivo já está liberado:\n\n"
        "🔒 Grupo Premium\n"
        "https://t.me/+Olel0chcCJgwMTZh\n\n"
        "Desejamos ótimas operações!"
    ),
    "post_payment_message": (
        "✅ Pagamento confirmado!\n\n"
        "Seja muito bem-vindo à família MagnataZ VIP.\n\n"
        "Seu acesso exclusivo já está liberado:\n\n"
        "🔒 Grupo Premium\n"
        "https://t.me/+Olel0chcCJgwMTZh\n\n"
        "Desejamos ótimas operações!"
    ),
    "marketing_welcome_text": (
        "Bem-vindo ao MagnataZ Free.\n\n"
        "Aqui você recebe sinais gratuitos em horários estratégicos, com foco em oportunidades rápidas e objetivas na IQ Option."
    ),
    "marketing_vip_pitch": (
        "MAGNATAZ VIP\n"
        "Acesso exclusivo para quem quer ir para o próximo nível.\n\n"
        "Aqui você recebe sinais ao longo de todo o dia, com entradas filtradas e acompanhadas para buscar as melhores oportunidades do mercado."
    ),
    "marketing_testimonials": (
        "• Resultados consistentes\n"
        "• Alertas organizados\n"
        "• Comunidade ativa"
    ),
    "marketing_free_group_text": (
        "Participe do grupo free para acompanhar os sinais e conhecer a estrutura antes de migrar para o VIP."
    ),
    "welcome_message": (
        "Bem-vindo ao MagnataZ Free.\n\n"
        "Aqui você recebe sinais gratuitos em horários estratégicos, com foco em oportunidades rápidas e objetivas na IQ Option."
    ),
    "vip_pitch_message": (
        "MAGNATAZ VIP\n"
        "Acesso exclusivo para quem quer ir para o próximo nível.\n\n"
        "Aqui você recebe sinais ao longo de todo o dia, com entradas filtradas e acompanhadas para buscar as melhores oportunidades do mercado."
    ),
    "use_optimized": True,
    "strategy_version": "optimized",
}


class Colors:
    PRIMARY = "#00d4ff"
    SUCCESS = "#00ff88"
    DANGER = "#ff4c6b"
    WARNING = "#ffd700"
    NEUTRAL = "#888888"
    BG_DARK = "#1a1d24"
    BG_DARKER = "#12141a"
