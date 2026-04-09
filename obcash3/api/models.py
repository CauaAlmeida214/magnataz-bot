from __future__ import absolute_import
"""
Pydantic models for API requests/responses.
"""

from typing import Optional, List, Dict, Any, get_type_hints
from datetime import datetime

try:
    from pydantic import BaseModel, ConfigDict, Field
except ImportError:  # pragma: no cover - lightweight fallback for bot/service mode
    class BaseModel:
        """Minimal fallback used when pydantic is unavailable."""

        def __init__(self, **kwargs):
            annotations = get_type_hints(self.__class__)
            for key in annotations:
                default = getattr(self.__class__, key, ...)
                if key in kwargs:
                    value = kwargs[key]
                elif default is ...:
                    raise TypeError(f"Missing required field: {key}")
                else:
                    value = default
                setattr(self, key, value)

        def model_dump(self, exclude_unset: bool = False, **kwargs):
            annotations = get_type_hints(self.__class__)
            return {key: getattr(self, key) for key in annotations}

    class ConfigDict(dict):
        pass

    def Field(default=..., **kwargs):
        return default


class SignalRequest(BaseModel):
    """Request model for signal analysis."""
    pair: str = Field(..., description="Trading pair (e.g., EUR/USD)")
    timeframe: str = Field(..., description="Timeframe (1m, 5m, 15m, 30m, 1h)")


class SignalResponse(BaseModel):
    """Response model for signal analysis."""
    asset: str
    interval: str
    timestamp: datetime
    action: str  # COMPRA, VENDA, EVITA, AGUARDAR
    strength: str  # FORTE, MODERADO, FRACO, NEUTRO
    score: float
    price: float
    sl: Optional[float] = None
    tp: Optional[float] = None
    atr: float
    rsi: float
    adx: float
    macd_hist: float
    stoch: float
    bb_width: float
    market_regime: str
    session: str
    mtf_confirmation: str
    divergence: str
    source: str
    confidence_score: float = 0.0
    confidence_label: str = "OBSERVAR"
    policy_state: str = "learning"
    policy_notes: List[str] = []
    technical_score: float = 0.0
    ml_score: float = 0.0
    ml_confidence: float = 0.0
    ml_backend: str = "none"
    ml_used: bool = False
    decision_score: float = 0.0
    selection_reason: str = ""
    note: str = ""
    filters: List[str]
    conditions_buy: List[bool]
    conditions_sell: List[bool]


class ScanAllRequest(BaseModel):
    """Request to scan all pairs."""
    timeframe: str = Field("5m", description="Timeframe to scan")
    send_telegram: bool = Field(True, description="Send alerts to Telegram")


class ScanAllResponse(BaseModel):
    """Response for bulk scan."""
    total_pairs: int
    signals_found: int
    strong_signals: int
    results: List[SignalResponse]
    scan_duration_seconds: float
    best_signal: Optional[SignalResponse] = None
    qualified_candidates: int = 0
    ignored_pairs: int = 0
    ml_backend: str = "none"
    selection_notes: List[str] = []


class BacktestRequest(BaseModel):
    """Request for backtesting."""
    pair: str
    timeframe: str
    initial_balance: float = Field(1000.0, ge=10.0)
    risk_percent: float = Field(1.0, ge=0.1, le=100.0)
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


class BacktestResponse(BaseModel):
    """Backtest results."""
    pair: str
    timeframe: str
    total_trades: int
    wins: int
    losses: int
    draw_trades: int
    win_rate: float
    profit_factor: float
    total_pnl_pct: float
    max_drawdown_pct: float
    avg_score: float
    equity_curve: List[float]


class StatsResponse(BaseModel):
    """Application statistics."""
    uptime_seconds: int
    total_scans: int
    total_signals: int
    strong_signals_today: int
    average_score: float
    last_scan: Optional[datetime]
    active_pairs: List[str]
    cache_hit_rate: float


class ConfigUpdate(BaseModel):
    """Configuration update model."""
    model_config = ConfigDict(populate_by_name=True)

    twelve_api_key: Optional[str] = None
    av_api_key: Optional[str] = None
    telegram_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    free_telegram_chat_id: Optional[str] = None
    vip_telegram_chat_id: Optional[str] = None
    telegram_enabled: Optional[bool] = None
    group_tier: Optional[str] = None
    message_mode: Optional[str] = None
    min_score: Optional[float] = Field(None, ge=30.0, le=100.0)
    send_only_strong: Optional[bool] = None
    min_signal_interval_seconds: Optional[int] = Field(None, ge=15, le=86400)
    allowed_pairs: Optional[List[str]] = None
    favorite_pairs: Optional[List[str]] = None
    allowed_hours: Optional[str] = None
    auto_pause_enabled: Optional[bool] = None
    max_consecutive_losses: Optional[int] = Field(None, ge=1, le=20)
    min_daily_win_rate_pause: Optional[float] = Field(None, ge=0.0, le=100.0)
    social_proof_enabled: Optional[bool] = None
    social_proof_min_streak: Optional[int] = Field(None, ge=1, le=20)
    social_proof_min_win_rate: Optional[float] = Field(None, ge=0.0, le=100.0)
    social_proof_min_decisive: Optional[int] = Field(None, ge=1, le=50)
    daily_summary_enabled: Optional[bool] = None
    daily_summary_time: Optional[str] = None
    account_balance: Optional[float] = Field(None, ge=10.0)
    risk_pct: Optional[float] = Field(None, alias="risk_percent", ge=0.1, le=100.0)
    filter_hours: Optional[bool] = None
    mtf_confirm: Optional[bool] = None
    divergence_detect: Optional[bool] = None
    simple_mode: Optional[bool] = None
    show_advanced_panel: Optional[bool] = None
    adaptive_filtering: Optional[bool] = None
    telegram_min_strength: Optional[str] = None
    min_resolved_trades: Optional[int] = Field(None, ge=1, le=50)
    min_win_rate: Optional[float] = Field(None, ge=0.30, le=0.90)
    min_profit_factor: Optional[float] = Field(None, ge=0.50, le=3.00)
    global_scan_enabled: Optional[bool] = None
    best_signal_min_score: Optional[float] = Field(None, ge=30.0, le=100.0)
    ml_enabled: Optional[bool] = None
    ml_weight: Optional[float] = Field(None, ge=0.0, le=0.80)
    ml_min_samples: Optional[int] = Field(None, ge=4, le=500)
    free_group_link: Optional[str] = None
    private_welcome_link: Optional[str] = None
    vip_payment_link: Optional[str] = None
    payment_link: Optional[str] = None
    lovable_url: Optional[str] = None
    vip_group_link: Optional[str] = None
    post_purchase_message: Optional[str] = None
    post_payment_message: Optional[str] = None
    marketing_welcome_text: Optional[str] = None
    marketing_vip_pitch: Optional[str] = None
    marketing_testimonials: Optional[str] = None
    marketing_free_group_text: Optional[str] = None
    welcome_message: Optional[str] = None
    vip_pitch_message: Optional[str] = None


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    timestamp: datetime
    version: str
    dependencies: Dict[str, bool]


class WebhookSignal(BaseModel):
    """Webhook payload for external integrations."""
    signal: SignalResponse
    webhook_url: Optional[str] = None
    secret: Optional[str] = None
