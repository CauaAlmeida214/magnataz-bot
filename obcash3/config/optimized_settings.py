"""
Optimized strategy parameters based on backtest results.

These settings are tuned to improve win rate and profit factor.
"""

from obcash3.config.settings import StrategyConfig, SESSIONS

class OptimizedStrategyConfig(StrategyConfig):
    """Optimized parameters for higher assertiveness."""

    # Score thresholds (adjusted)
    STRONG_SIGNAL_MIN_SCORE = 75.0  # Era 70
    MODERATE_SIGNAL_MIN_SCORE = 50.0  # Era 45
    MIN_SCORE_FOR_SIGNAL = 35.0  # Era 30

    # Market session adjustments (tighter in range, same in trend)
    SESSION_MIN_SCORES = {
        "ASIANA": 65,          # Era 55
        "LONDON_OPEN": 55,     # Era 50
        "TRANSICAO": 55,       # Era 50
        "LONDON_NY": 50,       # Era 45 (keep - best session)
        "NEW_YORK": 55,        # Era 50
        "QUIET": 70,           # Era 60
    }

    # Volume spike - raised bar
    VOLUME_SPIKE_RATIO = 1.8  # Era 1.2 (more conservative)

    # ADX trend threshold - be more strict
    ADX_TREND_THRESHOLD = 30  # Era 25 (stronger trend required)

    # RSI extremes - tighter
    RSI_OVERBOUGHT = 75      # Era 72
    RSI_OVERSOLD = 25        # Era 28

    # Stochastic extremes - tighter
    STOCH_OVERBOUGHT = 80    # Era 85
    STOCH_OVERSOLD = 20      # Era 15

    # EMA100 distance - allow further
    MAX_EMA100_DISTANCE_PCT = 0.8  # Era 0.5% (more flexible)

    # SMA50 slope - more sensitive
    SMA50_SLOPE_THRESHOLD = 0.00005  # Era 0.00008 (lower = more sensitive)

    # Bollinger Band width - tighter compression required
    BB_MIN_WIDTH = 0.0003  # Era 0.0005

    # Multi-timeframe bonus
    MTF_BONUS_POINTS = 4  # Era 3 (more weight)

    # Divergence bonus
    DIVERGENCE_BONUS_POINTS = 4  # Era 3 (more weight)

    # New: Candle pattern bonus (if integrated)
    CANDLE_PATTERN_BONUS = 2

    # New: Volume confirmation multiplier
    VOLUME_CONFIRM_MULTIPLIER = 1.5  # Multiply score if volume confirms

    # Filters: be more selective in these regimes
    STRICT_LATERAL = True  # Apply stricter rules in LATERAL regime
    REQUIRE_MTF_CORRELATION = True  # Require HTF alignment


# Helper function to get session-specific min score
def get_optimized_session_min_score(session_name: str) -> int:
    """Get optimized min score for session."""
    return OptimizedStrategyConfig.SESSION_MIN_SCORES.get(
        session_name,
        StrategyConfig.MODERATE_SIGNAL_MIN_SCORE
    )
