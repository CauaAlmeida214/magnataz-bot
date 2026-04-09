from __future__ import absolute_import
"""
Technical indicator calculations for OB CASH 3.0.

All functions accept pandas Series/DataFrame and return calculated values.
Optimized for performance with vectorized operations.
"""

from typing import Tuple, Optional
import numpy as np
import pandas as pd


def calculate_adx(df: pd.DataFrame, period: int = 14) -> Tuple[pd.Series, pd.Series]:
    """
    Calculate Average Directional Index (ADX) and Average True Range (ATR).

    Args:
        df: DataFrame with High, Low, Close columns
        period: Period for calculation (default 14)

    Returns:
        Tuple of (ADX series, ATR series)
    """
    high = df["High"]
    low = df["Low"]
    close = df["Close"]

    # True Range
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()

    # Plus DM and Minus DM
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    # Smooth with EMA-like calculation
    plus_di = plus_dm.rolling(period).mean() / atr.replace(0, np.nan) * 100
    minus_di = minus_dm.rolling(period).mean() / atr.replace(0, np.nan) * 100

    # DX and ADX
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan) * 100
    adx = dx.rolling(period).mean()

    return adx, atr


def calculate_stochastic(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate Stochastic Oscillator (%K).

    Args:
        df: DataFrame with High, Low, Close columns
        period: Lookback period

    Returns:
        Stochastic oscillator values (0-100)
    """
    low_min = df["Low"].rolling(period).min()
    high_max = df["High"].rolling(period).max()
    stoch = (df["Close"] - low_min) / (high_max - low_min).replace(0, np.nan) * 100
    return stoch


def calculate_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
    """
    Calculate Relative Strength Index (RSI).

    Uses Wilder's smoothing (EMA with alpha=1/period).

    Args:
        prices: Series of closing prices
        period: RSI period (default 14)

    Returns:
        RSI values (0-100)
    """
    delta = prices.diff()

    # Separate gains and losses
    gains = delta.clip(lower=0)
    losses = (-delta).clip(lower=0)

    # Exponential moving average of gains and losses
    avg_gain = gains.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

    # Calculate RSI
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    return rsi


def calculate_macd(prices: pd.Series,
                   fast: int = 12,
                   slow: int = 26,
                   signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    Calculate MACD (Moving Average Convergence Divergence).

    Args:
        prices: Series of closing prices
        fast: Fast EMA period
        slow: Slow EMA period
        signal: Signal line period

    Returns:
        Tuple of (MACD line, Signal line, Histogram)
    """
    ema_fast = prices.ewm(span=fast, adjust=False).mean()
    ema_slow = prices.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line

    return macd_line, signal_line, histogram


def calculate_bollinger_bands(df: pd.Series,
                             period: int = 20,
                             std_dev: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    Calculate Bollinger Bands.

    Args:
        df: Price Series (typically Close)
        period: Moving average period
        std_dev: Number of standard deviations

    Returns:
        Tuple of (Middle Band, Upper Band, Lower Band)
    """
    middle = df.rolling(period).mean()
    std = df.rolling(period).std()
    upper = middle + (std * std_dev)
    lower = middle - (std * std_dev)

    return middle, upper, lower


def calculate_bb_width(df: pd.Series, period: int = 20, std_dev: float = 2.0) -> pd.Series:
    """
    Calculate Bollinger Band Width (normalized).

    Args:
        df: Price Series
        period: MA period
        std_dev: Standard deviation multiplier

    Returns:
        Bollinger Band Width as percentage of middle band
    """
    _, upper, lower = calculate_bollinger_bands(df, period, std_dev)
    middle = df.rolling(period).mean()
    width = (upper - lower) / middle.replace(0, np.nan)
    return width


def calculate_ema(series: pd.Series, span: int) -> pd.Series:
    """Calculate Exponential Moving Average."""
    return series.ewm(span=span, adjust=False).mean()


def calculate_sma(series: pd.Series, period: int) -> pd.Series:
    """Calculate Simple Moving Average."""
    return series.rolling(period).mean()


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate Average True Range (ATR).

    Args:
        df: DataFrame with High, Low, Close columns
        period: Period for smoothing

    Returns:
        ATR series
    """
    high = df["High"]
    low = df["Low"]
    close = df["Close"]

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)

    return tr.rolling(period).mean()


def calculate_market_trend(prices: pd.Series, lookback: int = 14) -> Tuple[bool, float]:
    """
    Determine market trend direction and momentum.

    Args:
        prices: Price series
        lookback: Period for momentum calculation

    Returns:
        Tuple of (is_rising, momentum_value)
    """
    # Momentum: current price minus N-period ago price
    momentum = prices - prices.shift(lookback)
    return momentum.iloc[-1] > 0, momentum.iloc[-1]


def calculate_slope(series: pd.Series, lookback: int = 6) -> str:
    """
    Calculate slope direction of a series over recent lookback.

    Args:
        series: Data series
        lookback: Number of recent points to consider

    Returns:
        "alta" (rising), "baixa" (falling), or "plano" (flat)
    """
    recent = series.iloc[-lookback:]
    x = np.arange(len(recent))
    y = recent.values

    # Linear regression slope
    if len(y) < 2:
        return "plano"

    slope = np.polyfit(x, y, 1)[0]

    # Threshold for flat detection
    threshold = np.std(y) * 0.1 if np.std(y) > 0 else 0.0001

    if slope > threshold:
        return "alta"
    elif slope < -threshold:
        return "baixa"
    else:
        return "plano"


def get_latest_values(df: pd.DataFrame,
                      indicators: dict) -> dict:
    """
    Extract latest values from indicator series safely.

    Args:
        df: Source DataFrame
        indicators: Dictionary of indicator Series

    Returns:
        Dictionary with latest values (0.0 if NaN/empty)
    """
    result = {}

    for key, series in indicators.items():
        if series is None or len(series) == 0:
            result[key] = 0.0
        else:
            val = series.iloc[-1]
            result[key] = float(val) if not pd.isna(val) else 0.0

    return result
