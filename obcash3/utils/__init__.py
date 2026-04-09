from __future__ import absolute_import
"""Utility functions for OB CASH 3.0."""

from typing import Any
import pandas as pd
import numpy as np


def get_latest_value(series: pd.Series, default: float = 0.0, offset: int = 0) -> float:
    """
    Get the latest value from a pandas Series with optional offset.

    Args:
        series: Pandas Series
        default: Value to return if series is empty or invalid
        offset: How many positions back from the end (0=last, 1=second to last, etc.)

    Returns:
        Float value or default
    """
    # Validate input type - defense against non-Series inputs (e.g., float)
    if not isinstance(series, (pd.Series, np.ndarray, list, tuple)):
        return default
    if series is None or len(series) == 0:
        return default
    idx = -1 - offset
    if abs(idx) > len(series):
        return default
    val = series.iloc[idx]
    return float(val) if not pd.isna(val) else default


def safe_divide(numerator, denominator, default: float = 0.0):
    """Safe division that returns default if denominator is zero or NaN.

    Works with both scalars and pandas Series.
    """
    # Handle pandas Series
    if isinstance(denominator, pd.Series):
        # For Series, perform element-wise safe division
        result = numerator / denominator
        # Replace inf, -inf, and NaN with default
        result = result.replace([np.inf, -np.inf, np.nan], default)
        return result
    else:
        # Scalar case
        if denominator == 0 or pd.isna(denominator):
            return default
        return numerator / denominator


def format_price(price: float, digits: int = 5) -> str:
    """Format price with appropriate decimal places."""
    if price == 0:
        return "0.00000"
    return f"{price:.{digits}f}"


def pct_to_float(pct_str: str) -> float:
    """Convert percentage string to float (e.g., '75%' -> 75.0)."""
    return float(pct_str.strip().replace('%', ''))


def float_to_pct(value: float, decimals: int = 1) -> str:
    """Convert float to percentage string (e.g., 75.0 -> '75.0%')."""
    return f"{value:.{decimals}f}%"


__all__ = [
    "get_latest_value",
    "safe_divide",
    "format_price",
    "pct_to_float",
    "float_to_pct",
]

