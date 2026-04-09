from __future__ import absolute_import
"""
Divergence detection for OB CASH 3.0.

Detects bullish and bearish divergences between price and momentum indicators
(RSI, MACD) for high-probability reversal signals.
"""

from typing import Literal, Tuple
import numpy as np
import pandas as pd


class DivergenceDetector:
    """Detects divergences between price and momentum indicators."""

    @staticmethod
    def detect_rsi_divergence(
        price: pd.Series,
        rsi: pd.Series,
        lookback: int = 20
    ) -> Literal["BULL", "BEAR", "NONE"]:
        """
        Detect RSI divergence patterns.

        Bullish divergence: Price makes lower lows but RSI makes higher lows.
        Bearish divergence: Price makes higher highs but RSI makes lower highs.

        Args:
            price: Price series (typically Close)
            rsi: RSI values (0-100)
            lookback: Number of recent bars to analyze

        Returns:
            "BULL", "BEAR", or "NONE"
        """
        if len(price) < lookback + 5 or len(rsi) < lookback + 5:
            return "NONE"

        # Get recent data
        recent_price = price.iloc[-lookback:].values
        recent_rsi = rsi.iloc[-lookback:].values

        # Find local extrema
        price_highs_idx = DivergenceDetector._find_peaks(recent_price, order=3)
        price_lows_idx = DivergenceDetector._find_peaks(-recent_price, order=3)
        rsi_highs_idx = DivergenceDetector._find_peaks(recent_rsi, order=3)
        rsi_lows_idx = DivergenceDetector._find_peaks(-recent_rsi, order=3)

        MIN_DIV_CANDLES = 3  # mínimo de velas entre os pivots para divergência válida

        # Check for bearish divergence (price higher highs, RSI lower highs)
        if len(price_highs_idx) >= 2 and len(rsi_highs_idx) >= 2:
            ph_idx1, ph_idx2 = price_highs_idx[-2], price_highs_idx[-1]
            rh_idx1, rh_idx2 = rsi_highs_idx[-2], rsi_highs_idx[-1]

            if (abs(ph_idx2 - ph_idx1) >= MIN_DIV_CANDLES and
                recent_price[ph_idx2] > recent_price[ph_idx1] and
                recent_rsi[rh_idx2] < recent_rsi[rh_idx1]):
                return "BEAR"

        # Check for bullish divergence (price lower lows, RSI higher lows)
        if len(price_lows_idx) >= 2 and len(rsi_lows_idx) >= 2:
            pl_idx1, pl_idx2 = price_lows_idx[-2], price_lows_idx[-1]
            rl_idx1, rl_idx2 = rsi_lows_idx[-2], rsi_lows_idx[-1]

            if (abs(pl_idx2 - pl_idx1) >= MIN_DIV_CANDLES and
                recent_price[pl_idx2] < recent_price[pl_idx1] and
                recent_rsi[rl_idx2] > recent_rsi[rl_idx1]):
                return "BULL"

        return "NONE"

    @staticmethod
    def detect_macd_divergence(
        price: pd.Series,
        macd_hist: pd.Series,
        lookback: int = 20
    ) -> Literal["BULL", "BEAR", "NONE"]:
        """
        Detect MACD histogram divergence patterns.

        Similar logic to RSI divergence but using MACD histogram.

        Args:
            price: Price series
            macd_hist: MACD histogram values
            lookback: Analysis period

        Returns:
            "BULL", "BEAR", or "NONE"
        """
        if len(price) < lookback + 5 or len(macd_hist) < lookback + 5:
            return "NONE"

        recent_price = price.iloc[-lookback:].values
        recent_macd = macd_hist.iloc[-lookback:].values

        # Extrema detection
        price_highs_idx = DivergenceDetector._find_peaks(recent_price, order=3)
        price_lows_idx = DivergenceDetector._find_peaks(-recent_price, order=3)
        macd_highs_idx = DivergenceDetector._find_peaks(recent_macd, order=3)
        macd_lows_idx = DivergenceDetector._find_peaks(-recent_macd, order=3)

        MIN_DIV_CANDLES = 3  # mínimo de velas entre os pivots para divergência válida

        # Bearish divergence
        if len(price_highs_idx) >= 2 and len(macd_highs_idx) >= 2:
            ph_idx1, ph_idx2 = price_highs_idx[-2], price_highs_idx[-1]
            mh_idx1, mh_idx2 = macd_highs_idx[-2], macd_highs_idx[-1]

            if (abs(ph_idx2 - ph_idx1) >= MIN_DIV_CANDLES and
                recent_price[ph_idx2] > recent_price[ph_idx1] and
                recent_macd[mh_idx2] < recent_macd[mh_idx1]):
                return "BEAR"

        # Bullish divergence
        if len(price_lows_idx) >= 2 and len(macd_lows_idx) >= 2:
            pl_idx1, pl_idx2 = price_lows_idx[-2], price_lows_idx[-1]
            ml_idx1, ml_idx2 = macd_lows_idx[-2], macd_lows_idx[-1]

            if (abs(pl_idx2 - pl_idx1) >= MIN_DIV_CANDLES and
                recent_price[pl_idx2] < recent_price[pl_idx1] and
                recent_macd[ml_idx2] > recent_macd[ml_idx1]):
                return "BULL"

        return "NONE"

    @staticmethod
    def _find_peaks(data: np.ndarray, order: int = 3) -> np.ndarray:
        """
        Find peaks (local maxima) in 1D array.

        Simple peak detection algorithm.

        Args:
            data: 1D numpy array
            order: Minimum number of points on each side

        Returns:
            Array of peak indices
        """
        peaks = []
        n = len(data)

        for i in range(order, n - order):
            window = data[i - order:i + order + 1]
            if data[i] == np.max(window):
                peaks.append(i)

        return np.array(peaks)

    @staticmethod
    def detect_weighted(
        price: pd.Series,
        rsi: pd.Series,
        macd_hist: pd.Series,
        weights: dict = None
    ) -> Tuple[Literal["BULL", "BEAR", "NONE"], float]:
        """
        Detect divergence combining multiple indicators with weights.

        Args:
            price: Price series
            rsi: RSI series
            macd_hist: MACD histogram series
            weights: Dictionary with weights for each indicator

        Returns:
            Tuple of (divergence_type, confidence_score)
        """
        if weights is None:
            weights = {"rsi": 0.5, "macd": 0.5}

        rsi_div = DivergenceDetector.detect_rsi_divergence(price, rsi)
        macd_div = DivergenceDetector.detect_macd_divergence(price, macd_hist)

        # Count votes
        votes = {"BULL": 0, "BEAR": 0}

        if rsi_div != "NONE":
            votes[rsi_div] += weights.get("rsi", 0.5)
        if macd_div != "NONE":
            votes[macd_div] += weights.get("macd", 0.5)

        # Determine winner
        if votes["BULL"] > 0 and votes["BULL"] >= votes["BEAR"]:
            confidence = votes["BULL"] / sum(weights.values())
            return "BULL", confidence
        elif votes["BEAR"] > 0:
            confidence = votes["BEAR"] / sum(weights.values())
            return "BEAR", confidence

        return "NONE", 0.0
