"""
Candle pattern detection for OB CASH 3.0.

Detects high-probability reversal and continuation patterns.
"""

from typing import Literal
import pandas as pd
import numpy as np


class CandlePatterns:
    """Detects candlestick patterns."""

    @staticmethod
    def is_bullish_engulfing(df: pd.DataFrame) -> bool:
        """
        Bullish Engulfing pattern:
        - Previous candle: red (close < open)
        - Current candle: green (close > open)
        - Current body completely engulfs previous body
        """
        if len(df) < 2:
            return False

        prev = df.iloc[-2]
        curr = df.iloc[-1]

        prev_red = prev['Close'] < prev['Open']
        curr_green = curr['Close'] > curr['Open']
        engulfs = (curr['Open'] <= prev['Close'] and curr['Close'] >= prev['Open'])

        return prev_red and curr_green and engulfs

    @staticmethod
    def is_bearish_engulfing(df: pd.DataFrame) -> bool:
        """
        Bearish Engulfing pattern:
        - Previous candle: green
        - Current candle: red
        - Current body completely engulfs previous body
        """
        if len(df) < 2:
            return False

        prev = df.iloc[-2]
        curr = df.iloc[-1]

        prev_green = prev['Close'] > prev['Open']
        curr_red = curr['Close'] < curr['Open']
        engulfs = (curr['Open'] >= prev['Close'] and curr['Close'] <= prev['Open'])

        return prev_green and curr_red and engulfs

    @staticmethod
    def is_hammer(df: pd.DataFrame) -> bool:
        """
        Hammer: Bullish reversal pattern at bottom.
        - Small body (near top)
        - Long lower wick (at least 2x body)
        - Little/no upper wick
        """
        if len(df) < 1:
            return False

        curr = df.iloc[-1]
        body = abs(curr['Close'] - curr['Open'])
        lower_wick = curr['Low'] - min(curr['Open'], curr['Close'])
        upper_wick = max(curr['Open'], curr['Close']) - curr['High']

        # Hammer conditions
        small_body = body <= (curr['High'] - curr['Low']) * 0.3
        long_lower = lower_wick >= body * 2
        small_upper = upper_wick <= body * 0.2

        return small_body and long_lower and small_upper

    @staticmethod
    def is_shooting_star(df: pd.DataFrame) -> bool:
        """
        Shooting Star: Bearish reversal pattern at top.
        - Small body (near bottom)
        - Long upper wick (at least 2x body)
        - Little/no lower wick
        """
        if len(df) < 1:
            return False

        curr = df.iloc[-1]
        body = abs(curr['Close'] - curr['Open'])
        upper_wick = max(curr['Open'], curr['Close']) - curr['High']
        lower_wick = min(curr['Open'], curr['Close']) - curr['Low']

        # Shooting star conditions
        small_body = body <= (curr['High'] - curr['Low']) * 0.3
        long_upper = upper_wick >= body * 2
        small_lower = lower_wick <= body * 0.2

        return small_body and long_upper and small_lower

    @staticmethod
    def is_morning_star(df: pd.DataFrame) -> bool:
        """
        Morning Star: 3-candle bullish reversal.
        - 1st: long red candle
        - 2nd: small body (star) with gap down
        - 3rd: long green candle that enters 1st's body
        """
        if len(df) < 3:
            return False

        c1 = df.iloc[-3]
        c2 = df.iloc[-2]
        c3 = df.iloc[-1]

        # Candle 1: long red
        c1_red = c1['Close'] < c1['Open']
        c1_long = abs(c1['Close'] - c1['Open']) >= (c1['High'] - c1['Low']) * 0.6

        # Candle 2: small body (star) with gap
        c2_small = abs(c2['Close'] - c2['Open']) <= (c2['High'] - c2['Low']) * 0.3
        gap_down = c2['High'] < min(c1['Open'], c1['Close'])

        # Candle 3: long green that enters c1 body
        c3_green = c3['Close'] > c3['Open']
        c3_long = abs(c3['Close'] - c3['Open']) >= (c3['High'] - c3['Low']) * 0.6
        enters_c1 = c3['Close'] > min(c1['Open'], c1['Close'])

        return c1_red and c1_long and c2_small and gap_down and c3_green and c3_long and enters_c1

    @staticmethod
    def is_evening_star(df: pd.DataFrame) -> bool:
        """
        Evening Star: 3-candle bearish reversal.
        - 1st: long green candle
        - 2nd: small body (star) with gap up
        - 3rd: long red candle that enters 1st's body
        """
        if len(df) < 3:
            return False

        c1 = df.iloc[-3]
        c2 = df.iloc[-2]
        c3 = df.iloc[-1]

        # Candle 1: long green
        c1_green = c1['Close'] > c1['Open']
        c1_long = abs(c1['Close'] - c1['Open']) >= (c1['High'] - c1['Low']) * 0.6

        # Candle 2: small body with gap
        c2_small = abs(c2['Close'] - c2['Open']) <= (c2['High'] - c2['Low']) * 0.3
        gap_up = c2['Low'] > max(c1['Open'], c1['Close'])

        # Candle 3: long red that enters c1 body
        c3_red = c3['Close'] < c3['Open']
        c3_long = abs(c3['Close'] - c3['Open']) >= (c3['High'] - c3['Low']) * 0.6
        enters_c1 = c3['Close'] < max(c1['Open'], c1['Close'])

        return c1_green and c1_long and c2_small and gap_up and c3_red and c3_long and enters_c1

    @staticmethod
    def is_three_white_soldiers(df: pd.DataFrame) -> bool:
        """
        Three White Soldiers: Strong bullish continuation.
        - 3 consecutive green candles
        - Each closes near its high
        - Each opens within previous body
        """
        if len(df) < 3:
            return False

        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]

        all_green = (
            c1['Close'] > c1['Open'] and
            c2['Close'] > c2['Open'] and
            c3['Close'] > c3['Open']
        )

        closes_near_high = (
            (c1['High'] - c1['Low']) > 0 and (c1['Close'] - c1['Low']) / (c1['High'] - c1['Low']) > 0.7 and
            (c2['High'] - c2['Low']) > 0 and (c2['Close'] - c2['Low']) / (c2['High'] - c2['Low']) > 0.7 and
            (c3['High'] - c3['Low']) > 0 and (c3['Close'] - c3['Low']) / (c3['High'] - c3['Low']) > 0.7
        )

        opens_inside = (
            c2['Open'] >= min(c1['Open'], c1['Close']) and c2['Open'] <= max(c1['Open'], c1['Close']) and
            c3['Open'] >= min(c2['Open'], c2['Close']) and c3['Open'] <= max(c2['Open'], c2['Close'])
        )

        return all_green and closes_near_high and opens_inside

    @staticmethod
    def is_three_black_crows(df: pd.DataFrame) -> bool:
        """
        Three Black Crows: Strong bearish continuation.
        - 3 consecutive red candles
        - Each closes near its low
        - Each opens within previous body
        """
        if len(df) < 3:
            return False

        c1, c2, c3 = df.iloc[-3], df.iloc[-2], df.iloc[-1]

        all_red = (
            c1['Close'] < c1['Open'] and
            c2['Close'] < c2['Open'] and
            c3['Close'] < c3['Open']
        )

        closes_near_low = (
            (c1['High'] - c1['Low']) > 0 and (c1['High'] - c1['Close']) / (c1['High'] - c1['Low']) > 0.7 and
            (c2['High'] - c2['Low']) > 0 and (c2['High'] - c2['Close']) / (c2['High'] - c2['Low']) > 0.7 and
            (c3['High'] - c3['Low']) > 0 and (c3['High'] - c3['Close']) / (c3['High'] - c3['Low']) > 0.7
        )

        opens_inside = (
            c2['Open'] >= min(c1['Open'], c1['Close']) and c2['Open'] <= max(c1['Open'], c1['Close']) and
            c3['Open'] >= min(c2['Open'], c2['Close']) and c3['Open'] <= max(c2['Open'], c2['Close'])
        )

        return all_red and closes_near_low and opens_inside

    @staticmethod
    def detect_pattern(df: pd.DataFrame) -> tuple:
        """
        Detect any significant pattern.

        Returns:
            (pattern_name, is_bullish) or ("NONE", False)
        """
        patterns = [
            ("BULLISH_ENGULFING", True, CandlePatterns.is_bullish_engulfing(df)),
            ("BEARISH_ENGULFING", False, CandlePatterns.is_bearish_engulfing(df)),
            ("HAMMER", True, CandlePatterns.is_hammer(df)),
            ("SHOOTING_STAR", False, CandlePatterns.is_shooting_star(df)),
            ("MORNING_STAR", True, CandlePatterns.is_morning_star(df)),
            ("EVENING_STAR", False, CandlePatterns.is_evening_star(df)),
            ("THREE_WHITE_SOLDIERS", True, CandlePatterns.is_three_white_soldiers(df)),
            ("THREE_BLACK_CROWS", False, CandlePatterns.is_three_black_crows(df)),
        ]

        for name, is_bullish, detected in patterns:
            if detected:
                return name, is_bullish

        return "NONE", False
