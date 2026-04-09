from __future__ import absolute_import
"""
Signal generation engine for OB CASH 3.0.

This module contains the main logic for analyzing market data and generating
trading signals based on technical indicators and strategy rules.
"""

from typing import Tuple, Optional, List, Dict, Any
import pandas as pd
import numpy as np

from obcash3.config.settings import (
    DEFAULT_CONFIG, StrategyConfig
)
from obcash3.indicators.calculator import (
    calculate_adx, calculate_stochastic, calculate_rsi, calculate_macd,
    calculate_bollinger_bands, calculate_atr, calculate_ema, calculate_sma,
    calculate_slope
)
from obcash3.indicators.detector import DivergenceDetector
from obcash3.utils.time import now_br, get_current_session, get_session_config, is_liquid_hours
from obcash3.data.models import Signal
from obcash3.utils.logger import get_logger
from obcash3.utils import get_latest_value, safe_divide

logger = get_logger(__name__)


class SignalEngine:
    """Generates trading signals from OHLCV data."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize signal engine.

        Args:
            config: Configuration dictionary (uses defaults if None)
        """
        self.config = {**DEFAULT_CONFIG, **(config or {})}

    def generate_signal(
        self,
        df: pd.DataFrame,
        pair_name: str,
        interval: str,
        htf_df: Optional[pd.DataFrame] = None
    ) -> Signal:
        """
        Generate a trading signal from market data.

        Args:
            df: OHLCV DataFrame with at least 60 rows
            pair_name: Name of the trading pair
            interval: Timeframe used
            htf_df: Optional higher timeframe data for MTF confirmation

        Returns:
            Signal object with analysis results
        """
        # Validation
        if df is None or len(df) < 60:
            return Signal(
                asset=pair_name,
                interval=interval,
                timestamp=now_br(),
                action="AGUARDAR",
                strength="NEUTRO",
                score=0.0,
                price=0.0,
                filters=["Dados insuficientes"],
                raw_data={"error": f"Only {len(df) if df is not None else 0} candles"}
            )

        required_cols = ["Close", "Open"]
        for col in required_cols:
            if col not in df.columns:
                return Signal(
                    asset=pair_name,
                    interval=interval,
                    timestamp=now_br(),
                    action="AGUARDAR",
                    strength="NEUTRO",
                    score=0.0,
                    price=0.0,
                    filters=[f"Coluna ausente: {col}"],
                    raw_data={"error": f"Missing column: {col}"}
                )

        # Check if we have High/Low for advanced indicators
        has_hl = "High" in df.columns and "Low" in df.columns
        has_volume = "Volume" in df.columns and df["Volume"].sum() > 0

        # ================ CALCULATE INDICATORS ================
        c = df["Close"]
        o = df["Open"]
        h = df["High"] if has_hl else c
        l = df["Low"] if has_hl else c

        # Moving averages
        sma3_c = c.rolling(StrategyConfig.SMA_FAST).mean()
        sma50_c = c.rolling(StrategyConfig.SMA_SLOW).mean()
        sma3_o = o.rolling(StrategyConfig.SMA_FAST).mean()
        sma50_o = o.rolling(StrategyConfig.SMA_SLOW).mean()
        ema21 = calculate_ema(c, StrategyConfig.EMA_FAST)
        ema100 = calculate_ema(c, StrategyConfig.EMA_SLOW)

        # Momentum
        momentum = c - c.rolling(StrategyConfig.RSI_PERIOD).mean()
        dt = c.diff()
        avg_gain = dt.clip(lower=0).ewm(alpha=1/StrategyConfig.RSI_PERIOD,
                                        min_periods=StrategyConfig.RSI_PERIOD,
                                        adjust=False).mean()
        avg_loss = (-dt).clip(lower=0).ewm(alpha=1/StrategyConfig.RSI_PERIOD,
                                           min_periods=StrategyConfig.RSI_PERIOD,
                                           adjust=False).mean()
        rsi = 100 - (100 / (1 + avg_gain / avg_loss.replace(0, np.nan)))

        # Bollinger Bands
        bb_middle, bb_upper, bb_lower = calculate_bollinger_bands(
            c,
            StrategyConfig.BB_PERIOD,
            StrategyConfig.BB_STD
        )
        bb_width = (bb_upper - bb_lower) / bb_middle.replace(0, np.nan)

        # MACD
        macd_line, signal_line, macd_hist = calculate_macd(
            c,
            StrategyConfig.MACD_FAST,
            StrategyConfig.MACD_SLOW,
            StrategyConfig.MACD_SIGNAL
        )

        # ADX and ATR
        if has_hl:
            adx, atr = calculate_adx(df, StrategyConfig.ADX_PERIOD)
            stoch = calculate_stochastic(df, StrategyConfig.STOCH_PERIOD)
        else:
            adx = pd.Series([25.0] * len(c))
            atr = pd.Series([0.0] * len(c))
            stoch = pd.Series([50.0] * len(c))
        if has_volume:
            vol_mean_series = df["Volume"].rolling(20).mean()
            vol_ratio_series = safe_divide(df["Volume"], vol_mean_series, default=1.0)
            volume_ratio = vol_ratio_series.fillna(1.0)
        else:
            volume_ratio = pd.Series([1.0] * len(c))

        # ================ GET LATEST VALUES ================
        price = get_latest_value(c)
        sma3 = get_latest_value(sma3_c)
        sma50 = get_latest_value(sma50_c)
        sma3_o_val = get_latest_value(sma3_o)
        sma50_o_val = get_latest_value(sma50_o)
        ema21_val = get_latest_value(ema21)
        ema100_val = get_latest_value(ema100)
        momentum_val = get_latest_value(momentum)
        momentum_prev = get_latest_value(momentum, offset=1)
        close_prev2 = get_latest_value(c, offset=1)
        close_prev4 = get_latest_value(c, offset=3)
        open_val = get_latest_value(o)  # Get latest open price
        rsi_val = get_latest_value(rsi) or 50.0
        bbw_val = get_latest_value(bb_width) or 0.01
        macd_hist_val = get_latest_value(macd_hist)
        macd_hist_prev = get_latest_value(macd_hist, offset=1)
        adx_val = get_latest_value(adx) or 20.0
        atr_val = get_latest_value(atr)
        stoch_val = get_latest_value(stoch) or 50.0

        # Safe volume ratio calculation
        vol_mean = get_latest_value(df["Volume"].rolling(20).mean())
        vol_ratio = safe_divide(get_latest_value(df["Volume"]), vol_mean) if vol_mean > 0 else 1.0

        # SMA50 slope (last 6 periods) - use series, not single value
        if len(sma50_c) >= 6:
            sma50_recent_series = sma50_c.iloc[-6:]
            sma50_slope = (sma50_recent_series.iloc[-1] - sma50_recent_series.iloc[0]) / sma50_recent_series.iloc[0]
        else:
            sma50_slope = 0.0
        sma50_dir = "alta" if sma50_slope > StrategyConfig.SMA50_SLOPE_THRESHOLD else "baixa" if sma50_slope < -StrategyConfig.SMA50_SLOPE_THRESHOLD else "plano"

        # Distance to EMA100 (%)
        ema100_dist = abs(price - ema100_val) / ema100_val if ema100_val else 0.0

        # Vela direction and body %
        vela_alta = price > open_val
        corpo_pct = abs(price - open_val) / open_val if open_val else 0

        # Current session and min score
        session_name = get_current_session()
        session_config = get_session_config(session_name)
        session_min_score = session_config["min_score"]

        # ================ CONDITION CHECKS ================
        # Basic trend conditions
        is_bullish_setup = (
            momentum_val > momentum_prev and  # Momentum rising
            sma3 > sma50 and                  # Short MA above long MA
            price >= ema100_val and           # Price above EMA100
            sma3_o_val > sma50_o_val          # Open MA alignment
        )

        is_bearish_setup = (
            momentum_val < momentum_prev and  # Momentum falling
            sma3 < sma50 and                  # Short MA below long MA
            price <= ema100_val and           # Price below EMA100
            sma3_o_val < sma50_o_val          # Open MA alignment
        )

        # ================ FILTERS ================
        filters: List[str] = []

        # Liquidity filter
        if not is_liquid_hours() and self.config.get("filter_hours", True):
            filters.append("Horário baixa liquidez (22h-02h BRT) - filtrado")

        # Check if setup exists and apply additional filters
        blocked = False
        if is_bullish_setup or is_bearish_setup:
            # Price body check
            if is_bullish_setup and not vela_alta:
                filters.append("Vela sem corpo comprador - aguardar verde")
            elif is_bearish_setup and vela_alta:
                filters.append("Vela sem corpo vendedor - aguardar vermelha")

            # Market regime
            market_regime = "TENDENCIA" if adx_val > 25 else "NORMAL" if adx_val > 15 else "LATERAL"
            if market_regime == "LATERAL":
                filters.append(f"Mercado lateral (ADX={adx_val:.1f})")

            # EMA distance
            if ema100_dist * 100 > StrategyConfig.MAX_EMA100_DISTANCE_PCT:
                filters.append(f"Preço longe da EMA100 ({ema100_dist * 100:.2f}%)")

            # RSI extremes
            if is_bullish_setup and rsi_val > StrategyConfig.RSI_OVERBOUGHT:
                filters.append(f"RSI sobrecomprado ({rsi_val:.1f})")
            elif is_bearish_setup and rsi_val < StrategyConfig.RSI_OVERSOLD:
                filters.append(f"RSI sobrevendido ({rsi_val:.1f})")

            # Stochastic extremes
            if is_bullish_setup and stoch_val > StrategyConfig.STOCH_OVERBOUGHT:
                filters.append(f"Stoch sobrecomprado ({stoch_val:.1f})")
            elif is_bearish_setup and stoch_val < StrategyConfig.STOCH_OVERSOLD:
                filters.append(f"Stoch sobrevendido ({stoch_val:.1f})")

            blocked = len(filters) > 0
        else:
            # Not a setup yet
            market_regime = "TENDENCIA" if adx_val > 25 else "NORMAL" if adx_val > 15 else "LATERAL"

        # ================ SCORE CALCULATION ================
        def calculate_score(is_buy: bool) -> float:
            """
            Calculate signal score (0-100%).

            Scoring weights:
            - RSI zone: 2 pts (ideal 35-65)
            - MACD alignment: 2 pts
            - MACD momentum: 1 pt
            - EMA alignment: 2 pts
            - SMA50 slope: 1 pt
            - Position vs EMA100: 2 pts
            - 3-candle momentum: 1 pt
            - Price vs 4-candle ago: 1 pt
            - Volume spike: 1 pt
            - BB width: 1 pt
            - Stoch zone: 1 pt
            - Market regime: 2 pts
            """
            score = 0.0

            # RSI (2 pts)
            if is_buy:
                score += 2 if 35 < rsi_val < 65 else (1 if 30 < rsi_val < 70 else 0)
            else:
                score += 2 if 35 < rsi_val < 65 else (1 if 30 < rsi_val < 70 else 0)

            # MACD histogram sign (2 pts)
            if (is_buy and macd_hist_val > 0) or (not is_buy and macd_hist_val < 0):
                score += 2

            # MACD momentum (1 pt)
            if is_buy and macd_hist_val > macd_hist_prev:
                score += 1
            elif not is_buy and macd_hist_val < macd_hist_prev:
                score += 1

            # EMA21 alignment (2 pts)
            if (is_buy and price > ema21_val) or (not is_buy and price < ema21_val):
                score += 2

            # SMA50 slope (1 pt)
            if (is_buy and sma50_slope > 0) or (not is_buy and sma50_slope < 0):
                score += 1

            # Price above/below EMA100 (2 pts)
            if (is_buy and price >= ema100_val) or (not is_buy and price <= ema100_val):
                score += 2

            # Momentum vs 2 candles ago (1 pt)
            if (is_buy and momentum_val > momentum_prev) or (not is_buy and momentum_val < momentum_prev):
                score += 1

            # Price vs 4 candles ago (1 pt)
            if (is_buy and price > close_prev4) or (not is_buy and price < close_prev4):
                score += 1

            # Volume (1 pt)
            if vol_ratio >= StrategyConfig.VOLUME_SPIKE_RATIO:
                score += 1

            # Bollinger Band width (1 pt)
            if bbw_val > StrategyConfig.BB_MIN_WIDTH:
                score += 1

            # Stochastic zone (1 pt)
            if (is_buy and 20 < stoch_val < 70) or (not is_buy and 30 < stoch_val < 80):
                score += 1

            # Market regime (2 pts)
            if market_regime == "TENDENCIA":
                score += 2
            elif market_regime == "NORMAL":
                score += 1

            # Apply multiplier for strong momentum
            final_score = score / 15 * 100
            return max(0, min(100, final_score))

        buy_score = calculate_score(True)
        sell_score = calculate_score(False)

        # ================ MULTI-TIMEFRAME CONFIRMATION ================
        mtf_result = "N/A"
        if self.config.get("mtf_confirm", True) and htf_df is not None and len(htf_df) >= 50:
            mtf_trend = self._check_mtf_trend(htf_df)
            if is_bullish_setup and mtf_trend in ("Bullish", "Neutral"):
                buy_score += StrategyConfig.MTF_BONUS_POINTS
                mtf_result = "Agree"
            elif is_bullish_setup and mtf_trend == "Bearish":
                buy_score -= StrategyConfig.MTF_BONUS_POINTS
                mtf_result = "Disagree"
            elif is_bearish_setup and mtf_trend in ("Bearish", "Neutral"):
                sell_score += StrategyConfig.MTF_BONUS_POINTS
                mtf_result = "Agree"
            elif is_bearish_setup and mtf_trend == "Bullish":
                sell_score -= StrategyConfig.MTF_BONUS_POINTS
                mtf_result = "Disagree"

        # Clamp scores
        buy_score = max(0, min(100, buy_score))
        sell_score = max(0, min(100, sell_score))

        # ================ DIVERGENCE DETECTION ================
        divergence = "NONE"
        if self.config.get("divergence_detect", True):
            div_type, confidence = DivergenceDetector.detect_weighted(c, rsi, macd_hist)
            if div_type == "BULL" and is_bullish_setup:
                buy_score += StrategyConfig.DIVERGENCE_BONUS_POINTS
                divergence = "BULL"
            elif div_type == "BEAR" and is_bearish_setup:
                sell_score += StrategyConfig.DIVERGENCE_BONUS_POINTS
                divergence = "BEAR"

        # ================ FINAL DECISION ================
        action: str
        strength: str
        final_score: float
        final_filters = filters.copy()

        if is_bullish_setup and not blocked and buy_score >= session_min_score:
            action = "COMPRA"
            strength = "FORTE" if buy_score >= 70 else "MODERADO"
            final_score = buy_score
            final_filters = ["Todos os filtros OK"]
        elif is_bearish_setup and not blocked and sell_score >= session_min_score:
            action = "VENDA"
            strength = "FORTE" if sell_score >= 70 else "MODERADO"
            final_score = sell_score
            final_filters = ["Todos os filtros OK"]
        elif is_bullish_setup and (blocked or buy_score < session_min_score):
            action = "EVITA"
            strength = "FRACO"
            final_score = buy_score
            if not blocked:
                final_filters = [f"Score baixo ({buy_score:.0f}%<{session_min_score:.0f}%)"]
            final_filters.insert(0, "FRACO - NÃO ENTRE")
        elif is_bearish_setup and (blocked or sell_score < session_min_score):
            action = "EVITA"
            strength = "FRACO"
            final_score = sell_score
            if not blocked:
                final_filters = [f"Score baixo ({sell_score:.0f}%<{session_min_score:.0f}%)"]
            final_filters.insert(0, "FRACO - NÃO ENTRE")
        else:
            action = "AGUARDAR"
            strength = "NEUTRO"
            final_score = 0.0
            cond_buy_list = [
                momentum_val > momentum_prev,
                sma3 > sma50,
                price >= ema100_val,
                sma3_o_val > sma50_o_val,
            ]
            cond_sell_list = [
                momentum_val < momentum_prev,
                sma3 < sma50,
                price <= ema100_val,
                sma3_o_val < sma50_o_val,
            ]
            fc = sum(1 for c in cond_buy_list if not c)   # quantas FALTAM para compra
            fv = sum(1 for c in cond_sell_list if not c)  # quantas FALTAM para venda
            near = "COMPRA" if fc <= fv else "VENDA"
            missing = min(fc, fv)
            final_filters = [f"Perto de: {near} (falta {missing} condição{'ões' if missing > 1 else ''})"]

        # ================ STOP LOSS / TAKE PROFIT ================
        if action in ("COMPRA", "VENDA") and atr_val > 0:
            sl_mult = StrategyConfig.STOP_LOSS_ATR_MULTIPLIER
            tp_mult = StrategyConfig.TAKE_PROFIT_ATR_MULTIPLIER
            if action == "COMPRA":
                sl_price = price - sl_mult * atr_val
                tp_price = price + tp_mult * atr_val
            else:  # VENDA
                sl_price = price + sl_mult * atr_val
                tp_price = price - tp_mult * atr_val
        else:
            sl_price = None
            tp_price = None

        # Position sizing
        balance = float(self.config.get("account_balance", 1000))
        risk_pct = float(self.config.get("risk_pct", 1.0))
        risk_usd = balance * (risk_pct / 100)
        stop_distance = abs(price - sl_price) if sl_price else 0
        position_units = risk_usd / stop_distance if stop_distance > 0 else 0

        # Create Signal object
        signal = Signal(
            asset=pair_name,
            interval=interval,
            timestamp=now_br(),
            action=action,
            strength=strength,
            score=round(final_score, 1),
            price=price,
            sl=sl_price,
            tp=tp_price,
            atr=atr_val,
            rsi=rsi_val,
            adx=adx_val,
            macd_hist=macd_hist_val,
            stoch=stoch_val,
            bb_width=bbw_val,
            market_regime=market_regime,
            session=session_config["label"],
            session_raw=session_name,
            mtf_confirmation=mtf_result,
            divergence=divergence,
            source="OB CASH 3.0",
            conditions_buy=[
                momentum_val > momentum_prev,
                sma3 > sma50,
                price >= ema100_val,
                sma3_o_val > sma50_o_val
            ],
            conditions_sell=[
                momentum_val < momentum_prev,
                sma3 < sma50,
                price <= ema100_val,
                sma3_o_val < sma50_o_val
            ],
            filters=final_filters
        )

        # Add raw data for charting
        signal.raw_data = {
            "close_series": c.tolist()[-100:],  # Last 100 points for chart
            "ema21_series": ema21.tolist()[-100:],
            "ema100_series": ema100.tolist()[-100:],
        }

        logger.debug("Signal generated: %s %s %s score=%.1f",
                     pair_name, interval, action, final_score)

        return signal

    def _check_mtf_trend(self, htf_df: pd.DataFrame) -> str:
        """
        Check trend on higher timeframe.

        Args:
            htf_df: Higher timeframe OHLCV data

        Returns:
            "Bullish", "Bearish", or "Neutral"
        """
        c = htf_df["Close"]
        ema50 = c.ewm(span=50, adjust=False).mean()
        momentum = c - c.rolling(14).mean()

        current_close = c.iloc[-1]
        current_ema = ema50.iloc[-1]
        current_momentum = momentum.iloc[-1]
        prev_momentum = momentum.iloc[-2]

        if current_close > current_ema and current_momentum > prev_momentum:
            return "Bullish"
        elif current_close < current_ema and current_momentum < prev_momentum:
            return "Bearish"
        else:
            return "Neutral"
