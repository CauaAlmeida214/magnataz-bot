"""
Signal generation engine v2 - enhanced scoring with stricter entry quality.

This version keeps the original scoring idea, but reduces false positives by:
- using only confirmed candles for decision-making
- adding trend and impulse quality gates
- being stricter on lower timeframes, especially 1m
"""

from typing import Optional, List, Dict, Any
import hashlib

import numpy as np
import pandas as pd

from obcash3.config.settings import StrategyConfig, DEFAULT_CONFIG
from obcash3.indicators.calculator import (
    calculate_adx,
    calculate_stochastic,
    calculate_rsi,
    calculate_macd,
    calculate_bollinger_bands,
    calculate_ema,
)
from obcash3.indicators.detector import DivergenceDetector
from obcash3.indicators.candle_patterns import CandlePatterns
from obcash3.utils.time import now_br, get_current_session, get_session_config, is_liquid_hours
from obcash3.data.models import Signal
from obcash3.utils.logger import get_logger
from obcash3.utils import get_latest_value, safe_divide

logger = get_logger(__name__)


class SignalEngineV2:
    """Enhanced signal generation with improved scoring and stricter quality gates."""

    MIN_SCORE_BY_INTERVAL = {
        "1m": 60.0,
        "5m": 55.0,
        "15m": 50.0,
        "30m": 48.0,
        "1h": 45.0,
    }

    STRONG_SCORE_BY_INTERVAL = {
        "1m": 70.0,
        "5m": 72.0,
        "15m": 75.0,
        "30m": 75.0,
        "1h": 75.0,
    }

    MIN_BODY_RATIO_BY_INTERVAL = {
        "1m": 0.45,
        "5m": 0.38,
        "15m": 0.32,
        "30m": 0.28,
        "1h": 0.24,
    }

    MIN_BODY_ATR_RATIO_BY_INTERVAL = {
        "1m": 0.12,
        "5m": 0.10,
        "15m": 0.08,
        "30m": 0.07,
        "1h": 0.06,
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self._last_hash: Dict[str, str] = {}
        self._last_signal: Dict[str, Any] = {}

    def generate_signal(
        self,
        df: pd.DataFrame,
        pair_name: str,
        interval: str,
        htf_df: Optional[pd.DataFrame] = None,
        market_context: Optional[Dict[str, Any]] = None,
    ) -> Signal:
        """Generate a trading signal with stricter confirmation logic."""
        if df is None or len(df) < 60:
            count = len(df) if df is not None else 0
            return self._error_signal(pair_name, interval, f"Dados insuficientes ({count} candles)")

        required_cols = ["Close", "Open", "High", "Low"]
        for col in required_cols:
            if col not in df.columns:
                return self._error_signal(pair_name, interval, f"Coluna ausente: {col}")

        cache_key = f"{pair_name}_{interval}"
        data_hash = hashlib.md5(
            f"{df['Close'].iloc[-1]}{df['Open'].iloc[-1]}{len(df)}".encode()
        ).hexdigest()

        c = df["Close"]
        o = df["Open"]
        h = df["High"]
        l = df["Low"]

        sma3 = c.rolling(3).mean()
        sma50 = c.rolling(50).mean()
        sma3_o = o.rolling(3).mean()
        sma50_o = o.rolling(50).mean()
        ema21 = calculate_ema(c, 21)
        ema100 = calculate_ema(c, 100)

        rsi = calculate_rsi(c, 14)
        stoch = calculate_stochastic(df, 14)
        _, _, macd_hist = calculate_macd(c, 12, 26, 9)
        adx, atr = calculate_adx(df, 14)

        bb_middle, bb_upper, bb_lower = calculate_bollinger_bands(c, 20, 2.0)
        bb_width = (bb_upper - bb_lower) / bb_middle.replace(0, np.nan)

        vol_mean = df["Volume"].rolling(20).mean()
        volume_val = self._clean_number(get_latest_value(df["Volume"], offset=1), 0.0)
        vol_mean_val = self._clean_number(get_latest_value(vol_mean, offset=1), 0.0)
        vol_ratio = safe_divide(volume_val, vol_mean_val, default=1.0)

        momentum_series = c - c.rolling(14).mean()

        price_confirmed = self._clean_number(get_latest_value(c, offset=1), self._clean_number(get_latest_value(c), 0.0))
        open_confirmed = self._clean_number(get_latest_value(o, offset=1), price_confirmed)
        high_confirmed = self._clean_number(get_latest_value(h, offset=1), price_confirmed)
        low_confirmed = self._clean_number(get_latest_value(l, offset=1), price_confirmed)

        sma3_val = self._clean_number(get_latest_value(sma3, offset=1), price_confirmed)
        sma50_val = self._clean_number(get_latest_value(sma50, offset=1), price_confirmed)
        sma3_o_val = self._clean_number(get_latest_value(sma3_o, offset=1), open_confirmed)
        sma50_o_val = self._clean_number(get_latest_value(sma50_o, offset=1), open_confirmed)
        ema21_val = self._clean_number(get_latest_value(ema21, offset=1), price_confirmed)
        ema21_prev = self._clean_number(get_latest_value(ema21, offset=2), ema21_val)
        ema100_val = self._clean_number(get_latest_value(ema100, offset=1), price_confirmed)
        momentum_val = self._clean_number(get_latest_value(momentum_series, offset=1), 0.0)
        momentum_prev = self._clean_number(get_latest_value(momentum_series, offset=2), momentum_val)

        rsi_val = self._clean_number(get_latest_value(rsi, offset=1), 50.0)
        stoch_val = self._clean_number(get_latest_value(stoch, offset=1), 50.0)
        macd_hist_val = self._clean_number(get_latest_value(macd_hist, offset=1), 0.0)
        macd_hist_prev = self._clean_number(get_latest_value(macd_hist, offset=2), macd_hist_val)
        adx_val = self._clean_number(get_latest_value(adx, offset=1), 20.0)
        atr_val = self._clean_number(get_latest_value(atr, offset=1), 0.0)
        bbw_val = self._clean_number(get_latest_value(bb_width, offset=1), 0.01)
        bb_upper_val = self._clean_number(get_latest_value(bb_upper, offset=1), price_confirmed)
        bb_lower_val = self._clean_number(get_latest_value(bb_lower, offset=1), price_confirmed)
        close_prev4 = self._clean_number(get_latest_value(c, offset=4), price_confirmed)

        if len(sma50) >= 6:
            sma50_recent = sma50.iloc[-6:]
            denom = self._clean_number(sma50_recent.iloc[0], 0.0)
            sma50_slope = safe_divide(sma50_recent.iloc[-1] - sma50_recent.iloc[0], denom, default=0.0)
        else:
            sma50_slope = 0.0

        ema21_slope = safe_divide(ema21_val - ema21_prev, abs(ema21_prev), default=0.0) if ema21_prev else 0.0
        candle_range = max(high_confirmed - low_confirmed, 1e-9)
        candle_body = abs(price_confirmed - open_confirmed)
        body_ratio = candle_body / candle_range
        body_atr_ratio = safe_divide(candle_body, atr_val, default=0.0)
        price = price_confirmed

        market_regime = "TENDENCIA" if adx_val > 25 else "NORMAL" if adx_val > 15 else "LATERAL"

        session_name = get_current_session()
        session_config = get_session_config(session_name)
        session_min_score = session_config["min_score"]
        market_context = market_context or {}
        adaptive_filtering = bool(self.config.get("adaptive_filtering", True))
        context_policy_state = str(market_context.get("policy_state", "learning"))
        context_notes = [str(item) for item in market_context.get("notes", []) if str(item).strip()]
        context_win_rate = self._clean_number(market_context.get("win_rate"), 0.50)
        context_recent_win_rate = self._clean_number(market_context.get("recent_win_rate"), context_win_rate)
        context_profit_factor = self._clean_number(market_context.get("profit_factor"), 1.00)
        context_resolved_trades = int(market_context.get("resolved_trades", 0) or 0)
        threshold_adjustment = (
            self._clean_number(market_context.get("threshold_adjustment"), 0.0)
            if adaptive_filtering
            else 0.0
        )
        confidence_boost = (
            self._clean_number(market_context.get("confidence_boost"), 0.0)
            if adaptive_filtering
            else 0.0
        )

        is_bullish_setup = (
            sma3_val > sma50_val
            and price_confirmed >= ema100_val
            and momentum_val > momentum_prev
            and sma3_o_val > sma50_o_val
        )

        is_bearish_setup = (
            sma3_val < sma50_val
            and price_confirmed <= ema100_val
            and momentum_val < momentum_prev
            and sma3_o_val < sma50_o_val
        )

        trend_stack_buy = (
            price_confirmed > ema21_val > ema100_val
            and ema21_slope > 0
            and sma50_slope > 0
        )

        trend_stack_sell = (
            price_confirmed < ema21_val < ema100_val
            and ema21_slope < 0
            and sma50_slope < 0
        )

        macd_impulse_buy = macd_hist_val > 0 and macd_hist_val > macd_hist_prev
        macd_impulse_sell = macd_hist_val < 0 and macd_hist_val < macd_hist_prev

        pattern_df = df.iloc[:-1] if len(df) > 1 else df

        # Consecutive candle direction check (last 3 confirmed candles)
        consec_bullish = 0
        consec_bearish = 0
        for back in range(1, 4):
            c_back = self._clean_number(get_latest_value(c, offset=back), 0.0)
            o_back = self._clean_number(get_latest_value(o, offset=back), 0.0)
            if c_back > o_back:
                consec_bullish += 1
            elif c_back < o_back:
                consec_bearish += 1

        # Reliable volume detection (forex APIs often return 0)
        has_reliable_volume = vol_mean_val > 0 and volume_val > 0

        def calculate_enhanced_score(is_buy: bool) -> float:
            score = 0.0
            strong_confirmations = 0

            # RSI zone (max 2)
            if is_buy:
                rsi_pts = 2 if 30 < rsi_val < 55 else (1 if 25 < rsi_val < 65 else 0)
            else:
                rsi_pts = 2 if 45 < rsi_val < 70 else (1 if 35 < rsi_val < 75 else 0)
            score += rsi_pts
            if rsi_pts == 2:
                strong_confirmations += 1

            # MACD alignment (max 2.5 — increased weight)
            macd_pts = 0.0
            if (is_buy and macd_hist_val > 0) or (not is_buy and macd_hist_val < 0):
                macd_pts = 2.5
                strong_confirmations += 1
            score += macd_pts

            # EMA alignment (max 2)
            ema_pts = 0
            if (is_buy and price_confirmed > ema21_val) or (not is_buy and price_confirmed < ema21_val):
                ema_pts = 2
                strong_confirmations += 1
            score += ema_pts

            # Stochastic zone (max 1.5 — reduced, overlaps with RSI)
            if is_buy:
                stoch_pts = 1.5 if 20 < stoch_val < 60 else (0.5 if 15 < stoch_val < 75 else 0)
            else:
                stoch_pts = 1.5 if 40 < stoch_val < 80 else (0.5 if 25 < stoch_val < 85 else 0)
            score += stoch_pts

            # ADX/regime (max 2.5 — increased weight, most important for trend detection)
            adx_pts = 0.0
            if market_regime == "TENDENCIA":
                adx_pts = 2.5
                strong_confirmations += 1
            elif market_regime == "NORMAL":
                adx_pts = 1.0
            score += adx_pts

            # SMA slope (max 1)
            if (is_buy and sma50_slope > 0.0001) or (not is_buy and sma50_slope < -0.0001):
                score += 1

            # Momentum (max 1.5 — increased weight)
            mom_pts = 0.0
            if (is_buy and momentum_val > momentum_prev) or (not is_buy and momentum_val < momentum_prev):
                mom_pts = 1.5
            score += mom_pts

            # Price momentum (max 1)
            if (is_buy and price_confirmed > close_prev4) or (not is_buy and price_confirmed < close_prev4):
                score += 1

            # Volume (max 0.5 for forex — reduced from 2)
            if has_reliable_volume:
                if vol_ratio >= 2.0:
                    score += 0.5
                elif vol_ratio >= 1.5:
                    score += 0.25

            # BB width (max 1.5)
            if bbw_val > 0.002:
                score += 1.5
            elif bbw_val > 0.0008:
                score += 0.5

            # BB position (max 1)
            bb_pos = 0.5
            if bb_upper_val != bb_lower_val:
                bb_pos = (price_confirmed - bb_lower_val) / (bb_upper_val - bb_lower_val)
            if is_buy and bb_pos < 0.3:
                score += 1
            elif not is_buy and bb_pos > 0.7:
                score += 1

            # Pattern (max 1.5)
            pattern_name, pattern_bullish = CandlePatterns.detect_pattern(pattern_df)
            if pattern_name != "NONE":
                if (is_buy and pattern_bullish) or (not is_buy and not pattern_bullish):
                    score += 1.5
                else:
                    score -= 1

            # Consecutive candle direction bonus (max 1.5)
            if is_buy and consec_bullish >= 2:
                score += 1.0 if consec_bullish == 2 else 1.5
                strong_confirmations += 1
            elif not is_buy and consec_bearish >= 2:
                score += 1.0 if consec_bearish == 2 else 1.5
                strong_confirmations += 1

            return score, strong_confirmations

        buy_score_pts, buy_strong_confirms = calculate_enhanced_score(True)
        sell_score_pts, sell_strong_confirms = calculate_enhanced_score(False)

        # Max possible score: ~19 (adjusted for forex volume reality)
        SCORE_DENOMINATOR = 19.0

        hard_filters: List[str] = []
        advisory_filters: List[str] = []

        if not is_liquid_hours() and self.config.get("filter_hours", True):
            hard_filters.append("Horario baixa liquidez")

        bullish_candle = price_confirmed > open_confirmed
        if is_bullish_setup and not bullish_candle:
            advisory_filters.append("Vela sem corpo comprador")
        elif is_bearish_setup and bullish_candle:
            advisory_filters.append("Vela sem corpo vendedor")

        if is_bullish_setup and rsi_val > 80:
            advisory_filters.append(f"RSI sobrecomprado ({rsi_val:.0f})")
        elif is_bearish_setup and rsi_val < 20:
            advisory_filters.append(f"RSI sobrevendido ({rsi_val:.0f})")

        if is_bullish_setup and stoch_val > 90:
            advisory_filters.append(f"Stoch sobrecomprado ({stoch_val:.0f})")
        elif is_bearish_setup and stoch_val < 10:
            advisory_filters.append(f"Stoch sobrevendido ({stoch_val:.0f})")

        ema100_dist = safe_divide(abs(price_confirmed - ema100_val), ema100_val, default=0.0)
        if ema100_dist * 100 > 0.5:
            advisory_filters.append(f"Preco longe da EMA100 ({ema100_dist * 100:.1f}%)")

        mtf_result = "N/A"
        if self.config.get("mtf_confirm", True) and htf_df is not None and len(htf_df) >= 50:
            mtf_trend = self._check_mtf_structure(htf_df)
            if is_bullish_setup and mtf_trend in ("Bullish", "Neutral"):
                buy_score_pts += 3
                mtf_result = "Agree"
            elif is_bullish_setup and mtf_trend == "Bearish":
                buy_score_pts -= 2
                mtf_result = "Disagree"
            elif is_bearish_setup and mtf_trend in ("Bearish", "Neutral"):
                sell_score_pts += 3
                mtf_result = "Agree"
            elif is_bearish_setup and mtf_trend == "Bullish":
                sell_score_pts -= 2
                mtf_result = "Disagree"

        divergence = "NONE"
        if self.config.get("divergence_detect", True):
            div_type, confidence = DivergenceDetector.detect_weighted(c, rsi, macd_hist)
            if div_type == "BULL" and is_bullish_setup:
                buy_score_pts += int(3 * confidence)
                divergence = f"BULL({confidence:.0%})"
            elif div_type == "BEAR" and is_bearish_setup:
                sell_score_pts += int(3 * confidence)
                divergence = f"BEAR({confidence:.0%})"

        if adaptive_filtering:
            # Scale adaptive impact based on sample size (more trades = stronger adjustment)
            sample_strength = min(1.0, context_resolved_trades / 8.0) if context_resolved_trades > 0 else 0.0
            if context_policy_state == "boost":
                boost_pts = 2.5 * sample_strength
                buy_score_pts += boost_pts
                sell_score_pts += boost_pts
            elif context_policy_state == "caution":
                penalty_pts = 2.0 * sample_strength
                buy_score_pts -= penalty_pts
                sell_score_pts -= penalty_pts
            elif context_policy_state == "blocked":
                penalty_pts = 3.5 * sample_strength
                buy_score_pts -= penalty_pts
                sell_score_pts -= penalty_pts

        buy_score = max(0.0, min(100.0, (buy_score_pts / SCORE_DENOMINATOR) * 100.0))
        sell_score = max(0.0, min(100.0, (sell_score_pts / SCORE_DENOMINATOR) * 100.0))

        if is_bullish_setup:
            if not trend_stack_buy:
                advisory_filters.append("Tendencia curta desalinhada")
            if not macd_impulse_buy:
                advisory_filters.append("MACD sem impulso comprador")

        if is_bearish_setup:
            if not trend_stack_sell:
                advisory_filters.append("Tendencia curta desalinhada")
            if not macd_impulse_sell:
                advisory_filters.append("MACD sem impulso vendedor")

        if (is_bullish_setup or is_bearish_setup) and body_ratio < self._min_body_ratio(interval):
            advisory_filters.append(f"Corpo fraco ({body_ratio:.0%})")

        if (is_bullish_setup or is_bearish_setup) and body_atr_ratio < self._min_body_atr_ratio(interval):
            advisory_filters.append("Corpo pequeno vs ATR")

        # Minimum strong confirmations filter (confluence gate — advisory only)
        active_confirms = buy_strong_confirms if is_bullish_setup else sell_strong_confirms
        min_confirms_required = {"1m": 3, "5m": 2, "15m": 2, "30m": 2, "1h": 2}.get(interval, 2)
        if (is_bullish_setup or is_bearish_setup) and active_confirms < min_confirms_required:
            advisory_filters.append(f"Poucas confluencias ({active_confirms}/{min_confirms_required})")

        # Consecutive candle direction filter (advisory only)
        if is_bullish_setup and consec_bearish >= 3 and consec_bullish == 0:
            advisory_filters.append("Ultimas velas contra a direcao")
        elif is_bearish_setup and consec_bullish >= 3 and consec_bearish == 0:
            advisory_filters.append("Ultimas velas contra a direcao")

        # MACD impulse (advisory for all timeframes)
        if interval in ("1m", "5m") and is_bullish_setup and not macd_impulse_buy:
            advisory_filters.append("MACD sem impulso comprador")
        elif interval in ("1m", "5m") and is_bearish_setup and not macd_impulse_sell:
            advisory_filters.append("MACD sem impulso vendedor")

        # ADX minimum: hard filter only for 1m, advisory for others
        if interval == "1m" and (is_bullish_setup or is_bearish_setup) and adx_val < 20:
            hard_filters.append(f"ADX fraco no 1m ({adx_val:.0f})")
        elif interval == "5m" and (is_bullish_setup or is_bearish_setup) and adx_val < 15:
            advisory_filters.append(f"ADX fraco ({adx_val:.0f})")

        if interval == "1m" and (is_bullish_setup or is_bearish_setup):
            if htf_df is not None and mtf_result != "Agree":
                hard_filters.append("Sem confirmacao MTF no 1m")

        if interval == "5m" and (is_bullish_setup or is_bearish_setup) and mtf_result == "Disagree":
            hard_filters.append("MTF contrario")

        if market_regime == "LATERAL" and interval in ("1m", "5m") and (is_bullish_setup or is_bearish_setup):
            hard_filters.append("Mercado lateral")

        if adaptive_filtering and context_policy_state == "blocked" and context_resolved_trades > 0:
            hard_filters.append("Bloqueado pelo historico desse par/timeframe")
        elif adaptive_filtering and context_policy_state == "caution" and interval == "1m" and context_resolved_trades > 0:
            advisory_filters.append("Historico pede cautela no 1m")

        blocked = len(hard_filters) > 0

        adjusted_min_score = session_min_score
        if market_regime == "LATERAL":
            adjusted_min_score += 10
        elif market_regime == "TENDENCIA":
            adjusted_min_score -= 5

        adjusted_min_score = max(adjusted_min_score, self._min_score_floor(interval))
        if mtf_result == "Agree":
            adjusted_min_score = max(self._min_score_floor(interval) - 2, adjusted_min_score - 2)
        adjusted_min_score += threshold_adjustment
        adjusted_min_score = max(self._min_score_floor(interval), min(90.0, adjusted_min_score))

        action = "AGUARDAR"
        strength = "NEUTRO"
        final_score = 0.0
        final_filters = ["Sem setup valido"]

        if is_bullish_setup:
            if not blocked and buy_score >= adjusted_min_score:
                action = "COMPRA"
                strength = "FORTE" if buy_score >= self._strong_score_threshold(interval) else "MODERADO"
                final_score = buy_score
                final_filters = ["Todos os filtros OK"] if not advisory_filters else ["Entrada liberada com cautela", *advisory_filters[:2]]
            else:
                action = "EVITA"
                strength = "FRACO"
                final_score = buy_score
                final_filters = [*hard_filters, *advisory_filters] if blocked else [f"Score baixo ({buy_score:.0f}%<{adjusted_min_score:.0f}%)", *advisory_filters]
                final_filters.insert(0, "FRACO - NAO ENTRE")

        elif is_bearish_setup:
            if not blocked and sell_score >= adjusted_min_score:
                action = "VENDA"
                strength = "FORTE" if sell_score >= self._strong_score_threshold(interval) else "MODERADO"
                final_score = sell_score
                final_filters = ["Todos os filtros OK"] if not advisory_filters else ["Entrada liberada com cautela", *advisory_filters[:2]]
            else:
                action = "EVITA"
                strength = "FRACO"
                final_score = sell_score
                final_filters = [*hard_filters, *advisory_filters] if blocked else [f"Score baixo ({sell_score:.0f}%<{adjusted_min_score:.0f}%)", *advisory_filters]
                final_filters.insert(0, "FRACO - NAO ENTRE")

        if adaptive_filtering and context_notes:
            if action in ("COMPRA", "VENDA"):
                if context_policy_state == "boost":
                    final_filters.append(context_notes[0])
            else:
                for note in context_notes[:2]:
                    if note not in final_filters:
                        final_filters.append(note)

        if action in ("COMPRA", "VENDA") and atr_val > 0:
            sl_mult = StrategyConfig.STOP_LOSS_ATR_MULTIPLIER
            tp_mult = StrategyConfig.TAKE_PROFIT_ATR_MULTIPLIER
            if action == "COMPRA":
                sl_price = price - sl_mult * atr_val
                tp_price = price + tp_mult * atr_val
            else:
                sl_price = price + sl_mult * atr_val
                tp_price = price - tp_mult * atr_val
        else:
            sl_price = None
            tp_price = None

        history_score = 50.0
        if adaptive_filtering and context_resolved_trades > 0:
            sample_factor = min(1.0, context_resolved_trades / max(6.0, float(self.config.get("min_resolved_trades", 5))))
            history_score = (
                (context_win_rate * 55.0)
                + (min(context_profit_factor, 2.0) / 2.0 * 20.0)
                + (context_recent_win_rate * 25.0)  # Recent results weighted more
            )
            history_score = (history_score * sample_factor) + (50.0 * (1.0 - sample_factor))

        # Dynamic history weight: more resolved trades = more trust in history
        history_weight = 0.20  # base: 20%
        if adaptive_filtering and context_resolved_trades >= 5:
            history_weight = min(0.40, 0.20 + (context_resolved_trades - 5) * 0.02)
        technical_weight = 1.0 - history_weight

        confidence_score = max(
            0.0,
            min(100.0, (final_score * technical_weight) + (history_score * history_weight) + confidence_boost),
        )
        if action not in ("COMPRA", "VENDA"):
            confidence_score = min(confidence_score, max(final_score, history_score * 0.75))

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
            source="OB CASH 3.0 V2",
            confidence_score=round(confidence_score, 1),
            confidence_label=self._confidence_label(confidence_score),
            policy_state=context_policy_state,
            policy_notes=context_notes,
            note=self._build_signal_note(
                action=action,
                strength=strength,
                market_regime=market_regime,
                mtf_result=mtf_result,
                advisory_filters=advisory_filters,
                hard_filters=hard_filters,
            ),
            conditions_buy=[
                sma3_val > sma50_val,
                price_confirmed >= ema100_val,
                momentum_val > momentum_prev,
                sma3_o_val > sma50_o_val,
            ],
            conditions_sell=[
                sma3_val < sma50_val,
                price_confirmed <= ema100_val,
                momentum_val < momentum_prev,
                sma3_o_val < sma50_o_val,
            ],
            filters=final_filters,
        )

        signal.raw_data = {
            "close_series": c.tolist()[-100:],
            "ema21_series": ema21.tolist()[-100:],
            "ema100_series": ema100.tolist()[-100:],
            "prev_close": self._clean_number(get_latest_value(c, offset=2), price),
            "volume_ratio": vol_ratio,
            "body_ratio": body_ratio,
            "body_atr_ratio": body_atr_ratio,
            "ema21_slope": ema21_slope,
            "historical_win_rate": round(context_win_rate, 4),
            "historical_profit_factor": round(context_profit_factor, 4),
            "resolved_trades": context_resolved_trades,
            "policy_state": context_policy_state,
            "recent_win_rate": round(context_recent_win_rate, 4),
            "strong_confirmations": active_confirms,
            "has_reliable_volume": has_reliable_volume,
            "consec_bullish": consec_bullish,
            "consec_bearish": consec_bearish,
            "history_weight": round(history_weight, 2),
        }

        logger.debug("Signal V2: %s %s %s score=%.1f", pair_name, interval, action, final_score)

        self._last_hash[cache_key] = data_hash
        self._last_signal[cache_key] = signal
        return signal

    def _check_mtf_structure(self, htf_df: pd.DataFrame) -> str:
        """Enhanced MTF check using the latest confirmed candle."""
        c = htf_df["Close"]
        ema50 = c.ewm(span=50, adjust=False).mean()
        momentum = c - c.rolling(14).mean()

        current_close = self._clean_number(get_latest_value(c, offset=1), self._clean_number(get_latest_value(c), 0.0))
        current_ema = self._clean_number(get_latest_value(ema50, offset=1), current_close)
        current_momentum = self._clean_number(get_latest_value(momentum, offset=1), 0.0)
        prev_momentum = self._clean_number(get_latest_value(momentum, offset=2), current_momentum)

        if current_close > current_ema and current_momentum > prev_momentum:
            return "Bullish"
        if current_close < current_ema and current_momentum < prev_momentum:
            return "Bearish"
        return "Neutral"

    def _error_signal(self, pair_name: str, interval: str, error_msg: str) -> Signal:
        """Create an error signal."""
        return Signal(
            asset=pair_name,
            interval=interval,
            timestamp=now_br(),
            action="ERRO",
            strength="NEUTRO",
            score=0.0,
            price=0.0,
            filters=[error_msg],
            raw_data={"error": error_msg},
        )

    def _min_score_floor(self, interval: str) -> float:
        return self.MIN_SCORE_BY_INTERVAL.get(interval, 50.0)

    def _strong_score_threshold(self, interval: str) -> float:
        return self.STRONG_SCORE_BY_INTERVAL.get(interval, 75.0)

    def _min_body_ratio(self, interval: str) -> float:
        return self.MIN_BODY_RATIO_BY_INTERVAL.get(interval, 0.30)

    def _min_body_atr_ratio(self, interval: str) -> float:
        return self.MIN_BODY_ATR_RATIO_BY_INTERVAL.get(interval, 0.08)

    def _confidence_label(self, confidence_score: float) -> str:
        if confidence_score >= 78:
            return "Alta confianca"
        if confidence_score >= 68:
            return "Media confianca"
        if confidence_score >= 58:
            return "Baixa confianca"
        return "Evitar"

    def _build_signal_note(
        self,
        action: str,
        strength: str,
        market_regime: str,
        mtf_result: str,
        advisory_filters: List[str],
        hard_filters: List[str],
    ) -> str:
        if action not in ("COMPRA", "VENDA"):
            if hard_filters:
                return hard_filters[0]
            return "Aguardar confirmacao"

        if market_regime == "LATERAL":
            return "Mercado lateral, risco moderado"
        if mtf_result == "Agree" and strength == "FORTE":
            return "Tendencia favoravel com confirmacao"
        if advisory_filters:
            return advisory_filters[0]
        if strength == "FORTE":
            return "Entrada limpa com contexto favoravel"
        return "Sinal moderado, acompanhar contexto"

    def _clean_number(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None or pd.isna(value):
                return float(default)
            return float(value)
        except Exception:
            return float(default)
