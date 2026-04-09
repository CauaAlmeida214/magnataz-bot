"""
Simple backtesting engine for OB CASH strategies.

Tests signal generation against historical data and calculates performance metrics.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from dataclasses import dataclass

from obcash3.data.fetcher import DataFetcher
from obcash3.signals.engine import SignalEngine
from obcash3.signals.engine_v2 import SignalEngineV2


@dataclass
class Trade:
    """Represents a single trade."""
    entry_time: datetime
    exit_time: datetime
    pair: str
    direction: str  # "BUY" or "SELL"
    entry_price: float
    exit_price: float
    sl: float
    tp: float
    pnl: float
    pnl_pct: float
    signal_score: float
    exit_reason: str  # "TP", "SL", "TIME_EXIT"


class Backtester:
    """Simple backtesting engine."""

    def __init__(
        self,
        initial_balance: float = 10000.0,
        risk_pct: float = 1.0,
        sl_atr_mult: float = 2.0,
        tp_atr_mult: float = 3.0,
        max_hold_periods: int = 20  # Max 20 bars
    ):
        self.initial_balance = initial_balance
        self.risk_pct = risk_pct
        self.sl_atr_mult = sl_atr_mult
        self.tp_atr_mult = tp_atr_mult
        self.max_hold_periods = max_hold_periods

    def run(
        self,
        df: pd.DataFrame,
        engine,
        pair_name: str,
        interval: str
    ) -> Tuple[List[Trade], Dict]:
        """Run backtest on historical data."""
        min_required = 100 + self.max_hold_periods + 10
        if len(df) < min_required:
            import logging
            logging.getLogger(__name__).warning(
                "Backtest: apenas %d candles, mínimo necessário é %d", len(df), min_required
            )
            return [], {"error": f"Dados insuficientes: {len(df)} candles (mínimo: {min_required})"}

        trades = []
        balance = self.initial_balance

        start_idx = 100

        for i in range(start_idx, len(df) - self.max_hold_periods - 5):
            current_df = df.iloc[:i+1].copy()
            signal = engine.generate_signal(current_df, pair_name, interval, htf_df=None)

            if signal.action not in ("COMPRA", "VENDA"):
                continue

            if trades and trades[-1].exit_time is None:
                continue

            entry_price = signal.price
            direction = "BUY" if signal.action == "COMPRA" else "SELL"
            atr = signal.atr

            if atr <= 0:
                continue

            if direction == "BUY":
                sl_price = entry_price - self.sl_atr_mult * atr
                tp_price = entry_price + self.tp_atr_mult * atr
            else:
                sl_price = entry_price + self.sl_atr_mult * atr
                tp_price = entry_price - self.tp_atr_mult * atr

            risk_amount = balance * (self.risk_pct / 100)
            stop_distance = abs(entry_price - sl_price)
            position_units = risk_amount / stop_distance if stop_distance > 0 else 0

            exit_idx = None
            exit_price = None
            exit_reason = None

            for j in range(i+1, min(i+self.max_hold_periods+1, len(df))):
                future_low = df['Low'].iloc[j]
                future_high = df['High'].iloc[j]

                if direction == "BUY":
                    if future_low <= sl_price:
                        exit_idx = j
                        exit_price = sl_price
                        exit_reason = "SL"
                        break
                    if future_high >= tp_price:
                        exit_idx = j
                        exit_price = tp_price
                        exit_reason = "TP"
                        break
                else:
                    if future_high >= sl_price:
                        exit_idx = j
                        exit_price = sl_price
                        exit_reason = "SL"
                        break
                    if future_low <= tp_price:
                        exit_idx = j
                        exit_price = tp_price
                        exit_reason = "TP"
                        break

            if exit_idx is None:
                exit_idx = min(i + self.max_hold_periods, len(df)-1)
                exit_price = df['Close'].iloc[exit_idx]
                exit_reason = "TIME_EXIT"

            if direction == "BUY":
                pnl = (exit_price - entry_price) * position_units
                pnl_pct = ((exit_price / entry_price) - 1) * 100
            else:
                pnl = (entry_price - exit_price) * position_units
                pnl_pct = ((entry_price / exit_price) - 1) * 100

            balance += pnl

            trade = Trade(
                entry_time=df.index[i] if hasattr(df.index, '__len__') else datetime.now() + timedelta(minutes=i),
                exit_time=df.index[exit_idx] if hasattr(df.index, '__len__') else datetime.now() + timedelta(minutes=exit_idx),
                pair=pair_name,
                direction=direction,
                entry_price=entry_price,
                exit_price=exit_price,
                sl=sl_price,
                tp=tp_price,
                pnl=pnl,
                pnl_pct=pnl_pct,
                signal_score=signal.score,
                exit_reason=exit_reason
            )
            trades.append(trade)

        stats = self._calculate_stats(trades, balance)
        return trades, stats

    def _calculate_stats(self, trades: List[Trade], final_balance: float) -> Dict:
        """Calculate performance statistics."""
        if not trades:
            return {
                "total_trades": 0,
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "total_pnl": 0.0,
                "total_pnl_pct": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "max_drawdown": 0.0,
                "sharpe_ratio": 0.0,
                "expectancy": 0.0
            }

        pnls = [t.pnl for t in trades]
        winners = [p for p in pnls if p > 0]
        losers = [p for p in pnls if p < 0]

        total_pnl = sum(pnls)
        gross_profit = sum(winners) if winners else 0
        gross_loss = abs(sum(losers)) if losers else 0

        win_rate = len(winners) / len(trades) if trades else 0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
        avg_win = np.mean(winners) if winners else 0
        avg_loss = np.mean(losers) if losers else 0
        expectancy = (win_rate * avg_win) - ((1-win_rate) * avg_loss) if avg_loss != 0 else 0

        equity_curve = [self.initial_balance]
        for trade in trades:
            equity_curve.append(equity_curve[-1] + trade.pnl)

        running_max = np.maximum.accumulate(equity_curve)
        drawdowns = (running_max - equity_curve) / running_max
        max_drawdown = np.max(drawdowns) if len(drawdowns) > 0 else 0

        returns = np.diff(equity_curve) / equity_curve[:-1]
        if len(returns) > 0 and np.std(returns) > 0:
            sharpe = np.mean(returns) / np.std(returns) * np.sqrt(252*24)
        else:
            sharpe = 0.0

        return {
            "total_trades": len(trades),
            "win_rate": round(win_rate * 100, 1),
            "profit_factor": round(profit_factor, 2) if profit_factor != float('inf') else 999,
            "total_pnl": round(total_pnl, 2),
            "total_pnl_pct": round((final_balance / self.initial_balance - 1) * 100, 1),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "max_drawdown": round(max_drawdown * 100, 1),
            "sharpe_ratio": round(sharpe, 2),
            "expectancy": round(expectancy, 2)
        }


def compare_strategies(
    pair: str = "EUR/USD",
    interval: str = "1h",
    days: int = 90
) -> Dict:
    """
    Compare V1 (original) vs V2 (enhanced) strategies.
    """
    print("="*60)
    print(f"BACKTEST: {pair} {interval}")
    print(f"Period: last {days} days")
    print("="*60)

    fetcher = DataFetcher()
    df, source = fetcher.fetch_data(pair, interval, "", "", use_cache=False)

    if df is None or len(df) < 200:
        print(f"Failed to fetch data: {source}")
        return {"error": source}

    print(f"Got {len(df)} candles from {source}")

    backtester = Backtester(
        initial_balance=10000,
        risk_pct=1.0,
        sl_atr_mult=2.0,
        tp_atr_mult=3.0,
        max_hold_periods=20
    )

    print("\nTesting V1 (original)...")
    engine_v1 = SignalEngine({
        "filter_hours": False,
        "mtf_confirm": False,
        "divergence_detect": False
    })
    trades_v1, stats_v1 = backtester.run(df, engine_v1, pair, interval)

    print("Testing V2 (enhanced)...")
    engine_v2 = SignalEngineV2({
        "filter_hours": False,
        "mtf_confirm": False,
        "divergence_detect": False
    })
    trades_v2, stats_v2 = backtester.run(df, engine_v2, pair, interval)

    print("\n" + "="*60)
    print("RESULTS COMPARISON")
    print("="*60)

    print("\nV1 (Original):")
    for k, v in stats_v1.items():
        print(f"  {k}: {v}")

    print("\nV2 (Enhanced):")
    for k, v in stats_v2.items():
        print(f"  {k}: {v}")

    print("\nIMPROVEMENT:")
    if stats_v1['total_trades'] > 0 and stats_v2['total_trades'] > 0:
        wr_improvement = stats_v2['win_rate'] - stats_v1['win_rate']
        pf_improvement = stats_v2['profit_factor'] - stats_v1['profit_factor']
        pnl_improvement = stats_v2['total_pnl'] - stats_v1['total_pnl']

        print(f"  Win Rate: {wr_improvement:+.1f}%")
        print(f"  Profit Factor: {pf_improvement:+.2f}")
        print(f"  Total P&L: ${pnl_improvement:+,.2f}")

        if stats_v2['win_rate'] > stats_v1['win_rate'] and stats_v2['profit_factor'] > stats_v1['profit_factor']:
            print("\n[SUCCESS] V2 is better! Consider upgrading.")
        else:
            print("\n[WARNING] Mixed results - further tuning needed.")

    return {
        "v1": stats_v1,
        "v2": stats_v2,
        "pair": pair,
        "interval": interval,
        "candles": len(df)
    }


if __name__ == "__main__":
    result = compare_strategies("EUR/USD", "1h", 90)
    print("\nDone.")
