from __future__ import absolute_import
"""
FREE-window result resolution.

Results are updated from market data and persisted both in SQLite and in the
shared CSV history used by dashboard/ML, without sending Telegram messages.
"""

from datetime import timedelta
from typing import Any, Optional

import pandas as pd

from obcash3.bot.signal_store import RESULT_LOSS, RESULT_PENDING, RESULT_WIN, WindowSignalStore
from obcash3.data.signal_store import update_signal_result as update_shared_history_result
from obcash3.utils.logger import get_logger
from obcash3.utils.time import BRT_TZ, now_br

logger = get_logger(__name__)

INTERVAL_MINUTES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
}


class FreeResultsEngine:
    """Resolves pending FREE signals after their candle closes."""

    def __init__(self, store: WindowSignalStore, fetcher: Any, config_supplier, history_store=None):
        self.store = store
        self.fetcher = fetcher
        self.config_supplier = config_supplier
        self.history_store = history_store

    async def process_pending_results(self) -> int:
        """Resolve all due pending FREE signals."""
        due_signals = self.store.get_pending_signals_due(as_of_iso=now_br().isoformat())
        if not due_signals:
            return 0

        resolved_count = 0
        for signal in due_signals:
            if not self._signal_due(signal):
                continue
            resolved = self._resolve_signal(signal)
            if resolved:
                resolved_count += 1
        if resolved_count > 0:
            logger.info("FREE results engine resolved %d signals", resolved_count)
        return resolved_count

    def _signal_due(self, signal_row: dict[str, Any]) -> bool:
        interval = str(signal_row.get("interval", "1m") or "1m")
        interval_minutes = INTERVAL_MINUTES.get(interval, 1)
        try:
            entry_timestamp = pd.Timestamp(str(signal_row.get("entry_timestamp", "")))
        except Exception:
            return False
        if entry_timestamp.tzinfo is None:
            entry_timestamp = entry_timestamp.tz_localize(BRT_TZ)
        else:
            entry_timestamp = entry_timestamp.tz_convert(BRT_TZ)
        return (entry_timestamp + timedelta(minutes=interval_minutes)) <= pd.Timestamp(now_br())

    def _resolve_signal(self, signal_row: dict[str, Any]) -> bool:
        asset = str(signal_row.get("asset", "") or "")
        interval = str(signal_row.get("interval", "1m") or "1m")
        config = self.config_supplier()
        df, source = self.fetcher.fetch_data(
            asset,
            interval,
            getattr(config, "twelve_api_key", ""),
            getattr(config, "av_api_key", ""),
            use_cache=False,
        )
        if df is None or df.empty or "Timestamp" not in df.columns:
            logger.warning("FREE result resolution failed for %s %s: %s", asset, interval, source)
            return False

        outcome = self._evaluate_result(signal_row, df, source)
        if outcome is None:
            return False

        updated = self.store.update_signal_result(
            signal_id=str(signal_row["id"]),
            result=outcome["result"],
            profit_estimate=outcome["profit_estimate"],
        )
        if not updated:
            return False

        history_signal_id = str(signal_row.get("history_signal_id", "") or "").strip()
        if history_signal_id:
            update_shared_history_result(
                history_signal_id,
                outcome["result"],
                note=f"FREE window auto result ({outcome['source']})",
                store=self.history_store,
            )

        logger.info(
            "FREE signal resolved: %s -> %s final_price=%.5f source=%s",
            signal_row["id"],
            outcome["result"],
            outcome["final_price"],
            outcome["source"],
        )
        return True

    def _evaluate_result(self, signal_row: dict[str, Any], market_df: pd.DataFrame, source: str) -> Optional[dict[str, Any]]:
        market = market_df.copy()
        market["Timestamp"] = pd.to_datetime(market["Timestamp"], utc=True, errors="coerce")
        market = market.dropna(subset=["Timestamp"]).reset_index(drop=True)
        if market.empty:
            return None

        entry_timestamp = pd.Timestamp(str(signal_row.get("entry_timestamp", "")))
        if entry_timestamp.tzinfo is None:
            entry_timestamp = entry_timestamp.tz_localize(BRT_TZ)
        else:
            entry_timestamp = entry_timestamp.tz_convert(BRT_TZ)

        interval = str(signal_row.get("interval", "1m") or "1m")
        interval_minutes = INTERVAL_MINUTES.get(interval, 1)
        exit_timestamp = entry_timestamp + timedelta(minutes=interval_minutes)

        market["Timestamp"] = market["Timestamp"].dt.tz_convert(BRT_TZ)
        candle = market[
            (market["Timestamp"] >= entry_timestamp)
            & (market["Timestamp"] < exit_timestamp)
        ].copy()
        if candle.empty:
            return None

        final_price = float(pd.to_numeric(candle["Close"], errors="coerce").dropna().iloc[-1])
        entry_price = float(signal_row.get("price", 0.0) or 0.0)
        if entry_price <= 0:
            return None

        action = str(signal_row.get("action", "") or "").upper()
        if action == "COMPRA":
            result = RESULT_WIN if final_price > entry_price else RESULT_LOSS
        else:
            result = RESULT_WIN if final_price < entry_price else RESULT_LOSS

        stake_estimate = float(signal_row.get("stake_estimate", 0.0) or 0.0)
        if stake_estimate <= 0:
            stake_estimate = self._stake_estimate()
        payout_ratio = self._payout_ratio()
        profit_estimate = round(stake_estimate * payout_ratio, 2) if result == RESULT_WIN else round(-stake_estimate, 2)

        return {
            "result": result,
            "profit_estimate": profit_estimate,
            "final_price": final_price,
            "source": str(source or signal_row.get("source", "") or "market_data"),
        }

    def _stake_estimate(self) -> float:
        config = self.config_supplier()
        balance = float(getattr(config, "account_balance", 1000.0) or 1000.0)
        risk_pct = float(getattr(config, "risk_pct", 1.0) or 1.0)
        return round(balance * (risk_pct / 100.0), 2)

    def _payout_ratio(self) -> float:
        return 0.82
