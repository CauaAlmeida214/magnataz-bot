from __future__ import absolute_import
"""
Shared signal persistence helpers backed by the single history store.

This module keeps History, Dashboard and ML attached to the same persisted
dataset instead of creating parallel CSV files.
"""

from typing import Any, Dict, Optional

import pandas as pd

from obcash3.utils.history import market_data_coverage_start
from obcash3.utils.history import (
    RESULT_LOSS,
    RESULT_WIN,
    SignalHistoryStore,
    ensure_history_schema,
)
from obcash3.utils.logger import get_logger
from obcash3.utils.time import now_br

logger = get_logger(__name__)

INTERVAL_MINUTES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
}


def _get_store(store: Optional[SignalHistoryStore] = None) -> SignalHistoryStore:
    return store or SignalHistoryStore()


def save_signal_record(signal: Any, store: Optional[SignalHistoryStore] = None) -> bool:
    """Persist a signal record in the shared history store."""
    saved = _get_store(store).append(signal)
    if saved:
        logger.info(
            "Signal persisted for ML/history: %s %s %s",
            getattr(signal, "asset", "-"),
            getattr(signal, "interval", "-"),
            getattr(signal, "action", "-"),
        )
    return saved


def update_signal_result(
    signal_id: str,
    result: str,
    note: str = "",
    store: Optional[SignalHistoryStore] = None,
) -> Optional[Dict[str, Any]]:
    """Update a persisted signal result by stable signal_id."""
    updated = _get_store(store).update_signal_result_manual(signal_id, result, note)
    if updated:
        logger.info("Signal result updated: %s -> %s", signal_id, result)
    return updated


def resolve_signal_results(
    asset: str,
    interval: str,
    market_df: pd.DataFrame,
    store: Optional[SignalHistoryStore] = None,
) -> int:
    """Resolve pending signals for the same asset/timeframe using fresh market data."""
    resolved = _get_store(store).resolve_market_data(asset, interval, market_df)
    if resolved > 0:
        logger.info("Resolved %d pending signals for %s %s", resolved, asset, interval)
    return resolved


def list_due_pending_groups(
    store: Optional[SignalHistoryStore] = None,
    as_of: Optional[pd.Timestamp] = None,
) -> list[tuple[str, str]]:
    """Return pending groups already due for candle-close resolution."""
    history_store = _get_store(store)
    current_time = pd.Timestamp(as_of or now_br())
    due_groups: list[tuple[str, str]] = []

    for asset, interval in history_store.list_pending_groups():
        oldest_entry = history_store.oldest_open_entry_time(asset, interval)
        if oldest_entry is None:
            continue
        due_time = oldest_entry + pd.Timedelta(minutes=INTERVAL_MINUTES.get(str(interval), 5))
        if due_time <= current_time:
            due_groups.append((asset, interval))
    return due_groups


def resolve_signal_results_with_fallback(
    asset: str,
    interval: str,
    market_df: pd.DataFrame,
    source: str,
    fetcher: Any,
    store: Optional[SignalHistoryStore] = None,
) -> int:
    """Resolve pending signals and fallback to deeper Yahoo coverage when needed."""
    history_store = _get_store(store)
    resolved = resolve_signal_results(asset, interval, market_df, store=history_store)
    oldest_open = history_store.oldest_open_entry_time(asset, interval)
    coverage_start = market_data_coverage_start(market_df)
    if oldest_open is None or coverage_start is None:
        return resolved
    if coverage_start <= oldest_open:
        return resolved
    if str(source or "").startswith("Yahoo Finance"):
        return resolved

    yahoo_df, yahoo_source = fetcher.fetch_from_yahoo(asset, interval)
    if yahoo_df is None:
        logger.debug(
            "Yahoo fallback unavailable for pending resolution of %s %s: %s",
            asset,
            interval,
            yahoo_source,
        )
        return resolved

    extra_resolved = resolve_signal_results(asset, interval, yahoo_df, store=history_store)
    if extra_resolved > 0:
        logger.info(
            "Resolved %d stale pending signals for %s %s using deeper Yahoo fallback",
            extra_resolved,
            asset,
            interval,
        )
    return resolved + extra_resolved


def refresh_pending_signal_results(
    fetcher: Any,
    config: Any,
    store: Optional[SignalHistoryStore] = None,
    limit: Optional[int] = None,
) -> int:
    """Fetch fresh data for due pending groups and resolve their WIN/LOSS."""
    history_store = _get_store(store)
    due_groups = list_due_pending_groups(store=history_store)
    if limit is not None:
        due_groups = due_groups[: int(limit)]
    if not due_groups:
        return 0

    total_resolved = 0
    for asset, interval in due_groups:
        df, source = fetcher.fetch_data(
            asset,
            interval,
            getattr(config, "twelve_api_key", ""),
            getattr(config, "av_api_key", ""),
            use_cache=False,
        )
        if df is None:
            logger.warning("Pending result refresh failed for %s %s: %s", asset, interval, source)
            continue
        total_resolved += resolve_signal_results_with_fallback(
            asset,
            interval,
            df,
            source,
            fetcher=fetcher,
            store=history_store,
        )

    if total_resolved > 0:
        logger.info("Pending result refresh resolved %d signals", total_resolved)
    return total_resolved


def load_signal_history(store: Optional[SignalHistoryStore] = None) -> pd.DataFrame:
    """Load the raw persisted signal history."""
    return _get_store(store).load_dataframe()


def get_ml_ready_history(store: Optional[SignalHistoryStore] = None) -> pd.DataFrame:
    """Return resolved signals normalized for ML preparation."""
    history = ensure_history_schema(load_signal_history(store))
    entries = history[
        history["action"].isin(["COMPRA", "VENDA"])
        & history["result_status"].isin([RESULT_WIN, RESULT_LOSS])
    ].copy()
    if entries.empty:
        return entries

    timestamp_series = pd.to_datetime(entries["timestamp"], errors="coerce")
    time_series = entries["time"].astype(str)
    hour_series = pd.to_numeric(entries["hour"], errors="coerce") if "hour" in entries.columns else pd.Series(index=entries.index, dtype=float)
    if "timeframe" in entries.columns:
        timeframe_series = entries["timeframe"]
    else:
        timeframe_series = entries["interval"]

    entries["hour"] = hour_series.fillna(
        timestamp_series.dt.hour.fillna(
            pd.to_numeric(time_series.str.slice(0, 2), errors="coerce")
        )
    ).fillna(0).astype(int)
    entries["timeframe"] = timeframe_series.astype(str)
    entries["score"] = pd.to_numeric(entries["technical_score"], errors="coerce").where(
        pd.to_numeric(entries["technical_score"], errors="coerce") > 0,
        pd.to_numeric(entries["score"], errors="coerce"),
    ).fillna(0.0)
    entries["result_label"] = (entries["result_status"] == RESULT_WIN).astype(int)
    entries["source"] = entries["source"].fillna("").astype(str)

    numeric_columns = [
        "score",
        "rsi",
        "adx",
        "stoch",
        "bb_width",
        "price",
        "sl",
        "tp",
    ]
    for column in numeric_columns:
        entries[column] = pd.to_numeric(entries[column], errors="coerce").fillna(0.0)

    return entries.reset_index(drop=True)
