from __future__ import absolute_import
"""
Persistent signal history helpers with lightweight performance learning.
"""

import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from obcash3.config.settings import BRT, HISTORY_PATH
from obcash3.utils.logger import get_logger
from obcash3.utils.time import format_date_brazil, format_time_brazil, next_candle_start

logger = get_logger(__name__)


FIELDNAMES = [
    "signal_id",
    "timestamp",
    "date",
    "time",
    "hour",
    "entry_time",
    "asset",
    "interval",
    "timeframe",
    "action",
    "strength",
    "score",
    "confidence_score",
    "confidence_label",
    "policy_state",
    "policy_notes",
    "technical_score",
    "ml_score",
    "ml_confidence",
    "ml_backend",
    "ml_used",
    "decision_score",
    "selection_reason",
    "historical_win_rate",
    "historical_profit_factor",
    "resolved_trades",
    "price",
    "sl",
    "tp",
    "atr",
    "rsi",
    "adx",
    "macd_hist",
    "stoch",
    "bb_width",
    "market_regime",
    "session",
    "mtf_confirmation",
    "divergence",
    "source",
    "filters",
    "note",
    "result_status",
    "result",
    "result_reason",
    "resolved_at",
    "manual_updated_at",
    "final_price",
    "bars_monitored",
    "rr_ratio",
]

FLOAT_COLUMNS = {
    "score",
    "confidence_score",
    "technical_score",
    "ml_score",
    "ml_confidence",
    "decision_score",
    "historical_win_rate",
    "historical_profit_factor",
    "price",
    "sl",
    "tp",
    "atr",
    "rsi",
    "adx",
    "macd_hist",
    "stoch",
    "bb_width",
    "final_price",
    "rr_ratio",
}

INT_COLUMNS = {"hour", "resolved_trades", "bars_monitored"}

RESULT_WIN = "WIN"
RESULT_LOSS = "LOSS"
RESULT_TIMEOUT = "TIMEOUT"
RESULT_DRAW = "DRAW"
RESULT_OPEN = "OPEN"
RESULT_PENDING = "PENDENTE"
ENTRY_ACTIONS = {"COMPRA", "VENDA"}
HISTORY_TABLE_COLUMNS = ["asset", "date", "time", "action", "score", "result"]

MAX_MONITOR_BARS = {
    "1m": 6,
    "5m": 8,
    "15m": 10,
    "30m": 12,
    "1h": 14,
}


@dataclass
class PerformanceProfile:
    asset: str
    interval: str
    session: str = ""
    resolved_trades: int = 0
    wins: int = 0
    losses: int = 0
    timeouts: int = 0
    draws: int = 0
    win_rate: float = 0.50
    recent_win_rate: float = 0.50
    profit_factor: float = 1.00
    policy_state: str = "learning"
    threshold_adjustment: float = 0.0
    confidence_boost: float = 0.0
    notes: List[str] = field(default_factory=list)
    scope: str = "pair_interval"

    def to_context(self) -> Dict[str, Any]:
        return {
            "asset": self.asset,
            "interval": self.interval,
            "session": self.session,
            "resolved_trades": self.resolved_trades,
            "wins": self.wins,
            "losses": self.losses,
            "timeouts": self.timeouts,
            "draws": self.draws,
            "win_rate": self.win_rate,
            "recent_win_rate": self.recent_win_rate,
            "profit_factor": self.profit_factor,
            "policy_state": self.policy_state,
            "threshold_adjustment": self.threshold_adjustment,
            "confidence_boost": self.confidence_boost,
            "notes": list(self.notes),
            "scope": self.scope,
        }


def _get_value(signal: Any, name: str, default: Any = "") -> Any:
    if isinstance(signal, dict):
        return signal.get(name, default)
    return getattr(signal, name, default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in ("", None):
            return int(default)
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _safe_timestamp(value: Any) -> pd.Timestamp | None:
    return _coerce_brt_timestamp(value)


def _coerce_brt_timestamp(value: Any) -> Optional[pd.Timestamp]:
    if value in ("", None):
        return None
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None
    if pd.isna(ts):
        return None

    try:
        if ts.tzinfo is None:
            return ts.tz_localize(BRT)
        return ts.tz_convert(BRT)
    except Exception:
        return None


def _coerce_brt_series(series: pd.Series) -> pd.Series:
    """Normalize a timestamp series into Brazil timezone."""
    parsed = pd.to_datetime(series, utc=True, errors="coerce")
    try:
        return parsed.dt.tz_convert(BRT)
    except Exception:
        return parsed


def market_data_coverage_start(market_df: pd.DataFrame) -> Optional[pd.Timestamp]:
    """Return the earliest valid candle timestamp in Brazil timezone."""
    if market_df is None or market_df.empty or "Timestamp" not in market_df.columns:
        return None
    normalized = _coerce_brt_series(market_df["Timestamp"]).dropna()
    if normalized.empty:
        return None
    return normalized.min()


def _format_signal_parts(timestamp_value: Any) -> tuple[str, str]:
    signal_ts = _coerce_brt_timestamp(timestamp_value)
    if signal_ts is None:
        return "", ""

    signal_dt = signal_ts.to_pydatetime()
    return format_date_brazil(signal_dt), format_time_brazil(signal_dt)


def _result_label(result_status: Any, action: Any = "") -> str:
    status = str(result_status or "").strip().upper()
    normalized_action = str(action or "").strip().upper()

    if status in {RESULT_WIN, RESULT_LOSS, RESULT_TIMEOUT, RESULT_DRAW}:
        return status
    if status in {"", RESULT_OPEN} and normalized_action in ENTRY_ACTIONS:
        return RESULT_PENDING
    return status


def _default_for_column(column: str) -> Any:
    if column in FLOAT_COLUMNS:
        return 0.0
    if column in INT_COLUMNS:
        return 0
    return ""


def _build_signal_id(
    timestamp: Any,
    asset: Any,
    interval: Any,
    action: Any,
    price: Any,
) -> str:
    return "|".join(
        [
            str(timestamp or ""),
            str(asset or ""),
            str(interval or ""),
            str(action or ""),
            f"{_safe_float(price, 0.0):.5f}",
        ]
    )


def ensure_history_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Add missing columns and normalize dtypes for backward compatibility."""
    normalized = df.copy()

    for column in FIELDNAMES:
        if column not in normalized.columns:
            normalized[column] = _default_for_column(column)

    normalized = normalized.loc[:, FIELDNAMES].copy()

    for column in FLOAT_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce").fillna(0.0)
    for column in INT_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce").fillna(0).astype(int)

    string_columns = [column for column in FIELDNAMES if column not in FLOAT_COLUMNS and column not in INT_COLUMNS]
    for column in string_columns:
        normalized[column] = normalized[column].fillna("").astype(str)

    signal_ids: List[str] = []
    date_values: List[str] = []
    time_values: List[str] = []
    hour_values: List[int] = []
    entry_time_values: List[str] = []
    timeframe_values: List[str] = []
    result_values: List[str] = []
    note_values: List[str] = []

    for signal_id, timestamp_value, existing_date, existing_time, existing_hour, existing_entry_time, asset, interval, existing_timeframe, action, price, filters, policy_notes, result_status, existing_note in zip(
        normalized["signal_id"],
        normalized["timestamp"],
        normalized["date"],
        normalized["time"],
        normalized["hour"],
        normalized["entry_time"],
        normalized["asset"],
        normalized["interval"],
        normalized["timeframe"],
        normalized["action"],
        normalized["price"],
        normalized["filters"],
        normalized["policy_notes"],
        normalized["result_status"],
        normalized["note"],
    ):
        derived_date, derived_time = _format_signal_parts(timestamp_value)
        signal_ts = _safe_timestamp(timestamp_value)
        entry_ts = _safe_timestamp(existing_entry_time)
        if entry_ts is None and str(action or "").strip().upper() in ENTRY_ACTIONS:
            if signal_ts is not None:
                try:
                    entry_ts = pd.Timestamp(next_candle_start(str(interval or ""), base_time=signal_ts.to_pydatetime()))
                except Exception:
                    entry_ts = None
        derived_note = str(existing_note or "").strip()
        if not derived_note:
            derived_note = str(filters or "").strip()
        if not derived_note:
            derived_note = str(policy_notes or "").strip()
        signal_ids.append(
            str(signal_id or "").strip()
            or _build_signal_id(timestamp_value, asset, interval, action, price)
        )
        date_values.append(derived_date or str(existing_date or ""))
        time_values.append(derived_time or str(existing_time or ""))
        if signal_ts is not None:
            hour_values.append(int(signal_ts.hour))
        else:
            hour_values.append(_safe_int(existing_hour, 0))
        entry_time_values.append(entry_ts.isoformat() if entry_ts is not None else str(existing_entry_time or ""))
        timeframe_values.append(str(existing_timeframe or interval or ""))
        result_values.append(_result_label(result_status, action))
        note_values.append(derived_note)

    normalized["signal_id"] = signal_ids
    normalized["date"] = date_values
    normalized["time"] = time_values
    normalized["hour"] = hour_values
    normalized["entry_time"] = entry_time_values
    normalized["timeframe"] = timeframe_values
    normalized["result"] = result_values
    normalized["note"] = note_values

    return normalized


def signal_to_row(signal: Any) -> Dict[str, Any]:
    """Convert a signal-like object to a CSV row."""
    filters = _get_value(signal, "filters", []) or []
    policy_notes = _get_value(signal, "policy_notes", []) or []
    signal_note = str(_get_value(signal, "note", "") or "").strip()
    timestamp = _get_value(signal, "timestamp")
    raw_data = _get_value(signal, "raw_data", {}) or {}
    action = _get_value(signal, "action")
    interval = _get_value(signal, "interval")
    signal_date = ""
    signal_time = ""
    signal_hour = 0
    if timestamp:
        signal_date, signal_time = _format_signal_parts(timestamp)
        signal_hour = int(pd.Timestamp(timestamp).hour)

    entry_time = ""
    result_status = ""
    if action in ENTRY_ACTIONS and timestamp:
        entry_time = next_candle_start(interval, base_time=timestamp).isoformat()
        result_status = RESULT_OPEN

    rr_ratio = 0.0
    price = _safe_float(_get_value(signal, "price", 0.0), 0.0)
    sl = _safe_float(_get_value(signal, "sl", 0.0), 0.0)
    tp = _safe_float(_get_value(signal, "tp", 0.0), 0.0)
    technical_score = _safe_float(_get_value(signal, "technical_score", 0.0), 0.0)
    if technical_score <= 0:
        technical_score = _safe_float(_get_value(signal, "score", 0.0), 0.0)
    decision_score = _safe_float(_get_value(signal, "decision_score", 0.0), 0.0)
    if decision_score <= 0:
        decision_score = technical_score
    risk = abs(price - sl)
    reward = abs(tp - price)
    if risk > 0:
        rr_ratio = reward / risk

    if not signal_note:
        signal_note = " | ".join(str(item) for item in filters[:2])

    signal_id = _build_signal_id(
        timestamp.isoformat() if timestamp else "",
        _get_value(signal, "asset"),
        interval,
        action,
        price,
    )

    return {
        "signal_id": signal_id,
        "timestamp": timestamp.isoformat() if timestamp else "",
        "date": signal_date,
        "time": signal_time,
        "hour": signal_hour,
        "entry_time": entry_time,
        "asset": _get_value(signal, "asset"),
        "interval": interval,
        "timeframe": interval,
        "action": action,
        "strength": _get_value(signal, "strength"),
        "score": _get_value(signal, "score", 0.0),
        "confidence_score": _get_value(signal, "confidence_score", 0.0),
        "confidence_label": _get_value(signal, "confidence_label", "OBSERVAR"),
        "policy_state": _get_value(signal, "policy_state", "learning"),
        "policy_notes": " | ".join(str(item) for item in policy_notes),
        "technical_score": technical_score,
        "ml_score": _get_value(signal, "ml_score", 0.0),
        "ml_confidence": _get_value(signal, "ml_confidence", 0.0),
        "ml_backend": _get_value(signal, "ml_backend", ""),
        "ml_used": "1" if bool(_get_value(signal, "ml_used", False)) else "0",
        "decision_score": decision_score,
        "selection_reason": _get_value(signal, "selection_reason", ""),
        "historical_win_rate": raw_data.get("historical_win_rate", 0.0),
        "historical_profit_factor": raw_data.get("historical_profit_factor", 1.0),
        "resolved_trades": raw_data.get("resolved_trades", 0),
        "price": price,
        "sl": sl if sl else "",
        "tp": tp if tp else "",
        "atr": _get_value(signal, "atr", 0.0),
        "rsi": _get_value(signal, "rsi", 0.0),
        "adx": _get_value(signal, "adx", 0.0),
        "macd_hist": _get_value(signal, "macd_hist", 0.0),
        "stoch": _get_value(signal, "stoch", 0.0),
        "bb_width": _get_value(signal, "bb_width", 0.0),
        "market_regime": _get_value(signal, "market_regime", ""),
        "session": _get_value(signal, "session", ""),
        "mtf_confirmation": _get_value(signal, "mtf_confirmation", ""),
        "divergence": _get_value(signal, "divergence", ""),
        "source": _get_value(signal, "source", ""),
        "filters": " | ".join(str(item) for item in filters),
        "note": signal_note,
        "result_status": result_status,
        "result": _result_label(result_status, action),
        "result_reason": "",
        "resolved_at": "",
        "manual_updated_at": "",
        "final_price": "",
        "bars_monitored": 0,
        "rr_ratio": rr_ratio,
    }


class SignalHistoryStore:
    """Thread-safe CSV history store plus profile analytics."""

    def __init__(self, path: Path = HISTORY_PATH):
        self.path = Path(path)
        self._lock = threading.Lock()

    def append(self, signal: Any) -> bool:
        """Append a signal to the history CSV."""
        row = signal_to_row(signal)

        try:
            with self._lock:
                df = self._load_dataframe_unlocked()
                if not df.empty and "signal_id" in df.columns:
                    df = df[df["signal_id"] != row["signal_id"]].copy()
                df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
                self._save_dataframe_unlocked(df)
            return True
        except Exception as exc:
            logger.error("Failed to append signal history: %s", exc, exc_info=True)
            return False

    def load_dataframe(self) -> pd.DataFrame:
        """Load the full history with schema migration applied."""
        with self._lock:
            return self._load_dataframe_unlocked()

    def load_signal_table(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Load display-ready signal records sorted from newest to oldest."""
        with self._lock:
            history = self._load_dataframe_unlocked()
        return build_history_table(history, limit=limit)

    def get_signal(self, signal_id: str) -> Optional[Dict[str, Any]]:
        """Return a single signal record by signal_id."""
        if not signal_id:
            return None
        with self._lock:
            history = self._load_dataframe_unlocked()
        rows = history[history["signal_id"] == str(signal_id)].copy()
        if rows.empty:
            return None
        row = rows.iloc[-1].to_dict()
        row["result"] = _result_label(row.get("result_status", ""), row.get("action", ""))
        return row

    def oldest_open_entry_time(self, asset: str, interval: str) -> Optional[pd.Timestamp]:
        """Return the oldest pending/open entry time for an asset/timeframe."""
        with self._lock:
            history = self._load_dataframe_unlocked()

        mask = (
            (history["asset"] == str(asset))
            & (history["interval"] == str(interval))
            & (history["action"].isin(["COMPRA", "VENDA"]))
            & (history["result_status"].isin(["", RESULT_OPEN]))
            & (history["entry_time"].astype(str) != "")
        )
        pending = history.loc[mask, "entry_time"]
        if pending.empty:
            return None

        normalized = pending.map(_safe_timestamp).dropna()
        if normalized.empty:
            return None
        return min(normalized)

    def list_pending_groups(self) -> List[tuple[str, str]]:
        """Return unique asset/timeframe pairs with unresolved entry signals."""
        with self._lock:
            history = self._load_dataframe_unlocked()

        mask = (
            history["action"].isin(["COMPRA", "VENDA"])
            & history["result_status"].isin(["", RESULT_OPEN])
        )
        pending = history.loc[mask, ["asset", "interval"]].drop_duplicates()
        return [
            (str(row.asset), str(row.interval))
            for row in pending.itertuples(index=False)
            if str(row.asset).strip() and str(row.interval).strip()
        ]

    def reopen_stale_auto_resolutions(self, grace_bars: int = 1) -> int:
        """
        Reopen suspicious auto-resolved trades.

        Old buggy builds could resolve a trade with a candle many periods after
        its intended close when the selected data source did not cover the entry.
        Those rows are reset to OPEN/PENDENTE so they can be resolved again with
        a better dataset or updated manually.
        """
        reopened = 0
        interval_minutes = {
            "1m": 1,
            "5m": 5,
            "15m": 15,
            "30m": 30,
            "1h": 60,
        }

        try:
            with self._lock:
                history = self._load_dataframe_unlocked()
                if history.empty:
                    return 0

                mask = (
                    history["action"].isin(["COMPRA", "VENDA"])
                    & history["result_status"].isin([RESULT_WIN, RESULT_LOSS, RESULT_TIMEOUT, RESULT_DRAW])
                    & (history["result_reason"] == "CANDLE_CLOSE")
                    & (history["resolved_at"].astype(str) != "")
                )
                if not mask.any():
                    return 0

                for row_index in history.index[mask]:
                    row = history.loc[row_index]
                    entry_time = _safe_timestamp(row["entry_time"])
                    resolved_at = _safe_timestamp(row["resolved_at"])
                    minutes = interval_minutes.get(str(row["interval"]), 5)
                    if entry_time is None or resolved_at is None:
                        continue

                    stale_deadline = entry_time + pd.Timedelta(minutes=minutes * (1 + max(1, grace_bars)))
                    if resolved_at <= stale_deadline:
                        continue

                    action = str(row["action"])
                    history.at[row_index, "result_status"] = RESULT_OPEN
                    history.at[row_index, "result"] = _result_label(RESULT_OPEN, action)
                    history.at[row_index, "result_reason"] = ""
                    history.at[row_index, "resolved_at"] = ""
                    history.at[row_index, "final_price"] = 0.0
                    history.at[row_index, "bars_monitored"] = 0
                    reopened += 1

                if reopened:
                    self._save_dataframe_unlocked(history)
            return reopened
        except Exception as exc:
            logger.error("Failed to reopen stale auto resolutions: %s", exc, exc_info=True)
            return 0

    def update_signal_result(
        self,
        asset: str,
        interval: str,
        signal_timestamp: Any,
        result_status: str,
        result_reason: str = "CLOSE",
        resolved_at: Any = "",
        final_price: Any = "",
        note: str = "",
    ) -> bool:
        """Update a specific signal row with its final result."""
        target_ts = str(signal_timestamp or "")
        if not target_ts:
            return False

        try:
            with self._lock:
                history = self._load_dataframe_unlocked()
                mask = (
                    (history["asset"] == str(asset))
                    & (history["interval"] == str(interval))
                    & (history["timestamp"] == target_ts)
                )
                if not mask.any():
                    return False

                row_index = history.index[mask][-1]
                action = history.at[row_index, "action"]
                history.at[row_index, "result_status"] = str(result_status).upper()
                history.at[row_index, "result"] = _result_label(result_status, action)
                history.at[row_index, "result_reason"] = str(result_reason or "")
                history.at[row_index, "resolved_at"] = str(resolved_at or "")
                history.at[row_index, "final_price"] = final_price
                if note:
                    history.at[row_index, "note"] = str(note).strip()
                if str(result_reason or "").upper() == "MANUAL":
                    history.at[row_index, "manual_updated_at"] = pd.Timestamp.now(tz=BRT).isoformat()
                self._save_dataframe_unlocked(history)
                return True
        except Exception as exc:
            logger.error("Failed to update signal result: %s", exc, exc_info=True)
            return False

    def update_signal_result_manual(
        self,
        signal_id: str,
        result_status: str,
        note: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Update a signal result manually using the stable signal_id."""
        if not signal_id:
            return None

        normalized_status = str(result_status or "").strip().upper()
        if normalized_status not in {RESULT_WIN, RESULT_LOSS, RESULT_PENDING}:
            return None

        try:
            with self._lock:
                history = self._load_dataframe_unlocked()
                mask = history["signal_id"] == str(signal_id)
                if not mask.any():
                    return None

                row_index = history.index[mask][-1]
                action = str(history.at[row_index, "action"])
                history.at[row_index, "result_status"] = normalized_status if normalized_status != RESULT_PENDING else RESULT_OPEN
                history.at[row_index, "result"] = _result_label(history.at[row_index, "result_status"], action)
                history.at[row_index, "result_reason"] = "MANUAL" if normalized_status != RESULT_PENDING else ""
                history.at[row_index, "resolved_at"] = pd.Timestamp.now(tz=BRT).isoformat() if normalized_status != RESULT_PENDING else ""
                history.at[row_index, "manual_updated_at"] = pd.Timestamp.now(tz=BRT).isoformat()
                history.at[row_index, "note"] = str(note or "").strip()
                if normalized_status == RESULT_PENDING:
                    history.at[row_index, "final_price"] = ""
                    history.at[row_index, "bars_monitored"] = 0
                self._save_dataframe_unlocked(history)
                return history.loc[row_index].to_dict()
        except Exception as exc:
            logger.error("Failed to manually update signal result: %s", exc, exc_info=True)
            return None

    def resolve_market_data(self, asset: str, interval: str, market_df: pd.DataFrame) -> int:
        """Resolve open signals using fresh market data for the same asset and timeframe."""
        if market_df is None or market_df.empty or "Timestamp" not in market_df.columns:
            return 0

        resolved = 0
        market = market_df.copy()
        market["Timestamp"] = _coerce_brt_series(market["Timestamp"])
        market = market.dropna(subset=["Timestamp"]).reset_index(drop=True)
        if market.empty:
            return 0

        try:
            with self._lock:
                history = self._load_dataframe_unlocked()
                mask = (
                    (history["asset"] == asset)
                    & (history["interval"] == interval)
                    & (history["action"].isin(["COMPRA", "VENDA"]))
                    & (history["result_status"].isin(["", RESULT_OPEN]))
                )
                if not mask.any():
                    return 0

                for row_index in history.index[mask]:
                    row = history.loc[row_index]
                    entry_time = _safe_timestamp(row["entry_time"])
                    if entry_time is None:
                        continue

                    interval_minutes = {
                        "1m": 1,
                        "5m": 5,
                        "15m": 15,
                        "30m": 30,
                        "1h": 60,
                    }.get(interval, 5)
                    exit_time = entry_time + pd.Timedelta(minutes=interval_minutes)
                    fallback_deadline = exit_time + pd.Timedelta(minutes=interval_minutes)

                    entry_slice = market[
                        (market["Timestamp"] >= entry_time)
                        & (market["Timestamp"] < exit_time)
                    ].copy()
                    if entry_slice.empty:
                        fallback_slice = market[market["Timestamp"] >= entry_time].head(1)
                        if fallback_slice.empty:
                            continue
                        fallback_time = fallback_slice["Timestamp"].iloc[0]
                        if fallback_time > fallback_deadline:
                            continue
                        entry_slice = fallback_slice

                    status = ""
                    reason = "CANDLE_CLOSE"
                    bars_monitored = len(entry_slice)
                    action = str(row["action"])
                    entry_price = _safe_float(row["price"], 0.0)
                    final_close = _safe_float(entry_slice["Close"].iloc[-1], 0.0)
                    candle_time = entry_slice["Timestamp"].iloc[-1]
                    if final_close <= 0 or entry_price <= 0:
                        continue

                    if action == "COMPRA":
                        status = RESULT_WIN if final_close > entry_price else RESULT_LOSS
                    else:
                        status = RESULT_WIN if final_close < entry_price else RESULT_LOSS

                    if status:
                        resolved_at = pd.Timestamp(candle_time).isoformat()
                        history.at[row_index, "result_status"] = status
                        history.at[row_index, "result"] = _result_label(status, action)
                        history.at[row_index, "result_reason"] = reason
                        history.at[row_index, "resolved_at"] = resolved_at
                        history.at[row_index, "final_price"] = final_close
                        history.at[row_index, "bars_monitored"] = int(bars_monitored)
                        resolved += 1

                if resolved:
                    self._save_dataframe_unlocked(history)
            return resolved
        except Exception as exc:
            logger.error("Failed to resolve open signals: %s", exc, exc_info=True)
            return 0

    def build_profile(
        self,
        asset: str,
        interval: str,
        session: str = "",
        min_resolved_trades: int = 5,
        min_win_rate: float = 0.52,
        min_profit_factor: float = 1.05,
    ) -> PerformanceProfile:
        """Build a performance profile for an asset/timeframe combination."""
        with self._lock:
            history = self._load_dataframe_unlocked()

        profile = PerformanceProfile(asset=asset, interval=interval, session=session)

        if history.empty:
            profile.notes.append("Sem historico suficiente")
            return profile

        entries = _analytics_entries(history)
        if entries.empty:
            profile.notes.append("Sem historico suficiente")
            return profile

        base_mask = (
            (entries["asset"] == asset)
            & (entries["interval"] == interval)
            & (entries["result"].isin([RESULT_WIN, RESULT_LOSS, RESULT_TIMEOUT, RESULT_DRAW]))
        )
        scoped = entries.loc[base_mask].copy()
        if scoped.empty:
            interval_scoped = entries[
                (entries["interval"] == interval)
                & (entries["result"].isin([RESULT_WIN, RESULT_LOSS, RESULT_TIMEOUT, RESULT_DRAW]))
            ].copy()
            if not interval_scoped.empty:
                scoped = interval_scoped
                profile.scope = "interval_global"
                profile.notes.append("Usando historico global do timeframe")
            else:
                global_scoped = entries[entries["result"].isin([RESULT_WIN, RESULT_LOSS, RESULT_TIMEOUT, RESULT_DRAW])].copy()
                if not global_scoped.empty:
                    scoped = global_scoped
                    profile.scope = "global"
                    profile.notes.append("Usando historico global")
                else:
                    profile.notes.append("Sem trades resolvidos")
                    return profile

        session_scoped = pd.DataFrame()
        if session:
            session_scoped = scoped[scoped["session"] == session].copy()
            decisive_session = session_scoped["result"].isin([RESULT_WIN, RESULT_LOSS]).sum()
            if decisive_session >= max(3, min_resolved_trades - 1):
                scoped = session_scoped
                profile.scope = "pair_interval_session"

        decisive = scoped[scoped["result"].isin([RESULT_WIN, RESULT_LOSS])].copy()
        recent = decisive.head(8).copy()

        profile.resolved_trades = int(len(decisive))
        profile.wins = int((decisive["result"] == RESULT_WIN).sum())
        profile.losses = int((decisive["result"] == RESULT_LOSS).sum())
        profile.timeouts = int((scoped["result"] == RESULT_TIMEOUT).sum())
        profile.draws = int((scoped["result"] == RESULT_DRAW).sum())

        if profile.resolved_trades > 0:
            profile.win_rate = profile.wins / profile.resolved_trades
            recent_total = max(1, len(recent))
            profile.recent_win_rate = float((recent["result"] == RESULT_WIN).sum()) / recent_total
        else:
            profile.notes.append("Historico em aprendizado")

        gross_profit = float(
            decisive.loc[decisive["result"] == RESULT_WIN, "rr_ratio"].clip(lower=0.0).sum()
        )
        gross_loss = float((decisive["result"] == RESULT_LOSS).sum())
        if gross_loss > 0:
            profile.profit_factor = gross_profit / gross_loss
        elif gross_profit > 0:
            profile.profit_factor = gross_profit

        recent_statuses = recent["result"].tolist()
        recent_loss_streak = 0
        for status in recent_statuses:
            if status == RESULT_LOSS:
                recent_loss_streak += 1
            else:
                break

        if profile.resolved_trades < min_resolved_trades:
            profile.policy_state = "learning"
            profile.notes.append("Historico curto para filtro adaptativo")
            return profile

        # Strength of evidence scales with sample size
        evidence_factor = min(1.0, profile.resolved_trades / 12.0)

        if (
            recent_loss_streak >= 3
            or profile.win_rate < (min_win_rate - 0.06)
            or (profile.recent_win_rate < 0.30 and profile.resolved_trades >= 6)
            or profile.profit_factor < (min_profit_factor - 0.20)
        ):
            profile.policy_state = "blocked"
            profile.threshold_adjustment = 12.0 * evidence_factor
            profile.confidence_boost = -18.0 * evidence_factor
            profile.notes.append(f"Bloqueado: WR {profile.win_rate:.0%}, PF {profile.profit_factor:.2f}, streak -{recent_loss_streak}")
            return profile

        if profile.win_rate < min_win_rate or profile.profit_factor < min_profit_factor:
            profile.policy_state = "caution"
            profile.threshold_adjustment = 7.0 * evidence_factor
            profile.confidence_boost = -10.0 * evidence_factor
            profile.notes.append(f"Cautela: WR {profile.win_rate:.0%}, PF {profile.profit_factor:.2f}")
            return profile

        if (
            profile.win_rate >= min(min_win_rate + 0.12, 0.78)
            and profile.profit_factor >= (min_profit_factor + 0.20)
            and profile.recent_win_rate >= min_win_rate
        ):
            profile.policy_state = "boost"
            profile.threshold_adjustment = -6.0 * evidence_factor
            profile.confidence_boost = 10.0 * evidence_factor
            profile.notes.append(f"Favoravel: WR {profile.win_rate:.0%}, PF {profile.profit_factor:.2f}")
            return profile

        profile.policy_state = "neutral"
        profile.notes.append(f"Neutro: WR {profile.win_rate:.0%}")
        return profile

    def _load_dataframe_unlocked(self) -> pd.DataFrame:
        """Read the CSV and upgrade it to the current schema."""
        if not self.path.exists() or self.path.stat().st_size == 0:
            return ensure_history_schema(pd.DataFrame(columns=FIELDNAMES))

        try:
            df = pd.read_csv(self.path)
        except pd.errors.EmptyDataError:
            return ensure_history_schema(pd.DataFrame(columns=FIELDNAMES))
        except Exception as exc:
            logger.warning("Failed to read signal history, recreating schema: %s", exc)
            return ensure_history_schema(pd.DataFrame(columns=FIELDNAMES))

        normalized = ensure_history_schema(df)
        if list(normalized.columns) != list(df.columns) or len(normalized.columns) != len(df.columns):
            self._save_dataframe_unlocked(normalized)
        return normalized

    def _save_dataframe_unlocked(self, df: pd.DataFrame) -> None:
        """Persist a normalized dataframe to disk."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        normalized = ensure_history_schema(df)
        normalized.to_csv(self.path, index=False)


def build_history_table(df: pd.DataFrame, limit: Optional[int] = None) -> pd.DataFrame:
    """Return a clean history table with the columns shown in the desktop UI."""
    entries = _analytics_entries(df)
    if entries.empty:
        return pd.DataFrame(columns=["signal_id", *HISTORY_TABLE_COLUMNS, "interval", "session", "strength", "confidence_label", "market_regime", "note", "source"])
    if limit is not None:
        entries = entries.head(limit)
    display = entries.copy()
    display["score"] = display["score"].map(lambda value: f"{float(value):.1f}%")
    return display.loc[:, ["signal_id", *HISTORY_TABLE_COLUMNS, "interval", "session", "strength", "confidence_label", "market_regime", "note", "source"]].reset_index(drop=True)


def summarize_history(df: pd.DataFrame) -> Dict[str, float]:
    """Build compact metrics for the history dashboard."""
    entries = _analytics_entries(df)
    if entries.empty:
        return {
            "total": 0,
            "wins": 0,
            "losses": 0,
            "pending": 0,
            "timeouts": 0,
            "draws": 0,
            "win_rate": 0.0,
            "avg_score": 0.0,
        }

    wins = int((entries["result"] == RESULT_WIN).sum())
    losses = int((entries["result"] == RESULT_LOSS).sum())
    pending = int((entries["result"] == RESULT_PENDING).sum())
    timeouts = int((entries["result"] == RESULT_TIMEOUT).sum())
    draws = int((entries["result"] == RESULT_DRAW).sum())
    decisive_total = max(1, wins + losses)

    return {
        "total": int(len(entries)),
        "wins": wins,
        "losses": losses,
        "pending": pending,
        "timeouts": timeouts,
        "draws": draws,
        "win_rate": (wins / decisive_total) * 100.0 if (wins + losses) > 0 else 0.0,
        "avg_score": float(entries["score"].mean()) if not entries.empty else 0.0,
    }


def build_history_overview(df: pd.DataFrame) -> Dict[str, Any]:
    """Return a shared analytics snapshot for both History and Dashboard."""
    entries = _analytics_entries(df)
    summary = summarize_history(entries)
    today = pd.Timestamp.now(tz=BRT).strftime("%d/%m/%Y")
    today_entries = entries[entries["date"] == today].copy()
    decisive = entries[entries["result"].isin([RESULT_WIN, RESULT_LOSS])].copy()

    def _rank(field: str, pick_max: bool = True) -> str:
        ranked = _group_win_rates(decisive, field)
        if ranked.empty:
            return "-"
        ranked = ranked.sort_values(["win_rate", "total"], ascending=[not pick_max, False])
        return str(ranked.iloc[0][field])

    return {
        **summary,
        "total_signals": int(summary["total"]),
        "wins": int(summary["wins"]),
        "losses": int(summary["losses"]),
        "signals_today": int(len(today_entries)),
        "best_pair": _rank("asset", pick_max=True),
        "worst_pair": _rank("asset", pick_max=False),
        "best_timeframe": _rank("interval", pick_max=True),
        "best_session": _rank("session", pick_max=True),
        "best_regime": _rank("market_regime", pick_max=True),
        "best_hour": _rank("hour", pick_max=True),
        "best_score_bucket": _rank("score_bucket", pick_max=True),
        "current_win_streak": _current_streak(entries, RESULT_WIN),
        "current_loss_streak": _current_streak(entries, RESULT_LOSS),
        "best_pairs": _top_labels(decisive, "asset", ascending=False),
        "worst_pairs": _top_labels(decisive, "asset", ascending=True),
        "win_rate_by_pair": _group_win_rates(decisive, "asset").to_dict("records"),
        "win_rate_by_timeframe": _group_win_rates(decisive, "interval").to_dict("records"),
        "win_rate_by_hour": _group_win_rates(decisive, "hour").to_dict("records"),
        "win_rate_by_session": _group_win_rates(decisive, "session").to_dict("records"),
        "win_rate_by_regime": _group_win_rates(decisive, "market_regime").to_dict("records"),
        "win_rate_by_score_bucket": _group_win_rates(decisive, "score_bucket").to_dict("records"),
    }


def evaluate_operational_pause(
    df: pd.DataFrame,
    enabled: bool = True,
    max_consecutive_losses: int = 3,
    min_daily_win_rate: float = 0.0,
) -> Dict[str, Any]:
    """Evaluate whether automatic signal delivery should pause."""
    state = {
        "enabled": bool(enabled),
        "paused": False,
        "reason": "",
        "current_loss_streak": 0,
        "today_win_rate": 0.0,
    }
    if not enabled:
        return state

    entries = _analytics_entries(df)
    decisive = entries[entries["result"].isin([RESULT_WIN, RESULT_LOSS])].copy()
    today = pd.Timestamp.now(tz=BRT).strftime("%d/%m/%Y")
    today_decisive = decisive[decisive["date"] == today].copy()
    loss_streak = _current_streak(entries, RESULT_LOSS)
    wins_today = int((today_decisive["result"] == RESULT_WIN).sum())
    losses_today = int((today_decisive["result"] == RESULT_LOSS).sum())
    decisive_today_total = wins_today + losses_today
    today_win_rate = (wins_today / decisive_today_total) * 100.0 if decisive_today_total else 0.0

    state["current_loss_streak"] = loss_streak
    state["today_win_rate"] = today_win_rate

    if max_consecutive_losses > 0 and loss_streak >= max_consecutive_losses:
        state["paused"] = True
        state["reason"] = f"sequencia de {loss_streak} losses seguidos"
        return state

    if min_daily_win_rate > 0 and decisive_today_total >= 3 and today_win_rate < min_daily_win_rate:
        state["paused"] = True
        state["reason"] = f"win rate do dia abaixo de {min_daily_win_rate:.1f}%"
        return state

    return state


def _analytics_entries(df: pd.DataFrame) -> pd.DataFrame:
    normalized = ensure_history_schema(df)
    entries = normalized[normalized["action"].isin(sorted(ENTRY_ACTIONS))].copy()
    if entries.empty:
        return entries

    entries["sort_ts"] = pd.to_datetime(entries["timestamp"], utc=True, errors="coerce")
    entries = entries.sort_values("sort_ts", ascending=False, na_position="last")
    entries = entries.drop_duplicates(subset=["signal_id"], keep="last").reset_index(drop=True)
    entries["hour"] = pd.to_numeric(entries["hour"], errors="coerce").fillna(
        pd.to_numeric(entries["time"].astype(str).str.slice(0, 2), errors="coerce")
    ).fillna(0).astype(int).astype(str).str.zfill(2)
    entries["score_bucket"] = entries["score"].map(_score_bucket)
    entries["result"] = [
        _result_label(status, action)
        for status, action in zip(entries["result_status"], entries["action"])
    ]
    return entries


def _score_bucket(value: Any) -> str:
    score = _safe_float(value, 0.0)
    if score >= 85:
        return "85+"
    if score >= 75:
        return "75-84"
    if score >= 65:
        return "65-74"
    if score >= 55:
        return "55-64"
    return "0-54"


def _group_win_rates(entries: pd.DataFrame, field: str) -> pd.DataFrame:
    if entries.empty or field not in entries.columns:
        return pd.DataFrame(columns=[field, "wins", "losses", "total", "win_rate"])

    grouped = (
        entries.groupby(field, dropna=False)
        .agg(
            wins=("result", lambda items: int((items == RESULT_WIN).sum())),
            losses=("result", lambda items: int((items == RESULT_LOSS).sum())),
            total=("result", "count"),
        )
        .reset_index()
    )
    if grouped.empty:
        return grouped
    grouped["win_rate"] = grouped["wins"] / grouped["total"]
    grouped[field] = grouped[field].replace("", "-")
    return grouped


def _current_streak(entries: pd.DataFrame, target: str) -> int:
    streak = 0
    for result in entries["result"].tolist():
        if result == target:
            streak += 1
        elif result in {RESULT_WIN, RESULT_LOSS}:
            break
    return streak


def _top_labels(entries: pd.DataFrame, field: str, ascending: bool) -> List[str]:
    grouped = _group_win_rates(entries, field)
    if grouped.empty:
        return []
    ordered = grouped.sort_values(["win_rate", "total"], ascending=[ascending, False]).head(3)
    return [str(value) for value in ordered[field].tolist()]
