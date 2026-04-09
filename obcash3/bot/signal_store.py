from __future__ import absolute_import
"""
SQLite persistence for FREE window orchestration.

This store is the source of truth for:
- signals sent in each FREE window
- result status
- summary sent status
- VIP CTA sent status
"""

import sqlite3
import threading
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from obcash3.config.settings import BOT_DB_PATH
from obcash3.utils.logger import get_logger
from obcash3.utils.time import now_br

logger = get_logger(__name__)

GROUP_FREE = "free"
RESULT_PENDING = "PENDENTE"
RESULT_WIN = "WIN"
RESULT_LOSS = "LOSS"


@dataclass(frozen=True)
class WindowStats:
    date: str
    window: str
    total_signals: int
    total_wins: int
    total_losses: int
    assertividade_percentual: float
    lucro_total_estimado: float


class WindowSignalStore:
    """Thread-safe SQLite store used by the cloud bot scheduler."""

    def __init__(self, db_path: Path = BOT_DB_PATH):
        self.db_path = Path(db_path)
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_db(self) -> None:
        with self._lock:
            connection = self._connect()
            try:
                cursor = connection.cursor()
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS window_signals (
                        id TEXT PRIMARY KEY,
                        history_signal_id TEXT,
                        date TEXT NOT NULL,
                        window TEXT NOT NULL,
                        sequence INTEGER NOT NULL,
                        group_name TEXT NOT NULL,
                        asset TEXT NOT NULL,
                        direction TEXT NOT NULL,
                        action TEXT NOT NULL,
                        interval TEXT NOT NULL,
                        price REAL NOT NULL,
                        entry_time TEXT NOT NULL,
                        entry_timestamp TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        sent_at TEXT NOT NULL,
                        technical_score REAL NOT NULL,
                        score_ml REAL NOT NULL,
                        score_final REAL NOT NULL,
                        status TEXT NOT NULL,
                        result TEXT NOT NULL,
                        profit_estimate REAL NOT NULL,
                        stake_estimate REAL NOT NULL,
                        telegram_message_id INTEGER,
                        source TEXT,
                        report_sent INTEGER NOT NULL DEFAULT 0,
                        cta_sent INTEGER NOT NULL DEFAULT 0,
                        result_updated_at TEXT,
                        selection_reason TEXT,
                        extra_payload TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS window_state (
                        date TEXT NOT NULL,
                        window TEXT NOT NULL,
                        group_name TEXT NOT NULL,
                        report_sent INTEGER NOT NULL DEFAULT 0,
                        report_sent_at TEXT,
                        report_message_id INTEGER,
                        image_sent INTEGER NOT NULL DEFAULT 0,
                        image_sent_at TEXT,
                        image_message_id INTEGER,
                        cta_sent INTEGER NOT NULL DEFAULT 0,
                        cta_sent_at TEXT,
                        cta_message_id INTEGER,
                        PRIMARY KEY (date, window, group_name)
                    )
                    """
                )
                # SQLite migration: preserve production DBs created before image support.
                state_columns = {
                    row["name"] if isinstance(row, sqlite3.Row) else row[1]
                    for row in cursor.execute("PRAGMA table_info(window_state)").fetchall()
                }
                if "image_sent" not in state_columns:
                    cursor.execute("ALTER TABLE window_state ADD COLUMN image_sent INTEGER NOT NULL DEFAULT 0")
                if "image_sent_at" not in state_columns:
                    cursor.execute("ALTER TABLE window_state ADD COLUMN image_sent_at TEXT")
                if "image_message_id" not in state_columns:
                    cursor.execute("ALTER TABLE window_state ADD COLUMN image_message_id INTEGER")
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_window_signals_date_window ON window_signals(date, window, group_name)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_window_signals_result ON window_signals(result, status)"
                )
                connection.commit()
            finally:
                connection.close()

    def save_signal(self, signal: Dict[str, Any]) -> bool:
        """Persist a sent FREE-window signal once."""
        payload = dict(signal)
        with self._lock:
            connection = self._connect()
            try:
                cursor = connection.cursor()
                cursor.execute("SELECT 1 FROM window_signals WHERE id = ?", (payload["id"],))
                if cursor.fetchone() is not None:
                    return False

                cursor.execute(
                    """
                    INSERT INTO window_signals (
                        id, history_signal_id, date, window, sequence, group_name,
                        asset, direction, action, interval, price, entry_time, entry_timestamp,
                        created_at, sent_at, technical_score, score_ml, score_final,
                        status, result, profit_estimate, stake_estimate,
                        telegram_message_id, source, report_sent, cta_sent,
                        result_updated_at, selection_reason, extra_payload
                    )
                    VALUES (
                        :id, :history_signal_id, :date, :window, :sequence, :group_name,
                        :asset, :direction, :action, :interval, :price, :entry_time, :entry_timestamp,
                        :created_at, :sent_at, :technical_score, :score_ml, :score_final,
                        :status, :result, :profit_estimate, :stake_estimate,
                        :telegram_message_id, :source, :report_sent, :cta_sent,
                        :result_updated_at, :selection_reason, :extra_payload
                    )
                    """,
                    payload,
                )
                connection.commit()
                return True
            finally:
                connection.close()

    def attach_message_id(self, signal_id: str, message_id: Optional[int]) -> bool:
        """Persist the Telegram message_id after a successful send."""
        with self._lock:
            connection = self._connect()
            try:
                cursor = connection.cursor()
                cursor.execute(
                    """
                    UPDATE window_signals
                    SET telegram_message_id = ?, status = 'sent', sent_at = ?
                    WHERE id = ?
                    """,
                    (message_id, now_br().isoformat(), signal_id),
                )
                connection.commit()
                return cursor.rowcount > 0
            finally:
                connection.close()

    def delete_signal(self, signal_id: str) -> bool:
        """Rollback a signal that failed before the Telegram send was confirmed."""
        with self._lock:
            connection = self._connect()
            try:
                cursor = connection.cursor()
                cursor.execute("DELETE FROM window_signals WHERE id = ?", (signal_id,))
                connection.commit()
                return cursor.rowcount > 0
            finally:
                connection.close()

    def signal_exists_for_window(
        self,
        date: str,
        window: str,
        asset: str,
        entry_time: str,
        action: str,
    ) -> bool:
        with self._lock:
            connection = self._connect()
            try:
                cursor = connection.cursor()
                cursor.execute(
                    """
                    SELECT 1
                    FROM window_signals
                    WHERE date = ? AND window = ? AND group_name = ?
                      AND asset = ? AND entry_time = ? AND action = ?
                    LIMIT 1
                    """,
                    (date, window, GROUP_FREE, asset, entry_time, action),
                )
                return cursor.fetchone() is not None
            finally:
                connection.close()

    def count_window_signals(self, date: str, window: str, group_name: str = GROUP_FREE) -> int:
        with self._lock:
            connection = self._connect()
            try:
                cursor = connection.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM window_signals WHERE date = ? AND window = ? AND group_name = ?",
                    (date, window, group_name),
                )
                row = cursor.fetchone()
                return int(row[0]) if row else 0
            finally:
                connection.close()

    def last_signal_sent_at(self, date: str, window: str, group_name: str = GROUP_FREE) -> str:
        with self._lock:
            connection = self._connect()
            try:
                cursor = connection.cursor()
                cursor.execute(
                    """
                    SELECT sent_at
                    FROM window_signals
                    WHERE date = ? AND window = ? AND group_name = ?
                    ORDER BY sequence DESC
                    LIMIT 1
                    """,
                    (date, window, group_name),
                )
                row = cursor.fetchone()
                return str(row["sent_at"]) if row else ""
            finally:
                connection.close()

    def next_sequence(self, date: str, window: str, group_name: str = GROUP_FREE) -> int:
        return self.count_window_signals(date, window, group_name) + 1

    def update_signal_result(
        self,
        signal_id: str,
        result: str,
        profit_estimate: float,
    ) -> bool:
        with self._lock:
            connection = self._connect()
            try:
                cursor = connection.cursor()
                cursor.execute(
                    """
                    UPDATE window_signals
                    SET result = ?, status = ?, profit_estimate = ?, result_updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        str(result).strip().upper(),
                        "resolved",
                        float(profit_estimate),
                        now_br().isoformat(),
                        signal_id,
                    ),
                )
                connection.commit()
                return cursor.rowcount > 0
            finally:
                connection.close()

    def get_window_signals(self, date: str, window: str, group_name: str = GROUP_FREE) -> list[Dict[str, Any]]:
        with self._lock:
            connection = self._connect()
            try:
                cursor = connection.cursor()
                cursor.execute(
                    """
                    SELECT *
                    FROM window_signals
                    WHERE date = ? AND window = ? AND group_name = ?
                    ORDER BY sequence ASC
                    """,
                    (date, window, group_name),
                )
                return [dict(row) for row in cursor.fetchall()]
            finally:
                connection.close()

    def get_pending_signals_due(self, as_of_iso: str, group_name: str = GROUP_FREE) -> list[Dict[str, Any]]:
        with self._lock:
            connection = self._connect()
            try:
                cursor = connection.cursor()
                cursor.execute(
                    """
                    SELECT *
                    FROM window_signals
                    WHERE group_name = ?
                      AND result = ?
                      AND status = 'sent'
                      AND entry_timestamp <= ?
                    ORDER BY entry_timestamp ASC
                    """,
                    (group_name, RESULT_PENDING, as_of_iso),
                )
                return [dict(row) for row in cursor.fetchall()]
            finally:
                connection.close()

    def is_window_complete(self, date: str, window: str, group_name: str = GROUP_FREE) -> bool:
        signals = self.get_window_signals(date, window, group_name)
        if len(signals) < 2:
            return False
        return all(str(item.get("result", RESULT_PENDING)).upper() in {RESULT_WIN, RESULT_LOSS} for item in signals)

    def calculate_window_stats(self, date: str, window: str, group_name: str = GROUP_FREE) -> WindowStats:
        signals = self.get_window_signals(date, window, group_name)
        wins = sum(1 for item in signals if str(item.get("result", "")).upper() == RESULT_WIN)
        losses = sum(1 for item in signals if str(item.get("result", "")).upper() == RESULT_LOSS)
        total = len(signals)
        decisive = wins + losses
        accuracy = (wins / decisive) * 100.0 if decisive > 0 else 0.0
        total_profit = sum(float(item.get("profit_estimate", 0.0) or 0.0) for item in signals if str(item.get("result", "")).upper() in {RESULT_WIN, RESULT_LOSS})
        return WindowStats(
            date=date,
            window=window,
            total_signals=total,
            total_wins=wins,
            total_losses=losses,
            assertividade_percentual=accuracy,
            lucro_total_estimado=total_profit,
        )

    def get_window_state(self, date: str, window: str, group_name: str = GROUP_FREE) -> Dict[str, Any]:
        with self._lock:
            connection = self._connect()
            try:
                cursor = connection.cursor()
                cursor.execute(
                    """
                    SELECT *
                    FROM window_state
                    WHERE date = ? AND window = ? AND group_name = ?
                    """,
                    (date, window, group_name),
                )
                row = cursor.fetchone()
                if row is not None:
                    return dict(row)
            finally:
                connection.close()
        return {
            "date": date,
            "window": window,
            "group_name": group_name,
            "report_sent": 0,
            "report_sent_at": "",
            "report_message_id": None,
            "image_sent": 0,
            "image_sent_at": "",
            "image_message_id": None,
            "cta_sent": 0,
            "cta_sent_at": "",
            "cta_message_id": None,
        }

    def mark_window_report_sent(self, date: str, window: str, message_id: Optional[int], group_name: str = GROUP_FREE) -> None:
        self._upsert_window_state(
            date=date,
            window=window,
            group_name=group_name,
            report_sent=1,
            report_sent_at=now_br().isoformat(),
            report_message_id=message_id,
        )

    def mark_window_image_sent(self, date: str, window: str, message_id: Optional[int], group_name: str = GROUP_FREE) -> None:
        self._upsert_window_state(
            date=date,
            window=window,
            group_name=group_name,
            image_sent=1,
            image_sent_at=now_br().isoformat(),
            image_message_id=message_id,
        )

    def mark_window_cta_sent(self, date: str, window: str, message_id: Optional[int], group_name: str = GROUP_FREE) -> None:
        self._upsert_window_state(
            date=date,
            window=window,
            group_name=group_name,
            cta_sent=1,
            cta_sent_at=now_br().isoformat(),
            cta_message_id=message_id,
        )

    def _upsert_window_state(self, date: str, window: str, group_name: str, **updates: Any) -> None:
        state = self.get_window_state(date, window, group_name)
        state.update(updates)
        with self._lock:
            connection = self._connect()
            try:
                cursor = connection.cursor()
                cursor.execute(
                    """
                    INSERT INTO window_state (
                        date, window, group_name,
                        report_sent, report_sent_at, report_message_id,
                        image_sent, image_sent_at, image_message_id,
                        cta_sent, cta_sent_at, cta_message_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(date, window, group_name)
                    DO UPDATE SET
                        report_sent = excluded.report_sent,
                        report_sent_at = excluded.report_sent_at,
                        report_message_id = excluded.report_message_id,
                        image_sent = excluded.image_sent,
                        image_sent_at = excluded.image_sent_at,
                        image_message_id = excluded.image_message_id,
                        cta_sent = excluded.cta_sent,
                        cta_sent_at = excluded.cta_sent_at,
                        cta_message_id = excluded.cta_message_id
                    """,
                    (
                        state["date"],
                        state["window"],
                        state["group_name"],
                        int(state.get("report_sent", 0)),
                        state.get("report_sent_at", ""),
                        state.get("report_message_id"),
                        int(state.get("image_sent", 0)),
                        state.get("image_sent_at", ""),
                        state.get("image_message_id"),
                        int(state.get("cta_sent", 0)),
                        state.get("cta_sent_at", ""),
                        state.get("cta_message_id"),
                    ),
                )
                connection.commit()
            finally:
                connection.close()

    def list_windows_for_date(self, date: str, group_name: str = GROUP_FREE) -> list[str]:
        with self._lock:
            connection = self._connect()
            try:
                cursor = connection.cursor()
                cursor.execute(
                    """
                    SELECT DISTINCT window
                    FROM window_signals
                    WHERE date = ? AND group_name = ?
                    ORDER BY window ASC
                    """,
                    (date, group_name),
                )
                return [str(row["window"]) for row in cursor.fetchall()]
            finally:
                connection.close()

    def prune_old_data(self, keep_days: int = 14) -> None:
        threshold = now_br().date().toordinal() - max(1, int(keep_days))
        with self._lock:
            connection = self._connect()
            try:
                cursor = connection.cursor()
                cursor.execute("SELECT DISTINCT date FROM window_signals")
                dates = [str(row["date"]) for row in cursor.fetchall()]
                removable = []
                for item in dates:
                    try:
                        if date.fromisoformat(item).toordinal() < threshold:
                            removable.append(item)
                    except Exception:
                        continue
                for item in removable:
                    cursor.execute("DELETE FROM window_signals WHERE date = ?", (item,))
                    cursor.execute("DELETE FROM window_state WHERE date = ?", (item,))
                connection.commit()
            finally:
                connection.close()


def _get_store(store: Optional[WindowSignalStore] = None) -> WindowSignalStore:
    return store or WindowSignalStore()


def save_signal(signal: Dict[str, Any], store: Optional[WindowSignalStore] = None) -> bool:
    return _get_store(store).save_signal(signal)


def update_signal_result(signal_id: str, result: str, profit_estimate: float, store: Optional[WindowSignalStore] = None) -> bool:
    return _get_store(store).update_signal_result(signal_id, result, profit_estimate)


def get_window_signals(date: str, window: str, store: Optional[WindowSignalStore] = None) -> list[Dict[str, Any]]:
    return _get_store(store).get_window_signals(date, window)


def is_window_complete(date: str, window: str, store: Optional[WindowSignalStore] = None) -> bool:
    return _get_store(store).is_window_complete(date, window)


def calculate_window_stats(date: str, window: str, store: Optional[WindowSignalStore] = None) -> WindowStats:
    return _get_store(store).calculate_window_stats(date, window)
