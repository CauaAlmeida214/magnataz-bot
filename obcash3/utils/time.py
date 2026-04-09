from __future__ import absolute_import
"""
Time utilities for OB CASH 3.0.

Handles timezone conversions, market sessions, and scheduling.
"""

from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from obcash3.config.settings import BRT, SESSIONS

# Brazilian timezone
BRT_TZ = BRT


def now_br() -> datetime:
    """Get current time in Brazil timezone."""
    return datetime.now(BRT_TZ)


def to_brt_datetime(value: Optional[datetime]) -> datetime:
    """Convert naive/aware/string-like values into Brazil timezone."""
    if value is None:
        return now_br()
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except Exception:
            return now_br()
    if not isinstance(value, datetime):
        return now_br()
    if value.tzinfo is None:
        return value.replace(tzinfo=BRT_TZ)
    return value.astimezone(BRT_TZ)


def next_candle_start(interval: str, base_time: Optional[datetime] = None) -> datetime:
    """
    Calculate the start time of the next candlestick.

    Args:
        interval: Timeframe (e.g., "5m", "1h")
        base_time: Reference time (default: current time)

    Returns:
        Datetime of next candle opening (minute-aligned)
    """
    if base_time is None:
        base_time = now_br()
    else:
        base_time = to_brt_datetime(base_time)

    # Align to minute
    n = base_time.replace(second=0, microsecond=0)

    # Get interval in minutes
    interval_map = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60}
    m = interval_map.get(interval, 5)

    # Round down to current interval, then add one interval
    return n - timedelta(minutes=n.minute % m) + timedelta(minutes=m)


def is_liquid_hours(hour: Optional[int] = None) -> bool:
    """
    Check if current (or given) hour is during liquid market hours.

    Liquid hours: 02:00 - 22:00 BRT (excludes low liquidity period)

    Args:
        hour: Hour to check (0-23). Default: current hour

    Returns:
        True if within liquid hours
    """
    if hour is None:
        hour = now_br().hour
    return 2 <= hour < 22


def get_current_session() -> str:
    """
    Determine the current market session based on BRT hour.

    Returns:
        Session name (e.g., "LONDON_NY", "ASIANA")
    """
    h = now_br().hour
    for session in SESSIONS:
        if session.start_hour <= h < session.end_hour:
            return session.name

    # Wrap around for sessions crossing midnight
    for session in SESSIONS:
        if session.start_hour > session.end_hour:
            if h >= session.start_hour or h < session.end_hour:
                return session.name

    return "ASIANA"  # default


def get_session_config(session_name: Optional[str] = None) -> dict:
    """
    Get configuration for a specific session.

    Args:
        session_name: Session name. Default: current session

    Returns:
        Dictionary with session configuration
    """
    if session_name is None:
        session_name = get_current_session()

    for session in SESSIONS:
        if session.name == session_name:
            return {
                "label": session.label,
                "min_score": session.min_score,
                "start_hour": session.start_hour,
                "end_hour": session.end_hour,
            }

    return {"label": session_name, "min_score": 60}


def format_time_brazil(dt: datetime) -> str:
    """Format datetime for Brazilian display."""
    return dt.strftime("%H:%M:%S")


def format_date_brazil(dt: datetime) -> str:
    """Format date for Brazilian display."""
    return dt.strftime("%d/%m/%Y")


def seconds_until_next_candle(interval: str) -> int:
    """
    Get seconds until next candle opens.

    Useful for scheduling scans.

    Args:
        interval: Timeframe

    Returns:
        Seconds until next candle (integer, >= 1)
    """
    next_time = next_candle_start(interval)
    now = now_br()
    delta = next_time - now
    return max(1, int(delta.total_seconds()))


def align_to_candle(df_time_index: datetime, interval: str) -> datetime:
    """
    Align a timestamp to the start of its candlestick period.

    Args:
        df_time_index: Timestamp to align
        interval: Timeframe

    Returns:
        Aligned timestamp (candle open time)
    """
    interval_map = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60}
    minutes = interval_map.get(interval, 5)

    # Round down to nearest interval
    minute = (df_time_index.minute // minutes) * minutes
    return df_time_index.replace(minute=minute, second=0, microsecond=0)
