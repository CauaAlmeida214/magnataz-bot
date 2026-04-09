from __future__ import absolute_import
"""
Helper utilities for OB CASH 3.0.
"""

from typing import List, Dict, Any
from datetime import datetime
import json
import os
from pathlib import Path


def ensure_dir(path: str | Path) -> Path:
    """Ensure directory exists, create if needed."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def load_json(path: str | Path, default: Any = None) -> Any:
    """Load JSON file with error handling."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save_json(data: Any, path: str | Path, indent: int = 2) -> bool:
    """Save data to JSON file."""
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"Error saving JSON: {e}")
        return False


def format_number(num: float, decimals: int = 2) -> str:
    """Format number with thousands separator."""
    return f"{num:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def safe_divide(a: float, b: float, default: float = 0.0) -> float:
    """Safe division with zero protection."""
    try:
        return a / b if b != 0 else default
    except:
        return default


def exponential_backoff(attempt: int, base: float = 2.0, max_delay: float = 60.0) -> float:
    """Calculate exponential backoff delay."""
    delay = min(base ** attempt, max_delay)
    return delay + (os.getrandom(1)[0] % 100) / 100  # Add jitter


def is_market_hours(holidays: List[datetime] = None) -> bool:
    """Check if current time is within market hours."""
    now = datetime.now()
    # Simplified: Monday-Friday, 00:00-23:59 UTC
    is_weekday = now.weekday() < 5
    if holidays and now.date() in holidays:
        return False
    return is_weekday


def chunk_list(lst: List[Any], size: int) -> List[List[Any]]:
    """Split list into chunks of size."""
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def truncate_string(s: str, max_len: int = 50, suffix: str = "...") -> str:
    """Truncate string with suffix."""
    if len(s) <= max_len:
        return s
    return s[:max_len - len(suffix)] + suffix


def human_readable_time(seconds: int) -> str:
    """Convert seconds to human readable format."""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m"
    elif seconds < 86400:
        return f"{seconds // 3600}h"
    else:
        return f"{seconds // 86400}d"


def calculate_position_size(
    account_balance: float,
    risk_percent: float,
    entry_price: float,
    stop_loss: float
) -> float:
    """Calculate position size based on risk management."""
    if entry_price == stop_loss:
        return 0.0
    risk_amount = account_balance * (risk_percent / 100)
    price_risk = abs(entry_price - stop_loss)
    return risk_amount / price_risk if price_risk > 0 else 0.0


def validate_email(email: str) -> bool:
    """Basic email validation."""
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


def validate_http_url(url: str) -> bool:
    """Basic URL validation."""
    import re
    pattern = r'^https?://[^\s/$.?#].[^\s]*$'
    return re.match(pattern, url) is not None


def merge_dicts(dict1: Dict, dict2: Dict) -> Dict:
    """Deep merge two dictionaries."""
    result = dict1.copy()
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def get_ellapsed_time(start: datetime) -> str:
    """Get formatted elapsed time from start."""
    delta = datetime.now() - start
    seconds = delta.total_seconds()
    return human_readable_time(int(seconds))
