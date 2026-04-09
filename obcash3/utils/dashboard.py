from __future__ import absolute_import
"""
Dashboard metrics helpers backed by the shared history analytics layer.
"""

from typing import Any, Dict

import pandas as pd

from obcash3.utils.history import SignalHistoryStore, build_history_overview


def build_dashboard_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """Compute premium dashboard metrics from the shared history dataset."""
    return build_history_overview(df)


def build_dashboard_metrics_from_store(store: SignalHistoryStore) -> Dict[str, Any]:
    """Load history from store and compute dashboard metrics."""
    return build_dashboard_metrics(store.load_dataframe())
