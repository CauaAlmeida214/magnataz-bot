from __future__ import absolute_import
"""
Data fetchers for OB CASH 3.0.

Implements a multi-provider collection pipeline with:
1. Twelve Data as the preferred source
2. Alpha Vantage as secondary source
3. Yahoo Finance as reserve source

All enabled providers are queried in parallel. The returned datasets are
validated, compared, and the most reliable result is selected while preserving
compatibility with the rest of the application.
"""

import json
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from obcash3.config.settings import AV_I, HDR, PAIRS, PDAYS, TW_I, YF_I
from obcash3.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ProviderResult:
    """Normalized result for a single market data provider."""

    provider: str
    source: str
    df: Optional[pd.DataFrame]
    valid: bool
    rows: int
    status: str
    deviation_pct: Optional[float] = None


class DataFetcher:
    """Fetch market data from multiple providers with validation and consensus."""

    REQUIRED_COLUMNS = ("Open", "High", "Low", "Close", "Volume")
    PRICE_COLUMNS = ("Open", "High", "Low", "Close")
    MIN_ROWS = 60
    CACHE_SOURCE = "multi_source_v2"
    PROVIDER_PRIORITY = {
        "Twelve Data": 0,
        "Alpha Vantage": 1,
        "Yahoo Finance": 2,
    }
    CONSENSUS_WINDOW = 40
    AGREEMENT_EPSILON_PCT = 0.015

    def __init__(self, cache_manager=None):
        self.cache = cache_manager
        self._provider_backoff_until: Dict[str, float] = {}
        self._import_yfinance()

    def _import_yfinance(self) -> None:
        """Lazy import yfinance to avoid hard dependency."""
        try:
            import yfinance as yf

            self._yf = yf
            self._has_yf = True
        except ImportError:
            self._has_yf = False
            logger.warning("yfinance not available - Yahoo Finance source disabled")

    def fetch_data(
        self,
        pair_name: str,
        interval: str,
        twelve_key: str = "",
        av_key: str = "",
        use_cache: bool = True,
    ) -> Tuple[Optional[pd.DataFrame], str]:
        """
        Fetch OHLCV data using concurrent providers plus consensus selection.

        Returns:
            Tuple of (DataFrame, source_name) or (None, "none")
        """
        try:
            normalized_pair, symbols = self._resolve_symbols(pair_name)
        except ValueError as exc:
            logger.error("Pair normalization failed: %s", exc)
            return None, "none"

        normalized_interval = self._normalize_interval(interval)
        cache_key = f"{normalized_pair}_{normalized_interval}"

        cached_result = self._get_cached_result(cache_key, normalized_interval, use_cache)
        if cached_result is not None:
            cached_df, cached_source = cached_result
            logger.info(
                "Using cached data for %s %s from %s",
                normalized_pair,
                normalized_interval,
                cached_source,
            )
            return cached_df, cached_source

        provider_results = self._fetch_all_providers(
            symbols=symbols,
            interval=normalized_interval,
            twelve_key=twelve_key,
            av_key=av_key,
        )

        selected = self._select_best_result(provider_results, normalized_pair, normalized_interval)
        if selected is None:
            return None, "none"

        self._store_cached_result(
            cache_key=cache_key,
            interval=normalized_interval,
            df=selected.df,
            source=selected.source,
            use_cache=use_cache,
        )
        return selected.df, selected.source

    def fetch_for_mtf(
        self,
        pair_name: str,
        higher_tf: str,
        twelve_key: str = "",
        av_key: str = "",
        use_cache: bool = True,
    ) -> Optional[pd.DataFrame]:
        """Fetch data for higher timeframe confirmation using the same pipeline."""
        df, _ = self.fetch_data(
            pair_name=pair_name,
            interval=higher_tf,
            twelve_key=twelve_key,
            av_key=av_key,
            use_cache=use_cache,
        )
        return df

    def fetch_from_twelve(
        self,
        pair_or_symbol: str,
        interval: str,
        api_key: str = "",
    ) -> Tuple[Optional[pd.DataFrame], str]:
        """Fetch from Twelve Data API."""
        if not api_key:
            return None, "skip"

        symbol = self._normalize_symbol(pair_or_symbol, "twelve")
        normalized_interval = self._normalize_interval(interval)
        url = (
            "https://api.twelvedata.com/time_series"
            f"?symbol={symbol}&interval={TW_I.get(normalized_interval, '5min')}"
            f"&apikey={api_key}&outputsize=500"
        )

        try:
            data = self._request_json(url)
            if "values" not in data or not data["values"]:
                message = data.get("message", "no data")
                return None, f"twelve_{message}"

            rows = [
                {
                    "Timestamp": item.get("datetime"),
                    "Open": float(item["open"]),
                    "High": float(item["high"]),
                    "Low": float(item["low"]),
                    "Close": float(item["close"]),
                    "Volume": float(item.get("volume", 0) or 0),
                }
                for item in data["values"]
            ]
            df = pd.DataFrame(rows).iloc[::-1].reset_index(drop=True)
            standardized = self._standardize_dataframe(df)
            if standardized is None:
                return None, "twelve_invalid_dataframe"

            logger.info("Twelve Data: fetched %d candles for %s", len(standardized), symbol)
            return standardized, "Twelve Data"
        except urllib.error.HTTPError as exc:
            if exc.code in (401, 403):
                return None, "twelve_invalid_key"
            return None, f"twelve_http_{exc.code}"
        except Exception as exc:
            return None, f"twelve_error: {str(exc)[:80]}"

    def fetch_from_alpha_vantage(
        self,
        pair_or_symbol: str,
        interval: str,
        api_key: str = "",
    ) -> Tuple[Optional[pd.DataFrame], str]:
        """Fetch from Alpha Vantage API."""
        if not api_key:
            return None, "skip"

        symbol = self._normalize_symbol(pair_or_symbol, "alpha_vantage")
        normalized_interval = self._normalize_interval(interval)
        from_symbol = symbol[:3]
        to_symbol = symbol[3:]
        url = (
            "https://www.alphavantage.co/query"
            "?function=FX_INTRADAY"
            f"&from_symbol={from_symbol}&to_symbol={to_symbol}"
            f"&interval={AV_I.get(normalized_interval, '60min')}"
            f"&apikey={api_key}&outputsize=full"
        )

        try:
            data = self._request_json(url)
            ts_key = next((key for key in data if "Time Series" in key), None)
            if not ts_key:
                note = data.get("Note", data.get("Information", "no data"))
                return None, f"av_{str(note)[:60]}"

            rows = [
                {
                    "Timestamp": timestamp,
                    "Open": float(values["1. open"]),
                    "High": float(values["2. high"]),
                    "Low": float(values["3. low"]),
                    "Close": float(values["4. close"]),
                    "Volume": 0.0,
                }
                for timestamp, values in data[ts_key].items()
            ]
            df = pd.DataFrame(rows).iloc[::-1].reset_index(drop=True)
            standardized = self._standardize_dataframe(df)
            if standardized is None:
                return None, "av_invalid_dataframe"

            logger.info("Alpha Vantage: fetched %d candles for %s", len(standardized), symbol)
            return standardized, "Alpha Vantage"
        except urllib.error.HTTPError as exc:
            return None, f"av_http_{exc.code}"
        except Exception as exc:
            return None, f"av_error: {str(exc)[:80]}"

    def fetch_from_yahoo(
        self,
        pair_or_symbol: str,
        interval: str,
    ) -> Tuple[Optional[pd.DataFrame], str]:
        """Fetch from Yahoo Finance as the reserve fallback."""
        if not self._has_yf:
            return None, "yfinance_unavailable"

        symbol = self._normalize_symbol(pair_or_symbol, "yahoo")
        normalized_interval = self._normalize_interval(interval)
        days = PDAYS.get(normalized_interval, 60)
        end = int(time.time())
        start = end - days * 86400

        try:
            ticker = self._yf.Ticker(symbol)
            df = ticker.history(
                start=start,
                end=end,
                interval=YF_I.get(normalized_interval, "1h"),
                timeout=15,
            )
            standardized = self._standardize_dataframe(df)
            if standardized is None:
                return None, "yahoo_invalid_dataframe"

            logger.info("Yahoo Finance: fetched %d candles for %s", len(standardized), symbol)
            return standardized, "Yahoo Finance"
        except TypeError:
            try:
                ticker = self._yf.Ticker(symbol)
                df = ticker.history(start=start, end=end, interval=YF_I.get(normalized_interval, "1h"))
                standardized = self._standardize_dataframe(df)
                if standardized is None:
                    return None, "yahoo_invalid_dataframe"

                logger.info("Yahoo Finance: fetched %d candles for %s", len(standardized), symbol)
                return standardized, "Yahoo Finance"
            except Exception as exc:
                return None, f"yahoo_error: {str(exc)[:80]}"
        except Exception as exc:
            return None, f"yahoo_error: {str(exc)[:80]}"

    def _fetch_all_providers(
        self,
        symbols: Dict[str, str],
        interval: str,
        twelve_key: str,
        av_key: str,
    ) -> List[ProviderResult]:
        """Fetch all providers concurrently and collect normalized results."""
        providers: List[Tuple[str, Callable[[], Tuple[Optional[pd.DataFrame], str]]]] = [
            (
                "Twelve Data",
                lambda: self.fetch_from_twelve(symbols["twelve"], interval, twelve_key),
            ),
            (
                "Alpha Vantage",
                lambda: self.fetch_from_alpha_vantage(symbols["alpha_vantage"], interval, av_key),
            ),
            (
                "Yahoo Finance",
                lambda: self.fetch_from_yahoo(symbols["yahoo"], interval),
            ),
        ]

        results: List[ProviderResult] = []
        active_providers: List[Tuple[str, Callable[[], Tuple[Optional[pd.DataFrame], str]]]] = []
        for provider_name, provider_fn in providers:
            if self._provider_ready(provider_name):
                active_providers.append((provider_name, provider_fn))
            else:
                results.append(
                    ProviderResult(
                        provider=provider_name,
                        source="skip",
                        df=None,
                        valid=False,
                        rows=0,
                        status=self._provider_skip_reason(provider_name),
                    )
                )

        if not active_providers:
            return results

        with ThreadPoolExecutor(max_workers=len(active_providers), thread_name_prefix="market-fetch") as executor:
            future_map = {executor.submit(provider_fn): provider_name for provider_name, provider_fn in active_providers}
            for future in as_completed(future_map):
                provider_name = future_map[future]
                try:
                    df, source = future.result()
                except Exception as exc:
                    df, source = None, f"unexpected_error: {str(exc)[:80]}"

                result = self._build_provider_result(provider_name, source, df)
                results.append(result)
                self._update_provider_backoff(provider_name, result)

                if result.valid:
                    logger.info(
                        "%s returned valid data with %d rows",
                        provider_name,
                        result.rows,
                    )
                elif result.status not in ("skip", "yfinance_unavailable"):
                    logger.warning("%s failed: %s", provider_name, result.status)

        results.sort(key=lambda item: self.PROVIDER_PRIORITY.get(item.provider, 99))
        return results

    def _build_provider_result(
        self,
        provider_name: str,
        source: str,
        df: Optional[pd.DataFrame],
    ) -> ProviderResult:
        """Create a normalized provider result object."""
        valid = self._is_valid_dataframe(df)
        rows = len(df) if df is not None else 0
        status = source if not valid else "ok"
        return ProviderResult(
            provider=provider_name,
            source=source,
            df=df,
            valid=valid,
            rows=rows,
            status=status,
        )

    def _select_best_result(
        self,
        results: List[ProviderResult],
        pair_name: str,
        interval: str,
    ) -> Optional[ProviderResult]:
        """Compare valid providers and choose the most reliable result."""
        valid_results = [result for result in results if result.valid and result.df is not None]
        if not valid_results:
            failures = [f"{result.provider}: {result.status}" for result in results if result.status not in ("skip", "yfinance_unavailable")]
            if failures:
                logger.error(
                    "All providers failed for %s %s. Reasons: %s",
                    pair_name,
                    interval,
                    "; ".join(failures),
                )
            return None

        if len(valid_results) == 1:
            selected = valid_results[0]
            logger.info(
                "Only one valid provider for %s %s: %s",
                pair_name,
                interval,
                selected.provider,
            )
            return ProviderResult(
                provider=selected.provider,
                source=selected.source,
                df=selected.df,
                valid=True,
                rows=selected.rows,
                status="selected_single_source",
                deviation_pct=0.0,
            )

        self._annotate_consensus(valid_results)
        selected = self._pick_consensus_winner(valid_results)
        selected_source = f"{selected.provider} [consensus]"

        summary = ", ".join(
            f"{result.provider}: rows={result.rows}, dev={result.deviation_pct:.5f}%"
            for result in valid_results
            if result.deviation_pct is not None
        )
        logger.info(
            "Consensus for %s %s -> selected %s | %s",
            pair_name,
            interval,
            selected.provider,
            summary,
        )

        return ProviderResult(
            provider=selected.provider,
            source=selected_source,
            df=selected.df,
            valid=True,
            rows=selected.rows,
            status="selected_consensus",
            deviation_pct=selected.deviation_pct,
        )

    def _annotate_consensus(self, results: List[ProviderResult]) -> None:
        """Compute mean deviation of each provider against the group median."""
        comparison_frames = self._build_comparison_frames(results)
        if not comparison_frames:
            for result in results:
                result.deviation_pct = 0.0
            return

        median_columns: Dict[str, pd.Series] = {}
        for column in self.PRICE_COLUMNS:
            median_columns[column] = pd.concat(
                [frame[column] for frame in comparison_frames.values()],
                axis=1,
            ).median(axis=1)

        for result in results:
            frame = comparison_frames[result.provider]
            deviations = []
            for column in self.PRICE_COLUMNS:
                benchmark = median_columns[column].abs().clip(lower=1e-9)
                delta = ((frame[column] - median_columns[column]).abs() / benchmark) * 100.0
                deviations.append(delta)

            deviation_series = pd.concat(deviations, axis=0)
            result.deviation_pct = float(deviation_series.mean()) if not deviation_series.empty else 0.0

    def _provider_ready(self, provider_name: str) -> bool:
        """Check whether a provider is currently outside its cooldown window."""
        return time.time() >= self._provider_backoff_until.get(provider_name, 0.0)

    def _provider_skip_reason(self, provider_name: str) -> str:
        """Describe why a provider was skipped."""
        remaining = max(0, int(self._provider_backoff_until.get(provider_name, 0.0) - time.time()))
        return f"cooldown_{remaining}s" if remaining > 0 else "skip"

    def _update_provider_backoff(self, provider_name: str, result: ProviderResult) -> None:
        """Apply provider cooldowns after known temporary or persistent failures."""
        if result.valid:
            self._provider_backoff_until.pop(provider_name, None)
            return

        cooldown = self._suggest_backoff_seconds(provider_name, result.status)
        if cooldown <= 0:
            return

        until = time.time() + cooldown
        previous = self._provider_backoff_until.get(provider_name, 0.0)
        self._provider_backoff_until[provider_name] = max(previous, until)

    def _suggest_backoff_seconds(self, provider_name: str, status: str) -> int:
        """Return provider-specific cooldown based on failure type."""
        status_text = str(status).lower()

        if provider_name == "Twelve Data":
            if "run out of api credits" in status_text:
                return 70
            if "invalid_key" in status_text:
                return 600

        if provider_name == "Alpha Vantage":
            if "premium endpoin" in status_text:
                return 1800
            if "please consider spreading" in status_text:
                return 300
            if "http_429" in status_text:
                return 300

        if provider_name == "Yahoo Finance" and "yahoo_error" in status_text:
            return 60

        return 0

    def _build_comparison_frames(self, results: List[ProviderResult]) -> Dict[str, pd.DataFrame]:
        """
        Build aligned frames for consensus scoring.

        The last open candle is ignored when possible to reduce mismatch caused
        by providers updating the current interval at different times.
        """
        min_rows = min(result.rows for result in results)
        if min_rows <= 0:
            return {}

        usable_rows = min(self.CONSENSUS_WINDOW, min_rows - 1) if min_rows > 1 else 1
        if usable_rows <= 0:
            usable_rows = 1

        comparison_frames: Dict[str, pd.DataFrame] = {}
        for result in results:
            frame = result.df.tail(usable_rows + 1).reset_index(drop=True)
            if len(frame) > usable_rows:
                frame = frame.iloc[:usable_rows]
            comparison_frames[result.provider] = frame.loc[:, list(self.PRICE_COLUMNS)].reset_index(drop=True)

        return comparison_frames

    def _pick_consensus_winner(self, results: List[ProviderResult]) -> ProviderResult:
        """Choose the best provider, preferring priority when agreement is close."""
        if len(results) == 1:
            return results[0]

        best_deviation = min((result.deviation_pct or 0.0) for result in results)
        contenders = [
            result
            for result in results
            if (result.deviation_pct or 0.0) <= best_deviation + self.AGREEMENT_EPSILON_PCT
        ]

        contenders.sort(
            key=lambda result: (
                self.PROVIDER_PRIORITY.get(result.provider, 99),
                -result.rows,
                result.deviation_pct or 0.0,
            )
        )
        return contenders[0]

    def _request_json(self, url: str) -> Dict[str, Any]:
        """Perform an HTTP GET and decode JSON."""
        request = urllib.request.Request(url, headers=HDR)
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))

    def _resolve_symbols(self, pair_name: str) -> Tuple[str, Dict[str, str]]:
        """Resolve a display pair into provider-specific symbols."""
        normalized_pair = self._normalize_pair_name(pair_name)
        if normalized_pair in PAIRS:
            twelve_symbol, alpha_symbol, yahoo_symbol = PAIRS[normalized_pair]
            return normalized_pair, {
                "twelve": twelve_symbol,
                "alpha_vantage": alpha_symbol,
                "yahoo": yahoo_symbol,
            }

        compact = self._compact_symbol(pair_name)
        for display_pair, symbols in PAIRS.items():
            candidates = {
                self._compact_symbol(display_pair),
                self._compact_symbol(symbols[0]),
                self._compact_symbol(symbols[1]),
                self._compact_symbol(symbols[2]),
            }
            if compact in candidates:
                return display_pair, {
                    "twelve": symbols[0],
                    "alpha_vantage": symbols[1],
                    "yahoo": symbols[2],
                }

        raise ValueError(f"Unknown pair: {pair_name}")

    def _normalize_pair_name(self, pair_name: str) -> str:
        """Normalize a display pair name."""
        pair = str(pair_name).strip().upper().replace("-", "/").replace("_", "/")
        if "/" not in pair and len(pair) == 6:
            pair = f"{pair[:3]}/{pair[3:]}"
        return pair

    def _normalize_interval(self, interval: str) -> str:
        """Normalize timeframe notation."""
        normalized = str(interval).strip().lower()
        return normalized if normalized in TW_I else "5m"

    def _normalize_symbol(self, pair_or_symbol: str, provider: str) -> str:
        """Normalize symbols for each provider."""
        try:
            _, symbols = self._resolve_symbols(pair_or_symbol)
            symbol = symbols[provider]
        except ValueError:
            symbol = self._compact_symbol(pair_or_symbol)

        if provider == "twelve":
            return self._format_twelve_symbol(symbol)
        if provider == "yahoo":
            compact = self._compact_symbol(symbol)
            return compact if compact.endswith("=X") else f"{compact}=X"
        return self._compact_symbol(symbol)

    def _format_twelve_symbol(self, value: str) -> str:
        """Format forex symbols for Twelve Data."""
        compact = self._compact_symbol(value)
        if len(compact) == 6:
            return f"{compact[:3]}/{compact[3:]}"
        return value

    def _compact_symbol(self, value: str) -> str:
        """Convert pair/symbol strings into compact comparable form."""
        return (
            str(value)
            .strip()
            .upper()
            .replace("/", "")
            .replace("-", "")
            .replace("_", "")
            .replace("=X", "")
        )

    def _standardize_dataframe(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """Normalize provider output into the app's standard OHLCV format."""
        if df is None or getattr(df, "empty", True):
            return None

        normalized = df.copy()
        timestamp_series = None

        if "Timestamp" in normalized.columns:
            timestamp_series = pd.to_datetime(normalized["Timestamp"], errors="coerce", utc=True)
        elif normalized.index.name or isinstance(normalized.index, pd.DatetimeIndex):
            timestamp_series = pd.to_datetime(normalized.index, errors="coerce", utc=True)

        column_map: Dict[str, str] = {}
        for column in normalized.columns:
            column_lower = str(column).strip().lower()
            if column_lower == "open":
                column_map[column] = "Open"
            elif column_lower == "high":
                column_map[column] = "High"
            elif column_lower == "low":
                column_map[column] = "Low"
            elif column_lower == "close":
                column_map[column] = "Close"
            elif column_lower == "volume":
                column_map[column] = "Volume"

        normalized = normalized.rename(columns=column_map)
        if "Volume" not in normalized.columns:
            normalized["Volume"] = 0.0
        if timestamp_series is not None:
            normalized["Timestamp"] = timestamp_series

        missing = [column for column in self.REQUIRED_COLUMNS if column not in normalized.columns]
        if missing:
            logger.warning("Missing required columns after normalization: %s", missing)
            return None

        selected_columns = list(self.REQUIRED_COLUMNS)
        if "Timestamp" in normalized.columns:
            selected_columns = ["Timestamp", *selected_columns]

        normalized = normalized.loc[:, selected_columns].copy()
        for column in self.REQUIRED_COLUMNS:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

        normalized["Volume"] = normalized["Volume"].fillna(0.0)
        if "Timestamp" in normalized.columns:
            normalized = normalized.dropna(subset=["Timestamp"])
            normalized = normalized.sort_values("Timestamp").reset_index(drop=True)
        normalized = normalized.dropna(subset=["Open", "High", "Low", "Close"]).reset_index(drop=True)

        if len(normalized) < self.MIN_ROWS:
            return None
        return normalized

    def _is_valid_dataframe(self, df: Optional[pd.DataFrame]) -> bool:
        """Check if a dataframe is valid for downstream usage."""
        if df is None or getattr(df, "empty", True):
            return False
        if len(df) < self.MIN_ROWS:
            return False
        return all(column in df.columns for column in self.REQUIRED_COLUMNS)

    def _get_cached_result(
        self,
        cache_key: str,
        interval: str,
        use_cache: bool,
    ) -> Optional[Tuple[pd.DataFrame, str]]:
        """Read a cached fetch result if available."""
        if not use_cache or not self.cache:
            return None

        cached = self.cache.get(cache_key, interval, self.CACHE_SOURCE)
        if cached is None:
            return None

        if isinstance(cached, dict) and "df" in cached:
            df = cached.get("df")
            source = str(cached.get("source", "cache"))
            if self._is_valid_dataframe(df):
                return df, source
            return None

        if isinstance(cached, pd.DataFrame) and self._is_valid_dataframe(cached):
            return cached, "cache"
        return None

    def _store_cached_result(
        self,
        cache_key: str,
        interval: str,
        df: pd.DataFrame,
        source: str,
        use_cache: bool,
    ) -> None:
        """Store a fetch result in cache."""
        if not use_cache or not self.cache:
            return

        self.cache.set(
            cache_key,
            interval,
            self.CACHE_SOURCE,
            {"df": df, "source": source},
        )
