from __future__ import absolute_import
"""
API services - business logic wrapper.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from obcash3.api.models import BacktestResponse, ScanAllResponse, SignalResponse, StatsResponse
from obcash3.backtesting.backtest import Backtester
from obcash3.config.manager import ConfigManager
from obcash3.config.settings import HISTORY_PATH, HTF, PAIRS
from obcash3.data.cache import CacheManager
from obcash3.data.fetcher import DataFetcher
from obcash3.data.signal_store import (
    resolve_signal_results_with_fallback,
    save_signal_record,
    update_signal_result,
)
from obcash3.ml.ml_manager import MachineLearningManager
from obcash3.signals.engine import SignalEngine
from obcash3.signals.engine_v2 import SignalEngineV2
from obcash3.signals.market_support import SignalMLAdvisor, log_market_selection, select_best_signal
from obcash3.utils.automation import DailySummaryScheduler, PendingResultResolver, PremiumAutomationManager
from obcash3.utils.dashboard import build_dashboard_metrics
from obcash3.utils.history import SignalHistoryStore
from obcash3.utils.logger import get_logger
from obcash3.utils.telegram import TelegramNotifier

logger = get_logger(__name__)


class OBCCashService:
    """Service layer for OB CASH operations."""

    def __init__(self, config_path: Optional[str] = None, enable_background_tasks: bool = True):
        self.config_manager = ConfigManager(config_path)
        self.config = self.config_manager.get()
        self.cache_manager = CacheManager()
        self.fetcher = DataFetcher(cache_manager=self.cache_manager)
        self.signal_engine = self._build_signal_engine(self.config.to_dict())
        self.notifier = TelegramNotifier.from_config(self.config)
        self.history_store = SignalHistoryStore()
        self.ml_manager = MachineLearningManager(history_store=self.history_store)
        self.ml_manager.load_model()
        self.ml_advisor = SignalMLAdvisor(self.history_store, self.config.to_dict(), ml_manager=self.ml_manager)
        self.automation_manager = PremiumAutomationManager(
            history_store=self.history_store,
            notifier=self.notifier,
            config_supplier=lambda: self.config,
        )
        self.daily_summary = DailySummaryScheduler(
            history_store=self.history_store,
            notifier=self.notifier,
            config_supplier=lambda: self.config,
        )
        self.pending_result_resolver = PendingResultResolver(
            history_store=self.history_store,
            fetcher=self.fetcher,
            config_supplier=lambda: self.config,
        )
        self.enable_background_tasks = bool(enable_background_tasks)
        self.executor = ThreadPoolExecutor(max_workers=4)

        self.start_time = datetime.now()
        self.total_scans = 0
        self.total_signals = 0
        self.last_scan: Optional[datetime] = None
        self._recent_history: Dict[str, datetime] = {}
        if self.enable_background_tasks:
            self.daily_summary.start()
            self.pending_result_resolver.start()

    def _build_signal_engine(self, config: Dict[str, Any]):
        strategy_version = str(config.get("strategy_version", "")).lower()
        use_optimized = bool(config.get("use_optimized", True))
        if strategy_version in {"original", "v1"} and not use_optimized:
            return SignalEngine(config)
        return SignalEngineV2(config)

    def get_stats(self) -> StatsResponse:
        """Get application statistics."""
        from obcash3.utils.time import now_br

        cache_hit_rate = self._cache_hit_rate()
        strong_today = 0
        average_score = 0.0
        history_path = Path(HISTORY_PATH)

        try:
            if history_path.exists():
                history = pd.read_csv(history_path)
                if not history.empty:
                    average_score = float(history["score"].mean()) if "score" in history.columns else 0.0
                    today = now_br().strftime("%Y-%m-%d")
                    today_history = history[history["timestamp"].astype(str).str.startswith(today)]
                    strong_today = len(
                        today_history[
                            (today_history["action"].isin(["COMPRA", "VENDA"]))
                            & (today_history["strength"] == "FORTE")
                        ]
                    )
        except Exception as exc:
            logger.warning("Failed to read signal history for stats: %s", exc)

        return StatsResponse(
            uptime_seconds=int((datetime.now() - self.start_time).total_seconds()),
            total_scans=self.total_scans,
            total_signals=self.total_signals,
            strong_signals_today=strong_today,
            average_score=average_score,
            last_scan=self.last_scan,
            active_pairs=list(PAIRS.keys()),
            cache_hit_rate=cache_hit_rate,
        )

    async def analyze_pair(
        self,
        pair: str,
        timeframe: str,
        send_notification: bool = False,
    ) -> SignalResponse:
        """Analyze a single pair."""
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(self.executor, self._analyze_sync, pair, timeframe)
        result = self._apply_market_support([result], log_selection=False)[0]

        self.total_scans += 1
        self.last_scan = datetime.now()
        if result.action in ("COMPRA", "VENDA"):
            self.total_signals += 1

        self._persist_signal(result)
        if send_notification and result.action in ("COMPRA", "VENDA"):
            await loop.run_in_executor(
                self.executor,
                self._send_notification_sync,
                result,
            )

        return result

    def _analyze_sync(self, pair: str, timeframe: str) -> SignalResponse:
        """Synchronous analysis executed in the thread pool."""
        df, source = self.fetcher.fetch_data(
            pair,
            timeframe,
            self.config.twelve_api_key,
            self.config.av_api_key,
            use_cache=False,
        )
        if df is None:
            raise ValueError(f"Failed to fetch data: {source}")

        resolved = self._resolve_history_with_fallback(pair, timeframe, df, source)
        if resolved > 0:
            logger.info("Resolved %d open signals for %s %s", resolved, pair, timeframe)

        profile = self.history_store.build_profile(
            asset=pair,
            interval=timeframe,
            min_resolved_trades=getattr(self.config, "min_resolved_trades", 5),
            min_win_rate=getattr(self.config, "min_win_rate", 0.52),
            min_profit_factor=getattr(self.config, "min_profit_factor", 1.05),
        )

        htf_df = None
        if self.config.mtf_confirm:
            higher_tf = HTF.get(timeframe)
            if higher_tf:
                htf_df = self.fetcher.fetch_for_mtf(
                    pair,
                    higher_tf,
                    self.config.twelve_api_key,
                    self.config.av_api_key,
                    use_cache=False,
                )

        signal = self.signal_engine.generate_signal(
            df,
            pair,
            timeframe,
            htf_df,
            market_context=profile.to_context(),
        )
        signal.source = source
        signal.policy_state = profile.policy_state
        signal.policy_notes = profile.notes
        signal.raw_data = signal.raw_data or {}
        signal.raw_data.update(
            {
                "historical_win_rate": profile.win_rate,
                "historical_profit_factor": profile.profit_factor,
                "resolved_trades": profile.resolved_trades,
                "recent_win_rate": profile.recent_win_rate,
            }
        )
        return self._signal_to_response(signal)

    def _resolve_history_with_fallback(
        self,
        pair: str,
        timeframe: str,
        market_df: pd.DataFrame,
        source: str,
    ) -> int:
        """Resolve pending trades and fallback to deeper Yahoo coverage when needed."""
        if market_df is None or market_df.empty or "Timestamp" not in market_df.columns:
            return 0

        return resolve_signal_results_with_fallback(
            pair,
            timeframe,
            market_df,
            source,
            fetcher=self.fetcher,
            store=self.history_store,
        )

    def _send_notification_sync(self, signal: SignalResponse) -> bool:
        """Send Telegram notification and persist migrated chat IDs if needed."""
        if not self.automation_manager.can_dispatch_signals():
            logger.info("Telegram paused by operational protection")
            return False
        sent = self.notifier.send_signal(signal, self._signal_key(signal))
        config_updates = self.notifier.consume_config_updates()
        if config_updates:
            updated = self.config_manager.update(**config_updates)
            if updated:
                self.config = self.config_manager.get()
                logger.info("Rotas do Telegram atualizadas automaticamente: %s", config_updates)
            else:
                logger.warning("Falha ao persistir novas rotas do Telegram: %s", config_updates)
        return sent

    def _apply_market_support(self, results: list[SignalResponse], log_selection: bool = True) -> list[SignalResponse]:
        """Apply the secondary decision layer and annotate signals in place."""
        if not results:
            return results

        self.ml_advisor.update_config(self.config.to_dict())
        self.ml_advisor.refresh()
        selection = select_best_signal(
            results,
            min_score=float(getattr(self.config, "best_signal_min_score", getattr(self.config, "min_score", 70.0))),
            ml_advisor=self.ml_advisor,
            ml_weight=float(getattr(self.config, "ml_weight", 0.30)),
            send_only_strong=bool(getattr(self.config, "send_only_strong", False)),
        )
        if log_selection:
            log_market_selection(selection)
        return [candidate.signal for candidate in selection.candidates]

    async def scan_all_pairs(
        self,
        timeframe: str,
        send_notifications: bool = True,
    ) -> ScanAllResponse:
        """Scan all configured pairs."""
        import time

        start_time = time.time()
        scan_pairs = self._scan_pairs()
        loop = asyncio.get_running_loop()
        tasks = [loop.run_in_executor(self.executor, self._analyze_sync, pair, timeframe) for pair in scan_pairs]
        raw_results: list[SignalResponse] = []

        for index in range(0, len(tasks), 5):
            batch = tasks[index:index + 5]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)
            for item in batch_results:
                if isinstance(item, Exception):
                    logger.error("Error scanning pair batch: %s", item)
                else:
                    raw_results.append(item)
            if index + 5 < len(tasks):
                await asyncio.sleep(1)

        results = self._apply_market_support(raw_results, log_selection=True)
        selection = select_best_signal(
            results,
            min_score=float(getattr(self.config, "best_signal_min_score", getattr(self.config, "min_score", 70.0))),
            ml_advisor=self.ml_advisor,
            ml_weight=float(getattr(self.config, "ml_weight", 0.30)),
            send_only_strong=bool(getattr(self.config, "send_only_strong", False)),
        )

        self.total_scans += len(scan_pairs)
        self.last_scan = datetime.now()

        best_signal = selection.best_signal
        if best_signal is not None and best_signal.action in ("COMPRA", "VENDA"):
            self.total_signals += 1
            if send_notifications:
                self._persist_signal(best_signal)
                await loop.run_in_executor(self.executor, self._send_notification_sync, best_signal)

        signals_found = sum(1 for item in results if item.action in ("COMPRA", "VENDA"))
        strong = sum(1 for item in results if item.strength == "FORTE" and item.action in ("COMPRA", "VENDA"))
        duration = time.time() - start_time

        return ScanAllResponse(
            total_pairs=len(scan_pairs),
            signals_found=signals_found,
            strong_signals=strong,
            results=results,
            scan_duration_seconds=duration,
            best_signal=best_signal,
            qualified_candidates=selection.qualified_count,
            ignored_pairs=len(selection.ignored_pairs),
            ml_backend=selection.ml_backend,
            selection_notes=selection.ignored_pairs[:12],
        )

    async def run_backtest(
        self,
        pair: str,
        timeframe: str,
        initial_balance: float = 1000.0,
        risk_percent: float = 1.0,
        **kwargs: Any,
    ) -> BacktestResponse:
        """Run backtest for a pair."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self.executor,
            self._run_backtest_sync,
            pair,
            timeframe,
            initial_balance,
            risk_percent,
        )

    def _run_backtest_sync(
        self,
        pair: str,
        timeframe: str,
        initial_balance: float,
        risk_percent: float,
    ) -> BacktestResponse:
        """Synchronous backtest execution."""
        df, source = self.fetcher.fetch_data(
            pair,
            timeframe,
            self.config.twelve_api_key,
            self.config.av_api_key,
            use_cache=False,
        )
        if df is None:
            raise ValueError(f"Failed to fetch data: {source}")

        engine_config = {
            **self.config.to_dict(),
            "account_balance": initial_balance,
            "risk_pct": risk_percent,
            "mtf_confirm": False,
            "divergence_detect": False,
        }
        engine = self._build_signal_engine(engine_config)
        backtester = Backtester(initial_balance=initial_balance, risk_pct=risk_percent)
        trades, stats = backtester.run(df, engine, pair, timeframe)

        if "error" in stats:
            raise ValueError(stats["error"])

        wins = sum(1 for trade in trades if trade.pnl > 0)
        losses = sum(1 for trade in trades if trade.pnl < 0)
        draw_trades = len(trades) - wins - losses
        avg_score = sum(trade.signal_score for trade in trades) / len(trades) if trades else 0.0
        equity_curve = [initial_balance]
        for trade in trades:
            equity_curve.append(equity_curve[-1] + trade.pnl)

        return BacktestResponse(
            pair=pair,
            timeframe=timeframe,
            total_trades=stats["total_trades"],
            wins=wins,
            losses=losses,
            draw_trades=draw_trades,
            win_rate=float(stats["win_rate"]),
            profit_factor=float(stats["profit_factor"]),
            total_pnl_pct=float(stats["total_pnl_pct"]),
            max_drawdown_pct=float(stats["max_drawdown"]),
            avg_score=avg_score,
            equity_curve=equity_curve,
        )

    def update_config(self, **kwargs: Any) -> bool:
        """Update configuration and refresh runtime dependencies."""
        success = self.config_manager.update(**kwargs)
        if not success:
            return False

        self.config = self.config_manager.get()
        self.signal_engine = self._build_signal_engine(self.config.to_dict())
        self.ml_advisor.update_config(self.config.to_dict())
        self.notifier.configure(
            token=self.config.telegram_token,
            chat_id=self.config.telegram_chat_id,
            free_chat_id=getattr(self.config, "free_telegram_chat_id", ""),
            vip_chat_id=getattr(self.config, "vip_telegram_chat_id", ""),
            enabled=self.config.telegram_enabled,
            group_tier=getattr(self.config, "group_tier", "free"),
            message_mode=self.config.message_mode,
            min_strength=self.config.telegram_min_strength,
            min_score=self.config.min_score,
            send_only_strong=self.config.send_only_strong,
            min_signal_interval_seconds=self.config.min_signal_interval_seconds,
            allowed_pairs=self.config.allowed_pairs,
            allowed_hours=self.config.allowed_hours,
        )
        self.daily_summary.reschedule()
        return True

    def get_config_dict(self) -> Dict[str, Any]:
        """Get current configuration as a dictionary."""
        self.config = self.config_manager.get()
        return self.config.to_dict()

    def get_dashboard_metrics(self) -> Dict[str, Any]:
        """Return dashboard metrics for UI and bot usage."""
        return build_dashboard_metrics(self.history_store.load_dataframe())

    def shutdown(self) -> None:
        """Cleanup resources."""
        if self.enable_background_tasks:
            self.daily_summary.stop()
            self.pending_result_resolver.stop()
        self.executor.shutdown(wait=True)

    def update_trade_result(self, signal_id: str, result_status: str, note: str = "") -> Optional[Dict[str, Any]]:
        """Manually update a trade result and trigger premium automation hooks."""
        updated = update_signal_result(signal_id, result_status, note, store=self.history_store)
        if updated:
            self.automation_manager.handle_history_update(updated)
            if self.ml_manager.should_retrain():
                logger.info(
                    "ML pronto para novo treino manual: %d registros resolvidos novos desde o ultimo modelo",
                    self.ml_manager.new_resolved_records_since_training(),
                )
        return updated

    def _cache_hit_rate(self) -> float:
        stats = self.cache_manager.stats()
        memory_stats = stats.get("memory", {})
        total_entries = float(memory_stats.get("total_entries", 0))
        valid_entries = float(memory_stats.get("valid_entries", 0))
        if total_entries <= 0:
            return 0.0
        return (valid_entries / total_entries) * 100

    def _persist_signal(self, signal: SignalResponse) -> None:
        if signal.action not in ("COMPRA", "VENDA", "EVITA"):
            return

        key = self._signal_key(signal)
        now = datetime.now()
        cutoff = now - timedelta(minutes=3)
        self._recent_history = {
            item_key: item_time
            for item_key, item_time in self._recent_history.items()
            if item_time >= cutoff
        }
        if key in self._recent_history:
            return

        if save_signal_record(signal, store=self.history_store):
            self._recent_history[key] = now

    def _scan_pairs(self) -> list[str]:
        """Return scan order honoring favorites and allowed pairs."""
        all_pairs = list(PAIRS.keys())
        allowed_pairs = list(getattr(self.config, "allowed_pairs", []) or [])
        favorite_pairs = list(getattr(self.config, "favorite_pairs", []) or [])

        if allowed_pairs:
            all_pairs = [pair for pair in all_pairs if pair in allowed_pairs]

        ordered: list[str] = []
        for pair in favorite_pairs:
            if pair in all_pairs and pair not in ordered:
                ordered.append(pair)
        for pair in all_pairs:
            if pair not in ordered:
                ordered.append(pair)
        return ordered or list(PAIRS.keys())

    def _signal_key(self, signal: SignalResponse) -> str:
        return "|".join(
            [
                signal.asset,
                signal.interval,
                signal.action,
                f"{signal.score:.1f}",
                f"{signal.price:.5f}",
            ]
        )

    def _signal_to_response(self, signal) -> SignalResponse:
        technical_score = float(getattr(signal, "technical_score", 0.0) or 0.0)
        if technical_score <= 0:
            technical_score = float(signal.score)
        decision_score = float(getattr(signal, "decision_score", 0.0) or 0.0)
        if decision_score <= 0:
            decision_score = technical_score
        return SignalResponse(
            asset=signal.asset,
            interval=signal.interval,
            timestamp=signal.timestamp,
            action=signal.action,
            strength=signal.strength,
            score=signal.score,
            price=signal.price,
            sl=signal.sl,
            tp=signal.tp,
            atr=signal.atr,
            rsi=signal.rsi,
            adx=signal.adx,
            macd_hist=signal.macd_hist,
            stoch=signal.stoch,
            bb_width=signal.bb_width,
            market_regime=signal.market_regime,
            session=signal.session,
            mtf_confirmation=signal.mtf_confirmation,
            divergence=signal.divergence,
            source=signal.source,
            confidence_score=signal.confidence_score,
            confidence_label=signal.confidence_label,
            policy_state=signal.policy_state,
            policy_notes=signal.policy_notes,
            technical_score=technical_score,
            ml_score=getattr(signal, "ml_score", 0.0),
            ml_confidence=getattr(signal, "ml_confidence", 0.0),
            ml_backend=getattr(signal, "ml_backend", "none"),
            ml_used=bool(getattr(signal, "ml_used", False)),
            decision_score=decision_score,
            selection_reason=getattr(signal, "selection_reason", ""),
            note=getattr(signal, "note", ""),
            filters=signal.filters,
            conditions_buy=signal.conditions_buy,
            conditions_sell=signal.conditions_sell,
        )
