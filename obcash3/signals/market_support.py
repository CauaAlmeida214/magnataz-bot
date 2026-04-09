from __future__ import absolute_import
"""
Market-wide signal selection and persisted ML/historical decision support.

The technical engine remains the primary decision layer. This module adds a
secondary layer that can rank opportunities using either:
1. A persisted machine learning model trained from the shared history store
2. A historical pattern prior derived from resolved WIN/LOSS records
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from obcash3.ml.ml_manager import MachineLearningManager
from obcash3.utils.history import RESULT_LOSS, RESULT_WIN, ensure_history_schema
from obcash3.utils.logger import get_logger

logger = get_logger(__name__)

ENTRY_ACTIONS = {"COMPRA", "VENDA"}


@dataclass
class MLPrediction:
    probability: float = 0.0
    confidence: float = 0.0
    backend: str = "none"
    used: bool = False
    sample_count: int = 0
    reason: str = ""


@dataclass
class SignalCandidate:
    signal: Any
    technical_score: float
    ml_prediction: MLPrediction
    decision_score: float
    qualifies: bool
    discard_reason: str
    selection_reason: str
    rank_key: Tuple[float, ...]


@dataclass
class MarketScanSelection:
    analyzed_pairs: int
    ignored_pairs: List[str] = field(default_factory=list)
    candidates: List[SignalCandidate] = field(default_factory=list)
    best_candidate: Optional[SignalCandidate] = None
    qualified_count: int = 0
    ml_backend: str = "none"

    @property
    def best_signal(self) -> Any:
        return self.best_candidate.signal if self.best_candidate else None


def _signal_attr(signal: Any, name: str, default: Any = None) -> Any:
    if signal is None:
        return default
    if isinstance(signal, dict):
        return signal.get(name, default)
    return getattr(signal, name, default)


def _set_signal_attr(signal: Any, name: str, value: Any) -> None:
    if isinstance(signal, dict):
        signal[name] = value
    else:
        setattr(signal, name, value)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _score_bucket(score: Any) -> str:
    value = _safe_float(score, 0.0)
    if value >= 85:
        return "85+"
    if value >= 75:
        return "75-84"
    if value >= 65:
        return "65-74"
    if value >= 55:
        return "55-64"
    return "0-54"


def _strength_rank(value: Any) -> int:
    return {
        "FORTE": 3,
        "MODERADO": 2,
        "FRACO": 1,
        "NEUTRO": 0,
    }.get(str(value or "").upper(), 0)


def _regime_rank(value: Any) -> int:
    return {
        "TENDENCIA": 3,
        "NORMAL": 2,
        "LATERAL": 1,
    }.get(str(value or "").upper(), 0)


def _mtf_rank(value: Any) -> int:
    return {
        "AGREE": 2,
        "BULLISH": 2,
        "BEARISH": 2,
        "NEUTRAL": 1,
        "DISAGREE": 0,
    }.get(str(value or "").upper(), 1)


def _policy_rank(value: Any) -> int:
    return {
        "BOOST": 3,
        "NEUTRAL": 2,
        "CAUTION": 1,
        "LEARNING": 1,
        "BLOCKED": 0,
    }.get(str(value or "").upper(), 1)


def _normalize_hour(timestamp_text: Any, time_text: Any) -> str:
    ts = pd.to_datetime(timestamp_text, errors="coerce")
    if pd.notna(ts):
        return ts.strftime("%H")
    raw_time = str(time_text or "").strip()
    return raw_time[:2] if len(raw_time) >= 2 else "--"


def _signal_feature_dict(signal: Any) -> Dict[str, Any]:
    price = _safe_float(_signal_attr(signal, "price", 0.0), 0.0)
    sl = _safe_float(_signal_attr(signal, "sl", 0.0), 0.0)
    tp = _safe_float(_signal_attr(signal, "tp", 0.0), 0.0)
    risk_pct = (abs(price - sl) / abs(price) * 100.0) if price and sl else 0.0
    reward_pct = (abs(tp - price) / abs(price) * 100.0) if price and tp else 0.0
    timestamp = _signal_attr(signal, "timestamp", "")
    time_text = ""
    if hasattr(timestamp, "strftime"):
        time_text = timestamp.strftime("%H:%M:%S")
    return {
        "asset": str(_signal_attr(signal, "asset", "")),
        "interval": str(_signal_attr(signal, "interval", "")),
        "action": str(_signal_attr(signal, "action", "")),
        "strength": str(_signal_attr(signal, "strength", "")),
        "score": _safe_float(_signal_attr(signal, "technical_score", _signal_attr(signal, "score", 0.0)), 0.0),
        "confidence_label": str(_signal_attr(signal, "confidence_label", "")),
        "rsi": _safe_float(_signal_attr(signal, "rsi", 0.0), 0.0),
        "adx": _safe_float(_signal_attr(signal, "adx", 0.0), 0.0),
        "market_regime": str(_signal_attr(signal, "market_regime", "")),
        "session": str(_signal_attr(signal, "session", "")),
        "mtf_confirmation": str(_signal_attr(signal, "mtf_confirmation", "")),
        "divergence": str(_signal_attr(signal, "divergence", "")),
        "risk_pct": risk_pct,
        "reward_pct": reward_pct,
        "hour": _normalize_hour(timestamp, time_text),
        "score_bucket": _score_bucket(_signal_attr(signal, "technical_score", _signal_attr(signal, "score", 0.0))),
    }


def _history_training_frame(history_df: pd.DataFrame) -> pd.DataFrame:
    normalized = ensure_history_schema(history_df)
    entries = normalized[
        normalized["action"].isin(sorted(ENTRY_ACTIONS))
        & normalized["result_status"].isin([RESULT_WIN, RESULT_LOSS])
    ].copy()
    if entries.empty:
        return pd.DataFrame()

    entries["label"] = (entries["result_status"] == RESULT_WIN).astype(int)
    entries["score"] = pd.to_numeric(entries["technical_score"], errors="coerce").where(
        pd.to_numeric(entries["technical_score"], errors="coerce") > 0,
        pd.to_numeric(entries["score"], errors="coerce"),
    ).fillna(0.0)
    entries["rsi"] = pd.to_numeric(entries["rsi"], errors="coerce").fillna(0.0)
    entries["adx"] = pd.to_numeric(entries["adx"], errors="coerce").fillna(0.0)
    entries["price"] = pd.to_numeric(entries["price"], errors="coerce").fillna(0.0)
    entries["sl"] = pd.to_numeric(entries["sl"], errors="coerce").fillna(0.0)
    entries["tp"] = pd.to_numeric(entries["tp"], errors="coerce").fillna(0.0)
    entries["risk_pct"] = ((entries["price"] - entries["sl"]).abs() / entries["price"].abs().replace(0, pd.NA) * 100.0).fillna(0.0)
    entries["reward_pct"] = ((entries["tp"] - entries["price"]).abs() / entries["price"].abs().replace(0, pd.NA) * 100.0).fillna(0.0)
    entries["hour"] = entries.apply(lambda row: _normalize_hour(row.get("timestamp", ""), row.get("time", "")), axis=1)
    entries["score_bucket"] = entries["score"].map(_score_bucket)
    return entries


class SignalMLAdvisor:
    """Optional ML/historical support layer for signal ranking."""

    def __init__(
        self,
        history_store,
        config: Optional[Dict[str, Any]] = None,
        ml_manager: Optional[MachineLearningManager] = None,
    ):
        self.history_store = history_store
        self.config = config or {}
        self.enabled = bool(self.config.get("ml_enabled", True))
        self.min_samples = int(self.config.get("ml_min_samples", 24) or 24)
        self.ml_manager = ml_manager or MachineLearningManager(history_store=history_store)
        self._training_frame = pd.DataFrame()
        self.backend = "none"
        self._global_win_rate = 50.0
        self._sample_count = 0
        self._last_reason = "ML desativado"

    def update_config(self, config: Optional[Dict[str, Any]]) -> None:
        self.config = config or {}
        self.enabled = bool(self.config.get("ml_enabled", True))
        self.min_samples = int(self.config.get("ml_min_samples", 24) or 24)
        self.backend = "none"
        self._last_reason = "ML aguardando treino"
        self.ml_manager.load_model(force_reload=True)

    def refresh(self) -> None:
        """Refresh the support backend from persisted history."""
        self.backend = "none"
        self._last_reason = "ML indisponivel"
        self._training_frame = _history_training_frame(self.history_store.load_dataframe())
        self._sample_count = int(len(self._training_frame))

        if not self.enabled:
            self._last_reason = "ML desativado em configuracao"
            return

        if self.ml_manager.load_model():
            self.backend = self.ml_manager.backend
            self._sample_count = int(
                self.ml_manager.metadata.get(
                    "sample_count",
                    self.ml_manager.encoder_bundle.get("trained_samples", len(self._training_frame)),
                )
            )
            self._last_reason = f"Modelo persistido carregado com {self._sample_count} amostras"
            return

        if self._training_frame.empty:
            self._last_reason = "Sem historico resolvido"
            return

        self._global_win_rate = float(self._training_frame["label"].mean() * 100.0)

        if len(self._training_frame) >= max(8, self.min_samples // 2):
            self.backend = "historical_prior"
            self._last_reason = f"Usando historico estatistico com {len(self._training_frame)} amostras"
        else:
            self._last_reason = f"Historico insuficiente para apoio ({len(self._training_frame)} amostras)"

    def predict(self, signal: Any) -> MLPrediction:
        """Estimate a support probability for a candidate signal."""
        if not self.enabled:
            return MLPrediction(reason="ML desativado")

        if self.ml_manager.is_available:
            try:
                runtime_prediction = self.ml_manager.predict_win_probability(signal)
                return MLPrediction(
                    probability=runtime_prediction.probability,
                    confidence=runtime_prediction.confidence,
                    backend=runtime_prediction.backend,
                    used=runtime_prediction.available,
                    sample_count=runtime_prediction.sample_count,
                    reason=runtime_prediction.reason,
                )
            except Exception as exc:
                logger.warning("Falha ao aplicar modelo ML persistido: %s", exc)

        if self._training_frame.empty:
            return MLPrediction(reason="Sem historico resolvido")

        if self.backend == "historical_prior":
            return self._historical_prior(signal)

        return MLPrediction(
            probability=self._global_win_rate,
            confidence=0.0,
            backend="none",
            used=False,
            sample_count=self._sample_count,
            reason=self._last_reason,
        )

    def calculate_final_score(self, technical_score: float, ml_probability: float, ml_weight: float = 0.35) -> float:
        """Blend technical and ML support scores."""
        return self.ml_manager.calculate_final_score(technical_score, ml_probability, ml_weight)

    def _historical_prior(self, signal: Any) -> MLPrediction:
        features = _signal_feature_dict(signal)
        action = features["action"]
        interval = features["interval"]
        asset = features["asset"]
        score_bucket = features["score_bucket"]
        decisive = self._training_frame
        if decisive.empty:
            return MLPrediction(reason="Sem historico para apoio estatistico")

        pieces: List[Tuple[float, float, int]] = []

        def _collect(mask, base_weight: float) -> None:
            subset = decisive.loc[mask].copy()
            total = len(subset)
            if total <= 0:
                return
            wins = int(subset["label"].sum())
            probability = ((wins + 1) / (total + 2)) * 100.0
            evidence = min(1.0, total / 12.0)
            pieces.append((probability, base_weight * evidence, total))

        _collect((decisive["interval"] == interval) & (decisive["action"] == action), 0.22)
        _collect((decisive["asset"] == asset) & (decisive["interval"] == interval) & (decisive["action"] == action), 0.32)
        _collect((decisive["interval"] == interval) & (decisive["session"] == features["session"]) & (decisive["action"] == action), 0.14)
        _collect((decisive["interval"] == interval) & (decisive["market_regime"] == features["market_regime"]), 0.10)
        _collect((decisive["interval"] == interval) & (decisive["score_bucket"] == score_bucket), 0.12)
        _collect((decisive["confidence_label"] == features["confidence_label"]) & (decisive["action"] == action), 0.10)

        if not pieces:
            return MLPrediction(
                probability=self._global_win_rate,
                confidence=0.0,
                backend="none",
                used=False,
                sample_count=self._sample_count,
                reason="Sem recortes historicos semelhantes",
            )

        total_weight = sum(weight for _, weight, _ in pieces)
        weighted_probability = sum(prob * weight for prob, weight, _ in pieces) / max(total_weight, 1e-9)
        evidence_samples = sum(total for _, _, total in pieces)
        confidence = min(95.0, 18.0 + min(70.0, evidence_samples * 2.5))
        return MLPrediction(
            probability=weighted_probability,
            confidence=confidence,
            backend="historical_prior",
            used=True,
            sample_count=evidence_samples,
            reason=self._last_reason,
        )


def select_best_signal(
    signals: Iterable[Any],
    min_score: float = 70.0,
    ml_advisor: Optional[SignalMLAdvisor] = None,
    ml_weight: float = 0.35,
    send_only_strong: bool = False,
) -> MarketScanSelection:
    """Select the single best market-wide signal above the quality floor."""
    candidates: List[SignalCandidate] = []
    ignored_pairs: List[str] = []
    backend = "none"

    for signal in signals:
        technical_score = _safe_float(_signal_attr(signal, "technical_score", 0.0), 0.0)
        if technical_score <= 0:
            technical_score = _safe_float(_signal_attr(signal, "score", 0.0), 0.0)
        action = str(_signal_attr(signal, "action", "")).upper()
        strength = str(_signal_attr(signal, "strength", "")).upper()

        prediction = ml_advisor.predict(signal) if ml_advisor else MLPrediction(reason="ML nao configurado")
        backend = prediction.backend if prediction.used else backend
        decision_score = technical_score
        if prediction.used and prediction.backend != "none":
            if ml_advisor is not None:
                decision_score = ml_advisor.calculate_final_score(technical_score, prediction.probability, ml_weight)
            else:
                decision_score = (technical_score * (1.0 - ml_weight)) + (prediction.probability * ml_weight)

        qualifies = True
        discard_reason = ""
        if action not in ENTRY_ACTIONS:
            qualifies = False
            discard_reason = f"acao {action or 'N/A'} nao operavel"
        elif technical_score < float(min_score):
            qualifies = False
            discard_reason = f"score tecnico abaixo de {float(min_score):.1f}%"
        elif prediction.used and decision_score < float(min_score):
            qualifies = False
            discard_reason = f"score final abaixo de {float(min_score):.1f}%"
        elif send_only_strong and strength != "FORTE":
            qualifies = False
            discard_reason = "forca abaixo do filtro atual"

        selection_reason = (
            f"tecnico {technical_score:.1f}%"
            + (
                f" + {prediction.backend} {prediction.probability:.1f}%"
                if prediction.used and prediction.backend != "none"
                else ""
            )
        )
        rank_key = (
            decision_score,
            technical_score,
            _strength_rank(strength),
            _safe_float(_signal_attr(signal, "confidence_score", 0.0), 0.0),
            _mtf_rank(_signal_attr(signal, "mtf_confirmation", "")),
            _regime_rank(_signal_attr(signal, "market_regime", "")),
            _policy_rank(_signal_attr(signal, "policy_state", "")),
        )

        _set_signal_attr(signal, "technical_score", round(technical_score, 1))
        _set_signal_attr(signal, "ml_score", round(prediction.probability, 1))
        _set_signal_attr(signal, "ml_confidence", round(prediction.confidence, 1))
        _set_signal_attr(signal, "ml_backend", prediction.backend)
        _set_signal_attr(signal, "ml_used", bool(prediction.used))
        _set_signal_attr(signal, "decision_score", round(decision_score, 1))
        _set_signal_attr(signal, "selection_reason", selection_reason if qualifies else discard_reason)

        candidates.append(
            SignalCandidate(
                signal=signal,
                technical_score=technical_score,
                ml_prediction=prediction,
                decision_score=decision_score,
                qualifies=qualifies,
                discard_reason=discard_reason,
                selection_reason=selection_reason,
                rank_key=rank_key,
            )
        )

        if not qualifies:
            ignored_pairs.append(f"{_signal_attr(signal, 'asset', '-')} -> {discard_reason}")

    qualified = [candidate for candidate in candidates if candidate.qualifies]
    qualified.sort(key=lambda item: item.rank_key, reverse=True)
    best_candidate = qualified[0] if qualified else None

    if best_candidate is not None:
        _set_signal_attr(best_candidate.signal, "selection_reason", best_candidate.selection_reason)

    return MarketScanSelection(
        analyzed_pairs=len(candidates),
        ignored_pairs=ignored_pairs,
        candidates=candidates,
        best_candidate=best_candidate,
        qualified_count=len(qualified),
        ml_backend=backend if backend != "none" else (ml_advisor.backend if ml_advisor else "none"),
    )


def log_market_selection(selection: MarketScanSelection) -> None:
    """Emit concise debug logs for the global market scan."""
    logger.info(
        "Scanner global: %d pares analisados | %d qualificados | %d ignorados | backend=%s",
        selection.analyzed_pairs,
        selection.qualified_count,
        len(selection.ignored_pairs),
        selection.ml_backend,
    )
    for candidate in selection.candidates:
        signal = candidate.signal
        logger.info(
            "Par %s %s -> action=%s tech=%.1f final=%.1f ml=%s %.1f discard=%s",
            _signal_attr(signal, "asset", "-"),
            _signal_attr(signal, "interval", "-"),
            _signal_attr(signal, "action", "-"),
            candidate.technical_score,
            candidate.decision_score,
            candidate.ml_prediction.backend,
            candidate.ml_prediction.probability,
            candidate.discard_reason or "-",
        )

    if selection.best_candidate is None:
        logger.info("Scanner global: nenhuma oportunidade qualificada acima do piso")
        return

    best = selection.best_candidate
    logger.info(
        "Melhor sinal: %s %s | action=%s | tech=%.1f | final=%.1f | backend=%s | motivo=%s",
        _signal_attr(best.signal, "asset", "-"),
        _signal_attr(best.signal, "interval", "-"),
        _signal_attr(best.signal, "action", "-"),
        best.technical_score,
        best.decision_score,
        best.ml_prediction.backend,
        best.selection_reason,
    )
