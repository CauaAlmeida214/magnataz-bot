from __future__ import absolute_import
"""
Persisted machine learning support for market signal ranking.

The technical engine remains the primary decision maker. This manager only
provides an additional probability of WIN based on the shared signal history.
"""

import json
import pickle
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from obcash3.config.settings import BRT, ML_ENCODERS_PATH, ML_METADATA_PATH, ML_MODEL_PATH
from obcash3.data.signal_store import get_ml_ready_history
from obcash3.utils.history import SignalHistoryStore
from obcash3.utils.logger import get_logger

logger = get_logger(__name__)

try:  # pragma: no cover - optional dependency in some deployments
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
    from sklearn.model_selection import train_test_split

    HAS_SKLEARN = True
except Exception:  # pragma: no cover - graceful fallback
    RandomForestClassifier = None  # type: ignore[assignment]
    accuracy_score = None  # type: ignore[assignment]
    f1_score = None  # type: ignore[assignment]
    precision_score = None  # type: ignore[assignment]
    recall_score = None  # type: ignore[assignment]
    train_test_split = None  # type: ignore[assignment]
    HAS_SKLEARN = False


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _signal_attr(signal: Any, name: str, default: Any = None) -> Any:
    if isinstance(signal, dict):
        return signal.get(name, default)
    return getattr(signal, name, default)


def _hour_from_signal(signal: Any) -> int:
    timestamp = _signal_attr(signal, "timestamp", None)
    if timestamp is None:
        return 0
    try:
        ts = pd.Timestamp(timestamp)
        if ts.tzinfo is None:
            ts = ts.tz_localize(BRT)
        else:
            ts = ts.tz_convert(BRT)
        return int(ts.hour)
    except Exception:
        return 0


def _normalize_signal_features(signal: Any) -> Dict[str, Any]:
    technical_score = _safe_float(_signal_attr(signal, "technical_score", 0.0), 0.0)
    if technical_score <= 0:
        technical_score = _safe_float(_signal_attr(signal, "score", 0.0), 0.0)
    return {
        "asset": str(_signal_attr(signal, "asset", "") or ""),
        "timeframe": str(_signal_attr(signal, "interval", "") or ""),
        "action": str(_signal_attr(signal, "action", "") or ""),
        "hour": _hour_from_signal(signal),
        "score": technical_score,
        "strength": str(_signal_attr(signal, "strength", "") or ""),
        "confidence_label": str(_signal_attr(signal, "confidence_label", "") or ""),
        "session": str(_signal_attr(signal, "session", "") or ""),
        "market_regime": str(_signal_attr(signal, "market_regime", "") or ""),
        "source": str(_signal_attr(signal, "source", "") or ""),
        "rsi": _safe_float(_signal_attr(signal, "rsi", 0.0), 0.0),
        "adx": _safe_float(_signal_attr(signal, "adx", 0.0), 0.0),
        "stoch": _safe_float(_signal_attr(signal, "stoch", 0.0), 0.0),
        "bb_width": _safe_float(_signal_attr(signal, "bb_width", 0.0), 0.0),
        "mtf_confirmation": str(_signal_attr(signal, "mtf_confirmation", "") or ""),
        "divergence": str(_signal_attr(signal, "divergence", "") or ""),
        "price": _safe_float(_signal_attr(signal, "price", 0.0), 0.0),
        "sl": _safe_float(_signal_attr(signal, "sl", 0.0), 0.0),
        "tp": _safe_float(_signal_attr(signal, "tp", 0.0), 0.0),
    }


@dataclass
class MLPreparedDataset:
    features: pd.DataFrame
    target: pd.Series
    source_frame: pd.DataFrame
    categorical_columns: list[str]
    numeric_columns: list[str]

    @property
    def rows(self) -> int:
        return int(len(self.source_frame))


@dataclass
class MLPredictionResult:
    probability: float = 0.0
    confidence: float = 0.0
    backend: str = "none"
    available: bool = False
    sample_count: int = 0
    reason: str = ""


@dataclass
class MLTrainingReport:
    trained: bool
    backend: str = "none"
    reason: str = ""
    sample_count: int = 0
    train_rows: int = 0
    test_rows: int = 0
    accuracy: float = 0.0
    precision: float = 0.0
    recall: float = 0.0
    f1: float = 0.0
    model_path: str = ""
    encoders_path: str = ""
    metadata_path: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class MachineLearningManager:
    """Load, train and apply the persisted ML model."""

    CATEGORICAL_COLUMNS = [
        "asset",
        "timeframe",
        "action",
        "strength",
        "confidence_label",
        "session",
        "market_regime",
        "mtf_confirmation",
        "divergence",
        "source",
    ]
    NUMERIC_COLUMNS = [
        "hour",
        "score",
        "rsi",
        "adx",
        "stoch",
        "bb_width",
        "price",
        "sl",
        "tp",
    ]

    def __init__(
        self,
        history_store: Optional[SignalHistoryStore] = None,
        model_path: Path = ML_MODEL_PATH,
        encoders_path: Path = ML_ENCODERS_PATH,
        metadata_path: Path = ML_METADATA_PATH,
    ):
        self.history_store = history_store or SignalHistoryStore()
        self.model_path = Path(model_path)
        self.encoders_path = Path(encoders_path)
        self.metadata_path = Path(metadata_path)
        self.model: Any = None
        self.encoder_bundle: Dict[str, Any] = {}
        self.metadata: Dict[str, Any] = {}
        self.backend = "none"
        self._loaded_signature: tuple[float, float] | None = None
        self._missing_logged = False

    @property
    def is_available(self) -> bool:
        return self.model is not None and bool(self.encoder_bundle.get("feature_columns"))

    def prepare_ml_dataset(self) -> MLPreparedDataset:
        """Build the resolved WIN/LOSS dataset used for training."""
        history = get_ml_ready_history(self.history_store)
        if history.empty:
            return MLPreparedDataset(
                features=pd.DataFrame(),
                target=pd.Series(dtype=int),
                source_frame=history,
                categorical_columns=list(self.CATEGORICAL_COLUMNS),
                numeric_columns=list(self.NUMERIC_COLUMNS),
            )

        frame = history.copy()
        for column in self.NUMERIC_COLUMNS:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
        for column in self.CATEGORICAL_COLUMNS:
            frame[column] = frame[column].fillna("").astype(str)

        encoded = pd.get_dummies(
            frame.loc[:, self.CATEGORICAL_COLUMNS + self.NUMERIC_COLUMNS],
            columns=self.CATEGORICAL_COLUMNS,
            dtype=float,
        )
        target = frame["result_label"].astype(int)
        return MLPreparedDataset(
            features=encoded,
            target=target,
            source_frame=frame,
            categorical_columns=list(self.CATEGORICAL_COLUMNS),
            numeric_columns=list(self.NUMERIC_COLUMNS),
        )

    def train_model(self, min_samples: int = 24) -> MLTrainingReport:
        """Train and persist a RandomForest model from resolved history."""
        if not HAS_SKLEARN:
            reason = "scikit-learn nao disponivel"
            logger.warning("ML training skipped: %s", reason)
            return MLTrainingReport(trained=False, reason=reason)

        dataset = self.prepare_ml_dataset()
        sample_count = dataset.rows
        if sample_count < int(min_samples):
            reason = f"dados insuficientes para treinamento ({sample_count}/{int(min_samples)})"
            logger.info("ML training skipped: %s", reason)
            return MLTrainingReport(trained=False, reason=reason, sample_count=sample_count)

        if dataset.target.nunique() < 2:
            reason = "historico sem diversidade entre WIN e LOSS"
            logger.info("ML training skipped: %s", reason)
            return MLTrainingReport(trained=False, reason=reason, sample_count=sample_count)

        try:
            class_counts = dataset.target.value_counts()
            stratify = dataset.target if int(class_counts.min()) >= 2 else None
            test_size = 0.25 if sample_count >= 40 else 0.20
            features_train, features_test, target_train, target_test = train_test_split(
                dataset.features,
                dataset.target,
                test_size=test_size,
                random_state=42,
                stratify=stratify,
            )
        except Exception as exc:
            reason = f"falha no split do dataset: {exc}"
            logger.warning("ML training skipped: %s", reason)
            return MLTrainingReport(trained=False, reason=reason, sample_count=sample_count)

        model = RandomForestClassifier(
            n_estimators=300,
            max_depth=8,
            min_samples_leaf=2,
            random_state=42,
            class_weight="balanced_subsample",
            n_jobs=-1,
        )
        model.fit(features_train, target_train)

        predictions = model.predict(features_test)
        accuracy = float(accuracy_score(target_test, predictions))
        precision = float(precision_score(target_test, predictions, zero_division=0))
        recall = float(recall_score(target_test, predictions, zero_division=0))
        f1 = float(f1_score(target_test, predictions, zero_division=0))

        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.model_path, "wb") as model_file:
            pickle.dump(model, model_file)

        encoder_bundle = {
            "feature_columns": list(dataset.features.columns),
            "categorical_columns": list(self.CATEGORICAL_COLUMNS),
            "numeric_columns": list(self.NUMERIC_COLUMNS),
            "backend": "random_forest",
            "trained_samples": sample_count,
        }
        with open(self.encoders_path, "wb") as encoders_file:
            pickle.dump(encoder_bundle, encoders_file)

        metadata = {
            "trained_at": datetime.now(tz=BRT).isoformat(),
            "backend": "random_forest",
            "sample_count": sample_count,
            "train_rows": int(len(features_train)),
            "test_rows": int(len(features_test)),
            "win_rows": int((dataset.target == 1).sum()),
            "loss_rows": int((dataset.target == 0).sum()),
            "feature_count": int(dataset.features.shape[1]),
            "metrics": {
                "accuracy": accuracy,
                "precision": precision,
                "recall": recall,
                "f1": f1,
            },
        }
        self.metadata_path.write_text(
            json.dumps(metadata, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        self.model = model
        self.encoder_bundle = encoder_bundle
        self.metadata = metadata
        self.backend = "random_forest"
        self._loaded_signature = (
            self.model_path.stat().st_mtime,
            self.encoders_path.stat().st_mtime,
        )
        self._missing_logged = False

        logger.info(
            "ML trained with %d registros | accuracy=%.3f precision=%.3f recall=%.3f f1=%.3f",
            sample_count,
            accuracy,
            precision,
            recall,
            f1,
        )

        return MLTrainingReport(
            trained=True,
            backend="random_forest",
            reason="treino concluido",
            sample_count=sample_count,
            train_rows=int(len(features_train)),
            test_rows=int(len(features_test)),
            accuracy=accuracy,
            precision=precision,
            recall=recall,
            f1=f1,
            model_path=str(self.model_path),
            encoders_path=str(self.encoders_path),
            metadata_path=str(self.metadata_path),
        )

    def load_model(self, force_reload: bool = False) -> bool:
        """Load the persisted model and encoder artifacts if available."""
        if not self.model_path.exists() or not self.encoders_path.exists():
            self.model = None
            self.encoder_bundle = {}
            self.metadata = {}
            self.backend = "none"
            if not self._missing_logged:
                logger.info("ML model not found yet: running with technical analysis only")
                self._missing_logged = True
            return False

        signature = (
            self.model_path.stat().st_mtime,
            self.encoders_path.stat().st_mtime,
        )
        if not force_reload and self.is_available and self._loaded_signature == signature:
            return True

        try:
            with open(self.model_path, "rb") as model_file:
                self.model = pickle.load(model_file)
            with open(self.encoders_path, "rb") as encoders_file:
                self.encoder_bundle = pickle.load(encoders_file)
            if self.metadata_path.exists():
                self.metadata = json.loads(self.metadata_path.read_text(encoding="utf-8"))
            else:
                self.metadata = {}

            self.backend = str(
                self.metadata.get("backend")
                or self.encoder_bundle.get("backend")
                or "random_forest"
            )
            self._loaded_signature = signature
            self._missing_logged = False
            logger.info(
                "ML model loaded: backend=%s samples=%s",
                self.backend,
                self.metadata.get("sample_count", self.encoder_bundle.get("trained_samples", 0)),
            )
            return True
        except Exception as exc:
            self.model = None
            self.encoder_bundle = {}
            self.metadata = {}
            self.backend = "none"
            logger.warning("Failed to load ML model: %s", exc)
            return False

    def predict_win_probability(self, signal: Any) -> MLPredictionResult:
        """Predict the probability of WIN for a single signal candidate."""
        if not self.is_available:
            return MLPredictionResult(reason="modelo nao carregado")

        try:
            feature_row = pd.DataFrame([_normalize_signal_features(signal)])
            for column in self.NUMERIC_COLUMNS:
                feature_row[column] = pd.to_numeric(feature_row[column], errors="coerce").fillna(0.0)
            for column in self.CATEGORICAL_COLUMNS:
                feature_row[column] = feature_row[column].fillna("").astype(str)

            encoded = pd.get_dummies(
                feature_row.loc[:, self.CATEGORICAL_COLUMNS + self.NUMERIC_COLUMNS],
                columns=self.CATEGORICAL_COLUMNS,
                dtype=float,
            )
            feature_columns = list(self.encoder_bundle.get("feature_columns", []))
            for column in feature_columns:
                if column not in encoded.columns:
                    encoded[column] = 0.0
            encoded = encoded.reindex(columns=feature_columns, fill_value=0.0)

            probability = float(self.model.predict_proba(encoded)[0][1] * 100.0)
            metric_f1 = _safe_float(self.metadata.get("metrics", {}).get("f1", 0.0), 0.0) * 100.0
            confidence = min(99.0, 35.0 + abs(probability - 50.0) * 1.1 + (metric_f1 * 0.20))
            return MLPredictionResult(
                probability=probability,
                confidence=confidence,
                backend=self.backend,
                available=True,
                sample_count=int(self.metadata.get("sample_count", self.encoder_bundle.get("trained_samples", 0))),
                reason="modelo aplicado",
            )
        except Exception as exc:
            logger.warning("ML prediction failed: %s", exc)
            return MLPredictionResult(reason=f"falha na previsao: {exc}")

    def calculate_final_score(
        self,
        technical_score: float,
        ml_probability: float,
        ml_weight: float = 0.35,
    ) -> float:
        """Blend technical score with ML support probability."""
        tech = max(0.0, min(100.0, float(technical_score)))
        ml_prob = max(0.0, min(100.0, float(ml_probability)))
        weight = max(0.0, min(0.80, float(ml_weight)))
        return round((tech * (1.0 - weight)) + (ml_prob * weight), 1)

    def new_resolved_records_since_training(self) -> int:
        """Return how many new WIN/LOSS records were added since the last training."""
        trained_samples = int(self.metadata.get("sample_count", self.encoder_bundle.get("trained_samples", 0)) or 0)
        current_samples = int(len(get_ml_ready_history(self.history_store)))
        return max(0, current_samples - trained_samples)

    def should_retrain(self, threshold: int = 50) -> bool:
        """Simple helper for future automatic retraining routines."""
        return self.new_resolved_records_since_training() >= int(threshold)


def prepare_ml_dataset(store: Optional[SignalHistoryStore] = None) -> MLPreparedDataset:
    return MachineLearningManager(history_store=store).prepare_ml_dataset()


def train_model(
    store: Optional[SignalHistoryStore] = None,
    min_samples: int = 24,
) -> MLTrainingReport:
    return MachineLearningManager(history_store=store).train_model(min_samples=min_samples)


def load_model(store: Optional[SignalHistoryStore] = None) -> bool:
    return MachineLearningManager(history_store=store).load_model()


def predict_win_probability(
    signal: Any,
    store: Optional[SignalHistoryStore] = None,
) -> MLPredictionResult:
    manager = MachineLearningManager(history_store=store)
    manager.load_model()
    return manager.predict_win_probability(signal)


def calculate_final_score(
    technical_score: float,
    ml_probability: float,
    ml_weight: float = 0.35,
) -> float:
    return MachineLearningManager().calculate_final_score(technical_score, ml_probability, ml_weight)
