from __future__ import absolute_import
"""
Manual training entrypoint for the persisted ML model.
"""

import argparse

from obcash3.ml.ml_manager import MachineLearningManager
from obcash3.utils.history import SignalHistoryStore
from obcash3.utils.logger import setup_logging


def main() -> int:
    parser = argparse.ArgumentParser(description="Treinar modelo de machine learning do OBCash3")
    parser.add_argument("--min-samples", type=int, default=24, help="minimo de registros WIN/LOSS para treinar")
    args = parser.parse_args()

    setup_logging()
    manager = MachineLearningManager(history_store=SignalHistoryStore())
    report = manager.train_model(min_samples=args.min_samples)

    if not report.trained:
        print(f"Treino nao executado: {report.reason}")
        return 0

    print(f"Modelo treinado com {report.sample_count} registros")
    print(f"Accuracy: {report.accuracy:.3f}")
    print(f"Precision: {report.precision:.3f}")
    print(f"Recall: {report.recall:.3f}")
    print(f"F1-score: {report.f1:.3f}")
    print(f"Modelo salvo em: {report.model_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
