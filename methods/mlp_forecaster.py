import numpy as np
from sklearn.neural_network import MLPRegressor
from typing import Any, Dict, List, Optional
from methods.base import ForecastMethod
from dataobject.io_hygiene import Batch, Example
import joblib
from pathlib import Path

class MLPForecaster(ForecastMethod):
    name = "mlp_nn"

    def __init__(self, hidden_layer_sizes=(64, 32), max_iter=500, history_len=5, max_options: Optional[int] = None):
        self.model = MLPRegressor(
            hidden_layer_sizes=hidden_layer_sizes, 
            max_iter=max_iter,
            random_state=42
        )
        self.history_len = history_len
        self.max_options = max_options
        self.is_fitted = False

    def _infer_max_options(self, examples: List[Example], allow_expand: bool = True) -> None:
        if not examples:
            return
        max_len = max(len(ex.options) for ex in examples)
        if self.max_options is None:
            self.max_options = max_len
        elif allow_expand and self.max_options < max_len:
            self.max_options = max_len

    def _extract_features(self, examples: List[Example]) -> np.ndarray:
        features = []
        for ex in examples:
            option_vectors = []
            for option in ex.options[: self.max_options]:
                h = list(option.history_belief[-self.history_len:])
                if len(h) < self.history_len:
                    h = [0.0] * (self.history_len - len(h)) + h
                option_vectors.extend(h)
            # Pad missing options with zeros
            missing = self.max_options - len(ex.options)
            if missing > 0:
                option_vectors.extend([0.0] * (missing * self.history_len))
            features.append(option_vectors)
        return np.array(features)

    def fit(self, train_batches: List[Batch], spec: Dict[str, Any]) -> None:
        X_list = []
        y_list = []
        for batch in train_batches:
            if not batch.examples:
                continue
            self._infer_max_options(batch.examples, allow_expand=True)
            X_list.append(self._extract_features(batch.examples))
            for ex in batch.examples:
                target = list(ex.target)
                if len(target) < self.max_options:
                    target = target + [0.0] * (self.max_options - len(target))
                y_list.append(target)
        
        if not X_list:
            print("No training data found for MLP fit.")
            return

        X = np.vstack(X_list)
        y = np.array(y_list)
        self.model.fit(X, y)
        self.is_fitted = True
        print(f"MLP fitted on {len(y)} examples.")

    def _normalize(self, values: List[float]) -> List[float]:
        total = sum(values)
        if total <= 0:
            return [1.0 / len(values) for _ in values]
        return [v / total for v in values]

    def _fallback_last_price(self, batch: Batch) -> List[List[float]]:
        preds = []
        for ex in batch.examples:
            option_scores = []
            for option in ex.options:
                option_scores.append(option.history_belief[-1] if option.history_belief else 0.0)
            preds.append(self._normalize(option_scores))
        return preds

    def predict(self, batch: Batch, spec: Dict[str, Any]) -> List[List[float]]:
        if not self.is_fitted or not batch.examples:
            return self._fallback_last_price(batch)

        self._infer_max_options(batch.examples, allow_expand=False)
        X = self._extract_features(batch.examples)

        try:
            raw_preds = self.model.predict(X)
            if raw_preds.ndim == 1:
                raw_preds = raw_preds.reshape(-1, 1)
            preds = []
            for ex, row in zip(batch.examples, raw_preds):
                option_count = len(ex.options)
                scores = [max(0.0, float(v)) for v in row[:option_count]]
                preds.append(self._normalize(scores))
            return preds
        except Exception as e:
            print(f"MLP predict error: {e}. Falling back.")
            return self._fallback_last_price(batch)

    def save(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self.model, path)
        print(f"MLP saved to {path}")

    @classmethod
    def load(cls, path: str) -> "MLPForecaster":
        instance = cls()
        instance.model = joblib.load(path)
        instance.is_fitted = True
        return instance

# --- LESSONS LEARNED ---
# 1. Variable options: pad to max_options for a fixed-width regressor.
# 2. Inference truncation: cap to training max_options to avoid shape drift.
# 3. Normalize outputs to keep valid distributions.

