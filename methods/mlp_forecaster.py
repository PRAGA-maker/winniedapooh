import numpy as np
from sklearn.neural_network import MLPClassifier
from typing import Any, Dict, List, Optional
from methods.base import ForecastMethod
from dataobject.io_hygiene import Batch, Example
import joblib
from pathlib import Path

class MLPForecaster(ForecastMethod):
    name = "mlp_nn"

    def __init__(self, hidden_layer_sizes=(64, 32), max_iter=500, history_len=5):
        self.model = MLPClassifier(
            hidden_layer_sizes=hidden_layer_sizes, 
            max_iter=max_iter,
            random_state=42
        )
        self.history_len = history_len
        self.is_fitted = False

    def _extract_features(self, examples: List[Example]) -> np.ndarray:
        features = []
        for ex in examples:
            # Take last history_len beliefs, pad with 0 if necessary
            h = list(ex.history_belief[-self.history_len:])
            if len(h) < self.history_len:
                h = [0.0] * (self.history_len - len(h)) + h
            features.append(h)
        return np.array(features)

    def fit(self, train_batches: List[Batch], spec: Dict[str, Any]) -> None:
        X_list = []
        y_list = []
        for batch in train_batches:
            if not batch.examples:
                continue
            X_list.append(self._extract_features(batch.examples))
            y_list.extend([float(ex.target) for ex in batch.examples])
        
        if not X_list:
            print("No training data found for MLP fit.")
            return

        X = np.vstack(X_list)
        y = np.array(y_list)
        
        # Ensure y is binary for MLPClassifier if it's meant for classification
        # In this stress test, we assume ResolveBinary task
        y = (y > 0.5).astype(int)

        unique_classes = np.unique(y)
        if len(unique_classes) < 2:
            print(f"MLP fit skipped: only one class found in training data ({unique_classes}).")
            return

        self.model.fit(X, y)
        self.is_fitted = True
        print(f"MLP fitted on {len(y)} examples.")

    def predict(self, batch: Batch, spec: Dict[str, Any]) -> List[float]:
        if not self.is_fitted or not batch.examples:
            # Fallback to last price or 0.5 if not fitted
            return [ex.history_belief[-1] if (ex.history_belief and len(ex.history_belief) > 0) else 0.5 for ex in batch.examples]
        
        X = self._extract_features(batch.examples)
        
        # Probabilities for class 1
        try:
            probs = self.model.predict_proba(X)
            if probs.shape[1] > 1:
                return probs[:, 1].tolist()
            else:
                # This should not happen if we check unique_classes during fit
                return [float(probs[0, 0])] * len(batch.examples)
        except Exception as e:
            print(f"MLP predict error: {e}. Falling back.")
            return [ex.history_belief[-1] if (ex.history_belief and len(ex.history_belief) > 0) else 0.5 for ex in batch.examples]

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

