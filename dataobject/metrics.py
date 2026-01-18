import math
from typing import List


def _normalize(values: List[float]) -> List[float]:
    if not values:
        return []
    total = sum(values)
    if total <= 0:
        return [1.0 / len(values) for _ in values]
    return [v / total for v in values]


def multiclass_brier(y_true: List[List[float]], y_pred: List[List[float]]) -> float:
    if len(y_true) == 0:
        return float("nan")
    total = 0.0
    for true_vec, pred_vec in zip(y_true, y_pred):
        if len(true_vec) != len(pred_vec):
            raise ValueError("Brier score requires matching vector lengths per example.")
        pred_norm = _normalize([max(0.0, float(v)) for v in pred_vec])
        if not pred_norm:
            raise ValueError("Brier score requires non-empty prediction vectors.")
        true_vals = [float(v) for v in true_vec]
        total += sum((t - p) ** 2 for t, p in zip(true_vals, pred_norm))
    return total / len(y_true)


def multiclass_logloss(y_true: List[List[float]], y_pred: List[List[float]], eps: float = 1e-15) -> float:
    if len(y_true) == 0:
        return float("nan")
    total = 0.0
    for true_vec, pred_vec in zip(y_true, y_pred):
        if len(true_vec) != len(pred_vec):
            raise ValueError("Logloss requires matching vector lengths per example.")
        pred_norm = _normalize([max(0.0, float(v)) for v in pred_vec])
        if not pred_norm:
            raise ValueError("Logloss requires non-empty prediction vectors.")
        loss = 0.0
        for t, p in zip(true_vec, pred_norm):
            p_clipped = min(max(p, eps), 1.0 - eps)
            loss += float(t) * math.log(p_clipped)
        total += -loss
    return total / len(y_true)

# --- LESSONS LEARNED ---
# 1. Normalize per-example predictions before scoring.
# 2. Enforce vector length match to catch model/schema drift early.
