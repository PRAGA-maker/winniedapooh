from typing import Dict, Any
from methods.base import ForecastMethod
from methods.baselines.last_price import LastPriceBaseline
from methods.baselines.random_baseline import RandomBaseline
from methods.mlp_forecaster import MLPForecaster
from methods.rlm_forecaster import RLMForecaster
from methods.rlm_no_market import RLMNoMarketForecaster
from methods.full_recursive_forecaster import FullRecursiveForecaster
from methods.neurallambda_forecaster import NeuralLambdaForecaster

METHODS = {
    "last_price": LastPriceBaseline,
    "random_baseline": RandomBaseline,
    "mlp_nn": MLPForecaster,
    "rlm": RLMForecaster,
    "rlm-no-market": RLMNoMarketForecaster,  # Ablation: RLM without current market prices
    "full_recursive": FullRecursiveForecaster,
    "neurallambda": NeuralLambdaForecaster,
}

def build_method(name: str, params: Dict[str, Any]) -> ForecastMethod:
    if name not in METHODS:
        raise ValueError(f"Unknown method: {name}")

    # Auto-inject parquet_path and diagnostic_mode for RLM methods
    if name in ["rlm", "rlm-no-market"]:
        params = params.copy()
        if "parquet_path" not in params:
            # Try to find the most recent dataset
            from pathlib import Path
            data_dir = Path("data/datasets")
            if data_dir.exists():
                datasets = sorted(list(data_dir.glob("v*_unified")))
                if datasets:
                    params["parquet_path"] = str(datasets[-1] / "data.parquet")
        # Always enable diagnostics for debugging
        if "diagnostic_mode" not in params:
            params["diagnostic_mode"] = True

    return METHODS[name](**params)

