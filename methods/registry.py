from typing import Dict, Any
from methods.base import ForecastMethod
from methods.baselines.last_price import LastPriceBaseline
from methods.baselines.random_baseline import RandomBaseline
from methods.mlp_forecaster import MLPForecaster

METHODS = {
    "last_price": LastPriceBaseline,
    "random_baseline": RandomBaseline,
    "mlp_nn": MLPForecaster,
}

def build_method(name: str, params: Dict[str, Any]) -> ForecastMethod:
    if name not in METHODS:
        raise ValueError(f"Unknown method: {name}")
    return METHODS[name](**params)

