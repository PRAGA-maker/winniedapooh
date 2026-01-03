from typing import Dict, Any
from methods.base import ForecastMethod
from methods.baselines.last_price import LastPriceBaseline

METHODS = {
    "last_price": LastPriceBaseline,
}

def build_method(name: str, params: Dict[str, Any]) -> ForecastMethod:
    if name not in METHODS:
        raise ValueError(f"Unknown method: {name}")
    return METHODS[name](**params)

