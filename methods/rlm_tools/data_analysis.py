"""
Data analysis tools for RLM forecaster.
Provides trend analysis and base rate computation for market data.
"""
from typing import List, Dict, Any, Optional
import numpy as np


def analyze_trend(history_belief: List[float]) -> Dict[str, Any]:
    """
    Analyze trend in belief history.
    
    Args:
        history_belief: List of belief values over time
        
    Returns:
        Dict with slope, volatility, last_value, mean, min, max, length
    """
    if not history_belief:
        return {
            "slope": 0.0,
            "volatility": 0.0,
            "last_value": 0.0,
            "mean": 0.0,
            "min": 0.0,
            "max": 0.0,
            "length": 0
        }
    
    arr = np.array(history_belief, dtype=float)
    
    # Calculate slope via linear regression
    if len(arr) >= 2:
        x = np.arange(len(arr))
        slope = np.polyfit(x, arr, 1)[0]
    else:
        slope = 0.0
    
    # Calculate volatility (std of returns)
    if len(arr) >= 2:
        returns = np.diff(arr)
        volatility = float(np.std(returns))
    else:
        volatility = 0.0
    
    return {
        "slope": float(slope),
        "volatility": volatility,
        "last_value": float(arr[-1]),
        "mean": float(np.mean(arr)),
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
        "length": len(arr)
    }


def compute_base_rate(resolved_outcomes: List[bool]) -> Dict[str, Any]:
    """
    Compute base rate from resolved market outcomes.
    
    Args:
        resolved_outcomes: List of boolean outcomes (True = yes, False = no)
        
    Returns:
        Dict with rate, count, confidence
    """
    if not resolved_outcomes:
        return {
            "rate": 0.5,  # Prior when no data
            "count": 0,
            "confidence": "none"
        }
    
    yes_count = sum(resolved_outcomes)
    total = len(resolved_outcomes)
    rate = yes_count / total
    
    # Simple confidence based on sample size
    if total >= 20:
        confidence = "high"
    elif total >= 5:
        confidence = "medium"
    else:
        confidence = "low"
    
    return {
        "rate": rate,
        "count": total,
        "yes_count": yes_count,
        "confidence": confidence
    }


def summarize_options(options_data: List[Dict[str, Any]]) -> str:
    """
    Create a human-readable summary of option data.
    
    Args:
        options_data: List of option dicts with history_belief, title, etc.
        
    Returns:
        Formatted string summary
    """
    lines = []
    for i, opt in enumerate(options_data):
        title = opt.get("title", f"Option {i+1}")
        belief = opt.get("history_belief", [])
        trend = analyze_trend(belief)
        
        lines.append(
            f"  [{i+1}] {title}: "
            f"last={trend['last_value']:.2%}, "
            f"mean={trend['mean']:.2%}, "
            f"slope={trend['slope']:+.4f}, "
            f"vol={trend['volatility']:.4f}"
        )
    
    return "\n".join(lines)


# --- LESSONS LEARNED ---
# 1. np.polyfit is fast for simple slope calculation - no need for statsmodels.
# 2. Volatility of returns (not levels) is more meaningful for prediction markets.
# 3. Base rate confidence depends heavily on sample size - document this clearly.
