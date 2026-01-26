"""
Data format conversion: Example → neurallambda's prepared_data format.

Based on neurallambda's column-batched training format from t14_homoiconic_llm_05.py.
"""

from forecasting.dataclasses import Example, OptionHistory
from typing import List, Dict, Any


def create_empty_lor_ixs(num_layers: int = 24) -> Dict[str, List]:
    """Create empty LoR indices (no parsing) for a block."""
    return {
        'lor_gs': [None] * num_layers,
        'lor_us': [None] * num_layers,
        'lor_ds': [None] * num_layers,
    }


def create_parse_lor_ixs(num_layers: int = 24, target_layer: int = 14) -> Dict[str, List]:
    """
    Create LoR indices with parsing at target_layer.

    For MLP-only (WHICH_LOR=2), we parse G, U, D at specific indices.
    Format: (left_singular_ix, right_singular_ix) within the lor_block token sequence.

    LoR block format: "^@G^@|^@|^@U^@|^@|^@D^@|^@|"
    Indices:           0  1  2  3  4  5  6  7  8
    """
    lor_ixs = create_empty_lor_ixs(num_layers)
    lor_ixs['lor_gs'][target_layer] = (0, 1)  # G: indices 0, 1
    lor_ixs['lor_us'][target_layer] = (3, 4)  # U: indices 3, 4
    lor_ixs['lor_ds'][target_layer] = (6, 7)  # D: indices 6, 7
    return lor_ixs


def format_belief_history(option: OptionHistory, max_points: int = 5) -> str:
    """
    Convert belief history to compact text representation.

    Uses structured format (most token-efficient):
    "History: t1=0.50, t2=0.55, t3=0.60, t4=0.65, t5=0.70"
    """
    if not option.history_belief:
        return "No prior beliefs"

    recent_beliefs = option.history_belief[-max_points:]

    formatted = "History: " + ", ".join([
        f"t{i+1}={belief:.2f}" for i, belief in enumerate(recent_beliefs)
    ])

    return formatted


def format_market_context(example: Example) -> str:
    """
    Format market metadata as text.

    Example output:
    "Event: KXDRUGPRICEOZEMPIC | Source: kalshi | Options: 2 | Choices: Yes, No"
    """
    parts = [
        f"Event: {example.event_id}",
        f"Source: {example.source}",
        f"Options: {len(example.options)}",
    ]

    option_titles = [opt.title for opt in example.options]
    # Truncate long titles
    option_titles = [title[:50] + "..." if len(title) > 50 else title for title in option_titles]
    parts.append(f"Choices: {', '.join(option_titles)}")

    return " | ".join(parts)


def example_to_neurallambda_format(
    example: Example,
    num_layers: int = 24,
    target_layer: int = 14,
    version: str = "v1",
    max_history_points: int = 5,
) -> Dict[str, Any]:
    """
    Convert Example → neurallambda prepared_data format.

    Args:
        example: Forecasting example with market data and belief history
        num_layers: Number of transformer layers (default 24 for Qwen2-0.5B)
        target_layer: Which layer to apply LoR updates (default 14, middle layer)
        version: "v1" (single LoR) or "v2" (curriculum with LoR after each update)
        max_history_points: Number of recent history points to include

    Returns:
        Dictionary with key "prepared_data" containing list of blocks.

    Format for each block:
        {
            "type": "text" | "lor" | "pad_block",
            "content": <text content>,
            "include_in_loss": <bool>,
            "lor_gs": [None, None, ..., (left_ix, right_ix) or None, ...],
            "lor_us": [...],
            "lor_ds": [...],
            "loss_mask": <optional list of bool> (for LoR blocks only)
        }
    """
    if version == "v1":
        return _example_to_neurallambda_v1(example, num_layers, target_layer, max_history_points)
    elif version == "v2":
        return _example_to_neurallambda_v2(example, num_layers, target_layer, max_history_points)
    else:
        raise ValueError(f"Unknown version: {version}. Use 'v1' or 'v2'.")


def _example_to_neurallambda_v1(
    example: Example,
    num_layers: int,
    target_layer: int,
    max_history_points: int,
) -> Dict[str, Any]:
    """
    Version 1: Single LoR block after all context.

    Format:
    1. Market context (text, not in loss)
    2. Belief history (text, not in loss)
    3. LoR block (with parsing, in loss but meta-tokens masked)
    4. Target (text, in loss)

    Total: 4 blocks
    """
    empty_lors = create_empty_lor_ixs(num_layers)
    parse_lors = create_parse_lor_ixs(num_layers, target_layer)

    # LoR block for MLP-only (WHICH_LOR=2)
    # Meta-tokens: ^@G, ^@U, ^@D, ^@| (must be added to tokenizer vocabulary)
    lor_block = "^@G^@|^@|^@U^@|^@|^@D^@|^@|"
    lor_mask = [0, 0, 0] * 3  # Don't learn to predict meta-tokens (per neurallambda)

    prepared_data = []

    # 1. Market context
    context_text = format_market_context(example)
    prepared_data.append({
        "type": "text",
        "content": context_text,
        "include_in_loss": False,
        **empty_lors
    })

    # 2. Belief history (for binary/simple, use first option; multi-option needs more thought)
    # TODO: For multi-option markets, consider aggregating or using primary option
    history_text = format_belief_history(example.options[0], max_history_points)
    prepared_data.append({
        "type": "text",
        "content": history_text,
        "include_in_loss": False,
        **empty_lors
    })

    # 3. LoR block (model emits weight updates here)
    prepared_data.append({
        "type": "lor",
        "content": lor_block,
        "include_in_loss": True,
        "loss_mask": lor_mask,
        **parse_lors
    })

    # 4. Target (what we're predicting)
    # Target is always a list of probabilities
    if isinstance(example.target, (list, tuple)):
        if len(example.target) == 2:
            # Binary market - show first option's probability
            target = f"Probability: {example.target[0]:.2f}"
        else:
            # Multi-option
            formatted = ", ".join([f"{p:.2f}" for p in example.target])
            target = f"Probabilities: [{formatted}]"
    else:
        # Single float (legacy)
        target = f"Probability: {float(example.target):.2f}"

    prepared_data.append({
        "type": "text",
        "content": target,
        "include_in_loss": True,
        **empty_lors
    })

    return {"prepared_data": prepared_data}


def _example_to_neurallambda_v2(
    example: Example,
    num_layers: int,
    target_layer: int,
    max_history_points: int,
) -> Dict[str, Any]:
    """
    Version 2: LoR block after EACH belief update (curriculum learning).

    Format:
    1. Market context (text, not in loss)
    2. For each belief point:
       a. Update text (text, not in loss)
       b. LoR block (with parsing, in loss)
    3. Target (text, in loss)

    Total: 1 + 2*N + 1 blocks (where N = number of history points)

    Hypothesis: Model learns incremental adaptation as new information arrives.
    """
    empty_lors = create_empty_lor_ixs(num_layers)
    parse_lors = create_parse_lor_ixs(num_layers, target_layer)

    lor_block = "^@G^@|^@|^@U^@|^@|^@D^@|^@|"
    lor_mask = [0, 0, 0] * 3

    prepared_data = []

    # 1. Market context
    context_text = format_market_context(example)
    prepared_data.append({
        "type": "text",
        "content": context_text,
        "include_in_loss": False,
        **empty_lors
    })

    # 2. For each belief point, add text + LoR block
    option = example.options[0]  # For binary/simple markets
    recent_beliefs = option.history_belief[-max_history_points:]

    for i, belief in enumerate(recent_beliefs):
        belief_text = f"Update {i+1}: belief={belief:.2f}"
        prepared_data.append({
            "type": "text",
            "content": belief_text,
            "include_in_loss": False,
            **empty_lors
        })

        # LoR block after each update
        prepared_data.append({
            "type": "lor",
            "content": lor_block,
            "include_in_loss": True,
            "loss_mask": lor_mask,
            **parse_lors
        })

    # 3. Target
    if isinstance(example.target, (list, tuple)):
        if len(example.target) == 2:
            target = f"Final: {example.target[0]:.2f}"
        else:
            formatted = ", ".join([f"{p:.2f}" for p in example.target])
            target = f"Final: [{formatted}]"
    else:
        target = f"Final: {float(example.target):.2f}"

    prepared_data.append({
        "type": "text",
        "content": target,
        "include_in_loss": True,
        **empty_lors
    })

    return {"prepared_data": prepared_data}


# --- LESSONS LEARNED ---
# 1. LoR block format must match neurallambda's meta-token expectations
# 2. loss_mask is CRITICAL - don't train model to predict meta-tokens, only their outputs
# 3. For multi-option markets, need to decide: aggregate history or separate per option?
# 4. Version 1 (simple) for initial experiments, Version 2 (curriculum) if V1 works
# 5. max_history_points tradeoff: more = better context, fewer = faster/less overfitting
