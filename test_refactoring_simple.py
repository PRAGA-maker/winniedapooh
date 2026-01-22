"""Simple test to debug refactoring issues."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from methods.rlm_forecaster import RLMForecaster, build_context, build_setup_code
from methods.rlm_tools.semantic_search import MarketSearchIndex
from forecasting.dataclasses import Example, OptionHistory, Batch
from datetime import datetime
import os
from dotenv import load_dotenv

# Load API key
load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")
print(f"API key present: {bool(api_key)}")
print(f"API key length: {len(api_key) if api_key else 0}")

# Create test example
example = Example(
    event_id='test_market',
    source='kalshi',
    cutoff_ts=datetime(2024, 12, 15, 12, 0),
    static_features={
        'title': 'Will it rain tomorrow?',
        'description': 'A simple test market about weather',
        'end_time': datetime(2024, 12, 16),
    },
    options=[
        OptionHistory(
            option_id='opt_yes',
            market_id='test_market',
            title='Yes',
            history_belief=[0.3, 0.4, 0.5],
            history_ts=[datetime(2024, 12, i) for i in range(1, 4)],
        ),
        OptionHistory(
            option_id='opt_no',
            market_id='test_market',
            title='No',
            history_belief=[0.7, 0.6, 0.5],
            history_ts=[datetime(2024, 12, i) for i in range(1, 4)],
        ),
    ],
    target=[0.0, 1.0],
)

print("\n" + "="*60)
print("Testing context building")
print("="*60)
context = build_context(example)
print(f"Context keys: {list(context.keys())}")
print(f"Context: {context}")

print("\n" + "="*60)
print("Testing setup code")
print("="*60)
search_index = MarketSearchIndex()
setup_code = build_setup_code(search_index, example)
print(f"Setup code length: {len(setup_code)} chars")
print(f"\nFirst 800 chars:\n{setup_code[:800]}")

print("\n" + "="*60)
print("Testing RLM initialization")
print("="*60)
try:
    rlm = RLMForecaster(
        api_key=api_key,
        model='gemini-3-flash',
        max_iterations=5,
        call_budget=10,
        verbose=False,
        use_repl=True,
        diagnostic_mode=True,  # Enable diagnostics
    )
    print("[SUCCESS] RLMForecaster initialized")
except Exception as e:
    print(f"[ERROR] Failed to initialize: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "="*60)
print("Building search index")
print("="*60)
batch = Batch(examples=[example])
rlm.fit([batch], {})
print("[SUCCESS] Search index built")

print("\n" + "="*60)
print("Running prediction")
print("="*60)

# Manually call _predict_with_repl to see errors
import traceback
try:
    pred, stats = rlm._predict_with_repl(example)
    print(f"[SUCCESS] Prediction completed")
    print(f"Prediction: {pred}")
    print(f"Fallback used: {stats.fallback_used}")
    print(f"API calls: {stats.api_calls}")
    print(f"Code blocks: {stats.code_blocks_executed}")
    print(f"Iterations: {stats.iterations_used}")
    print(f"Raw response (first 200 chars): {stats.raw_response[:200]}")
except Exception as e:
    print(f"[ERROR] Prediction failed: {e}")
    traceback.print_exc()
    print(f"\nException type: {type(e)}")
    sys.exit(1)

print("\n" + "="*60)
print("Test completed successfully!")
print("="*60)
