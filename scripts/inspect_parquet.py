import pandas as pd
from pathlib import Path
import json

def inspect_latest_parquet():
    datasets_dir = Path("data/datasets")
    datasets_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all dataset directories
    datasets = sorted(datasets_dir.glob("v*_unified"))
    if not datasets:
        print("No datasets found in data/datasets/")
        print("\nTo create a dataset, run:")
        print("  uv run scripts/build_db.py --start 2023-01-01 --end 2025-12-31")
        return
    
    latest_dataset = datasets[-1]
    parquet_path = latest_dataset / "data.parquet"
    
    if not parquet_path.exists():
        print(f"Dataset directory found: {latest_dataset}")
        print(f"But parquet file not found: {parquet_path}")
        print("\nThe dataset may be incomplete. Try rebuilding it.")
        return
    
    print(f"Inspecting: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    
    print(f"Total rows: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    
    print("\nSource counts:")
    print(df['source'].value_counts())
    
    print("\nSample row (first 5 columns):")
    print(df.iloc[0, :5])
    
    print("\nStats on option count:")
    def count_options(raw):
        if not raw:
            return 0
        try:
            return len(json.loads(raw))
        except json.JSONDecodeError:
            return 0
    df['option_count'] = df['options_json'].apply(count_options)
    print(df.groupby('source')['option_count'].describe())
    
    # Check for empty belief lists
    empty_options = df[df['option_count'] == 0]
    if len(empty_options) > 0:
        print(f"\nRows with empty options: {len(empty_options)}")
        print(empty_options.groupby('source').size())
    else:
        print("\nNo rows with empty options found.")

    # More granular belief value stats (min/max/mean)
    print("\nBelief value stats per source (expanded):")
    def get_option_beliefs(raw):
        if not raw:
            return []
        try:
            options = json.loads(raw)
        except json.JSONDecodeError:
            return []
        beliefs = []
        for option in options:
            vals = option.get("belief") or []
            beliefs.extend([v for v in vals if v is not None and not (isinstance(v, float) and pd.isna(v))])
        return beliefs

    def get_list_stats(vals):
        if not vals:
            return pd.Series({'min': None, 'max': None, 'mean': None, 'count': 0})
        return pd.Series({'min': min(vals), 'max': max(vals), 'mean': sum(vals)/len(vals), 'count': len(vals)})

    source_stats = df.groupby('source')['options_json'].apply(lambda x: x.apply(get_option_beliefs).apply(get_list_stats).mean(numeric_only=True))
    print(source_stats)

    # Ratio of non-empty to total
    print("\nNon-empty option ratio:")
    non_empty_ratio = df.groupby('source')['option_count'].apply(lambda x: (x > 0).mean())
    print(non_empty_ratio)

    # Check Kalshi specifically
    kalshi_df = df[df['source'] == 'kalshi']
    if len(kalshi_df) > 0:
        print("\nKalshi sample (first 5 events and option counts):")
        print(kalshi_df[['event_id', 'option_count']].head(5))

if __name__ == "__main__":
    inspect_latest_parquet()

# --- LESSONS LEARNED ---
# 1. options_json can be large; only sample when exploring heavy fields.
# 2. Keep option_count derived to avoid bloating the canonical schema.
