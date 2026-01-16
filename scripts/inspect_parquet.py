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
    
    print("\nStats on 'belief' list length:")
    df['belief_len'] = df['belief'].apply(len)
    print(df.groupby('source')['belief_len'].describe())
    
    # Check for empty belief lists
    empty_belief = df[df['belief_len'] == 0]
    if len(empty_belief) > 0:
        print(f"\nRows with empty belief lists: {len(empty_belief)}")
        print(empty_belief.groupby('source').size())
    else:
        print("\nNo rows with empty belief lists found.")

    # More granular belief value stats (min/max/mean)
    print("\nBelief value stats per source (expanded):")
    def get_list_stats(lst):
        if lst is None or len(lst) == 0:
            return pd.Series({'min': None, 'max': None, 'mean': None, 'count': 0})
        
        # Filter out None values from the list
        vals = [v for v in lst if v is not None and not (isinstance(v, float) and pd.isna(v))]
        if not vals:
            return pd.Series({'min': None, 'max': None, 'mean': None, 'count': 0})
        return pd.Series({'min': min(vals), 'max': max(vals), 'mean': sum(vals)/len(vals), 'count': len(vals)})

    source_stats = df.groupby('source')['belief'].apply(lambda x: x.apply(get_list_stats).mean(numeric_only=True))
    print(source_stats)

    # Ratio of non-empty to total
    print("\nNon-empty timeseries ratio:")
    non_empty_ratio = df.groupby('source')['belief_len'].apply(lambda x: (x > 0).mean())
    print(non_empty_ratio)

    # Check Kalshi specifically
    kalshi_df = df[df['source'] == 'kalshi']
    if len(kalshi_df) > 0:
        print("\nKalshi sample (first 5 tickers and belief lengths):")
        print(kalshi_df[['market_id', 'belief_len']].head(5))
        
        # Check if any have non-None beliefs
        non_null_belief = kalshi_df['belief'].apply(lambda x: any(v is not None for v in x)).sum()
        print(f"Kalshi markets with at least one non-null belief: {non_null_belief}/{len(kalshi_df)}")

if __name__ == "__main__":
    inspect_latest_parquet()
