import sys
from pathlib import Path

# Add project root to sys.path
sys.path.append(str(Path(__file__).parent.parent))

from dataobject.dataset import MarketDataset

def smoke_dataset_load():
    data_dir = Path("data/datasets")
    datasets = sorted(list(data_dir.glob("v*_unified")))
    if not datasets:
        print("No datasets found.")
        return
        
    ds_path = str(datasets[-1])
    print(f"Loading latest dataset: {ds_path}")
    dataset = MarketDataset.load(ds_path)
    
    view = dataset.slice(source="kalshi")
    print(f"Kalshi records: {len(view.records())}")
    
    view = dataset.slice(source="metaculus")
    print(f"Metaculus records: {len(view.records())}")

if __name__ == "__main__":
    smoke_dataset_load()

