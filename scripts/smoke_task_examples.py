import sys
import random
from pathlib import Path

# Add project root to sys.path
sys.path.append(str(Path(__file__).parent.parent))

from dataobject.dataset import MarketDataset
from dataobject.tasks.resolve_binary import ResolveBinaryTask

def smoke_task_examples():
    data_dir = Path("data/datasets")
    datasets = sorted(list(data_dir.glob("v*_unified")))
    if not datasets:
        print("No datasets found.")
        return
        
    ds_path = str(datasets[-1])
    dataset = MarketDataset.load(ds_path)
    
    task = ResolveBinaryTask()
    rng = random.Random(42)
    
    count = 0
    for record in dataset.slice().records():
        examples = task.make_examples(record, rng)
        if examples:
            print(f"\nExample for {record.source}:{record.market_id}:")
            ex = examples[0]
            print(f"  Cutoff: {ex.cutoff_ts}")
            print(f"  History points: {len(ex.history_ts)}")
            print(f"  Target: {ex.target}")
            count += 1
        if count >= 3:
            break

if __name__ == "__main__":
    smoke_task_examples()

