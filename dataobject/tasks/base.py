from typing import List, Dict, Any
from dataobject.dataset import MarketRecordWrapper
from dataobject.io_hygiene import Example, Batch

class Task:
    name: str
    
    def make_examples(self, record: MarketRecordWrapper, rng: Any) -> List[Example]:
        raise NotImplementedError
        
    def collate(self, examples: List[Example]) -> Batch:
        return Batch(examples=examples)
        
    def metric_fns(self) -> Dict[str, Any]:
        raise NotImplementedError

