import random
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from dataobject.dataset import EventDataset
from dataobject.tasks.resolve_binary import ResolveEventTask


def main() -> int:
    dataset = EventDataset.load("data/datasets/v20260118_1223_metaculus_verify_unified")
    metaculus_view = dataset.slice(source="metaculus")
    total_events = len(metaculus_view.df)
    print(f"Metaculus events: {total_events}")

    task = ResolveEventTask(relax_status=True, min_history_points=5)
    rng = random.Random(42)

    examples_count = 0
    events_with_examples = 0
    failed_records = []

    for record in metaculus_view.records():
        examples = task.make_examples(record, rng)
        if examples:
            examples_count += len(examples)
            events_with_examples += 1
        else:
            failed_records.append(
                {
                    "event_id": record.event_id,
                    "title": record.title,
                    "status": record.status,
                    "options_count": len(record.options),
                }
            )

    print("\nResults:")
    print(f"  Total examples generated: {examples_count}")
    print(f"  Events with examples: {events_with_examples}")
    print(f"  Failed records: {len(failed_records)}")

    if failed_records:
        print("\nFirst 5 failures:")
        for record in failed_records[:5]:
            title = record["title"]
            truncated_title = title[:50] + "..." if len(title) > 50 else title
            print(
                f"  - {record['event_id']}: {truncated_title} "
                f"(status={record['status']}, opts={record['options_count']})"
            )

    success_rate = (events_with_examples / total_events) if total_events else 0.0
    print(f"\nSuccess rate: {success_rate:.1%}")
    if success_rate < 0.3:
        print("FAIL: Too few examples generated.")
        return 1

    print("PASS: Rig integration target met.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
