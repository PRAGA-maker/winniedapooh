import json

import pandas as pd


def main() -> None:
    df = pd.read_parquet("data/datasets/v20260118_1223_metaculus_verify_unified/data.parquet")

    for source in ["kalshi", "metaculus"]:
        subset = df[df["source"] == source]
        total_events = len(subset)
        print(f"\n=== {source.upper()} ===")
        print(f"Events: {total_events}")

        non_empty_histories = 0
        total_history_points = 0
        empty_descriptions = 0

        for _, row in subset.iterrows():
            options = json.loads(row["options_json"])
            if options and len(options[0].get("ts", [])) > 0:
                non_empty_histories += 1
                total_history_points += len(options[0]["ts"])
            description = row["description"]
            if not description or description.strip() == "":
                empty_descriptions += 1

        history_rate = (non_empty_histories / total_events) if total_events else 0.0
        avg_history = (total_history_points / total_events) if total_events else 0.0
        empty_desc_rate = (empty_descriptions / total_events) if total_events else 0.0

        print(
            f"Non-empty histories: {non_empty_histories}/{total_events} "
            f"({history_rate * 100:.1f}%)"
        )
        print(f"Avg history length: {avg_history:.1f} points")
        print(
            f"Empty descriptions: {empty_descriptions}/{total_events} "
            f"({empty_desc_rate * 100:.1f}%)"
        )


if __name__ == "__main__":
    main()
