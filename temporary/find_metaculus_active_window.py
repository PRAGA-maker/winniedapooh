from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from src.metaculus.grabber import MetaculusGrabber
from src.metaculus.map_to_canonical import map_metaculus_history_point


def extract_history_points(question: dict) -> list[dict]:
    if not question:
        return []
    aggs = question.get("aggregations", {})
    for agg_key in ["recency_weighted", "unweighted", "weighted"]:
        agg_block = aggs.get(agg_key)
        if not agg_block:
            continue
        points = agg_block.get("history", [])
        if not points and agg_block.get("latest"):
            points = [agg_block["latest"]]
        if points:
            return points
    return []


def to_date(dt_value: datetime) -> date:
    return dt_value.date()


def main() -> None:
    grabber = MetaculusGrabber()
    limit = 400
    status = "closed"
    posts = grabber.fetch_posts(limit=limit, status=status)

    date_counts = Counter()
    questions_with_history = 0
    questions_with_10_plus = 0
    total_questions = 0

    for post in posts:
        detail = post
        needs_detail = False
        question = detail.get("question")
        if question and "aggregations" not in question:
            needs_detail = True
        if needs_detail:
            detail = grabber.fetch_post_detail(post["id"])

        sub_questions = []
        if detail.get("question"):
            sub_questions.append(detail["question"])
        if detail.get("group_questions"):
            sub_questions.extend(detail["group_questions"])
        if detail.get("conditional"):
            cond = detail["conditional"]
            for sub_name in ["condition", "condition_child", "question_yes", "question_no"]:
                sub_q = cond.get(sub_name)
                if sub_q and isinstance(sub_q, dict):
                    sub_questions.append(sub_q)

        for q in sub_questions:
            if not q or not isinstance(q, dict) or "id" not in q:
                continue
            total_questions += 1
            points = extract_history_points(q)
            if not points:
                continue
            questions_with_history += 1
            if len(points) >= 10:
                questions_with_10_plus += 1
            for pt in points:
                ts_point = map_metaculus_history_point(str(q["id"]), pt)
                date_counts[to_date(ts_point.ts)] += 1

    print(f"Posts scanned: {len(posts)} (status={status})")
    print(f"Questions found: {total_questions}")
    print(f"Questions with any history: {questions_with_history}")
    print(f"Questions with 10+ points: {questions_with_10_plus}")

    if not date_counts:
        print("No history points found in sampled posts.")
        return

    print("\nTop 10 dates by history points:")
    for day, count in date_counts.most_common(10):
        print(f"  {day.isoformat()}: {count}")

    # Find best 15-day window by total points
    window_days = 15
    all_dates = sorted(date_counts)
    best_window = None
    best_count = 0

    for start in all_dates:
        end = start + timedelta(days=window_days - 1)
        total = 0
        current = start
        while current <= end:
            total += date_counts.get(current, 0)
            current += timedelta(days=1)
        if total > best_count:
            best_count = total
            best_window = (start, end)

    if best_window:
        start, end = best_window
        print(
            f"\nRecommended window (15 days): {start.isoformat()} to {end.isoformat()} "
            f"with {best_count} points"
        )


if __name__ == "__main__":
    main()
