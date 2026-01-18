from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from src.common.http import get_metaculus_client
from src.metaculus.grabber import MetaculusGrabber


def main() -> None:
    grabber = MetaculusGrabber()
    client = get_metaculus_client()
    posts = grabber.fetch_posts(limit=50, status="resolved")
    target = None

    for post in posts:
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
            status = str(q.get("status") or "").lower()
            if "resolved" in status or "closed" in status:
                target = q
                break
        if target:
            break

    if not target:
        print("No resolved/closed question found in sample.")
        return

    q_id = target["id"]
    print(f"Target question id: {q_id} status={target.get('status')}")
    aggs = target.get("aggregations", {})
    rw = aggs.get("recency_weighted", {})
    history_len = len(rw.get("history", []) or [])
    print(f"recency_weighted history_len={history_len}")

    try:
        response = client.get(f"/api2/questions/{q_id}/prediction-history/")
        print(f"prediction-history status: {response.status_code}")
        print(f"prediction-history length: {len(response.json())}")
    except Exception as exc:
        print(f"prediction-history error: {exc}")


if __name__ == "__main__":
    main()
