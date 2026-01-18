from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from src.metaculus.grabber import MetaculusGrabber
from src.common.http import get_metaculus_client


def summarize_question(q: dict) -> str:
    aggs = q.get("aggregations", {}) if q else {}
    summary = []
    summary.append(f"id={q.get('id')}")
    summary.append(f"status={q.get('status')}")
    summary.append(f"aggregations_keys={list(aggs.keys())}")
    for key in ["recency_weighted", "unweighted", "weighted"]:
        block = aggs.get(key) or {}
        history_len = len(block.get("history", []) or [])
        has_latest = bool(block.get("latest"))
        summary.append(f"{key}: history_len={history_len}, latest={has_latest}")
    return ", ".join(summary)


def main() -> None:
    grabber = MetaculusGrabber()
    posts = grabber.fetch_posts(limit=5)
    if not posts:
        print("No posts returned.")
        return
    first_post = posts[0]
    post_id = first_post["id"]
    feed_question = first_post.get("question") or {}
    feed_aggs = feed_question.get("aggregations", {})
    feed_rw = feed_aggs.get("recency_weighted", {})
    feed_history_len = len(feed_rw.get("history", []) or [])
    print(f"Feed question history_len={feed_history_len}")
    detail = grabber.fetch_post_detail(post_id)
    question = detail.get("question")
    print(f"Post id: {post_id}")
    if question:
        print("Question summary:", summarize_question(question))
        history_like = [
            key
            for key in question.keys()
            if "history" in key.lower() or "prediction" in key.lower()
        ]
        print(f"post question history-like keys: {history_like}")
        # Retry with explicit include_cp_history=1 to verify behavior.
        client = get_metaculus_client()
        raw_detail = client.get(f"/api/posts/{post_id}/", params={"include_cp_history": 1}).json()
        raw_q = raw_detail.get("question") or {}
        aggs = raw_q.get("aggregations", {})
        rw = aggs.get("recency_weighted", {})
        history_len = len(rw.get("history", []) or [])
        print(f"include_cp_history=1 history_len={history_len}")
        q_id = question.get("id")
        if q_id:
            client = get_metaculus_client()
            try:
                response = client.get(f"/api2/questions/{q_id}/")
                print(f"/api2/questions/{q_id}/ status: {response.status_code}")
                data = response.json()
                history_like = [
                    key
                    for key in data.keys()
                    if "history" in key.lower() or "prediction" in key.lower()
                ]
                print(f"api2 question history-like keys: {history_like}")
            except Exception as exc:
                print(f"/api2/questions/{q_id}/ failed: {exc}")
            for endpoint in [
                f"/api2/questions/{q_id}/prediction-history/",
                f"/api2/questions/{q_id}/predictions/",
                f"/api2/questions/{q_id}/prediction-for-date/",
            ]:
                try:
                    response = client.get(endpoint)
                    data = response.json()
                    length = len(data) if isinstance(data, list) else len(data.keys())
                    print(f"{endpoint} status={response.status_code} len={length}")
                except Exception as exc:
                    print(f"{endpoint} failed: {exc}")
    group_questions = detail.get("group_questions") or []
    if group_questions:
        print(f"Group questions: {len(group_questions)}")
        for q in group_questions[:3]:
            print("  ", summarize_question(q))


if __name__ == "__main__":
    main()
