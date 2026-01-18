from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from src.metaculus.grabber import MetaculusGrabber


def main() -> None:
    grabber = MetaculusGrabber()
    posts = grabber.fetch_posts(limit=100)
    for post in posts:
        forecasts_count = post.get("forecasts_count", 0) or 0
        question = post.get("question") or {}
        aggs = question.get("aggregations", {})
        rw = aggs.get("recency_weighted", {})
        history_len = len(rw.get("history", []) or [])
        if forecasts_count and forecasts_count > 10:
            print(
                f"post_id={post.get('id')} forecasts_count={forecasts_count} "
                f"history_len={history_len}"
            )
            return
    print("No post with forecasts_count > 10 found in sample.")


if __name__ == "__main__":
    main()
