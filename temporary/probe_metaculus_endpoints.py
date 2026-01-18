from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from src.common.http import get_metaculus_client


def main() -> None:
    client = get_metaculus_client()
    question_id = 41339
    endpoints = [
        f"/api/questions/{question_id}/",
        f"/api/questions/{question_id}/prediction-history/",
        f"/api/questions/{question_id}/predictions/",
        f"/api/questions/{question_id}/prediction-for-date/",
        f"/api2/questions/{question_id}/",
    ]

    for endpoint in endpoints:
        try:
            response = client.get(endpoint)
            print(f"{endpoint} status={response.status_code}")
            if endpoint == f"/api/questions/{question_id}/":
                data = response.json()
                history_like = [
                    key
                    for key in data.keys()
                    if "history" in key.lower() or "prediction" in key.lower()
                ]
                print(f"api question history-like keys: {history_like}")
        except Exception as exc:
            print(f"{endpoint} failed: {exc}")


if __name__ == "__main__":
    main()
