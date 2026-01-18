import argparse
import csv
import json
from pathlib import Path
from typing import Dict, Any, List, Optional

import matplotlib.pyplot as plt


SUMMARY_PREFIX = "KALSHI_MARKETS_BATCH_SUMMARY "
METRICS_PREFIX = "KALSHI_MARKETS_BATCH_METRICS "


def _parse_json_payload(payload: str) -> Optional[Dict[str, Any]]:
    payload = payload.strip()
    if not payload:
        return None
    if payload.startswith("{") and payload.endswith("}"):
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return None
    start = payload.find("{")
    end = payload.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        return json.loads(payload[start:end + 1])
    except json.JSONDecodeError:
        return None


def parse_summary_line(line: str) -> Optional[Dict[str, Any]]:
    if SUMMARY_PREFIX not in line:
        return None
    _, payload = line.split(SUMMARY_PREFIX, 1)
    return _parse_json_payload(payload)


def parse_metrics_line(line: str) -> Optional[Dict[str, Any]]:
    if METRICS_PREFIX not in line:
        return None
    _, payload = line.split(METRICS_PREFIX, 1)
    return _parse_json_payload(payload)


def read_summary_from_log(log_path: Path) -> Optional[Dict[str, Any]]:
    summary = None
    metrics: List[Dict[str, Any]] = []
    with log_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            parsed = parse_summary_line(line)
            if parsed:
                summary = parsed
                continue
            metric = parse_metrics_line(line)
            if metric:
                metrics.append(metric)
    if summary:
        return summary
    if not metrics:
        return None
    total_batches = len(metrics)
    total_tickers = sum(int(m.get("batch_size", 0)) for m in metrics)
    markets_returned = sum(int(m.get("markets_returned", 0)) for m in metrics)
    error_413_count = sum(1 for m in metrics if m.get("status_code") == 413)
    error_other_count = sum(
        1 for m in metrics
        if m.get("status_code") not in (200, None, 413)
    )
    batch_size = metrics[0].get("batch_size")
    return {
        "batch_size": batch_size,
        "total_batches": total_batches,
        "total_tickers": total_tickers,
        "markets_returned": markets_returned,
        "error_413_count": error_413_count,
        "error_other_count": error_other_count,
        "wall_clock_seconds": None,
    }


def summarize(log_paths: List[Path]) -> List[Dict[str, Any]]:
    summaries = []
    for path in log_paths:
        summary = read_summary_from_log(path)
        if not summary:
            raise ValueError(f"No batch summary/metrics found in log: {path}")
        summary["log_path"] = str(path)
        summaries.append(summary)
    return summaries


def pick_baseline(summaries: List[Dict[str, Any]], baseline_log: Optional[Path]) -> Optional[Dict[str, Any]]:
    if baseline_log:
        baseline = read_summary_from_log(baseline_log)
        if not baseline:
            raise ValueError(f"No batch summary found in baseline log: {baseline_log}")
        return baseline

    for summary in summaries:
        if summary.get("batch_size") == 50:
            return summary
    return None


def write_csv(rows: List[Dict[str, Any]], csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "batch_size",
                "api_calls",
                "wall_clock_s",
                "error_413_count",
                "error_rate",
                "markets_enriched",
                "speedup_vs_50",
                "quality_pass",
                "log_path",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_pareto_plot(rows: List[Dict[str, Any]], output_path: Path) -> None:
    batch_sizes = [row["batch_size"] for row in rows]
    speedups = [row.get("speedup_vs_50") for row in rows]
    error_rates = [row["error_rate"] for row in rows]

    if all(s is None for s in speedups):
        speedups = [1.0 for _ in rows]

    fig, ax1 = plt.subplots()
    ax1.plot(batch_sizes, speedups, "b-o", label="Speedup vs 50")
    ax1.set_xlabel("Batch size")
    ax1.set_ylabel("Speedup", color="b")
    ax1.tick_params(axis="y", labelcolor="b")
    ax1.axhline(y=2.0, color="g", linestyle="--", label="Target: 2x")

    ax2 = ax1.twinx()
    ax2.plot(batch_sizes, error_rates, "r-x", label="413 Error Rate")
    ax2.set_ylabel("413 Error Rate", color="r")
    ax2.tick_params(axis="y", labelcolor="r")
    ax2.axhline(y=0.01, color="orange", linestyle="--", label="Max Error: 1%")

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def write_recommendation(rows: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    eligible = [
        row for row in rows
        if row["error_rate"] is not None
        and row["error_rate"] < 0.01
        and row.get("speedup_vs_50") is not None
    ]
    eligible = sorted(eligible, key=lambda r: r["speedup_vs_50"], reverse=True)
    best = eligible[0] if eligible else None

    lines = [
        "# Batch Size Recommendation",
        "",
        "## Summary",
    ]
    if best:
        lines += [
            f"- Recommended batch size: {best['batch_size']}",
            f"- Speedup vs 50: {best['speedup_vs_50']:.2f}x",
            f"- 413 error rate: {best['error_rate']:.2%}",
            f"- API calls: {best['api_calls']}",
            f"- Markets enriched: {best['markets_enriched']}",
        ]
    else:
        lines += [
            "- No batch size met the <1% 413 error rate with a computed speedup.",
            "- Re-run with a baseline log or add `batch_size=50` results.",
        ]

    lines += [
        "",
        "## Risks",
        "- Large ticker batches can exceed URL length limits and trigger 413 errors.",
        "- Error rates may spike for long-ticker distributions (e.g., KXCITIESWEATHER).",
        "",
        "## Next Steps",
        "- Run data quality tests for each batch size and mark PASS/FAIL in the CSV.",
        "- If error rate exceeds 1%, reduce batch size or adopt adaptive batching.",
    ]

    output_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse Kalshi batch logs into CSV + Pareto plot.")
    parser.add_argument("logs", nargs="+", help="Log file(s) to parse")
    parser.add_argument("--baseline-log", type=str, default=None, help="Optional baseline log (batch_size=50)")
    parser.add_argument("--csv", type=str, default="temporary/batch_size_results.csv", help="Output CSV path")
    parser.add_argument("--plot", type=str, default="temporary/batch_size_pareto.png", help="Output plot path")
    parser.add_argument("--report", type=str, default="temporary/BATCH_SIZE_RECOMMENDATION.md", help="Output report path")
    args = parser.parse_args()

    log_paths = [Path(p) for p in args.logs]
    baseline_log = Path(args.baseline_log) if args.baseline_log else None

    summaries = summarize(log_paths)
    baseline = pick_baseline(summaries, baseline_log)
    baseline_time = baseline.get("wall_clock_seconds") if baseline else None

    rows = []
    for summary in summaries:
        api_calls = summary.get("total_batches")
        error_413_count = summary.get("error_413_count", 0)
        error_rate = None
        if api_calls:
            error_rate = error_413_count / api_calls
        speedup = None
        if baseline_time and summary.get("wall_clock_seconds"):
            speedup = baseline_time / summary["wall_clock_seconds"]

        rows.append({
            "batch_size": summary.get("batch_size"),
            "api_calls": api_calls,
            "wall_clock_s": summary.get("wall_clock_seconds"),
            "error_413_count": error_413_count,
            "error_rate": error_rate,
            "markets_enriched": summary.get("markets_returned"),
            "speedup_vs_50": speedup,
            "quality_pass": "PENDING",
            "log_path": summary.get("log_path"),
        })

    rows = sorted(rows, key=lambda r: r["batch_size"] or 0)
    write_csv(rows, Path(args.csv))
    write_pareto_plot(rows, Path(args.plot))
    write_recommendation(rows, Path(args.report))


if __name__ == "__main__":
    main()
