# Batch Size Optimization for Kalshi API Enrichment

**Agent Task:** Optimize API batch size for Kalshi enrichment while maintaining data quality. Follow scientific workflow per @.cursor/rules.md.

---

## Context & Background

**Previous Investigation Findings:**
- 2026 S3 files contain ~6.4M records/day (vs 0.46 MB in 2024)
- Current batch size: **50 tickers/request** (reduced from 100 due to 413 errors)
- Bottleneck: API enrichment takes ~6.5 minutes for 49K markets (2-day window)
- Rate limit: 20 req/s (Kalshi API hard limit)
- Current approach: ALL markets get full API enrichment (data quality > speed)

**Key Files:**
- `src/kalshi/grabber.py` - `fetch_markets_by_tickers()` (line 46-70)
- `src/build_unified_parquet.py` - enrichment logic (line 895-920)
- `tests/test_data_correctness.py` - quality validation
- `tests/parquet_quality.py` - schema compliance

**Previous Agent's Findings:**
- S3-only mode (100x speedup) → REJECTED: lost data quality
- Status-based filtering (8x speedup) → REJECTED: lost metadata for resolved markets
- Progress logging → KEPT: UX improvement, no actual speedup

---

## Goal & Mandate

**Primary Goal:** Find optimal batch size that maximizes API throughput while maintaining 100% data quality.

**Success Criteria:**
1. ≥2x speedup on API enrichment phase (current: ~6.5 min for 49K markets)
2. Zero data quality regressions (all tests pass)
3. <1% 413 error rate
4. Pareto curve showing batch size vs speedup vs error rate

---

## Scientific Workflow (per .cursor/rules.md)

### Phase 1: Hypothesis Development
**Hypotheses to test (in order):**

1. **H1: Optimal batch size is between 50-100**
   - Current: 50 (safe)
   - Previous: 100 (caused 413s with long ticker names)
   - Test: 60, 70, 80, 90, 100
   - Metric: API calls saved, 413 error rate

2. **H2: Error rate depends on ticker name length**
   - Long tickers (KXCITIESWEATHER) → 413s
   - Short tickers (NASDAQ) → safe at higher batch sizes
   - Test: Separate batches by ticker length distribution

3. **H3: Adaptive batching beats fixed size**
   - Use ticker length to predict URL size
   - Dynamically adjust batch size per request
   - Target: max tickers while staying under 8KB URL limit

### Phase 2: Test Design

**Test Dataset:**
- Use 2024-12-30 to 2024-12-31 (49K markets, known good)
- Name each run with batch size: `--name batch_{size}_test`

**Metrics to Track:**
```python
{
    "batch_size": int,
    "total_markets": int,
    "total_api_calls": int,
    "wall_clock_seconds": float,
    "error_413_count": int,
    "error_rate": float,
    "markets_enriched": int,
    "speedup_vs_baseline": float,  # vs batch_size=50
    "data_quality_score": float    # from validation tests
}
```

**Data Quality Tests (run after each experiment):**
1. `uv run pytest tests/test_data_correctness.py` (must pass 10/10)
2. `uv run python tests/parquet_quality.py` (zero schema violations)
3. Custom checks:
   - 0% ticker-only titles
   - 0% empty descriptions
   - 100% proper URLs
   - All options have ts/belief alignment

### Phase 3: Implementation

**Baseline Run (for comparison):**
```bash
uv run scripts/build_db.py --start 2024-12-30 --end 2024-12-31 \
  --name batch_50_baseline --metaculus-limit 0 --no-cache
```

**Experiments (run all):**
```bash
# Test various fixed batch sizes
for size in 60 70 80 90 100; do
  uv run scripts/build_db.py --start 2024-12-30 --end 2024-12-31 \
    --name batch_${size}_test --metaculus-limit 0 --no-cache
done
```

**Code Changes Needed:**
1. Add `--kalshi-batch-size` CLI flag to `scripts/build_db.py`
2. Pass through to `KalshiGrabber` constructor or as parameter
3. Update `fetch_markets_by_tickers()` to use configurable batch size
4. Add instrumentation:
   - Log 413 errors separately (don't just retry silently)
   - Track per-batch metrics (size, URL length, success/fail)
   - Record timing per batch

**Example instrumentation:**
```python
# In fetch_markets_by_tickers():
batch_metrics = {
    "batch_size": len(batch_tickers),
    "url_length": len(tickers_str),
    "status_code": response.status_code,
    "elapsed_ms": elapsed_time
}
logger.info(f"Batch metrics: {batch_metrics}")
```

### Phase 4: Verification Loop

**For each batch size experiment:**

1. **Run build** with instrumented code
2. **Extract metrics** from logs (use grep/parsing script)
3. **Run quality tests:**
   ```bash
   uv run pytest tests/test_data_correctness.py
   uv run python tests/parquet_quality.py
   ```
4. **Compare to baseline:**
   - API calls saved: `(baseline_calls - test_calls) / baseline_calls`
   - Speedup: `baseline_time / test_time`
   - Error rate: `413_errors / total_requests`
5. **Record in results table** (see below)

**Results Table (create as CSV):**
```csv
batch_size,api_calls,wall_clock_s,error_413_count,error_rate,markets_enriched,speedup_vs_50,quality_pass
50,982,390,0,0.0%,49068,1.0x,PASS
60,818,350,2,0.2%,49068,1.11x,PASS
70,701,320,8,1.1%,49065,1.22x,FAIL
...
```

### Phase 5: Pareto Analysis

**Create Pareto Curve:**
- X-axis: Batch size
- Y-axis 1: Speedup (vs baseline)
- Y-axis 2: Error rate (%)
- Find "knee" where speedup plateaus and errors begin

**Optimal Point Definition:**
- Error rate <1%
- Quality tests 100% pass
- Maximum speedup achievable

**Visualization (use matplotlib):**
```python
import matplotlib.pyplot as plt
fig, ax1 = plt.subplots()
ax1.plot(batch_sizes, speedups, 'b-o', label='Speedup')
ax2 = ax1.twinx()
ax2.plot(batch_sizes, error_rates, 'r-x', label='Error Rate')
ax1.axhline(y=2.0, color='g', linestyle='--', label='Target: 2x')
ax2.axhline(y=1.0, color='orange', linestyle='--', label='Max Error: 1%')
```

---

## Data Quality Testing Improvements

**Current Tests (in `/tests`):**
1. `test_data_correctness.py` - 10 tests covering schema, belief ranges, timestamps
2. `parquet_quality.py` - JSON parsing, null counts, type validation

**Known Limitations & Improvements Needed:**

### 1. **Enrichment Quality Not Directly Tested**
**Issue:** Tests don't verify API enrichment happened vs S3-only metadata.

**Improvement:**
```python
# Add to test_data_correctness.py
def test_api_enrichment_quality(latest_parquet_df):
    """Verify markets have API-enriched metadata, not just S3 fallbacks."""
    df = latest_parquet_df[latest_parquet_df['source'] == 'kalshi']
    
    # Check 1: Titles should not just be tickers
    options = df['options_json'].apply(json.loads).iloc[0]
    for opt in options:
        title = opt.get('title', '')
        market_id = opt.get('market_id', '')
        # If title == market_id, it's likely unenriched
        assert title != market_id or len(title.split()) > 1, \
            f"Ticker-only title detected: {title}"
    
    # Check 2: Descriptions should be substantive (>50 chars)
    short_desc = df[df['description'].str.len() < 50]
    assert len(short_desc) / len(df) < 0.05, \
        f"{len(short_desc)} markets have suspiciously short descriptions"
    
    # Check 3: URLs should have proper kalshi.com/markets/ format
    proper_urls = df['url'].str.contains('kalshi.com/markets/', na=False)
    assert proper_urls.sum() / len(df) > 0.95, \
        f"Only {proper_urls.sum()} / {len(df)} have proper URLs"
```

### 2. **No 413 Error Detection**
**Issue:** Tests don't fail if markets are silently skipped due to 413 errors.

**Improvement:**
```python
def test_complete_enrichment(latest_parquet_df):
    """Verify all active markets from S3 are present and enriched."""
    # Compare against S3 scan output (needs instrumentation)
    # This requires tracking "expected markets" vs "actual markets"
    # Currently we don't have this ground truth
    pass  # TODO: Add after instrumenting S3 scan counts
```

### 3. **Batch Size Impact Not Measured**
**Issue:** No tests for whether batch size affects metadata completeness.

**Improvement:**
```python
def test_batch_consistency():
    """Compare two datasets built with different batch sizes."""
    # Build with batch_size=50 and batch_size=80
    # Assert same market count, same metadata quality
    # This catches silent failures from 413s
    pass  # TODO: Implement as part of this task
```

### 4. **No URL Length Validation**
**Issue:** Don't validate that URL construction stays under 8KB.

**Improvement:**
```python
# Add to src/kalshi/grabber.py
def estimate_url_length(tickers: List[str]) -> int:
    """Estimate GET URL length for /markets?tickers=..."""
    base = "https://api.elections.kalshi.com/v2/markets?tickers="
    tickers_str = ",".join(tickers)
    return len(base) + len(tickers_str)

# Validate before request
estimated_length = estimate_url_length(batch_tickers)
if estimated_length > 7500:  # 8KB limit with margin
    logger.warning(f"URL length {estimated_length} bytes may exceed limit")
```

### 5. **No Sampling Bias Check**
**Issue:** Random sampling (50 markets) might miss rare failure modes.

**Improvement:**
- Sample by ticker length distribution (short, medium, long)
- Sample by market type (binary, scalar if exists)
- Sample by date range (beginning, middle, end of window)
- Ensure edge cases are covered

---

## Deliverables

1. **CSV Results Table** (`temporary/batch_size_results.csv`)
2. **Pareto Curve Plot** (`temporary/batch_size_pareto.png`)
3. **Recommendation Report** (`temporary/BATCH_SIZE_RECOMMENDATION.md`)
   - Optimal batch size with justification
   - Expected speedup vs current
   - Risks and mitigation strategies
4. **Updated Code** (if implementing optimal size):
   - `src/kalshi/grabber.py` with new batch size
   - Updated LESSONS LEARNED in affected files
5. **Enhanced Tests** (at least 2 of the 5 improvements above)

---

## Notes & Warnings

1. **Rate Limiting:** 20 req/s is hard limit. Batch size only reduces request count, doesn't change rate limit handling.
2. **413 Errors:** KXCITIESWEATHER markets have extremely long ticker names (100+ chars). These will always be problematic at high batch sizes.
3. **Cleanup:** Delete all test datasets (`data/datasets/*_test_unified` and `data/clean/canonical_*_test.db`) when done.
4. **Lessons Learned:** Add findings to bottom of `src/kalshi/grabber.py` per .cursor/rules.md.
5. **Failed Experiments:** Document what didn't work and why (just as important as successes).

---

## Success Metrics

- ✓ Speedup ≥2x on API enrichment phase
- ✓ Zero data quality regressions (10/10 tests pass)
- ✓ Error rate <1%
- ✓ Pareto curve shows clear optimal point
- ✓ At least 2 new quality tests added
- ✓ Recommendation is actionable with concrete numbers

---

## Example Timeline (estimate only, work until done)

1. **Setup & Baseline** (~30 min): Run batch_size=50 baseline, extract metrics
2. **Experiments** (~2-3 hours): Run 5 batch sizes (60,70,80,90,100), each takes ~7 min + validation
3. **Analysis** (~1 hour): Parse logs, create CSV, generate Pareto curve
4. **Quality Improvements** (~1-2 hours): Implement 2-3 new test improvements
5. **Documentation** (~30 min): Write recommendation report, update LESSONS LEARNED

**Total: ~4-6 hours** (but work continuously until all deliverables complete)
