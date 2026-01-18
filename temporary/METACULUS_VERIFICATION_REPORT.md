# Metaculus Data Verification Report
**Date**: 2026-01-18  
**Dataset**: v20260118_1358_metaculus_verify_20260110_unified  
**Window**: 2026-01-10 to 2026-01-17  
**Posts**: 200 | **Events**: 186

---

## Executive Summary

**Hypothesis**: The download-data fallback successfully populates aggregate forecast histories for Metaculus posts in the Jan 10-17, 2026 window, producing schema-compliant parquet data with >90% history coverage and >30% rig example generation rate.

**Result**: **HYPOTHESIS REJECTED** ❌

The verification revealed a critical upstream issue with the Metaculus API that prevents history from being fetched during build time, despite the API working correctly when called manually afterward.

---

## Success Criteria Results

| Criterion | Target | Actual | Status |
|-----------|--------|--------|--------|
| Non-empty histories | >90% | 19.6% | ❌ FAIL (shortfall: 70.4%) |
| Schema/parse errors | 0 | 0 | ✅ PASS |
| Rig example generation | ≥30% | 5.4% | ❌ FAIL (shortfall: 24.6%) |
| Issues documented | All | All | ✅ PASS |

---

## Detailed Findings

### 1. Quality Audit Results

**From**: `tests/e2e_outputs/metaculus_verify_meta/parquet_quality_summary.json`

- Total Metaculus events: **186**
- Total options: **281**
- Options with non-empty history: **55/281 (19.6%)**
- Options with empty history: **226/281 (80.4%)**
- Parse errors: **0** ✅
- ts/belief length mismatches: **0** ✅

**Positive Signal**: The 55 options that DO have history have excellent quality:
- Average: 98.7 points per option
- Range: 2-252 points
- Median: 97 points

This proves the pipeline correctly processes history when present.

### 2. Rig Integration Results

**From**: `temporary/test_metaculus_rig_integration.py`

- Events tested: **186**
- Successfully generated examples: **10 (5.4%)**
- Failed to generate: **176 (94.6%)**

**Failure Breakdown**:
- Empty history: 123 events (69.9% of failures)
- Wrong status (open, not resolved/closed): 53 events (30.1%)

### 3. Root Cause Analysis

**Investigation Steps**:
1. ✅ Verified aggregations exist in API (post 41339 has 26 history points)
2. ✅ Confirmed history existed BEFORE build time (all 26 points predated build)
3. ✅ Validated points fall within date window (13/26 points = 50% in window)
4. ✅ Checked `include_cp_history=true` parameter is used
5. ❌ **ISSUE**: Stored metadata shows empty `[]` history arrays

**Root Cause**: Metaculus API returned empty aggregation history blocks during build time (2026-01-18 13:52-13:58), despite:
- History points existing before build
- ~50% falling within the date window
- `include_cp_history=true` being used
- Manual API calls afterward returning history correctly

**Why download-data fallback didn't trigger**: The aggregation blocks were PRESENT but EMPTY, so the condition `if not points:` evaluated to False (empty list is truthy in the aggregation check context).

### 4. Key Observations

1. **API Timing Issue**: The Metaculus API had inconsistent behavior during the specific build window
2. **Question Types**: Sample investigation showed post 41339 is a "date" (continuous) question type, not binary/multiple_choice - this may require different handling
3. **No Date Filtering Issue**: Diagnostic tests proved date filtering is NOT removing points - they never arrived from the API
4. **Schema Compliance**: Despite empty histories, all data is schema-compliant (0 parse errors)
5. **Description Quality**: 100% of events have non-empty descriptions (186/186)

---

## Recommendations

### Immediate Fixes

1. **Fix fallback trigger logic**:
   ```python
   # Current (broken):
   if not points:
       # trigger fallback
   
   # Fixed:
   if not points or len(points) == 0:
       # trigger fallback
   ```

2. **Make download-data primary source** instead of fallback:
   - Call download-data endpoint FIRST for all posts
   - Only fall back to aggregation blocks if download-data fails
   - This provides more robust history coverage

3. **Add question type handling**:
   - Investigate if `date`/`continuous` questions require different API parameters
   - Consider separate code paths for different question types

### Testing Strategy

1. **Rebuild with fix**: Run `build_db.py` again with the same date window
2. **Spot check**: Manually verify a few posts have history after rebuild
3. **Re-run verification**: Confirm >90% history coverage achieved
4. **Rig test**: Verify >30% example generation rate

### Long-term Improvements

1. **Add retry logic** for empty history responses
2. **Log when fallback is triggered** for monitoring
3. **Add telemetry** tracking history coverage per question type
4. **Consider caching strategy** to avoid repeated empty responses

---

## Verification Statement

**The download-data fallback validation: FAILED ❌**

**Reason**: The fallback was never triggered because the Metaculus API returned present-but-empty aggregation blocks during build time. When the aggregation block exists (even if `history: []`), the fallback condition is not met. This is a pipeline logic issue that can be fixed by checking `len(points) > 0` instead of just truthiness.

**Secondary Issue**: Even if the fallback was triggered correctly, the underlying API behavior of returning empty histories during certain time windows remains unexplained and may require investigation with Metaculus team or additional robustness measures.

**Data Quality**: Schema compliance is 100% (0 parse errors, 0 mismatches), but functional quality is poor (only 19.6% have usable histories).

---

## Files Modified/Created

### Analysis Scripts
- `temporary/analyze_metaculus_quality.py` - Quality analysis tool
- `temporary/test_metaculus_rig_integration.py` - Rig compatibility test
- `temporary/diagnose_metaculus_fallback.py` - API diagnostic tool
- `temporary/check_timestamp_ranges.py` - Timestamp validation tool

### Documentation
- `src/build_unified_parquet.py` - Added lesson #41
- `src/metaculus/grabber.py` - Added lesson #8
- `temporary/analyze_metaculus_quality.py` - Added lessons learned section

### Test Outputs
- `tests/e2e_outputs/metaculus_verify_meta/parquet_quality_summary.json`
- `tests/e2e_outputs/metaculus_verify_meta/parquet_quality_report.txt`
- `data/datasets/v20260118_1358_metaculus_verify_20260110_unified/metaculus_quality_metrics.json`

---

## Next Steps

1. **Fix the fallback trigger logic** in `src/build_unified_parquet.py` (lines 1154-1165)
2. **Rebuild the dataset** with the fix
3. **Re-run verification** to confirm success criteria met
4. **Consider** making download-data the primary source if issues persist
5. **Investigate** question type differences (date vs binary vs multiple_choice)
