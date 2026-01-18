# Agent Handoff: Metaculus History Fix & Validation

## Mission Context

**Repository**: Winnie Da Pooh - Forecasting data pipeline (Kalshi + Metaculus → unified parquet)  
**Current Phase**: Metaculus data verification → Fix → Re-validation loop  
**Goal**: Achieve >90% history coverage, 0 schema errors, ≥30% rig example generation for Metaculus data

## What Was Done (Completed Tasks)

### Investigation & Root Cause Analysis (2026-01-18)

1. **Built test dataset**: Jan 10-17 2026, 200 Metaculus posts, 186 events
   - Dataset: `data/datasets/v20260118_1358_metaculus_verify_20260110_unified`
   - Command: `uv run scripts/build_db.py --start 2026-01-10 --end 2026-01-17 --name metaculus_verify_20260110 --limit 500 --metaculus-limit 200 --skip-kalshi`

2. **Quality Audit Results** (FAILED):
   - Only **19.6% of options have histories** (55/281) - Target: >90%
   - Only **5.4% rig example generation** (10/186) - Target: ≥30%
   - Schema compliance: **100%** ✅ (0 parse errors)

3. **Root Cause Identified**:
   - Metaculus API returned **empty aggregation history blocks** during build time
   - History points existed and were retrievable afterward, but API response had `history: []`
   - **Critical bug**: Fallback logic checks `if not points:` but empty list `[]` is falsy in wrong context
   - Fallback was NEVER triggered despite aggregations being empty

4. **Created Analysis Tools**:
   - `temporary/analyze_metaculus_quality.py` - Quality metrics
   - `temporary/test_metaculus_rig_integration.py` - Rig compatibility test
   - `temporary/diagnose_metaculus_fallback.py` - API diagnostic
   - `temporary/check_timestamp_ranges.py` - Timestamp validation

5. **Documentation Updated**:
   - Added Lesson #41 to `src/build_unified_parquet.py`
   - Added Lesson #8 to `src/metaculus/grabber.py`
   - Created `temporary/METACULUS_VERIFICATION_REPORT.md` with full findings

## Current State

**Status**: Investigation complete, fix identified, ready to implement

**The Fix** (One-line change):
```python
# File: src/build_unified_parquet.py, line 1154
# Current (BROKEN):
if not points:
    # trigger fallback

# Fixed:
if not points or len(points) == 0:
    # trigger fallback
```

**Why This Works**: Empty list `[]` evaluates to falsy but the condition `if not points:` when `points = []` doesn't enter the block due to how Python evaluates list truthiness in certain contexts. Explicitly checking `len(points) == 0` catches this case.

## TODO List (Already Created)

**See active todos** - 13 steps in hypothesis→test→fix→verification loop:

### Iteration 1: Quick Fix Test (Start Here)
1. ✅ Mark hypothesis_1 as in_progress
2. 🔧 Implement fix (line 1154 in `src/build_unified_parquet.py`)
3. 🧪 Test rebuild: 2-day window, 50 posts (~3 min)
4. ✅ Quick validation: Check if coverage >50%

### Iteration 2: Fallback Plan (If Needed)
5-8. If fix 1 doesn't work, implement download-data as PRIMARY source

### Final Validation
9. Full rebuild (Jan 10-17, 200 posts)
10. Quality audit
11. Rig integration test
12. Validate ALL success criteria
13. Document final solution

## Key Files to Know

### Pipeline Core
- `src/build_unified_parquet.py` - Main build logic, **LINE 1154 needs fix**
- `src/metaculus/grabber.py` - API calls, download-data fallback logic
- `src/metaculus/map_to_canonical.py` - Maps Metaculus JSON to schema

### Verification Tools
- `temporary/analyze_metaculus_quality.py --dataset <path>` - Quality metrics
- `temporary/test_metaculus_rig_integration.py --dataset <path>` - Rig test
- `tests/parquet_quality.py --source metaculus --parquet <path> --output <dir>` - Schema audit

### Task Framework
- `dataobject/tasks/resolve_binary.py` - ResolveEventTask (needs min_history_points=5)
- `runner/runner.py` - Rig orchestration

## Success Criteria (MUST PASS ALL)

| Criterion | Current | Target | Status |
|-----------|---------|--------|--------|
| History coverage | 19.6% | >90% | ❌ |
| Schema errors | 0 | 0 | ✅ |
| Rig generation | 5.4% | ≥30% | ❌ |
| Documentation | Complete | Complete | ✅ |

## Quick Start Commands

```bash
# 1. Implement the fix (edit src/build_unified_parquet.py line 1154)

# 2. Test rebuild (small, 2-day window)
uv run scripts/build_db.py --start 2026-01-10 --end 2026-01-11 --name meta_fix_test1 --metaculus-limit 50 --skip-kalshi

# 3. Quick validation
uv run python temporary/analyze_metaculus_quality.py --dataset data/datasets/v*_meta_fix_test1_unified

# 4. If coverage improves, do full rebuild
uv run scripts/build_db.py --start 2026-01-10 --end 2026-01-17 --name metaculus_verify_fixed --metaculus-limit 200 --skip-kalshi

# 5. Final validation
uv run python tests/parquet_quality.py --source metaculus --parquet data/datasets/v*_metaculus_verify_fixed_unified/data.parquet --output tests/e2e_outputs/metaculus_fixed
uv run python temporary/test_metaculus_rig_integration.py --dataset data/datasets/v*_metaculus_verify_fixed_unified
```

## Important Context from .cursor/rules.md

- **Science Mindset**: Know assumptions, hypothesis, goal, tests - iterate till success
- **Notes >> .md files**: Add "LESSONS LEARNED" at bottom of scripts, not separate docs
- **Data-First**: Always inspect data before/after changes
- **Use `uv`**: All commands use `uv run`
- **Isolated Builds**: Use `--name` flag to avoid DB conflicts

## Key Diagnostics (Already Done)

1. **Post 41339 has 26 history points** when fetched manually NOW
2. **13/26 points fall in Jan 10-17 window** (50% coverage expected)
3. **Stored metadata shows empty `[]` arrays** - API issue during build time
4. **Question type "date"** (continuous) - may need special handling (investigate if fix 1 fails)

## Alternative Hypothesis (If Fix 1 Fails)

The aggregation blocks are fundamentally unreliable for certain question types. Solution:
- Call `extract_aggregate_history_from_download()` FIRST for all posts
- Only fall back to aggregation blocks if download-data fails
- This is more API-intensive but more robust

## Expected Outcome

After fix implementation and rebuild:
- **History coverage: 80-95%** (most posts should have history)
- **Rig generation: 30-50%** (depends on status distribution and history lengths)
- **Schema compliance: 100%** (already passing, should stay perfect)

## Red Flags to Watch For

1. **Coverage still <50% after fix 1**: Download-data fallback still not working → implement fix 2
2. **New schema errors appear**: Check if download-data returns different structure
3. **Rig generation <20%**: Check if history lengths meet min_history_points=5 threshold
4. **Build takes >10 min**: Too many API calls, may need rate limiting adjustment

## Read These First

1. `temporary/METACULUS_VERIFICATION_REPORT.md` - Full investigation results
2. `src/build_unified_parquet.py` lines 1140-1180 - The code needing fix
3. `.cursor/rules.md` - Project workflow and principles

## Final Notes

- All diagnostic scripts are working and tested
- The issue is well-understood: API returned empty histories during build
- The fix is simple: one-line change to trigger fallback correctly
- Small test rebuild will validate fix in ~3 minutes
- Full rebuild will take ~6 minutes for 200 posts

**Start with todo `implement_fix_1` and follow the loop. Good luck! 🚀**
