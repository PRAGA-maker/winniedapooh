# TodoAgents Folder

This folder contains **one-shot task files** for dedicated agents. Each file is a self-contained, verifiable task that can be assigned to a dedicated Claude agent.

## How It Works

1. **Task files are self-contained**: Each `.md` file describes a single, verifiable task
2. **Agents pick up tasks**: When you spin up a dedicated agent, point them to a task file
3. **Delete after completion**: Agents MUST delete the task file once done (verified)

## Task File Format

Each task file follows this structure:

```markdown
# Task: [Task Name]

> **About this folder**: [boilerplate - explains this system]

**Type:** Bug fix / Enhancement / Feature
**Scope:** Which files to modify
**Verifiable:** How to confirm success

---

## Problem
What's wrong or missing

## Solution
What to do about it

## Implementation
Step-by-step code/instructions

## Verification
Commands to run to confirm it works

## Files to Modify
Explicit scope boundary

## Notes
Additional context

---

## Cleanup Instructions
**IMPORTANT**: After completing this task:
1. Verify the fix works
2. Commit changes
3. **DELETE THIS FILE**
4. Confirm deletion to user
```

## Current Tasks

| File | Description | Priority |
|------|-------------|----------|
| 005_true_async_execution.md | Refactor ThreadPoolExecutor → asyncio | Medium |
| 006_token_cost_tracking.md | Accurate cost estimates with model pricing | Medium |
| 007_source_deduplication.md | Track URLs across iterations | Medium |
| 008_better_error_messages.md | Retry logic + helpful parse errors | Medium |
| **009_max_options_filter.md** | Skip markets with >10 options (500 error fix) | **High** |
| **010_windows_unicode_fix.md** | Fix verbose mode crash on Windows | High |

## Completed Work

### 2026-01-21: Wayback Machine Validation
- `wayback_validator.py` - CDX API integration
- Citation date validation in pipeline
- 29 tests passing

### 2026-01-21: Gemini Backoff (001)
- Exponential backoff in GeminiAgentClient
- Rate limit handling

### 2026-01-21: JSON Escaping (003)
- Fixed special char escaping in setup_code

### 2026-01-21: API Tracking (004)
- Hooked actual API call counting

### 2026-01-22: Flash Preset
- Added gemini-3-flash preset for rate limit workaround

## Why This Pattern?

- **Isolation**: Each task is independent, can be done by separate agents
- **Verifiability**: Clear success criteria prevents ambiguity
- **Cleanup**: Auto-deletion keeps the folder from growing stale
- **Context transfer**: Task files contain all needed context for new agents
