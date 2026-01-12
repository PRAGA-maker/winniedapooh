#!/bin/bash
# End-to-End Test Script for Winnie Da Pooh Forecasting Pipeline
# This script stress tests the entire system from data gathering to visualization

set +e  # Continue on errors for stress testing (see all failures)
set -u  # Exit on undefined variables

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
START_DATE="2025-12-01"
END_DATE="2025-12-31"
OUTPUT_DIR="tests/e2e_outputs"
ERROR_LOG="$OUTPUT_DIR/errors.log"

# Methods and tasks to test
METHODS=("last_price" "random_baseline" "mlp_nn")
TASKS=("resolve_binary" "predict_week_out")

# Create output directory
mkdir -p "$OUTPUT_DIR"
echo "" > "$ERROR_LOG"

# Find uv command
find_uv() {
    # Try direct command first
    if command -v uv >/dev/null 2>&1; then
        echo "uv"
        return 0
    fi
    
    # Try uv.exe (Windows)
    if command -v uv.exe >/dev/null 2>&1; then
        echo "uv.exe"
        return 0
    fi
    
    # Try common Windows installation locations
    if [ -n "$USERPROFILE" ]; then
        # Windows - try .cargo/bin
        if [ -f "$USERPROFILE/.cargo/bin/uv.exe" ]; then
            echo "$USERPROFILE/.cargo/bin/uv.exe"
            return 0
        fi
        # Try AppData\Local
        if [ -f "$USERPROFILE/AppData/Local/Programs/uv/uv.exe" ]; then
            echo "$USERPROFILE/AppData/Local/Programs/uv/uv.exe"
            return 0
        fi
    fi
    
    # Try local uv installation
    if [ -f ".venv/bin/uv" ]; then
        echo ".venv/bin/uv"
        return 0
    fi
    
    return 1
}

UV_CMD=$(find_uv)
if [ -z "$UV_CMD" ]; then
    echo "Error: 'uv' command not found in PATH."
    echo "Please ensure uv is installed and available in your PATH."
    echo "You can install uv from: https://github.com/astral-sh/uv"
    echo ""
    echo "On Windows, you may need to add uv to your PATH, or run this script from PowerShell where uv is available."
    exit 1
fi

# Export UV_CMD for use in commands
export UV_CMD
echo "[INFO] Using uv command: $UV_CMD"

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
    echo "[ERROR] $1" >> "$ERROR_LOG"
}

run_command() {
    local cmd="$1"
    local description="$2"
    
    # Replace 'uv run' with the found uv command (handle both 'uv run' and variations)
    cmd=$(echo "$cmd" | sed "s|\buv run\b|$UV_CMD run|g")
    
    log_info "$description"
    if eval "$cmd"; then
        log_success "$description completed"
        return 0
    else
        log_error "$description failed (command: $cmd)"
        return 1
    fi
}

# Print header
echo "================================================================================"
echo "  Winnie Da Pooh - End-to-End Test Script"
echo "================================================================================"
echo "Date Range: $START_DATE to $END_DATE"
echo "Output Directory: $OUTPUT_DIR"
echo "================================================================================"
echo ""

# ============================================================================
# Phase 1: Data Gathering
# ============================================================================
log_info "Starting Phase 1: Data Gathering"

run_command \
    "uv run scripts/build_db.py --start $START_DATE --end $END_DATE" \
    "Building dataset for December 2025"

# Verify dataset was created
log_info "Verifying dataset creation..."
LATEST_DATASET=$(ls -td data/datasets/v*_unified 2>/dev/null | head -n 1)
if [ -z "$LATEST_DATASET" ]; then
    log_error "No dataset found! Check data/datasets/ directory."
    exit 1
else
    log_success "Dataset found: $LATEST_DATASET"
    DATASET_PATH="$LATEST_DATASET"
fi

# Inspect the dataset
run_command \
    "uv run scripts/inspect_parquet.py" \
    "Inspecting dataset statistics"

echo ""

# ============================================================================
# Phase 2: Experiment Execution
# ============================================================================
log_info "Starting Phase 2: Experiment Execution"
log_info "Running all method/task combinations (${#METHODS[@]} methods × ${#TASKS[@]} tasks = $((${#METHODS[@]} * ${#TASKS[@]})) experiments)"

EXPERIMENT_COUNT=0
FAILED_EXPERIMENTS=0

for method in "${METHODS[@]}"; do
    for task in "${TASKS[@]}"; do
        EXPERIMENT_COUNT=$((EXPERIMENT_COUNT + 1))
        RUN_NAME="e2e_test_${method}_${task}"
        
        log_info "[$EXPERIMENT_COUNT/$((${#METHODS[@]} * ${#TASKS[@]}))] Running: method=$method, task=$task"
        
        run_command \
            "uv run runner/runner.py --method $method --task $task --name $RUN_NAME --desc 'End-to-end test run'" \
            "Experiment: $RUN_NAME"
        
        if [ $? -ne 0 ]; then
            FAILED_EXPERIMENTS=$((FAILED_EXPERIMENTS + 1))
        fi
        
        echo ""
    done
done

log_info "Phase 2 complete: $EXPERIMENT_COUNT experiments run, $FAILED_EXPERIMENTS failed"
echo ""

# ============================================================================
# Phase 3: Results Analysis
# ============================================================================
log_info "Starting Phase 3: Results Analysis"

# Scan for any existing runs that might not be indexed
run_command \
    "uv run scripts/results/scan_existing_runs.py" \
    "Scanning and indexing existing runs"

# List recent runs
run_command \
    "uv run scripts/results/list_runs.py --limit 20" \
    "Listing recent runs"

echo ""

# Compare methods on each task
log_info "Comparing methods for each task..."
for task in "${TASKS[@]}"; do
    log_info "Comparing methods for task: $task"
    run_command \
        "uv run scripts/results/compare_runs.py --task $task --latest" \
        "Method comparison for $task"
    echo ""
done

# Generate visualizations
log_info "Generating visualizations..."

# Bar charts for each task and metric
for task in "${TASKS[@]}"; do
    # Test Brier Score
    run_command \
        "uv run scripts/results/plot_results.py --task $task --metric test_brier --output $OUTPUT_DIR/e2e_${task}_test_brier.png" \
        "Generating test_brier bar chart for $task"
    
    # Test Log Loss
    run_command \
        "uv run scripts/results/plot_results.py --task $task --metric test_logloss --output $OUTPUT_DIR/e2e_${task}_test_logloss.png" \
        "Generating test_logloss bar chart for $task"
    
    # Test vs Bench scatter
    run_command \
        "uv run scripts/results/plot_results.py --task $task --scatter --output $OUTPUT_DIR/e2e_${task}_scatter.png" \
        "Generating test vs bench scatter plot for $task"
    
    # Multiple metrics comparison
    run_command \
        "uv run scripts/results/plot_results.py --task $task --metrics test_brier,test_logloss --output $OUTPUT_DIR/e2e_${task}_multiple_metrics.png" \
        "Generating multiple metrics comparison for $task"
    
    echo ""
done

# Export comparison to CSV
log_info "Exporting comparison results to CSV..."
for task in "${TASKS[@]}"; do
    run_command \
        "uv run scripts/results/compare_runs.py --task $task --latest --output $OUTPUT_DIR/e2e_${task}_comparison.csv" \
        "Exporting $task comparison to CSV"
done

echo ""

# ============================================================================
# Summary
# ============================================================================
echo "================================================================================"
echo "  End-to-End Test Summary"
echo "================================================================================"
log_info "Dataset: $DATASET_PATH"
log_info "Experiments run: $EXPERIMENT_COUNT"
log_info "Failed experiments: $FAILED_EXPERIMENTS"
log_info "Output directory: $OUTPUT_DIR"
log_info "Error log: $ERROR_LOG"

if [ -f "$ERROR_LOG" ] && [ -s "$ERROR_LOG" ]; then
    ERROR_COUNT=$(grep -c "\[ERROR\]" "$ERROR_LOG" 2>/dev/null || echo "0")
    if [ "$ERROR_COUNT" -gt 0 ]; then
        log_warning "$ERROR_COUNT errors were logged. Check $ERROR_LOG for details."
    fi
fi

# List generated files
if [ -d "$OUTPUT_DIR" ]; then
    echo ""
    log_info "Generated files in $OUTPUT_DIR:"
    ls -lh "$OUTPUT_DIR" 2>/dev/null | grep -v "^total" | tail -n +2 || echo "  (no files found)"
fi

echo ""
if [ "$FAILED_EXPERIMENTS" -eq 0 ]; then
    log_success "All experiments completed successfully!"
else
    log_warning "Some experiments failed. Check the error log for details."
fi

echo "================================================================================"
echo "Test complete!"
echo "================================================================================"
