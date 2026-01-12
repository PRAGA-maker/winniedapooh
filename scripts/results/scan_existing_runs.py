#!/usr/bin/env python3
"""
Scan and index existing experiment runs into the database.

This utility is useful for:
- Indexing runs that were created before the database system existed
- Re-indexing runs after database schema changes
- Recovering from database corruption

Usage examples:
    # Scan and index all runs in data/outputs/
    uv run scripts/scan_existing_runs.py
    
    # Dry run (show what would be indexed)
    uv run scripts/scan_existing_runs.py --dry-run
    
    # Re-index everything (clear database first)
    uv run scripts/scan_existing_runs.py --reindex
"""

import sys
import json
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from runner.results_db import ResultsDatabase


def find_run_directories(base_path: Path = Path("data/outputs")) -> list:
    """
    Find all run directories in the outputs folder.
    
    A run directory is identified by having a metrics/ subdirectory with
    spec.json and metrics.json files.
    
    Args:
        base_path: Base directory to search (default: data/outputs)
        
    Returns:
        List of (run_dir, spec_path, metrics_path) tuples
    """
    if not base_path.exists():
        return []
    
    runs = []
    
    # Iterate through method folders
    for method_folder in base_path.iterdir():
        if not method_folder.is_dir():
            continue
        
        # Iterate through run folders
        for run_folder in method_folder.iterdir():
            if not run_folder.is_dir():
                continue
            
            # Check if this looks like a run directory
            metrics_dir = run_folder / "metrics"
            if not metrics_dir.exists():
                continue
            
            spec_path = metrics_dir / "spec.json"
            metrics_path = metrics_dir / "metrics.json"
            
            if spec_path.exists() and metrics_path.exists():
                runs.append((run_folder, spec_path, metrics_path))
    
    return runs


def load_json_file(path: Path) -> dict:
    """Load and parse a JSON file."""
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {path}: {e}")
        return {}


def scan_and_index(dry_run: bool = False, reindex: bool = False):
    """
    Scan the outputs directory and index all runs.
    
    Args:
        dry_run: If True, only show what would be indexed
        reindex: If True, clear database before indexing
    """
    # Find all run directories
    print("Scanning for run directories...")
    runs = find_run_directories()
    
    if not runs:
        print("No run directories found in data/outputs/")
        return
    
    print(f"Found {len(runs)} run directories")
    print()
    
    if dry_run:
        print("DRY RUN - would index the following runs:")
        print()
        for run_dir, spec_path, metrics_path in runs:
            spec = load_json_file(spec_path)
            print(f"  {run_dir.parent.name}/{run_dir.name}")
            print(f"    Method: {spec.get('method', 'unknown')}")
            print(f"    Task:   {spec.get('task', 'unknown')}")
            print()
        return
    
    # Initialize database
    db = ResultsDatabase()
    
    # Reindex if requested
    if reindex:
        print("Clearing existing database...")
        db_path = db.db_path
        if db_path.exists():
            db_path.unlink()
        # Reinitialize with empty database
        db = ResultsDatabase()
        print("Database cleared")
        print()
    
    # Index each run
    indexed_count = 0
    failed_count = 0
    
    print("Indexing runs...")
    for run_dir, spec_path, metrics_path in runs:
        try:
            spec = load_json_file(spec_path)
            metrics = load_json_file(metrics_path)
            
            if not spec or not metrics:
                print(f"  Skipping {run_dir.name} - invalid spec or metrics")
                failed_count += 1
                continue
            
            db.index_run(run_dir, spec, metrics)
            print(f"  Indexed: {run_dir.parent.name}/{run_dir.name}")
            indexed_count += 1
            
        except Exception as e:
            print(f"  Error indexing {run_dir.name}: {e}")
            failed_count += 1
    
    print()
    print("=" * 60)
    print(f"Indexing complete!")
    print(f"  Successfully indexed: {indexed_count}")
    print(f"  Failed:              {failed_count}")
    print(f"  Total runs in DB:    {db.count_runs()}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Scan and index existing experiment runs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("--dry-run", action="store_true", 
                       help="Show what would be indexed without actually indexing")
    parser.add_argument("--reindex", action="store_true",
                       help="Clear database and re-index everything")
    
    args = parser.parse_args()
    
    if args.reindex:
        response = input("Are you sure you want to clear the database and re-index? (yes/no): ")
        if response.lower() != "yes":
            print("Aborted.")
            return
    
    scan_and_index(dry_run=args.dry_run, reindex=args.reindex)


if __name__ == "__main__":
    main()


# LESSONS LEARNED:
# 1. Identifying run directories by the presence of metrics/spec.json and metrics/metrics.json
#    is more robust than relying on naming conventions alone.
# 2. Dry run mode is essential for letting users preview what will happen before
#    making changes to the database.
# 3. Requiring explicit "yes" confirmation for destructive operations (--reindex)
#    prevents accidental data loss.
# 4. Showing progress and summary statistics helps users understand what happened.
# 5. Graceful error handling (try/except per run) means one bad run doesn't break
#    the entire indexing process.
