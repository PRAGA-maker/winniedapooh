#!/usr/bin/env python
"""
Quick test runner for Winnie Da Pooh.
Runs the full test suite and generates a summary report.
"""
import subprocess
import sys
from pathlib import Path
from datetime import datetime


def run_tests():
    """Run the full test suite."""
    print("="*80)
    print("Winnie Da Pooh - Test Suite Runner")
    print("="*80)
    print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Run pytest with verbose output
    result = subprocess.run(
        ["uv", "run", "pytest", "tests/", "-v", "--tb=short"],
        capture_output=True,
        text=True
    )
    
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    
    # Parse results
    if "passed" in result.stdout:
        # Extract test counts
        lines = result.stdout.split('\n')
        for line in lines:
            if "passed" in line and "warning" in line:
                print(f"\n{'='*80}")
                print("TEST SUMMARY")
                print("="*80)
                print(line)
                print()
                
                if result.returncode == 0:
                    print("ALL TESTS PASSED")
                else:
                    print("SOME TESTS FAILED")
                
                break
    
    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return result.returncode


def main():
    """Main entry point."""
    exit_code = run_tests()
    
    if exit_code == 0:
        print("\n" + "="*80)
        print("Next Steps:")
        print("="*80)
        print("1. Review TESTING_REPORT.md for detailed findings")
        print("2. Run 'uv run scripts/build_db.py' to build full dataset")
        print("3. Run 'uv run runner/runner.py' to test forecasting models")
        print("="*80)
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
