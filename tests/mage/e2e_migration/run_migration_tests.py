#!/usr/bin/env python3
"""
Simple migration test runner for e2e_migration tests.
This script runs pytest directly without Docker management.
Docker containers are managed by test_e2e_migration.py.
"""

import argparse
import subprocess
import sys


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run e2e migration tests")
    parser.add_argument(
        "-k",
        dest="test_filter",
        help="Filter tests by database type (e.g., 'mysql', 'postgresql')",
        type=str,
        required=False,
    )
    return parser.parse_args()


def main():
    """Main function to run migration tests."""
    args = parse_arguments()

    # Build pytest command
    pytest_cmd = ["python3", "-m", "pytest", "test_migration.py", "-v", "--tb=short"]

    # Add filter if provided
    if args.test_filter:
        pytest_cmd.extend(["-k", args.test_filter])

    # Run pytest
    result = subprocess.run(pytest_cmd)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
