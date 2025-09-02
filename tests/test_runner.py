#!/usr/bin/env python3
"""Test runner script for the Loan Analytics Dashboard.

This script provides a convenient way to run different test suites
and generate reports.
"""

import sys
import subprocess
import argparse
from pathlib import Path


def run_command(cmd: list[str], description: str) -> int:
    """Run a command and return the exit code."""
    print(f"\n🔄 {description}")
    print(f"Running: {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=False)
    
    if result.returncode == 0:
        print(f"✅ {description} - SUCCESS")
    else:
        print(f"❌ {description} - FAILED")
    
    return result.returncode


def run_tests(test_type: str = "all") -> int:
    """Run the specified test suite."""
    base_cmd = ["python", "-m", "pytest"]
    
    if test_type == "unit":
        cmd = base_cmd + ["tests/unit", "-v"]
        description = "Running unit tests"
    elif test_type == "integration":
        cmd = base_cmd + ["tests/integration", "-v"]
        description = "Running integration tests"
    elif test_type == "fast":
        cmd = base_cmd + ["-m", "not slow", "-v"]
        description = "Running fast tests"
    elif test_type == "slow":
        cmd = base_cmd + ["-m", "slow", "-v"]
        description = "Running slow tests"
    elif test_type == "coverage":
        cmd = base_cmd + [
            "--cov=src",
            "--cov-report=html:htmlcov",
            "--cov-report=term-missing",
            "--cov-report=xml",
        ]
        description = "Running tests with coverage"
    else:  # all
        cmd = base_cmd + ["-v"]
        description = "Running all tests"
    
    return run_command(cmd, description)


def run_linting() -> int:
    """Run code linting and type checking."""
    exit_codes = []
    
    # Run ruff
    exit_codes.append(run_command(
        ["python", "-m", "ruff", "check", "src", "tests"],
        "Running ruff linting"
    ))
    
    # Run mypy
    exit_codes.append(run_command(
        ["python", "-m", "mypy", "src"],
        "Running mypy type checking"
    ))
    
    return max(exit_codes)


def run_formatting() -> int:
    """Run code formatting."""
    exit_codes = []
    
    # Run black
    exit_codes.append(run_command(
        ["python", "-m", "black", "src", "tests"],
        "Running black formatting"
    ))
    
    # Run ruff with fix
    exit_codes.append(run_command(
        ["python", "-m", "ruff", "check", "--fix", "src", "tests"],
        "Running ruff auto-fixes"
    ))
    
    return max(exit_codes)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Test runner for Loan Analytics Dashboard"
    )
    parser.add_argument(
        "command",
        choices=["test", "lint", "format", "coverage", "all"],
        help="Command to run"
    )
    parser.add_argument(
        "--test-type",
        choices=["all", "unit", "integration", "fast", "slow"],
        default="all",
        help="Type of tests to run (only applies to 'test' command)"
    )
    
    args = parser.parse_args()
    
    print("🧪 Loan Analytics Dashboard - Test Runner")
    print("=" * 50)
    
    if args.command == "test":
        return run_tests(args.test_type)
    elif args.command == "lint":
        return run_linting()
    elif args.command == "format":
        return run_formatting()
    elif args.command == "coverage":
        return run_tests("coverage")
    elif args.command == "all":
        # Run formatting, linting, and tests
        exit_codes = [
            run_formatting(),
            run_linting(),
            run_tests("all")
        ]
        return max(exit_codes)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
