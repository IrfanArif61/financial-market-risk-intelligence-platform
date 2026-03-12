"""
Financial Market Risk Intelligence Platform — Pipeline Orchestrator

Runs the complete data pipeline from extraction to database loading.
Each stage is executed sequentially with status reporting.

Usage:
    python run_pipeline.py
"""

import subprocess
import sys
import time


PIPELINE_STAGES = [
    {
        "name": "Stage 1 — Data Extraction",
        "script": "python/extraction/extract_market_data.py",
        "description": "Fetching historical market data from Yahoo Finance",
    },
    {
        "name": "Stage 2 — Load Raw Data to PostgreSQL",
        "script": "python/database/load_to_postgres.py",
        "description": "Loading raw market data into the data warehouse",
    },
    {
        "name": "Stage 3 — Data Transformation",
        "script": "python/transformation/clean_market_data.py",
        "description": "Cleaning and standardizing market data",
    },
    {
        "name": "Stage 4 — Calculate Returns",
        "script": "python/analytics/calculate_returns.py",
        "description": "Computing daily and cumulative returns",
    },
    {
        "name": "Stage 5 — Calculate Volatility",
        "script": "python/analytics/calculate_volatility.py",
        "description": "Computing 7-day and 30-day rolling volatility",
    },
    {
        "name": "Stage 6 — Calculate Value at Risk",
        "script": "python/analytics/calculate_var.py",
        "description": "Computing 30-day rolling VaR (95%)",
    },
    {
        "name": "Stage 7 — Portfolio Risk Metrics",
        "script": "python/analytics/calculate_portfolio_metrics.py",
        "description": "Computing weighted portfolio returns, volatility, and VaR",
    },
    {
        "name": "Stage 8 — Advanced Risk Metrics",
        "script": "python/analytics/calculate_advanced_metrics.py",
        "description": "Computing Sharpe ratio, drawdowns, and rolling correlations",
    },
    {
        "name": "Stage 9 — Load Analytics to PostgreSQL",
        "script": "python/database/load_reporting_tables.py",
        "description": "Loading final analytics tables into the reporting layer",
    },
]


def run_stage(stage: dict) -> bool:
    """Run a single pipeline stage and return True if successful."""
    print(f"\n{'=' * 60}")
    print(f"  {stage['name']}")
    print(f"  {stage['description']}")
    print(f"{'=' * 60}\n")

    start_time = time.time()

    try:
        result = subprocess.run(
            [sys.executable, stage["script"]],
            check=True,
            text=True,
        )
        elapsed = time.time() - start_time
        print(f"\n✓ {stage['name']} completed in {elapsed:.1f}s")
        return True

    except subprocess.CalledProcessError as e:
        elapsed = time.time() - start_time
        print(f"\n✗ {stage['name']} FAILED after {elapsed:.1f}s")
        print(f"  Exit code: {e.returncode}")
        return False


def main():
    print("\n" + "=" * 60)
    print("  Financial Market Risk Intelligence Platform")
    print("  Full Pipeline Execution")
    print("=" * 60)

    total_start = time.time()
    results = []

    for i, stage in enumerate(PIPELINE_STAGES, 1):
        print(f"\n[{i}/{len(PIPELINE_STAGES)}] Starting {stage['name']}...")
        success = run_stage(stage)
        results.append((stage["name"], success))

        if not success:
            print(f"\n{'!' * 60}")
            print(f"  Pipeline stopped at {stage['name']}")
            print(f"  Fix the error above and re-run the pipeline.")
            print(f"{'!' * 60}")
            break

    total_elapsed = time.time() - total_start

    print(f"\n\n{'=' * 60}")
    print("  Pipeline Summary")
    print(f"{'=' * 60}")

    for name, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"  {status}  {name}")

    passed = sum(1 for _, s in results if s)
    total = len(results)
    print(f"\n  {passed}/{total} stages completed in {total_elapsed:.1f}s")

    if all(s for _, s in results):
        print("\n  Pipeline completed successfully!")
        print("  Connect Power BI to the semantic views to explore the dashboard.")
    else:
        print("\n  Pipeline completed with errors. See above for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()
