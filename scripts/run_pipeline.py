from pathlib import Path
import sys

# Make the project's src/ directory importable without requiring package installation.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ecommerce_de.pipeline import run_pipeline  # noqa: E402


if __name__ == "__main__":
    result = run_pipeline(PROJECT_ROOT)
    print("\nPIPELINE SUMMARY")
    print("Status:", result["status"])
    print("Source tables:", result["summary"]["source_tables"])
    print("Checks passed:", result["summary"]["checks_passed"])
    print("Warnings:", result["summary"]["checks_warned"])
    print("Failures:", result["summary"]["checks_failed"])
    print("Report: reports/data_quality_report.json")
