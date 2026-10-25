"""Print Bronze or Silver run reports as Markdown, e.g. for the GitHub job summary.

Usage:
    python scripts/report_summary.py out/*.json >> "$GITHUB_STEP_SUMMARY"
"""

import json
import sys
from pathlib import Path

from threew.bronze.summary import summarize


def main(paths: list[str]) -> int:
    reports = [Path(p) for p in paths if Path(p).is_file()]
    if not reports:  # e.g. the run never started because the environment couldn't be built
        print("## ❌ No run report was produced\n\nSee the job log for what happened before the run.")
        return 0
    for path in reports:
        report = json.loads(path.read_text())
        if "run_id" not in report:
            print(summarize(report))
            continue
        print(f"## Silver: {report['status']}")
        print(f"\nRun: `{report['run_id']}`; environment: `{report['env']}`")
        if report.get("skip_reason"):
            print(f"\nReason: {report['skip_reason']}; reused run: `{report.get('reused_run_id', '')}`")
        counts = report.get("reconcile", {}).get("counts", {})
        if counts:
            print(f"\nObservations: {counts.get('silver_observation_rows', 0):,}")
        if report.get("error"):
            print(f"\nError: {report['error']['message']}")
        print(f"\nDuration: {report.get('duration_seconds', 0):,.1f} seconds")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
