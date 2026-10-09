"""Print run reports as Markdown, e.g. for the GitHub job summary.

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
        print(summarize(json.loads(path.read_text())))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
