"""Bound Spark work by complete recordings without changing Silver's row grain."""

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from threew.silver.reconcile import RECORDING_KEY

DEFAULT_BATCH_ROWS = 1_000_000


@dataclass(frozen=True)
class RecordingBatch:
    metadata: list[dict[str, Any]]
    counts: list[dict[str, Any]]

    @property
    def rows(self) -> int:
        return sum(row["bronze_rows"] for row in self.counts)


def plan_batches(
    metadata: Sequence[Mapping[str, Any]], counts: Sequence[Mapping[str, Any]], target_rows: int
) -> list[RecordingBatch]:
    """Pack recordings in source-key order; an oversized recording runs alone."""
    if type(target_rows) is not int or target_rows <= 0:
        raise ValueError("batch_rows must be a positive integer.")

    def keyed(rows):
        result, paths = {}, set()
        for row in rows:
            key = tuple(row[name] for name in RECORDING_KEY)
            if any(not isinstance(value, str) or not value for value in key) or key[0] in paths:
                raise ValueError("The recording catalogue must have one non-null current version per source path.")
            result[key] = dict(row)
            paths.add(key[0])
        return result

    details, sizes = keyed(metadata), keyed(counts)
    if not details or details.keys() != sizes.keys():
        raise ValueError("Recording metadata and counts must describe the same non-empty catalogue.")
    batches, pending, pending_rows = [], [], 0
    for key in sorted(sizes):
        size = sizes[key]["bronze_rows"]
        if type(size) is not int or size < 0:
            raise ValueError("Recording row counts must be non-negative integers.")
        if pending and pending_rows + size > target_rows:
            batches.append(RecordingBatch([details[k] for k in pending], [sizes[k] for k in pending]))
            pending, pending_rows = [], 0
        pending.append(key)
        pending_rows += size
    if pending:
        batches.append(RecordingBatch([details[k] for k in pending], [sizes[k] for k in pending]))
    return batches


def merge_quality_summaries(parts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Combine exact counts from batches with disjoint recording/observation keys."""
    totals = {
        name: sum(part[name] for part in parts)
        for name in (
            "observation_rows",
            "issue_entries",
            "rows_with_issues",
            "quarantine_issue_entries",
            "rows_with_quarantine_issues",
        )
    }
    rules, recording, sensors = Counter(), Counter(), {}
    for part in parts:
        recording.update(part["recording_totals"])
        for row in part["by_rule"]:
            rules[(row["issue_code"], row["severity"], row["affected_use"])] += row["count"]
        for row in part["sensor_availability"]:
            sensor = sensors.setdefault(row["sensor_name"], Counter())
            sensor.update({name: value for name, value in row.items() if name != "sensor_name"})
    quarantined, rows = totals["rows_with_quarantine_issues"], totals["observation_rows"]
    return {
        **totals,
        "quarantine_row_percent": round(100 * quarantined / rows, 4) if rows else 0.0,
        "has_quality_errors": quarantined > 0,
        "by_rule": [
            {"issue_code": code, "severity": severity, "affected_use": use, "count": count}
            for (code, severity, use), count in sorted(rules.items())
        ],
        "recording_totals": dict(recording),
        "sensor_availability": [{"sensor_name": name, **sensors[name]} for name in sorted(sensors)],
    }
