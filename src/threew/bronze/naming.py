"""Parse 3W file paths into instance fields: label, source type, well, start time and instance ID.

    data/2/WELL-00002_20131104004101.parquet -> label 2, REAL, well 2, 2013-11-04 00:41:01, WELL-00002_20131104004101

The path is the unique key: the same instance ID (e.g. SIMULATED_00001) appears in several label folders.

Known limitation: instance_start has no timezone. 3W doesn't say which timezone its timestamps use, so none is
assumed here (in particular not UTC). Silver decides how to handle it.
"""

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

# re.ASCII: without it \d also matches non-ASCII digits such as "٢", which int() would accept
PATH_RE = re.compile(r"data/(\d)/([^/]+)\.parquet", re.ASCII)
REAL_RE = re.compile(r"WELL-(\d{5})_(\d{14})", re.ASCII)
SIMULATED_RE = re.compile(r"SIMULATED_(\d{5})", re.ASCII)
DRAWN_RE = re.compile(r"DRAWN_(\d{5})", re.ASCII)
TIMESTAMP_FORMAT = "%Y%m%d%H%M%S"


class SourceType(StrEnum):
    REAL = "REAL"
    SIMULATED = "SIMULATED"
    DRAWN = "DRAWN"


@dataclass(frozen=True)
class InstanceInfo:
    path: str
    label: int
    source_type: SourceType
    well_id: int | None  # REAL only
    instance_start: datetime | None  # REAL only; naive, see the module docstring
    instance_id: str


class NameParseError(ValueError):
    def __init__(self, path: str, reason: str) -> None:
        super().__init__(f"{path}: {reason}")
        self.path = path
        self.reason = reason


def parse_path(path: str) -> InstanceInfo:
    """Parse one repo path, or raise NameParseError with the reason it doesn't fit."""
    # fullmatch, not match + "$": "$" would also accept a trailing newline
    m = PATH_RE.fullmatch(path)
    if m is None:
        raise NameParseError(path, _why_path_does_not_match(path))
    label, name = int(m[1]), m[2]

    if name.startswith("WELL-"):
        n = REAL_RE.fullmatch(name)
        if n is None:
            raise NameParseError(path, "malformed real name, expected WELL-<5 digits>_<14-digit timestamp>")
        return InstanceInfo(path, label, SourceType.REAL, int(n[1]), _parse_timestamp(path, n[2]), name)
    if name.startswith("SIMULATED_"):
        if SIMULATED_RE.fullmatch(name) is None:
            raise NameParseError(path, "malformed simulated name, expected SIMULATED_<5 digits>")
        return InstanceInfo(path, label, SourceType.SIMULATED, None, None, name)
    if name.startswith("DRAWN_"):
        if DRAWN_RE.fullmatch(name) is None:
            raise NameParseError(path, "malformed drawn name, expected DRAWN_<5 digits>")
        return InstanceInfo(path, label, SourceType.DRAWN, None, None, name)
    raise NameParseError(path, "unknown prefix, expected WELL-, SIMULATED_ or DRAWN_")


def parse_all(paths: Iterable[str]) -> tuple[list[InstanceInfo], list[tuple[str, str]]]:
    """Parse every path; a bad name goes to the rejects as (path, reason) instead of stopping the batch."""
    parsed: list[InstanceInfo] = []
    rejected: list[tuple[str, str]] = []
    for path in paths:
        try:
            parsed.append(parse_path(path))
        except NameParseError as e:
            rejected.append((e.path, e.reason))
    return parsed, rejected


def _parse_timestamp(path: str, ts: str) -> datetime:
    try:
        parsed = datetime.strptime(ts, TIMESTAMP_FORMAT)  # noqa: DTZ007  naive on purpose, 3W gives no timezone
    except ValueError as e:
        raise NameParseError(path, f"invalid timestamp {ts!r}: {e}") from None
    if parsed.strftime(TIMESTAMP_FORMAT) != ts:  # strptime must have read every digit as written
        raise NameParseError(path, f"invalid timestamp {ts!r}")
    return parsed


def _why_path_does_not_match(path: str) -> str:
    """A specific reason for a path that doesn't match data/<digit>/<name>.parquet."""
    parts = path.split("/")
    if parts[0] != "data":
        return "not under data/"
    if len(parts) < 3:
        return "no label folder"
    if len(parts) > 3:
        return "nested path, expected data/<digit>/<name>.parquet"
    if not re.fullmatch(r"\d", parts[1], re.ASCII):
        return f"label folder {parts[1]!r} is not a single digit"
    if not parts[2].endswith(".parquet"):
        return "not a .parquet file"
    return "does not match data/<digit>/<name>.parquet"
