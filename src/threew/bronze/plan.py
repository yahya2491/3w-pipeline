"""Decide what to download by comparing the Hugging Face listing with what the manifest says has landed.

Pure: no network and no file access. The caller lists the source and reads the manifest, then passes both in.
"""

from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from itertools import zip_longest
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from threew.bronze.manifest import Status
from threew.bronze.naming import InstanceInfo, SourceType, parse_all
from threew.source.hub import RemoteFile

# Sampling cycles through the source types in this order, so a sample covers every type a class has
SAMPLE_ORDER = (SourceType.REAL, SourceType.SIMULATED, SourceType.DRAWN)


class Reason(StrEnum):
    NEW = "NEW"
    CHANGED = "CHANGED"


def content_key(sha256: str | None, blob_id: str) -> str:
    """What identifies a file's content: its sha256, or blob:<git id> when Hugging Face gives no sha256."""
    return sha256 if sha256 else f"blob:{blob_id}"


@dataclass(frozen=True)
class Candidate:
    """Everything known about one file before downloading it."""

    remote: RemoteFile
    info: InstanceInfo

    @property
    def path(self) -> str:
        return self.remote.path

    @property
    def content_key(self) -> str:
        return content_key(self.remote.sha256, self.remote.blob_id)


@dataclass(frozen=True)
class Plan:
    to_download: tuple[tuple[Candidate, Reason], ...]  # sorted by path
    unchanged: int
    removed_upstream: tuple[str, ...]  # landed before, no longer in the listing
    rejected: tuple[tuple[str, str], ...]  # (path, reason) for names that don't parse

    def summary(self) -> dict[str, Any]:
        reasons = Counter(reason for _, reason in self.to_download)
        per_class = Counter(candidate.info.label for candidate, _ in self.to_download)
        return {
            "new": reasons[Reason.NEW],
            "changed": reasons[Reason.CHANGED],
            "unchanged": self.unchanged,
            "removed_upstream": len(self.removed_upstream),
            "rejected": len(self.rejected),
            "files_to_download": len(self.to_download),
            "bytes_to_download": sum(candidate.remote.size for candidate, _ in self.to_download),
            "files_per_class": {label: per_class[label] for label in sorted(per_class)},
        }


def build_plan(listing: Sequence[RemoteFile], current_manifest: pa.Table, sample_per_class: int | None) -> Plan:
    """Plan a run: which listed files are NEW or CHANGED compared with the manifest.

    current_manifest is Manifest.current(): one LANDED row per path. Rows with another status are ignored, so a
    file that only ever FAILED comes back as NEW. sample_per_class=None plans every file.
    """
    if not listing:
        raise ValueError("the listing is empty: wrong revision or include pattern? Refusing to plan an empty run")
    paths = [f.path for f in listing]
    if len(set(paths)) != len(paths):
        raise ValueError("the listing has duplicate paths")

    remote_by_path = {f.path: f for f in listing}
    parsed, rejected = parse_all(paths)
    candidates = _sample([Candidate(remote_by_path[info.path], info) for info in parsed], sample_per_class)
    landed = _landed_content_keys(current_manifest)

    to_download: list[tuple[Candidate, Reason]] = []
    unchanged = 0
    for candidate in candidates:
        previous = landed.get(candidate.path)
        if previous is None:
            to_download.append((candidate, Reason.NEW))
        elif previous != candidate.content_key:
            to_download.append((candidate, Reason.CHANGED))
        else:
            unchanged += 1

    # Against the full listing, not the sample: a file left out of a dev sample still exists upstream
    removed_upstream = sorted(landed.keys() - set(paths))
    return Plan(tuple(to_download), unchanged, tuple(removed_upstream), tuple(rejected))


def _sample(candidates: list[Candidate], per_class: int | None) -> list[Candidate]:
    """Up to per_class files of each class, taken in rounds: the next REAL, SIMULATED and DRAWN file each round.

    So a class gets one file of each type it has before any type gets a second. Within a type, files go by path,
    which makes the sample deterministic: same listing, same pick.
    """
    if per_class is None:
        return sorted(candidates, key=lambda c: c.path)
    if per_class < 1:
        raise ValueError(f"sample_per_class must be at least 1 or None, got {per_class}")
    by_label_and_type: dict[int, dict[SourceType, list[Candidate]]] = defaultdict(lambda: defaultdict(list))
    for candidate in candidates:
        by_label_and_type[candidate.info.label][candidate.info.source_type].append(candidate)
    picked = []
    for label in sorted(by_label_and_type):
        by_type = by_label_and_type[label]
        queues = [sorted(by_type[t], key=lambda c: c.path) for t in SAMPLE_ORDER if by_type[t]]
        in_rounds = [c for round_ in zip_longest(*queues) for c in round_ if c is not None]
        picked.extend(in_rounds[:per_class])
    return sorted(picked, key=lambda c: c.path)


def _landed_content_keys(current_manifest: pa.Table) -> dict[str, str]:
    landed = current_manifest.filter(pc.equal(current_manifest["status"], Status.LANDED.value))
    keys: dict[str, str] = {}
    columns = (landed["path"].to_pylist(), landed["sha256"].to_pylist(), landed["blob_id"].to_pylist())
    for path, sha256, blob_id in zip(*columns, strict=True):
        if path in keys:
            raise ValueError(f"{path} has more than one LANDED row: pass Manifest.current(), not Manifest.read()")
        keys[path] = content_key(sha256, blob_id)
    return keys
