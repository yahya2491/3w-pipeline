"""Prove the Bronze table holds exactly what landed: six checks against the manifest and the landing files.

1 Schema         Spark's schema is the expected one, partition columns included
2 Total rows     count() equals the manifest's num_rows total
3 Per file       every (file, version) has the manifest's row count; no file missing, none extra
4 Partitions     each file sits in the source_type= and label= folders the manifest says
5 Row order      per file, _row_in_file is exactly 0 .. n-1: no row duplicated or missing. Fast mode checks count,
                 min, max, sum and sum of squares, all cheap; deep mode adds countDistinct, which proves it
6 Fidelity       for one REAL, one SIMULATED and one DRAWN file, every value Spark reads from Bronze equals what
                 pyarrow reads from the landing file, row by row, nulls included

Checks 3, 4 and 5 share one pass: a groupBy over the whole table, joined with the manifest (about 2,228 rows) as a
broadcast, so the big side is never shuffled for the join.
"""

import io
import math
import time
from collections import defaultdict
from contextlib import redirect_stdout
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DataType,
    DoubleType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from threew.bronze.inspect import load_contract
from threew.bronze.manifest import Manifest
from threew.config import Config

# Contract types (as pyarrow prints them) -> what Spark reads from the Bronze files
CONTRACT_TO_SPARK: dict[str, DataType] = {
    "double": DoubleType(),
    "int16": ShortType(),
    "timestamp[ns]": TimestampNTZType(),  # converted to timestamp[us] without a time zone: Spark's timestamp_ntz
}
LINEAGE_SCHEMA = [
    StructField("_source_path", StringType()),
    StructField("_file_sha256", StringType()),
    StructField("_ingest_run_id", StringType()),
    StructField("_row_in_file", LongType()),
]
PARTITION_SCHEMA = [StructField("source_type", StringType()), StructField("label", IntegerType())]
KEYS = ["_source_path", "_file_sha256"]
FIDELITY_TYPES = ("REAL", "SIMULATED", "DRAWN")
MAX_LISTED = 20  # problems listed per check; the counts are always complete


@dataclass
class CheckResult:
    number: int
    name: str
    passed: bool
    seconds: float
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ReconcileResult:
    env: str
    bronze_path: str
    mode: str  # "fast" or "deep"
    excluded: int  # files left out on purpose, e.g. because their conversion failed in this run
    checks: list[CheckResult]
    spark_seconds: float
    plan: str  # formatted physical plan of the checks 3-5 query
    profile: dict[str, float] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["passed"] = self.passed
        d["plan_has_broadcast_hash_join"] = "BroadcastHashJoin" in self.plan
        del d["plan"]  # saved separately as text
        return d


def expected_schema(contract: dict[str, str]) -> StructType:
    """What Spark should read: the contract's columns in file order, the lineage columns, then the partitions."""
    columns = [StructField(name, CONTRACT_TO_SPARK[type_]) for name, type_ in contract.items()]
    return StructType(columns + LINEAGE_SCHEMA + PARTITION_SCHEMA)


def schema_differences(actual: StructType, expected: StructType) -> list[str]:
    """Differences in column names, order and types (nullability is ignored: Parquet columns are all nullable)."""
    a = [(f.name, f.dataType.simpleString()) for f in actual]
    e = [(f.name, f.dataType.simpleString()) for f in expected]
    if a == e:
        return []
    problems = []
    a_types, e_types = dict(a), dict(e)
    problems += [f"missing column {n} ({t})" for n, t in e if n not in a_types]
    problems += [f"unexpected column {n} ({t})" for n, t in a if n not in e_types]
    problems += [f"{n}: expected {e_types[n]}, got {t}" for n, t in a if n in e_types and e_types[n] != t]
    if not problems:
        problems.append(f"column order differs: expected {[n for n, _ in e]}, got {[n for n, _ in a]}")
    return problems


def reconcile(
    spark: SparkSession,
    cfg: Config,
    manifest: Manifest,
    *,
    deep: bool = True,
    profile: bool = False,
    exclude: frozenset[tuple[str, str]] = frozenset(),
) -> ReconcileResult:
    """Run the six checks.

    deep=False skips countDistinct (see check 5). profile=True also times the checks 3-5 pass the other way, for
    comparison. exclude lists (path, sha256) pairs the table isn't expected to hold.
    """
    spark_start = time.monotonic()
    contract = load_contract(cfg.schema.contract)
    # The files Bronze should hold: the current LANDED version of each path that matched the contract
    current = [r for r in manifest.current().to_pylist() if r["schema_ok"] is True]
    expected = [r for r in current if (r["path"], r["sha256"]) not in exclude]
    by_key = {(r["path"], r["sha256"]): r for r in expected}
    bronze = spark.read.parquet(str(cfg.storage.bronze))
    checks: list[CheckResult] = []

    # 1 Schema
    start = time.monotonic()
    problems = schema_differences(bronze.schema, expected_schema(contract))
    checks.append(CheckResult(1, "schema", not problems, _since(start), {"differences": problems}))

    # 2 Total rows
    start = time.monotonic()
    total = bronze.count()
    expected_total = sum(r["num_rows"] for r in expected)
    checks.append(
        CheckResult(
            2, "total rows", total == expected_total, _since(start), {"spark": total, "manifest": expected_total}
        )
    )

    # 3, 4, 5: one pass over the table, joined with the broadcast manifest
    start = time.monotonic()
    per_file = per_file_stats(bronze, distinct=deep)
    joined = per_file.join(F.broadcast(manifest_frame(spark, expected)), on=KEYS, how="left")
    groups = joined.collect()
    pass_seconds = _since(start)
    plan = _explain(joined)
    checks += _per_file_checks(groups, by_key, pass_seconds, deep)

    if profile:  # the same pass the other way, to see what the distinct count costs
        start = time.monotonic()
        per_file_stats(bronze, distinct=not deep).collect()
        other = _since(start)
        with_distinct, without = (pass_seconds, other) if deep else (other, pass_seconds)
        profile_data = {"with_count_distinct_seconds": with_distinct, "without_count_distinct_seconds": without}
    else:
        profile_data = {}

    # 6 Fidelity
    start = time.monotonic()
    checks.append(_fidelity_check(bronze, cfg, contract, expected, start))
    mode = "deep" if deep else "fast"
    excluded = len(current) - len(expected)
    seconds = _since(spark_start)
    return ReconcileResult(cfg.env, str(cfg.storage.bronze), mode, excluded, checks, seconds, plan, profile_data)


def per_file_stats(bronze: DataFrame, distinct: bool = True) -> DataFrame:
    """Per file version and partition: rows, and the min, max, sum and sum of squares of the row numbers
    (with distinct=True also the number of distinct row numbers)."""
    aggs = [
        F.count(F.lit(1)).alias("rows"),
        F.min("_row_in_file").alias("min_row"),
        F.max("_row_in_file").alias("max_row"),
        F.sum("_row_in_file").alias("sum_row"),
        F.sum(F.col("_row_in_file") * F.col("_row_in_file")).alias("sum_sq_row"),
    ]
    if distinct:
        aggs.append(F.countDistinct("_row_in_file").alias("distinct_rows"))
    return bronze.groupBy(*KEYS, "source_type", "label").agg(*aggs)


def manifest_frame(spark: SparkSession, rows: list[dict[str, Any]]) -> DataFrame:
    schema = StructType(
        [
            StructField("_source_path", StringType()),
            StructField("_file_sha256", StringType()),
            StructField("m_source_type", StringType()),
            StructField("m_label", IntegerType()),
            StructField("m_num_rows", LongType()),
        ]
    )
    data = [(r["path"], r["sha256"], r["source_type"], r["label"], r["num_rows"]) for r in rows]
    return spark.createDataFrame(data, schema)


def _row_numbers_ok(g, deep: bool) -> bool:
    """0 .. n-1 has min 0, max n-1, sum n(n-1)/2 and sum of squares (n-1)n(2n-1)/6; deep also counts distinct."""
    n = g["rows"]
    ok = (
        g["min_row"] == 0
        and g["max_row"] == n - 1
        and g["sum_row"] == n * (n - 1) // 2
        and g["sum_sq_row"] == (n - 1) * n * (2 * n - 1) // 6
    )
    return ok and (not deep or g["distinct_rows"] == n)


def _per_file_checks(groups, by_key: dict, pass_seconds: float, deep: bool) -> list[CheckResult]:
    in_bronze: dict[tuple, list] = defaultdict(list)
    for g in groups:
        in_bronze[(g["_source_path"], g["_file_sha256"])].append(g)

    missing = sorted(k for k in by_key if k not in in_bronze)
    extra = sorted(k for k, gs in in_bronze.items() if gs[0]["m_num_rows"] is None)
    mismatches, partition_problems, order_problems = [], [], []
    for key, gs in sorted(in_bronze.items()):
        for g in gs:  # 5: within one file, row numbers are exactly 0 .. n-1
            if not _row_numbers_ok(g, deep):
                problem = {"file": list(key), "rows": g["rows"], "min": g["min_row"], "max": g["max_row"]}
                problem |= {"sum": g["sum_row"], "distinct": g["distinct_rows"] if deep else None}
                order_problems.append(problem)
        if key not in by_key:
            continue
        rows = sum(g["rows"] for g in gs)
        if rows != by_key[key]["num_rows"]:  # 3
            mismatches.append({"file": list(key), "spark": rows, "manifest": by_key[key]["num_rows"]})
        found = sorted((g["source_type"], g["label"]) for g in gs)
        wanted = [(by_key[key]["source_type"], by_key[key]["label"])]
        if found != wanted:  # 4
            partition_problems.append({"file": list(key), "found": found, "manifest": wanted[0]})

    shared = {"shared_pass": "checks 3-5", "files_in_bronze": len(in_bronze), "files_in_manifest": len(by_key)}
    return [
        CheckResult(
            3,
            "per file",
            not (missing or extra or mismatches),
            pass_seconds,
            {
                **shared,
                "missing": len(missing),
                "extra": len(extra),
                "mismatches": len(mismatches),
                "missing_files": [list(k) for k in missing[:MAX_LISTED]],
                "extra_files": [list(k) for k in extra[:MAX_LISTED]],
                "mismatched_files": mismatches[:MAX_LISTED],
            },
        ),
        CheckResult(
            4,
            "partitions",
            not partition_problems,
            pass_seconds,
            {**shared, "problems": len(partition_problems), "files": partition_problems[:MAX_LISTED]},
        ),
        CheckResult(
            5,
            "row order",
            not order_problems,
            pass_seconds,
            {
                **shared,
                "mode": "deep (with countDistinct)" if deep else "fast (count, min, max, sum, sum of squares)",
                "problems": len(order_problems),
                "files": order_problems[:MAX_LISTED],
            },
        ),
    ]


def _fidelity_check(bronze: DataFrame, cfg: Config, contract: dict[str, str], expected, start: float) -> CheckResult:
    """Row-by-row comparison for the first file (by path) of each source type."""
    files, problems = [], []
    for source_type in FIDELITY_TYPES:
        candidates = sorted((r for r in expected if r["source_type"] == source_type), key=lambda r: r["path"])
        if not candidates:
            continue
        row = candidates[0]
        source = pq.read_table(cfg.storage.root / row["landing_path"], columns=list(contract))
        # The conversion's cast is exact (safe=True), so comparing at microseconds compares the same instants
        source = source.set_column(
            source.schema.get_field_index("timestamp"),
            "timestamp",
            pc.cast(source["timestamp"], pa.timestamp("us"), safe=True),
        )
        spark_rows = (
            bronze.where(
                (F.col("source_type") == row["source_type"])  # partition pruning: only this folder is read
                & (F.col("label") == row["label"])
                & (F.col("_source_path") == row["path"])
                & (F.col("_file_sha256") == row["sha256"])
            )
            .orderBy("_row_in_file")
            .select(*[F.col(f"`{name}`") for name in contract])
            .collect()
        )
        differences = _compare(source, spark_rows, list(contract))
        files.append({"path": row["path"], "rows": source.num_rows, "values": source.num_rows * len(contract)})
        problems += [{"path": row["path"], **d} for d in differences]
    passed = not problems and len(files) > 0
    return CheckResult(6, "fidelity", passed, _since(start), {"files": files, "differences": problems[:MAX_LISTED]})


def _compare(source: pa.Table, spark_rows, columns: list[str]) -> list[dict[str, Any]]:
    if source.num_rows != len(spark_rows):
        return [{"problem": f"pyarrow read {source.num_rows} rows, Spark read {len(spark_rows)}"}]
    differences = []
    for name in columns:
        for i, (a, b) in enumerate(zip(source[name].to_pylist(), (r[name] for r in spark_rows), strict=True)):
            if not _same(a, b):
                differences.append({"column": name, "row": i, "landing": repr(a), "bronze": repr(b)})
                if len(differences) >= MAX_LISTED:
                    return differences
    return differences


def _same(a: Any, b: Any) -> bool:
    if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
        return True  # NaN never equals itself, but two NaNs are the same value here
    if isinstance(a, datetime) and isinstance(b, datetime) and (a.tzinfo or b.tzinfo):
        return False  # both sides must be naive: 3W has no time zone
    return (a is None and b is None) or (a == b and type(a) is type(b))


def _explain(df: DataFrame) -> str:
    buffer = io.StringIO()
    with redirect_stdout(buffer):  # DataFrame.explain prints from Python
        df.explain("formatted")
    return buffer.getvalue()


def _since(start: float) -> float:
    return round(time.monotonic() - start, 3)
