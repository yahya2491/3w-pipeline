import math

import pytest

from threew.silver.batch import plan_batches
from threew.silver.reconcile import same_rows


def test_batches_keep_whole_recordings_and_isolate_oversized_recordings():
    sizes = [4, 3, 12, 2, 4]
    metadata = [{"_source_path": str(i), "_file_sha256": str(i)} for i in range(len(sizes))]
    counts = [{**row, "bronze_rows": size} for row, size in zip(metadata, sizes, strict=True)]
    batches = plan_batches(list(reversed(metadata)), counts, target_rows=7)
    assert [batch.rows for batch in batches] == [7, 12, 6]
    assert [row for batch in batches for row in batch.metadata] == metadata
    assert [row for batch in batches for row in batch.counts] == counts


@pytest.mark.parametrize("target", [0, -1, True, 1.5])
def test_invalid_batch_target_is_rejected(target):
    with pytest.raises(ValueError, match="positive integer"):
        plan_batches([], [], target)


def test_duplicate_or_incomplete_catalogue_cannot_drop_recordings():
    row = {"_source_path": "a", "_file_sha256": "b"}
    count = {**row, "bronze_rows": 1}
    other_version = {**row, "_file_sha256": "c"}
    for metadata, counts in (
        ([row, row], [count]),
        ([row], [count, count]),
        ([row], []),
        ([row, other_version], [count, {**other_version, "bronze_rows": 1}]),
    ):
        with pytest.raises(ValueError):
            plan_batches(metadata, counts, 10)


@pytest.mark.parametrize(
    "left,right,equal",
    [
        ([1.0, 1.0, None], [None, 1.0, 1.0], True),
        ([1.0, 1.0], [1.0], False),
        ([1.0], [1.0, 1.0], False),
        ([None], [None, None], False),
        ([math.nan, None], [None, math.nan], True),
        ([], [], True),
        ([], [None], False),
    ],
)
def test_exact_comparison_preserves_duplicate_null_and_nan_semantics(spark, left, right, equal):
    def frame(values):
        # Exercise internal-column name collisions as well as multiset equality.
        return spark.createDataFrame([(value,) for value in values], "__threew_row_weight double")

    assert same_rows(frame(left), frame(right)) is equal
