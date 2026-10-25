from dataclasses import replace

from threew.silver.labels import prepare_class_labels
from threew.silver.rules import load_rules


def labels(spark, values, path="a", file_hash="v1", statuses=None):
    statuses = statuses or ["unlabeled" if value is None else "consistent" for value in values]
    return spark.createDataFrame(
        [(path, file_hash, i, value, statuses[i]) for i, value in enumerate(values)],
        "_source_path string, _file_sha256 string, _row_in_file long, class_code short, class_label_status string",
    )


def test_only_leading_nulls_receive_the_first_known_class_and_sources_stay_unchanged(spark):
    values = [None, None, 0, 105, None, 5, None]
    source = labels(spark, values)
    result = prepare_class_labels(source, load_rules())
    rows = result.orderBy("_row_in_file").collect()
    assert [row.class_code for row in rows] == values
    assert [row.class_code_clean for row in rows] == [0, 0, 0, 105, None, 5, None]
    assert result.schema["class_code_clean"].dataType == source.schema["class_code"].dataType
    assert len(result.columns) == len(source.columns) + 2
    assert result.schema["class_was_inferred"].dataType.simpleString() == "boolean"
    assert result.schema["class_was_inferred"].nullable is False
    assert [row.class_was_inferred for row in rows] == [True, True, False, False, False, False, False]


def test_recordings_and_source_versions_are_isolated_and_unlabelled_recordings_stay_unknown(spark):
    source = (
        labels(spark, [None, None], file_hash="v1")
        .unionByName(labels(spark, [None, 4], file_hash="v2"))
        .unionByName(labels(spark, [None, 3], path="b"))
    )
    rows = prepare_class_labels(source, load_rules()).orderBy("_source_path", "_file_sha256", "_row_in_file").collect()
    assert [row.class_code_clean for row in rows] == [None, None, 4, 4, 3, 3]
    assert [row.class_was_inferred for row in rows] == [False, False, True, False, True, False]


def test_an_inconsistent_first_label_is_not_copied_and_policy_can_preserve_unknowns(spark):
    source = labels(spark, [None, 999, 0], statuses=["unlabeled", "inconsistent", "consistent"])
    rows = prepare_class_labels(source, load_rules()).orderBy("_row_in_file").collect()
    assert [row.class_code_clean for row in rows] == [None, 999, 0]
    assert all(row.class_was_inferred is False for row in rows)
    source = labels(spark, [None, 0])
    rows = (
        prepare_class_labels(source, replace(load_rules(), missing_classes="preserve_unknown"))
        .orderBy("_row_in_file")
        .collect()
    )
    assert [row.class_code_clean for row in rows] == [None, 0]
    assert all(row.class_was_inferred is False for row in rows)


def test_matching_internal_gaps_and_leading_labels_follow_the_requested_example(spark):
    values = [None, None, None, 0, 0, 105, 105, 105, None, None, 105, 5, 5, None, None, 5]
    source = labels(spark, values)
    rows = prepare_class_labels(source, load_rules()).orderBy("_row_in_file").collect()
    assert [row.class_code for row in rows] == values
    assert [row.class_code_clean for row in rows] == [0, 0, 0, 0, 0, 105, 105, 105, 105, 105, 105, 5, 5, 5, 5, 5]
    assert [row.class_was_inferred for row in rows] == [
        True,
        True,
        True,
        False,
        False,
        False,
        False,
        False,
        True,
        True,
        False,
        False,
        False,
        True,
        True,
        False,
    ]
    leading_only = replace(load_rules(), missing_classes="backfill_leading")
    rows = prepare_class_labels(source, leading_only).orderBy("_row_in_file").collect()
    assert [row.class_code_clean for row in rows] == [
        0,
        0,
        0,
        0,
        0,
        105,
        105,
        105,
        None,
        None,
        105,
        5,
        5,
        None,
        None,
        5,
    ]


def test_internal_gaps_require_two_consistent_originals_from_the_same_recording(spark):
    source = labels(spark, [105, None, 5, None, 5, None], path="changes")
    source = source.unionByName(
        labels(spark, [0, None, 0], path="invalid", statuses=["consistent", "unlabeled", "inconsistent"])
    )
    source = source.unionByName(labels(spark, [0, None], path="versions", file_hash="v1"))
    source = source.unionByName(labels(spark, [None, 0], path="versions", file_hash="v2"))
    rows = prepare_class_labels(source, load_rules()).orderBy("_source_path", "_file_sha256", "_row_in_file").collect()
    assert [row.class_code_clean for row in rows] == [105, None, 5, 5, 5, None, 0, None, 0, 0, None, 0, 0]
    assert [row.class_was_inferred for row in rows] == [
        False,
        False,
        False,
        True,
        False,
        False,
        False,
        False,
        False,
        False,
        False,
        True,
        False,
    ]
