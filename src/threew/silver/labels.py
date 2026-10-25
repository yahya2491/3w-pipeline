"""Apply the declared leading-class persistence assumption, preserving source labels."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from threew.silver.reference import OBSERVATION_KEY, RECORDING_KEY


def prepare_class_labels(observations, rules):
    """Add clean labels and a non-null flag identifying inferred source-null classes."""
    prepared = _infer_class_labels(observations, rules)
    return prepared.withColumn(
        "class_was_inferred", F.col("class_code").isNull() & F.col("class_code_clean").isNotNull()
    )


def _infer_class_labels(observations, rules):
    """Infer leading labels and optionally internal gaps between matching originals.

    Leading anchors use a small recording aggregate. Internal-gap windows use
    only narrow label rows from recordings with gaps after their first label.
    Later labels are used for target inference, never sensor predictors.
    """
    if rules.missing_classes == "preserve_unknown":
        return observations.withColumn("class_code_clean", F.col("class_code"))
    anchors = observations.groupBy(*RECORDING_KEY).agg(
        F.min(
            F.when(
                F.col("class_code").isNotNull(),
                F.struct(
                    F.col("_row_in_file").alias("row"),
                    F.col("class_code").alias("code"),
                    F.col("class_label_status").alias("status"),
                ),
            )
        ).alias("_prep_first_class"),
        F.max(F.when(F.col("class_code").isNull(), F.col("_row_in_file"))).alias("_prep_last_missing_class_row"),
    )
    prepared = observations.join(F.broadcast(anchors), list(RECORDING_KEY), "left")
    infer = (
        F.col("class_code").isNull()
        & (F.col("_row_in_file") < F.col("_prep_first_class.row"))
        & (F.col("_prep_first_class.status") == "consistent")
    )
    prepared = prepared.withColumn(
        "class_code_clean", F.when(infer, F.col("_prep_first_class.code")).otherwise(F.col("class_code"))
    ).drop("_prep_first_class", "_prep_last_missing_class_row")
    if rules.missing_classes == "backfill_leading":
        return prepared

    candidates = anchors.filter(F.col("_prep_last_missing_class_row") > F.col("_prep_first_class.row")).select(
        *RECORDING_KEY
    )
    labels = observations.select(*OBSERVATION_KEY, "class_code", "class_label_status").join(
        F.broadcast(candidates), list(RECORDING_KEY), "left_semi"
    )
    ordered = Window.partitionBy(*RECORDING_KEY).orderBy("_row_in_file")
    original = F.when(
        F.col("class_code").isNotNull(),
        F.struct(F.col("class_code").alias("code"), F.col("class_label_status").alias("status")),
    )
    labels = labels.withColumn(
        "_previous", F.last(original, ignorenulls=True).over(ordered.rowsBetween(Window.unboundedPreceding, -1))
    ).withColumn("_next", F.first(original, ignorenulls=True).over(ordered.rowsBetween(1, Window.unboundedFollowing)))
    matching = (
        F.col("class_code").isNull()
        & (F.col("_previous.code") == F.col("_next.code"))
        & (F.col("_previous.status") == "consistent")
        & (F.col("_next.status") == "consistent")
    )
    patches = labels.filter(matching).select(*OBSERVATION_KEY, F.col("_previous.code").alias("_prep_internal_class"))
    return (
        prepared.join(F.broadcast(patches), list(OBSERVATION_KEY), "left")
        .withColumn("class_code_clean", F.coalesce(F.col("class_code_clean"), F.col("_prep_internal_class")))
        .drop("_prep_internal_class")
    )
