"""Apply the declared leading-class persistence assumption, preserving source labels."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from threew.silver.reference import OBSERVATION_KEY, RECORDING_KEY


def prepare_class_labels(observations, rules):
    """Add clean labels and a non-null flag identifying inferred source-null classes."""
    prepared = _infer_class_labels(observations, rules)  # Keep the inference flag separate from the fill rules.
    return prepared.withColumn(
        "class_was_inferred", F.col("class_code").isNull() & F.col("class_code_clean").isNotNull()  # Flag fills.
    )


def _infer_class_labels(observations, rules):
    """Infer leading labels and optionally internal gaps between matching originals.

    Leading anchors use a small recording aggregate. Internal-gap windows use
    only narrow label rows from recordings with gaps after their first label.
    Later labels are used for target inference, never sensor predictors.
    """
    if rules.missing_classes == "preserve_unknown":  # This policy leaves supplied and missing labels as they are.
        return observations.withColumn("class_code_clean", F.col("class_code"))
    anchors = observations.groupBy(*RECORDING_KEY).agg(
        F.min(  # Find the first supplied label, even if it is inconsistent.
            F.when(
                F.col("class_code").isNotNull(),
                F.struct(
                    F.col("_row_in_file").alias("row"),  # Put source position first to find the earliest label.
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
        & (F.col("_row_in_file") < F.col("_prep_first_class.row"))  # Only the leading gap uses the first label.
        & (F.col("_prep_first_class.status") == "consistent")  # Only copy a label that fits this event.
    )
    prepared = prepared.withColumn(
        "class_code_clean", F.when(infer, F.col("_prep_first_class.code")).otherwise(F.col("class_code"))
    ).drop("_prep_first_class", "_prep_last_missing_class_row")
    if rules.missing_classes == "backfill_leading":  # Stop here if internal gap filling was not requested.
        return prepared

    candidates = anchors.filter(F.col("_prep_last_missing_class_row") > F.col("_prep_first_class.row")).select(
        *RECORDING_KEY
    )
    labels = observations.select(*OBSERVATION_KEY, "class_code", "class_label_status").join(  # Keep window rows narrow.
        F.broadcast(candidates), list(RECORDING_KEY), "left_semi"  # Only recordings with later gaps need these windows.
    )
    ordered = Window.partitionBy(*RECORDING_KEY).orderBy("_row_in_file")
    original = F.when(  # Use supplied labels as anchors, never labels we filled earlier.
        F.col("class_code").isNotNull(),
        F.struct(F.col("class_code").alias("code"), F.col("class_label_status").alias("status")),
    )
    labels = labels.withColumn(
        "_previous", F.last(original, ignorenulls=True).over(ordered.rowsBetween(Window.unboundedPreceding, -1))
    ).withColumn("_next", F.first(original, ignorenulls=True).over(ordered.rowsBetween(1, Window.unboundedFollowing)))
    matching = (
        F.col("class_code").isNull()
        & (F.col("_previous.code") == F.col("_next.code"))  # Fill the gap only if both sides agree.
        & (F.col("_previous.status") == "consistent")
        & (F.col("_next.status") == "consistent")  # A trailing gap has no next label, so it stays unknown.
    )
    patches = labels.filter(matching).select(*OBSERVATION_KEY, F.col("_previous.code").alias("_prep_internal_class"))
    return (
        prepared.join(F.broadcast(patches), list(OBSERVATION_KEY), "left")  # Keep rows even when no fill was found.
        .withColumn("class_code_clean", F.coalesce(F.col("class_code_clean"), F.col("_prep_internal_class")))
        .drop("_prep_internal_class")
    )
