"""Build the Gold table from Silver: labelled rows, complete and plausible windows, stride, recording split, class cap.

Usage (on the VM, from the repo root):
    SPARK_LOCAL_DIRS=/mnt/silver-data/spark-tmp PYSPARK_PYTHON=~/venvs/3w/bin/python \
        ~/venvs/3w/bin/spark-submit --driver-memory 3g gold/build_gold_table.py
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("gold_table").getOrCreate()

SILVER = "/mnt/silver-data/3w-data/silver-v2/runs/20261018T091117552005Z-3280e5d3/silver_observations"
GOLD = "/mnt/silver-data/gold/gold_table"
STRIDE = 60
RARE_LABEL, RARE_STRIDE = 2, 10
CAP = 200_000
SEED = 42
TEST_FRACTION = 0.2
VALUE_LIMIT = 1e12  # Silver keeps corrupt readings such as -1.18e42 Pa; beyond this a feature counts as missing

obs = spark.read.parquet(SILVER)

# the 7 feature sensors, found from the column names instead of typed by hand
sensors = sorted(c[: -len("_count_120s")] for c in obs.columns if c.endswith("_count_120s"))
print(sensors)

# labels: keep labelled rows, merge transient codes 101-109 into events 1-9
df = obs.filter(F.col("class_code").isNotNull())
df = df.withColumn(
    "label",
    F.when(F.col("class_code") >= 100, F.col("class_code") - 100).otherwise(F.col("class_code")).cast("int"),
)

# per sensor: its features are null unless the 120-second window is complete and no value is beyond VALUE_LIMIT
feature_cols = []
for s in sensors:
    names = [f"{s}_{stat}" for stat in ["mean_120s", "stddev_120s", "change"]]

    ok = F.col(f"{s}_count_120s") == 120
    for name in names:
        # a null value counts as fine here, otherwise one null would wipe the other two
        ok = ok & F.coalesce(F.abs(F.col(name)) <= VALUE_LIMIT, F.lit(True))

    # withColumns replaces all three at once, so ok is checked against the original values;
    # one withColumn at a time would check the second and third against an already-nulled mean
    df = df.withColumns({name: F.when(ok, F.col(name)) for name in names})
    feature_cols += names

# drop rows where every feature is null
df = df.filter(F.coalesce(*[F.col(c) for c in feature_cols]).isNotNull())

# stride: every 60th row, every 10th for the rare class
stride = F.when(F.col("label") == RARE_LABEL, RARE_STRIDE).otherwise(STRIDE)
df = df.filter(F.col("_row_in_file") % stride == 0)

keep = ["_source_path", "source_type", "well_id", "_row_in_file", "class_code", "label"] + feature_cols
df = df.select(*keep).withColumnRenamed("_source_path", "recording_id").cache()

# one row per recording: its event (highest label) and its source
rec = df.groupBy("recording_id").agg(
    F.max("label").alias("recording_event"),
    F.first("source_type").alias("rec_source"),
)

# within each (event, source) group, order recordings by a seeded hash; the first 20% go to test
w = Window.partitionBy("recording_event", "rec_source").orderBy(F.xxhash64("recording_id", F.lit(SEED)))
rec = rec.withColumn("rank", F.percent_rank().over(w))
rec = rec.withColumn("split", F.when(F.col("rank") < TEST_FRACTION, "test").otherwise("train"))

# broadcast keeps df's partitions and row order (no shuffle), so sampleBy below picks the same rows every run
df = df.join(F.broadcast(rec.select("recording_id", "recording_event", "split")), on="recording_id")

# cap each class at about CAP rows; classes under the cap keep everything
counts = {r["label"]: r["count"] for r in df.groupBy("label").count().collect()}
fractions = {label: min(1.0, CAP / n) for label, n in counts.items()}
df = df.sampleBy("label", fractions, seed=SEED)

df.coalesce(1).write.mode("overwrite").parquet(GOLD)
print("Gold table written to", GOLD)
spark.stop()
