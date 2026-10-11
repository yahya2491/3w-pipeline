# Silver run guide

This guide shows how to use the small dev sample while working on Silver, and how to run Silver on the full dataset later.

Bronze downloads and prepares the sample. Silver reads the prepared Bronze data. **Silver does not download the 30 files itself.**


## Prepare the 30-file dev sample

If the dev Bronze sample is not already there, run:

```bash
python -m threew.bronze.run --env dev
```

Bronze downloads 30 original Parquet files, checks them, and converts them into a Bronze table that Spark can read under `data/dev/bronze/`. The sample has about 1.9 million observation rows. You need internet access for this step.

You normally do this once. While developing Silver, reuse the same sample so you can check your changes against the same data each time.

To see a read-only overview of that Bronze data, run:

```bash
python -m threew.silver.profile --env dev
```

This reports what is in Bronze. It does not create or change Silver. So  basically it reads the dev 30 file sample under `data/dev/bronze/` and prints a summary of the recordings, sensors, and labels. 

## Build Silver from the dev sample

Run:

```bash
python -m threew.silver.run --env dev
```

This command reads the Bronze table already in `data/dev/bronze/`. It does not download the files again. It:

1. Checks that Bronze finished successfully.
2. Looks up which recording each row came from and adds recording details where available.
3. Renames the sensor and label columns to clearer names and adds readable event descriptions.
4. Keeps the original readings, timestamps, missing values, and rows.
5. Makes two small summaries: one about each recording, and one about each sensor in each recording.
6. Saves the three Silver tables and checks that they match the Bronze input before marking the run ready.

For the dev sample, expect 1,931,052 observation rows, 30 recording summaries, and 810 sensor summaries.

Each attempt gets a new folder under `data/dev/silver/runs/`. The file `data/dev/silver/silver_ready.json` points to the latest successful run. Use the path in that marker; a failed attempt can also leave a folder behind.

The three tables are:

- `silver_observations`: the original observations with clearer names and added recording details.
- `instance_quality`: one row per recording, with counts such as missing labels and timestamp problems.
- `sensor_availability`: one row per recording and sensor, showing how many readings can be used.

## Check your code changes

The Silver run above checks the whole pipeline against the 30-file sample. The automated tests use smaller sample data so they run quickly and do not download the dataset.

From the project folder, run:

```bash
ruff check .
ruff format --check .
HF_HUB_OFFLINE=1 pytest -q
```

For the column and label transformation tests only:

```bash
pytest -q tests/test_silver_input.py tests/test_silver_transform.py
```

For the summaries and time checks only:

```bash
pytest -q tests/test_silver_quality.py
```

The [milestone report](SILVER_MILESTONE_REPORT.md) explains what each step does. The [Silver contract](SILVER_CONTRACT.md) lists every output column and its meaning.

## Run on the full dataset on the VM

Do this after the dev checks pass. In your own VM checkout, with its Python environment active, run:

```bash
python -m threew.silver.run --env prod --output-root /home/ubuntu/3w-data/silver-oh
```

Run the same command again to get a second result to compare. This reads the full production Bronze table but writes the test result to the separate `silver-oh` folder. It does not overwrite the official Silver output.

The expected baseline is 76,587,318 observations, 2,228 recording summaries, and 60,156 sensor summaries. Check the VM's Bronze ready marker for the exact counts. Record both run IDs, the counts, how long each run took, and the output sizes. Compare all three tables from the two runs with the `same_rows` helper in `threew.silver.reconcile`. It checks whether the runs contain the same rows, including repeated rows. Record the two run IDs, counts, run times, and output sizes.
