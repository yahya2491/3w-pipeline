# Silver run guide

Use the locked Linux environment described in the [README](../../README.md). The commands below build the current **79-column Silver**. Field definitions are in the [contract](SILVER_CONTRACT.md); measured results are in the [milestone report](SILVER_MILESTONE_REPORT.md).

## Run the dev sample

Reuse the existing dev Bronze table. If it has not been prepared, run Bronze once:

```bash
python -m threew.bronze.run --env dev
```

Then build Silver:

```bash
python -u -m threew.silver.run --env dev
```

The sample contains 30 recordings and 1,931,052 observations. To inspect Bronze without rebuilding it, use `python -m threew.silver.profile --env dev`.

## Run on the full dataset on the VM

The [two recorded full VM builds](../evidence/silver/v3/README.md) use the current
79-column contract. Both passed 32 checks over 2,228 recordings and 76,587,318
observations. Each saved 8,549,212,642 bytes of Parquet across seven tables;
runtimes were 6 h 19 min 21 s and 5 h 51 min 44 s. The saved comparison confirms
identical schemas and complete contents. Each new full build keeps its own output
copy; allow additional temporary disk space.

The attached volume is mounted at `/mnt/silver-data`; [VM storage](#vm-storage) describes the layout. Run one job at a time and reuse the existing production Bronze table under `/home/ubuntu/3w-data/`. For a reproducibility pair, keep the same commit, clean working tree, dependencies, input, rules and configuration; do not pull or change batch size between the two runs. Production deployment remains restricted to `main`; feature changes reach it through pull requests.

```bash
cd ~/3w-pipeline
source .venv/bin/activate
mountpoint /mnt/silver-data
df -h / /mnt/silver-data

SPARK_LOCAL_DIRS=/mnt/silver-data/spark-tmp \
TMPDIR=/mnt/silver-data/spark-tmp \
python -u -m threew.silver.run \
  --env prod \
  --output-root /mnt/silver-data/silver-schedule
```

Run the build only after `mountpoint` confirms the volume is mounted. This foreground command requires the SSH session to stay connected. Each new attempt gets its own run directory on the volume. The validated manual builds are under `/mnt/silver-data/3w-data/silver-v3/`; use that explicit output root when repeating those builds, or the scheduled root shown above for scheduled-destination builds. Silver reads the ready marker at `/home/ubuntu/3w-data/bronze/bronze_ready.json`; its output root is separate from that Bronze storage root.

## Automated VM builds

.github/workflows/silver.yml follows the existing Bronze Actions setup. It runs
only trusted main code on the self-hosted uis-openstack runner in the production
environment, after successful Bronze or CI runs on main. Manual dispatch has a
force option and a batch-row target. The normal weekly cadence comes from the
Bronze schedule: Mondays at 03:17 UTC; no separate competing Silver cron is needed.
Bronze's workflow and storage remain unchanged under /home/ubuntu/3w-data.
Silver reads that existing Bronze ready marker and publishes under
/mnt/silver-data/silver-schedule. The Silver job requires the volume to be mounted
and uses its own temporary directory under /mnt/silver-data/spark-tmp.

After merging to main, manually dispatch Silver to populate the new output root,
or let the next successful Bronze or CI run trigger it. No new Bronze build or
copy is required when its existing ready marker is valid. An empty Silver output
folder is sufficient; the Silver runner creates its own run and report directories.

The workflow must first be merged into main through a PR. GitHub workflow_run
and workflow_dispatch require the workflow to exist on the default branch:
[official trigger documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflow_run).
No production workflow is started from silver-v3. Existing runner/environment
protections remain in effect. This change does not provision a new VM runner.

Every Silver job fetches the latest main branch from GitHub when checkout starts,
cleans the runner workspace, and rebuilds its locked environment from that checkout.
It verifies HEAD against the fetched origin/main and records the actual source
commit in the Actions job summary and out/git-revision.txt artifact. Checkout or
revision-check failures stop the build. This also applies to queued and retried
jobs. The code stays fixed for the duration of a build; later merges are picked
up by a subsequent job.

The job checks that /mnt/silver-data is mounted, uses a per-job scratch directory, installs requirements.lock, and invokes:

    python -u -m threew.silver.run --env prod --output-root /mnt/silver-data/silver-schedule --skip-unchanged --min-free-gib 20 --report-copy out/

A 12-hour self-hosted job timeout accommodates the historical six-hour builds.
Concurrency never cancels an active Silver build. Pipeline locks also protect
against manual runs and Bronze writes. Logs/reports are kept as Actions artifacts
for 90 days, with a job summary and a failing job status on pipeline errors.
Only the job's own temporary Spark directory is removed; published runs are not
automatically deleted. A new build requires at least 20 GiB free, failing instead of removing old
evidence. Unchanged output can still be reused below that floor.

--skip-unchanged compares source-file versions/counts and recording metadata,
resolved rules/configuration, the contract, runtime source and locked dependencies.
Runtime Python files are compared by syntax structure without comments, formatting
or module/class/function docstrings. Executable string values remain included.
Dependency-lock comments and blank lines are ignored, while pins, options and URL
hashes remain included. Package TOML is parsed so comments and formatting do not
change its signature. Actual package settings still count as changes.
This signature version requires one rebuild of publications made with the old
signature; later comment-only edits reuse the newly published output.
It ignores changing Bronze run IDs when content is unchanged. Reuse requires a
successful current-schema marker and all seven output directories with recorded
Parquet byte sizes. It does not rescan all output values for same-length external
corruption: published runs should remain immutable. Use manual force to rebuild
or revalidate. On a genuine reuse, Spark is not started and the ready marker is
not rewritten. Unsuccessful attempts retain the prior published marker.

## What batches mean

A batch is a group of complete recordings, targeting 1,000,000 observations. A recording larger than the target runs alone. Silver writes and checks each batch, stops its Spark context to release temporary files, and then continues. All batches contribute to the same seven tables.

The full prepared-observation table is not cached. Only smaller recording counts, summaries, and issue data are cached during a batch. The writer uses 32 MiB Parquet row groups and 256 KiB pages to limit column buffers.

To reduce the batch target, add `--batch-rows 500000`. This cannot split an oversized recording. The dev configuration uses two local cores; production uses four. Both use a 4 GB Spark driver.

The 120-second feature history is separate from batching. Complete recordings preserve the sequence needed for repairs and features.

## Find the output and reports

Under the selected Silver root:

| Path | Contents |
|---|---|
| `runs/<run_id>/<table_name>/` | The seven Parquet tables for that attempt. |
| `_runs/<run_id>.json` | Full report: code/configuration, batch progress, checks, counts, sizes, timings, and quality results. |
| `silver_ready.json` | Summary and table paths for the latest successful run. Also points to the full report. |


To display the full report for the latest successful VM run without starting Spark:

```bash
python - <<'PY'
import json
from pathlib import Path

root = Path("/mnt/silver-data/silver-schedule")
ready = json.loads((root / "silver_ready.json").read_text())
report_path = Path(ready["report"])
print("Run:", ready["run_id"])
print("Report:", report_path)
print(report_path.read_text())
PY
```

## Check code changes

```bash
ruff check .
ruff format --check .
HF_HUB_OFFLINE=1 python -m pytest -q
```

## Compare two saved runs

Equal row counts alone do not establish equal contents. Compare the seven tables only when both runs' outputs are still present. This separate comparison reads whole tables and needs extra shuffle space; the runner's batch limit does not apply. Do not run it on the constrained VM without enough room for both outputs and the comparison.

Run the following inside the VM. It compares the seven saved tables directly in the two explicit run folders below. It checks schemas and complete contents, including duplicate rows and nulls, while ignoring physical file layout and row order. It does not read run reports, ready markers or code metadata. To compare a different pair, change `first` and `second`.

The [saved comparison log](../evidence/silver/v3/comparison.log) already records seven `MATCH` results for these two successful 79-column runs. Their [reports and markers](../evidence/silver/v3/README.md) record the matching inputs and settings. The command below repeats that check; run it only after both builds have completed successfully and no Silver build is active. A different pair requires its own completed comparison before claiming equality.

```bash
cd /home/ubuntu/3w-pipeline
source .venv/bin/activate

export SPARK_LOCAL_DIRS=/mnt/silver-data/spark-tmp
export TMPDIR=/mnt/silver-data/spark-tmp
set -o pipefail

mountpoint -q /mnt/silver-data &&
python -u - <<'PY' 2>&1 | tee /mnt/silver-data/comparison-silver-v3.log
from pathlib import Path

from threew.config import load_config
from threew.silver.reconcile import same_rows
from threew.silver.run import spark_session

run_root = Path("/mnt/silver-data/3w-data/silver-v3/runs")
first = run_root / "20261024T175806357259Z-675ce602"
second = run_root / "20261025T010710680886Z-216d5afd"

tables = [
    "silver_observations",
    "instance_quality",
    "sensor_availability",
    "quality_issues",
    "quality_quarantine",
    "event_reference",
    "sensor_reference",
]

for root in (first, second):
    for name in tables:
        if not (root / name).is_dir():
            raise SystemExit(f"Missing table: {root / name}")

results = {}

with spark_session(load_config("prod")) as spark:
    for name in tables:
        print(f"Comparing {name}...", flush=True)

        left = spark.read.parquet(str(first / name))
        right = spark.read.parquet(str(second / name))

        if left.schema != right.schema:
            results[name] = False
            print(f"{name}: DIFFERENT SCHEMA; contents not compared", flush=True)
            continue

        results[name] = same_rows(left, right)
        result = "MATCH" if results[name] else "DIFFERENT CONTENT"
        print(f"{name}: {result}", flush=True)

if all(results.values()):
    print("RESULT: All seven tables have identical schemas and complete contents.")
else:
    raise SystemExit("RESULT: One or more tables differ. See results above.")
PY
```

The command saves its log at `/mnt/silver-data/comparison-silver-v3.log`. A successful comparison prints seven `MATCH` results and the final message that all seven tables have identical schemas and complete contents. If Spark fails, the comparison is incomplete.


## VM storage

### Current paths

The UiS OpenStack VM is `group20` (Ubuntu 24.04, user `ubuntu`). Its original
`/dev/vda` disk is about 40 GB. The attached `group20-silver` volume is a 100 GB
ext4 filesystem on `/dev/vdb`, mounted at `/mnt/silver-data`.

| Purpose | VM location |
|---|---|
| Production Bronze storage root, unchanged | `/home/ubuntu/3w-data/` |
| Bronze observations | `/home/ubuntu/3w-data/bronze/3w_observations/` |
| Bronze ready marker used by Silver | `/home/ubuntu/3w-data/bronze/bronze_ready.json` |
| Validated manual Silver root | `/mnt/silver-data/3w-data/silver-v3/` |
| Scheduled Silver root | `/mnt/silver-data/silver-schedule/` |
| Spark temporary root | `/mnt/silver-data/spark-tmp/` |
| Gold table | `/mnt/silver-data/gold/gold_table/` |
| Current comparison log on VM | `/mnt/silver-data/comparison-silver-v3.log` |

Each Silver root contains `runs/<run_id>/<table_name>/`, `_runs/<run_id>.json`
and a `silver_ready.json` selecting its latest successful publication. Saved
[current evidence](../evidence/silver/v3/README.md) comes from manual builds;
it does not establish that the scheduled destination has been populated.
The commands above cover both uses.

### Capacity and retention

```bash
mountpoint /mnt/silver-data
df -h / /mnt/silver-data
du -h --max-depth=2 /mnt/silver-data | sort -h
```

Each recorded current-schema build saved 8,549,212,642 bytes of Parquet across
seven tables. A new build needs additional output and temporary space; the
20 GiB free-space check is a starting floor, not a peak-usage guarantee.
Unchanged verified output can be reused. New or forced builds keep separate
complete datasets; published runs are not automatically removed.

The scheduled Silver job uses a unique scratch directory and removes only that
directory after finishing. Inspect manual scratch only when its job has stopped.
Do not delete an active scratch root, the current ready-selected run, Gold data,
`lost+found`, or the only retained validation dataset. Older copied Bronze and
Silver folders are cleanup candidates only after checking reader/report paths.

### Persistence

Disconnecting SSH does not erase or unmount the volume. Reboot persistence needs
a valid mount configuration. The recorded filesystem UUID is
`94ea1fc0-7bdf-4495-9222-d115b5cb029a`; the intended entry is:

```text
UUID=94ea1fc0-7bdf-4495-9222-d115b5cb029a /mnt/silver-data ext4 defaults,nofail 0 2
```

The saved builds establish successful writes to the mount, not that `/etc/fstab`
was installed or reboot-tested. Check the existing configuration and validate
with `sudo findmnt --verify --verbose` before changing it. Do not format the
volume again. The scheduled Silver job checks the mount before large writes.
Copied markers/reports retain their original absolute paths; select a live
publication rather than assuming that a copied marker points to copied data.
