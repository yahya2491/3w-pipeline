# VM storage for Silver and Gold

## Mounted volume and paths

The UiS OpenStack VM is `group20`, running Ubuntu 24.04 as `ubuntu`. Its original disk is `/dev/vda` (about 40 GB). The attached OpenStack volume `group20-silver` is `/dev/vdb` (100 GB), formatted as ext4 and mounted at `/mnt/silver-data`. This mount is a directory inside the VM filesystem. It holds copied landing, Bronze and Silver data despite its Silver-specific name.

The VM reported about 93 GB available before the copy and second run. Check current capacity with `df`; that earlier figure is not current free space.

| Purpose | Location inside the VM |
|---|---|
| Attached volume | `/mnt/silver-data/` |
| Copied existing pipeline data | `/mnt/silver-data/3w-data/` |
| Scheduled Silver output root | `/mnt/silver-data/silver-schedule/` |
| Historical Silver output root | `/mnt/silver-data/3w-data/silver-v2/` |
| Second validated v2 run | `/mnt/silver-data/3w-data/silver-v2/runs/20261018T091117552005Z-3280e5d3/` |
| Spark temporary data | `/mnt/silver-data/spark-tmp/` |
| Original v2 baseline | `/home/ubuntu/3w-data/silver-v2/runs/20261017T232322182032Z-ba450086/` |
| Historical v1 output | `/home/ubuntu/3w-data/silver-oh/` (also copied under `/mnt/silver-data/3w-data/silver-oh/`) |
| Configured production Bronze input | `/home/ubuntu/3w-data/bronze/3w_observations/` |
| VM comparison log | `/mnt/silver-data/comparison-20261017-vs-20261018.log` |

The copied `3w-data/` contains `_staging/`, `landing/`, `bronze/`, `silver-oh/` and `silver-v2/`. The earlier empty `/mnt/silver-data/silver-v2/` is not the selected output root. Its presence or removal does not change the nested path used here.

## Access from the VM

After connecting through SSH, use normal directory commands:

```bash
mountpoint /mnt/silver-data
df -h / /mnt/silver-data
cd /mnt/silver-data/3w-data/silver-v2
ls
ls runs/20261018T091117552005Z-3280e5d3
```

The last directory contains seven Parquet table folders. For example, observations are at `runs/20261018T091117552005Z-3280e5d3/silver_observations/`. Read the complete table folder with Spark or PyArrow. The [Gold handover](../gold/GOLD_HANDOVER.md#where-the-input-is-and-how-to-work) describes selecting all input paths from one ready marker.

To inspect data and scratch sizes:

```bash
du -sh /mnt/silver-data/3w-data/*
du -sh /mnt/silver-data/spark-tmp
```

## Silver output and Gold input

`conf/prod.yaml` keeps the production storage root at `/home/ubuntu/3w-data`.
Bronze's workflow and its landing files, manifests, reports and converted data
remain unchanged. Silver consumes `/home/ubuntu/3w-data/bronze/bronze_ready.json`
and uses the explicit destination `--output-root /mnt/silver-data/silver-schedule`.
Gold defaults to this Silver root. The scheduled Silver job places
`SPARK_LOCAL_DIRS` and `TMPDIR` in its own job directory under
`/mnt/silver-data/spark-tmp` and fails if the volume is not mounted.

The Silver destination becomes active after merging to main through a PR.
Dispatch Silver manually or wait for the next successful Bronze or CI run to
populate it. The existing Bronze publication can be reused; no new Bronze root
is needed. Existing data under `/mnt/silver-data/3w-data/` is not automatically
moved or deleted. The [run guide](SILVER_RUN_GUIDE.md) contains the launch commands.

For the historical 106-column baseline, the second run report confirms successful publication to the volume. Its saved input marker is `/mnt/silver-data/3w-data/silver-v2/silver_ready.json`; use that run's recorded table paths. Its saved snapshot selects `20261018T091117552005Z-3280e5d3`. Reports are in `_runs/`; tables are in `runs/<run_id>/<table_name>/`. Copying an older report or marker does not rewrite its absolute paths, so copied baseline evidence can still refer to the original root disk.

## Cleanup after the scheduled builds are validated

Do not erase or format the mount. Keep at least one validated historical Silver
run and its evidence until the replacement Silver publication has
passed its checks and downstream readers have switched to the new marker.
Preserve `/mnt/silver-data/gold/` and any model/results folders still in use.

After checking actual disk usage and reader references, cleanup candidates are
old duplicate landing/Bronze copies under `3w-data/`, surplus historical Silver
run directories, and abandoned staging or Spark scratch directories. Remove
scratch only when its job has stopped; do not delete a whole scratch root while
any pipeline is running. The previously empty top-level `silver-v2/` is removable
only if it is still empty. Reports and copied markers may reference old absolute
paths, so inspect them before removing the corresponding data.

The Silver workflow removes only its own scratch. Weekly Bronze reuses its manifest;
Silver skips unchanged verified output. New or forced Silver builds retain
separate outputs, so these changes do not establish automatic data retention.
To inventory the mount without changing it:

```bash
mountpoint /mnt/silver-data
df -h /mnt/silver-data
du -h --max-depth=2 /mnt/silver-data | sort -h
```

## Persistence

Disconnecting SSH does not unmount the volume or erase its files. Data remains on the volume across a VM reboot; automatic remounting requires the filesystem configuration. The known UUID is `94ea1fc0-7bdf-4495-9222-d115b5cb029a`, with this intended `/etc/fstab` entry:

```text
UUID=94ea1fc0-7bdf-4495-9222-d115b5cb029a /mnt/silver-data ext4 defaults,nofail 0 2
```

The supplied evidence confirms the mounted volume and a successful build there. It does not establish that this entry has been installed or a reboot tested. Check for an existing entry, back up `/etc/fstab` before any edit, and validate with `sudo findmnt --verify --verbose`. Do not format this volume again. Check that it is mounted before large writes, especially after a reboot.

## Saved evidence

The repository contains both [v2 run reports, ready-marker snapshots and the comparison log](../evidence/silver/v2/README.md). All seven tables matched in the recorded comparison. These are data-output checks; the mount adds capacity, and no controlled speedup is claimed. Keep Parquet datasets, Spark temporary files, virtual environments and filesystem backups outside Git.
