# VM storage for Silver and Gold

## Mounted volume and paths

The UiS OpenStack VM is `group20`, running Ubuntu 24.04 as `ubuntu`. Its original disk is `/dev/vda` (about 40 GB). The attached OpenStack volume `group20-silver` is `/dev/vdb` (100 GB), formatted as ext4 and mounted at `/mnt/silver-data`. This mount is a directory inside the VM filesystem. It holds copied landing, Bronze and Silver data despite its Silver-specific name.

The VM reported about 93 GB available before the copy and second run. Check current capacity with `df`; that earlier figure is not current free space.

| Purpose | Location inside the VM |
|---|---|
| Attached volume | `/mnt/silver-data/` |
| Copied existing pipeline data | `/mnt/silver-data/3w-data/` |
| Current Silver v2 output root | `/mnt/silver-data/3w-data/silver-v2/` |
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

`conf/prod.yaml` still sets the production storage root to `/home/ubuntu/3w-data`. Copying data does not change the configured Bronze input. Silver selects its volume destination using `--output-root /mnt/silver-data/3w-data/silver-v2`, with both `SPARK_LOCAL_DIRS` and `TMPDIR` pointing to `/mnt/silver-data/spark-tmp`. The [run guide](SILVER_RUN_GUIDE.md) contains the complete launch command.

The second run report confirms successful publication to the volume. Gold should resolve `/mnt/silver-data/3w-data/silver-v2/silver_ready.json` once and use that run's recorded table paths. Its saved snapshot selects `20261018T091117552005Z-3280e5d3`. Reports are in `_runs/`; tables are in `runs/<run_id>/<table_name>/`. Copying an older report or marker does not rewrite its absolute paths, so copied baseline evidence can still refer to the original root disk.

## Persistence

Disconnecting SSH does not unmount the volume or erase its files. Data remains on the volume across a VM reboot; automatic remounting requires the filesystem configuration. The known UUID is `94ea1fc0-7bdf-4495-9222-d115b5cb029a`, with this intended `/etc/fstab` entry:

```text
UUID=94ea1fc0-7bdf-4495-9222-d115b5cb029a /mnt/silver-data ext4 defaults,nofail 0 2
```

The supplied evidence confirms the mounted volume and a successful build there. It does not establish that this entry has been installed or a reboot tested. Check for an existing entry, back up `/etc/fstab` before any edit, and validate with `sudo findmnt --verify --verbose`. Do not format this volume again. Check that it is mounted before large writes, especially after a reboot.

## Saved evidence

The repository contains both [v2 run reports, ready-marker snapshots and the comparison log](../evidence/silver/v2/README.md). All seven tables matched in the recorded comparison. These are data-output checks; the mount adds capacity, and no controlled speedup is claimed. Keep Parquet datasets, Spark temporary files, virtual environments and filesystem backups outside Git.
