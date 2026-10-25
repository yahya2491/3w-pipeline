# VM storage

## Current paths

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
The [run guide](SILVER_RUN_GUIDE.md) contains commands for both uses.

## Capacity and retention

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
This documentation cleanup does not remove any VM data.

## Persistence

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
