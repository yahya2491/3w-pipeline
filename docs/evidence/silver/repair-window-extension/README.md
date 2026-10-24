# Choke repair-window extension

The default maximum age of an original valid choke opening was extended from
3 to 360 seconds on 24 October 2026. Only finite out-of-range openings are
eligible. The original/clean values, existing provenance fields, one-second
sequence boundaries and observation schema retain their existing meanings.

## Why six minutes

The [full-source audit](../choke-repair-review/README.md) found 161,872 finite
invalid production-choke openings in two recordings. Of these, 161,538 belong
to an entirely invalid channel with no original valid opening anywhere. The
other 334 are trailing readings after an original valid opening of 99.95956%.
Their maximum original-anchor age is 334 seconds. A 360-second cap is the
smallest whole-minute limit covering this entire reachable stretch. Extending
it further cannot recover additional invalid openings in this pinned source.

This choice expands bounded imputation coverage. It is not an optimum learned
from held-out event-detection performance or a guarantee that the earlier
opening remained unchanged for six minutes.

## Actual Spark verification

The [verification](spark_verification.json) runs prepare_observations on both
affected original recordings, covering 883,730 observations, with the old and
new limits. The all-source audit establishes that these contain every finite
out-of-range opening in the pinned dataset.

| Maximum original-anchor age | Production-choke estimates | Unresolved finite invalid openings |
|---|---:|---:|
| 3 seconds | 3 | 161,869 |
| 360 seconds | 334 | 161,538 |

All 334 new-policy replacements are 99.95956%; the oldest anchor age actually
used is 334 seconds. Gas-lift repairs remain zero. Valid originals are unchanged,
null originals are not filled, and raw error counts are preserved. Estimates
never become new anchors. Timestamp/cadence/row-position breaks and recording
boundaries continue to prevent carry-forward.

This verifies the transformation on every recording affected by the new limit.
It is not a new complete seven-table build or publication. Existing VM reports
and ready markers still describe their saved three-second policy. A new full
Silver run is needed before claiming the extended policy in published outputs.

## Masking experiment

The [evaluation](masking_evaluation.json) checks all 2,228 pinned source files
against their manifest hashes. Within uninterrupted original-valid opening runs,
it takes disjoint 601-row blocks, masks the final 600 readings, and holds the
first original reading constant. All age horizons use the same blocks. The
reported errors cover every masked position up to each horizon.

| Sensor | Original-valid blocks | Age limit | Mean absolute error (percentage points) | RMSE (percentage points) | Within 1 percentage point |
|---|---:|---:|---:|---:|---:|
| Production choke | 20,006 | 3 s | 0.00165 | 0.03793 | 99.967% |
| Production choke | 20,006 | 360 s | 0.07470 | 1.50204 | 99.198% |
| Gas-lift choke | 13,904 | 3 s | 0.00069 | 0.01191 | 100.000% |
| Gas-lift choke | 13,904 | 360 s | 0.05164 | 1.23078 | 99.243% |

The 360-second comparison uses 7,202,160 masked production-choke readings across
298 real recordings and 5,005,440 gas-lift readings across 286 real recordings.
Most errors are small. The maximum error reaches 100 percentage points at long
horizons for both sensors, showing that persistence fails during some changes.
These are artificial gaps in known-valid data; they do not validate the true
values of the actual out-of-range readings. Runs shorter than 601 valid readings
are excluded, and this is a descriptive experiment rather than a held-out model
accuracy result.

The removed evaluator was checked with explicit known-answer tests for disjoint original-valid
blocks, matched-horizon errors, domain/non-finite exclusions and source-integrity
failures. Spark tests cover the inclusive 360-second boundary, expiry at 361
seconds, non-chaining anchors, both opening sensors, preserved null/non-finite
values, recording/time breaks and future independence.

## Validation

The [saved checks](checks.json) record 289 passing offline tests, including
batching, repeat-run equality and failed-publication guards. The focused repair
tests and final evaluator checks passed; repository lint and formatting passed.

## Other repair options reviewed

- A longer same-recording window cannot recover the entirely invalid opening
  channel. Backward fill and interpolation have no valid endpoint there either.
- No overlapping same-well recording supplies replacement readings for either
  affected recording in this pinned source.
- Clipping slightly excessive openings to 100% would enforce a domain but has
  no independent evidence that 100% was their actual value. Carry-forward keeps
  the existing repair interpretation and explicit age limit.
- There are no finite invalid valve codes or non-null NaN/infinite values in
  the reviewed source. The handling rules remain necessary for future inputs.
- [Missing-sensor gaps](../sensor-gap-review/README.md) offer further imputation
  experiments: 1,837 null cells in complete internal gaps up to 10 seconds and
  9,885 up to 60 seconds. These counts do not validate estimates or their effect
  on event detection. Future-endpoint interpolation changes the online timing
  assumptions. Universal forward-fill has not been enabled for these sensors.
- Four sensors have no finite readings anywhere in the pinned source; their
  reconstruction cannot be trained or checked against measured targets here.
  Other entirely absent recording channels would require separately evaluated
  other-sensor models or independent measurements.
- [Missing classes](../class-label-review/README.md) are leading unknown periods,
  not internal gaps between repeated expert labels. Backfilling these creates
  inferred targets and cannot supply verified evaluation truth.
- Finite pressure, temperature and flow spikes can reflect the well events under
  study. No new physical bounds, smoothing or outlier replacement is justified
  merely by a large finite value.

The bounded same-channel repair route is now exhausted for the known invalid
openings. Further recovery remains possible as a separately evaluated imputation
or weak-labelling experiment; it is not established by filling more cells.

## Audit cleanup

The one-off masking evaluator and its audit-only tests were removed after
validation at the project owner's request. Saved JSON results, limitations and
historical test records remain. Commands in those historical records describe
the checks performed then; they are not current test-suite instructions.
Production repair regressions remain in tests/test_silver_prepare.py.
