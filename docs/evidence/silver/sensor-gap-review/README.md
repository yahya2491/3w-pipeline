# Sensor-gap repair opportunity review

Reviewed on 24 October 2026 against all 2,228 original Parquet recordings,
76,587,318 observations and 27 sensors at source revision
c1a7e8d0cfefc0875974ea079d11349a37fa0c56. Every source file was checked against
the pinned manifest SHA-256 before inspection. The [profile](profile.json)
contains per-sensor counts, totals, source integrity metadata and limitations.

This is a read-only opportunity assessment. It does not change sensor values,
production repair rules or the published 106-column schema.

## What was counted

For each original sensor column, validity follows the current Silver rules:
finite readings, choke openings within 0-100%, and valve codes in 0, 0.5, 1.
Other finite sensor values have no additional physical bounds. Consecutive
unusable readings are grouped into runs. A complete internal gap must have an
original valid reading at each end and remain within an uninterrupted one-second
timestamp sequence. Source recordings are never joined to create anchors.

All recordings have uninterrupted usable one-second timestamps in this source.
No sensor has original non-null NaN or infinite values. The only finite domain
violations are the 161,872 already audited production-choke readings. These
occur in a wholly invalid channel and a trailing run, so the complete internal
gaps counted below consist of original null readings.

Five synthetic checks cover gap lengths, timestamp breaks, leading gaps,
trailing gaps and entirely missing channels. Count conservation checks run for
every sensor in every recording. The total observation count and original
production-choke domain-invalid count agree with the earlier full-source audit.
The one-off read-only script and temporary source copy were removed after
validation during repository cleanup. Its method, counts and historical hash
remain recorded in the profile.

## Possible short-gap estimates

| Maximum complete gap length | Missing sensor cells | Separate sensor gaps |
|---|---:|---:|
| 3 seconds | 0 | 0 |
| 5 seconds | 435 | 87 |
| 10 seconds | 1,837 | 257 |
| 30 seconds | 7,855 | 636 |
| 60 seconds | 9,885 | 684 |
| 120 seconds | 20,337 | 810 |
| 300 seconds | 54,434 | 1,022 |

All these gaps occur in real recordings. Counts are cumulative sensor cells,
not unique observation rows, and do not include leading, trailing or entirely
unmeasured channels. The gap-length threshold is not the maximum age of a
previous reading: a three-cell gap at 1 Hz has four seconds between its bounding
valid readings. A policy that fills the start of a longer gap is a different
assessment. Complete-gap counts do not describe the first eligible portion of
longer or trailing runs. The historical three-second choke policy therefore produced
three estimates despite zero complete internal gaps of up to three cells. The
[current six-minute policy](../repair-window-extension/README.md) estimates
334 trailing invalid openings; that is a separate count from null sensor gaps.

There are 1,397,304,850 original null sensor cells across all source types.
There are 1,395,545,475 null cells in sensor channels that are null for their
entire recording. Such absence cannot be resolved by within-channel temporal
interpolation. These counts include sensors not supplied by the different
source-generation methods; they do not prove that an acquisition error occurred.

A same-well recording-range check also found no overlapping recording for either
of the two recordings with invalid production-choke values. Another copy of an
opening at the affected timestamps is therefore not available through that route
in this pinned source.

## How to evaluate repairs

Treat the gap counts as opportunities to test estimates, without calling them
recovered true readings. Use whole-recording train, validation and test splits;
where evaluation requires unseen wells, group by well. Fit learned imputers on
training data only. Mask known valid blocks with realistic lengths in validation
recordings, compare per-sensor reconstruction errors and downstream detection
performance, and select sensor-specific gap limits using validation results.
Artificial masking cannot establish accuracy for an entirely unobserved channel
or fully reproduce the mechanism behind real missingness.

For immediate event detection, test past-only estimates and measure age from the
last original valid reading. Retrospective linear interpolation uses the future
bounding value and requires a delayed or offline use case. Valve states are
categorical codes and should not be linearly interpolated; an equal-code bounded
gap can be tested under an explicit continuity assumption. Some state changes
may still occur inside such a gap.

For long or entirely absent channels, other-sensor prediction is a separate
model-based imputation experiment. Constant training medians can make selected
model inputs usable but do not reconstruct measured values. Four sensors have no
finite observations anywhere in this pinned source and offer no targets with
which to train or validate a sensor-specific reconstruction model. Keep source
values and inferred targets separate, and retain missingness/repair information
where consumers need it.
