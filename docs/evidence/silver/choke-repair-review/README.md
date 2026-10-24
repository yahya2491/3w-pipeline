# Production-choke repair coverage review

The three-second policy had negligible imputation coverage. A full-source review
on 22 October 2026 establishes that widening the window alone cannot repair half
of the invalid production-choke readings. Even unlimited carry-forward within a
recording can reach only 334 of 161,872 readings (about 0.21%).

## Source and verification

The review covers all 2,228 original Parquet recordings and 76,587,318
observations from `Yahya2491/3w-pipeline`, pinned to
`c1a7e8d0cfefc0875974ea079d11349a37fa0c56`. All source files were checked against
the pinned Hub file hashes before analysis. The saved [profile](profile.json)
records source-manifest integrity verification, the affected recordings' hashes,
and agreement with the first full run's observation and three-second repair
counts. Original source files and published Silver runs were not rewritten.

The audit uses the production rule's boundaries: consecutive source rows at
one-second intervals, usable timestamps, the same recording, and an earlier
original finite opening within 0–100%. Estimated values never become anchors.
Six focused tests cover anchor ages, sequence breaks, future-only anchors,
entirely invalid recordings, non-finite values, and the half-coverage calculation.

## What the invalid readings actually contain

| Source recording | Invalid readings | Evidence |
|---|---:|---|
| `8/WELL-00030_20141108012623.parquet` | 161,538 | Every observation reports exactly `-99.99`. There is no valid production-choke measurement anywhere in this recording, before or after the invalid values. |
| `8/WELL-00025_20200629194141.parquet` | 334 | The final 334 readings range from 100.198% to 100.4375%. The preceding original valid opening is 99.95956%. There is no later valid reading. |

The first recording accounts for about 99.79% of all finite out-of-range
production-choke readings. The meaning of `-99.99` is unconfirmed; its repeated
appearance does not establish the true opening. Neither longer forward-fill nor
within-recording backward-fill or interpolation can reconstruct this entirely
unobserved channel from its own measurements.

## Coverage of wider windows

| Maximum age of the original valid opening | Eligible invalid readings |
|---|---:|
| 3 seconds | 3 |
| 10 seconds | 10 |
| 30 seconds | 30 |
| 60 seconds | 60 |
| 120 seconds | 120 |
| 300 seconds | 300 |
| 600 seconds | 334 |
| 1 day | 334 |
| Unlimited, within the same recording and continuous segment | 334 |

The maximum original-anchor age among reachable readings is 334 seconds. Half
coverage would require 80,936 readings. No age limit can meet that target using
the same recording's original valid openings.

These counts measure possible estimates, not their accuracy. The earlier
[short-gap masking experiment](../v2/dev_evidence/choke_repair_evaluation.json)
tested one-to-three-second carry-forward on known valid readings. It does not
validate a five-minute fill or estimates for the entirely invalid recording.
This original source audit did not change the three-second configuration. A
subsequent [extension review](../repair-window-extension/README.md) evaluated
longer windows and changed the default to 360 seconds on 24 October 2026,
reaching all 334 eligible readings. The saved historical reports and original
audit provenance remain unchanged. The finding corrects the earlier suggestion
that a more lenient window might substantially improve coverage.

Recovering the dominant recording's opening would require independent
measurements or a separately evaluated model using other sensors. Such outputs
would be predictions of an unobserved channel. A count of filled cells alone
would not establish successful recovery. Its other measured sensors remain
available for analysis under the existing field-specific quality rules.

## Audit cleanup

The one-off source-review program, its audit-only tests and downloaded source
copy were removed after validation at the project owner's request. The pinned
source revision, measured counts, file-integrity results and historical checks
remain in the saved evidence. These audit programs were never part of normal
Silver processing. Current regression tests cover the production repair rule.
