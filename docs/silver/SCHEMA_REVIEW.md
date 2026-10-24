# Silver schema review

Reviewed on 24 October 2026. This is a design assessment of the current
108-column contract. The [measurements](../evidence/silver/schema-review/profile.json)
cover candidate-feature availability in the full pinned source, repair-column
storage in a saved development build, and current downstream notebook use.

## Decision

The four repair-provenance fields are optional export fields. Given the
preference for a simpler observation table, a future export can omit them while
retaining the internal repair calculations, aggregate quality counts and policy
documentation. Removing all four would produce 104 observation columns with the
same clean readings. This remains a design proposal. New builds now add
`class_code_clean` for the explicitly requested class assumptions and
`class_was_inferred` for their provenance, giving 108 columns; the published full VM baseline still has 106 columns.

Add no further persisted Silver columns merely to increase the column count.
The first additional feature to evaluate in Gold is downhole pressure minus
subsea-tree pressure. A production-choke pressure difference is a secondary
candidate with more limited coverage. Persist an additional Silver feature when
a consumer needs it and evaluation supports it. Predictive improvement has not
been established.

## Can repair information remain internal?

Yes. Under the current policy, an imputation flag can be reconstructed from a
finite, out-of-range original opening and a non-null clean replacement.
Source-reading age can also be reconstructed, but requires ordered original
readings, sequence boundaries and the saved policy.

Keeping the flags explicitly tells consumers which clean values are estimates.
Keeping the ages lets consumers audit the age limit without repeating the
sequence calculation. Current source-age fields also describe earlier anchors
for some unresolved values; they must be interpreted together with the flag.
Neither field is an automatic classifier predictor.

The four fields are computed during cleaning whether or not they are exported.
In the saved 30-recording, 1,931,052-observation build, their compressed Parquet
column data totals 104,435 bytes (about 102 KiB), or 0.045% of all compressed
column data. The 28 rolling-feature columns account for 121,606,139 bytes,
about 52%. These are development measurements; they do not quantify production
peak temporary storage. Removing four provenance fields offers little saved
storage benefit in this measured build.

For the current policy, internal calculation plus aggregate reporting is
sufficient if consumers do not need immediate access to row-level provenance.
The original and clean openings allow an imputation flag to be reconstructed.
A separate sparse repair log is another option when row-level repair audits are
needed, at the cost of another representation and a join. If repair methods
expand to other sensors or learned estimates, preserve their provenance in an
appropriate audit representation even if it is omitted from the main table.

The [sensor-gap review](../evidence/silver/sensor-gap-review/README.md) identifies
actual internal missing-sensor gaps to evaluate before changing repair scope.

## Additional feature candidates

Coverage below means both input readings are finite under the existing rules.
The source is all 2,228 recordings and 76,587,318 observations at pinned revision
c1a7e8d0cfefc0875974ea079d11349a37fa0c56.

| Candidate | Overall coverage | Real-data coverage or limitation | Decision |
|---|---:|---|---|
| Downhole PDG pressure minus subsea-tree TPT pressure | 87.97% | 23,657,646 real observations, about 72% of real rows | First Gold feature to evaluate; use a signed pressure difference in Pa. |
| Production-choke upstream minus downstream pressure | 28.88% | 3,448,901 real observations, about 10.5% of real rows | Secondary Gold candidate; report coverage by source type. |
| Gas-lift choke upstream minus downstream pressure | 0.40% | Available in only two recordings | Do not prioritise for the current dataset. |
| Production-choke upstream minus downstream temperature | 24.37% | Available only in simulated recordings | Do not prioritise for evaluation on real wells. |

Compute each candidate from clean inputs and return null when either input is
unavailable. Preserve the sign. The downhole-to-tree difference spans different
physical locations and includes operating and hydrostatic effects; it is not a
direct measurement of one component's pressure loss.

The downstream notebook currently constructs sequences from selected clean
sensors, then calculates window mean, standard deviation, minimum, maximum,
slope and missing fraction. It does not currently feed Silver's repair fields
or 120-second summaries into the model matrix. Adding more Silver columns would
therefore require an explicit consumer change to affect those experiments.

Evaluate the proposed pressure difference against the same model without it,
using identical recording splits and observed held-out targets. Fit sensor
selection, imputation and scaling using training data only. Availability and
physical interpretation make it a candidate; predictive benefit remains
unmeasured.

## Fields not worth adding to Silver now

- Coverage percentage duplicates the existing usable count divided by 120.
- A per-second rate duplicates the previous-reading change within a continuous
  one-second segment.
- Deviation from the existing mean is a simple subtraction in Gold.
- Twenty-seven additional validity flags largely duplicate original/clean
  values and the existing opening-imputation flags.
- Label-availability flags duplicate class-null checks and class-label status.
- Additional 300-second means, deviations and trends contain potentially new
  temporal information, but their usefulness and history length require model
  evaluation. They should be compared in Gold before being materialized at
  every Silver observation.

The existing count fields remain useful: a mean based on one reading and a
mean based on 120 readings have different support. Their retention is more
important than adding duplicate percentages or validity fields.
