# Silver assumptions

This document records why Silver prepares the data as it does and where those choices limit its use. The [contract](SILVER_CONTRACT.md) lists the saved fields. Exact executable settings are in [`silver_rules.yaml`](../../conf/quality/silver_rules.yaml); that file is the source of truth for thresholds.

## Labels are source information

The recording label describes the event assigned to a recording. The row class describes a condition at that point in the recording. A missing class does not reveal when a transition occurred, so Silver leaves it unknown. It does not copy the recording label into missing row labels or infer a transition from neighboring classes. This avoids creating training labels that were not observed.

Label-derived names and consistency status are metadata. Gold defines the model target and its labelled windows. Silver does not infer when an event begins or claim to predict one.

## Measurement preparation

`<sensor>_clean` contains the original value when it is usable, a permitted choke repair when available, or null otherwise. Silver applies no invented physical bounds to pressure, temperature, or flow. It does not replace missing sensor readings with zero; other sensors on the same observation remain usable. The original sensor columns and detailed quality issues remain available for review.

For choke openings, Silver repairs only finite out-of-range values when a recent original valid reading exists earlier in the same continuous sequence. It carries that original value forward for the configured maximum age. Repairs never become new anchors. Missing, NaN, infinite, leading, stale, or cross-boundary values are not repaired. This is a bounded causal carry-forward, not interpolation or clipping. The original value and quality issue remain available, and provenance columns identify repairs and anchor age.

The carry-forward assumes an opening remains unchanged briefly when its reported value is invalid. That may be wrong during a real choke adjustment. The limit is an engineering assumption, not a property proven by the dataset. The masking experiment in `scripts/evaluate_choke_repair.py` measures reconstruction error on sampled valid readings; it does not prove that repairs recover faults or improve forecasting.

Rows with no usable sensor values remain in the table and sequence. Silver does not store per-row sensor counts; Gold can calculate them from clean values when needed.

## Time and sequences

Source order (`_row_in_file`) defines predecessors. Missing time, non-increasing time, a gap or cadence change, or a break in row positions starts a new sequence segment. Choke repairs do not cross these boundaries. Unusable timestamps remain in the data. Read sequences in source-row order; timestamp sorting can conceal backward source times.

Silver stores observation-level facts and quality details, not model-specific window aggregates. Gold can calculate sensor counts, pressure differences, and model-window rates, coverage, and statistics from the cleaned sensor values, timestamps, sequence segments, and labels. This keeps Silver reusable if Gold later changes its model or window size. Silver learns no scaling or model parameters.
