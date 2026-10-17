# Silver assumptions

This document records why Silver prepares the data as it does and where those choices limit its use. The [contract](SILVER_CONTRACT.md) lists the saved fields. Exact executable settings are in [`silver_rules.yaml`](../../conf/quality/silver_rules.yaml); that file is the source of truth for thresholds.

## Labels are source information

The recording label describes the event assigned to a recording. The row class describes a condition at that point in the recording. A missing class does not reveal when a transition occurred, so Silver leaves it unknown. It does not copy the recording label into missing row labels or infer a transition from neighboring classes. This avoids creating training labels that were not observed.

Label-derived names and consistency status are metadata. A future forecasting model must define its own event-onset target and prediction horizon. It must also exclude target or post-event information from its predictors. Silver does not claim that its current features predict an event.

## Measurement preparation

`<sensor>_is_valid` describes the original value using finiteness and, where defined, the sensor's domain. `<sensor>_clean` contains a usable value or null. Silver applies no invented physical bounds to pressure, temperature, or flow. It does not replace missing sensor readings with zero; other sensors on the same observation remain usable.

For choke openings, Silver repairs only finite out-of-range values when a recent original valid reading exists earlier in the same continuous sequence. It carries that original value forward for the configured maximum age. Repairs never become new anchors. Missing, NaN, infinite, leading, stale, or cross-boundary values are not repaired. This is a bounded causal carry-forward, not interpolation or clipping. The original value and quality issue remain available, and provenance columns identify repairs and anchor age.

The carry-forward assumes an opening remains unchanged briefly when its reported value is invalid. That may be wrong during a real choke adjustment. The limit is an engineering assumption, not a property proven by the dataset. The masking experiment in `scripts/evaluate_choke_repair.py` measures reconstruction error on sampled valid readings; it does not prove that repairs recover faults or improve forecasting.

Rows with no finite original sensor value remain in the table and sequence. `has_observed_sensor_evidence` indicates whether at least one original sensor was valid; it is not a complete model eligibility rule. Prepared values can include a repaired opening, so prepared and observed sensor counts have different meanings.

## Time and features

Source order (`_row_in_file`) defines predecessors. Missing time, non-increasing time, a gap or cadence change, or a break in row positions starts a new history segment. Temporal features and repairs do not cross these boundaries. Unusable timestamps remain in the data and do not receive temporal features. Read sequences in source-row order; timestamp sorting can conceal backward source times.

For the seven selected continuous sensors, rolling windows use elapsed time and contain readings in `(t-W, t]`; deviation compares the current value with the earlier window `[t-W, t)`. Rolling averages and variation use clean values. Silver uses original valid readings to decide whether a window has enough coverage, but does not save a separate coverage field. Gold can calculate rates and coverage from the cleaned readings, validity flags, timestamps, and sequence segments if it needs them.

The minimum observed coverage, selected sensors, cadence, and window lengths are starting assumptions. They allow partial histories while limiting very sparse windows, but have not been selected through downstream model evaluation. Silver learns no scaling, imputation, or model parameters. Evaluation should split by recording and, where appropriate, well; unknown future labels and recording endings must not be treated as negative outcomes.
