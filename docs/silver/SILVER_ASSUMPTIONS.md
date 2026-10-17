# Silver assumptions

These are the assumptions behind the current v2 implementation. Field definitions are in the [contract](SILVER_CONTRACT.md); executable settings are in [`silver_rules.yaml`](../../conf/quality/silver_rules.yaml).

- A missing row class is unknown. The recording's event label does not tell us exactly when its rows changed class, so we do not invent labels.
- Documented choke percentages and valve states provide usable value domains. We do not invent pressure, temperature, or flow limits that could remove real well events.
- A valid choke opening may remain unchanged briefly while its reported value is invalid. The three-second carry-forward is an engineering assumption and can be wrong during an actual adjustment.
- Only original valid openings can anchor repairs. Estimated values never become anchors, and originals remain available for review.
- Missing sensors are not zero. Rows with no usable readings remain in Silver so their timestamps, labels, and source identities are preserved.
- Original row order defines predecessors. Missing/bad time, a cadence change, or a row-position gap breaks continuity; repairs and features must restart there.
- The documented cadence is one second. Fractional timestamp precision is preserved; the 120-second feature range is based on elapsed time.
- Recent levels, variation, and changes in the seven selected sensors are useful candidates for Gold. Their contribution to classification must still be evaluated; 120 seconds is not proven optimal.
- Features use current and earlier clean readings only. Silver learns no imputation, scaling, or model parameters from the full dataset.
- Repeated invalid choke openings describe a sensor reporting problem. They do not establish that a labelled well event is occurring.
- Historical masked-reading results measure reconstruction on hidden valid values. They do not establish the true value of faulty measurements or prove improved classification.
