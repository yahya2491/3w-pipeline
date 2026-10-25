# Silver assumptions

These assumptions explain the [quality rules](QUALITY_RULES.md). The
[two full-run reports](../evidence/silver/v3/README.md) record their application;
validation establishes implementation and repeatability, not physical truth.

- A missing source class means no condition was supplied. For cleaning, we assume
  the leading unknown period matches the first supplied class in that recording,
  provided that class agrees with its recording event. For an internal null gap,
  we assume the class stayed unchanged only when the nearest original labels on
  both sides match and are consistent. Different-class gaps, trailing nulls,
  inconsistent boundaries and entirely unlabelled recordings remain unknown.
  The recording event alone is never used as the replacement class.
- Inferred targets are saved in `class_code_clean` and identified by
  `class_was_inferred`; original classes stay in Bronze. This uses later labels
  and cannot establish the true event onset. Both full builds inferred 4,028,400
  labels, leaving zero clean-class nulls. Inferred targets may train a model but
  must not be treated as supplied expert labels in held-out evaluation. The class,
  inference flag and event descriptions must stay out of model predictors.
- Missing sensor readings are not zero. A problem with one measurement does not
  make all other measurements in that observation unusable.
- The source's documented opening and valve domains provide validation limits.
  We have no justified physical limits for pressure, temperature or flow;
  extreme finite values may be real well behaviour and are retained.
- An original valid choke opening may persist for up to 360 seconds while the
  reported opening is finite but out of range. The same recording and continuous
  one-second segment are required. Estimates never become repair anchors.
  Persistence can be wrong during a real adjustment; an imputation flag and
  source-reading age identify each estimate. Both builds report 334 repairs and
  161,538 unresolved invalid openings. The rule does not reconstruct a channel
  without an eligible original-valid anchor and does not fill missing readings.
- Original row positions define source order, with one-second sampling expected.
  Sequence breaks prevent repairs and temporal histories from spanning the gap.
- The seven selected sensors and 120-second history are candidate model inputs.
  Their predictive value and the best history length require Gold evaluation.
  Persistent invalid openings indicate measurement problems, not proof of an event.
- The 79-column export keeps one clean value per sensor/class. Exact source keys
  recover originals from Bronze; source-quality reports remain source-based even
  after an inferred target or repaired sensor becomes available.
