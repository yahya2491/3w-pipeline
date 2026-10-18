# Silver assumptions

These assumptions explain the choices behind the [quality rules](QUALITY_RULES.md).

- A missing class means the condition is unknown. The recording label cannot tell us exactly when its rows changed class.
- Missing sensor readings are not zero, and a problem with one sensor does not make every other sensor in that row unusable.
- The source's documented opening and valve domains are suitable validation limits. We have no justified physical limits for pressure, temperature, or flow; extreme readings may be real well behaviour.
- A choke opening may remain unchanged for up to three seconds while its reported value is invalid. Carrying forward an original valid reading is an estimate and can be wrong during an actual adjustment.
- Original row positions describe the sequence, with one-second sampling expected. A break in that sequence prevents us from treating readings on either side as continuous.
- The seven selected sensors and 120-second history are reasonable feature candidates. Their value for classification, and the best history length, still need evaluation in Gold.
- Persistent invalid openings indicate a problem with the reported measurements. They do not establish that a labelled well event is occurring.
