# Class-label coverage review

Reviewed on 24 October 2026 against all 2,228 original Parquet recordings at
Hugging Face revision c1a7e8d0cfefc0875974ea079d11349a37fa0c56.
All source files were verified against the previously saved pinned source
manifest. The [profile](profile.json) records that verification and the results.

Of 76,587,318 observations, 4,028,400 have no class (5.26%). The remaining
72,558,918 observations have a source class. Real recordings contain 28,843,850
labelled observations and 4,028,400 unlabelled observations.

Every one of the 1,119 real recordings has exactly 3,600 initial observations
with no class. No internal or trailing class gaps occur in this pinned dataset.
Simulated and hand-drawn observations have no missing classes.

The method run-length-encodes each original class column and counts each null
run by location and by its adjacent observed labels. All missing runs start at
row zero and end before row 3,600. The profile includes example event-5
recordings with observed runs 'null, 0, 105, 5' or 'null, 0, 105'.

A hypothetical internal gap bounded by identical classes could be assigned
inferred labels under a continuity assumption. This rule has no applicable gaps
in the reviewed dataset. Filling the initial hour from a later class would
require an assumption about an unlabelled period; the recording event alone
does not identify the row class throughout that period.

Such inferred labels can be studied separately for training with explicit
provenance. Model evaluation should use observed source labels. Labelled real
rows remain eligible for supervised work when the other target and sensor
requirements are met. Unlabelled initial rows can still provide sensor history
for features calculated at later labelled observations. This review does not
change original or published class values.

## Adopted cleaning assumption

On 24 October 2026 the project owner explicitly chose to assume that each leading
unknown period has the same class as its first supplied class. The
[implementation and verification](../class-label-backfill/README.md) add
`class_code_clean` while retaining original classes and source-quality counts.
This changes the cleaning policy, not the earlier source-profile findings.
