# Leading class-label backfill

On 24 October 2026 the project owner explicitly authorized this assumption:
the leading unknown period has the same class as the first supplied class in
the recording. New Silver builds apply it in class_code_clean. Original
class_code and its source-quality warnings remain unchanged.

The rule uses source-file path and hash to isolate recordings, and original row
positions to find the first supplied class. That class must be consistent with
the recording event. It is copied into earlier null rows. The later matching-endpoint extension also
fills internal null gaps when the nearest original classes before and after the
gap match and both are consistent. Trailing nulls, gaps between different classes,
entirely unlabelled recordings and inconsistent boundaries remain unfilled. Supplied labels are never overwritten. No recording-event
code is substituted for an absent row class.

The implementation adds a nullable short clean class column and the non-null
boolean class_was_inferred, making the observation schema 108 columns (internal
contract version 3). The flag is true for a source-null class with a non-null
clean class, and false for observed labels or unresolved missing classes. Preparation
summaries add class_code_inferred_rows and class_code_unresolved_rows. The policy
is saved in the resolved rules and can be switched back to preserve_unknown.

## Source coverage

All 2,228 pinned original recordings contain 76,587,318 observations. The
4,028,400 source null classes are the first 3,600 rows of each of 1,119 real
recordings. Their first supplied class is consistent with the recording event.
The requested assumption fills all these targets:

| Inferred class | Rows |
|---|---:|
| 0 (normal) | 2,678,400 |
| 3 (severe slugging) | 115,200 |
| 4 (flow instability) | 1,234,800 |
| Total | 4,028,400 |

The [verification](verification.json) checks every pinned source file hash and
scans all original class columns. Its Spark input contains every original null
class row plus the first supplied anchor row in each affected recording:
4,029,519 rows. Later observed rows are omitted from that compact verification
because their labels are preserved directly. Focused tests cover source
preservation, internal/trailing gaps, all-null recordings, file-version
isolation, inconsistent anchors and disabling the assumption.

## Tests

The [saved checks](checks.json) record 20 focused tests and two publication
integration tests passing. The latter verify successful publication, repeat-run
equality across batch settings, and rejection of corrupted clean classes.
Repository lint, formatting and whitespace checks passed.

## Interpretation and publication

This verifies implementation and coverage, not the correctness of the assumption.
The initial class and precise event onset were not annotated there. The inferred
label uses future label information, so it is a target construction step rather
than a causal sensor feature. Assumed targets can be used for training; source
labels distinguish expert-labelled evaluation from assumed-label evaluation.
Original class descriptions and missing-label summaries continue to describe the
source, even when the new clean target is present.

Existing full VM datasets retain their original 106-column schema and label
policy. Rebuild Silver to publish the new field; this review does not claim a
new full seven-table publication. The six-minute choke repair remains in effect.

## Runtime simplification and repository cleanup

Label inference now aggregates only label metadata and broadcasts one anchor
per recording, avoiding a full-recording look-ahead window over sensor rows.
The [cleanup verification](cleanup_verification.json) checks the same 4,028,400
assumed targets with both implementations and records a small cached component
benchmark. Those timings are not a full Silver-runtime prediction.

One-off audit programs, their tests and the temporary downloaded/converted data
were removed at the project owner's request. Compact evidence reports and
production regression tests remain. Historical test commands refer to files
that existed when those earlier checks were performed.

After cleanup, 21 focused preparation, label and publication/repeat-run tests
passed against the simplified implementation. Lint and formatting passed.

## Matching-endpoint gap rule

At the owner's request, the default missing_classes policy is now
backfill_leading_and_matching. The earlier backfill_leading and preserve_unknown
settings remain supported. Internal gaps use only original labels from the same
source path and file hash: 105, null, null, 105 becomes 105, 105, 105, 105. A gap
between 105 and 5 stays unknown. Matching endpoints are an explicit continuity
assumption rather than confirmation of every missing observation's class.

The matching-gap rule reuses class_code_clean. The subsequently requested
class_was_inferred flag identifies both leading and internal inferred labels.
Narrow neighbour windows are restricted to recordings with missing labels after
their first supplied class. Supplied labels and source-quality reports remain
unchanged. The earlier full-source profile found no internal class gaps, so its
4,028,400 leading-gap inference count remains the expected count for that pinned
source. The extension is covered by synthetic examples and boundary tests; it
does not claim a fresh scan of the current production Bronze table.

## Explicit inference flag

At the owner's request, class_was_inferred is now exported as a non-null boolean.
It is true for labels filled by either inference rule, and false for original
labels, unresolved gaps and every row when preserve_unknown is configured. The
preparation inferred-row count uses this flag. This is a simple projection over
original/clean class availability and adds no extra join or inference operation.
Existing saved historical verification records retain their original schemas.
