# Severity map — the mapping as policy, and the literals observed

Two things live in this document, and the boundary between them is deliberate.

**Part 1 is the mapping as fixed policy.** Every table, boundary and precedence
rule below was decided in advance, from the plan, and none of it was fitted to
what the artifacts turned out to contain. That is the whole point of stating it
separately: a mapping authored after the output is read can be arranged to
produce a distribution somebody wanted, and one authored before it cannot.

**Part 2 is what the artifacts actually contained** — every native severity
literal observed, per tool, with the row count it affected.

This document and `oss-scan-results/tool-status.md` are together the
**authoritative inventory of the nine tools**. `oss-scan-results/findings.json`
and `oss-scan-results/findings.csv` are row-only — one record per finding, twelve
fields, no metadata envelope — so a tool that produced no row is invisible in
them by construction. Two of the nine are in exactly that position here,
`osv-scanner` and `dependency-check`, and both hold a full entry below with
`oss-scan-results/tool-status.md` cited for their status.

All nine canonical identifiers have an entry. Nothing here ranks the tools,
contrasts their severity vocabularies, or reads one tool's figure against
another's; mapping two tools' labels into the same band is a mapping statement
and not a comparison. Nothing here judges a finding.

## Where this document sits in the pipeline

It is an **output** of normalization, never an input to it. The policy is owned
by code; the counts are owned by the run record; this document renders both and
measures neither a second time. Where a figure appears both here and in another
document it is **one measurement cited twice**.

| Input | What is taken from it |
| --- | --- |
| `harness/lib/normalize/severity.py` | The three mapping tables, the closed output vocabulary, the five basis constants, the four authored policy statements quoted verbatim below, and the `LiteralTally` accumulator whose readout Part 2 renders |
| `harness/artifacts/logs/normalize-run.json` | `severity_literals` — the tally itself: per tool, every observed literal with its band, its basis and its row count, plus the per-tool and dataset row totals |
| `oss-scan-results/findings.json`, `oss-scan-results/findings.csv` | The dataset the literals were observed in; the row counts here are the counts in those two files |
| `oss-scan-results/tool-status.md` | The two zero-row tools' status, and the comparability determination for every tool |

The tally is **rendered, not recounted**. `harness/lib/normalize/severity.py`
accumulated it while the rows were built, `harness/lib/normalize/cli.py` wrote it
into `harness/artifacts/logs/normalize-run.json` under `severity_literals`, and
Part 2 is that readout. The independent traversal that reconciles record counts
belongs to `harness/lib/normalize/reconcile.py` and its results belong to
`oss-scan-results/tool-status.md`; this document does not restate them.

The policy code carries its own self-check: `python3
harness/lib/normalize/severity.py` exits 0 reporting `severity policy
self-check: 270/270 passed`. Among those checks is one that matters to Part 1
directly — that the **displayed** form of the CVSS band table and the ordered
numeric comparisons that implement it agree, so the table below is not a
description of the code that could drift from it.

---

# Part 1 — The mapping as fixed policy

## The closed output vocabulary

`severity_norm` takes one of exactly five literals, spelled as follows, most
severe first:

`Critical` | `High` | `Medium` | `Low` | `Info`

**`severity_norm` is never absent.** The invariant is enforced on every
construction of a severity result in `harness/lib/normalize/severity.py` and
raises rather than asserting, so `python -O` cannot strip it. Absence is
permitted for `severity_native` and for four other fields of the schema, and
never for this one. Across the dataset, `row_validation.severity_norm_absent` is
**0**.

## Table 1 — SARIF `level`

The lookup is case-insensitive; `severity_native` records the literal as
observed.

| SARIF `level` | `severity_norm` |
| --- | --- |
| `error` | High |
| `warning` | Medium |
| `note` | Low |
| `none` | Info |

## Table 2 — label vocabularies

Applies to Trivy, Checkov, Dependency-Check and OSV ecosystem labels. The lookup
strips surrounding whitespace and upper-cases the literal; nothing else is done
to it, and the observed spelling is what reaches `severity_native`. Case folding
is load-bearing rather than cosmetic: a real artifact can carry both `MODERATE`
and lower-case `moderate` for the same band, and a case-sensitive table would map
one and miss the other.

| Native label (matched case-insensitively) | `severity_norm` |
| --- | --- |
| `CRITICAL` | Critical |
| `HIGH` | High |
| `MODERATE`, `MEDIUM` | Medium |
| `LOW` | Low |
| `NEGLIGIBLE`, `INFO`, `INFORMATIONAL`, `UNKNOWN`, `NONE` | Info |

## Table 3 — CVSS numeric score

| Score | `severity_norm` |
| --- | --- |
| 9.0 – 10.0 | Critical |
| 7.0 – 8.9 | High |
| 4.0 – 6.9 | Medium |
| 0.1 – 3.9 | Low |
| 0.0 | Info |

**The banding is numeric.** It is an ordered comparison on a float —
`>= 9.0`, `>= 7.0`, `>= 4.0`, `> 0.0`, `== 0.0` — and never a comparison on text.
That is not a stylistic preference: a severity field can arrive carrying a
float32-to-float64 representation tail, and a lexical test bands such a value
wrongly.

A value that will not coerce to a float, or that coerces to a number outside the
closed interval 0.0 – 10.0, is **not banded and not clamped**. It is disclosed as
an unmapped literal instead.

## The CVSS `None` band, emitted under this dataset's own label `Info`

The CVSS v3.1 qualitative severity rating scale, specification document section
5, names **five** bands: **None at 0.0**, Low 0.1 – 3.9, Medium 4.0 – 6.9, High
7.0 – 8.9 and Critical 9.0 – 10.0.

**The four boundaries and the other four labels in Table 3 are the standard's.**
This dataset's `severity_norm` vocabulary has no `None` label, so the standard's
`None` band is emitted under this dataset's own label `Info`.

**That relabelling is a mapping this dataset defines. It is not a CVSS label.**

The statement as authored in `harness/lib/normalize/severity.py`, quoted verbatim
so that this document and the code cannot disagree:

> The CVSS v3.1 qualitative scale (specification document, section 5) names five
> bands: None at 0.0, Low 0.1-3.9, Medium 4.0-6.9, High 7.0-8.9 and Critical
> 9.0-10.0. This dataset's severity_norm vocabulary has no None label, so the
> standard's None band is emitted under this dataset's own label Info. That
> relabelling is a mapping this dataset defines, not a CVSS label. The other four
> labels and all four boundaries are the standard's.

`Info` also serves as the band for the no-vocabulary case and for a disclosed
unmapped literal, which is exactly why the **basis** of every row is recorded
alongside its band: an `Info` reached by policy and an `Info` reached from the
standard's `None` band are not the same fact, and the band alone cannot tell them
apart.

## No vocabulary at all

Where a record carries no severity vocabulary, `severity_native` is **absent**,
`severity_norm` is **`Info`**, and the basis is recorded as `no_vocabulary` — the
**absence is stated rather than a level assumed**. Absence is `null` in
`findings.json` and an empty field in `findings.csv`, and the two agree row for
row.

This is the path for a tool whose record shape defines no severity member at all,
and for a record whose severity member is present and null. Part 2 names which
tools took it and on which of those two grounds.

## Precedence: the label governs; a score is consulted only where no mapped label exists

> Precedence when a label and one or more scores coexist: the native label
> governs whenever it is in the mapped vocabulary, and a CVSS score is consulted
> only where no mapped label exists. Either way the entry used is recorded -- the
> label, or the score with its source and version.

The evaluation order in `harness/lib/normalize/severity.py`, in full, with the
basis each step records:

| Order | Condition | Band from | Basis recorded |
| --- | --- | --- | --- |
| 1 | A SARIF `level` present and in Table 1 | Table 1 | `sarif_level` |
| 2 | A label present and in Table 2 | Table 2 | `label` |
| 3 | No mapped label, and at least one bandable score candidate | Table 3 | `cvss_score` |
| 4 | No mapped label and no bandable candidate, but the label is itself a numeric CVSS base score in range | Table 3 | `cvss_score`, with the entry's source recorded as `label` |
| 5 | A literal surviving all of the above | Policy: `Info` | `unmapped_literal` |
| 6 | Nothing at all | Policy: `Info`, `severity_native` absent | `no_vocabulary` |

Step 2 does not consult a score even when one was supplied. That is the
precedence rule doing its work rather than an omission.

## Recording the entry that was used

Every row records **what was used**, not merely the band it produced. Two shapes
occur, and no third:

- `{"label": "<the literal as observed>"}` — for a mapped label and for a SARIF
  level.
- `{"score": <full-precision float>, "source": <source or absent>, "version":
  <CVSS version or absent>}` — for a score.

Recording the score's **source and version** is not decoration. An advisory
commonly carries several scores from different providers and different CVSS
versions, so *which* entry governed has to be recorded rather than implied.
Where more than one candidate is bandable, selection is by a documented total
order — highest CVSS version, then highest score, then lexicographically smallest
source, then earliest position in the supplied sequence — so two runs over the
same input cannot select differently.

## Score rendering: exactly one decimal place

> Where a CVSS score is what severity_native records, it is rendered with exactly
> one decimal place: 3.200000047683716 is recorded as 3.2, 7.5 as 7.5, 10.0 as
> 10.0 and 0.0 as 0.0. Band selection is performed on the full-precision float,
> never on the rendered text, and the full-precision value is retained in the
> selected score entry.

Two consequences, both load-bearing. **No representation tail reaches a text
field**: an input measured as `3.200000047683716` reaches `severity_native` as
`3.2`. And **nothing is lost**: the full-precision value is retained in the
selected score entry, which is where a reader who needs the exact input finds it.
The band is decided before the rendering and from the float, so the rendering
cannot move a value across a boundary.

## A literal outside every mapped vocabulary

> A literal outside every mapped vocabulary maps to Info and is listed here with
> the rows it affected. A CVSS vector string is such a literal rather than a
> score: it is neither a mapped label nor a number, so it is disclosed as an
> unmapped literal instead of being banded numerically. A numeric value that will
> not coerce to a float, or that falls outside 0.0-10.0, is likewise disclosed
> rather than clamped.

`Info` on its own would be indistinguishable from a deliberate `Info`, which is
why the basis is `unmapped_literal` and the literal itself is carried into the
tally. Part 2 lists every unmapped literal observed, with its rows.

---

# Part 2 — The literals observed

Every figure in this part is the tally in `harness/artifacts/logs/normalize-run.json`
under `severity_literals`, rendered. The dataset it describes is
`oss-scan-results/findings.json` and `oss-scan-results/findings.csv`, **9,433
rows**, whose per-tool row counts are the same measurement
`oss-scan-results/tool-status.md` carries in its inventory table.

## How the bands were arrived at, across the dataset

The basis is recorded per row, so the paths of Part 1 can be counted rather than
assumed:

| Basis | Rows |
| --- | --- |
| `sarif_level` (Table 1) | 9,316 |
| `label` (Table 2) | 110 |
| `no_vocabulary` (policy) | 7 |
| `cvss_score` (Table 3) | **0** |
| `unmapped_literal` (policy) | **0** |
| Total | 9,433 |

Two of those zeros are statements about this run rather than about the policy.
**No row in this dataset was banded from a CVSS numeric score**, so no score
entry — no source, no version — was recorded for any row: the requirement that
the selected entry be recorded wherever a score governed is satisfied over an
empty set, and Table 3 was exercised here only by the committed adapter fixtures
named at the end of this part. **No unmapped literal was observed**, so the
disclosure list below is empty for every one of the nine tools; every literal
that arrived fell inside a mapped vocabulary.

`severity_native` is absent on exactly **7** rows, which is the
`no_vocabulary` count above and matches
`normalize-run.json` `row_validation.absence_by_optional_field.severity_native`.

Band totals across the dataset, which are the same 9,433 rows grouped the other
way:

| `severity_norm` | Rows |
| --- | --- |
| Critical | 0 |
| High | 850 |
| Medium | 2,792 |
| Low | 5,764 |
| Info | 27 |
| Total | 9,433 |

The `Critical` band is unrepresented: no row of any tool took it. That is a
count of this dataset and nothing more — it is not read as a statement about any
tool, any rule set or the code that was scanned.

## opengrep

`scanner_class` **sast**. Vocabulary: **SARIF `level`** (Table 1). 1,322 rows.

| `severity_native` | `severity_norm` | Basis | Rows |
| --- | --- | --- | --- |
| `error` | High | `sarif_level` | 314 |
| `warning` | Medium | `sarif_level` | 731 |
| `note` | Low | `sarif_level` | 277 |

Unmapped literals: **none**. Entry recorded for every row: the observed level as
a label entry. Comparability: **comparable** — the observed ruleset identity is
the expected identity.

## semgrep

`scanner_class` **sast**. Vocabulary: **SARIF `level`** (Table 1). 1,162 rows.

| `severity_native` | `severity_norm` | Basis | Rows |
| --- | --- | --- | --- |
| `error` | High | `sarif_level` | 278 |
| `warning` | Medium | `sarif_level` | 675 |
| `note` | Low | `sarif_level` | 209 |

Unmapped literals: **none**. Comparability: **comparable** — the observed ruleset
identity is the expected identity.

## datadog-static-analyzer

`scanner_class` **sast**. Vocabulary: **SARIF `level`** (Table 1). 6,832 rows.

| `severity_native` | `severity_norm` | Basis | Rows |
| --- | --- | --- | --- |
| `error` | High | `sarif_level` | 195 |
| `warning` | Medium | `sarif_level` | 1,342 |
| `note` | Low | `sarif_level` | 5,275 |
| `none` | Info | `sarif_level` | 20 |

Unmapped literals: **none**. The 20 rows carrying the SARIF level `none` reached
the `Info` band through a mapped vocabulary rather than through the no-vocabulary
path; the recorded basis `sarif_level` is what distinguishes them from an `Info`
row whose basis is `no_vocabulary`.

**Comparability: NOT COMPARABLE WITH THE REHEARSAL.** The ruleset digest differs
from the expected identity — observed sha256
`4f397e81414f8e9469d20abc18c80c85c722e72b9f85b8bcf69dbe34b8fef6f1`, expected
sha256 `e70ede308813b6d8c4087b0995609cdafdb9ab48159a313fe58ac343ff6c44f7` — and a
different rule set produces a different count for reasons that have nothing to do
with the code, so the per-literal counts above and this tool's finding count must
not be read against the rehearsal's figures. The ruleset count (48) and the rule
count (1,093) do match. This is the same determination
`oss-scan-results/tool-status.md` records for this tool, cited here rather than
made again.

## joern

`scanner_class` **sast**. Vocabulary: **label** (Table 2). 107 rows.

| `severity_native` | `severity_norm` | Basis | Rows |
| --- | --- | --- | --- |
| `HIGH` | High | `label` | 63 |
| `MEDIUM` | Medium | `label` | 44 |

Unmapped literals: **none**.

**A query does define a severity here, so these rows take the label path rather
than the no-vocabulary path.** The policy anticipates both cases for this tool —
no vocabulary *unless a query defines one* — and in this provisioning the baked
query set defines one per query, declared in `harness/lib/joern-scan.sc` at lines
48 to 71, where the query record type carries a `severity` member and each of the
six entries supplies it:

| Query identifier | Declared severity |
| --- | --- |
| `joern-process-exec` | `HIGH` |
| `joern-unsafe-deserialization` | `HIGH` |
| `joern-reflection-forname` | `MEDIUM` |
| `joern-message-digest` | `MEDIUM` |
| `joern-cipher-getinstance` | `MEDIUM` |
| `joern-xml-factory` | `MEDIUM` |

Every one of the artifact's 692 finding records carries the declared severity of
the query that produced it, giving `HIGH` 233 and `MEDIUM` 459 **in the
artifact**. The row counts in the table above are **63 and 44**, over the 107 rows
this tool contributed to the dataset: this artifact's parse status is `partial`
and 585 of its records were rejected under the single class `unresolvable_path`,
so a rejected record contributes no row and therefore no literal. The two figures
are different measurements of different things and are not reconciled against each
other here; the record-level identity `692 = 107 + 585` belongs to
`oss-scan-results/tool-status.md`, which owns it.

Comparability: **comparable** — the observed query-set identity is the expected
one.

## gitleaks

`scanner_class` **secret**. **This tool defines no severity vocabulary.** Its
record shape carries no severity member at all — the members are `Author`,
`Commit`, `Date`, `Description`, `Email`, `EndColumn`, `EndLine`, `Entropy`,
`File`, `Fingerprint`, `Match`, `Message`, `RuleID`, `Secret`, `StartColumn`,
`StartLine`, `SymlinkFile` and `Tags`, and none of them is a severity. 1 row.

| `severity_native` | `severity_norm` | Basis | Rows |
| --- | --- | --- | --- |
| *absent* | Info | `no_vocabulary` | 1 |

The absence is **stated, not filled in**: `null` in `findings.json`, an empty
field in `findings.csv`, and the basis `no_vocabulary` recording that the `Info`
band came from policy rather than from anything the tool said. No level was
assumed for this tool. Unmapped literals: **none** — there was no literal to map.

Comparability: **comparable** — the observed rule-set identity is the expected
one, the default set built into the pinned version.

## checkov

`scanner_class` **misconfig**. **`severity` is null per row in the unlicensed
configuration.** The member is present on every one of the six failed checks and
its value is `null` on all six; that is the **observed state of the field**, not a
measurement this run failed to take. 6 rows.

| `severity_native` | `severity_norm` | Basis | Rows |
| --- | --- | --- | --- |
| *absent* | Info | `no_vocabulary` | 6 |

Because the value is null rather than a literal, these rows take the
no-vocabulary path: absence stated, `Info` from policy, basis `no_vocabulary`. Had
a licensed configuration supplied a label, Table 2 would have governed it — that
is the policy, not an observation. Unmapped literals: **none**.

Comparability: **comparable** — the policy identity is the bundled set of the
expected version.

## osv-scanner

`scanner_class` **vuln**. **Zero rows, and no observed literal: this tool wrote no
artifact.** It holds an entry here for exactly that reason — the row-only dataset
files cannot represent it, so the inventory has to carry it.

| `severity_native` | `severity_norm` | Basis | Rows |
| --- | --- | --- | --- |
| *no literal observed* | — | — | 0 |

`oss-scan-results/tool-status.md` owns this tool's status and states it in the
tool's own words: exit 128 with `No package sources found` after `0 Extract
calls`, classified as the tool having completed with nothing in scope to work on
rather than as a failure, with the reconciliation recorded as `not applicable —
artifact absent` and not as a zero-equals-zero pass.

**No mapping decision was exercised for this tool in this run.** Its ecosystem
labels would have been governed by Table 2 and a CVSS entry by Table 3, under the
precedence of Part 1 — that is the policy that was in force, and no literal
arrived to be mapped under it. Unmapped literals: **none**.

Comparability: **comparable on identity** — the observed feed identity is the
expected one, no local database with the OSV API queried at scan time. There is
no count to compare. `oss-scan-results/tool-status.md` records the named
reproducibility gap that follows from having no on-disk provenance for that data;
it is that document's field, cited here rather than restated.

## dependency-check

`scanner_class` **vuln**. **Zero rows, and no observed literal — on a different
ground: its artifact is present and carries zero finding records.** That is the
second of the two ways a tool reaches zero rows, and the distinction is why both
entries are stated rather than collapsed into one. The count unit is
`dependencies[].vulnerabilities[]`, and
across the 32 dependency records the artifact analysed there is not one
vulnerability member; the string `severity` occurs nowhere in the artifact.

| `severity_native` | `severity_norm` | Basis | Rows |
| --- | --- | --- | --- |
| *no literal observed* | — | — | 0 |

The parse status is `clean` and the per-artifact reconciliation is a real
`0 = 0 + 0` with the artifact present. `oss-scan-results/tool-status.md` owns
that status and the per-record accounting behind it.

**Comparability: NOT COMPARABLE WITH THE REHEARSAL.** The feed identity differs
from the expected identity — observed keyless NIST NVD JSON 2.0 datafeed,
`NVD API Last Modified 2026-08-24T08:00:04-04`, against an expected
`2026-08-23T08:00:06-04`, one day apart — and a different feed resolves a
different advisory set, so this tool's count is not comparable with the
rehearsal's for reasons that have nothing to do with the code. The status applies
to a count of zero exactly as it would to a non-zero one: what is not comparable
is the figure, whatever it happens to be. This is the same determination
`oss-scan-results/tool-status.md` records for this tool.

Unmapped literals: **none**.

## trivy

`scanner_class` **per record — `vuln`, `secret` or `misconfig`**. This is the one
identifier the class table does not fix to a single class, which is why the
literals below are attributed to the section they were read from. 3 rows.

| Section read from | `severity_native` | `severity_norm` | Basis | `scanner_class` | Rows |
| --- | --- | --- | --- | --- | --- |
| `Results[].Misconfigurations[]` | `LOW` | Low | `label` | misconfig | 3 |

Section accounting in the artifact: **Vulnerabilities 0, Secrets 0,
Misconfigurations 3**. All three sit in `Results` members whose `Class` is
`config` and whose `Type` is `dockerfile`. So every literal this tool contributed
came from the misconfiguration section, and no literal was read from the
vulnerability or secret sections in this run — there were none to read.
Unmapped literals: **none**.

**Comparability: NOT COMPARABLE WITH THE REHEARSAL.** The feed identity differs
from the expected identity — observed vulnerability DB v2
`UpdatedAt=2026-08-24T06:55:32.451220873Z` and java DB v1
`UpdatedAt=2026-08-24T01:07:04.599776272Z`, against an expected
`2026-08-23T06:56:50Z` and `2026-08-23T01:05:59Z`, both one day later, with both
database versions matching — and a feed one day newer resolves a different
advisory set. This is the same determination
`oss-scan-results/tool-status.md` records for this tool.

## Every observed literal, in one table

The complete set, for a reader who needs it in one place. Fifteen
`(tool, literal, band, basis)` entries over 9,433 rows, plus the two tools that
contributed none.

| tool | `severity_native` | `severity_norm` | Basis | Rows | Unmapped |
| --- | --- | --- | --- | --- | --- |
| `opengrep` | `error` | High | `sarif_level` | 314 | no |
| `opengrep` | `warning` | Medium | `sarif_level` | 731 | no |
| `opengrep` | `note` | Low | `sarif_level` | 277 | no |
| `semgrep` | `error` | High | `sarif_level` | 278 | no |
| `semgrep` | `warning` | Medium | `sarif_level` | 675 | no |
| `semgrep` | `note` | Low | `sarif_level` | 209 | no |
| `datadog-static-analyzer` | `error` | High | `sarif_level` | 195 | no |
| `datadog-static-analyzer` | `warning` | Medium | `sarif_level` | 1,342 | no |
| `datadog-static-analyzer` | `note` | Low | `sarif_level` | 5,275 | no |
| `datadog-static-analyzer` | `none` | Info | `sarif_level` | 20 | no |
| `joern` | `HIGH` | High | `label` | 63 | no |
| `joern` | `MEDIUM` | Medium | `label` | 44 | no |
| `gitleaks` | *absent* | Info | `no_vocabulary` | 1 | no |
| `checkov` | *absent* | Info | `no_vocabulary` | 6 | no |
| `trivy` | `LOW` | Low | `label` | 3 | no |
| `osv-scanner` | *no literal observed* | — | — | 0 | — |
| `dependency-check` | *no literal observed* | — | — | 0 | — |

Row total over the fifteen literal entries: **9,433**, which is the row count of
`findings.json` and of `findings.csv`.

## Unmapped literals

**None, for any of the nine tools.** Every literal that arrived fell inside a
mapped vocabulary, so the `unmapped_literal` basis was recorded zero times and
there is no literal to list with the rows it affected. The disclosure path is
policy that was in force and was not exercised, not a check that was skipped: the
committed adapter tests exercise it directly, including a CVSS **vector** string,
which the policy excludes from the numeric route by an explicit shape test so a
vector can never be read as a score, and which is therefore disclosed as an
unmapped literal with its rows rather than banded numerically.

## Literals exercised by a committed fixture rather than observed in this dataset

Named separately, and deliberately outside every table above, because a literal
that is not in the dataset must not be listed as though it were: every literal in
this document's observed-literal tables corresponds to a literal present in
`findings.json` and `findings.csv`, and this section is what keeps that true while
still recording the hazards the policy was written for — the two measured raw
floats, the lower-case label, and the score selection that decides a band when a
record carries several scores.

`dependency-check` contributed **zero rows** to this dataset, so it has **no
observed native literal** — the entry above states that, and no figure is
attributed to it here. The literals below are in the committed fixture
`oss-scan-results/adapter-tests/fixtures/dependency-check.json`, and they are
what exercises this adapter's numeric-banding and case-folding paths in
`oss-scan-results/adapter-tests/test_dependency_check_adapter.py`:

Each band and each rendered text below is the expectation asserted in
`oss-scan-results/adapter-tests/expected/dependency-check.rows.json`, read from
that file rather than restated from the policy:

| Fixture literal | Kind | `severity_norm` | Basis | Reaches `severity_native` as |
| --- | --- | --- | --- | --- |
| `3.200000047683716` | raw float in the `severity` member, carrying a float32-to-float64 representation tail | Low | `cvss_score` | `3.2` |
| `5.300000190734863` | raw float in the `severity` member, carrying the same kind of tail | Medium | `cvss_score` | `5.3` |
| `moderate` | lower-case label, absorbed by the case-insensitive Table 2 | Medium | `label` | `moderate`, as observed |
| `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` | upper-case labels | Low, Medium, High, Critical | `label` | as observed |
| `severity` null, with two CVSS entries on the record | no mapped label, so a score is consulted — step 3 of the evaluation order | High | `cvss_score` | `7.5` |

Two points about the two floats, which are the reason this section exists at all.
**The banding was numeric** — an ordered comparison on the full-precision float,
which is what puts `3.200000047683716` in Low and `5.300000190734863` in Medium,
and which a lexical test would get wrong. And **no spurious precision is rendered
into a text field**: the values are quoted here as measured inputs, and what
reaches `severity_native` is the one-decimal rendering `3.2` and `5.3`, with the
full-precision value retained in the selected score entry. The test module
asserts both halves — the band from the float and the exact text of the field —
and the suite result is in `harness/artifacts/logs/adapter-tests-run.json`.

The lower-case `moderate` is the measured case that makes case folding in Table 2
load-bearing rather than cosmetic: an upper-case-only table would map `MODERATE`
and miss it.

The last row is where **recording the entry that was used** stops being an
abstraction. That record's `severity` member is null, so no mapped label exists
and the score path is reached — and the record carries **two** score entries: a
CVSS **v2.0** score of **5.0** and a CVSS **v3.1** base score of **7.5**. The
selection order takes the highest CVSS version first, so the v3.1 entry governs,
`7.5` bands High, and the entry recorded is that score with its source and its
version. Had only the band been recorded, a reader could not tell which of the
two scores produced it, and the two produce different bands.

Nothing in this section is a dataset count. Every figure here is a property of a
committed fixture and of the adapter that reads it.

---

# Comparability with the rehearsal

Where an observed ruleset digest or feed timestamp differed from the expected
identity, that tool's counts are marked **not comparable with the rehearsal** —
because a different rule set or a different feed produces a different count for
reasons that have nothing to do with the code, and recording the difference
without the status would leave a reader comparing two numbers that were never
comparable.

Each verdict below is **one determination**, made where the identities were
observed and recorded in `oss-scan-results/tool-status.md`, and cited here. It is
not a second determination made in this document.

| tool | Comparability | Ground |
| --- | --- | --- |
| `opengrep` | comparable | Ruleset commit `f1d2b562b414783763fd02a6ed2736eaed622efa` observed and expected; 2,006 rules against 2,006 |
| `semgrep` | comparable | Ruleset commit `40b8c63f75dc7c22c8a77482d73bfb864b146f7e` observed and expected; 2,149 rules against 2,149, 19 Pro-only skipped |
| `datadog-static-analyzer` | **NOT COMPARABLE** | Ruleset sha256 observed `4f397e81…b8fef6f1`, expected `e70ede30…ff6c44f7` — differs; 48 rulesets and 1,093 rules do match |
| `joern` | comparable | Query-set identity observed as the expected one: 6 bounded structural queries baked into the runner |
| `gitleaks` | comparable | The default rule set built into the pinned version, observed and expected |
| `checkov` | comparable | Policies bundled with the expected version, observed and expected |
| `osv-scanner` | comparable on identity | No local database, the OSV API queried at scan time, observed and expected; no count to compare |
| `dependency-check` | **NOT COMPARABLE** | Keyless NVD JSON 2.0 feed, `NVD API Last Modified` observed `2026-08-24T08:00:04-04`, expected `2026-08-23T08:00:06-04` — one day apart |
| `trivy` | **NOT COMPARABLE** | Vulnerability DB v2 observed `2026-08-24T06:55:32.451220873Z` against expected `2026-08-23T06:56:50Z`, java DB v1 observed `2026-08-24T01:07:04.599776272Z` against expected `2026-08-23T01:05:59Z` — both one day later; both DB versions match |

The two digests in the `datadog-static-analyzer` row are abbreviated for the
table's width only; both full values appear in that tool's entry in Part 2 and in
`oss-scan-results/tool-status.md`.

# Values that could not be established

Named rather than omitted, because a value missing from the record is a value
nothing downstream can check.

| Value | Scope | Why |
| --- | --- | --- |
| The native severity vocabulary `osv-scanner` would have used | `osv-scanner` | It wrote no artifact, so no record arrived and no mapping decision was exercised. Table 2 for an ecosystem label and Table 3 for a CVSS entry were the policy in force; which of them a record would have taken is not established by this run, and no vocabulary was attributed to the tool on the strength of what it usually emits |
| The native severity literals `dependency-check` emits | `dependency-check` | Its artifact carries zero finding records, so no literal was observed. The adapter's handling of this shape's label and score paths is established by the committed fixture named in Part 2, which is a statement about the adapter and not a dataset count |
| Behaviour of the `cvss_score` basis on this run's own artifacts | dataset-wide | No artifact exercised it: the basis was recorded 0 times. It is established against the committed fixtures instead — three of the fixture records in the section above take that basis, one of them through the two-candidate selection — and is stated as such rather than left to look like a path nobody tested |
| Behaviour of the `unmapped_literal` disclosure on this run's own artifacts | dataset-wide | Likewise recorded 0 times. Every literal that arrived was inside a mapped vocabulary |

`oss-scan-results/tool-status.md` carries the values that could not be
established outside this document's subject — among them the `gitleaks` rule count
and the `checkov` policy count, neither separately versioned and neither reported
by its tool. None was invented there and none is invented here.

# Cross-references that must hold in both directions

Each of these was checked, and each is one measurement appearing twice rather
than two measurements agreeing.

1. **Every literal listed here is present in the dataset.** The fifteen
   `(tool, literal, band)` entries were recounted directly from
   `findings.json` and independently from `findings.csv`; both produce the same
   fifteen entries with the same row counts as the tally rendered here.
2. **Every `severity_native` literal in the dataset appears here.** The recount
   yields no sixteenth entry: the distinct literals in the dataset are `error`,
   `warning`, `note`, `none`, `HIGH`, `MEDIUM`, `LOW` and the absent literal, and
   each appears above against the tool that produced it.
3. **Per-tool row counts agree with `oss-scan-results/tool-status.md`** and with
   `normalize-run.json` `totals.rows_by_tool`, in the normalizer's processing
   order: `opengrep` 1,322, `semgrep` 1,162, `datadog-static-analyzer` 6,832,
   `gitleaks` 1, `checkov` 6, `trivy` 3, `osv-scanner` 0, `dependency-check` 0 and
   `joern` 107, summing to 9,433.
4. **`findings.json` and `findings.csv` agree** on every severity field, row for
   row: absence is `null` in the one and an empty field in the other, over the
   same 9,433 rows, with the typed comparison in `normalize-run.json`
   `output_comparison` reporting 9,433 rows and 113,196 fields compared and no
   first mismatch.
5. **The comparability verdicts match `oss-scan-results/tool-status.md`**
   tool for tool: three not comparable — `datadog-static-analyzer`, `trivy`,
   `dependency-check` — and six comparable.
6. **`severity_norm` is absent nowhere**, and `severity_native` is absent on
   exactly the 7 no-vocabulary rows.

# What this document does not do

- It draws **no comparison between tools**. It does not rank them, contrast their
  severity vocabularies, explain why one reported something another did not, or
  characterise any tool's vocabulary as better, stricter or more accurate. Two
  tools' labels arriving in the same band is a mapping statement about this
  dataset's schema and nothing else.
- It **judges no finding**. Nothing here is called real, important, a false
  positive or a duplicate of another tool's, and **nothing is deduplicated across
  tools**: two tools reporting the same location produce two rows and no comment.
- It makes **no comparison against any commercial or third-party scanner**, no
  such data being part of this run.
- It reads **no count as a verdict on the code**. A band total, an unrepresented
  band and a zero-row tool are counts of this dataset under the policy in Part 1.
- It carries **no credential value and no secret value**. The `gitleaks` entry
  names that tool's record members to show that none of them is a severity; no
  member's value appears here, and no adapter carries a secret value into any
  field of the dataset.
- It **owns** the severity mapping and the observed literals, and with
  `oss-scan-results/tool-status.md` the nine-tool inventory. It does **not** own
  the per-tool status contract (`oss-scan-results/tool-status.md`), the
  per-project build and graph coverage verdicts
  (`oss-scan-results/build-record.md`), the capability probe
  (`oss-scan-results/joern-probe.md`), or the run-wide index and the artifact-tree
  manifests (`oss-scan-results/run-record.md`). Where one of those documents
  carries a figure that appears here, it is this measurement cited again.
- It **recounts nothing**. The tally is rendered from
  `harness/lib/normalize/severity.py`'s accumulator as written into
  `harness/artifacts/logs/normalize-run.json`; the cross-checks in the section
  above verify that rendering against the dataset rather than replacing it with a
  second figure.
