# Severity map — the mapping as policy, and the literals observed

Two things live in this document, and the boundary between them is deliberate.

**Part 1 is the mapping as fixed policy.** Every table, boundary and precedence
rule below was decided in advance, from the plan, and none of it was fitted to
what the artifacts turned out to contain. That is the whole point of stating it
separately: a mapping authored after the output is read can be arranged to
produce a distribution somebody wanted, and one authored before it cannot.

**Part 2 is what the artifacts actually contained** — every native severity
literal observed, per tool, with the row count it affected and the entry that
governed its band.

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
| `harness/lib/normalize/adapters/sarif.py` | Which SARIF fields may state a level, and which field is deliberately not consulted |
| `harness/artifacts/logs/normalize-run.json` | `severity_literals` — the tally itself: per tool, every observed literal with its band, its basis, the **selected entry** decomposed into its four scalar parts, and its row count, plus the per-tool and dataset row totals; and each artifact's `counters`, which record which field source stated each severity |
| `oss-scan-results/findings.json`, `oss-scan-results/findings.csv` | The dataset the literals were observed in; the row counts here are the counts in those two files |
| `harness/artifacts/logs/gate-record.json`, `harness/artifacts/logs/datadog-sast-rules.captured.meta.json` | Each tool's observed ruleset or feed identity beside the identity AAP 0.4.2 expects — the readings the comparability verdicts rest on |
| `harness/artifacts/logs/cpg-identity.txt` | The identity of the graph `joern`'s literals were observed over, cited rather than restated |
| `oss-scan-results/tool-status.md` | The two zero-row tools' status, and the per-tool status contract each comparability verdict is recorded against |

The tally is **rendered, not recounted**. `harness/lib/normalize/severity.py`
accumulated it while the rows were built, `harness/lib/normalize/cli.py` wrote it
into `harness/artifacts/logs/normalize-run.json` under `severity_literals`, and
Part 2 is that readout. The independent traversal that reconciles record counts
belongs to `harness/lib/normalize/reconcile.py` and its results belong to
`oss-scan-results/tool-status.md`; this document does not restate them.

## Which generation this document renders

A severity distribution is only interpretable against the dataset it was measured
over, so the generation is named rather than implied. Every figure in Part 2 is a
figure of **this** generation and of no earlier one:

| Field | Value |
| --- | --- |
| Publication identifier | `1cbb9d77d199323c62d9da1354f6583b` (`normalize-run.json` `outputs.publication.publication_id`) |
| `findings.json` | 4,408,640 bytes, sha256 `d4e28c823fd1e76c2158130dc941762e0c6cf23424c0c990c930cc84ece6fc54` |
| `findings.csv` | 2,081,618 bytes, sha256 `9f646532494fcba3ad95a8e10f15f77957b9f16bea0b486b513e2a830f5445e6` |
| Rows | 9,430 |
| Normalizer invocation | started 2026-09-02T22:56:48Z, finished 2026-09-02T22:56:54Z, exit 0 (`normalize-run.json` `started_at_utc`, `finished_at_utc`, `exit_status`) |

The publication identifier is **content-derived** — a sha256 over the scheme name
followed by one `<role>\t<sha256>` line per member in ascending role order,
truncated to 32 hex characters — so a reader holding the two dataset files can
recompute it, and a mixed generation yields an identifier matching neither
recorded value. That is what makes it usable as the anchor: this document's
figures belong to the publication above, and any figure of a superseded
generation is absent from it rather than reconciled against it.

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
**0** (`harness/artifacts/logs/normalize-run.json`
`outputs.row_validation`).

## Table 1 — SARIF `level`

The lookup is case-insensitive; `severity_native` records the literal as
observed.

| SARIF `level` | `severity_norm` |
| --- | --- |
| `error` | High |
| `warning` | Medium |
| `note` | Low |
| `none` | Info |

## Which SARIF fields may state a severity, and which may not

Table 1 says how a level is banded. This says **where a level may be read from**,
which is a separate decision and the one that moved rows in this run.

AAP 0.5.4's per-shape table enumerates the field sources for a shared-SARIF
record as *"`severity_native` ← `level`, or the rule's
`properties.severity`/`problem.severity` where `level` is absent"*. That is the
whole of the authorisation, and `harness/lib/normalize/adapters/sarif.py`
consults exactly those three, in this fixed order, the first present literal
winning outright:

| Order | Field | Vocabulary it maps through | Basis recorded |
| --- | --- | --- | --- |
| 1 | `result.level` — the level the producer stated on this very result | Table 1 | `sarif_level` |
| 2 | `rule.properties.severity` | Table 2 | `label` |
| 3 | `rule.properties.problem.severity` | Table 2 | `label` |

**`rule.defaultConfiguration.level` is not an authorised source and is not
consulted.** The field exists, SARIF 2.1.0 does describe deriving an omitted
`result.level` through it, and mainstream consumers do implement that
derivation — and none of that is an authorisation here, because AAP 0.5.4
enumerates the field sources for this shape and does not carry it. Where the
specification and the AAP describe different behaviour, the AAP is what this
pipeline is held to. Two further surfaces are excluded for the same reason and
are named so the omissions are visible rather than silent: the specification's
terminal `warning` default for a result whose level cannot be derived at all,
which would manufacture a Medium band for a record nothing in the artifact
assigns a severity to; and `run.policies` /
`invocation.ruleConfigurationOverrides`, the run-time override surfaces that can
outrank a rule's configuration.

An earlier source outranks a later one **even when its literal is unmappable**: a
`level` outside the SARIF vocabulary is disclosed as an unmapped literal rather
than quietly replaced by a rule property, because reaching past a literal the
producer did state would be inference.

The measured consequence for this run's three SARIF producers is in Part 2 under
*Which SARIF field stated each severity, measured*. It is not symmetric across
the three, and the asymmetry is a property of what each producer emits rather
than of the policy.

## Table 2 — label vocabularies

Applies to Trivy, Checkov, Dependency-Check, OSV ecosystem labels and a SARIF
rule property. The lookup strips surrounding whitespace and upper-cases the
literal; nothing else is done to it, and the observed spelling is what reaches
`severity_native`. Case folding is load-bearing rather than cosmetic: a real
artifact can carry both `MODERATE` and lower-case `moderate` for the same band,
and a case-sensitive table would map one and miss the other.

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

Three shapes take this path, and Part 2 names which tools took it on which
ground:

- a record shape that defines no severity member at all;
- a record whose severity member is present and null;
- a record none of whose **authorised** field sources states a literal.

The third is not a weaker case than the other two. It is the same outcome — a
severity this pipeline is not authorised to read is a severity it does not have —
and stating the absence is what keeps it distinguishable from a level that was
assumed.

**The policy names two tools at this row in advance**, exactly as AAP 0.5.4 fixes
it: `gitleaks`, which defines no severity vocabulary at all, and `joern`, which
defines none *unless a query defines one*. The second is conditional by design, so
which way it fell is an observation rather than a policy statement — and in this
generation `joern`'s baked queries **do** define one, `HIGH` or `MEDIUM` per
query, so its rows take the label path of Table 2 and not this one. Part 2's
`joern` entry states that with the declaration it rests on.

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

Every row records **what was used**, not merely the band it produced, and the
entry is carried through aggregation rather than discarded at it. The tally's
bucket key is the observed literal, the band, the basis **and the selected entry
decomposed into four scalar fields** — `selected_label`, `selected_score`,
`selected_source`, `selected_version` — each carried in its own field rather than
as one rendered string. The policy as written into the tally, quoted verbatim
from `harness/artifacts/logs/normalize-run.json`
`severity_literals.selected_entry_policy`:

> The entry that governed a band is part of a tallied literal's identity, not a
> detail discarded when rows are aggregated. A bucket is keyed on the observed
> literal, the band, the basis and the selected entry decomposed into its four
> scalar parts -- the label, and a score's value, source and version -- each
> carried as its own field rather than as one rendered string. So two advisories
> scored 7.5 by different sources, or under different CVSS versions, are reported
> as two entries naming their sources rather than as one entry that names neither.

Two shapes of selected entry occur, and no third:

- a **label** entry — `selected_label` carries the literal as observed, and the
  three score fields are absent. This is the shape for a mapped label and for a
  SARIF `level`.
- a **score** entry — `selected_score` carries the full-precision float,
  `selected_source` the source it came from and `selected_version` the CVSS
  version it declared; `selected_label` is absent.

Recording the score's **source and version** is not decoration. An advisory
commonly carries several scores from different providers and different CVSS
versions, so *which* entry governed has to be recorded rather than implied.
Where more than one candidate is bandable, selection is by a documented total
order — highest CVSS version, then highest score, then lexicographically smallest
source, then earliest position in the supplied sequence — so two runs over the
same input cannot select differently. The worked case is in Part 2 under
*Literals exercised by a committed fixture*, where one record carries a CVSS
v2.0 score of 5.0 and a CVSS v3.1 base score of 7.5 and the recorded entry names
the source `NVD:cvssv3` and the version `3.1` alongside the score that governed.

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
`oss-scan-results/findings.json` and `oss-scan-results/findings.csv`, **9,430
rows** — the publication named above — whose per-tool row counts are the same
measurement `oss-scan-results/tool-status.md` carries in its inventory table.

## How the bands were arrived at, across the dataset

The basis is recorded per row, so the paths of Part 1 can be counted rather than
assumed:

| Basis | Rows |
| --- | --- |
| `sarif_level` (Table 1) | 6,832 |
| `label` (Table 2) | 110 |
| `no_vocabulary` (policy) | 2,488 |
| `cvss_score` (Table 3) | **0** |
| `unmapped_literal` (policy) | **0** |
| Total | 9,430 |

Two of those zeros are statements about this run rather than about the policy.
**No row in this dataset was banded from a CVSS numeric score**, so no score
entry — no `selected_score`, no `selected_source`, no `selected_version` — was
recorded for any row: the requirement that the selected entry be recorded
wherever a score governed is satisfied over an empty set, and Table 3 was
exercised here only by the committed adapter fixtures named at the end of this
part. **No unmapped literal was observed**, so the disclosure list below is empty
for every one of the nine tools; every literal that arrived fell inside a mapped
vocabulary.

`severity_native` is absent on exactly **2,488** rows, which is the
`no_vocabulary` count above and matches
`normalize-run.json` `outputs.row_validation.absence_by_optional_field.severity_native`.

Band totals across the dataset, which are the same 9,430 rows grouped the other
way:

| `severity_norm` | Rows |
| --- | --- |
| Critical | 0 |
| High | 258 |
| Medium | 1,386 |
| Low | 5,278 |
| Info | 2,508 |
| Total | 9,430 |

The `Critical` band is unrepresented: no row of any tool took it. That is a
count of this dataset and nothing more — it is not read as a statement about any
tool, any rule set or the code that was scanned.

## Which SARIF field stated each severity, measured

Where a SARIF row's band came from is a **field-source** question rather than a
mapping question, and it is the one that decides the distribution for this
family, so it is measured per tool rather than assumed uniform. Each artifact's
`counters` in `harness/artifacts/logs/normalize-run.json` record which of the
three authorised sources stated each severity:

| tool | `severity_from_level` | `severity_from_rule_property` | `severity_absent` | Consequence |
| --- | ---: | ---: | ---: | --- |
| `opengrep` | 0 | 0 | 1,319 | every row's `severity_native` absent, `severity_norm` `Info`, basis `no_vocabulary` |
| `semgrep` | 0 | 0 | 1,162 | the same, over 1,162 rows |
| `datadog-static-analyzer` | 6,832 | 0 | 0 | every row's band comes from the `level` the producer stated on the result itself |

**The three SARIF producers are not alike, and the asymmetry is the point.**
`datadog-static-analyzer` states a `level` on every one of its 6,832 results —
`harness/artifacts/raw/datadog-static-analyzer.sarif` carries 6,832 `"level"`
keys over 1,147 rule descriptors and **no** `defaultConfiguration` object at all
— so its severities come from the AAP's *first* authorised source. `opengrep` and
`semgrep` state a level nowhere the AAP authorises: their results carry no
`level` and their rules carry no `properties.severity` and no
`properties.problem.severity`, and what those two artifacts do say about
severity sits in the rule descriptors' `defaultConfiguration` objects — 2,002 of
them in `harness/artifacts/raw/opengrep.sarif` and 2,126 in
`harness/artifacts/raw/semgrep.sarif`, one on every rule either artifact
declares. **No reader should conclude from the two `Info`-only entries below that
the SARIF family lost severity wholesale**: 2,481 rows carry the absence because
the field that would have banded them is not a field this pipeline is authorised
to read, and the absence is stated rather than filled from it.

**An earlier publication of this document reported a High/Medium/Low
distribution for those two tools**, because the shared SARIF adapter then read
`rule.defaultConfiguration.level` as a fourth field source. AAP 0.5.4 does not
authorise it, it is no longer consulted, and that generation is superseded. Its
figures are **not restated here**, in either direction: they were produced by
reading an unauthorised field over a different dataset, so they are not a second
measurement of anything in this publication, and the only honest thing this
document can do with them is name the reason they are gone. Nothing in Part 1
changed with them — not one table, boundary or precedence rule.

## opengrep

`scanner_class` **sast**. **No authorised field source states a severity in this
artifact**, so this tool contributes no native literal. 1,319 rows.

| `severity_native` | `severity_norm` | Basis | Selected entry | Rows |
| --- | --- | --- | --- | --- |
| *absent* | Info | `no_vocabulary` | *none — no entry governed* | 1,319 |

The absence is **stated, not filled in**: `null` in `findings.json`, an empty
field in `findings.csv`, and the basis `no_vocabulary` recording that the `Info`
band came from policy rather than from anything this pipeline was authorised to
read. No level was assumed. Unmapped literals: **none** — there was no literal to
map, so nothing survived to be disclosed.

Where this artifact's own severity statements sit, and why they are not read, is
in *Which SARIF field stated each severity, measured* above. Comparability:
**comparable** — the observed ruleset identity is the expected identity, commit
`f1d2b562b414783763fd02a6ed2736eaed622efa` over 2,006 rules.

## semgrep

`scanner_class` **sast**. The same ground as `opengrep`, on the same measurement:
no `level` on any result and no authorised rule property. 1,162 rows.

| `severity_native` | `severity_norm` | Basis | Selected entry | Rows |
| --- | --- | --- | --- | --- |
| *absent* | Info | `no_vocabulary` | *none — no entry governed* | 1,162 |

Unmapped literals: **none**. Comparability: **comparable** — the observed ruleset
identity is the expected identity, commit
`40b8c63f75dc7c22c8a77482d73bfb864b146f7e` over 2,149 rules with 19 Pro-only
skipped.

## datadog-static-analyzer

`scanner_class` **sast**. Vocabulary: **SARIF `level`** (Table 1), read from the
`level` each result states — the AAP's first authorised field source, stated on
every one of this artifact's results. 6,832 rows.

| `severity_native` | `severity_norm` | Basis | Selected entry | Rows |
| --- | --- | --- | --- | --- |
| `error` | High | `sarif_level` | label `error` | 195 |
| `warning` | Medium | `sarif_level` | label `warning` | 1,342 |
| `note` | Low | `sarif_level` | label `note` | 5,275 |
| `none` | Info | `sarif_level` | label `none` | 20 |

Unmapped literals: **none**. The 20 rows carrying the SARIF level `none` reached
the `Info` band through a mapped vocabulary rather than through the no-vocabulary
path; the recorded basis `sarif_level` is what distinguishes them from an `Info`
row whose basis is `no_vocabulary`.

**Comparability: NOT COMPARABLE WITH THE REHEARSAL.** The ruleset identity
differs from the expected identity on all three of its fields — observed sha256
`c5fd464c2985119574f23599d44022e22b9442d7083acb17ec84addba354f322` over **53
rulesets and 1,147 rules**, against the AAP 0.4.2 expected sha256
`e70ede308813b6d8c4087b0995609cdafdb9ab48159a313fe58ac343ff6c44f7` over 48
rulesets and 1,093 rules. A different rule set produces a different count for
reasons that have nothing to do with the code, so the per-literal counts above
and this tool's finding count **must not be read against the rehearsal's
figures**. The observed identity is the reading the gate recorded
(`harness/artifacts/logs/gate-record.json`, and the captured rules file's own
metadata at `harness/artifacts/logs/datadog-sast-rules.captured.meta.json`); the
1,147 rules it declares are the 1,147 rule descriptors the artifact itself
carries. `oss-scan-results/tool-status.md` owns this tool's status contract and
records the same determination; it is cited here rather than made again.

## joern

`scanner_class` **sast**. Vocabulary: **label** (Table 2). 107 rows.

| `severity_native` | `severity_norm` | Basis | Selected entry | Rows |
| --- | --- | --- | --- | --- |
| `HIGH` | High | `label` | label `HIGH` | 63 |
| `MEDIUM` | Medium | `label` | label `MEDIUM` | 44 |

Unmapped literals: **none**.

**A query does define a severity here, so these rows take the label path rather
than the no-vocabulary path.** The policy anticipates both cases for this tool —
no vocabulary *unless a query defines one* — and **in this provisioning the baked
query set does define one per query**, declared in `harness/lib/joern-scan.sc` at
lines 48 to 78, where the query record type at line 48 carries a `severity`
member and each of the six entries at lines 51, 55, 59, 63, 67 and 71 supplies
it:

| Query identifier | Declared severity | Finding records carrying it |
| --- | --- | ---: |
| `joern-process-exec` | `HIGH` | 55 |
| `joern-unsafe-deserialization` | `HIGH` | 178 |
| `joern-reflection-forname` | `MEDIUM` | 413 |
| `joern-message-digest` | `MEDIUM` | 23 |
| `joern-cipher-getinstance` | `MEDIUM` | 11 |
| `joern-xml-factory` | `MEDIUM` | 13 |

Every one of the artifact's **693** finding records carries the declared severity
of the query that produced it, giving `HIGH` 233 and `MEDIUM` 460 **in the
artifact** (`harness/artifacts/raw/joern.json`). The row counts in the table
above are **63 and 44**, over the 107 rows this tool contributed to the dataset:
this artifact's parse status is `partial` and 586 of its records were rejected
under the single class `unresolvable_path`, so a rejected record contributes no
row and therefore no literal. The artifact-side and dataset-side figures are
different measurements of different things and are not reconciled against each
other here; the record-level identity `693 = 107 + 586` belongs to
`oss-scan-results/tool-status.md`, which owns it.

**These are counts over a graph rather than over the source tree**, which is what
separates this tool from the other eight, and the graph is identified once
elsewhere rather than described again here. Its identity — 541,309,809 bytes,
sha256 `4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7` — is
owned by `harness/artifacts/logs/cpg-identity.txt`, the one record of account for
it, which also records that those bytes were **written by provisioning and not by
this run**, over an input set narrower than every JAR the reactor produced; how
that graph was built, what it covers per module, and every divergence that
follows are owned by `oss-scan-results/build-record.md` and carried in
`oss-scan-results/run-record.md`. This document neither restates nor recomputes
any of it: the only thing the mapping needs from the graph is that the literals
above were observed over it, and nothing about the mapping policy depends on it —
the label path and the boundaries were fixed in advance of any output.

Comparability: **comparable** on query-set identity — the observed query set is
the expected one, the six bounded structural queries baked into the runner. This
tool's **counts**, however, are counts over one specific graph, and the graph
loaded here is not the graph the inherited environment record describes
(`harness/artifacts/logs/gate-record.json` records both identities), so they must
not be read against the previous provisioning's figures.
`oss-scan-results/tool-status.md` and `oss-scan-results/run-record.md` own that
determination and state both values.

## gitleaks

`scanner_class` **secret**. **This tool defines no severity vocabulary.** Its
record shape carries no severity member at all — the members are `Author`,
`Commit`, `Date`, `Description`, `Email`, `EndColumn`, `EndLine`, `Entropy`,
`File`, `Fingerprint`, `Match`, `Message`, `RuleID`, `Secret`, `StartColumn`,
`StartLine`, `SymlinkFile` and `Tags`, and none of them is a severity. 1 row.

| `severity_native` | `severity_norm` | Basis | Selected entry | Rows |
| --- | --- | --- | --- | --- |
| *absent* | Info | `no_vocabulary` | *none — no entry governed* | 1 |

The absence is **stated, not filled in**, and no level was assumed for this tool.
Unmapped literals: **none** — there was no literal to map.

Comparability: **comparable** — the observed rule-set identity is the expected
one: the default set built into Gitleaks 8.30.1, which the tool does not version
separately, over an observed version equal to the expected 8.30.1.

## checkov

`scanner_class` **misconfig**. **`severity` is null per row in the unlicensed
configuration.** The member is present on every one of the six failed checks and
its value is `null` on all six; that is the **observed state of the field**, not a
measurement this run failed to take. 6 rows.

| `severity_native` | `severity_norm` | Basis | Selected entry | Rows |
| --- | --- | --- | --- | --- |
| *absent* | Info | `no_vocabulary` | *none — no entry governed* | 6 |

Because the value is null rather than a literal, these rows take the
no-vocabulary path: absence stated, `Info` from policy, basis `no_vocabulary`. Had
a licensed configuration supplied a label, Table 2 would have governed it — that
is the policy, not an observation. Unmapped literals: **none**.

Comparability: **comparable** — the policy identity is the bundled set of the
expected version: the policies shipped with Checkov 3.3.12, which are not
versioned separately, over an observed version equal to the expected 3.3.12.

## osv-scanner

`scanner_class` **vuln**. **Zero rows, and no observed literal: this tool wrote no
artifact.** It holds an entry here for exactly that reason — the row-only dataset
files cannot represent it, so the inventory has to carry it.

| `severity_native` | `severity_norm` | Basis | Selected entry | Rows |
| --- | --- | --- | --- | --- |
| *no literal observed* | — | — | — | 0 |

`oss-scan-results/tool-status.md` owns this tool's status and states it in the
tool's own words: exit 128 with `No package sources found, --help for usage
information.` after `0 Extract calls`, classified as the tool having completed
with nothing in scope to work on rather than as a failure, with the
reconciliation recorded as `not applicable — artifact absent` and not as a
zero-equals-zero pass.

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
`dependencies[].vulnerabilities[]`, and across the 32 dependency records the
artifact analysed there is not one vulnerability member; the string `severity`
occurs nowhere in the artifact.

| `severity_native` | `severity_norm` | Basis | Selected entry | Rows |
| --- | --- | --- | --- | --- |
| *no literal observed* | — | — | — | 0 |

The parse status is `clean` and the per-artifact reconciliation is a real
`0 = 0 + 0` with the artifact present. `oss-scan-results/tool-status.md` owns
that status and the per-record accounting behind it.

**Comparability: NOT COMPARABLE WITH THE REHEARSAL.** The feed identity differs
from the expected identity — observed keyless NIST NVD JSON 2.0 datafeed,
`NVD API Last Modified 2026-08-30T12:00:19-04` over a 260,005,888-byte
`odc.mv.db`, against the AAP 0.4.2 expected `2026-08-23T08:00:06-04`, seven days
apart — and a different feed resolves a different advisory set, so this tool's
count is not comparable with the rehearsal's for reasons that have nothing to do
with the code. The status applies to a count of zero exactly as it would to a
non-zero one: what is not comparable is the figure, whatever it happens to be.
The observed identity is the reading the gate recorded
(`harness/artifacts/logs/gate-record.json`), and
`oss-scan-results/tool-status.md` records the same determination for this tool.

Unmapped literals: **none**.

## trivy

`scanner_class` **per record — `vuln`, `secret` or `misconfig`**. This is the one
identifier the class table does not fix to a single class, which is why the
literals below are attributed to the section they were read from. 3 rows.

| Section read from | `severity_native` | `severity_norm` | Basis | Selected entry | `scanner_class` | Rows |
| --- | --- | --- | --- | --- | --- | --- |
| `Results[].Misconfigurations[]` | `LOW` | Low | `label` | label `LOW` | misconfig | 3 |

Section accounting in the artifact: **Vulnerabilities 0, Secrets 0,
Misconfigurations 3**, one in each of the three `Results` members, whose `Class`
is `config` and whose `Type` is `dockerfile`. So every literal this tool
contributed came from the misconfiguration section, and no literal was read from
the vulnerability or secret sections in this run — there were none to read. The
two finding sections this version can also emit and this pipeline does not
support, `Licenses` and `ExperimentalModifiedFindings`, were **validated empty**
(`normalize-run.json` `artifacts[].extras.unsupported_finding_sections_validated_empty`),
which is what establishes that no literal was silently dropped rather than
mapped. Unmapped literals: **none**.

**Comparability: NOT COMPARABLE WITH THE REHEARSAL.** The feed identity differs
from the expected identity — observed vulnerability DB v2
`UpdatedAt=2026-08-30T13:05:01.49156526Z` and java DB v1
`UpdatedAt=2026-08-30T01:07:49.364681226Z`, against the AAP 0.4.2 expected
`2026-08-23T06:56:50Z` and `2026-08-23T01:05:59Z`, both seven days later, with
both database versions matching — and a newer feed resolves a different advisory
set, so this tool's three rows and their literal must not be read against the
rehearsal's figures. The observed identity is the reading the gate recorded
(`harness/artifacts/logs/gate-record.json`), and
`oss-scan-results/tool-status.md` records the same determination for this tool.

## Every observed literal, in one table

The complete set, for a reader who needs it in one place. Eleven
`(tool, literal, band, basis, selected entry)` entries over 9,430 rows, plus the
two tools that contributed none.

| tool | `severity_native` | `severity_norm` | Basis | Selected entry | Rows | Unmapped |
| --- | --- | --- | --- | --- | --- | --- |
| `opengrep` | *absent* | Info | `no_vocabulary` | *none* | 1,319 | no |
| `semgrep` | *absent* | Info | `no_vocabulary` | *none* | 1,162 | no |
| `datadog-static-analyzer` | `error` | High | `sarif_level` | label `error` | 195 | no |
| `datadog-static-analyzer` | `warning` | Medium | `sarif_level` | label `warning` | 1,342 | no |
| `datadog-static-analyzer` | `note` | Low | `sarif_level` | label `note` | 5,275 | no |
| `datadog-static-analyzer` | `none` | Info | `sarif_level` | label `none` | 20 | no |
| `joern` | `HIGH` | High | `label` | label `HIGH` | 63 | no |
| `joern` | `MEDIUM` | Medium | `label` | label `MEDIUM` | 44 | no |
| `gitleaks` | *absent* | Info | `no_vocabulary` | *none* | 1 | no |
| `checkov` | *absent* | Info | `no_vocabulary` | *none* | 6 | no |
| `trivy` | `LOW` | Low | `label` | label `LOW` | 3 | no |
| `osv-scanner` | *no literal observed* | — | — | — | 0 | — |
| `dependency-check` | *no literal observed* | — | — | — | 0 | — |

Row total over the eleven literal entries: **9,430**, which is the row count of
`findings.json` and of `findings.csv`. Every selected entry in this run is a
**label** entry: `selected_label` carries the literal as observed and the three
score fields are absent, because no row was banded from a score.

Per tool, the same eleven entries sum to **opengrep 1,319, semgrep 1,162,
datadog-static-analyzer 6,832, gitleaks 1, checkov 6, trivy 3, joern 107**, with
**osv-scanner 0 and dependency-check 0** — nine identifiers, 9,430 rows.

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
`oss-scan-results/adapter-tests/fixtures/derived-dependency-check-features.json`,
which is where this adapter's numeric-banding, case-folding and score-selection
paths are exercised by
`oss-scan-results/adapter-tests/test_dependency_check_adapter.py`. The **captured**
fixture `oss-scan-results/adapter-tests/fixtures/dependency-check.json` is a
byte-for-byte copy of the raw artifact and therefore carries **zero rows and no
literal at all**, for the same reason the tool contributed none: the artifact
holds no vulnerability record. That is why the literals below are attributed to
the derived fixture rather than to that capture — naming it as their source would
misdescribe both files.

**A second capture of this tool's own output does carry literals, and it is named
here so the record is complete.** Because one vulnerability record is this shape's
count unit, an artifact holding none exercises no positive mapping at all, so a
second invocation of the same tool build was captured over input that resolves to
packages the seeded feed carries advisories for:
`oss-scan-results/adapter-tests/fixtures/captured-dependency-check-vulnerabilities.json`,
an unmodified whole report retained beside its command and measurements at
`harness/artifacts/logs/dependency-check-positive-capture.{json,log}`. Its five
records carry **three distinct native literals**, each mapping by the
label-over-score precedence this document fixes: **`CRITICAL` → Critical** (×1),
**`HIGH` → High** (×2) and **`MEDIUM` → Medium** (×2). Every one of those records
also carries a CVSS score — 9.8, 8.1, 8.1, 5.3 and 5.3 — and **none was consulted**,
which is what makes the precedence rule non-vacuous for this adapter on genuine tool
output rather than only on a constructed fixture. The expectations are asserted field
by field in
`oss-scan-results/adapter-tests/expected/captured-dependency-check-vulnerabilities.rows.json`
by `test_dependency_check_adapter.py`'s `CapturedVulnerabilityFixtureTest`.

**Neither capture contributes a row to this dataset.** The second one was taken
outside `harness/artifacts/raw/`, over input that is not the pinned tree, so it is
adapter evidence and nothing else — the `dependency-check` entry in the table above
correctly reads zero rows and no observed literal, and the three literals in this
paragraph are attributed to the capture rather than to the dataset.

Each band and each rendered text below is the expectation asserted in
`oss-scan-results/adapter-tests/expected/derived-dependency-check-features.rows.json`,
read from that file rather than restated from the policy. Its eight expected rows
carry these literals:

| Fixture literal | Kind | `severity_norm` | Basis | Reaches `severity_native` as |
| --- | --- | --- | --- | --- |
| `3.200000047683716` | raw float in the `severity` member, carrying a float32-to-float64 representation tail | Low | `cvss_score` | `3.2` |
| `5.300000190734863` | raw float in the `severity` member, carrying the same kind of tail | Medium | `cvss_score` | `5.3` |
| `moderate` | lower-case label, absorbed by the case-insensitive Table 2 | Medium | `label` | `moderate`, as observed |
| `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` | upper-case labels, one row each | Low, Medium, High, Critical | `label` | as observed |
| no `severity` member at all, with two CVSS entries on the record | no mapped label, so a score is consulted — step 3 of the evaluation order | High | `cvss_score` | `7.5` |

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
abstraction. That record — `CVE-2020-11022` in the fixture — carries no
`severity` member at all, so no mapped label exists and the score path is
reached, and it carries **two** score entries: a CVSS **v2.0** score of **5.0**
and a CVSS **v3.1** base score of **7.5**. The selection order takes the highest
CVSS version first, so the v3.1 entry governs and `7.5` bands High. The entry
recorded for that row names the score together with its **source** `NVD:cvssv3` —
composed from the record's own source and the block key, because the `cvssv3`
block declares no source of its own — and its **version** `3.1`, as
`oss-scan-results/adapter-tests/expected/derived-dependency-check-features.rows.json`
asserts. Had only the band been recorded, a reader could not tell which of the two
scores produced it, and the two produce different bands.

**Two further rows of that fixture exercise the precedence in the direction that
is easiest to get wrong**, and they are named because no row of this dataset
exercises it: one carries a `LOW` label beside three CVSS blocks spanning
Critical, Medium and Low, and one carries a `CRITICAL` label beside a `cvssv3`
block scoring 6.1. In both the **label governs** and no score entry is consulted
or recorded — which is the rule of Part 1 doing its work, and which an
implementation reading `cvssv3` first would get wrong in both directions.

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
observed — the gate's readings in `harness/artifacts/logs/gate-record.json`, and
for the Datadog rule set the captured rules file's own metadata in
`harness/artifacts/logs/datadog-sast-rules.captured.meta.json` — and recorded
against each tool's status contract in `oss-scan-results/tool-status.md`. It is
cited here, not made a second time.

| tool | Comparability | Ground |
| --- | --- | --- |
| `opengrep` | comparable | Ruleset commit `f1d2b562b414783763fd02a6ed2736eaed622efa` observed and expected; 2,006 rules against 2,006 |
| `semgrep` | comparable | Ruleset commit `40b8c63f75dc7c22c8a77482d73bfb864b146f7e` observed and expected; 2,149 rules against 2,149, 19 Pro-only skipped |
| `datadog-static-analyzer` | **NOT COMPARABLE** | Ruleset sha256 observed `c5fd464c…a354f322` over 53 rulesets and 1,147 rules, expected `e70ede30…ff6c44f7` over 48 rulesets and 1,093 — all three fields differ |
| `joern` | comparable on query-set identity, **counts NOT COMPARABLE** | The query-set identity observed is the expected one: 6 bounded structural queries baked into the runner. Its counts are counts over one graph, and the graph loaded here is not the graph the inherited environment record describes; `harness/artifacts/logs/gate-record.json` records both identities and `oss-scan-results/tool-status.md` and `oss-scan-results/run-record.md` own the determination |
| `gitleaks` | comparable | The default rule set built into Gitleaks 8.30.1, not versioned separately; observed version equals the expected 8.30.1 |
| `checkov` | comparable | Policies bundled with Checkov 3.3.12, not versioned separately; observed version equals the expected 3.3.12 |
| `osv-scanner` | comparable on identity | No local database, the OSV API queried at scan time, observed and expected; no count to compare |
| `dependency-check` | **NOT COMPARABLE** | Keyless NVD JSON 2.0 feed, `NVD API Last Modified` observed `2026-08-30T12:00:19-04` over a 260,005,888-byte `odc.mv.db`, expected `2026-08-23T08:00:06-04` — seven days apart |
| `trivy` | **NOT COMPARABLE** | Vulnerability DB v2 observed `2026-08-30T13:05:01.49156526Z` against expected `2026-08-23T06:56:50Z`, java DB v1 observed `2026-08-30T01:07:49.364681226Z` against expected `2026-08-23T01:05:59Z` — both seven days later; both DB versions match |

The three digests in the `datadog-static-analyzer` row are abbreviated for the
table's width only; all three full values appear in that tool's entry in Part 2 and
in `oss-scan-results/tool-status.md`.

**Every digest and timestamp above is a reading of the provisioning as this run
found it**, recorded in `harness/artifacts/logs/gate-record.json` — which carries
the command run, its verbatim output and the expected value beside it for each
identity — together with
`harness/artifacts/logs/datadog-sast-rules.captured.meta.json` for the Datadog
rule set's digest, ruleset count and rule count. None is re-measured here. Three
of the paths behind them are host-global and shared — the Datadog rules file, the
Trivy cache metadata and the Dependency-Check datafeed — and each is a rule set
or feed this run neither installed nor updated, which is why the difference is a
recorded difference under AAP 0.1.3's authority rule rather than something to
repair. That the Datadog artifact was produced against the *observed* rule set is
visible in the artifact itself: it declares 1,147 rule descriptors, equal to the
observed rule count and not to the expected 1,093. The two ruleset checkouts named
in the first two rows equal their expected commits exactly.

**Which field a severity was read from is not a comparability status.** It is a
property of what each producer emitted and of what AAP 0.5.4 authorises, not of
the rule set or feed that produced the finding, so it is measured per tool in
Part 2 rather than folded into the table above.

# Values that could not be established

Named rather than omitted, because a value missing from the record is a value
nothing downstream can check.

| Value | Scope | Why |
| --- | --- | --- |
| The native severity vocabulary `osv-scanner` would have used | `osv-scanner` | It wrote no artifact, so no record arrived and no mapping decision was exercised. Table 2 for an ecosystem label and Table 3 for a CVSS entry were the policy in force; which of them a record would have taken is not established by this run, and no vocabulary was attributed to the tool on the strength of what it usually emits |
| The native severity literals `dependency-check` emits | `dependency-check` | Its artifact carries zero finding records, so no literal was observed. The adapter's handling of this shape's label and score paths is established by the committed fixture named in Part 2, which is a statement about the adapter and not a dataset count |
| The severity `opengrep` and `semgrep` would state through an authorised source | those two tools | Neither artifact states one: no result carries a `level` and no rule carries `properties.severity` or `properties.problem.severity`, measured as `severity_from_level` 0 and `severity_from_rule_property` 0 in `normalize-run.json`. What their `defaultConfiguration` objects say is not an authorised source and is therefore not reported as their native literal; the absence is stated instead of being filled from an unauthorised field |
| Behaviour of the `cvss_score` basis on this run's own artifacts | dataset-wide | No artifact exercised it: the basis was recorded 0 times over all 9,430 rows. It is established against the committed derived fixture instead — three of its records take that basis, one of them through the two-candidate selection that records a source and a version — and is stated as such rather than left to look like a path nobody tested |
| Behaviour of the `unmapped_literal` disclosure on this run's own artifacts | dataset-wide | Likewise recorded 0 times. Every literal that arrived was inside a mapped vocabulary |

`oss-scan-results/tool-status.md` carries the values that could not be
established outside this document's subject — among them the `gitleaks` rule count
and the `checkov` policy count, neither separately versioned and neither reported
by its tool. None was invented there and none is invented here.

# Cross-references that must hold in both directions

Each of these was checked, and each is one measurement appearing twice rather
than two measurements agreeing.

1. **Every literal listed here is present in the dataset.** The eleven
   `(tool, literal, band)` entries were recounted directly from
   `findings.json` and independently from `findings.csv`; both produce the same
   eleven entries with the same row counts as the tally rendered here.
2. **Every `severity_native` literal in the dataset appears here.** The recount
   yields no twelfth entry: the distinct literals in the dataset are `error`,
   `warning`, `note`, `none`, `HIGH`, `MEDIUM`, `LOW` and the absent literal, and
   each appears above against the tool that produced it.
3. **Per-tool row counts agree with `oss-scan-results/tool-status.md`** and with
   `normalize-run.json` `totals.rows_by_tool`, in the normalizer's processing
   order: `opengrep` 1,319, `semgrep` 1,162, `datadog-static-analyzer` 6,832,
   `gitleaks` 1, `checkov` 6, `trivy` 3, `osv-scanner` 0, `dependency-check` 0 and
   `joern` 107, summing to 9,430.
4. **`findings.json` and `findings.csv` agree** on every severity field, row for
   row: absence is `null` in the one and an empty field in the other, over the
   same 9,430 rows, with the typed comparison in `normalize-run.json`
   `output_comparison` reporting 9,430 rows and 113,160 fields compared and no
   first mismatch.
5. **The comparability verdicts match `oss-scan-results/tool-status.md`**
   tool for tool: three not comparable on ruleset or feed identity —
   `datadog-static-analyzer`, `trivy`, `dependency-check` — and `joern` not
   comparable on counts against the previous provisioning, on the separate ground
   of a different graph.
6. **`severity_norm` is absent nowhere** — 0 rows of the 9,430, both as counted
   over the dataset and as `normalize-run.json`
   `outputs.row_validation.severity_norm_absent` records it — and
   `severity_native` is absent on exactly the 2,488 no-vocabulary rows.
7. **The field-source counters close against the literals.** `sarif_level` 6,832
   equals `datadog-static-analyzer`'s `severity_from_level`; `label` 110 equals
   `joern`'s 107 plus `trivy`'s 3; `no_vocabulary` 2,488 equals the four
   absent-literal entries — 1,319 + 1,162 + 1 + 6 — and equals
   `outputs.row_validation.absence_by_optional_field.severity_native`. The five
   bases sum to 9,430.

# What this document does not do

- It draws **no comparison between tools**. It does not rank them, contrast their
  severity vocabularies, explain why one reported something another did not, or
  characterise any tool's vocabulary as better, stricter or more accurate. Two
  tools' labels arriving in the same band is a mapping statement about this
  dataset's schema and nothing else. That one producer states a `level` where two
  others do not is a fact about the artifacts, recorded because it decides which
  field the mapping reads, and it is not a judgement about any of the three.
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
