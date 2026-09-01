# Adapter tests for the normalizer

Which fixture came from which artifact, what each test module asserts, and how to run them.

The subject under test is `harness/lib/normalize/` — its shape detector, its path
resolver, its severity mapper, its reconciliation traversal, its emitter and its six
adapters. Nothing under `harness/lib/normalize/` is written from this tree, and no
fixture is ever rewritten to agree with an adapter: a disagreement between an adapter
and a hand-verified expectation is a finding to diagnose, not a file to overwrite.

## 1. Purpose

These tests are the evidence that the normalizer is **correct rather than merely
finished**. A finished normalizer writes twelve well-formed fields; a correct one
writes the twelve fields the artifact actually justifies, rejects what it cannot
attribute, and balances `raw finding records = dataset rows + rejected records` against
a count taken by a traversal that builds no rows. The difference between the two is not
visible in `oss-scan-results/findings.json` — a path resolved against the wrong base
still yields a plausible row, and a rejection counted under the wrong class still
balances the identity. It is visible here, or nowhere.

**These tests do execute.** The prohibition on running test suites covers **Spark's
own** suites, in any language (AAP §0.1.3, §0.3.1, §0.6.3) — including the Python test
modules under `python/pyspark/**`, which sit inside the authoritative scope globs,
carry no `src/test` path segment, are read by the scanners as any other in-scope source
is, and are never run. This run's own adapter and reconciliation tests are a different
thing entirely: they run, and a failure among them stops the run (AAP §0.9.2).

This document describes what each **adapter** asserts. It draws no comparison between
one tool and another, in any direction (AAP §0.3.2, §0.8.2).

Three sibling result documents are named below, each only as the **owner** of the values
cited to it: `oss-scan-results/tool-status.md` owns the per-tool status contract,
`oss-scan-results/severity-map.md` the severity mapping and the observed native
literals, and `oss-scan-results/run-record.md` the index and the manifest of the two
artifact trees. No count is taken from any of them here. A count appearing in two
documents must be one measurement cited twice (AAP §0.6.4), so every number in this file
is cited to the file it was measured from — the fixture and expectation trees
themselves, `harness/artifacts/logs/adapter-tests-run.json`,
`harness/artifacts/logs/osv-scanner.status`, and the authored constants in
`harness/lib/normalize/emit.py` and `harness/lib/normalize/paths.py`.

No user-specified rules were provided for this project — `review_rules` returns the
single line `No user rules provided.`, and that one line is the whole document, which
AAP §0.7 and §0.10.2 corroborate independently. Enterprise-standard best practice
applies in their place, and their absence is expressly not licence to lower the bar:
every claim below names the file it describes, and no number appears here that is not
also in a file on disk.

## 2. How to run them

From the repository root, the whole tree:

```bash
python3 -m unittest discover -s oss-scan-results/adapter-tests -p 'test_*.py' -v
```

That is the command `harness/artifacts/logs/adapter-tests-run.json` records, in every
argument. The one difference is the executable: the record names the resolved interpreter
`/usr/bin/python3`, because it records what actually ran, while this file writes `python3`
so the command works wherever the base interpreter sits on `PATH`. The two are the same
invocation after executable resolution, not the same string — and the record carries that
absolute path and the interpreter's version separately, which is what makes the
substitution checkable rather than assumed.

The `-p 'test_*.py'` is part of the command rather than a decoration. It happens to be
`unittest`'s default pattern, so omitting it selects the same 10 modules today — but
the documented arguments and the recorded ones must be the same arguments, or a reader
reproducing the run is reproducing something slightly different from what was measured,
and would not know it.

One module at a time — the form each per-module measurement in the run record was taken
with:

```bash
python3 -m unittest discover -s oss-scan-results/adapter-tests -p 'test_sarif_adapter.py' -v
```

The runner is the standard library's **`unittest`, not `pytest`**. AAP §0.4.1 fixes this
run's own code and tests to the CPython standard library and §0.4.3 adds no dependency
in any direction, so there is **no manifest, no lockfile and no install step** anywhere
in this tree, and no third-party plugin is imported or required. Every module imports
only `unittest` and other standard-library modules.

**Do not add `-t`.** The documented form passes `-s` and no `-t`; adding `-t .` makes
discovery treat the start directory as an importable package, which it is not, and
`unittest` exits 1 with an `ImportError` before running a single test.

The interpreter is the base CPython the gate recorded — expected **3.13.7**, observed
**3.13.7** at `/usr/bin/python3`, CPython `3.13.7 (main, Mar  3 2026, 12:19:54)
[GCC 15.2.0]`, as the run record carries it — reached as `python3` on `PATH`. Because the tests import only the standard library they run on that
base interpreter and touch neither of the two scanner virtualenvs that host the
Python-based scanners, so nothing here depends on a scanner's environment being active.

`harness/lib/normalize/` deliberately contains **no `__init__.py`**: PEP 420 implicit
namespace packages make `from normalize import paths` work once `harness/lib` is on
`sys.path`. Each test module therefore carries its own one-time `sys.path` bootstrap —
two lines that derive the repository root from `__file__` (`parents[2]` of the module),
insert `harness/lib`, and guard the insertion for idempotence because discovery imports
sibling modules that do the same. It mirrors the bootstrap `harness/lib/normalize/cli.py`
owns for its own direct-script route. Deriving the entry from `__file__` rather than from
the working directory is what lets the commands above run from any working directory
without anything being installed.

### The run record

`harness/artifacts/logs/adapter-tests-run.json` carries the exact command line, the
interpreter's absolute path and version, **an outcome for every test that executed**, a
per-module outcome, **a separate entry for every one of the 72 negative fixtures**, and
the exit status. It records exit status 0 over 1,325 tests and 26,008 subtests with zero
failures, errors, skips, expected failures and unexpected successes; its per-module test
counts sum to exactly that 1,325, and its per-test outcome inventory holds exactly 1,325
entries — so a module that had silently stopped contributing tests would show up as three
disagreeing numbers rather than as a passing run.

Each entry in that inventory is a fully qualified `<module>.<class>.<method>` identifier
— the identifier `python3 -m unittest <id>` re-runs — with its status. An aggregate could
not do that work: it cannot say *which* test produced a verdict, so a method that stopped
running would still reconcile against a total.

A failed adapter fixture, rejection or reconciliation test **halts the run** (AAP
§0.9.2). It is not a warning: the normalizer's correctness claim rests on this suite, so
a failure invalidates `oss-scan-results/findings.json`, `findings.csv` and every per-tool
number derived from them until it is diagnosed. Per-tool fixture results are rendered
into `oss-scan-results/tool-status.md`, which owns the per-tool status contract.

`.gitignore:31` is `artifacts/`. That pattern matches `harness/artifacts/` at any depth
and matches nothing under `oss-scan-results/`, so this tree — the fixtures, the
expectations, the test modules and this file — is collected by git normally, while the
run record sits outside ordinary collection and is published through the per-file
byte-size and sha256 manifest that `oss-scan-results/run-record.md` owns.

## 3. Fixture provenance

`fixtures/` holds 105 files and `expected/` holds 105, one expected file per fixture in
both directions. They divide, by filename prefix so the count can be re-taken from
the directory rather than trusted, into **9 captured positive fixtures** (the eight named in
§3.1 plus the second Dependency-Check capture), **7 declared-derived feature fixtures**
(`derived-*`), **72 negative fixtures** (`reject-*`), **6 structural-halt fixtures**
(`halt-*`), **5 malformed-known-filename fixtures** (`malformed-*`), **3 shape-detection
fixtures** (`near-*`) and **3 others** (`checkov-alt-shape`, `reconcile-mixed`,
`unknown-shape`) — 9 + 7 + 72 + 6 + 5 + 3 + 3 = 105. The accounting is exact and every one
of the 105 is claimed by some module's fixture inventory rather than sitting untested.

The captured count is 9 rather than one per adapter because Dependency-Check needs two.
Its scan-run artifact carries no vulnerability record at all, so no excerpt of it can
exercise a positive field; `captured-dependency-check-vulnerabilities.json` is the
unmodified whole report of a second invocation of the same tool build, over input that
resolves to packages the seeded feed carries advisories for. §3.1 states both, and
`expected/dependency-check.rows.json` records why the first cannot serve.

Attribution was measured rather than assumed: **every one of the 105 was opened by at
least one executed test**, recorded by instrumenting the file-read entry points during the
run, and 103 of the 105 are additionally named by at least one executed subTest. The two
that no subTest names — `captured-dependency-check-vulnerabilities.json` and
`reject-dependency-check-unresolvable-path.json` — are driven from class attributes rather
than from loop variables, so their stems never reach a subTest description;
`harness/artifacts/logs/adapter-tests-run.json` records the owning class, that class's
executed test count and the read attribution for each, rather than asserting coverage in
prose alone.

### 3.1 Captured positive fixtures, and what was taken from each artifact

One row per adapter exercised, three of them for the single shared SARIF adapter and
two for Dependency-Check, for the reason §3 gives. Each is
asserted **against the artifact itself** by that module's provenance class, which opens
the file under `harness/artifacts/raw/` and compares records and envelope; a comparison
against a digest this tree owns would establish self-consistency and nothing about
provenance.

| Fixture | Captured from | What was taken | Adapter under test |
| --- | --- | --- | --- |
| `fixtures/opengrep.sarif` | `harness/artifacts/raw/opengrep.sarif` | 8 whole results and the 7 rules they resolve through, behind the artifact's envelope with its complete **51-entry** `toolExecutionNotifications` array and no `originalUriBaseIds` | `adapters/sarif.py` |
| `fixtures/semgrep.sarif` | `harness/artifacts/raw/semgrep.sarif` | 9 whole results, the 9 rules they resolve through, the complete **179-entry** notification array, and no `originalUriBaseIds` — the producer emits none | `adapters/sarif.py` |
| `fixtures/datadog-static-analyzer.sarif` | `harness/artifacts/raw/datadog-static-analyzer.sarif` | 7 whole results keeping their own absolute `ruleIndex`, and therefore the **full 1,093-rule and 568-artifact arrays**; no `$schema`, because the artifact has none | `adapters/sarif.py` |
| `fixtures/trivy.json` | `harness/artifacts/raw/trivy.json` | the **whole artifact**, byte for byte: 3 `Results` elements, 3 misconfiguration records, no `Vulnerabilities`, no `Secrets` | `adapters/trivy.py` |
| `fixtures/gitleaks.json` | `harness/artifacts/raw/gitleaks.json` | the **whole artifact**, byte for byte: a bare array with one element, redacted by the tool itself | `adapters/gitleaks.py` |
| `fixtures/checkov.json` | `harness/artifacts/raw/checkov.json` | the **whole artifact**, byte for byte: the object form, `results` carrying only `failed_checks` with 6 records, and the tool's own `summary` | `adapters/checkov.py` |
| `fixtures/dependency-check.json` | `harness/artifacts/raw/dependency-check.json` | the **whole artifact**, byte for byte: 32 dependencies, **zero** vulnerability records, **zero** package objects — so **zero rows** | `adapters/dependency_check.py` |
| `fixtures/captured-dependency-check-vulnerabilities.json` | `harness/artifacts/logs/dependency-check-positive-capture.json` | the **whole report** of a second invocation of the same tool build, byte for byte: 2 dependencies, **5** vulnerability records, 2 package objects — so **5 rows**, which is what exercises this adapter's positive mapping | `adapters/dependency_check.py` |
| `fixtures/joern.json` | `harness/artifacts/raw/joern.json` | 14 whole findings in artifact order, spanning all six query ids and both outcomes the artifact really produces, behind the artifact's own envelope | `adapters/joern.py` |

For the five whole-artifact captures the fixture's sha256 **equals** the report's, which is
the strongest provenance available and is what those modules assert. Four compare against
the artifact under `harness/artifacts/raw/`; the fifth compares against the retained report
under `harness/artifacts/logs/`, because the invocation that produced it is deliberately not
part of the scanning run and writes nothing into `raw/`.

**One adapter needed a second capture, and got one rather than a substitute.**
AAP §0.6.2 requires a captured positive fixture that exercises the adapter's positive
field mapping. `harness/artifacts/raw/dependency-check.json` is the whole of that tool's
output for this run and contains no vulnerability record, and one vulnerability record is
that shape's count unit — so no excerpt of it can exercise any field of a row builder, and
the captured fixture taken from it carries zero rows. That measurement is a property of the
scanned scope rather than of the tool: exactly one manifest-shaped file is in scope (AAP
§0.2.1), widening the twelve globs is prohibited (AAP §0.3.2), and relabelling authored
data as a capture is prohibited outright.

So the tool was invoked a second time, unchanged — same build, same JDK, same seeded feed,
`--noupdate --disableOssIndex`, no network — over two JARs from the warm Maven local
repository that resolve to packages the feed carries advisories for. Its whole report is
retained at `harness/artifacts/logs/dependency-check-positive-capture.json`, with the exact
command and the measured output beside it in
`dependency-check-positive-capture.log`, and captured byte for byte as
`fixtures/captured-dependency-check-vulnerabilities.json`. Five vulnerability records
exercise `rule_id`, `message`, three distinct severity labels, the label-over-score
severity precedence, filesystem-absolute path relativization, `cve`, `cwe` — including the
ascending-numeric-identifier rule over a record that lists the *larger* identifier first —
and `package_coordinate` at candidate level 2. `expected/dependency-check.rows.json` records
the requirement as **SATISFIED**, states that it is not this fixture that satisfies it, and
keeps the zero-record measurement it always carried;
`test_dependency_check_adapter.py`'s `CapturedVulnerabilityFixtureTest` asserts the byte
equality and every field.

That invocation is deliberately outside the scanning run: it writes nothing into
`harness/artifacts/raw/`, is not normalized, and contributes no row to `findings.json` or
`findings.csv`, so `tool-status.md`'s zero-row entry for `dependency-check` remains a true
statement about the scanned scope. `fixtures/derived-dependency-check-features.json` stays
declared derived and now covers only what the genuine capture does not reach: an absent
severity label falling through to the CVSS score, coordinate candidate levels 1, 3 and 4
with the within-level tie, and the rejection conditions.

`fixtures/checkov.json` is the **object form**, which is the shape this run's artifact was
written in. It is one of two mutually exclusive top-level shapes the tool can emit, so the
other has to be exercised too: `fixtures/checkov-alt-shape.json` is the multi-framework
array form, **derived by shape transformation alone** from that capture, and §3.3 lists it
with the other derived fixtures.

`harness/artifacts/raw/` holds one artifact per tool that wrote one and nothing else
ever. There is no ninth artifact and no `osv-scanner.json`; §8 states that decision, its
ground and what would change if the observed outcome were ever different.

### 3.1.1 Declared-derived feature fixtures

Seven fixtures carry the cases the tools' own artifacts do not contain. Each is declared
derived in its own expected file, and each module's provenance class asserts that
declaration and that the file is **not** an excerpt of any artifact — so a derived
document can never be read as captured output.

| Fixture | What it exercises that no capture can |
| --- | --- |
| `fixtures/derived-semgrep-features.sarif` | an authored `originalUriBaseIds` base map and a chained walk; a multi-CVE tag ordering |
| `fixtures/derived-datadog-static-analyzer-features.sarif` | rule resolution through `ruleIndex` against a subset rules array |
| `fixtures/derived-trivy-features.json` | the `Vulnerabilities` and `Secrets` sections, package coordinates, and `scanner_class` varying row by row |
| `fixtures/derived-gitleaks-features.json` | rule, path and scope variety across six records |
| `fixtures/derived-checkov-features.json` | the `passed_checks`, `skipped_checks` and `parsing_errors` buckets, which must produce no row |
| `fixtures/derived-dependency-check-features.json` | vulnerability records at all: the coordinate-candidate precedence, the label-over-score precedence, absolute-path relativization |
| `fixtures/derived-joern-features.json` | a finding resolving into a `src/test` tree, retained with `in_scope: false` — measured: 0 of the artifact's 692 findings names a Suite or Test class, so the case cannot be captured |

### 3.2 The two kinds of fixture, and why each is the kind it is

**A positive fixture is an unmodified captured excerpt of the tool's own output** (AAP
§0.6.2). The reason is the one that matters: a hand-written fixture tests the adapter
against the shape you *believed* the tool emits rather than the shape it emits, and the
two diverge in exactly the places an adapter gets wrong — an optional member the
producer always omits, a numeric field arriving as a string, a base map the
specification requires and the producer never writes.

"Excerpt" has a precise meaning here. **Whole records are selected**, the **enclosing
structure is preserved byte for byte** — the schema declaration, the version, the run
and tool driver objects, the rule metadata a result resolves through, the per-query
tallies a collector wrote — and **no field value is edited**. Selecting records shortens
the file; it does not change any record. Every module's fixture-inventory class asserts
its fixtures are present, parse, and are unchanged by sha256 across the run, so an
assertion cannot pass over a fixture that silently failed to parse or was quietly
adjusted.

That last check is **change-detection, not provenance**: a fixture compared with a digest
this tree computed from that same fixture is self-consistent by construction. Provenance
is a separate assertion, and each module now carries it — `RawArtifactProvenanceTests` in
the SARIF, Gitleaks, Checkov, Dependency-Check and Joern modules and
`CapturedFixtureProvenanceTest` in the Trivy module. Each opens the corresponding artifact
under `harness/artifacts/raw/`, compares every retained record under a canonical
serialization in artifact order, checks the envelope member by member — including the
notification arrays, the rule and artifact arrays, and the **absence** of members the
artifact does not have — and fails with the artifact's path named if the artifact is
missing. None of them skips: the run record reports `skipped=0` as a property of this
suite, and a skipped provenance test would be indistinguishable from a passing one in a
total.

**Negative fixtures are derived from the positive ones**, one per rejection condition
its adapter can produce, and **their presence does not depend on this run's artifacts
happening to contain the case**: a rejection path with no test is a rejection path
nobody has exercised. Each asserts the offending record is **rejected and counted under
its class** — never dropped, never coerced into a row with a guessed field. The class is
asserted by name against a member of `normalize.paths.REJECT_CLASSES` read from the
module, because a rejection counted under the wrong class still balances the
reconciliation identity, and a test that only counts rejections cannot tell one
condition from another.

### 3.3 Derived fixtures, which were not captured

Named separately so no reader takes one of them for a capture. Each is derived from a
positive fixture and each exists for a reason a captured artifact could not serve:

| Fixture | What it is | Why it is not a capture |
| --- | --- | --- |
| `fixtures/checkov-alt-shape.json` | Checkov's other top-level shape: an array of report objects, here two | **Shape-transformed only** — the captured document's six `failed_checks`, in the same order, as whole unedited objects, rearranged into the multi-framework form so the identical rows in the identical order can be required of both shapes. `SourceDocumentEqualityTests` compares the two committed `failed_checks` documents directly, before any adapter runs, so an edit to a field the adapter ignores cannot pass unnoticed. Which shape a real artifact carries is decided by its content, so only one of the two can ever be captured on a given run |
| `fixtures/halt-trivy-unsupported-section.json` | A Trivy report carrying a non-empty finding section outside the three supported ones | Drives the structural **halt** rather than a row or a rejection — hence the different `halt-` prefix. A captured artifact validated its unsupported sections empty, so it cannot exercise the branch that stops the run |
| `fixtures/halt-trivy-unknown-section.json` | A member outside the known `Result` fields holding a non-empty array of objects | An unrecognised finding section, treated as one rather than dropped. The artifact carries no such member, so only an authored document reaches the branch |
| `fixtures/halt-trivy-section-not-an-array.json` | A supported section present as something other than an array or null | The branch that exists precisely to stop malformed output reconciling as a clean scan: `reconcile` would count it as zero records and the identity would balance |
| `fixtures/halt-trivy-declared-findings-unheld.json` | A `MisconfSummary` declaring failures that no supported section holds | Real tool output with nowhere to have come from. The captured artifact's summaries agree with its sections, so the condition cannot be captured |
| `fixtures/near-sarif-version-only.json` | Satisfies the `version` half of the SARIF test and fails the `runs` half | The detector's test is a conjunction, and a real artifact satisfies both halves or neither. Only an authored near-miss can fail exactly one |
| `fixtures/near-sarif-runs-only.json` | Satisfies the `runs` half and fails the `version` half | The mirror of the above. Neither fixture alone pins the conjunction; both are needed |
| `fixtures/unknown-shape.json` | A document matching neither SARIF nor any known native shape | The halt path for an unrecognised artifact has no captured instance by construction — every artifact this run produced matched a known shape |
| `fixtures/reconcile-mixed.json` | The at-least-one-rejection document the identity is asserted over | An identity asserted over a document with zero rejections is satisfied by an implementation that drops rejections entirely, so the fixture is authored to keep the identity non-degenerate |

## 4. Rejection-condition matrix

AAP §0.5.4 enumerates **nine** rejection conditions and `normalize.paths.REJECT_CLASSES`
holds **ten** members. That is not a discrepancy: one condition splits into two classes
and one condition spans two. "An unresolvable or absent path" is realised as
`absent_path` where the record names no location at all and `unresolvable_path` where it
names one that cannot be resolved. "A cyclic, over-deep or invalid `uriBaseId` chain"
spans `unresolvable_path` — the cycle, the over-depth, a base absent from the map with no
fallback available, and a chain with no absolute ancestor — and `invalid_uri`, which
keeps its own class for a syntactically invalid URI.

Cells naming a fixture give the stem after the `reject-<adapter>-` prefix; the full name
is `fixtures/reject-<adapter>-<stem>.sarif` for the SARIF adapter and
`fixtures/reject-<adapter>-<stem>.json` for the five native adapters. Superscript
letters mark a condition covered by a **named assertion rather than a fixture**, listed
under the table. Every other cell states the reason the condition cannot arise for that
adapter.

| Condition (AAP §0.5.4) | `sarif` | `trivy` | `gitleaks` | `checkov` | `dependency-check` | `joern` |
| --- | --- | --- | --- | --- | --- | --- |
| An unresolvable or absent path | `unresolvable-path`; `absent_path` <sup>a</sup> | `unresolvable-path`; `absent-path` | `unresolvable-path` <sup>b</sup>; `absent-path` | `unresolvable-path`, `unresolvable-path-uri-anchor`; `absent-path` | `unresolvable-path` <sup>d</sup>; `absent-path` | `unresolvable-path`; `absent_path` <sup>e</sup> |
| A cyclic, over-deep or invalid `uriBaseId` chain | `uribaseid-cycle`, `uribaseid-overdepth`, `uribaseid-missing-base` †, `uribaseid-relative-no-absolute-ancestor` † → `unresolvable_path`; `uribaseid-invalid-uri` → `invalid_uri` | Cannot arise: no SARIF base map is walked on this route | Cannot arise: `File` is a filesystem path, not a URI, so there is no chain to walk, cycle or exceed | Cannot arise: the tool emits no `uri`, no `uriBaseId` and no base map, so there is no reference to parse | Cannot arise: `filePath` is a filesystem path and this route parses no SARIF bases | Cannot arise: this shape carries no URI and no base map |
| An ambiguous bytecode-to-source resolution | Cannot arise: no bytecode input to resolve | Cannot arise: this adapter resolves reported paths, never bytecode | Cannot arise: no bytecode input, so no class identifier can resolve two ways | Cannot arise: a configuration-file report carries no bytecode class | Cannot arise: this route resolves a reported filesystem path, not bytecode | `ambiguous-path` — sole owner of the class |
| A missing rule identifier | `missing-rule-id` | `missing-rule-id` | `missing-rule-id` | `missing-rule-id` | `missing-rule-id` | `missing-rule-id` |
| A missing message | `missing-message` | `missing-message` | `missing-message` | `missing-message` | `missing-message` | `missing-message` |
| A `start_line` present but not an integer | `non-integer-start-line` | `non-integer-start-line` | `non-integer-start-line` | `non-integer-start-line` | Cannot arise: this shape carries no line information in any member at any depth, so there is no `start_line` for a non-integer to occupy — which is also why no such fixture exists here | `non-integer-start-line` |
| A dependency-oriented record with no formable package coordinate | Cannot arise: a SARIF result carries no package coordinate; the field is absent on every SARIF row by design | `no-package-coordinate` | Cannot arise: a secret finding names a code location rather than a package, so the field is absent by design and no record in this shape is dependency-oriented | Cannot arise: a misconfiguration names a location in a configuration file, so the field is absent by design and its absence is permitted rather than a rejection | `no-package-coordinate` | Cannot arise: a finding names a bytecode call site rather than a package, so the field is absent by design on every row |
| A record that cannot be attributed to a section | Cannot arise: a `runs[].results[]` element belongs to no finding section | `unattributable-section` ‡ | Cannot arise: the document is one flat array, so there is no per-section array a record could fail to be attributed to | Cannot arise: the report has one findings bucket, `results.failed_checks[]`, and no per-record section | Cannot arise: this shape has no sections, so no record can fail to be attributed to one | Cannot arise: this shape has no finding sections; `scanner_class` is fixed for the whole artifact |
| A malformed record | `malformed-record` | `malformed-record` | `malformed-record` | `malformed-record` | `malformed-record` | `malformed-record` |

Conditions covered by a **named assertion rather than a fixture**, because each is a
property of the invocation or of a whole class of record shapes rather than of one
record:

- <sup>a</sup> `sarif` / `absent_path` — `AbsentPathTests.test_the_ways_a_record_can_name_no_location_are_each_rejected`, which asserts each way a result can name no location individually rather than through one fixture.
- <sup>b</sup> `gitleaks` / `unresolvable_path` — `RejectionTests.test_unresolvable_path_is_produced_where_the_metadata_supplies_no_base`, exercised from the runner metadata because the condition is a property of the recorded invocation.
- <sup>c</sup> `checkov` / `absent_path` — `AnchorReconciliationTests.test_a_record_naming_no_location_at_all_is_the_absent_path_class`.
- <sup>d</sup> `dependency-check` / `unresolvable_path` — `NegativeFixtureTest.test_an_unresolvable_path_rejects_when_the_metadata_supplies_no_base`.
- <sup>e</sup> `joern` / `absent_path` — `UniqueResolutionOnlyTest.test_an_absent_or_sentinel_coordinate_is_a_counted_rejection`.

Three further notes the table cannot carry:

- <sup>n</sup> `trivy` / `absent_path` used to be the one class no fixture in this tree claimed, on the ground that the shape in which a Trivy path goes missing is the unresolvable one rather than the absent one. That was wrong: a missing, `null` or blank enclosing `Results[].Target` reaches the absent branch, so the class was producible and unexercised. `fixtures/reject-trivy-absent-path.json` now drives it — derived from the capture by emptying one `Target` and changing nothing else — and `RejectionClassPartitionTest` asserts the complete **producible / unreachable** partition over the closed ten-member vocabulary in both directions, so a reachable class cannot again be recorded as unexercised without a test disagreeing.
- † The two two-branch fixtures are driven under **both** metadata states. With an explicit recorded base for the tool, `uribaseid-missing-base` resolves to a row through the documented degenerate-base fallback; without one it is a counted `unresolvable_path` rejection. `uribaseid-relative-no-absolute-ancestor` rejects under **both** branches, and the contrast between the two fixtures is what shows the fallback is scoped to eligible walk outcomes rather than applied to anything that failed to resolve.
- ‡ The unattributable-section fixture is driven through the adapter's public seam rather than through `adapt()`, because `adapt()`'s iteration is section-bound by construction and cannot produce that condition. That is a property of the adapter's design, recorded here rather than worked around silently.

Negative fixture counts, taken from the directory rather than from the table above, because
several conditions are exercised by more than one document and a hand-summed total drifts
the moment one is added: **16** `reject-sarif-*`, **10** `reject-trivy-*`, **12**
`reject-gitleaks-*`, **15** `reject-checkov-*`, **6** `reject-dependency-check-*` and **13**
`reject-joern-*` — **72** in all, each with its own hand-verified expected file. The table
names the *conditions* and which documents drive them; these are the documents that exist.

Two fixture names are worth reading carefully, because the stem names the AAP *condition*
and the expected file names the *class*, and the two classes are genuinely different:
`absent_path` is a record that names no location at all, while `unresolvable_path` is a
record that names one which cannot be anchored to the root.
`reject-gitleaks-unresolvable-path.json` and
`reject-dependency-check-unresolvable-path.json` each carry a record whose location is
present but unanchorable — the runner metadata supplies no base for that tool — and each
asserts `unresolvable_path`, matching its stem. Their `absent-path` siblings carry the
record that names no location and assert `absent_path`. Every one of the four is a
document the tool's own shape could produce, so neither class is demonstrated by a
fixture written to suit the other.

## 5. What each test module asserts

10 modules. Each was also run on its own, so the test count beside it is that
module's own measurement rather than a share of the aggregate; all 10 counts are
recorded in `harness/artifacts/logs/adapter-tests-run.json`, and they sum to the
suite's 1,325.

| Module | Subject | Tests |
| --- | --- | --- |
| `test_sarif_adapter.py` | `adapters/sarif.py` | 122 |
| `test_trivy_adapter.py` | `adapters/trivy.py` | 194 |
| `test_gitleaks_adapter.py` | `adapters/gitleaks.py` | 93 |
| `test_checkov_adapter.py` | `adapters/checkov.py` | 127 |
| `test_dependency_check_adapter.py` | `adapters/dependency_check.py` | 102 |
| `test_joern_adapter.py` | `adapters/joern.py` | 117 |
| `test_shape_routing_negative.py` | `shape.py` | 114 |
| `test_reconciliation.py` | `reconcile.py` | 162 |
| `test_cli_writers.py` | `cli.py` — the composition, its option surface and the output-ownership guard | 219 |
| `test_emit_publication.py` | `emit.py` — the staged all-or-nothing publication of the dataset pair | 75 |

The last two subject the modules that carry no adapter of their own: `cli.py`'s
composition and its refusal to write outside the log directory it owns, and `emit.py`'s
publication protocol — the staged write, the typed re-parse of both written files, and
the boundary that refuses a `start_line` below 1.

One property is common to all 10 and is stated once: every rejection class is
asserted **by name** against a member of `normalize.paths.REJECT_CLASSES` read from the
module, every row is compared over the twelve fields **iterated from `emit.FIELDS`**
rather than spot-checked, and each module's fixture-inventory class runs first, because
a fixture silently absent or failing to parse would let every later assertion pass over
an empty loop.

### `test_sarif_adapter.py`

The one shared adapter serves all three SARIF producers, so this module carries the
`uriBaseId` work for the whole tree. Its distinctive assertions:

- **The chain is walked, not read one level.** `uriBaseId` is resolved through
  `run.originalUriBaseIds`, following a chained `uriBaseId` on the entry it finds. The
  specification's own example expresses one base relative to another, so a consumer that
  read one level would be wrong on conformant input.
- **The cycle guard and the depth guard**, each rejected under its named class rather
  than allowed to loop or to resolve on the wrong entry.
- **The metadata-backed fallback applies only where the runner metadata supplies an
  explicit base for that tool.** Everywhere else the record is rejected under
  `unresolvable_path` and counted, never guessed. That conditional is what the two
  two-branch fixtures pin: a single-branch fixture would let a catch-all fallback pass.
  Every base is read from `harness/artifacts/logs/runner-metadata.json` through the real
  loaders, and none is written into the test as a literal — because a path resolved
  against a wrong-but-plausible base still produces a well-formed row, so every
  `in_scope` value could be wrong while nothing raised.
- **Errata conformance**: `..` segments are preserved rather than normalized out, and an
  archive reference is kept in its `<container>!<member>` form rather than rejected for
  the single leading slash the archive-format exception permits.
- **The first-location rule** — the row takes `locations[0]`, the record still counts
  once, and the number of records carrying more than one is reported through a counter —
  and **the ascending-identifier rule** for `cwe` and `cve`, chosen by ascending numeric
  identifier, with its own counters.
- **`package_coordinate` absent on every row**, `severity_norm` and `path` never absent,
  and no emitted path ever absolute.
- Root-independence is **proved** rather than repeated from the expected files, and the
  `**` matcher is exercised on the two glob forms that break naive implementations.

### `test_trivy_adapter.py`

- **`scanner_class` derives from the section array the record was read from, never from
  record content**, and it is asserted per row rather than in aggregate — this is the one
  adapter whose `scanner_class` varies row by row.
- **Every one of the four structural halts `trivy.HALT_REASONS` declares** is asserted
  behaviourally, one committed fixture each, with the reason, section, `result_index` and
  structure diagnostics checked rather than merely that something raised — and the closed
  tuple is iterated, so a fifth reason cannot arrive untested. A non-empty unsupported
  finding section halts rather than being dropped, asserted by its exact exception type. The reason it must be a halt rather than a warning is that
  a silently dropped section is absent from *both* sides of the reconciliation identity:
  the count unit never saw it, so the identity balances exactly while real tool output
  leaves no trace in the dataset, in the counters or in the reconciliation. The module
  also asserts the counterfactual arithmetic that would have balanced.
- **`start_line` is section-dependent** — present for the sections that carry line
  information and absent for the section that does not — asserted from the section rather
  than inferred from whether a value happened to be there.
- The redacted match never reaches any field.

### `test_gitleaks_adapter.py`

- **The path base is taken from the recorded invocation**, not from a constant: change
  the recorded base and every resolved path changes with it. The number of paths the
  invocation passed is what decides whether reported paths are relative to that directory
  or to the recorded working directory, so the base is a property of the invocation and is
  read from the runner metadata.
- **The no-secret-value invariant, asserted structurally** — over every one of the twelve
  fields of every row, against every sensitive value present anywhere in the artifact,
  rather than spot-checked on the one field a reader would think of first. `message` is
  the rule's description, and the committed fixtures carry no live-looking credential.
- **No severity vocabulary**: `severity_native` is `None` on every row, `severity_norm` is
  `Info` on every row and never absent, the basis recorded is the no-vocabulary route so
  the absence is *stated rather than a level assumed*, and the tally records no native
  literal for this tool.
- The document is a **bare top-level array**: not a mapping, and an empty one is not an
  error.

### `test_checkov_adapter.py`

- **Both top-level shapes are handled**, each from a real committed fixture, and the same
  records in either form are asserted to produce **identical rows in identical order**.
  That equality is what shows shape handling is normalization rather than two divergent
  code paths.
- **The leading-slash rule**, carried in from the user's worked example unchanged: the
  module first asserts the trap is real — reading the slash as filesystem-absolute
  genuinely produces a long `../` chain, measured through the resolver — and then asserts
  the emitted path carries no `../` segment, is not absolute, and is in scope.
- **The `file_abs_path` reconciliation**, which is the reliable resolution route rather
  than a cross-check: this provisioning's runner passes one target directory per expanded
  scope directory in a single invocation, so a slash-stripped `file_path` is relative to
  *whichever target matched* and a strip-and-join against the tree root names a directory
  that does not exist even once the slash is handled correctly. An anchor field is what
  disambiguates it.
- **`passed_checks` and `skipped_checks` produce no rows**, asserted from
  `fixtures/derived-checkov-features.json`, which actually contains three passed checks, a
  skipped check and a parsing error; only failures are findings. `parsing_errors` are
  status evidence rather than findings. The captured artifact carries none of those
  buckets, so it establishes the other half of the rule instead: an **absent** bucket
  reads as zero rather than raising.
- The count unit is the **union** of `results.failed_checks[]` across every report object,
  in either shape; and a record defective in two ways takes the **first** class in a fixed
  order, so a class is reproducible rather than incidental to which check ran first.

### `test_dependency_check_adapter.py`

- **Absolute-path relativization**: `filePath` arrives filesystem-absolute on the
  enclosing dependency and is expressed against the scan root, with no absolute value
  reaching a row.
- **The four-level package-coordinate precedence**, each level asserted by its own test
  method, with the within-level lexicographic tiebreak asserted on inputs whose document
  order and lexicographic order disagree — so a passing implementation cannot be one that
  merely takes the first candidate it meets.
- **Label-over-score precedence, with the entry that governed recorded** — the label, or
  the score with its source and version — because the requirement is that the selection be
  recorded and not merely that a band be produced.
- **The float32-to-float64 representation tail.** The artifact carries `3.200000047683716`
  and `5.300000190734863` in its severity field; both are asserted to band **numerically**,
  and no spurious precision is allowed to reach a text field.
- **`start_line` is `None` on every row, on every path**, because this shape reports at
  dependency granularity and carries no line information in any member at any depth. The
  adapter never synthesises one, and the counter for absent start lines is asserted equal
  to the row count.

### `test_joern_adapter.py`

- **Dual `src/main` and `src/test` resolution, taken only where it is unique.** Every
  `-tests` artifact the build emitted is in the graph input, so a finding can legitimately
  name bytecode compiled from a test tree.
- **A test-JAR finding is retained with `in_scope: false`**, asserted on a **named row** —
  its path, its `in_scope` value and its presence — because a dropped test-tree row and a
  retained one are indistinguishable in a row count. It is asserted on
  `fixtures/derived-joern-features.json` rather than on the capture, and that is measured
  rather than preferred: 0 of the artifact's 692 findings names a Suite or Test class, so
  no capture of it can carry the case.
- **An ambiguous resolution is a counted rejection, and the boundary against retention is
  kept sharp.** Ambiguity is asserted twice: positively, that exactly the expected
  rejections arrive under the named class; and negatively, that **no** colliding candidate
  reaches any field of any row — the half a first-writer-wins implementation fails while
  every count still adds up. An *ambiguous* coordinate is a rejection; a coordinate that
  resolves *uniquely* into somewhere unwanted is a kept row with `in_scope: false`, and
  those are not the same instruction.
- **The count unit is `findings[]`**, never the collector's own per-query tallies. The
  positive fixture keeps the two numbers deliberately different, so an implementation
  counting the tallies fails loudly rather than agreeing silently.
- Every test builds its own absolute root inside a temporary directory and materialises
  there the exact relative paths the expected rows name, in both trees, with the three
  recorded collisions materialised as **genuine** collisions — so the rejection is produced
  by the resolver rather than arranged by the test. No Spark file is read at test time, no
  Spark test suite is executed and no Spark source is modified.

### `test_shape_routing_negative.py`

The **mandated** negative test (AAP §0.5.4). It asserts the direction that actually goes
wrong — **a native artifact must not route to the SARIF adapter** — because a permissive
detector that accepts a native file as SARIF produces an empty result set rather than an
error, and an empty result set is indistinguishable from a clean scan. `findings.json` is
row-only, so a tool contributing no row is invisible in it by construction, and the
detector is the only place the difference can be established.

The contract is exactly two conditions: `version == "2.1.0"` **together with** a `runs`
array. The conjunction is why the two near-miss fixtures are the substance of this module
rather than decoration — each fails exactly one half, so an implementation checking only
`version` is caught by one and one checking only `runs` is caught by the other. The
module further asserts that each native artifact routes to its own distinct key, that the
three SARIF producers share one key, that the routing decision is a key rather than a
module, that an `osv-scanner` key exists so a legitimately written artifact would have
routed rather than falling into the halt path, that an unknown shape halts by name, and
that detection is **content-based, not filename-based**.

### `test_reconciliation.py`

- **The identity**, per artifact:
  `raw finding records = dataset rows for that tool + rejected records`,
  with the left-hand side from `reconcile.count_records` — a traversal that walks the
  count units and builds nothing.
- **The independence of that traversal is asserted structurally**, against the import
  graph and the signatures, not by observing that the two agree on a fixture: a delegating
  implementation agrees on every fixture. `reconcile` may import nothing from the
  `normalize` package, and the rejection-class vocabulary must arrive as a **parameter**
  rather than as an import.
- **The identity is asserted over a fixture that carries at least one rejection**, and
  dropping the rejections is asserted to *break* it — because an identity asserted over a
  document with zero rejections is satisfied by an implementation that drops rejections
  entirely.
- **The parsed `findings.json` and parsed `findings.csv` row counts are compared to the
  identity separately** rather than assumed equal, and the two files are compared under
  typed coercion rather than by counting lines.
- **An absent artifact reconciles as the exact literal `reconcile.NOT_APPLICABLE_ABSENT`**
  — the words "not applicable", an em dash and "artifact absent" — and never as
  `0 = 0 + 0`, which would be a passing assertion over an artifact nobody looked at. All
  nine tools are asserted accounted for.

## 6. Expected files

`expected/<fixture-stem>.rows.json` accompanies each fixture, one to one in both
directions, 105 and 105.

For a **positive** fixture it carries the exact twelve-field rows the fixture must
produce, **hand-verified and asserted field by field** — never generated by running the
adapter under test, which would assert only that the adapter agrees with itself. It also
records the per-row derivations that make a value checkable rather than merely present:
the severity basis and the entry selected, and the path basis re-derived through the
resolver at the recorded location.

For a **negative** fixture it carries the expected reject class and its count, together
with the per-artifact identity restated for that fixture —
`raw finding records = expected rows + expected rejections` —
so a fixture cannot be satisfied by an adapter that rejects
the right number of records under the wrong class, or that quietly loses a row alongside
the rejection.

The twelve fields, in their fixed order:

`tool`, `scanner_class`, `rule_id`, `message`, `severity_native`, `severity_norm`,
`path`, `start_line`, `cwe`, `cve`, `package_coordinate`, `in_scope`

That order is fixed by the authored `emit.FIELDS` constant in
`harness/lib/normalize/emit.py`, which is the only place it is written down. The tests
**iterate** it rather than restating it, so a field added, removed or reordered there is a
failure here rather than a silent divergence between the writer and its tests.

The absence convention: JSON `null` and an empty CSV field, the two agreeing row for row
under typed comparison. Absence is permitted for exactly five fields —
`severity_native`, `start_line`, `cwe`, `cve` and `package_coordinate`. `severity_norm`
and `path` are **never** absent: a record whose path cannot be resolved is rejected and
counted rather than emitted with a guess. **No emitted path is ever absolute**, including
for an archive member or any other coordinate that is not a file in the tree, and the
emitter asserts it.

## 7. No coverage threshold

**No coverage threshold is set** (AAP §0.6.2). The requirement is the positive mapping
plus the per-condition rejections, and that is what §3 and §4 record. A percentage would
be a weaker and less honest claim than the enumeration: it can be met while a rejection
path nobody exercised sits behind an unreached branch, and it cannot distinguish a
condition covered by a fixture from a condition that cannot arise. `coverage_threshold`
is recorded as asserted `false` with a `null` value in
`harness/artifacts/logs/adapter-tests-run.json` for exactly that reason.

## 8. The OSV-Scanner decision

This section exists because AAP §0.9.4 requires that a value which could not be
established is **named as such rather than omitted**. The value here was established, and
it is a negative.

**The decision.** The quartet `fixtures/osv-scanner.json`,
`expected/osv-scanner.rows.json`, `test_osv_scanner_adapter.py` and
`harness/lib/normalize/adapters/osv_scanner.py` is created **if and only if OSV-Scanner
writes an artifact**, decided from the observed Stage 3 outcome and never left open (AAP
§0.6.1). It was decided: **none of the four exists**, verified by direct check on each of
the four paths rather than inferred from any one of them.

**The observed outcome is that it wrote none.** `harness/artifacts/raw/` holds eight
artifacts and no `osv-scanner.json`, established by listing the directory rather than by
reading an exit code. `harness/artifacts/logs/osv-scanner.status` records exit code 128
with `artifact_bytes=MISSING` — the literal rather than `0`, because a zero-byte artifact
and an absent artifact are different outcomes. The tool stated its reason in its own
words: its stderr lists each in-scope directory it scanned, then `0 Extract calls`, then
`No package sources found, --help for usage information.` That is documented, long-standing
zero-package behaviour rather than a crash — reported at
<https://github.com/google/osv-scanner/issues/348>, and the same message for SBOMs
carrying no package URLs at <https://github.com/google/osv-scanner/issues/93>. Only the
tool's own words can settle completion against failure, and they settle it as completion:
the run continues past it.

**The recorded ground, as a fact a reader can check.** Exactly **one** manifest-shaped
file is in scope: `core/src/main/resources/org/apache/spark/ui/static/package.json`,
verified **5 lines / 80 bytes**, carrying a name, a license and `"type": "module"` with
**no dependencies block** and no lockfile beside it. Across the eighteen in-scope
directories there is **no `pom.xml`, no `requirements*.txt`, no `setup.py`, no
`pyproject.toml` and no JAR**. Nothing in scope resolves to a package, so the tool had
nothing to work on. That is a property of the scope, not of the installation, and the
scope globs were not widened to give it something to resolve.

**The counter-fact, stated honestly.** A **differently configured** OSV runner in a prior
provisioning walked the whole tree recursively and did write an artifact. Presence is
therefore **observed at Stage 3 rather than assumed** from the scope argument above — the
argument explains the observation, it does not substitute for it. That prior harness is
nonbinding precedent about contract and format only and is never a source of a value; every
number in this document comes from a file in this run.

**Where `osv-scanner`'s inventory entry lives instead.** In
`oss-scan-results/tool-status.md` and `oss-scan-results/severity-map.md`, which are the
authoritative inventory of all nine tools precisely because `findings.json` and
`findings.csv` are **row-only**: one record per finding, twelve fields, no metadata
envelope, so a tool with zero rows is invisible in them by construction. The tool is also
covered from this tree without an adapter test of its own —
`test_reconciliation.py` asserts the absent-artifact case for it synthetically, through the
exact `NOT_APPLICABLE_ABSENT` literal, and `test_shape_routing_negative.py` asserts an
`osv-scanner` routing key exists, so a legitimately written artifact would have routed
rather than falling into the halt path.

**If the observed outcome ever differs**, create the quartet on the `dependency_check`
pattern — the nearest dependency-oriented adapter, with the same four-level coordinate
precedence, the same label-over-score severity with the selected entry recorded, and the
same unformable-coordinate rejection — capture the positive fixture **unmodified** from
the artifact actually written, and update §3.1's provenance table and §4's rejection
matrix accordingly.
