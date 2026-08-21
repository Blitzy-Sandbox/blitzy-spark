# `oss-scan-results/tool-status.md` — the per-tool record

This is the per-tool record for one run of the provisioned open-source security-scanner harness over
the pinned Apache Spark tree at `/opt/blitzy-harness/spark-src`, commit
`59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`. It exists to make a zero honest.

**A broken tool must never look like an absence of findings.** A crashed scanner, an unparseable
artifact, a stale vulnerability feed and a disabled taint engine are each reported here as what they
are, with the exit code and the log. Nothing is smoothed over, and no ambiguous outcome is given the
benefit of the doubt.

Two disciplines govern every line below.

**No finding is characterized.** Not as important, not as severe-in-context, not as real, not as a
false positive, not as a duplicate of another tool's finding. **This run compares nothing** — no
Apex, Cantina or other scanner's results are present, so there is no baseline here and no comparison
to draw. What follows records what each tool said and what happened when it ran.

**Every number traces to a raw artifact, a log, or `harness/ENVIRONMENT.md`.** No rule id, CVE, CWE,
line number, version, count, elapsed time or feed timestamp is inferred, carried over from a plan, or
invented. Where a value could not be read, or does not exist to be read, that is what is recorded
instead of a number.

No user rules were provided for this work, so none are cited here.

A note on cross-references. `harness/ENVIRONMENT.md §N` is a section of the environment record
written by the earlier setup run, which this run reads and never edits. `run-record.md §N` is a
section of `oss-scan-results/run-record.md`, this run's own environment and execution record.
`section N` refers to a section of *this* file.

---

## 0. Why this file exists, and what it can and cannot state

**This file comes into existence once runners have been invoked, and is finalized at every stop from
Phase 1 onward.** That is what makes a failure reportable at all: a status file written only on
success could never record the failure that stopped the run.

**All nine runners were invoked.** `harness/artifacts/logs/` holds `<tool>.stdout.log`,
`<tool>.stderr.log` and `<tool>.meta.json` for every one of the nine, and `harness/artifacts/raw/`
holds the eight artifacts they wrote. So this is not a gate stop — a gate stop precedes any tool
invocation and would have produced no `tool-status.md` at all, since there would be no tool status to
state. `run-record.md` section 2 records all twelve gate checks as passed.

**What stopped the run.** Phase 1 completed: nine runners, invoked individually and serially between
`2026-08-21T05:25:52Z` and `2026-08-21T06:02:17Z`. The run then stopped on the record-versus-reality
disagreement `run-record.md` §4.5 sets out — `harness/ENVIRONMENT.md` §8 states a uniform contract in
which every runner scans `$SPARK_SRC`, and `gitleaks` was observed to have scanned the working
checkout instead. Under this run's own rule a disagreement between what the record states and what is
observed is an environment failure to report with both values and stop on, not to repair. **Phase 2
normalization was therefore never entered, and Phase 3 was never launched.**

**The consequence for this file, stated plainly.** Four of the facts this record would normally carry
describe a parse that never happened, and they are reported as not determined rather than filled in:

| Fact | State | Why |
|---|---|---|
| Parse status | `absent` for `trivy` only; **not determined** for the other eight | `absent` follows from an artifact that was never written, which is observable without parsing anything. `clean`, `partial` and `failed` each describe the outcome of a parse, and no parse was performed |
| Records parsed and rejected | **not determined** for all nine | No adapter ran, so no record was ever parsed or rejected. A reject count of zero would assert a clean parse that never took place |
| Finding count | **no numeric count for any tool** — see section 3 | A tool's finding count is the number of rows carrying its token in `findings.json`, and `findings.json` does not exist |
| Both reconciliation assertions, and the row-validation result | **not evaluated** — see section 7 | Neither has a right-hand side to evaluate against |

What this file *can* state, and does, is every fact Phase 1 produced: for all nine tools the
execution state, the exit status, the elapsed time, whether an artifact was written and how large it
is, and — read independently from each artifact's own structure — how many records that artifact
holds. Sections 4 to 6 carry the datadog block, the dependency-feed block and the two
record-versus-reality checks only Phase 1 can make. Section 8 reports each of the six Done-when
conditions on its own.

**The Phase 3 driver never writes to this file,** and no line here is reserved for it. It was never
launched (`run-record.md` section 6), and its only record write anywhere is a single appended line in
`run-record.md`.

Nothing under `harness/` was created, edited or deleted in the course of writing this file.
`harness/artifacts/raw/` was not created; the eight artifacts and the twenty-eight log files were
read and left byte-for-byte as the runners wrote them. `harness/artifacts/smoke/` was never read, and
no tool's outcome here was substituted from it.

---

## 1. The nine tools at a glance

The row key is the nine-token join key. Each token is spelled identically to the stem of that tool's
raw artifact and of its three log files, and is the value that tool's rows would carry in the `tool`
column of `findings.json` and `findings.csv`: `trivy`, `osv-scanner`, `dependency-check`, `gitleaks`,
`checkov`, `opengrep`, `semgrep`, `joern`, `datadog-static-analyzer`.

**Execution state, exit status and parse status are three independent facts** and are kept in three
separate columns. A tool that exited non-zero may hold a perfectly complete artifact, and a tool that
exited zero may hold none — so neither column may be read off the other.

| # | Tool | Execution state | Exit status | Elapsed | Artifact written | Parse status | Finding count |
|---|---|---|---|---|---|---|---|
| 1 | `trivy` | invoked once, ran to an exit code | `1` | 8 s | **none** | `absent` | not applicable — artifact absent |
| 2 | `osv-scanner` | invoked once, ran to an exit code | `1` | 70 s | `osv-scanner.json`, 2,801,510 B | not determined — no parse performed | not applicable — no dataset exists |
| 3 | `dependency-check` | invoked once, ran to an exit code | `14` | 1,602 s | `dependency-check.json`, 7,114,893 B | not determined — no parse performed | not applicable — no dataset exists |
| 4 | `gitleaks` | invoked once, ran to an exit code | `1` | 55 s | `gitleaks.json`, 21,119 B | not determined — no parse performed | not applicable — no dataset exists |
| 5 | `checkov` | invoked once, ran to an exit code | `1` | 3 s | `checkov.json`, 8,644 B | not determined — no parse performed | not applicable — no dataset exists |
| 6 | `opengrep` | invoked once, ran to an exit code | `0` | 225 s | `opengrep.sarif`, 1,941,724 B | not determined — no parse performed | not applicable — no dataset exists |
| 7 | `semgrep` | invoked once, ran to an exit code | `0` | 136 s | `semgrep.sarif`, 1,578,299 B | not determined — no parse performed | not applicable — no dataset exists |
| 8 | `joern` | invoked once, ran to an exit code | `0` | 13 s | `joern.json`, 38,589 B | not determined — no parse performed | not applicable — no dataset exists |
| 9 | `datadog-static-analyzer` | invoked once, ran to an exit code | `0` | 72 s | `datadog-static-analyzer.sarif`, 5,676,503 B | not determined — no parse performed | not applicable — no dataset exists |

Source: each row's exit status, elapsed time and artifact fact comes from that tool's
`harness/artifacts/logs/<tool>.meta.json`; artifact presence and byte size were confirmed against
`harness/artifacts/raw/` itself.

**No tool carries the execution state `not run — prior environment stop`.** All nine were reached and
invoked before the run stopped. Labelling any of them as never invoked would be false, and labelling
one `absent` on that basis would be a different and equally false fact — `absent` asserts that a
runner ran and wrote nothing, which is true of `trivy` alone.

**`exit_status: timeout` was recorded for no tool.** No time limit was imposed on any runner and no
runner was terminated for slowness: `dependency-check` ran for 1,602 s and was left to finish. Every
one of the nine `meta.json` files carries `"no_time_limit_imposed": true` and an integer exit code,
so no process terminated without one. Were any to have done so, `exit_status: timeout` would be the
single recorded name for it — a recorded tool condition, never grounds to re-run.

**The artifact's own record count, for the eight artifacts that exist.** This is *not* a finding
count and is not a dataset row count. It is the number of records each artifact holds by its own
structure, counted directly from the artifact and independently of any row-building traversal — the
left-hand side of the per-tool reconciliation assertion in section 7, which was never evaluated
because there is no right-hand side to compare it against.

| Tool | Records in the artifact | Record locator counted | Independent agreement with the tool's own reported total |
|---|---|---|---|
| `trivy` | not applicable — artifact absent | — | — |
| `osv-scanner` | 288 | `results[].packages[].vulnerabilities[]`, over 27 `results[]` sources and 97 package objects | the tool reports no total in its own output |
| `dependency-check` | 1,742 | `dependencies[].vulnerabilities[]`, over 326 dependency objects, of which 131 carry at least one | the tool reports no total in its own output |
| `gitleaks` | 34 | the top-level array; 27 distinct `File` values, 3 distinct `RuleID` values | `gitleaks.stderr.log` line 2 reports 34 |
| `checkov` | 6 | `results.failed_checks[]`; 3 distinct `file_path` values | the artifact's own `summary.failed` is 6 |
| `opengrep` | 849 | SARIF `runs[].results[]`, one run | `opengrep.stderr.log` line 25 reports 849 |
| `semgrep` | 389 | SARIF `runs[].results[]`, one run | `semgrep.stderr.log` lines 22 and 30 report 389 |
| `joern` | 67 | `findings[]` of this harness's own schema | `joern.stdout.log` line 13 reports per-query counts summing to 67 |
| `datadog-static-analyzer` | 6,832 | SARIF `runs[].results[]`, one run | `datadog-static-analyzer.stdout.log` reports 6,832 total violations |

The right-hand column matters: for six of the eight, a count taken from the artifact's structure and a
count the tool printed in its own log were derived separately and agree. For `osv-scanner` and
`dependency-check` the tool prints no total of its own, so the structural count stands alone and is
labelled as such rather than presented as corroborated.

---

## 2. One block per tool

Each block states the three independent facts separately, then the counts, then the conditions
observed. Diagnostic output is **referenced by path and line range and never quoted**: stdout and
stderr are kept in separate per-tool files precisely so that a report can point at stderr without
copying its contents.

### 2.1 `trivy`

- **Execution state:** invoked once, with no arguments, at `2026-08-21T05:25:52Z`; ran to an exit code.
- **Exit status:** `1`. `harness/bin/run-trivy.sh` exits with Trivy's own code.
- **Elapsed time:** 8 s (`trivy.meta.json`).
- **Artifact:** **none written.** `harness/artifacts/raw/trivy.json` does not exist; `trivy.meta.json`
  carries `"artifact": null`, and the runner's own trailer records the path as `MISSING`
  (`trivy.stdout.log` line 11).
- **Parse status:** `absent` — no artifact was written. This is the one parse status determinable
  without a parse.
- **Records in the artifact:** not applicable — artifact absent.
- **Records parsed / rejected:** not applicable — artifact absent.
- **Finding count:** **not applicable — artifact absent.** This is deliberately not the number zero.
- **Contribution to the dataset:** zero rows. A tool that wrote no artifact has nothing to
  contribute, and that is a different fact from a tool that scanned successfully and found nothing.

**Conditions recorded.** The scan did not complete. The cause is a `FATAL` in
`harness/artifacts/logs/trivy.stderr.log`, lines 8-10: a remote Maven repository answered HTTP `429
Too Many Requests` with a `Retry-After` of 1800 s while Trivy's Java dependency scanner was resolving
a POM, and the repository blocks subsequent requests from the same address until that clears. The
failure is in remote POM resolution and not in the vulnerability database, which Trivy read from cache
— its own database metadata is on `trivy.stdout.log` line 7 and is reported in section 5. Its stderr
lines 1-6 record its three scanners as enabled together with cache and recommendation notes, and line
7 a `WARN` about a POM dependency version it could not determine.

Trivy was not re-invoked, its configuration was not changed, no scope was narrowed to get it through,
no substitute scanner was introduced, and `harness/artifacts/smoke/` was not read for it. Trivy is
also the only tool whose `scanner_class` varies per finding — determined structurally by which of
`Results[].Vulnerabilities[]`, `Results[].Secrets[]` or `Results[].Misconfigurations[]` a record came
from (`harness/ENVIRONMENT.md` §12) — so nothing in this run distinguishes `vuln`, `secret` and
`misconfig` for it.

### 2.2 `osv-scanner`

- **Execution state:** invoked once, with no arguments, at `2026-08-21T05:26:01Z`; ran to an exit code.
- **Exit status:** `1`. `harness/bin/run-osv-scanner.sh` documents the tool's codes as `0 = no vulns,
  1 = vulns found`, so this is its documented finding-bearing exit rather than a failure.
- **Elapsed time:** 70 s.
- **Artifact:** `harness/artifacts/raw/osv-scanner.json`, 2,801,510 B.
- **Parse status:** not determined — no parse was performed. Not `clean`, not `partial`, not `failed`:
  each of those describes the outcome of a parse, and Phase 2 was never entered.
- **Records in the artifact:** 288, at `results[].packages[].vulnerabilities[]`, spread over 27
  `results[]` source entries and 97 package objects. An artifact fact, not a finding count.
- **Records parsed / rejected:** not determined — no adapter ran.
- **Finding count:** not applicable — no dataset exists to count rows in.
- **Contribution to the dataset:** none, because no dataset was produced. This is not a statement that
  the artifact would yield no rows.

**Conditions recorded.** `osv-scanner.stderr.log` carries 51 lines reporting a manifest or lockfile
scanned with the package count found in it, and 85 `failed resolution` lines at stderr lines 59-190,
each naming a POM under `$SPARK_SRC` whose transitive resolution did not complete because a sibling
Spark module at `4.1.0-SNAPSHOT` was not found in a repository. Recorded as a tool condition bearing
on what the tool was able to resolve; nothing is concluded from it here. Feed state is in section 5.

### 2.3 `dependency-check`

- **Execution state:** invoked once, with no arguments, at `2026-08-21T05:27:11Z`; ran to an exit code.
- **Exit status:** `14`. `harness/bin/run-dependency-check.sh` exits with Dependency-Check's own code.
- **Elapsed time:** 1,602 s. No time limit was imposed and it was not terminated for slowness.
- **Artifact:** `harness/artifacts/raw/dependency-check.json`, 7,114,893 B — and complete: its own
  stdout records `Analysis Complete (1594 seconds)` at line 64 and `Writing JSON report` at line 65.
- **Parse status:** not determined — no parse was performed.
- **Records in the artifact:** 1,742, at `dependencies[].vulnerabilities[]`, over 326 dependency
  objects of which 131 carry at least one. An artifact fact, not a finding count.
- **Records parsed / rejected:** not determined — no adapter ran.
- **Finding count:** not applicable — no dataset exists to count rows in.
- **Contribution to the dataset:** none, because no dataset was produced.

**Conditions recorded.** This tool is the clearest case in the run of exit status and artifact state
being independent facts: **it exited non-zero having written a complete artifact.** Its stdout carries
`[ERROR]` lines at 26, 30-33 and 66 — the Ruby Bundle Audit Analyzer could not initialize because
`bundle-audit` is not present and was disabled, and the .NET Assembly Analyzer could not initialize
because `dotnet` is not on the path — and the artifact's own `scanInfo.analysisExceptions` holds one
entry, the Bundle Audit initialization exception. Line 59 records the Sonatype OSS Index Analyzer
disabled for want of credentials; no credential name is printed there and no credential value appears
in any file this run wrote. Lines 34-43 record npm manifests analyzed without a `node_modules`
directory and, in the tool's own words, without a lock file. Each is an analyzer condition affecting
what the tool could examine, recorded as such.

The runner was given `HARNESS_DC_DATA_DIR=/opt/blitzy-harness/dependency-check/data-0` on its own
command line, recorded in `dependency-check.meta.json` as `extra_env`. That is the per-clone operation
`harness/ENVIRONMENT.md` §13 requires — the H2 database takes no concurrent writers — and the runner
reads its data directory from that variable by design, so it is not a change to the scanner's
configuration. Feed state is in section 5.

### 2.4 `gitleaks`

- **Execution state:** invoked once, with no arguments, at `2026-08-21T05:53:53Z`; ran to an exit code.
- **Exit status:** `1`. `harness/bin/run-gitleaks.sh` documents the tool's codes as `0 = no leaks,
  1 = leaks found`, so this is its documented finding-bearing exit rather than a failure.
- **Elapsed time:** 55 s.
- **Artifact:** `harness/artifacts/raw/gitleaks.json`, 21,119 B.
- **Parse status:** not determined — no parse was performed.
- **Records in the artifact:** 34, in the top-level array, across 27 distinct `File` values and 3
  distinct `RuleID` values. `gitleaks.stderr.log` line 2 independently reports 34, and line 1 reports
  the byte volume scanned and the duration. An artifact fact, not a finding count.
- **Records parsed / rejected:** not determined — no adapter ran.
- **Finding count:** not applicable — no dataset exists to count rows in.
- **Contribution to the dataset:** none, because no dataset was produced.

**Conditions recorded, and this is the condition that stopped the run.** On exit code and artifact
size alone this tool looks like one of the healthiest of the nine: it exited with its documented
finding-bearing code, its artifact parses, and nothing in its own output announces a problem. What is
wrong is attribution. `harness/ENVIRONMENT.md` §8 states a uniform contract in which every runner
scans `$SPARK_SRC`, and `run-gitleaks.sh` implements it by passing 18 absolute directories under
`$SPARK_SRC` as positional arguments — all 18 are listed on `gitleaks.stdout.log` lines 6-23. The
artifact nonetheless reports files that cannot have come from those directories, and `run-record.md`
§4.5 establishes by two independent proofs that the tree actually read was the working checkout: one
reported file does not exist in `$SPARK_SRC` at all, and one reported column range is impossible
against the line as it stands there. **Its 34 records are therefore not attributable to the pinned
tree**, and its reported paths cannot be canonicalized against `$SPARK_SRC` as the row schema
requires.

Both values are reported and neither is reconciled: `harness/ENVIRONMENT.md` was not edited,
`harness/bin/run-gitleaks.sh` was not edited, gitleaks was not re-invoked, its configuration was not
changed, and its artifact and logs stand byte-for-byte as it wrote them.

One thing is unaffected. The runner bakes in `--redact`, so no matched secret material is in the
artifact: across all 34 records the `Secret` field is empty or redacted, verified by inspecting the
field rather than its contents. `Description` — the tool's own rule description — is the field a
normalizer would take as `message`; `Secret` and `Match` are not that description and were not read.
Nothing resembling secret material appears anywhere in this file. A secret a scanner matched in the
tree would be a finding, not a leak.

### 2.5 `checkov`

- **Execution state:** invoked once, with no arguments, at `2026-08-21T05:54:48Z`; ran to an exit code.
- **Exit status:** `1`. `harness/bin/run-checkov.sh` documents the tool's codes as `0 = no failed
  checks, 1 = failed checks found`, so this is its documented finding-bearing exit rather than a
  failure.
- **Elapsed time:** 3 s.
- **Artifact:** `harness/artifacts/raw/checkov.json`, 8,644 B.
- **Parse status:** not determined — no parse was performed.
- **Records in the artifact:** 6, at `results.failed_checks[]`, across 3 distinct `file_path` values.
  An artifact fact, not a finding count.
- **Records parsed / rejected:** not determined — no adapter ran.
- **Finding count:** not applicable — no dataset exists to count rows in.
- **Contribution to the dataset:** none, because no dataset was produced.

**Conditions recorded.** Three artifact facts bear on any later parse, and all three are recorded
because each one silently corrupts a naive read. The artifact's **top level is a single JSON object**
carrying `check_type` `dockerfile` — and it carries findings. `harness/ENVIRONMENT.md` §12 describes
the array form as the one that appears when findings span several frameworks and the object form as
the shape emitted with nothing to report, and directs a consumer to handle both; here one framework
reported, so the object form appears *with* six records in it. The observed shape and the recorded
description are both stated, and neither is reconciled. An adapter that took an object top level to
mean "no findings" would drop six records, which is the failure this file exists to prevent. Second,
`file_path` is scan-root-relative **with a leading slash** and is not filesystem-absolute; the
absolute form is carried separately in `file_abs_path` (`run-record.md` §3.4). Third, **`severity` is
`null` on all six records**, which is the per-row absence `severity-map.md` would resolve — and
`severity-map.md` does not exist, because Phase 2 was never entered.

The artifact's own `summary` block reads `passed` 201, `failed` 6, `skipped` 0, `parsing_errors` 0,
`resource_count` 3, `checkov_version` `3.3.13`. Those are the tool's own counters over what it
examined; **none of them is a dataset finding count**, and the two zeros in that block are the tool's
own skipped and parsing-error tallies. Configuration facts, from the runner's own text: frameworks
`kubernetes,dockerfile,yaml,json,helm,kustomize`, bundled policies with `--skip-download`, and the
`secrets` framework deliberately not enabled (`harness/bin/run-checkov.sh` lines 6-10).

### 2.6 `opengrep`

- **Execution state:** invoked once, with no arguments, at `2026-08-21T05:54:51Z`; ran to an exit code.
- **Exit status:** `0`.
- **Elapsed time:** 225 s.
- **Artifact:** `harness/artifacts/raw/opengrep.sarif`, 1,941,724 B; SARIF `2.1.0`, one run, driver
  `Opengrep OSS`, `invocations[].executionSuccessful` true.
- **Parse status:** not determined — no parse was performed. An exit status of `0` does not establish
  a parse status: a tool that exited zero can equally hold an unparseable artifact, and only a parse
  could tell.
- **Records in the artifact:** 849, at SARIF `runs[].results[]`. `opengrep.stderr.log` line 25
  independently reports 849. An artifact fact, not a finding count.
- **Records parsed / rejected:** not determined — no adapter ran.
- **Finding count:** not applicable — no dataset exists to count rows in.
- **Contribution to the dataset:** none, because no dataset was produced.

**Conditions recorded.** Coverage conditions the tool reported about its own scan, from
`opengrep.stderr.log`: it scanned 4,095 files tracked by git with 754 code rules loaded and 738 rules
run (lines 6 and 25); its per-language table at lines 8-14 gives the rules and files it applied per
language; and lines 21-23 report that some files were skipped or only partially analyzed, that the
scan was limited to files tracked by git, and that 39 files were only partially analyzed owing to
parsing or internal errors. These are the tool's own statements about the extent of its scan, recorded
so that no count derived from this artifact is later read as exhaustive. Taint configuration is a
separate matter and is in section 6.

### 2.7 `semgrep`

- **Execution state:** invoked once, with no arguments, at `2026-08-21T05:58:36Z`; ran to an exit code.
- **Exit status:** `0`.
- **Elapsed time:** 136 s.
- **Artifact:** `harness/artifacts/raw/semgrep.sarif`, 1,578,299 B; SARIF `2.1.0`, one run, driver
  `Semgrep OSS`, `invocations[].executionSuccessful` true.
- **Parse status:** not determined — no parse was performed.
- **Records in the artifact:** 389, at SARIF `runs[].results[]`. `semgrep.stderr.log` lines 22 and 30
  independently report 389. An artifact fact, not a finding count.
- **Records parsed / rejected:** not determined — no adapter ran.
- **Finding count:** not applicable — no dataset exists to count rows in.
- **Contribution to the dataset:** none, because no dataset was produced.

**Conditions recorded.** From `semgrep.stderr.log`: 3,249 files tracked by git scanned with 760 code
rules loaded and 742 run (lines 6, 23 and 30), a per-language table at lines 8-14, `Scan completed
successfully` at line 21, parsed lines reported as ~99.9% at line 25, 845 files skipped as matching
`.semgrepignore` patterns at line 27, and the scan limited to files tracked by git at line 28. As a
configuration fact, this runner passes **no taint flag** — `harness/bin/run-semgrep.sh` line 7 records
it as the control arm — and consistently with that, **0 of its 389 results carry a SARIF `codeFlows`
entry**, against 58 of 849 for `opengrep`. That zero is a count of dataflow-trace structures in an
artifact, not a finding count.

### 2.8 `joern`

- **Execution state:** invoked once, with no arguments, at `2026-08-21T06:02:04Z`; ran to an exit code.
- **Exit status:** `0`.
- **Elapsed time:** 13 s.
- **Artifact:** `harness/artifacts/raw/joern.json`, 38,589 B.
- **Parse status:** not determined — no parse was performed.
- **Records in the artifact:** 67, at `findings[]`. An artifact fact, not a finding count.
- **Records parsed / rejected:** not determined — no adapter ran.
- **Finding count:** not applicable — no dataset exists to count rows in.
- **Contribution to the dataset:** none, because no dataset was produced.

**Conditions recorded.** This artifact is **this harness's own schema and not a tool-native format**
(`harness/ENVIRONMENT.md` §12): alongside `findings[]` it carries `tool`, `cpg_path`, `generated_at`,
`cpg_methods` 445,567, `cpg_typedecls` 57,863, `source_index_size` 4,127 and
`declaration_index_size` 9,939. Its five baked queries and the number of returns each contributed are
recorded in the artifact and on `joern.stdout.log` line 13: `process-launch-site` 19,
`java-deserialization-site` 8, `reflective-class-load` 40, `weak-hash-algorithm` 0,
`rpc-handler-reaches-process-launch` 0 — summing to the 67 records. **Those five numbers are the
artifact's own per-query return counts, not finding counts and not dataset rows, and the two zeros
among them are queries that returned nothing on this graph.** Line 14 records the harness collector's
own tally, `rows=67 path_resolved=67 path_unresolved=0`, the last being a path-resolution counter.

The graph was **read and not built**: the runner loads `harness/cpg/spark.cpg` with `importCpg`, and
`importCode` appears nowhere (`harness/ENVIRONMENT.md` §8). It used a private `mktemp -d` workspace
for the invocation. The tool emits no severity of its own, which is the tool-level absence
`severity-map.md` would resolve — and that file does not exist, Phase 2 never having been entered.

This is the Phase 1 runner only. The separate Phase 3 capability probe over the same graph was never
launched (`run-record.md` section 6), and nothing in this block relates to it.

### 2.9 `datadog-static-analyzer`

- **Execution state:** invoked once, with no arguments, at `2026-08-21T06:00:52Z`; ran to an exit code.
- **Exit status:** `0`.
- **Elapsed time:** 72 s.
- **Artifact:** `harness/artifacts/raw/datadog-static-analyzer.sarif`, 5,676,503 B; SARIF `2.1.0`, one
  run.
- **Parse status:** not determined — no parse was performed.
- **Records in the artifact:** 6,832, at SARIF `runs[].results[]`. Its own stdout independently
  reports 6,832 total violations over 568 files with violations. An artifact fact, not a finding
  count.
- **Records parsed / rejected:** not determined — no adapter ran.
- **Finding count:** not applicable — no dataset exists to count rows in.
- **Contribution to the dataset:** none, because no dataset was produced.

**Conditions recorded.** From its own configuration banner on `datadog-static-analyzer.stdout.log`:
version `0.9.1` revision `f76636e43554f7f9a8e3984a31d03ec8dea5489f` (lines 14-15), `config method :
none (no local file and no remote configuration)` (line 16), 1,093 static-analysis rules (line 19),
`static analysis enabled: true` (line 23) and `secrets enabled : false` (line 24). Line 8 records a
warning that no SAST configuration was detected and that default rules were taken. Line 32 lists the
languages its rules cover, and Scala is not among them — a configuration fact from the tool's own
banner, recorded independently by `harness/ENVIRONMENT.md` §5, and stated here because a count from
this artifact should not be read as covering a language the tool's own banner does not list. Its
summary block adds 1,093 rules evaluated, 96 rules with matches, and a duration of 58.840 s. Its
credential-gated path is in section 4.

---

## 3. The finding count, and why no tool carries one

A finding count in this file means one thing: the number of rows carrying that tool's token in
`findings.json`. **`findings.json` does not exist.** Phase 2 normalization was never entered, no
staging file was ever written, and nothing was published (`run-record.md` §4.7). There is therefore no
row set for any tool, and no numeric finding count can be stated for any of the nine without
inventing it.

The rule this file is built around is that **a numeric zero means "scanned successfully and found
nothing" only when the exit status is `0` and the parse status is `clean`.** No tool in this run
satisfies both halves of that test — four exited `0` but none has a determined parse status — so **no
tool is reported with a finding count of zero.** Each carries a non-numeric substitute instead:

| Tool | Finding count recorded | Why that substitute and not a number |
|---|---|---|
| `trivy` | `not applicable — artifact absent` | Parse status `absent`. An absent artifact has nothing to count. Reporting zero would say the tool scanned and found nothing, when in fact it did not finish |
| `osv-scanner`, `dependency-check`, `gitleaks`, `checkov`, `opengrep`, `semgrep`, `joern`, `datadog-static-analyzer` | `not applicable — no dataset exists` | Each holds an artifact with a known record count, but no dataset was produced, so there are no rows to count. Zero would be false in the opposite direction: it would say the artifact yielded nothing, when in truth it was never read |

Three distinctions are kept apart deliberately, because collapsing any two of them is how a broken
tool comes to look like an absence of findings:

- **An absent artifact** (`trivy`) contributes zero rows to any dataset — a fact about the tool, and
  never a finding count of zero.
- **An artifact never parsed** (the other eight) has no row count at all — a fact about this run,
  and not a fact about the artifact.
- **An artifact's own record count** (section 1) is a property of the file the tool wrote. It is the
  left-hand side of an assertion that was never evaluated, and it is not a finding count.

No tool is in the `failed` state — an artifact present but with no records extractable — because
determining that state requires a parse and none was performed. This matters for section 8: the
clause that marks Done-when condition 2 failed whenever any tool is `failed` is not engaged here, and
condition 2 is reported as never reached rather than failed.

---

## 4. `datadog-static-analyzer` — the AI path

| Fact | Value | Source |
|---|---|---|
| AI / secrets path | **unavailable** | `harness/ENVIRONMENT.md` §5 records it as UNAVAILABLE; the runner reported the path DISABLED (`datadog-static-analyzer.stdout.log` line 6) and the analyzer's own banner reads `secrets enabled : false` (line 24) |
| Credential source | the environment variables **`DD_API_KEY`** and **`DD_APP_KEY`** — **names only** | `harness/ENVIRONMENT.md` §5; `harness/bin/run-datadog-static-analyzer.sh` lines 39-44 |
| State of those variables | `absent` / `absent`, as the literal word | the runner's own line 6, which prints the literal `absent` for an unset variable by construction |
| Static-analysis path | enabled — `static analysis enabled: true` (line 23), 1,093 bundled rules | its own configuration banner |

**No value of either variable exists in this environment, none was read, and no value appears in this
file, in any deliverable, or in any log this run wrote.** Only the names appear. The runner switches
the path on only when `HARNESS_DD_SECRETS=1` and both variables are genuinely set; neither condition
held, so `--enable-secrets false` is what executed — the baked configuration, unchanged.

---

## 5. Dependency-feed state

For each of the three dependency scanners: the vulnerability-database version or timestamp **as the
tool's own output reports it**, and one of exactly four update outcomes. A separate network probe was
not used and would not answer the question — it would evidence what the network could reach, not what
the tool used.

The four outcomes are kept distinct and none is collapsed into another: `succeeded`; `failed`, where
the baked configuration attempted an update that did not complete; `not attempted`, where it performs
no update at all; and `not reported`, where the tool emits no feed metadata. Reporting a tool that
never tried as having failed would invent a failure.

| Tool | Feed version / timestamp, from the tool's own output | Update outcome | Records in the artifact | Pinned commit date |
|---|---|---|---|---|
| `trivy` | `{"Version":2,"NextUpdate":"2026-08-22T01:31:14.037675793Z","UpdatedAt":"2026-08-21T01:31:14.037676497Z","DownloadedAt":"2026-08-21T03:07:51.763463561Z"}` — the database metadata Trivy itself emitted, on `trivy.stdout.log` line 7 | **`not attempted`** — the baked configuration passes `--skip-db-update --skip-java-db-update` (`harness/bin/run-trivy.sh` line 26), so no update was tried | not applicable — artifact absent | `2025-10-23T19:31:06Z` |
| `osv-scanner` | **none emitted.** Neither its stdout nor its 190 lines of stderr carries a database version, timestamp or feed identifier | **`not reported`** — the tool emits no feed metadata. Its runner records the feed as the online OSV and deps.dev APIs with no local database (`harness/bin/run-osv-scanner.sh` line 9), so there is no update step to have succeeded or failed; `failed` would invent a failure and `not attempted` would imply a skipped update the tool has no notion of | 288 | `2025-10-23T19:31:06Z` |
| `dependency-check` | `NVD API Last Checked` `2026-08-21T03:07:24Z`, `NVD API Last Modified` `2026-08-20T20:00:06-04`, `NVD Cache Last Checked` `2026-08-21T03:07:24Z`, `NVD Cache Last Modified` `2026-08-20T20:00:06-04` — the `scanInfo.dataSource` entries the tool wrote into its own artifact, with `engineVersion` `13.0.0`. Its stdout prints no feed timestamp; the runner's banner records the data directory and a 247,603,200 B `odc.mv.db` (`dependency-check.stdout.log` lines 6-7) | **`not attempted`** — the baked configuration passes `--noupdate` (`harness/bin/run-dependency-check.sh` line 33), so no update was tried | 1,742 | `2025-10-23T19:31:06Z` |

The record counts in that table are the artifacts' own record counts from section 1, not finding
counts; no dataset row count exists for any tool.

**A stale or unreachable feed does not stop the run,** and none of the three states above stopped it.
Feed state is recorded for the same reason a crashed tool is: a low count from a stale feed is
otherwise indistinguishable from a genuine absence of findings.

**The commit-date caveat, stated beside the counts.** The pinned commit is dated
**`2025-10-23T19:31:06Z`** — read once by `git -C "$SPARK_SRC" log -1 --format=%cI` and recorded in
`run-record.md` §3.1, from which this appearance is taken so the two cannot diverge. A dependency tree
of that vintage will show CVEs the upstream project has since moved past, while the feeds above are
current to 2026-08-21. **Counts are reported exactly as found with that date beside them. Nothing is
corrected, adjusted, or annotated as stale.**

---

## 6. The two record-versus-reality checks only Phase 1 can make

Each of these would stop the run on a disagreement, reporting both values, with this file finalized
before exit. **Both agreed.** Neither is the disagreement that stopped this run; that one is
`gitleaks` (section 2.4).

| Check | Recorded | Observed | Agrees |
|---|---|---|---|
| **Opengrep taint setting** | `harness/ENVIRONMENT.md` §5: ENABLED, `--taint-intrafile --dataflow-traces` | the runner echoed exactly those flags (`opengrep.stdout.log` line 7, from `harness/bin/run-opengrep.sh` lines 43-44), and the tool's own output carries dataflow-trace material: 58 occurrences of its taint-source trace marker in `opengrep.stdout.log`, and 58 of the 849 SARIF results carry a `codeFlows` entry | **yes** — taint was not observed disabled |
| **datadog AI path** | `harness/ENVIRONMENT.md` §5: UNAVAILABLE, credential source the variables `DD_API_KEY` and `DD_APP_KEY` | the runner reported the path DISABLED with both variables `absent`, and the analyzer's own banner reads `secrets enabled : false` | **yes** — the path was not observed available |

**Taint is reported here only as a configuration fact:** the setting baked into the runner, checked
against the record and against the tool's own output. **This file makes no claim about Opengrep's
taint coverage for any language, including Scala** — not that it is covered, and not that it is not.
The two counts of 58 above evidence that the engine emitted dataflow traces on this scan; they
evidence nothing about which languages those traces covered, and nothing is extrapolated from them.
`harness/ENVIRONMENT.md` §5 records its own account of language coverage and is the place to read it;
this file neither repeats nor contradicts it.

`harness/ENVIRONMENT.md` was not edited to reconcile anything. Where a disagreement exists, as with
`gitleaks`, both values are reported and the disagreement stands as the environment failure it is.

---

## 7. The assertions, the row-validation result, and adapter limitations

Both assertions are reported with their outcome. An assertion recorded only on success could never be
recorded as failed, so each is stated here whether or not it was reached.

| Assertion | Outcome | Detail |
|---|---|---|
| **Per-tool reconciliation** — the raw artifact's own record count, derived independently of the row-building traversal, equals emitted rows plus rejects | **not evaluated** | The left-hand side exists and is tabulated in section 1 for the eight artifacts that were written. The right-hand side does not exist: no row was emitted and no record was rejected, because Phase 2 was never entered. For `trivy` the assertion is `not applicable — artifact absent`, which is the one exception the run grants and which does not leave the run incomplete |
| **Overall** — the staged CSV's row count equals the staged JSON's row count, both obtained by parsing the staged files rather than counting lines | **not evaluated** | Neither staging file was ever written. `run-record.md` §4.7 records that none of `.staging-findings.json`, `.staging-findings.csv` or `.staging-severity-map.md` was created and that no rename into place was attempted |
| **Row validation** — the twelve-field row contract checked in memory before anything is written | **not performed** | No row was built. Had it run and failed, this file would carry that failure and no staging file would exist; had a counting or publication failure occurred, the staging files would have been retained. Neither happened |

**Per-tool assertion status, for the record:**

| Tool | Per-tool reconciliation assertion |
|---|---|
| `trivy` | not applicable — artifact absent |
| `osv-scanner` | not evaluated — 288 records in the artifact; no rows and no rejects to compare against |
| `dependency-check` | not evaluated — 1,742 records in the artifact; no rows and no rejects to compare against |
| `gitleaks` | not evaluated — 34 records in the artifact; no rows and no rejects to compare against |
| `checkov` | not evaluated — 6 records in the artifact; no rows and no rejects to compare against |
| `opengrep` | not evaluated — 849 records in the artifact; no rows and no rejects to compare against |
| `semgrep` | not evaluated — 389 records in the artifact; no rows and no rejects to compare against |
| `joern` | not evaluated — 67 records in the artifact; no rows and no rejects to compare against |
| `datadog-static-analyzer` | not evaluated — 6,832 records in the artifact; no rows and no rejects to compare against |

**Reconciliation against the dataset is impossible, and that is the finding.** Each tool's finding
count would have to equal the number of rows carrying its token in `findings.json`; that file does not
exist, so the check cannot be performed rather than having been performed and passed.

### Adapter limitations observed in the artifacts

**No record was rejected by any adapter, because no adapter ran.** Nothing below caused a rejection in
this run. Each item is recorded as a **limitation of the normalizing adapter rather than a defect in
the tool's output**, observed by reading the artifacts directly, so that a later run can extend the
adapter instead of re-deriving the gap.

| Artifact | Construct observed | Why it is an adapter concern |
|---|---|---|
| `opengrep.sarif`, `semgrep.sarif` | Results carry `uriBaseId` `%SRCROOT%` while `run.originalUriBaseIds` is **absent** | A legal SARIF form the resolution chain cannot resolve: there is no base to resolve `%SRCROOT%` against. Immaterial in these two artifacts only because the `uri` alongside it is already absolute (`run-record.md` §3.4), so an adapter must prefer the absolute `uri` rather than fail on the unresolvable base |
| `checkov.json` | Top level is a single JSON **object** that carries six findings | `harness/ENVIRONMENT.md` §12 describes the array form as the one carrying findings across several frameworks and the object form as the shape with nothing to report. An adapter that read an object top level as "no findings" would drop six records |
| `checkov.json` | `file_path` is scan-root-relative **with a leading slash**; `file_abs_path` carries the absolute form | Read as filesystem-absolute it yields a wrong path and a false out-of-scope verdict |
| `datadog-static-analyzer.sarif` | `uri`s are relative to the tool's `-i` root, and the run carries an `artifacts[]` array with no `uriBaseId` on locations | The base is a property of the invocation, not a default; `run-record.md` §3.4 records it per runner |
| `joern.json` | This harness's own schema, not a tool-native format | An adapter selecting by tool name and expecting a tool-native shape would not find one |

Constructs explicitly checked for and **not present** in any of the three SARIF artifacts: a result
identifying its rule by `ruleIndex` alone, a `message.id` without inline `text`, an
`artifactLocation.index` without a `uri`, `logicalLocations` in place of a physical location, a `taxa`
entry, and a `tool.extensions[]` component. None appears, so none of those resolution paths would have
been exercised.

---

## 8. Where the run reached, condition by condition

**The run is incomplete.** It is neither wholly successful nor wholly failed, and this file claims
neither. Six conditions define completion; the run is complete only if all six hold together, and
they do not. Each is reported on its own below.

**Which tools reached which state.** All nine were invoked once, serially, and all nine ran to an exit
code; no tool was never invoked and no tool terminated without an exit code. Eight wrote an artifact
and `trivy` wrote none. Of the nine, one has a determined parse status — `trivy`, `absent` — and the
other eight have none, no parse having been performed. No tool is `clean`, `partial` or `failed`.

| # | Condition | Verdict |
|---|---|---|
| 1 | Every tool ran once with its baked configuration, each with a log carrying stdout, stderr, elapsed time and either an exit code or `exit_status: timeout`; every tool that wrote output has a raw artifact, and a tool that wrote none is recorded with parse status `absent`, its exit code and its stderr | **passed, with one qualification.** All nine were invoked once with no arguments, individually and serially; all nine have `<tool>.stdout.log`, `<tool>.stderr.log` and `<tool>.meta.json` carrying elapsed seconds and an integer exit code; no tool terminated without one. Eight wrote a raw artifact. `trivy` wrote none and is recorded in section 2.1 with parse status `absent`, exit status `1` and its stderr referenced by line range. The qualification is `gitleaks`: it ran to completion, but its 34 records are not attributable to the pinned tree (section 2.4) |
| 2 | `findings.json` and `findings.csv` contain every row from every artifact, each row carrying `tool`, `scanner_class`, `severity_norm` and `in_scope`, with no row dropped; row validation passes; and the per-tool reconciliation assertions pass | **never reached.** Phase 2 was never entered: no row was built, neither file was written, and neither assertion was evaluated (sections 3 and 7). Not marked failed: no artifact was determined `failed`, since determining that requires a parse, so the clause that would mark this condition failed on an unreconcilable artifact is not engaged |
| 3 | `severity-map.md` carries a row for all nine tools, including any that produced no finding | **never reached.** `oss-scan-results/severity-map.md` does not exist. Two per-tool inputs it would have resolved are recorded here instead: `checkov`'s `severity` is `null` on all six of its records (section 2.5) and `joern` emits no severity at all (section 2.8) |
| 4 | `tool-status.md` lists all nine, including any that failed or timed out, each with its parse status, its records parsed and rejected, and its row-validation result | **not met.** `run-record.md` §5 recorded this condition as never reached at the point that file was finalized, before this one existed. This file now delivers the part a Phase-1 stop can support: all nine tools listed by their join-key token, each with execution state, exit status, elapsed time, artifact state, and the artifact's own record count where an artifact exists. The remainder of the condition is not met and cannot be, Phase 2 never having been entered — parse status is determined for `trivy` alone, no tool has a records-parsed or records-rejected count, and there is no row-validation result. Reported as not met rather than passed |
| 5 | Phase 3 delivers three or more committed queries with recorded outcomes, spurious-return counts and the three effort measures, the graph having been read rather than built | **never reached.** No query source was written and the Phase 3 driver was never launched (`run-record.md` section 6). Recorded separately, because it is the one part of the condition that does hold: the graph was **read and not built** on both occasions it was opened in this run — the gate's coverage check and `harness/bin/run-joern.sh` — each with `importCpg`, and `importCode` was used nowhere |
| 6 | `run-record.md` states the `$SPARK_SRC` path scanned, its commit and date, and every tool failure and missing module | **passed.** `run-record.md` §3.1 gives the path, commit and commit date as read from the tree; §4.2 gives every tool that failed or terminated with its exit status; §4.3 records that `harness/ENVIRONMENT.md` marks no module as having produced no JAR. This file does not restate those facts, except the commit date, which section 5 must carry beside the counts and takes from that single read |

**The `absent` exception, and its limits.** A tool whose artifact is absent has nothing to count:
"not applicable — artifact absent" replaces both assertions for `trivy`, the run is not stopped for
it, and that exception does not by itself leave the run incomplete. It is the only exception granted.
Nothing else in this file is treated as excused by it — and in particular the eight artifacts that
were never parsed are not treated as absent, because they exist.

---

## 9. Provenance

Every value in this file traces to one of three sources and to nothing else.

| Source | What came from it |
|---|---|
| `harness/artifacts/logs/*` | every exit code, `exit_status`, elapsed time, start timestamp and `extra_env`; every runner banner fact — Trivy's database metadata and its missing-artifact trailer, the taint flags, the datadog credential-source line and its configuration banner, joern's query counts and collector tally; and every tool-reported scan-coverage figure for `opengrep`, `semgrep`, `gitleaks` and `dependency-check`, referenced by path and line range |
| `harness/artifacts/raw/*` | artifact presence and byte size; every record count in sections 1, 5 and 7, each derived from the artifact's own structure via its record locator, independently of any row-building traversal; the `checkov` top-level shape, `summary` block and null severities; the `dependency-check` `scanInfo` feed timestamps and its one analysis exception; the SARIF version, driver, rule counts, `codeFlows` counts and `uriBaseId` observations; the gitleaks field-level redaction check |
| `harness/ENVIRONMENT.md`, and `oss-scan-results/run-record.md` for facts that file owns | the uniform runner contract (§8), the recorded Opengrep taint setting and the datadog AI-path availability with its credential-source variable names (§5), the artifact-shape notes (§12) and the per-clone data-directory requirement (§13); and from `run-record.md` the pinned commit and commit date, the per-runner path bases, the gitleaks tree determination and the publication state |

`harness/bin/run-<tool>.sh` was read for each runner's documented exit-code contract and its baked
feed, taint and secrets flags. Nothing under `harness/` was created, edited or deleted;
`harness/artifacts/raw/` was not created; `harness/artifacts/smoke/` was never read.

**Where a value could not be read, or does not exist to be read, that is recorded instead of a
number.** Nothing here is inferred. No rule id, CVE, CWE, line number, version, count, elapsed time or
feed timestamp was invented or carried over from a plan, and no record was completed by inference: a
value that would have needed one inferred field is reported as not determined.

**No credential value appears anywhere in this file.** The two credential-gated capabilities are
recorded by variable name only — `DD_API_KEY` and `DD_APP_KEY` — and the Sonatype OSS Index condition
in section 2.3 is recorded without a name because the tool printed none. Diagnostic output is
referenced by path and line range and is never quoted. One exception is inherent and is not "fixed"
here: `harness/artifacts/raw/` and `harness/artifacts/logs/` are preserved byte-for-byte, so anything
a tool printed is in them; they cannot be redacted without destroying the audit trail they exist to
be. No tool in this run was observed emitting a provisioned credential value.

### Index of every numeric zero in this file

Recorded so the zero-honesty rule can be checked mechanically. **No zero below is a finding count**,
and no tool is reported with a finding count of zero.

| Zero | What it is |
|---|---|
| exit status `0` for `opengrep`, `semgrep`, `joern`, `datadog-static-analyzer` (sections 1, 2.6-2.9) | an exit code read from `meta.json`. None is accompanied by a determined parse status, so none is presented as a finding of nothing |
| `trivy` contributes zero rows (section 2.1) | a contribution to a dataset by a tool that wrote no artifact — explicitly not a finding count of zero |
| `joern` queries `weak-hash-algorithm` `0` and `rpc-handler-reaches-process-launch` `0` (section 2.8) | the artifact's own per-query return counts, two of five, summing with the others to its 67 records |
| `joern` `path_unresolved=0` (section 2.8) | the harness collector's path-resolution counter |
| `semgrep` `0` of 389 results carrying `codeFlows` (section 2.7) | a count of dataflow-trace structures in an artifact, consistent with a runner that passes no taint flag |
| `checkov` `summary.skipped` `0` and `summary.parsing_errors` `0` (section 2.5) | the tool's own counters over what it examined |
