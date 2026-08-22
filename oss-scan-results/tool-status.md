# `oss-scan-results/tool-status.md` — the per-tool record

What ran, what it exited with, what its artifact yielded, and what could not be
established. Written by the controller from `harness/artifacts/logs/*`,
`harness/artifacts/raw/*` and `harness/ENVIRONMENT.md`, and from nothing else.

## 0. Why this file exists, and what it can and cannot state

A count of zero is ambiguous on its own, and this file exists to disambiguate it. Three
facts are kept separate for every tool, because conflating any two of them produces a
misreading:

* **Execution state** — whether the runner was invoked at all.
* **Exit status** — the exit code, or `exit_status: timeout` for any termination without
  one.
* **Parse status** — what its artifact yielded: `clean`, `partial`, `failed` or `absent`.

A numeric zero means *scanned successfully and found nothing* **only** when the exit status
is `0` and the parse status is `clean`. Any other combination is reported as what it is. A
tool whose artifact is `absent` or `failed` carries **no finding count at all**, and its
contribution to the dataset is stated separately as zero rows.

This file characterizes nothing any tool reported. No finding here is called real, false,
important or duplicated, and no two tools' findings are called the same issue.

## 1. The nine tools at a glance

| Tool | Execution state | Exit | Elapsed | Parse status | Records in artifact | Rows emitted | Rejected | Finding count |
|---|---|---|---|---|---|---|---|---|
| `trivy` | invoked once, serially, with no arguments | `1` | 7.3 s | `absent` | — | 0 | 0 | not applicable — artifact absent |
| `osv-scanner` | invoked once, serially, with no arguments | `1` | 29.3 s | `clean` | 288 | 288 | 0 | 288 |
| `dependency-check` | invoked once, serially, with no arguments | `14` | 1755.9 s | `partial` | 1742 | 1697 | 45 | 1697 |
| `gitleaks` | invoked once, serially, with no arguments | `1` | 66.0 s | `clean` | 50 | 50 | 0 | 50 |
| `checkov` | invoked once, serially, with no arguments | `1` | 2.4 s | `clean` | 6 | 6 | 0 | 6 |
| `opengrep` | invoked once, serially, with no arguments | `0` | 190.2 s | `clean` | 849 | 849 | 0 | 849 |
| `semgrep` | invoked once, serially, with no arguments | `0` | 232.1 s | `clean` | 389 | 389 | 0 | 389 |
| `joern` | invoked once, serially, with no arguments | `0` | 47.5 s | `clean` | 67 | 67 | 0 | 67 |
| `datadog-static-analyzer` | invoked once, serially, with no arguments | `0` | 190.4 s | `clean` | 6832 | 6832 | 0 | 6832 |

## 2. One block per tool

### 2.1 `trivy`

| | |
|---|---|
| Execution state | invoked once, serially, with no arguments |
| Invocation | `harness/bin/run-trivy.sh` with no arguments, working directory `/opt/blitzy-harness/spark-src` |
| Started / finished (UTC) | `2026-08-22T04:30:05Z` / `2026-08-22T04:30:12Z` |
| Elapsed | 7.3 s |
| Exit status | `1` |
| stdout / stderr | `harness/artifacts/logs/trivy.stdout.log:1-12` / `harness/artifacts/logs/trivy.stderr.log:1-10` |
| Artifact | **none written** |
| Artifact shape, as detected | — |
| Adapter | — |
| Parse status | `absent` |
| Records in the artifact | not derivable |
| Records parsed into rows | 0 |
| Records rejected | 0 |
| Finding count | not applicable — artifact absent |
| Rows contributed to the dataset | 0 |
| Per-tool reconciliation assertion | not applicable — artifact absent |
| Overall CSV/JSON row-count assertion | CSV 10178 rows == JSON 10178 rows |
| Row-validation result | passed over all 10178 rows of the dataset |

**No artifact, so no finding count.** The runner was invoked and exited `1`; the
cause is in its own stderr at `harness/artifacts/logs/trivy.stderr.log:1-10`. Its contribution to
the dataset is zero rows, and that is not a finding of nothing. Both reconciliation
assertions are recorded as *not applicable — artifact absent*, which is the one
exception the request grants, and it does not leave the run incomplete.

### 2.2 `osv-scanner`

| | |
|---|---|
| Execution state | invoked once, serially, with no arguments |
| Invocation | `harness/bin/run-osv-scanner.sh` with no arguments, working directory `/opt/blitzy-harness/spark-src` |
| Started / finished (UTC) | `2026-08-22T04:30:12Z` / `2026-08-22T04:30:41Z` |
| Elapsed | 29.3 s |
| Exit status | `1` |
| stdout / stderr | `harness/artifacts/logs/osv-scanner.stdout.log:1-9` / `harness/artifacts/logs/osv-scanner.stderr.log:1-190` |
| Artifact | `harness/artifacts/raw/osv-scanner.json` (2801633 B) |
| Artifact shape, as detected | native: results[].packages[].vulnerabilities[] |
| Adapter | the osv-scanner native adapter |
| Parse status | `clean` |
| Records in the artifact | 288 |
| Records parsed into rows | 288 |
| Records rejected | 0 |
| Finding count | 288 |
| Rows contributed to the dataset | 288 |
| Per-tool reconciliation assertion | record count 288 == rows 288 + rejects 0 |
| Overall CSV/JSON row-count assertion | CSV 10178 rows == JSON 10178 rows |
| Row-validation result | passed over all 10178 rows of the dataset |

### 2.3 `dependency-check`

| | |
|---|---|
| Execution state | invoked once, serially, with no arguments |
| Invocation | `harness/bin/run-dependency-check.sh` with no arguments, working directory `/opt/blitzy-harness/spark-src` |
| Started / finished (UTC) | `2026-08-22T04:30:41Z` / `2026-08-22T04:59:57Z` |
| Elapsed | 1755.9 s |
| Exit status | `14` |
| stdout / stderr | `harness/artifacts/logs/dependency-check.stdout.log:1-69` / `harness/artifacts/logs/dependency-check.stderr.log` (empty — 0 lines) |
| Artifact | `harness/artifacts/raw/dependency-check.json` (7114893 B) |
| Artifact shape, as detected | native: dependencies[].vulnerabilities[] |
| Adapter | the dependency-check native adapter |
| Parse status | `partial` |
| Records in the artifact | 1742 |
| Records parsed into rows | 1697 |
| Records rejected | 45 |
| Finding count | 1697 |
| Rows contributed to the dataset | 1697 |
| Per-tool reconciliation assertion | record count 1742 == rows 1697 + rejects 45 |
| Overall CSV/JSON row-count assertion | CSV 10178 rows == JSON 10178 rows |
| Row-validation result | passed over all 10178 rows of the dataset |

Rejected records, by reason — counted, never dropped silently and never repaired
by inference:

| Reason | Count |
|---|---|
| dependency vulnerability with no formable coordinate: no packages[] entry | 45 |

### 2.4 `gitleaks`

| | |
|---|---|
| Execution state | invoked once, serially, with no arguments |
| Invocation | `harness/bin/run-gitleaks.sh` with no arguments, working directory `/opt/blitzy-harness/spark-src` |
| Started / finished (UTC) | `2026-08-22T04:59:57Z` / `2026-08-22T05:01:03Z` |
| Elapsed | 66.0 s |
| Exit status | `1` |
| stdout / stderr | `harness/artifacts/logs/gitleaks.stdout.log:1-27` / `harness/artifacts/logs/gitleaks.stderr.log:1-2` |
| Artifact | `harness/artifacts/raw/gitleaks.json` (31371 B) |
| Artifact shape, as detected | native: a top-level array |
| Adapter | the gitleaks native adapter |
| Parse status | `clean` |
| Records in the artifact | 50 |
| Records parsed into rows | 50 |
| Records rejected | 0 |
| Finding count | 50 |
| Rows contributed to the dataset | 50 |
| Per-tool reconciliation assertion | record count 50 == rows 50 + rejects 0 |
| Overall CSV/JSON row-count assertion | CSV 10178 rows == JSON 10178 rows |
| Row-validation result | passed over all 10178 rows of the dataset |

### 2.5 `checkov`

| | |
|---|---|
| Execution state | invoked once, serially, with no arguments |
| Invocation | `harness/bin/run-checkov.sh` with no arguments, working directory `/opt/blitzy-harness/spark-src` |
| Started / finished (UTC) | `2026-08-22T05:01:03Z` / `2026-08-22T05:01:06Z` |
| Elapsed | 2.4 s |
| Exit status | `1` |
| stdout / stderr | `harness/artifacts/logs/checkov.stdout.log:1-2870` / `harness/artifacts/logs/checkov.stderr.log` (empty — 0 lines) |
| Artifact | `harness/artifacts/raw/checkov.json` (8470 B) |
| Artifact shape, as detected | native: results.failed_checks[] (object or array top level) |
| Adapter | the checkov native adapter |
| Parse status | `clean` |
| Records in the artifact | 6 |
| Records parsed into rows | 6 |
| Records rejected | 0 |
| Finding count | 6 |
| Rows contributed to the dataset | 6 |
| Per-tool reconciliation assertion | record count 6 == rows 6 + rejects 0 |
| Overall CSV/JSON row-count assertion | CSV 10178 rows == JSON 10178 rows |
| Row-validation result | passed over all 10178 rows of the dataset |

### 2.6 `opengrep`

| | |
|---|---|
| Execution state | invoked once, serially, with no arguments |
| Invocation | `harness/bin/run-opengrep.sh` with no arguments, working directory `/opt/blitzy-harness/spark-src` |
| Started / finished (UTC) | `2026-08-22T05:01:06Z` / `2026-08-22T05:04:16Z` |
| Elapsed | 190.2 s |
| Exit status | `0` |
| stdout / stderr | `harness/artifacts/logs/opengrep.stdout.log:1-5603` / `harness/artifacts/logs/opengrep.stderr.log:1-25` |
| Artifact | `harness/artifacts/raw/opengrep.sarif` (1941724 B) |
| Artifact shape, as detected | SARIF 2.1.0 (version=2.1.0 with runs[]) |
| Adapter | the shared SARIF adapter |
| Parse status | `clean` |
| Records in the artifact | 849 |
| Records parsed into rows | 849 |
| Records rejected | 0 |
| Finding count | 849 |
| Rows contributed to the dataset | 849 |
| Per-tool reconciliation assertion | record count 849 == rows 849 + rejects 0 |
| Overall CSV/JSON row-count assertion | CSV 10178 rows == JSON 10178 rows |
| Row-validation result | passed over all 10178 rows of the dataset |

### 2.7 `semgrep`

| | |
|---|---|
| Execution state | invoked once, serially, with no arguments |
| Invocation | `harness/bin/run-semgrep.sh` with no arguments, working directory `/opt/blitzy-harness/spark-src` |
| Started / finished (UTC) | `2026-08-22T05:04:16Z` / `2026-08-22T05:08:08Z` |
| Elapsed | 232.1 s |
| Exit status | `0` |
| stdout / stderr | `harness/artifacts/logs/semgrep.stdout.log:1-11` / `harness/artifacts/logs/semgrep.stderr.log:1-30` |
| Artifact | `harness/artifacts/raw/semgrep.sarif` (1578299 B) |
| Artifact shape, as detected | SARIF 2.1.0 (version=2.1.0 with runs[]) |
| Adapter | the shared SARIF adapter |
| Parse status | `clean` |
| Records in the artifact | 389 |
| Records parsed into rows | 389 |
| Records rejected | 0 |
| Finding count | 389 |
| Rows contributed to the dataset | 389 |
| Per-tool reconciliation assertion | record count 389 == rows 389 + rejects 0 |
| Overall CSV/JSON row-count assertion | CSV 10178 rows == JSON 10178 rows |
| Row-validation result | passed over all 10178 rows of the dataset |

### 2.8 `joern`

| | |
|---|---|
| Execution state | invoked once, serially, with no arguments |
| Invocation | `harness/bin/run-joern.sh` with no arguments, working directory `/opt/blitzy-harness/spark-src` |
| Started / finished (UTC) | `2026-08-22T05:08:08Z` / `2026-08-22T05:08:55Z` |
| Elapsed | 47.5 s |
| Exit status | `0` |
| stdout / stderr | `harness/artifacts/logs/joern.stdout.log:1-17` / `harness/artifacts/logs/joern.stderr.log` (empty — 0 lines) |
| Artifact | `harness/artifacts/raw/joern.json` (38595 B) |
| Artifact shape, as detected | native: findings[] |
| Adapter | the joern native adapter |
| Parse status | `clean` |
| Records in the artifact | 67 |
| Records parsed into rows | 67 |
| Records rejected | 0 |
| Finding count | 67 |
| Rows contributed to the dataset | 67 |
| Per-tool reconciliation assertion | record count 67 == rows 67 + rejects 0 |
| Overall CSV/JSON row-count assertion | CSV 10178 rows == JSON 10178 rows |
| Row-validation result | passed over all 10178 rows of the dataset |

### 2.9 `datadog-static-analyzer`

| | |
|---|---|
| Execution state | invoked once, serially, with no arguments |
| Invocation | `harness/bin/run-datadog-static-analyzer.sh` with no arguments, working directory `/opt/blitzy-harness/spark-src` |
| Started / finished (UTC) | `2026-08-22T05:08:55Z` / `2026-08-22T05:12:06Z` |
| Elapsed | 190.4 s |
| Exit status | `0` |
| stdout / stderr | `harness/artifacts/logs/datadog-static-analyzer.stdout.log:1-47` / `harness/artifacts/logs/datadog-static-analyzer.stderr.log` (empty — 0 lines) |
| Artifact | `harness/artifacts/raw/datadog-static-analyzer.sarif` (5676504 B) |
| Artifact shape, as detected | SARIF 2.1.0 (version=2.1.0 with runs[]) |
| Adapter | the shared SARIF adapter |
| Parse status | `clean` |
| Records in the artifact | 6832 |
| Records parsed into rows | 6832 |
| Records rejected | 0 |
| Finding count | 6832 |
| Rows contributed to the dataset | 6832 |
| Per-tool reconciliation assertion | record count 6832 == rows 6832 + rejects 0 |
| Overall CSV/JSON row-count assertion | CSV 10178 rows == JSON 10178 rows |
| Row-validation result | passed over all 10178 rows of the dataset |
| Ruleset provenance | **not pinned** — the analyzer detected no local SAST configuration and fetched its default rules from the Datadog API during this invocation, per `harness/artifacts/logs/datadog-static-analyzer.stdout.log:8`; the same log records the config method as `none`, no local file and no remote configuration, at `harness/artifacts/logs/datadog-static-analyzer.stdout.log:16` |
| Rules in the fetched ruleset | 1093 static-analysis rules, per `harness/artifacts/logs/datadog-static-analyzer.stdout.log:19`; 1093 rules evaluated, per `harness/artifacts/logs/datadog-static-analyzer.stdout.log:42` |

**The ruleset behind these 6832 rows is not pinned, so the count is not reproducible from the
recorded environment alone.** This tool contributed 6832 of the dataset's 10178 rows, and the
rules that produced them were fetched over the network while the runner ran rather than read
from a local configuration — the two facts above, at lines 8 and 16 of its own stdout, are the
tool's own statement of that, and nothing else in this run identifies the set that arrived.

Both sides of the identity are recorded here, unreconciled, because this record does not
adjudicate between them and `harness/ENVIRONMENT.md` is read and never edited. That file's §5
*Ruleset identity* gives this tool's ruleset as its own bundled rules, 1093 static-analysis
rules, all bundled, and — unlike the Opengrep and Semgrep CE rows of that same table, each of
which carries a pinned ruleset commit — records no commit, revision or digest for it. The
tool's own stdout states instead that no SAST configuration was detected and the default rules
came from the Datadog API. The rule count agrees on both sides at 1093. The version and
revision recorded for this tool in `harness/ENVIRONMENT.md` §4 are the analyzer build's, the
same pair the tool prints at lines 14-15 of its stdout, and the rules were fetched while it
ran, so that pair does not identify them.

The consequence for a reader of the counts: an invocation of the same runner, with the same
baked configuration and no arguments, at a later date may load a different set of rules and
emit a different row count, and neither this record nor `harness/ENVIRONMENT.md` carries a
revision against which the two sets could be told apart. Nothing was reconfigured to make this
so, nothing was retried, and no rule, finding or count of this tool's is characterized here:
this is the provenance of the ruleset, not a judgement of what it reported. The tool's secrets
and AI path is a separate fact, recorded in §4 as *UNAVAILABLE* on both sides with its
credential source named by variable name only.

## 3. The finding count, and what each numeric zero means

| Tool | Finding count | Exit | Parse status | What the number means here |
|---|---|---|---|---|
| `trivy` | not applicable — artifact absent | `1` | `absent` | no count exists: the runner wrote no artifact |
| `osv-scanner` | 288 | `1` | `clean` | 288 rows, every record in the artifact parsed, exit `1` |
| `dependency-check` | 1697 | `14` | `partial` | 1697 rows parsed with 45 record(s) rejected; the rejects are itemized in §2 |
| `gitleaks` | 50 | `1` | `clean` | 50 rows, every record in the artifact parsed, exit `1` |
| `checkov` | 6 | `1` | `clean` | 6 rows, every record in the artifact parsed, exit `1` |
| `opengrep` | 849 | `0` | `clean` | 849 rows, every record in the artifact parsed, exit `0` |
| `semgrep` | 389 | `0` | `clean` | 389 rows, every record in the artifact parsed, exit `0` |
| `joern` | 67 | `0` | `clean` | 67 rows, every record in the artifact parsed, exit `0` |
| `datadog-static-analyzer` | 6832 | `0` | `clean` | 6832 rows, every record in the artifact parsed, exit `0` |

## 4. `datadog-static-analyzer` — the AI path

| | |
|---|---|
| Recorded in `harness/ENVIRONMENT.md` §5 | UNAVAILABLE |
| Observed | UNAVAILABLE |
| Credential source | the environment variables `DD_API_KEY` and `DD_APP_KEY` |
| Credential presence | `DD_API_KEY`: absent · `DD_APP_KEY`: absent |
| The analyzer's own banner | `secrets enabled         : false` |

The variable **names** are recorded and no value is: neither variable is set in this
environment, and nothing in this run reads a credential value into any file or log.

## 5. Dependency-feed state, as found

Read from each tool's own stdout and stderr rather than from a separate network probe,
which would evidence what the network could reach rather than what the tool used. Four
outcomes are distinguished and none is collapsed into another: `succeeded`, `failed`,
`not attempted` and `not reported`.

| Tool | Feed | Version or timestamp | Update outcome | Evidence |
|---|---|---|---|---|
| `dependency-check` | the H2 database odc.mv.db in $HARNESS_DC_DATA_DIR | engine 13.0.0; NVD API Last Checked: 2026-08-22T01:54:12Z; NVD API Last Modified: 2026-08-21T20:00:05-04; NVD Cache Last Checked: 2026-08-22T01:54:12Z; NVD Cache Last Modified: 2026-08-21T20:00:05-04 | **not attempted** | the report's own dataSources block in harness/artifacts/raw/dependency-check.json, and the runner's echoed invocation in harness/artifacts/logs/dependency-check.stdout.log:1-69 |
| `osv-scanner` | the live OSV API (api.osv.dev) plus api.deps.dev — no offline database | not reported — the tool queries at scan time and states no feed version | **not attempted** | harness/artifacts/logs/osv-scanner.stdout.log:1-9 and harness/artifacts/logs/osv-scanner.stderr.log:1-190; the runner performs no database update because the tool holds no local database to update |
| `trivy` | trivy.db and trivy-java-db in the shared cache $TRIVY_CACHE_DIR | 2026-08-22T00:59:13.136195619Z | **not attempted** | harness/artifacts/logs/trivy.stdout.log:1-12 (the runner echoes the cache's own metadata.json and passes --skip-db-update --skip-java-db-update) |

**The commit-date caveat, stated beside the counts.** The pinned commit is dated `2025-10-23T19:31:06Z`.
A dependency tree of that vintage will show CVEs the upstream project has since moved
past. The counts in §1 and §3 are reported as found, with that date beside them, and
nothing is corrected, adjusted or annotated as stale.

## 6. The two record-versus-reality checks only Phase 1 can make

| Check | Recorded | Observed | Agrees |
|---|---|---|---|
| datadog ai path | UNAVAILABLE | UNAVAILABLE | yes |
| opengrep taint | ENABLED | ENABLED | yes |

## 7. The assertions, the row-validation result, and adapter limitations

| | |
|---|---|
| Row validation | **passed** over all 10178 rows, in memory, before anything was serialized |
| Overall CSV/JSON assertion | CSV 10178 rows, JSON 10178 rows — **passed** |
| How the counts were taken | findings.json parsed and its top-level array counted; findings.csv read with csv.DictReader after its twelve-column header was validated, so a verbatim message containing a quoted newline cannot corrupt the count |
| CSV header | `tool, scanner_class, rule_id, message, severity_native, severity_norm, path, start_line, cwe, cve, package_coordinate, in_scope` |
| Header equals the JSON key order | yes |
| Fields that may be absent | `severity_native`, `start_line`, `cwe`, `cve`, `package_coordinate` — JSON `null`, empty CSV field |
| Fields derived rather than read from a tool | `tool`, `scanner_class`, `severity_norm`, `in_scope` — a null in any of them would be an adapter defect |

| Tool | Per-tool assertion |
|---|---|
| `trivy` | not applicable — artifact absent |
| `osv-scanner` | record count 288 == rows 288 + rejects 0 |
| `dependency-check` | record count 1742 == rows 1697 + rejects 45 |
| `gitleaks` | record count 50 == rows 50 + rejects 0 |
| `checkov` | record count 6 == rows 6 + rejects 0 |
| `opengrep` | record count 849 == rows 849 + rejects 0 |
| `semgrep` | record count 389 == rows 389 + rejects 0 |
| `joern` | record count 67 == rows 67 + rejects 0 |
| `datadog-static-analyzer` | record count 6832 == rows 6832 + rejects 0 |

### Adapter limitations observed in the artifacts

None. Every artifact matched either the SARIF 2.1.0 shape or its tool's documented
native shape, and no result used a valid construct the shared adapter does not resolve.

## 8. Where the run reached, condition by condition

| # | Condition | Verdict |
|---|---|---|
| 1 | Every tool ran once with its baked configuration, to completion or to a termination outside this run's control, each with a log carrying stdout, stderr, elapsed time and either an exit code or `exit_status: timeout`; every tool that wrote output has a raw artifact, and a tool that wrote none is recorded with parse status `absent`, its exit code and its stderr, contributing zero rows | **passed.** all 9 runners invoked once, serially, with no arguments; 9 of 9 carry stdout, stderr and a meta.json with elapsed time and an exit code; 1 wrote no artifact and is recorded with parse status `absent`, its exit code and its stderr |
| 2 | `findings.json` and `findings.csv` contain every row from every artifact, each carrying `tool`, `scanner_class`, `severity_norm` and `in_scope`, with no row dropped; row validation passes; and the per-tool reconciliation assertions pass | **passed.** `findings.json` and `findings.csv` published from one validated row list; row validation passed over 10178 rows; every evaluable per-tool reconciliation assertion passed; the CSV and JSON row counts are equal (10178 == 10178) |
| 3 | `severity-map.md` carries a row for all nine tools, including any that produced no finding | **passed.** `severity-map.md` carries one row for 9 of the nine tools, including those that produced no finding |
| 4 | `tool-status.md` lists all nine, including any that failed or timed out, each with its parse status, its records parsed and rejected, and its row-validation result | **passed.** this file carries one block for each of the nine, each with its execution state, exit status, parse status, records parsed and rejected, both reconciliation assertions and the row-validation result |
| 5 | Phase 3 delivers three or more committed queries with recorded outcomes, spurious-return counts and the three effort measures, and the graph was read rather than built | **delegated.** delegated to the Phase 3 driver by design. Both records are finalized before the driver is launched, so no record depends on a process that has yet to run; the driver appends its outcome to `run-record.md` §6 and reports it in full in `joern-probe.md` |
| 6 | `run-record.md` states the `$SPARK_SRC` path scanned, its commit and date, and every tool failure and missing module | **passed.** `run-record.md` states the `$SPARK_SRC` path scanned with its commit and commit date, every tool failure and termination, and the missing-module answer |

**5 of the six conditions were established by this controller pass and all 5 hold. Condition 5 is the Phase 3 driver's to establish**, and its outcome is appended to `run-record.md` §6 and reported in `joern-probe.md`. The run is complete only if all six hold together.

## 9. Provenance

| Source | What came from it |
|---|---|
| `harness/artifacts/logs/*` | every execution state, timestamp, elapsed time, exit code and `exit_status`; the feed evidence; the taint and AI-path observations; the datadog ruleset-provenance and rule-count facts in §2.9 |
| `harness/artifacts/raw/*` | every artifact shape, record count, row count, reject count and reject reason, and Dependency-Check's own `dataSources` block |
| `harness/ENVIRONMENT.md` | the recorded tool versions, the recorded ruleset identity restated in §2.9, the Opengrep taint setting, the datadog AI-path availability and its credential-source variable names, the feed descriptions and the per-module JAR outcomes |
| `git` reads of `$SPARK_SRC` | the commit date stated beside the counts |

No count, rule id, CVE, CWE, line number or timestamp in this file was invented or carried
over from a plan, and no credential value appears in it: where diagnostic output is needed
this file cites the specific log **by path and line range** rather than quoting it, in three
forms and no others. A reference to a log as a whole is written `<path>:1-<last>`, where `<last>` is
that log's own line count, so the range is checkable with `wc -l`. A reference to one recorded
statement inside a log is written `<path>:<line>`, and that line lies inside the same count.
An empty log is stated as *(empty — 0 lines)* and is given no range at all, because a range
over an empty file would be a number tracing to nothing. The invocation lines above
name each runner relative to the directory that holds `harness/`; the byte-preserved
`<tool>.meta.json` beside each log carries the absolute path as it stood at execution time
and was not edited. Both artifact trees are excluded from the commit by the pre-existing
`.gitignore` line 31 (`artifacts/`), so they are preserved on disk beside this record rather
than inside git.
