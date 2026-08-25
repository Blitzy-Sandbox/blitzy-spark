# Tool status — the nine scanners, as they ran

One entry per canonical tool identifier, all nine present. This document and
`oss-scan-results/severity-map.md` are the authoritative inventory of the nine
tools, because `oss-scan-results/findings.json` and `oss-scan-results/findings.csv`
are row-only: one record per finding, twelve fields, no metadata envelope. A tool
that produced no row is invisible in those two files by construction, so the
inventory has to live here. Two of the nine are in exactly that position —
`osv-scanner`, which wrote no artifact at all, and `dependency-check`, which wrote
an artifact carrying zero finding records — and both hold a full entry below.

Nothing here judges a finding. No finding is called real, important, a false
positive or a duplicate of another tool's; nothing is deduplicated across tools;
and no sentence ranks the tools, contrasts their coverage or explains why one tool
reported something another did not. Two tools reporting the same location produce
two rows in the dataset and no comment anywhere.

## Where this document sits in the pipeline

It is **rendered from** `harness/artifacts/logs/runner-metadata.json` **joined
with** the normalization results. It is an **output** of the pipeline and never an
input to it: `harness/lib/normalize/paths.py` reads `runner-metadata.json`, not
this file, and `harness/lib/normalize/cli.py` never reads this file at all. That
direction is what keeps the two from being circular — the path resolver needs a
scan root and a path base before it can resolve a single path, while a parse
status, a record count, a reconciliation result and a fixture outcome cannot exist
until normalization has produced them.

Every figure below is cited from the file that measured it. Where two documents
carry the same number it is one measurement cited twice, never two measurements.

| Input | What is taken from it |
| --- | --- |
| `harness/artifacts/logs/runner-metadata.json` | Per-tool script classification, scan-target variable and the value set into it, resolved scan root, invocation form, working directory, path base, JDK major, interpreter path and version, baked flags, credential-reporting expression, argument guard, artifact filename |
| `harness/artifacts/logs/normalize-run.json` | The authoritative per-artifact parsed and rejected counts, the routing decision and its detection evidence, every reconciliation assertion with its result, and the row-validation and output-comparison results |
| `harness/artifacts/logs/<tool>.status` | The invocation's own outcome: exit code, elapsed seconds, artifact byte size, scan root and its source, plus the authored fields each runner's lane recorded beside them |
| `harness/artifacts/logs/<tool>.stdout.log`, `<tool>.stderr.log` | The tool's own words, verbatim: reduced-reach conditions, and for an absent artifact the stated reason |
| `harness/artifacts/logs/adapter-tests-run.json` | The per-tool adapter-fixture result |

The status filename is `<tool>.status`, the name the plan specifies, and
deliberately not the harness precedent's `<tool>.meta.json`.

## The inventory

Nine canonical identifiers, in the processing order the normalizer uses. This is
an inventory of what each tool did, and it is not a comparison: nothing here ranks
the tools, contrasts their coverage or reads one tool's figure against another's.
Each row is expanded into a full entry below.

| tool | scanner_class | exit code | artifact | parse status | dataset rows | rejected |
| --- | --- | --- | --- | --- | --- | --- |
| `opengrep` | sast | 0 | `opengrep.sarif` | clean | 1,322 | 0 |
| `semgrep` | sast | 0 | `semgrep.sarif` | clean | 1,162 | 0 |
| `datadog-static-analyzer` | sast | 0 | `datadog-static-analyzer.sarif` | clean | 6,832 | 0 |
| `gitleaks` | secret | 2 | `gitleaks.json` | clean | 1 | 0 |
| `checkov` | misconfig | 1 | `checkov.json` | clean | 6 | 0 |
| `trivy` | per record | 0 | `trivy.json` | clean | 3 | 0 |
| `osv-scanner` | vuln | 128 | **none written** | absent | 0 | not applicable |
| `dependency-check` | vuln | 0 | `dependency-check.json` | clean | 0 | 0 |
| `joern` | sast | 0 | `joern.json` | partial | 107 | 585 |

Row counts and rejection counts are `normalize-run.json`
`totals.rows_by_tool` and `totals.rejections_by_tool`; exit codes and artifacts
are each tool's `<tool>.status`. The two tools with zero rows —
`osv-scanner` and `dependency-check` — are the reason this document exists, and
their zeros mean different things: one wrote no artifact, the other wrote an
artifact carrying no finding record.

## Artifact status and exit status are independent

This is the most commonly mis-read pair in the record, so it is stated before any
entry rather than left to be inferred from them.

| Condition | Status | Consequence |
| --- | --- | --- |
| Artifact present, every record parsed | `clean` | The exit code is recorded as a fact and used for nothing else |
| Artifact present, some records rejected | `partial` | Every parsable record is emitted, each rejection counted under its named class, and any parser error retained verbatim |
| Artifact present but matching no known shape | `failed` | The run **halts** |
| Artifact absent **and** the tool stated a no-work reason in its own output | `absent` | The stderr is quoted verbatim, zero rows are emitted, and the run continues |
| Artifact absent with **no** stated reason | — | The run **halts**, including a termination that produced no exit code |

Which branch each of the nine took: **`clean` seven times** — `opengrep`,
`semgrep`, `datadog-static-analyzer`, `gitleaks`, `checkov`, `trivy` and
`dependency-check`; **`partial` once** — `joern`, 585 records rejected under one
named class; **`absent` once** — `osv-scanner`, with a reason of its own quoted
verbatim in its entry. **`failed` never**: all eight artifacts that were written
matched a known shape, so the unknown-shape halt was not engaged. The
missing-artifact halt was likewise not engaged: the one absent artifact came with
the tool's own stated reason.

Three consequences of that table govern the entries below.

**A valid artifact is never suppressed because its runner exited non-zero.**
`gitleaks` exited 2 and `checkov` exited 1, and both are expected to exit non-zero
precisely because they found something; both artifacts were written and both
parse.

**`exit_status: timeout` is the single recorded name for a termination that
produced no exit code.** It records how a process ended and excuses nothing: a
termination leaving neither a parsable artifact nor a reason of the tool's own
still falls under the missing-artifact halt. No invocation in this run took that
branch. Every one of the nine ended with its own exit code, so every entry below
carries an `exit code` field and none carries an `exit_status` field.

**Exit 78 is not a missing artifact.** `harness/lib/scope.sh` defines
`scope_fail` to print `CONFIGURATION FAULT: <message>` to stderr and exit 78
(`EX_CONFIG`). A runner that ends that way has named its fault, which makes it a
configuration fault to correct at the gate rather than an unexplained absence. No
runner took that path in this run.

## Dataset-level reconciliation

The identity is `raw finding records = dataset rows for that tool + rejected
records`, and the left side comes from a traversal that walks the count units and
builds no rows. Every per-tool identity is stated in that tool's entry; the two
assertions every entry carries are the per-artifact identity and the dataset-level
sum it contributes to.

| Assertion | Figures | Result | Source |
| --- | --- | --- | --- |
| Dataset-level sum of the per-artifact identities | `10018 = 9433 + 585` | pass | `normalize-run.json` `reconciliation.stage_b` |
| Parsed `findings.json` rows against the dataset's emitted rows | `9433` against `9433` | pass | `normalize-run.json` `reconciliation.stage_c[0]` |
| Parsed `findings.csv` rows against the dataset's emitted rows | `9433` against `9433` | pass | `normalize-run.json` `reconciliation.stage_c[1]` |
| Parsed `findings.json` rows against parsed `findings.csv` rows | `9433` against `9433` | pass | `normalize-run.json` `reconciliation.stage_c[2]` |

The JSON and CSV row counts are asserted **separately** rather than one being
inferred from the other, and then compared to each other as a third assertion.
Both files were parsed to obtain them; neither figure comes from counting physical
lines. Field-for-field comparison under typed coercion passed over 9,433 rows and
113,196 fields with no first mismatch
(`normalize-run.json` `output_comparison`).

Row validation passed over all 9,433 emitted rows with zero violations: every row
carries exactly the twelve fields in order, `path` and `severity_norm` are never
absent, absence appears only in `severity_native`, `start_line`, `cwe`, `cve` and
`package_coordinate`, and no emitted path is absolute
(`normalize-run.json` `outputs.row_validation`). Each entry below states that
result as it applies to that tool's own rows.

Eight artifacts were present and one absent. `osv-scanner`'s reconciliation entry
is the literal `not applicable — artifact absent`, which is not a
zero-equals-zero pass: no artifact was written, so there is nothing to traverse
and no identity to assert. `dependency-check` is the different case that reads
similarly — its artifact **is** present and its identity **is** asserted, at
`0 = 0 + 0`.

## Facts common to all nine

Established once, from the provisioned files, and true of every entry below.

- **Invocation discipline.** Each runner was invoked directly, with no arguments,
  one tool at a time, and never through an orchestrator. `harness/bin/` holds
  exactly the nine runners the class table names — nine entries, nine runners,
  no helper and no orchestrator (`runner-metadata.json`
  `harness_bin_inventory_summary`). The harness's non-runner helpers live outside
  that directory, in `harness/lib/`.
- **Argument guard.** Every runner's guard is its first executable statement and
  exits 64, ahead of the environment sourcing, the shared-library sourcing, the
  target resolution and the tool invocation. Each was established by
  **inspection** of the script; no rejection probe was run against any runner,
  because a blind probe against a defective runner could perform a real scan and
  contaminate the artifact tree.
- **Scan target.** Every runner resolves its target through `scope_resolve_target`
  in `harness/lib/scope.sh`, which reads `SPARK_SRC`. The variable in force was
  `SPARK_SRC=/opt/spark-src`, exported by `harness/env.sh` line 43; setting a
  variable a runner is written to consume is not runner reconfiguration, and no
  runner file and no baked flag was edited. The resolved scan root was
  `/opt/spark-src` with source `SPARK_SRC` for all nine, and
  `git -C /opt/spark-src rev-parse HEAD` is
  `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`, the pin.
- **Smoke override.** `HARNESS_SMOKE_TARGET` was unset throughout, which is why
  every `scan_root_source` reads `SPARK_SRC` rather than naming the override. Set,
  it would have redirected every runner at one small directory while still
  reporting success.
- **Scope.** `harness/scope/allowlist.txt`, sha256
  `0013edf6cdc3a48d69aed5d7db41cc6647cfd461d348f5e1d563ba85664143d1`, twelve
  globs expanding to 18 existing directories. Eight of the nine runners derive
  their own target set from that expansion; `joern` is the exception, its input
  being the graph. Whatever a runner walked, the `in_scope` field of every row is
  decided by the allowlist alone, and the file stayed byte-exact.
- **Credential reporting.** `scope_cred_state` (`harness/lib/scope.sh` lines
  105–109) uses the `${VAR:+set}` form only and prints exactly `set` or `absent`.
  The form whose `:-` arm expands to the variable's own value when the variable is
  set — which would write a live credential into a log this pipeline preserves
  verbatim — occurs in **no** executable expression in any runner or library
  (`runner-metadata.json` `gate_stage.credentials_observed`
  `unsafe_expression_form_present_anywhere: false`). Every credential below is
  reported as a boolean; no credential value appears anywhere in this document.
- **Credential presence.** `SEMGREP_APP_TOKEN`, `DD_API_KEY`, `DD_APP_KEY`,
  `NVD_API_KEY`, `BC_API_KEY` and any Sonatype OSS Index credential were all
  **absent**, as expected. `GITHUB_TOKEN` was present in the ambient environment
  for downloads, is read by no runner and printed by none, so no runner could
  expose it and no halt arose.
- **No time limit.** Elapsed times are recorded as facts. Every entry states the
  expected value against the observed one and stops there; no figure is read as
  late, slow or over budget.
- **Expected values.** The expected exit code, elapsed time and finding count for
  each tool come from the request's expected-values table, which
  `harness/ENVIRONMENT.md` section 9 and its inlined-values block state
  identically — so no field below has a table figure and a record figure pulling
  against each other. Where the two would have differed, the table governs.

---

## opengrep

`scanner_class`: **sast**, fixed for this tool.

| Field | Value |
| --- | --- |
| Version | observed **1.27.1**, expected 1.27.1 — as expected. `opengrep --version` printed it; the binary is `/opt/blitzy-tools/bin/opengrep`, and the artifact's own SARIF driver reads `Opengrep OSS` `1.27.1` |
| Ruleset identity | observed **commit `f1d2b562b414783763fd02a6ed2736eaed622efa`** at `/opt/blitzy-harness/rules/opengrep-rules`, expected the same commit — **matches**. 2,006 rules observed against 2,006 expected |
| Comparability | **comparable** — the observed ruleset identity is the expected identity, so no not-comparable status attaches to this tool's count |
| Rule-count unit | 29 rule-bearing `--config` directories, which is 58 argv tokens at two per directory. Both figures are recorded with their unit named rather than one being read as a contradiction of the other |
| Feed state | not applicable — this tool consults a pinned local ruleset checkout rather than a feed. `fetched_at_scan_time: false`, so there is no reproducibility gap of that kind |
| Exit code | **0**, expected 0 — as expected. The runner exits with the tool's own code and does not transform it |
| Elapsed | expected **929 s**, observed **944 s** — recorded, both values. Cross-checked independently: `finished_at` minus `started_at` is also 944 s |
| Finding count | expected **1,322**, observed **1,322** — as expected. The count unit is `runs[].results[]` |
| Output format | SARIF 2.1.0, artifact `harness/artifacts/raw/opengrep.sarif`, 73,840,948 bytes |
| Parse status | **clean** |
| Records parsed / rejected | 1,322 parsed, **0 rejected**. No rejection class was engaged and no parser error was raised |
| Reconciliation, per artifact | `1322 = 1322 + 0` — **pass** |
| Reconciliation, dataset level | contributes to `10018 = 9433 + 585` — **pass** |
| Row validation | pass; the 1,322 rows carry exactly the twelve fields, no absent `path` or `severity_norm`, and no absolute path |
| Adapter fixture | **pass** — the shared SARIF adapter, `test_sarif_adapter`, exit 0 |
| Scan-target variable | `SPARK_SRC`, set to `/opt/spark-src`. Resolved indirectly: the runner sources `harness/lib/scope.sh` and calls `scope_resolve_target`, which reads the variable and exports `SCAN_ROOT` |
| Resolved scan root | `/opt/spark-src`, verified |
| Invocation form | one invocation, the 18 root-relative allowlist directories passed together |
| Working directory | `/opt/spark-src` (`cd "$SCAN_ROOT"`, runner line 50), equal to the scan root |
| Path base | **scan root**, `/opt/spark-src`. Established from the invocation and the working directory rather than from the artifact |
| JDK major | none — no JVM is involved; the PATH entry is a native ELF executable and the runner exports no `JAVA_HOME` |
| Interpreter | none — not a Python-hosted tool, and the runner invokes no interpreter |
| Credential expression | `printf 'credential      : SEMGREP_APP_TOKEN=%s (unused by opengrep)\n' "$(scope_cred_state SEMGREP_APP_TOKEN)"` at runner line 46; prints a fixed token only. `SEMGREP_APP_TOKEN` **absent** |

**Baked flags, as read** from `harness/bin/run-opengrep.sh` lines 64–70:
`scan`, one `--config` per rule-bearing ruleset directory, `--sarif`,
`--sarif-output`, `--timeout 120`, `--timeout-threshold 0`,
`--max-target-bytes 20000000`, `--x-ignore-semgrepignore-files`,
`--disable-version-check`, then the 18 target directories. None of these appears
in the expected-values table, so none is an anchor; nothing was added, removed or
forced. `--disable-version-check` is the only update-suppression flag in the set.
`opengrep scan` has no `--metrics` flag — passing one errors with
`unknown option` — so this flag set is deliberately not a copy of the other SAST
runner's.

**SARIF base resolution.** The engine sets `uriBaseId` `%SRCROOT%` on every result
but emits no `run.originalUriBaseIds`, so the SARIF 2.1.0 §3.4.4 resolution
procedure cannot complete. The runner metadata therefore supplies the explicit
base recorded above, which is the condition under which the documented
degenerate-base fallback applies. Without it every row from this artifact would
have had to be rejected under `unresolvable_path`.

**Reduced-reach conditions, in the tool's own words**, from
`harness/artifacts/logs/opengrep.stderr.log`:

```
  Scanning 4095 files tracked by git with 2006 Code rules:
Some files were skipped or only partially analyzed.
  Scan was limited to files tracked by git.
  Partially scanned: 46 files only partially analyzed due to parsing or internal Opengrep errors
Ran 1138 rules on 4095 files: 1322 findings.
```

`--x-ignore-semgrepignore-files` is load-bearing for that reach rather than
cosmetic: without it the engine's bundled ignore patterns skip 846 of the 4,095
in-scope files, 834 of them `python/pyspark` test modules the allowlist puts
squarely in scope. The flag is marked internal by the tool, which is accepted at a
pinned version and is the subject of the first warning line in that stream.

**Absent-artifact stderr and verdict**: not applicable — the artifact is present
and parses.

**Second appearance.** Opengrep appears twice in this run by design: here as one
of the nine scanned runners, and separately as the subject of the taint A/B, whose
two arms are written to `harness/artifacts/logs/taint-ab-on.{log,sarif}` and
`harness/artifacts/logs/taint-ab-off.{log,sarif}` — outside
`harness/artifacts/raw/` so neither can overwrite this runner's artifact. That
A/B **contributes no dataset row**, and none of its findings is folded into the
1,322 above; doing so would corrupt both this tool's count and the dataset total.
The A/B's own result is recorded in those two files and in
`oss-scan-results/run-record.md`, which own it.

---

## semgrep

`scanner_class`: **sast**, fixed for this tool.

| Field | Value |
| --- | --- |
| Version | observed **1.173.0**, expected 1.173.0 — as expected. Two witnesses: `semgrep --version` at the gate and again on re-verification, and the artifact's own SARIF driver `Semgrep OSS 1.173.0`. The precedent provisioning carried 1.174.0, which is the named case for recording both values and continuing; that difference did not recur here, and both values are recorded so the match is a checked fact rather than an assumption |
| Ruleset identity | observed **commit `40b8c63f75dc7c22c8a77482d73bfb864b146f7e`** at `/opt/blitzy-harness/rules/semgrep-rules`, expected the same commit — **matches**. 2,149 rules observed against 2,149 expected, with 19 Pro-only rules skipped |
| Comparability | **comparable** — observed identity equals expected identity |
| Rule-count unit | 30 rule-bearing `--config` directories, 60 argv tokens at two per directory. Both recorded with the unit named |
| Feed state | not applicable — a pinned local ruleset checkout, not a feed. `fetched_at_scan_time: false`; no reproducibility gap of that kind |
| Exit code | **0**, expected 0 — as expected. No `--error` flag is baked in, so this engine's success code is not turned non-zero by the findings it reported. Cross-checked against the artifact's own `executionSuccessful: true` |
| Elapsed | expected **449 s**, observed **621 s** — recorded, both values, from the runner's own timer |
| Finding count | expected **1,162**, observed **1,162** — as expected. Count unit `runs[].results[]`; the tool's own stderr reports `Findings: 1162 (1162 blocking)` |
| Output format | SARIF 2.1.0, artifact `harness/artifacts/raw/semgrep.sarif`, 40,661,229 bytes |
| Parse status | **clean** |
| Records parsed / rejected | 1,162 parsed, **0 rejected**. No rejection class engaged, no parser error |
| Reconciliation, per artifact | `1162 = 1162 + 0` — **pass** |
| Reconciliation, dataset level | contributes to `10018 = 9433 + 585` — **pass** |
| Row validation | pass over this tool's 1,162 rows |
| Adapter fixture | **pass** — the shared SARIF adapter, `test_sarif_adapter`, exit 0 |
| Scan-target variable | `SPARK_SRC`, set to `/opt/spark-src`, resolved through `scope_resolve_target` at runner line 28. The target comes from the environment, never from the working directory |
| Resolved scan root | `/opt/spark-src`, verified |
| Invocation form | one invocation, the 18 root-relative allowlist directories passed together |
| Working directory | `/opt/spark-src` (`cd "$SCAN_ROOT"`, runner line 47) |
| Path base | **scan root**, `/opt/spark-src`, established from the invocation and working directory |
| JDK major | none — the runner exports no `JAVA_HOME` and the tool is Python-hosted |
| Interpreter | `/opt/blitzy-tools/venvs/semgrep/bin/python`, reporting **3.13.7** against an expected 3.13.7 — matches. Its real path is `/usr/bin/python3.13`. This virtualenv is semgrep's alone |
| Credential expression | `printf 'credential      : SEMGREP_APP_TOKEN=%s\n' "$(scope_cred_state SEMGREP_APP_TOKEN)"` at runner line 45; fixed token only. `SEMGREP_APP_TOKEN` **absent** |

**Baked flags, as read** from `harness/bin/run-semgrep.sh` lines 58–65:
`scan`, one `--config` per rule-bearing ruleset directory, `--sarif`,
`--sarif-output`, `--metrics=off`, `--disable-version-check`, `--timeout 120`,
`--timeout-threshold 0`, `--max-target-bytes 20000000`,
`--x-ignore-semgrepignore-files`, `--oss-only`, then the 18 target directories.
None appears in the expected-values table, so none is an anchor.

**Edition.** Community Edition under `--oss-only` with no token attached, so its
Pro and interfile analysis were unavailable for this invocation. That is recorded
as a configuration fact about how this tool was set up, and it is expressly not a
basis for any comparison.

**SARIF base resolution.** As with the other `%SRCROOT%` producer, `uriBaseId` is
set on every result with no `run.originalUriBaseIds` emitted, so the
specification's resolution cannot complete and the recorded base above is the one
available.

**Reduced-reach conditions, in the tool's own words**, from
`harness/artifacts/logs/semgrep.stderr.log`:

```
  Scanning 4094 files tracked by git with 2149 Code rules:
✅ Scan completed successfully.
 • Findings: 1162 (1162 blocking)
 • Rules run: 1238
 • Targets scanned: 4094
 • Parsed lines: ~99.9%
 • Scan was limited to files tracked by git
Ran 1238 rules on 4094 files: 1162 findings.
```

**Values that could not be established.** `started_at` and `finished_at` for this
invocation are **not established**, and are named rather than filled with a
plausible pair. The reason is structural: this tool prints its SARIF document to
stdout, so the runner gave stdout to
`harness/artifacts/logs/semgrep.stdout.log` and the console header and trailer
`scope_finish` would have written went to the runner's own console stream, which
was not captured to a file for this tool. The whole of that stdout log was
searched, not only its head and tail: the marker `elapsed seconds` occurs zero
times, and the artifact emits no `startTimeUtc` or `endTimeUtc`. What **is**
established is that the window is exactly 621 seconds long and closed no later
than the commit that recorded the evidence. Each quantity a trailer would have
carried is cross-checked against an independent measurement instead: the exit code
against the artifact's `executionSuccessful` and the tool's own summary, the
elapsed seconds against `scope_finish`'s arithmetic, and the artifact byte count
against a `stat` taken in the checkout.

**Absent-artifact stderr and verdict**: not applicable — the artifact is present
and parses.

---

## datadog-static-analyzer

`scanner_class`: **sast**, fixed for this tool.

| Field | Value |
| --- | --- |
| Version | observed **0.9.1**, revision `f76636e43554f7f9a8e3984a31d03ec8dea5489f`; expected 0.9.1 revision `f76636e4` — as expected, the observed revision's first eight characters being the abbreviated revision the expected value names. Read from the tool's own Configuration block and corroborated by the SARIF driver version. The release tag carries no leading `v`: `tags/0.9.1` resolves and `tags/v0.9.1` is a 404 |
| Ruleset identity | observed **sha256 `4f397e81414f8e9469d20abc18c80c85c722e72b9f85b8bcf69dbe34b8fef6f1`** at `/opt/blitzy-harness/rules/datadog/datadog-sast-rules.json`; expected **sha256 `e70ede308813b6d8c4087b0995609cdafdb9ab48159a313fe58ac343ff6c44f7`** — **DIFFERS**. Both values are recorded. Every comparable measure matches: 48 rulesets observed against 48 expected, and 1,093 rules observed against 1,093 expected, measured directly from the file |
| Comparability | **NOT COMPARABLE WITH THE REHEARSAL.** The ruleset digest differs from the expected identity, and a different rule set produces a different count for reasons that have nothing to do with the code. This tool's finding count must not be read against the rehearsal's figure even though the ruleset and rule counts match. The same status is carried in `oss-scan-results/severity-map.md` |
| Feed state | not applicable as a feed — the rules are a captured local file. `fetched_at_scan_time: false`, proven by the tool's own `config method : none (no local file and no remote configuration)` alongside `-r` pointing at the captured file, so **no API call was made for rules at scan time** and this invocation contributes no reproducibility gap of that kind |
| Exit code | **0**, expected 0 — as expected. The runner captures the tool's own code at line 53 and exits with it unchanged |
| Elapsed | expected **57 s**, observed **223 s** — recorded, both values. Two independent checks: `finished_at` minus `started_at` is 223 s, and the tool's own inner measurement reports `Duration: 220.936s`, 2.064 s below the runner's wall clock |
| Finding count | expected **6,832**, observed **6,832** — as expected. Taken from the parsed artifact's `runs[0].results` length, which equals the tool's own `Total violations: 6832` |
| Output format | SARIF 2.1.0, artifact `harness/artifacts/raw/datadog-static-analyzer.sarif`, 5,671,091 bytes |
| Parse status | **clean** |
| Records parsed / rejected | 6,832 parsed, **0 rejected**. No rejection class engaged, no parser error |
| Reconciliation, per artifact | `6832 = 6832 + 0` — **pass** |
| Reconciliation, dataset level | contributes to `10018 = 9433 + 585` — **pass** |
| Row validation | pass over this tool's 6,832 rows |
| Adapter fixture | **pass** — the shared SARIF adapter, `test_sarif_adapter`, exit 0 |
| Scan-target variable | `SPARK_SRC`, set to `/opt/spark-src`, resolved through `scope_resolve_target` |
| Resolved scan root | `/opt/spark-src`, verified; the tool's own Configuration block records `source directory : /opt/spark-src` |
| Invocation form | one invocation: `-i` takes the absolute scan root and 18 `-u` restrictions confine the walk to the in-scope subdirectories |
| Working directory | `/opt/spark-src` (`cd "$SCAN_ROOT"`, runner line 43) |
| Path base | **scan root**, `/opt/spark-src`. This producer emits **no** `uriBaseId` at all and no `run.originalUriBaseIds`, so there is no base map to walk and the recorded base is the only anchor; its SARIF `uri` values are plain relative references against that root |
| JDK major | none — a native ELF executable, and the runner exports no `JAVA_HOME` |
| Interpreter | none — not a Python-hosted tool |
| Credential expression | `printf 'credential      : DD_API_KEY=%s DD_APP_KEY=%s\n' "$(scope_cred_state DD_API_KEY)" "$(scope_cred_state DD_APP_KEY)"` at runner lines 39–40. Both values come from `scope_cred_state`, so the line prints exactly `set` or `absent` per variable. **`DD_API_KEY` absent, `DD_APP_KEY` absent** — booleans only, and no value appears here or in any log |

**Baked flags, as read** from `harness/bin/run-datadog-static-analyzer.sh` lines
46–52: `-i "$SCAN_ROOT"`, one `-u` per allowlist directory,
`-r "$DD_SAST_RULES_FILE"`, `-f sarif`, `-o "$ART"`,
`--enable-static-analysis true`, `--enable-secrets false`. None appears in the
expected-values table, so none is an anchor.

**Credential safety, stated because this is the stream where it matters most.**
This is the one runner where the precedent's `${VAR:+set}${VAR:-absent}` form
would have appeared. It does **not** appear here: both variables are reported
through `scope_cred_state`, whose set-arm-only form cannot echo a value. That form
is safe only while a variable is unset — with a value present its `:-` arm expands
to the variable's own value, into a log this pipeline preserves verbatim. Had a
prohibited credential been unexpectedly present and had the unmodifiable runner
been going to expose it, the run would have halted rather than invoking that
runner. Both variables were absent, so the question did not arise. The secrets
scanner was disabled by `--enable-secrets false`, which the tool confirms in its
own words as `secrets enabled         : false`, and with both keys absent this
tool's credentialed paths stayed disabled for this invocation. That is a recorded
configuration fact and not a basis for any comparison.

**Reach conditions, in the tool's own words**, from
`harness/artifacts/logs/datadog-static-analyzer.stdout.log`:

```
#static analysis rules  : 1093
rules languages         : javascript,typescript,go,kotlin,bash,java,rust,php,python,ruby,c#,dart
Analyzing 28 JavaScript files using 138 rules
Analyzing 2 Bash files using 35 rules
Analyzing 1149 Python files using 131 rules
Analyzing 591 Java files using 109 rules
  Files scanned: 4085
  Rules evaluated: 1093
  Rules with matches: 96
```

The `rules languages` line above is the pinned ruleset's own language list as the
tool printed it, and Scala is not among the twelve. The `Analyzing` lines are the
same fact from the other direction: the languages this invocation analysed were
JavaScript, Bash, Python and Java. This is invisible from the finding count alone,
which is why it is recorded here in the tool's own output rather than summarised.

**Absent-artifact stderr and verdict**: not applicable — the artifact is present
and parses. `harness/artifacts/logs/datadog-static-analyzer.stderr.log` is 0
bytes, which is immaterial because a stated reason is what an absent artifact
would need.

---

## gitleaks

`scanner_class`: **secret**, fixed for this tool.

| Field | Value |
| --- | --- |
| Version | observed **8.30.1**, expected 8.30.1 — as expected. `gitleaks version` printed it |
| Ruleset identity | observed **default rule set built into gitleaks 8.30.1**, expected the same — **matches**. The runner states it at line 39 and configures no external ruleset path |
| Rule count | **not established.** The rule set is not separately versioned, the tool does not report a count, and the expected-values table carries none. Named rather than omitted, and no count was invented |
| Ruleset digest | **none exists to compare**, the rules being compiled into the binary; none was invented |
| Comparability | **comparable** — the observed ruleset identity is the expected identity |
| Feed state | not applicable — no feed. `fetched_at_scan_time: false`; the rules are in the binary, so there is no scan-time fetch and no reproducibility gap of that kind |
| Exit code | **2**, expected 2 — as expected. Per invocation 0 means no leaks and 2 means leaks found, and both are successful scans; the runner keeps the worst code across its invocations. Cross-checked two ways: the 18 per-invocation lines carry seventeen exits of 0 and one of 2, and the tool's own stderr warns that it found one leak |
| Elapsed | expected **15 s**, observed **69 s** — recorded, both values, from the runner's own timer. The two printed timestamps stand 68 s apart because the timer starts before the header, which computes the allowlist digest and expands the allowlist; the runner's 69 is carried unchanged rather than reconciled |
| Finding count | expected **1**, observed **1** — as expected. The runner's own merge step records `merged 1 findings from 18 per-directory reports` |
| Output format | native JSON array, artifact `harness/artifacts/raw/gitleaks.json`, 561 bytes |
| Parse status | **clean** |
| Records parsed / rejected | 1 parsed, **0 rejected**. No rejection class engaged, no parser error |
| Reconciliation, per artifact | `1 = 1 + 0` — **pass** |
| Reconciliation, dataset level | contributes to `10018 = 9433 + 585` — **pass** |
| Row validation | pass over this tool's 1 row |
| Adapter fixture | **pass** — `test_gitleaks_adapter`, exit 0 |
| Scan-target variable | `SPARK_SRC`, set to `/opt/spark-src`, resolved through `scope_resolve_target` at runner line 33 |
| Resolved scan root | `/opt/spark-src`, verified |
| Invocation form | **18 invocations, each handed exactly one root-relative path.** `gitleaks dir` takes exactly one path and silently falls back to the working directory when handed more, which is why the runner loops instead of passing the directory list |
| Working directory | `/opt/spark-src` (`cd "$SCAN_ROOT"`, runner line 47), entered before the loop and unchanged throughout |
| Path base | **scan root**, `/opt/spark-src`; the record field is `File`. Because cwd is the scan root and each invocation receives one root-relative directory, every `File` value is root-relative to that root. The runner prints the base itself: `path base       : /opt/spark-src (root-relative paths; cwd is the scan root)` |
| JDK major | none — a statically linked native executable, and the runner exports no `JAVA_HOME` |
| Interpreter | `/usr/bin/python3`, reporting **3.13.7** against an expected 3.13.7 — matches. Post-processing only: the runner calls it once, at lines 66–78, to concatenate the 18 per-directory JSON arrays into the single artifact. The scanner itself uses no interpreter |
| Credential expression | **none.** This runner calls `scope_cred_state` nowhere and reads no credential in this configuration. Recorded as none rather than as an empty value |

**Baked flags, as read** from `harness/bin/run-gitleaks.sh` lines 53–56:
`dir <one root-relative path>`, `--report-format json`, `--report-path`,
`--redact=100`, `--no-banner`, `--no-color`, `--log-level warn`, `--exit-code 2`.
None appears in the expected-values table, so none is an anchor.

**Invocation form, recorded as read rather than as expected.** This provisioning
loops one path per invocation. The alternative shape — all eighteen paths handed
to a single `gitleaks dir` call — is the one that would have made the path base
the process working directory rather than the scan root, and a wrong base there
silently mis-resolves every row this tool produces. The base above is derived from
the invocation and the working directory that were actually read from the runner,
not from either expectation.

**No severity vocabulary.** This tool defines none, so `severity_native` is
**absent** on its row and `severity_norm` takes `Info` with the absence stated
rather than a level assumed. That is one of the two rows in the whole dataset with
an absent `severity_native`.

**Redaction.** `--redact=100` is baked in, which the runner also prints, so the
`Secret` and `Match` fields come through redacted. No matched secret value reaches
the artifact, this document or any dataset field.

**Reduced-reach conditions, in the tool's own words.**
`harness/artifacts/logs/gitleaks.stderr.log` is 27 bytes and carries exactly one
line, which is the tool reporting what it found rather than a condition about its
reach:

```
11:26PM WRN leaks found: 1
```

That prefix is gitleaks' own clock format and reads `11:26PM` against a run window
of 22:36:36 to 22:37:44; it is recorded as observed, since no value here is taken
from it and the count it states agrees with the merge line. **No reduced-reach
condition was reported by this tool.**

**Absent-artifact stderr and verdict**: not applicable — the artifact is present
and parses.

---

## checkov

`scanner_class`: **misconfig**, fixed for this tool.

| Field | Value |
| --- | --- |
| Version | observed **3.3.12**, expected 3.3.12 — as expected, on two witnesses: `checkov --version`, and the report's own `summary.checkov_version` from the invocation itself. The precedent carried 3.3.13; a difference would have been recorded with both values and the run continued, never corrected by installing |
| Policy identity | observed **policies bundled with checkov 3.3.12**, expected bundled with 3.3.12 — **matches**. The runner states it at line 37 |
| Policy count | **not established.** The policies are not separately versioned and the tool does not report a count; the expected-values table carries none. Named rather than omitted |
| Policy digest | **none** — bundled policies carry no separate version or digest, and none was invented |
| Comparability | **comparable** — the policy identity is the bundled set of the expected version, so no digest difference marks this tool's counts non-comparable |
| Feed state | **not attempted.** `--skip-download` is baked into the invocation, so no policy metadata and no external module was fetched. Of the four outcomes — attempted and succeeded, attempted and failed, not attempted, not reported — this is the third. No network fetch occurred at scan time, so this tool contributes no reproducibility gap of that kind |
| Exit code | **1**, expected 1 — as expected. A non-zero exit alongside an artifact that was written and parses is ordinary for this tool, which exits 1 because it found something |
| Elapsed | expected **88 s**, observed **136 s** — recorded, both values, from the runner's own timer, whose counter is the measurement of record against printed stamps 135 s apart |
| Finding count | expected **6**, observed **6** — as expected. The count unit is `results.failed_checks[]` in the shape that was written, and two independent paths agree: an enumeration of `failed_checks[]` that builds nothing returned 6, and the report's own `summary.failed` is 6 |
| Output format | native JSON, **object form** — one top-level report object with keys `check_type`, `results` and `summary`, `check_type` `dockerfile`. Artifact `harness/artifacts/raw/checkov.json`, 8,380 bytes |
| Parse status | **clean** |
| Records parsed / rejected | 6 parsed, **0 rejected**. No rejection class engaged, no parser error |
| Reconciliation, per artifact | `6 = 6 + 0` — **pass** |
| Reconciliation, dataset level | contributes to `10018 = 9433 + 585` — **pass** |
| Row validation | pass over this tool's 6 rows |
| Adapter fixture | **pass** — `test_checkov_adapter`, exit 0 |
| Scan-target variable | `SPARK_SRC`, set to `/opt/spark-src`, resolved through `scope_resolve_target` at runner line 28 |
| Resolved scan root | `/opt/spark-src`, verified |
| Invocation form | one invocation carrying **18 `-d` target roots**, one per allowlist directory, each root-relative |
| Working directory | `/opt/spark-src` (`cd "$SCAN_ROOT"`, runner line 44). For this tool cwd is **not** the base of `file_path` |
| Path base | **per target directory**, under `/opt/spark-src`. With 18 `-d` roots in one invocation a record's `file_path` is relative to whichever `-d` directory matched and carries a leading slash; `repo_file_path` is root-relative with a leading slash and `file_abs_path` is filesystem-absolute. The resolver anchors on those two and reconciles against `file_abs_path` |
| JDK major | none — the runner exports no `JAVA_HOME` and the tool is Python-hosted |
| Interpreter | `/opt/blitzy-tools/venvs/checkov/bin/python`, reporting **3.13.7** against an expected 3.13.7 — matches, resolving through a symlink to `/usr/bin/python3.13`. This virtualenv is checkov's alone |
| Credential expression | `printf 'credential      : BC_API_KEY=%s (severities require a licence and stay absent)\n' "$(scope_cred_state BC_API_KEY)"` at runner line 40; fixed token only. **`BC_API_KEY` absent** |

**Baked flags, as read** from `harness/bin/run-checkov.sh` lines 46–49: one `-d`
per allowlist directory, `--skip-download`, `--quiet`, `--compact`,
`--output json`, `--output-file-path`. No `--framework` filter is passed, so all
bundled frameworks were enabled. None of these flags appears in the
expected-values table, so none is an anchor; `--skip-download` was neither added
nor forced by this run.

**Which of the two output shapes was written.** The **object** form. The
alternative — a top-level array of per-framework report objects, which this tool
emits when more than one framework reports — was **not** written, and the two are
mutually exclusive. That was determined by measurement over two independent
routes rather than assumed: byte-size discrimination against the recorded 8,380
bytes over the candidate serializations of this invocation's own report (the
single object compact is exactly 8,380; an array holding that one object is 8,382;
an array of all 18 stdout documents is 92,993; an array of the 11 dockerfile
documents is 92,202; the single object at indent 4 is 12,648 — only the object
form matches), and direct observation of a re-invocation with the same flags over
the same 18 directories, which wrote a file opening and closing with a brace,
8,380 bytes, top-level keys `check_type`, `results` and `summary`. The runner
copies the tool's `results_json.json` to the artifact path unchanged, so no field
was rewritten by the harness.

**Severity.** `severity` is **null per row** in this unlicensed configuration, so
`severity_native` is absent on all 6 rows and `severity_norm` takes `Info` with
the absence stated. Those 6 rows plus the single gitleaks row are the 7 rows in
the dataset with an absent `severity_native`.

**What is counted, and what is not.** Only `results.failed_checks[]` are findings.
`passed_checks` and `skipped_checks` are **neither counted nor emitted** — the
written report's `results` object carries `failed_checks` only — and the adapter's
own test asserts that a fixture containing passes and skips produces rows for
neither.

**`parsing_errors` as status evidence.** `parsing_errors` is **0**. It is reported
here as status evidence and never as findings.

**Reduced-reach conditions, in the tool's own words.** The report's own summary is
the tool's statement about what it covered:

```
"summary": {"passed": 201, "failed": 6, "skipped": 0, "parsing_errors": 0, "resource_count": 3, "checkov_version": "3.3.12"}
```

`harness/artifacts/logs/checkov.stderr.log` is 0 bytes, so **no reduced-reach
condition and no diagnostic was reported by this tool**.

**Absent-artifact stderr and verdict**: not applicable — the artifact is present
and parses.

---

## trivy

`scanner_class`: **per record — `vuln`, `secret` or `misconfig`**. This is the one
tool in the class table whose class is not fixed, which is why this entry carries
a per-section breakdown rather than a single class.

| Field | Value |
| --- | --- |
| Version | observed **0.74.0**, expected 0.74.0 — as expected. `trivy --version` printed `Version: 0.74.0`, recorded at the gate; resolved path `/opt/blitzy-tools/bin/trivy` |
| Feed identity | observed **vulnerability DB v2 `UpdatedAt=2026-08-24T06:55:32.451220873Z`** and **java DB v1 `UpdatedAt=2026-08-24T01:07:04.599776272Z`**; expected **vulnerability DB v2, 2026-08-23T06:56:50Z** and **java DB v1, 2026-08-23T01:05:59Z** — **DIFFERS**. Both database versions match (v2 and v1); both timestamps are one day later than expected. Both values are recorded |
| Comparability | **NOT COMPARABLE WITH THE REHEARSAL.** A feed one day newer resolves a different advisory set, so this tool's counts differ for reasons that have nothing to do with the code. The same status is carried in `oss-scan-results/severity-map.md` |
| Feed identity provenance | `harness/artifacts/logs/trivy.stdout.log` lines 9–10, where the runner dumps its cache database metadata before invoking — read from `$TRIVY_CACHE_DIR/db/metadata.json` and `$TRIVY_CACHE_DIR/java-db/metadata.json`. That is the only place this identity exists, and it is cited once here rather than measured again |
| Feed state | **not attempted.** The runner bakes `--skip-db-update`, `--skip-java-db-update` and `--skip-check-update`, so no refresh was attempted and the seeded caches were used as found. Of the four outcomes this is the third. `--offline-scan` is also baked in, so no dependency resolution against a remote registry occurs at scan time; there was no scan-time fetch and therefore no reproducibility gap of that kind |
| Exit code | **0**, expected 0 — as expected. All 18 per-directory invocations printed `exit=0`, and the runner keeps the worst non-zero code across them |
| Elapsed | expected **17 s**, observed **22 s** — recorded, both values. `finished_at` minus `started_at` is also 22 s |
| Finding count | expected **3**, observed **3** — as expected. Count unit: one element of one of `Results[].Vulnerabilities[]`, `Results[].Secrets[]` or `Results[].Misconfigurations[]` |
| Per-section counts | **Vulnerabilities 0, Secrets 0, Misconfigurations 3** — against an expected 0 / 0 / 3. The three sit in 3 `Results` members, all `Class` `config` and `Type` `dockerfile`. `0 + 3 + 0 = 3` closes against the record count |
| Output format | native JSON, `SchemaVersion` 2, `ArtifactName` `.`; artifact `harness/artifacts/raw/trivy.json`, 3,496 bytes |
| Parse status | **clean** |
| Records parsed / rejected | 3 parsed, **0 rejected**. No rejection class engaged, no parser error |
| Reconciliation, per artifact | `3 = 3 + 0` — **pass** |
| Reconciliation, dataset level | contributes to `10018 = 9433 + 585` — **pass** |
| Row validation | pass over this tool's 3 rows. `start_line` is **absent** on all three, which is legitimate: line information appears on secrets and misconfigurations where the section supplies it, and all three of these records carry a `CauseMetadata` with `Provider` and `Service` only |
| Adapter fixture | **pass** — `test_trivy_adapter`, exit 0 |
| Scan-target variable | `SPARK_SRC`, set to `/opt/spark-src`, resolved through `scope_resolve_target` at runner line 45 |
| Resolved scan root | `/opt/spark-src`, verified |
| Invocation form | **18 invocations**, each `trivy fs` handed exactly one root-relative path, because `trivy fs` takes exactly one path. The runner writes one per-directory report per invocation into `$HARNESS_LOG_DIR/trivy.parts/` and merges the 18 into one report. Those parts were written into the log tree of the checkout the runner ran in and are **not present in this checkout's `harness/artifacts/logs/`**, so nothing here is measured from them; every trivy figure above comes from `trivy.status`, `trivy.stdout.log` or the merged artifact |
| Working directory | `/opt/spark-src` (`cd "$SCAN_ROOT"`, runner line 61) |
| Path base | **scan root**, `/opt/spark-src`; the record field is the enclosing `Results[].Target`, refined by a per-record path or `StartLine` where the section supplies one. Each part states `Target` relative to its own single path argument and names it in its own `ArtifactName`; the merge prefixes every `Target` with that part's `ArtifactName` and sets the merged `ArtifactName` to `.`, so in the merged artifact every `Target` is root-relative. The per-directory parts are **not** root-anchored and would have to be read with per-section target semantics rather than with this base; that caveat is recorded from the runner's own merge step rather than from files a reader can open in this checkout |
| JDK major | none — a statically linked ELF binary, and the runner exports no `JAVA_HOME`. The "java DB" it consults is a vulnerability database for Java artifacts, not a Java runtime |
| Interpreter | `/usr/bin/python3`, reporting **3.13.7** against an expected 3.13.7 — matches. Post-processing only: the two database metadata reads and the merge. The scanner itself is a native binary |
| Credential expression | **none.** This runner calls `scope_cred_state` nowhere and prints no credential line, because it reads no credential in this configuration. Recorded as none rather than as an empty value |

**Baked flags, as read** from `harness/bin/run-trivy.sh` lines 67–71:
`fs <one root-relative path>`, `--scanners vuln,secret,misconfig`,
`--format json`, `--output <per-directory part>`, `--skip-db-update`,
`--skip-java-db-update`, `--skip-check-update`, `--offline-scan`,
`--no-progress`, `--quiet`. None of the update-suppression flags is an anchor —
none appears in the expected-values table — and this run neither added, removed
nor forced any of them.

**`scanner_class` derivation.** Strictly from which of the three section arrays a
record was read from, never from the record's own content. That was verified
structurally rather than assumed: one identical record body carrying
`VulnerabilityID`, `RuleID` and `ID` at once was placed in each of the three
sections in turn and yielded `vuln`, `secret` and `misconfig` respectively, with
`rule_id` taken per section from the same body. All three rows in this dataset
took `misconfig` because all three records came from `Misconfigurations[]`.

**Unsupported finding sections confirmed empty.** `Licenses` and
`ExperimentalModifiedFindings` — the two further finding arrays this version can
emit — were **both validated empty**, by two independent checks that agree. The
runner sums the length of both keys over every `Results` element of all 18
per-directory reports and prints
`UNSUPPORTED NON-EMPTY SECTIONS PRESENT: {...}` if either is non-empty; that line
is absent from `trivy.stdout.log` while the same block's other output is present,
so the check ran to completion. Independently, every `Results` member key in the
merged artifact was enumerated — `Class`, `MisconfSummary`, `Misconfigurations`,
`Target`, `Type` — and neither key appears at all. A non-empty finding array
outside the three supported sections **halts** the run with the observed structure
quoted, rather than being dropped while the reconciliation identity still
balances, and that halt check was proven able to fire: copies of this artifact
with a non-empty `Licenses`, with a non-empty `ExperimentalModifiedFindings`, and
with an invented non-empty finding array each raised the adapter's
unsupported-section error. The check was not triggered here.

**Reduced-reach conditions.** `harness/artifacts/logs/trivy.stderr.log` is 0
bytes, and this tool made **no statement of its own** about a scanner having
nothing to resolve, so **no reduced-reach condition is recorded in its words**.
The recorded scope fact behind its vulnerability scanner having nothing to resolve
is stated in the closing section of this document, from the scope rather than
quoted from the tool.

**Absent-artifact stderr and verdict**: not applicable — the artifact is present
and parses.

---

## osv-scanner

`scanner_class`: **vuln**, fixed for this tool. **This is the one tool that wrote
no artifact.** It holds a full entry here for exactly that reason: it contributes
no row to `findings.json` or `findings.csv` and would otherwise be invisible in
the record.

| Field | Value |
| --- | --- |
| Version | observed **2.5.1** (`osv-scalibr` 0.5.2), expected 2.5.1 — as expected. `osv-scanner --version` printed `osv-scanner version: 2.5.1`; binary `/opt/blitzy-tools/bin/osv-scanner` |
| Feed identity | observed **no local database — queries the OSV API live at scan time**, expected the same — **matches**. The runner states it at line 40: `database : none local - queries the OSV API (https://api.osv.dev) at scan time` |
| Comparability | **comparable** on identity — the observed feed identity is the expected one. There is no count to compare, no artifact having been written |
| Reproducibility gap | **NAMED.** This tool holds no local database and no recorded digest for the data it would consult, so its counts are not reproducible from anything on disk: an identical re-run against an API whose contents have moved can legitimately produce a different number. Disclosed rather than repaired — no local mirror was seeded and no digest was invented. Its effect on this run is nil, because no query was made; it is disclosed anyway so that a reader knows this tool's count has no on-disk provenance behind it |
| Feed state | **not attempted.** It resolved no package, so it had nothing to ask the API about — `0 Extract calls` in its own words. Of the four outcomes this is the third. No query, no response, no rate-limit notice and no network error appears in either captured stream |
| Exit code | **128**, expected 128 — as expected. The tool's own code, passed through unchanged by the runner at line 58. Exit 128 with zero resolvable packages is this tool's documented long-standing behaviour: **not a crash and not a failure** |
| Elapsed | expected **0 s**, observed **3 s** — recorded, both values. Whole seconds by construction, `scope_finish` subtracting two `date +%s` readings; the tool's own inner measurement reads `296.253311ms elapsed` |
| Finding count | expected **0** with no artifact written, observed **no artifact** — as expected |
| Output format | **not applicable — no artifact.** The runner's `$ART` would have been `harness/artifacts/raw/osv-scanner.json`, native JSON, and only if packages were resolved |
| Parse status | **absent** |
| Records parsed / rejected | not applicable — no artifact to traverse. Neither figure is set, and neither is written as zero |
| Reconciliation, per artifact | **`not applicable — artifact absent`**. This is the literal recorded value and its status is `not_applicable`. It is **not** a zero-equals-zero pass: no artifact was written, so there is nothing to traverse and no identity to assert |
| Reconciliation, dataset level | contributes nothing to `10018 = 9433 + 585`; it is one of the nine artifacts counted as absent rather than a term in the sum |
| Row validation | not applicable — zero rows in `findings.json` and zero in `findings.csv` |
| Adapter fixture | **not applicable** — no artifact, so no adapter, no fixture and no test module. The absent case is covered synthetically by `test_reconciliation`, which asserts the `not applicable — artifact absent` sentinel |
| Scan-target variable | `SPARK_SRC`, set to `/opt/spark-src`, resolved indirectly through `scope_resolve_target` at runner line 34 |
| Resolved scan root | `/opt/spark-src`, verified |
| Invocation form | one invocation, the 18 root-relative allowlist directories passed after a `--` separator so no path can be read as a flag |
| Working directory | `/opt/spark-src` (`cd "$SCAN_ROOT"`, runner line 44), equal to the scan root |
| Path base | **scan root**, `/opt/spark-src`; the record field would be the enclosing `results[].source.path`. No row needs this base, the tool having written nothing; it is recorded so that a resolver would have had an explicit base rather than a rejection had an artifact appeared |
| JDK major | none — a statically linked ELF binary, and the runner exports no `JAVA_HOME` |
| Interpreter | none — the runner invokes no interpreter |
| Credential expression | **none.** This runner calls `scope_cred_state` nowhere and reads no credential |

**Baked flags, as read** from `harness/bin/run-osv-scanner.sh` lines 46–49:
`scan source`, `--recursive`, `--format json`, `--output-file "$ART"`,
`--verbosity info`, `--` then the 18 root-relative allowlist directories. No
`--allow-no-lockfiles` flag is passed. Nothing was added by this run.

**Absent-artifact stderr, verbatim**, from
`harness/artifacts/logs/osv-scanner.stderr.log` (969 bytes, sha256
`021347c72dcd98e06b26c579164cded04c26b0eacc203aff07d5eb0487f2c401`). The final
three lines are the decisive ones and are reproduced exactly as the tool wrote
them:

```
Starting filesystem walk for root: /
End status: 640 dirs visited, 4735 inodes visited, 0 Extract calls, 296.253311ms elapsed, 296.253503ms wall time
No package sources found, --help for usage information.
```

The eighteen lines preceding them are the tool naming each directory it scanned,
one per allowlist directory, beginning `Scanning dir common/network-common/src/main`
and ending `Scanning dir sql/hive/src/main`.

**Reduced-reach condition, in the tool's own words.** The same stream carries this
tool's one statement about its own reach, and it is the statement the verdict rests
on: `0 Extract calls` over `640 dirs visited, 4735 inodes visited`, followed by
`No package sources found, --help for usage information.` — no manifest or lockfile
was extracted for package data. That is the tool saying it found nothing in scope
to work on, in its own words, rather than reporting a failure.

**Completion-versus-failure verdict: COMPLETED, not failed.** The artifact is
absent **and** the tool stated a no-work reason in its own output, which is the
`absent` case: the stderr is quoted verbatim above, zero rows were emitted, and
**the run continues**. The verdict rests on the tool's own words rather than on
the exit code. The alternative — an artifact absent with no stated reason — would
have halted the run, and that condition was not met. The runner's own stderr was
empty, so it took neither the argument guard's 64 nor `scope_fail`'s 78; there was
no configuration fault.

**Why there was nothing to resolve, and what it is a property of.** The tool
walked the real scope — 640 directories and 4,735 inodes, in its own words — and
made `0 Extract calls`, meaning no manifest or lockfile was extracted for package
data. Exactly one manifest-shaped file is in scope,
`core/src/main/resources/org/apache/spark/ui/static/package.json`, independently
verified in the pinned tree at **5 lines / 80 bytes**, sha256
`43b4dcbf33dc23b3d62576dce22b371b1d0c852b05dfd73442e9c3e97a0b4717`, carrying a
name, a license and a type and **no dependencies block**, with no lockfile beside
it. Across the eighteen in-scope directories there is no `pom.xml`, no
`requirements*.txt`, no `setup.py`, no `pyproject.toml` and no JAR. That is a
property of the **scope**, not of the installation and not of this tool, and it
was reported rather than fixed: the allowlist was not widened.

The tool's own phrasing `Starting filesystem walk for root: /` is **not** evidence
that it walked the filesystem root. Its working directory was `/opt/spark-src`,
every target argument was root-relative, and the visited counts match the in-scope
surface rather than a whole-host walk.

**No artifact was manufactured.** No empty or placeholder `osv-scanner.json` was
written; one would have corrupted both the reconciliation identity and the
adapter-creation decision. The runner deletes a zero-byte artifact at lines 53–55,
deliberately, so that absence stays distinguishable from an empty parse. The
absence was established by a direct listing of `harness/artifacts/raw/` plus a
`test -e` on the artifact path, not inferred from the exit code or from the
`MISSING` marker — both of which agree with the listing anyway. The four
conditional deliverables for this tool — its adapter, its fixture, its expected
rows and its test module — were **not created**, decided by this observed outcome
rather than left open. `harness/lib/normalize/shape.py` nonetheless declares an
`osv-scanner` routing key, so a legitimately written artifact would have routed
rather than falling into the halt path.

---

## dependency-check

`scanner_class`: **vuln**, fixed for this tool. Its artifact **is** present and
carries **zero finding records**, which is a different case from the absent
artifact above and is stated as such below.

| Field | Value |
| --- | --- |
| Version | observed **13.0.0**, expected 13.0.0 — as expected. `$DEPENDENCY_CHECK_HOME/bin/dependency-check.sh --version` printed `dependency-check-cli version 13.0.0` (exit 0), re-measured in the checkout, and the artifact's own `scanInfo.engineVersion` reads 13.0.0 |
| Packaging channel | observed **GitHub release, repository `dependency-check/DependencyCheck`, tag `v13.0.0`**, archive sha256 `44d920d1ec03e948df862a253f0912782a31b9beee8a7c8895b9cb95760176ed`. Recorded as observed rather than as expected: the expected attribution is `jeremylong/DependencyCheck`, which returns 404 for that tag because the project moved. **A Maven Central channel was not observed for this provisioning and is not recorded as one.** Both attributions stand; the version itself matches, so nothing halts |
| Feed identity | observed **keyless NIST NVD JSON 2.0 datafeed, `NVD API Last Modified 2026-08-24T08:00:04-04`**; expected **keyless NVD datafeed, 2026-08-23T08:00:06-04** — **DIFFERS by one day**. Both are keyless NIST JSON 2.0 datafeeds, and both values are recorded |
| Comparability | **NOT COMPARABLE WITH THE REHEARSAL.** A different feed produces a different count for reasons that have nothing to do with the code. The same status is carried in `oss-scan-results/severity-map.md` |
| Feed identity provenance | Read out of the artifact's own `scanInfo.dataSource` block — the tool stating the identity of the data it used — and quoted in `harness/artifacts/logs/dependency-check.stdout.log` PHASE 2, where the four NVD timestamps appear. Corroborated to the day by `$HARNESS_DC_DATA_DIR/odc.mv.db` at 249,724,928 bytes with an unchanged mtime |
| Feed state | **not attempted.** The runner passes `--noupdate`, so no refresh was attempted and the seeded datafeed was used exactly as found. Of the four outcomes this is the third. The feed was unchanged by the invocation, and the artifact's own `NVD API Last Checked` of 2026-08-24T12:41:51Z **precedes** this invocation's start, so there was no scan-time fetch and this tool contributes no reproducibility gap of that kind |
| Exit code | **0**, expected 0 — as expected. The tool's own status, captured at runner line 58. No `--failOnCVSS` is passed, so the code reflects the run rather than a policy |
| Elapsed | expected **6 s**, observed **23 s** — recorded, both values, `finished_at` minus `started_at` agreeing at 23 s. The tool's own phase timings (`Created CPE Index (7 seconds)`, `Finished RetireJS Analyzer (5 seconds)`) account for it; no time limit applies and elapsed time is a fact rather than a budget |
| Finding count | expected **0**, observed **0** — as expected. Count unit `dependencies[].vulnerabilities[]`. Separately, 32 **dependency records** were analysed (`.js` 31, `.json` 1), all 32 matching the twelve globs, with 0 resolved package coordinates. A dependency record is not a finding record and the two are not summed |
| Output format | native JSON report, artifact `harness/artifacts/raw/dependency-check.json`, 17,097 bytes, sha256 `ebe98aed11973718591f8c7490eedde86f97bf4fb2047a059e499be50e02c3b9` |
| Parse status | **clean** |
| Records parsed / rejected | 0 parsed, **0 rejected**. There was nothing to parse under the count unit; no rejection class engaged and no parser error was raised |
| Reconciliation, per artifact | `0 = 0 + 0` — **pass**. A real zero with the artifact **present**, deliberately not the `not applicable — artifact absent` case |
| Reconciliation, dataset level | contributes a zero term to `10018 = 9433 + 585` — **pass** |
| Row validation | not applicable in substance — this tool emitted zero rows, and the dataset-level validation passed with zero rows attributed to it |
| Adapter fixture | **pass** — `test_dependency_check_adapter`, exit 0 |
| Scan-target variable | `SPARK_SRC`, set to `/opt/spark-src`, resolved through `scope_resolve_target` |
| Resolved scan root | `/opt/spark-src`, verified; the pinned commit re-verified as `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` |
| Invocation form | one invocation carrying **18 absolute `--scan` paths**, one per allowlist directory, built as `"$SCAN_ROOT/$d"` |
| Working directory | `/opt/spark-src` (`cd "$SCAN_ROOT"`, runner line 49). cwd is not the path base here: the tool reports absolute paths because it was handed absolute `--scan` arguments |
| Path base | **filesystem-absolute**, relativized against `/opt/spark-src`; the record field is the enclosing `dependencies[].filePath`. 32 of 32 were verified absolute under the scan root, and the dataset emits no absolute path |
| JDK major | **17** — `/opt/blitzy-tools/jdk/jdk-17.0.20+8`, `openjdk version "17.0.20" 2026-07-21`, build `Temurin-17.0.20+8`. Read from the runner rather than assumed, on three independent readings that agree: the runner invokes the tool with `JAVA_HOME="$JAVA_HOME"` at line 51 and states at line 8 that it runs under Temurin 17 with JDK 21 reserved for Joern; `$JAVA_HOME/bin/java -version` re-run in the checkout reports the same; and the JVM that ran the scan, sampled from `/proc` (pid 452914), has `exe` `/opt/blitzy-tools/jdk/jdk-17.0.20+8/bin/java`. Only `exe` and `argv` were read, never `/proc/*/environ` |
| Interpreter | none — a JVM application launched through a shell script; the runner invokes no Python interpreter |
| Heap | **none.** This runner sets no `JAVA_OPTS` and its argv carries no `-Xmx`; it is not one of the four heap-bound JVM invocations, so no heap is claimed for it |
| Credential expression | `printf 'credential      : NVD_API_KEY=%s  (OSS Index analyzer disabled explicitly)\n' "$(scope_cred_state NVD_API_KEY)"` at runner line 45; fixed token only. **`NVD_API_KEY` absent** — and it must stay unset rather than be set to an empty string, since an empty value makes this tool abort with `Invalid API Key, length of 0 too short`. The Sonatype OSS Index credential is likewise absent |

**JDK assignment, recorded as read.** A reader expecting 21 for this tool would be
wrong for this provisioning, and a reader expecting 17 for every non-Joern tool
would be assuming rather than reading. What halts the run is a **major**
misassignment where one is mandated — the build on anything but 17, or any Joern
invocation on anything but 21. This tool is neither, so its assignment is a
property of the provisioned runner; the observed major equals the recorded one,
and a patch-level difference would have been recorded with both values while the
run continued.

**Baked flags, as read** from `harness/bin/run-dependency-check.sh` lines 51–57,
corroborated by the `/proc` argv: `--project spark-pinned-<pinned sha>`,
`--noupdate`, `--disableOssIndex`, `--data "$HARNESS_DC_DATA_DIR"`, one `--scan`
per allowlist directory (18 absolute roots), `--format JSON`, `--prettyPrint`,
`--out`. None appears in the expected-values table, so none is an anchor.

`--disableOssIndex` **is passed here**, which differs from the harness precedent
that omitted it. The runner gives its own reason at lines 5–7: Sonatype OSS Index
no longer answers anonymously and 13.0.0 self-disables the analyzer with
`Authentication with token is now required`, so passing the flag makes the
disabling this harness's own and recorded rather than incidental. Recorded as
read, not as expected.

**Flags deliberately not passed, and what follows.** `--nvdApiKey`, `--purge`,
`--update-only`, `--disableRetireJS`, `--disableNodeJS`, `--disableNodeAudit`,
`--enableExperimental`, `--failOnCVSS` and `--suppression` are all absent. So the
RetireJS analyzer **ran**, offline against the seeded `jsrepository.json`; the
Node Package Analyzer **ran**, and produced the two reach statements quoted below;
no experimental analyzer contributed; the exit code reflects the run rather than a
policy; and only the feed's own `publishedSuppressions.xml` applies.

**Reduced-reach conditions, in the tool's own words**, from
`harness/artifacts/logs/dependency-check.stdout.log`:

```
[WARN] No lock file exists - this will result in false negatives; please run `npm install --package-lock`
[WARN] Analyzing `/opt/spark-src/core/src/main/resources/org/apache/spark/ui/static/package.json` - however, the node_modules directory does not exist. Please run `npm install` prior to running dependency-check
```

Both are statements about the input rather than failures: a `package.json` with no
lockfile and no installed tree is what the scope contains. The reach itself — 18
absolute `--scan` roots rather than the precedent's whole-root scan with a
test-path exclusion — is recorded as reach, not as an error.
`harness/artifacts/logs/dependency-check.stderr.log` is 0 bytes, so
`scope_fail` never ran and the exit 78 path was not taken.

**Absent-artifact stderr and verdict**: not applicable — the artifact is present
and parses. The zero here is a zero **finding count**, not an absent artifact, and
the two are recorded differently on purpose.

---

## joern

`scanner_class`: **sast**, fixed for this tool. This is the only runner whose input
is the code-property graph rather than a directory tree.

| Field | Value |
| --- | --- |
| Version | observed **4.0.607**, expected 4.0.607 — as expected. Read from the **startup banner** with stdin closed, this tool exposing no version flag and its REPL blocking on an open stdin: the banner line is `Version: 4.0.607`, captured at the pre-load gate (`harness/artifacts/logs/joern.preflight.log` lines 203–206) and re-read live from a scratch directory outside the repository so the workspace side effect could not land in the checkout. The artifact's own `tool_version` field also reads 4.0.607, which agrees but is the runner's claim rather than an independent reading and is recorded as corroboration only. The banner does not appear in `joern.stderr.log` because the runner invokes the tool with a script rather than interactively, and that path prints no banner |
| Query set identity | observed **6 bounded structural queries** baked into `harness/lib/joern-scan.sc`; expected the set baked into the provisioned runner, which the plan expects to be a 58-query bundle bounded to 6 structural queries with the actual count to be read from the runner — **matches**. The count was **read from the runner**, at lines 50–78 where the six entries are declared, and line 111 where the script labels its own output `6 bounded structural queries` |
| Query identifiers | `joern-process-exec`, `joern-unsafe-deserialization`, `joern-reflection-forname`, `joern-message-digest`, `joern-cipher-getinstance`, `joern-xml-factory` |
| Comparability | **comparable** — the observed query-set identity is the expected one |
| Feed state | not applicable — no feed and no ruleset fetch. `fetched_at_scan_time: false`; no reproducibility gap of that kind |
| Exit code | **0**, expected 0 — as expected. The tool's own code, untransformed by the runner. **Exit 78 was not observed**: had the runner's graph guard fired (lines 44–48, via `scope_fail`) it would have named the missing graph on stderr, which is a configuration fault to correct at the gate rather than an unexplained missing artifact |
| Elapsed | expected **734 s**, observed **1,074 s** — recorded, both values, and internally consistent with the two recorded timestamps |
| Finding count | expected **692**, observed **692** — as expected. Count unit: one element of the artifact's `findings` array. Per query, as the runner printed them: `joern-process-exec` 55, `joern-unsafe-deserialization` 178, `joern-reflection-forname` 412, `joern-message-digest` 23, `joern-cipher-getinstance` 11, `joern-xml-factory` 13 — summing to 692 |
| Traversal bound | 2,000 per query (`HARNESS_JOERN_QUERY_BOUND`, defaulted at runner line 36). **`bound_reached=false` for all six.** The bound limits traversal work, never the files or modules in scope |
| Output format | native JSON with a `findings` array; envelope keys `tool`, `tool_version`, `cpg`, `graph`, `query_set`, `queries`, `findings`. Artifact `harness/artifacts/raw/joern.json`, 354,343 bytes, sha256 `deb0cd765602cc0be2bf4ffa03cc8a39cccfb5e17fb0631d094d24af55204a4a` |
| Parse status | **partial** |
| Records parsed / rejected | **107 emitted, 585 rejected**, all 585 under the single class **`unresolvable_path`**. **No parser error was raised** — the artifact parses as JSON in full, and the rejections are per-record path-resolution outcomes rather than a parse fault, so there is no parser error text to retain |
| Reconciliation, per artifact | `692 = 107 + 585` — **pass** |
| Reconciliation, dataset level | contributes to `10018 = 9433 + 585` — **pass**; every rejected record in the whole dataset is one of these 585 |
| Row validation | pass over this tool's 107 rows. 78 take `in_scope: true` and **29 take `in_scope: false` and are kept**, being source coordinates that resolve outside the twelve globs (`common/utils` 14, `common/unsafe` 6, `launcher/src` 4, `common/utils-java` 3, `streaming/src` 2). **No row resolved into a `src/test` tree** — the counter reads 0 |
| Adapter fixture | **pass** — `test_joern_adapter`, exit 0 |
| Scan-target variable | `SPARK_SRC`, set to `/opt/spark-src`, resolved and verified. The scanned **input**, however, is the graph, passed through `HARNESS_CPG` |
| Resolved scan root | `/opt/spark-src`, verified |
| Invocation form | one invocation. No filesystem target appears on the command line: the graph path, the output path and the bound are passed through the environment and the script through `--script` |
| Working directory | `/tmp/blitzy-harness-scratch/31/joern-run` — **the one runner whose working directory is not the scan root**, deliberately: this tool exposes no workspace flag and writes its workspace into whatever directory it runs from, so the runner works in the per-clone scratch directory and never in the repository |
| Path base | **bytecode class**, with **no value** — no filesystem base exists for this tool's records, and none was invented. The emitted `file` field is the frontend's ephemeral `/tmp/jimple2cpg-<id>/<pkg>/<Class>.class` extraction path for all 692 findings and can never be a path in the Spark tree, so the `class` field is the only resolvable coordinate. Resolution is against `src/main` **and** `src/test` under the pinned root, taken only where unique |
| JDK major | **21** — `/opt/blitzy-tools/jdk/jdk-21.0.12.1+1`, `openjdk version "21.0.12.1" 2026-08-18 LTS`, VM `21.0.12.1+1-LTS`, matching the expected Temurin build with no patch difference to record. Taken from `java.specification.version` — the JVM's own property output — rather than off a banner. Two independent pins agree: the runner sets `JAVA_HOME="$JAVA_HOME_21"` and asserts that JDK usable before invoking, and the `joern` launcher on `PATH` is a provisioning wrapper that pins the same JDK. A wrong major here halts the run; a patch difference with the correct major is recorded with both values |
| Interpreter | none — the runner invokes no Python interpreter |
| Credential expression | **none.** This runner reads no credential and calls `scope_cred_state` nowhere |

**Baked flags, as read** from `harness/bin/run-joern.sh` lines 67–71:
`--script harness/lib/joern-scan.sc`, `-J-Xmx"$HARNESS_JOERN_HEAP"`, and stdin
redirected from `/dev/null`. `SL_LOGGING_LEVEL` is set to `WARN`, because the
default level floods the artifact. None of these is an anchor.

**Heap actually used: 64 GB**, as `-J-Xmx64g` (68,719,476,736 bytes), which the
runner also prints into its own stream. The mechanism is `HARNESS_JOERN_HEAP`, the
runner's own documented environment override applied at line 70 — a runtime value
rather than a configuration edit, no runner file or baked flag having been
changed. **No raise was required and none was made**: the provisioned default at
`harness/env.sh` line 85 is already 64 GB, which meets the mandated minimum. The
precedent's 48 GB `JAVA_OPTS` default — below that minimum — was **not in effect**
here, on two grounds: `JAVA_OPTS` was unset in the sourced environment, and this
runner reads `HARNESS_JOERN_HEAP` rather than `JAVA_OPTS`, so the precedent
default could not have applied even had the variable been set. The direction of
the rule is one-way: raising a heap is permitted and reported, while lowering one
produces a truncated graph whose silence cannot be told apart from a clean result.
Commit was proven rather than assumed —
`java -Xms64g -Xmx64g -XX:+AlwaysPreTouch -version` exits 0, and pre-touching
every page is strictly stronger than reserving it.

**Graph identity, re-verified immediately before the load.** The named path
`harness/cpg/spark.cpg` is a symlink resolving to the regular file
`/opt/blitzy-harness/cpg/spark.cpg`; both names are the same file, on the same
`dev:inode` `1048752:37891488`. Byte size **541,255,894** and sha256
`26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc` were compared
against the values recorded when the graph was written and **match**, with the
33-byte link-only measurement explicitly discarded in favour of the
symlink-following size. The check ran at 2026-08-24T22:38:33Z, before the load at
22:41:02Z, and the runner prints the path, its resolution, the byte size and the
digest it is about to load into its own stream. The load used `importCpg`, three
occurrences of it against **zero occurrences of `importCode`**, and reported
`methods=1397339 typeDecls=119691 files=45037` — more than zero methods. The graph
was not rebuilt by this invocation. Had the digest differed, the run would have
halted.

**Rejections, all 585 under `unresolvable_path`.** Each rejected record is a
bytecode class with no source coordinate in the pinned tree — third-party classes
shaded into Spark's JARs. Projecting the rejection records' own class fields
gives `org.sparkproject` 527, `org.apache` 44, `com.google` 12, `org.fusesource`
1 and `org.rocksdb` 1. The first rejection's own detail states the rule as
applied: no source file under `src/main` or `src/test` in the pinned tree is named
for the class or declares that type, so the class has no source coordinate. A
record whose path cannot be resolved is rejected and counted rather than guessed
into a field, and `path` is not an optional field.

**Reduced-reach conditions.** This tool reported none. Its stderr, 699 bytes,
carries the script-execution trace only — the project creation, the working copy,
the base-CPG load and the closing save. Its own statement about the work it did is
the per-query output in
`harness/artifacts/logs/joern.stdout.log`:

```
graph loaded: methods=1397339 typeDecls=119691 files=45037
query joern-process-exec               returned     55 bound_reached=false elapsed_ms=1271
query joern-unsafe-deserialization     returned    178 bound_reached=false elapsed_ms=8
query joern-reflection-forname         returned    412 bound_reached=false elapsed_ms=11
query joern-message-digest             returned     23 bound_reached=false elapsed_ms=1
query joern-cipher-getinstance         returned     11 bound_reached=false elapsed_ms=0
query joern-xml-factory                returned     13 bound_reached=false elapsed_ms=1
```

**Absent-artifact stderr and verdict**: not applicable — the artifact is present
and parses.

**Second appearance.** Joern appears twice in this run by design: here as one of
the nine scanned runners, and again as the subject of the capability probe under
`queries/joern/`. The probe writes outside `harness/artifacts/raw/` and
**contributes no dataset row**, so nothing in this entry mixes the two. Reading
the double appearance as a duplication would corrupt both counts. The probe's own
results belong to `oss-scan-results/joern-probe.md` and the per-query result files
under `queries/joern/results/`.

---

## The two runs behind the normalization fields

**The normalizer.** Command
`/usr/bin/python3 <checkout>/harness/lib/normalize/cli.py`, run from the checkout
root, interpreter `/usr/bin/python3` reporting **3.13.7** against an expected
3.13.7 — matches; CPython, `3.13.7 (main, Mar  3 2026, 12:19:54) [GCC 15.2.0]`.
It ran from 2026-08-25T04:10:15Z to 04:10:18Z and exited **0**, outcome
`completed`, with `reconciliation.passed` true, no failures and no halt. It uses
the standard library only, so it introduces no manifest, no lockfile and no
install step. Stages A and B are established **before** either output file is
written, so a dataset whose identity already failed would never have reached disk.
The parse status, record counts and reconciliation results in every entry above
are this run's measurements (`harness/artifacts/logs/normalize-run.json`).

**The adapter tests.** Suite exit **0**, **577 tests** and 12,955 subtests, with
0 failures, 0 errors, 0 skips, 0 expected failures and 0 unexpected successes;
verbatim trailer `Ran 577 tests in 1.534s` / `OK`. Interpreter
`/usr/bin/python3` at **3.13.7**, the same base interpreter as the normalizer and
independent of every scanner's environment. Per-module exit status 0 for all
eight modules: `test_sarif_adapter`, `test_trivy_adapter`,
`test_gitleaks_adapter`, `test_checkov_adapter`,
`test_dependency_check_adapter`, `test_joern_adapter`,
`test_shape_routing_negative` and `test_reconciliation`. A failed adapter
fixture, rejection or reconciliation test is a condition that **stops the run**;
it was never met, and no result here is recorded as a soft warning, a known
failure, an expected failure or a skip
(`harness/artifacts/logs/adapter-tests-run.json`).

## Shape detection and routing, per artifact

An artifact is SARIF when it carries `version` equal to `"2.1.0"` **together
with** a `runs` array; those two properties together are the whole test, and
nothing else is consulted — not `$schema`, not the filename, not `tool.driver`. A
SARIF artifact routes to the single shared adapter; anything else routes to the
native adapter keyed by the runner that wrote it. An artifact matching neither
SARIF nor a known native shape is `failed` and **halts** the run; none did.

| tool | detected shape | adapter | detection evidence |
| --- | --- | --- | --- |
| `opengrep` | sarif | `normalize.adapters.sarif` | `version` `2.1.0` observed, `runs` an array of length 1 |
| `semgrep` | sarif | `normalize.adapters.sarif` | `version` `2.1.0` observed, `runs` an array of length 1 |
| `datadog-static-analyzer` | sarif | `normalize.adapters.sarif` | `version` `2.1.0` observed, `runs` an array of length 1 |
| `gitleaks` | native | `normalize.adapters.gitleaks` | top level is an **array**, so no `version` and no `runs` |
| `checkov` | native | `normalize.adapters.checkov` | top-level keys `check_type`, `results`, `summary`; no `version`, no `runs` |
| `trivy` | native | `normalize.adapters.trivy` | top-level keys `SchemaVersion`, `Trivy`, `ReportID`, `CreatedAt`, `ArtifactName`, `ArtifactType`, `Results`; no `version`, no `runs` |
| `osv-scanner` | not routed | none | no artifact to route |
| `dependency-check` | native | `normalize.adapters.dependency_check` | top-level keys `reportSchema`, `scanInfo`, `projectInfo`, `dependencies`; no `version`, no `runs` |
| `joern` | native | `normalize.adapters.joern` | top-level keys `tool`, `tool_version`, `cpg`, `graph`, `query_set`, `queries`, `findings`; no `version`, no `runs` |

The mandated negative direction is asserted by `test_shape_routing_negative`: a
native artifact must **not** route to the SARIF adapter. A permissive detector
that accepted a native file as SARIF would produce an empty result set rather than
an error, and an empty result set is indistinguishable from a clean scan.

## Zero resolvable dependency manifests in scope, and its per-tool consequence

This is a property of the **scope**, established from the scope itself and not
from any tool's output, and it is reported rather than fixed: the twelve globs
stayed byte-exact and were neither widened nor narrowed. Widening them to give a
tool something to resolve would answer a scope question that is not this run's to
answer and would silently change what every count means.

Across the eighteen in-scope directories there is exactly one manifest-shaped
file — `core/src/main/resources/org/apache/spark/ui/static/package.json`,
verified in the pinned tree at 5 lines and 80 bytes, sha256
`43b4dcbf33dc23b3d62576dce22b371b1d0c852b05dfd73442e9c3e97a0b4717`, declaring a
name, a license and a type with **no dependencies block** — and no lockfile beside
it, no `pom.xml`, no `requirements*.txt`, no `setup.py`, no `pyproject.toml` and
no JAR anywhere.

The consequence for each tool that resolves packages, stated so that no reader has
to infer it:

- `osv-scanner` resolved zero packages, made `0 Extract calls`, wrote no artifact
  and said so in its own words. That is the tool completing, not failing.
- `dependency-check` analysed 32 dependency records — 31 `.js` and 1 `.json`,
  all vendored web assets — resolved 0 package coordinates and reported 0
  vulnerabilities, with an artifact present and parsing.
- `trivy`'s vulnerability scanner had nothing in scope to resolve; its
  `Vulnerabilities` section count is 0, and the 3 records it did report came from
  `Misconfigurations`.

None of the three is broken, and none of these figures is evidence about a tool's
capability.

## Values that could not be established

Named here rather than omitted, because a value missing from the record is a value
nothing downstream can check.

| Value | Tool | Why |
| --- | --- | --- |
| `started_at` / `finished_at` | `semgrep` | The tool prints its SARIF document to stdout, so the runner gave stdout to the artifact stream and `scope_finish`'s console header and trailer went to a stream not captured to a file for this tool. The whole stdout log was searched and the marker occurs zero times; the artifact emits no `startTimeUtc` or `endTimeUtc`. The 621-second window length **is** established |
| Rule count | `gitleaks` | The rule set is not separately versioned, the tool reports no count, and the expected-values table carries none |
| Ruleset digest | `gitleaks` | The rules are compiled into the binary; no digest exists to compare and none was invented |
| Policy count | `checkov` | The bundled policies are not separately versioned and the tool reports no count; the expected-values table carries none |
| Policy digest | `checkov` | Bundled policies carry no separate version or digest, and none was invented |
| Path base value | `joern` | No filesystem base exists for a bytecode-class coordinate. The base **kind** is recorded and the resolution route — the `class` field against `src/main` and `src/test` — is recorded; a plausible path was not invented in place of the missing value |

## What this document does not do

- It draws **no comparison between tools**. It does not rank them, contrast their
  coverage, explain why one reported something another did not, or characterise
  what any tool's output demonstrates about that tool. It makes no comparison
  against any commercial or third-party scanner, no such data being part of this
  run.
- It **judges no finding**. Nothing here is called real, important, a false
  positive or a duplicate, and nothing is deduplicated across tools.
- It carries **no credential value and no secret value**. Credential presence is
  a boolean throughout, and `gitleaks` ran with redaction so no matched secret
  value reached its artifact or any field.
- It **owns** the per-tool status contract and, with `severity-map.md`, the
  nine-tool inventory. It does **not** own the per-project build and graph
  coverage verdicts (`oss-scan-results/build-record.md`), the severity mapping and
  observed literals (`oss-scan-results/severity-map.md`), the capability probe
  (`oss-scan-results/joern-probe.md`), or the run-wide index and the artifact-tree
  manifests (`oss-scan-results/run-record.md`). Where one of those documents cites
  a figure that appears here, it is this measurement cited again rather than a
  second measurement.
- It records **inherited** facts as the run found them and applies the authority
  rule only to those: the expected-values table governs every field it carries and
  the environment record never overrides it. Three consequences are visible above.
  Where the table and the record would have differed, the table governs — and on
  every field this document carries they agree, so no entry rests on that
  tie-break. Where the record **agrees with observation** and both differ from the
  table, both values are recorded rather than adjudicated: `trivy`'s two database
  timestamps are that case, the record carrying the same 2026-08-24 stamps this
  run observed against the table's 2026-08-23. And where a difference is an
  **output this run deliberately produced** rather than an inherited fact
  contradicted, both values stand with their provenance and nothing halts — the
  `semgrep` artifact at 40,661,229 bytes against the record's 40,660,951 for its
  own rehearsal invocation, and the `datadog-static-analyzer` artifact at
  5,671,091 against the record's 5,671,090, each a different invocation's output
  and each with an identical finding count across the two.
