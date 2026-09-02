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
| `harness/artifacts/logs/<tool>.status` | The invocation's own outcome, and **exactly seven fields, nothing beside them**: `tool`, `exit_code`, `elapsed_seconds`, `artifact`, `artifact_bytes`, `scan_root`, `scan_root_source`. Every one of the nine is the runner's own verbatim `scope_finish` trailer at seven lines — 238 to 278 bytes each — so a citation of any other field name, or of a line above 7, names nothing. Anything a reader might expect beside them is owned elsewhere: the lane and its digests by `runner-sequence.json`, the configuration by `runner-metadata.json` |
| `harness/artifacts/logs/<tool>.stdout.log`, `<tool>.stderr.log` | The tool's own words, verbatim: reduced-reach conditions, and for an absent artifact the stated reason |
| `harness/artifacts/logs/adapter-tests-run.json` | The per-tool adapter-fixture result, and every adapter-test figure restated here |
| `harness/artifacts/logs/runner-sequence.json` | The serial lane and its chronology: per invocation the argv and argument count, the start and end stamps, the finer elapsed measurement, and the artifact, stream and `.status` byte sizes and sha256 values measured immediately after that invocation returned |
| `harness/artifacts/logs/gate-record.json` | The gate's 43 checks with their verdicts, and the tool version, ruleset and feed identities and credential absences it measured before anything was scanned |
| `harness/artifacts/logs/<tool>.runner-console.log` | Each runner's own console output verbatim, including the header and trailer `scope_begin` and `scope_finish` print and each runner's own statement of its scan root, path base, feeds and merge steps |
| `harness/artifacts/MANIFEST.json` | The published byte size and sha256 of every raw artifact and every log file, including each tool's side-artifact tree |

The status filename is `<tool>.status`, the name the plan specifies, and
deliberately not the harness precedent's `<tool>.meta.json`.

## The inventory

Nine canonical identifiers, in the processing order the normalizer uses. This is
an inventory of what each tool did, and it is not a comparison: nothing here ranks
the tools, contrasts their coverage or reads one tool's figure against another's.
Each row is expanded into a full entry below.

| tool | scanner_class | exit code | artifact | parse status | dataset rows | rejected |
| --- | --- | --- | --- | --- | --- | --- |
| `opengrep` | sast | 0 | `opengrep.sarif` | clean | 1,319 | 0 |
| `semgrep` | sast | 0 | `semgrep.sarif` | clean | 1,162 | 0 |
| `datadog-static-analyzer` | sast | 0 | `datadog-static-analyzer.sarif` | clean | 6,832 | 0 |
| `gitleaks` | secret | 2 | `gitleaks.json` | clean | 1 | 0 |
| `checkov` | misconfig | 1 | `checkov.json` | clean | 6 | 0 |
| `trivy` | per record | 0 | `trivy.json` | clean | 3 | 0 |
| `osv-scanner` | vuln | 128 | **none written** | absent | 0 | not applicable |
| `dependency-check` | vuln | 0 | `dependency-check.json` | clean | 0 | 0 |
| `joern` | sast | 0 | `joern.json` | partial | 107 | 586 |

Row counts and rejection counts are `normalize-run.json`
`totals.rows_by_tool` and `totals.rejections_by_tool`; exit codes and artifacts
are each tool's `<tool>.status`. The nine rows sum to the dataset's
**9,430** emitted rows and **586** rejected records. The two tools with zero rows —
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
`dependency-check`; **`partial` once** — `joern`, 586 records rejected under one
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
| Dataset-level sum of the per-artifact identities | `10016 = 9430 + 586` | pass | `normalize-run.json` `reconciliation.stage_b` |
| Parsed `findings.json` rows against the dataset's emitted rows | `9430` against `9430` | pass | `normalize-run.json` `reconciliation.stage_c[0]` |
| Parsed `findings.csv` rows against the dataset's emitted rows | `9430` against `9430` | pass | `normalize-run.json` `reconciliation.stage_c[1]` |
| Parsed `findings.json` rows against parsed `findings.csv` rows | `9430` against `9430` | pass | `normalize-run.json` `reconciliation.stage_c[2]` |

Stage B closes over nine artifacts, eight present and one absent: 10,016 raw
finding records against 9,430 emitted rows plus 586 rejected records, with
`failed_tools` empty.

The JSON and CSV row counts are asserted **separately** rather than one being
inferred from the other, and then compared to each other as a third assertion.
Both files were parsed to obtain them; neither figure comes from counting physical
lines. Field-for-field comparison under typed coercion passed over 9,430 rows and
113,160 fields with no first mismatch
(`normalize-run.json` `output_comparison`).

Row validation passed over all 9,430 emitted rows with zero violations: every row
carries exactly the twelve fields in order, `path` and `severity_norm` are never
absent, absence appears only in `severity_native`, `start_line`, `cwe`, `cve` and
`package_coordinate`, and no emitted path is absolute
(`normalize-run.json` `outputs.row_validation`). Each entry below states that
result as it applies to that tool's own rows.

Absence, counted per optional field over those 9,430 rows and taken from the same
record: `cve` absent on **9,430** rows, `package_coordinate` absent on **9,430**,
`cwe` absent on **8,674**, `severity_native` absent on **2,488** and `start_line`
absent on **3**. The `severity_native` figure is the sum of the four tools whose
every row was banded on basis `no_vocabulary` in this run — `opengrep` 1,319,
`semgrep` 1,162, `gitleaks` 1 and `checkov` 6, giving
1,319 + 1,162 + 1 + 6 = 2,488 (`normalize-run.json` `severity_literals.tools`) —
and the three absent `start_line` values are `trivy`'s three rows.

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
- **One serial lane, with its chronology.** All nine invocations belong to run
  `w013-20260901T132807Z`, clone index 13, and ran in **one serial lane** from
  **2026-09-01T13:49:39Z to 14:41:25Z**: invocation N+1 started only after
  invocation N returned, from one script in one process in one clone, in the
  canonical tool order, with monotonic non-overlapping stamps and no runner
  invoked twice (`harness/artifacts/logs/runner-sequence.json`, `lane` and
  `serialization`). Each invocation's `argument_count` is **0**. Every artifact,
  stream and `.status` file was measured by byte size and sha256 immediately
  after that invocation returned, which is what binds those bytes to that
  invocation and makes a later substitution detectable. The per-tool windows are
  `opengrep` 13:49:39→14:13:06, `semgrep` 14:13:07→14:22:02,
  `datadog-static-analyzer` 14:22:02→14:22:59, `gitleaks` 14:22:59→14:23:13,
  `checkov` 14:23:13→14:24:46, `trivy` 14:24:46→14:25:03, `osv-scanner`
  14:25:03→14:25:03, `dependency-check` 14:25:03→14:25:10 and `joern`
  14:25:10→14:41:24.
- **The gate this lane ran behind, and its verdict.**
  `harness/artifacts/logs/gate-record.json` records **43 checks — 38 `pass`, 3
  `recorded_difference`, 2 `halt`** — and an overall `gate_verdict` of
  **`halt`**. The two halts are `gate.artifact_trees_exist_and_empty`, because
  both artifact trees already held this run's predecessors' content at the
  emptiness check, and `gate.environment_record_graph_identity_agreement`,
  because the environment record's graph identity does not match the graph on
  disk. The three recorded differences are the ruleset or feed identities of
  `datadog-static-analyzer`, `trivy` and `dependency-check`, each stated in that
  tool's entry below with both values and a not-comparable mark. That verdict is
  reported here as measured; it authorises nothing, and no figure in this
  document is presented as having passed a gate it did not.
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
- **Credential presence.** All six variables the gate reads were **absent**:
  `gate-record.json` check `gate.credentials_absent` prints
  `SEMGREP_APP_TOKEN=absent, DD_API_KEY=absent, DD_APP_KEY=absent,
  NVD_API_KEY=absent, BC_API_KEY=absent, GITHUB_TOKEN=absent`, and any Sonatype
  OSS Index credential is likewise absent. Nothing was provisioned and nothing
  was attached. Because no credential was present, no runner could have written
  one into a preserved log and the halt that a live credential in an
  unmodifiable runner would have forced did not arise.
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
| Feed state | **not applicable — there is no feed**, so none of the four outcomes (attempted and succeeded, attempted and failed, not attempted, not reported) applies. This tool consults a pinned local ruleset checkout. `fetched_at_scan_time: false`, so there is no scan-time fetch and no reproducibility gap of that kind |
| Exit code | **0**, expected 0 — as expected. The runner exits with the tool's own code and does not transform it (`opengrep.status` `exit_code=0`) |
| Elapsed | expected **929 s**, observed **1,407 s** — recorded, both values, and no figure here is read as slow or over budget. `opengrep.status` `elapsed_seconds=1407` from the runner's own whole-second timer; the lane ledger's finer measurement of the same window is **1407.786 s**, 2026-09-01T13:49:39Z to 14:13:06Z (`runner-sequence.json` `invocations[1]`) |
| Finding count | expected **1,322**, observed **1,319** — recorded, both values. The count unit is `runs[].results[]`, and the tool's own summary line agrees: `Ran 1138 rules on 4095 files: 1319 findings.` |
| Output format | SARIF 2.1.0, artifact `harness/artifacts/raw/opengrep.sarif`, **73,768,116 bytes**, sha256 `740ab140d1224064ce3754470c0a90de66d730febec7fb10073421542b085758`, measured immediately after the invocation returned |
| Parse status | **clean** |
| Records parsed / rejected | 1,319 parsed, **0 rejected**. No rejection class was engaged and no parser error was raised |
| Reconciliation, per artifact | `1319 = 1319 + 0` — **pass** (`normalize-run.json` `reconciliation.stage_a`) |
| Reconciliation, dataset level | contributes to `10016 = 9430 + 586` — **pass** |
| Row validation | pass; the 1,319 rows carry exactly the twelve fields, no absent `path` or `severity_norm`, and no absolute path. `severity_native` is **absent on all 1,319**, banded `Info` on basis `no_vocabulary` |
| Adapter fixture | **pass** — the shared SARIF adapter, `test_sarif_adapter`, 122 tests, exit 0, result OK; per-adapter `verdict` `pass` with no AAP requirement recorded failed |
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
`harness/artifacts/logs/opengrep.stderr.log`. Each line is quoted exactly as the
tool wrote it, and they are the reach-bearing lines selected from that stream
rather than one contiguous span: the first is from the Scan Status box, the rest
are the Scan Summary block and the trailing total that follows it.

```
  Scanning 4095 files tracked by git with 2006 Code rules:
Some files were skipped or only partially analyzed.
  Scan was limited to files tracked by git.
  Partially scanned: 46 files only partially analyzed due to parsing or internal Opengrep errors

Ran 1138 rules on 4095 files: 1319 findings.
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
arms are written under `harness/artifacts/logs/` and are all present on disk:
`taint-ab-anchor-diskstore-{on,off}.{log,sarif}` for the mandated subject,
`taint-ab-anchor-diskstore-fullruleset-{on,off}.{log,sarif}` for that same subject
under the whole ruleset, `taint-ab-hiveshim-{on,off}.{log,sarif}` for the second
subject, `taint-ab-discriminating-{on,off}.{log,sarif}` for the discriminating
pair, and `taint-ab-{on,off}.{log,sarif}` for the analysis, beside the four
control captures `taint-ab-off-control-rule.txt`,
`taint-ab-source-removed-control-rule.txt`, `taint-ab-source-removed-control.sarif`
and `taint-ab-search-control.sarif`. Every one of them sits outside
`harness/artifacts/raw/`, so none can overwrite this runner's artifact. That
A/B **contributes no dataset row**, and none of its findings is folded into the
1,319 above; doing so would corrupt both this tool's count and the dataset total.
The A/B's own result is recorded in those files and in
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
| Feed state | **not applicable — there is no feed**, so none of the four outcomes applies. A pinned local ruleset checkout rather than a feed; `fetched_at_scan_time: false`, so no scan-time fetch and no reproducibility gap of that kind |
| Exit code | **0**, expected 0 — as expected. No `--error` flag is baked in, so this engine's success code is not turned non-zero by the findings it reported. Cross-checked against the artifact's own `executionSuccessful: true` |
| Elapsed | expected **449 s**, observed **535 s** — recorded, both values. `semgrep.status` `elapsed_seconds=535`; the lane ledger measures the same window as **535.569 s**, 2026-09-01T14:13:07Z to 14:22:02Z (`runner-sequence.json` `invocations[2]`) |
| Finding count | expected **1,162**, observed **1,162** — as expected. Count unit `runs[].results[]`; the tool's own stderr reports `Findings: 1162 (1162 blocking)` |
| Output format | SARIF 2.1.0, artifact `harness/artifacts/raw/semgrep.sarif`, **40,661,984 bytes**, sha256 `7111001f6518803274a80844c2a3d8249edd8f19ba68a771d309fa5d33da03cf`, measured immediately after the invocation returned |
| Parse status | **clean** |
| Records parsed / rejected | 1,162 parsed, **0 rejected**. No rejection class engaged, no parser error |
| Reconciliation, per artifact | `1162 = 1162 + 0` — **pass** |
| Reconciliation, dataset level | contributes to `10016 = 9430 + 586` — **pass** |
| Row validation | pass over this tool's 1,162 rows. `severity_native` is **absent on all 1,162**, banded `Info` on basis `no_vocabulary` |
| Adapter fixture | **pass** — the shared SARIF adapter, `test_sarif_adapter`, 122 tests, exit 0, result OK; per-adapter `verdict` `pass` with no AAP requirement recorded failed |
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
`harness/artifacts/logs/semgrep.stderr.log`. Each line is quoted exactly as the
tool wrote it, and they are the reach-bearing lines selected from that stream
rather than one contiguous span: the first is from the Scan Status box, and the
stream's own `• For a detailed list of skipped files and lines, run semgrep with
the --verbose flag` line sits between the last bullet and the trailing total.

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

**Timestamps, established for this generation.** `started_at` and `finished_at`
**are** established here — **2026-09-01T14:13:07Z to 14:22:02Z**, a 535.569-second
window — and they come from two records rather than from the artifact: the lane
ledger `runner-sequence.json`, at its invocation whose `invocation_index` is **2**
— this tool, second of the nine in canonical tool order — and this runner's own
console stream retained verbatim at
`harness/artifacts/logs/semgrep.runner-console.log`,
which carries `scope_begin`'s header and `scope_finish`'s trailer. The structural
reason a reader might expect them to be missing still holds and is worth stating:
this tool prints its SARIF document to stdout, so stdout belongs to
`harness/artifacts/logs/semgrep.stdout.log` and the console text could not share
it, and the artifact itself emits no `startTimeUtc` or `endTimeUtc`. Capturing the
runner's console to its own file is what makes the pair measurable. Each quantity
the trailer carries is still cross-checked against an independent measurement: the
exit code against the artifact's `executionSuccessful` and the tool's own summary,
the elapsed seconds against the lane ledger's finer reading of the same window,
and the artifact byte count and digest against the measurement taken immediately
after the invocation returned.

**Absent-artifact stderr and verdict**: not applicable — the artifact is present
and parses.

---

## datadog-static-analyzer

`scanner_class`: **sast**, fixed for this tool.

| Field | Value |
| --- | --- |
| Version | observed **0.9.1**, revision `f76636e43554f7f9a8e3984a31d03ec8dea5489f`; expected 0.9.1 revision `f76636e4` — as expected, the observed revision's first eight characters being the abbreviated revision the expected value names. Read from the tool's own Configuration block and corroborated by the SARIF driver version. The release tag carries no leading `v`: `tags/0.9.1` resolves and `tags/v0.9.1` is a 404 |
| Ruleset identity | **three values, all three recorded, and they do not agree.** Observed at `/opt/blitzy-harness/rules/datadog/datadog-sast-rules.json`: **sha256 `c5fd464c2985119574f23599d44022e22b9442d7083acb17ec84addba354f322`, 53 rulesets, 1,147 rules**, 4,068,707 bytes, counted from the file itself and printed by `sha256sum "$DD_SAST_RULES_FILE"` at the gate (`gate-record.json` check `gate.ruleset_identity.datadog-static-analyzer`, `stdout`). Expected by the request's table: **sha256 `e70ede308813b6d8c4087b0995609cdafdb9ab48159a313fe58ac343ff6c44f7`, 48 rulesets, 1,093 rules**. Stated by the inherited environment record: a third digest, **`4f397e81414f8e9469d20abc18c80c85c722e72b9f85b8bcf69dbe34b8fef6f1`, 48 rulesets, 1,093 rules**. The table governs the field it carries; no value is discarded and none is reconciled into another. The tool's own stdout corroborates the observed count from the other direction, printing `#static analysis rules : 1147` and `Rules evaluated: 1147` |
| Comparability | **NOT COMPARABLE WITH THE REHEARSAL.** The observed ruleset digest differs from the expected identity **and** carries 5 more rulesets and 54 more rules, so a different rule set produced this count for reasons that have nothing to do with the code. This tool's finding count must not be read against the rehearsal's figure. The gate records the same difference as `recorded_difference` — one of the three — rather than as a halt, which is where AAP 0.9.3 puts it. The same status is carried in `oss-scan-results/severity-map.md` |
| Reproducibility gap | **NAMED.** The rule set is fetched from Datadog's API at capture time and the publisher supplies no digest for it, so the captured file's own sha256 is the only identity that exists. Provisioning closing that gap — capturing the rules into one local file the runner reads offline with `-r` — is what makes this invocation reproducible at all; it does not make the upstream set identifiable |
| Feed state | **not applicable as a feed**, so none of the four outcomes applies — the rules are a captured local file. `fetched_at_scan_time: false`, proven by the tool's own `config method : none (no local file and no remote configuration)` alongside `-r` pointing at the captured file, so **no API call was made for rules at scan time**. The reproducibility gap this tool does carry is at **capture** time and is named in its own row above; it is not a scan-time fetch |
| Exit code | **0**, expected 0 — as expected. The runner captures the tool's own code at line 53 and exits with it unchanged |
| Elapsed | expected **57 s**, observed **57 s** — as expected. `datadog-static-analyzer.status` `elapsed_seconds=57`; the lane ledger measures the same window as **56.25 s**, 2026-09-01T14:22:02Z to 14:22:59Z, and the tool's own inner measurement reports `Duration: 55.638s`, 0.612 s below the ledger's wall clock |
| Finding count | expected **6,832**, observed **6,832** — as expected. Taken from the parsed artifact's `runs[0].results` length, which equals the tool's own `Total violations: 6832`. The identical count against a ruleset that differs in digest and rule count is recorded as observed and is **not** read as evidence that the two rule sets are equivalent; the not-comparable mark above stands regardless |
| Output format | SARIF 2.1.0, artifact `harness/artifacts/raw/datadog-static-analyzer.sarif`, **5,723,938 bytes**, sha256 `a71dc70d69fa9d93b84eed180e46b568dea98581e25e5cb3ebd5ae4668465372` |
| Parse status | **clean** |
| Records parsed / rejected | 6,832 parsed, **0 rejected**. No rejection class engaged, no parser error |
| Reconciliation, per artifact | `6832 = 6832 + 0` — **pass** |
| Reconciliation, dataset level | contributes to `10016 = 9430 + 586` — **pass** |
| Row validation | pass over this tool's 6,832 rows. This is the only SARIF producer of the three whose results carry a `level`, so it is the only one contributing a non-absent `severity_native`: `error` 195 rows → High, `warning` 1,342 → Medium, `note` 5,275 → Low, `none` 20 → Info, all on basis `sarif_level` (`normalize-run.json` `severity_literals.tools`) |
| Adapter fixture | **pass** — the shared SARIF adapter, `test_sarif_adapter`, 122 tests, exit 0, result OK; per-adapter `verdict` `pass` with no AAP requirement recorded failed |
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

**Ruleset provenance — the bytes this tool scanned with are identified by a
scan-time digest and preserved in the log tree.** `-r "$DD_SAST_RULES_FILE"`
points at a **shared, mutable** path outside this repository,
`/opt/blitzy-harness/rules/datadog/datadog-sast-rules.json`, so the path alone
identifies nothing. What identifies the bytes is that the runner measured them
**before invoking** and printed the digest into its own stream. The three
identities and their sources:

| Identity | sha256 | Rulesets / rules | Where it is recorded |
| --- | --- | ---: | --- |
| **Read by this invocation** | `c5fd464c2985119574f23599d44022e22b9442d7083acb17ec84addba354f322`, 4,068,707 bytes | 53 / 1,147 | printed at scan time by the runner itself — `harness/bin/run-datadog-static-analyzer.sh` lines 37–38 — into `harness/artifacts/logs/datadog-static-analyzer.runner-console.log` as `rules file : /opt/blitzy-harness/rules/datadog/datadog-sast-rules.json (sha256 c5fd464c…)`, a stream the lane ledger binds by byte size and sha256 to this tool's invocation. The same digest is the gate's own `sha256sum "$DD_SAST_RULES_FILE"` reading (`gate-record.json` check `gate.ruleset_identity.datadog-static-analyzer`, `stdout`) and `runner-metadata.json` `ruleset_or_feed_identity.observed_identity`; the counts are from parsing the file — a JSON array of 53 ruleset objects whose `rules` arrays sum to 1,147 — and the tool's own stdout agrees at `#static analysis rules  : 1147` (line 8) and `Rules evaluated: 1147` (line 31) |
| Expected by the AAP | `e70ede308813b6d8c4087b0995609cdafdb9ab48159a313fe58ac343ff6c44f7` | 48 / 1,093 | the request's expected-values table, carried in `runner-metadata.json` `expected_identity` with `identity_matches_expected: false`. Never observed on this host |
| Stated by the inherited environment record | `4f397e81414f8e9469d20abc18c80c85c722e72b9f85b8bcf69dbe34b8fef6f1` | 48 / 1,093 | `harness/ENVIRONMENT.md` lines 111 and 814. An inherited statement about the provisioning, not an observation of this invocation |

**The captured copy is the scan's input, and saying which file it equals is the
whole point.** `harness/artifacts/logs/datadog-sast-rules.captured.json` is
4,068,707 bytes with sha256
`c5fd464c2985119574f23599d44022e22b9442d7083acb17ec84addba354f322` — **byte-identical
to the shared file** (`cmp` reports no difference; both parse to 53 rulesets and
1,147 rules) **and equal to the digest the runner printed before invoking**. So the
rule bytes this tool evaluated are retained inside this run's own evidence tree,
and a reader can compare a finding against the rule that produced it rather than
against a later generation of a moving file.

**What survives as a reproducibility gap, and what does not.** Two things survive,
and neither is repaired: the rule set is fetched from Datadog's API at **capture**
time and the publisher supplies no digest, so the captured file's own sha256 is the
only identity that exists for it — the same kind of gap the `osv-scanner` entry
names for its live API; and `-r` reads a shared mutable path, so a later mutation
would change what a **later** run reads with nothing in the runner to notice it.
What does **not** survive is the traceability claim an earlier generation of this
entry made — that this tool's 6,832 rows cannot be traced to the rule bytes that
produced them. Measured against the files this checkout carries they can, through
the scan-time print and the retained capture, so that claim is withdrawn rather
than softened. The `Comparability` mark stays **NOT COMPARABLE WITH THE REHEARSAL**
on its own separate ground, untouched by any of this: the observed digest is not
the expected digest and the observed 53 / 1,147 is not the expected 48 / 1,093, so
this tool's count differs for reasons that have nothing to do with the code. Two
further facts are measured and neither bears on that mark: the tool made **no API
call for rules** at scan time (`fetched_at_scan_time: false`, and its own
`config method : none`), so the rules it evaluated came from a local file rather
than from a moving endpoint; and its own stdout preserves what that file contained
where it matters most — 1,147 rules over the fourteen languages its
`rules languages` line names, Scala absent — which is a property of the ruleset
recorded in the tool's own words.

**Superseded generation, retained as history rather than dropped.** An earlier
generation of this entry published `4f397e81…` over 48 rulesets and 1,093 rules as
the identity **observed at scan time**, attributed it to
`runner-metadata.json` `ruleset_or_feed_identity.observed_identity`, read the
tool's stdout as `#static analysis rules  : 1093` over twelve languages, and stated
that the bytes "no longer exist — not at that path, not in the log tree, not
anywhere this run can reach". None of those four readings is a measurement of any
file in this checkout: `observed_identity` carries `c5fd464c…, 53 rulesets, 1,147
rules`, the stream reads 1147 at line 8 and `Rules evaluated: 1147` at line 31, its
`rules languages` line lists fourteen, and the bytes are in the log tree at the
captured path above. `4f397e81…` is retained in the table as what it actually is,
the inherited record's own statement.

**One correction that belongs to a file this record may not edit.**
`runner-metadata.json` `tools.datadog-static-analyzer.ruleset_or_feed_identity`
carries `observed_identity` `sha256 c5fd464c…, 53 rulesets, 1,147 rules` beside the
scalar fields `observed_ruleset_count: 48`, `observed_rule_count: 1093` and
`rule_count_matches_expected: true`. Those three scalars are the superseded
generation's counts and disagree with that node's own `observed_identity`, with the
gate's `observed` for the same check, and with the tool's own stdout. **The counts
of record are 53 rulesets and 1,147 rules**, and `rule_count_matches_expected` is
therefore **false** against the expected 1,093 — which is the reading the
`Comparability` row above already applies. `harness/artifacts/` is this run's
published evidence, byte- and digest-exact against `MANIFEST.json`, so the
correction is **stated here naming the file and the fields** rather than applied to
them.

**Not repairable in this checkpoint, and why.** The remaining fix is to have the
runner read a private content-addressed copy instead of a shared mutable path, and
that is an edit to `harness/bin/run-datadog-static-analyzer.sh` line 48 — a runner
edit, which AAP 0.8.1 forbids outright. The runner is **present in this checkout**
and readable, which is how its lines 37–38 and 46–52 are quoted in this entry;
reading it is not editing it. Re-running the scanner to obtain a fresh
rules-and-findings pair is prohibited by the same rule, and AAP 0.6.4 makes a
second invocation a second measurement of a quantity already measured. Nothing was
repaired and nothing here is presented as repaired.

**What a human must do.** At **provisioning** time, before any scan: copy the rules
to a private content-addressed path — `<digest>.json` under a directory this run
owns — point `DD_SAST_RULES_FILE` at that copy, verify the digest before and after
the invocation, and publish it in the run manifest beside the artifact. The
alternative, if the ruleset must come from Datadog rather than from a captured file,
is to attach `DD_API_KEY` and `DD_APP_KEY` so the ruleset is fetched under a
pinnable configuration — a credential provisioning decision, not something this run
may take (AAP 0.3.2 prohibits provisioning a credential). The cost is a provisioning
change plus a re-run of this one tool, whose recorded elapsed time is **57 s**
(`datadog-static-analyzer.status` `elapsed_seconds=57`, the same measurement the
`Elapsed` row above cites), and the
regeneration of every figure that cites its 6,832 rows — which is this tool's row
count, its severity tally in `oss-scan-results/severity-map.md` and its terms in the
reconciliation identity. **Until that is done, two things stay true.** The rule set
is identifiable only by the digest this run measured itself, the publisher exposing
none, so nothing outside this evidence tree can confirm which upstream generation
those 1,147 rules are. And the path the runner reads stays shared and mutable, so
the identification holds for **this** invocation — pinned by the scan-time print
and the byte-identical capture — and gives a later run no protection at all.

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
`harness/artifacts/logs/datadog-static-analyzer.stdout.log`. Each line is quoted
exactly as the tool wrote it; they are the reach-bearing lines selected from
across that stream's lines 8 to 33 — two from the Configuration block, four
`Analyzing` lines and the six-line Static Analysis Summary — rather than one
contiguous span.

```
#static analysis rules  : 1147
rules languages         : java,c#,dart,php,javascript,go,python,rust,swift,apex,ruby,kotlin,bash,typescript
Analyzing 28 JavaScript files using 138 rules
Analyzing 2 Bash files using 35 rules
Analyzing 1149 Python files using 131 rules
Analyzing 591 Java files using 109 rules
  Files scanned: 4085
  Files with violations: 568
  Total violations: 6832
  Rules evaluated: 1147
  Rules with matches: 96
  Duration: 55.638s
```

The `rules languages` line above is the pinned ruleset's own language list as the
tool printed it, and Scala is not among the fourteen. The `Analyzing` lines are the
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
| Feed state | **not applicable — there is no feed**, so none of the four outcomes applies. The rules are compiled into the binary; `fetched_at_scan_time: false`, so no scan-time fetch and no reproducibility gap of that kind |
| Exit code | **2**, expected 2 — as expected, and **not a failure**. `--exit-code 2` is baked in, so per invocation 0 means no leaks and 2 means leaks found; both are successful scans, and the runner keeps the worst code across its 18 invocations. Artifact status is classified on the parse result alone and this artifact is `clean`, so the 2 is recorded as a fact and used for nothing else. Cross-checked two ways: the 18 per-invocation lines in `gitleaks.stdout.log` carry seventeen exits of 0 and one of 2, and the tool's own stderr warns that it found one leak |
| Elapsed | expected **15 s**, observed **14 s** — recorded, both values. `gitleaks.status` `elapsed_seconds=14`; the lane ledger measures the same window as **14.451 s**, 2026-09-01T14:22:59Z to 14:23:13Z |
| Finding count | expected **1**, observed **1** — as expected. The runner's own merge step records `merged 1 findings from 18 per-directory reports`, and exactly one of the 18 retained parts is non-empty |
| Output format | native JSON array, artifact `harness/artifacts/raw/gitleaks.json`, **561 bytes**, sha256 `12d50cf783bb966c77608cae6f93c50c688e0384e84662041ecfb1b6935d8467` |
| Parse status | **clean** |
| Records parsed / rejected | 1 parsed, **0 rejected**. No rejection class engaged, no parser error |
| Reconciliation, per artifact | `1 = 1 + 0` — **pass** |
| Reconciliation, dataset level | contributes to `10016 = 9430 + 586` — **pass** |
| Row validation | pass over this tool's 1 row |
| Adapter fixture | **pass** — `test_gitleaks_adapter`, 93 tests, exit 0, result OK; per-adapter `verdict` `pass` with no AAP requirement recorded failed |
| Scan-target variable | `SPARK_SRC`, set to `/opt/spark-src`, resolved through `scope_resolve_target` at runner line 33 |
| Resolved scan root | `/opt/spark-src`, verified |
| Invocation form | **18 invocations, each handed exactly one root-relative path.** `gitleaks dir` takes exactly one path and silently falls back to the working directory when handed more, which is why the runner loops instead of passing the directory list |
| Side artifacts | **retained and measured, not absent.** `harness/artifacts/logs/gitleaks.parts/` holds **18 members totalling 613 bytes**, one per invocation, each carried in `harness/artifacts/MANIFEST.json` under `logs.files` and each measured by byte size and sha256 from the filesystem immediately after the invocation returned (`runner-metadata.json` `tools.gitleaks.side_artifacts.tree`). Seventeen are 3 bytes and share sha256 `37517e5f…`, the empty JSON array `[]`; the eighteenth, `python_pyspark.json`, is 562 bytes with sha256 `72941b81…` and carries the single finding. `17 × 3 + 562 = 613` closes against the tree total, and one non-empty part against `merged 1 findings from 18 per-directory reports` |
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
rather than a level assumed. It is one of the **2,488** rows in this dataset whose
band rests on basis `no_vocabulary`; the other 2,487 are `opengrep`'s 1,319,
`semgrep`'s 1,162 and `checkov`'s 6.

**Redaction.** `--redact=100` is baked in, which the runner also prints, so the
`Secret` and `Match` fields come through redacted. No matched secret value reaches
the artifact, this document or any dataset field.

**Reduced-reach conditions, in the tool's own words.**
`harness/artifacts/logs/gitleaks.stderr.log` is **26 bytes** (sha256
`98467e49ee1b5e56b9b03a596c97f828f907bf0362096ef2bb74f9a5f5718177`) and carries
exactly one line, which is the tool reporting what it found rather than a
condition about its reach:

```
2:23PM WRN leaks found: 1
```

That prefix is gitleaks' own local-clock format, and it reads `2:23PM` against a
UTC run window of 14:22:59 to 14:23:13 — the same minute in a 12-hour rendering.
It is recorded as observed, since no value here is taken from it and the count it
states agrees with the merge line. **No reduced-reach condition was reported by
this tool.**

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
| Exit code | **1**, expected 1 — as expected, and **not a failure**. This tool exits 1 because it found something; the artifact was written and parses, so artifact status is `clean` and the 1 is recorded as a fact and used for nothing else. `normalize-run.json` carries the same note against this artifact: "runner exited 1 and wrote a parsable artifact. Artifact status and exit status are independent (AAP 0.5.4)" |
| Elapsed | expected **88 s**, observed **93 s** — recorded, both values. `checkov.status` `elapsed_seconds=93`; the lane ledger measures the same window as **93.009 s**, 2026-09-01T14:23:13Z to 14:24:46Z |
| Finding count | expected **6**, observed **6** — as expected. The count unit is `results.failed_checks[]` in the shape that was written, and two independent paths agree: an enumeration of `failed_checks[]` that builds nothing returned 6, and the report's own `summary.failed` is 6 |
| Output format | native JSON, **object form** — one top-level report object with keys `check_type`, `results` and `summary`, `check_type` `dockerfile`. Artifact `harness/artifacts/raw/checkov.json`, **8,380 bytes**, sha256 `91e9cf3cc81e17786af239cba88aa770ae96351a719bd6193ec19962cc238643` |
| Parse status | **clean** |
| Records parsed / rejected | 6 parsed, **0 rejected**. No rejection class engaged, no parser error, and `parsing_errors` is 0 |
| Reconciliation, per artifact | `6 = 6 + 0` — **pass** |
| Reconciliation, dataset level | contributes to `10016 = 9430 + 586` — **pass** |
| Row validation | pass over this tool's 6 rows |
| Adapter fixture | **pass** — `test_checkov_adapter`, 127 tests, exit 0, result OK; per-adapter `verdict` `pass` with no AAP requirement recorded failed |
| Scan-target variable | `SPARK_SRC`, set to `/opt/spark-src`, resolved through `scope_resolve_target` at runner line 28 |
| Resolved scan root | `/opt/spark-src`, verified |
| Invocation form | one invocation carrying **18 `-d` target roots**, one per allowlist directory, each root-relative |
| Side artifacts | **retained and measured, not absent.** `--output-file-path` sends the tool's own report to `harness/artifacts/logs/checkov.out/`, which holds **1 member, `results_json.json`, 8,380 bytes, sha256 `91e9cf3cc81e17786af239cba88aa770ae96351a719bd6193ec19962cc238643`** — measured from the filesystem by byte size and sha256 immediately after the invocation returned (`runner-metadata.json` `tools.checkov.side_artifacts.tree`) and carried in `harness/artifacts/MANIFEST.json` under `logs.files`. Its byte count and digest are identical to the raw artifact's, which is the direct evidence that the runner's copy to `harness/artifacts/raw/checkov.json` rewrote nothing |
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
mutually exclusive. That was determined by measurement over two independent routes
rather than assumed, and both routes are computed from bytes this evidence tree
retains rather than from any second invocation. **Directly, from the artifact
itself**: `harness/artifacts/raw/checkov.json` opens `{`, closes `}` and carries the
three top-level keys `check_type`, `results` and `summary`; it is **8,380 bytes**,
sha256 `91e9cf3cc81e17786af239cba88aa770ae96351a719bd6193ec19962cc238643`, and it is
**byte-identical** to the report the tool itself wrote at
`harness/artifacts/logs/checkov.out/results_json.json`, and
`harness/artifacts/MANIFEST.json` publishes that same size and digest for both — so
the runner copies the tool's `results_json.json` to the artifact path unchanged and
no field was rewritten by the harness. **By byte-size discrimination** against that
recorded 8,380, over the candidate serializations of this invocation's own report:
the single object as written is exactly **8,380**; an array holding that one object
is **8,382**, the same
bytes inside two brackets; the same object re-serialized at indent 4 is **12,648**;
and a multi-framework array would have had to carry the per-directory documents this
tool printed to stdout, of which `harness/artifacts/logs/checkov.stdout.log` retains
**18** — 11 `dockerfile` documents and 7 carrying no `check_type` — in a stream of
**140,105 bytes** (`MANIFEST.json`, `logs.files`). That array is measured under both
serializations a producer could plausibly have written, because the discrimination
must not rest on a formatting choice: with each document's own retained text
preserved and the documents joined inside one pair of brackets, all 18 measure
**140,106 bytes** and the 11 `dockerfile` documents alone **139,140**; re-serialized
compactly instead, with the same separators the artifact itself uses, the same two
arrays measure **92,993** and **92,202**. All four figures are an order of magnitude
away from 8,380, so **only the object form matches, under either serialization** —
and every one of the four is recomputable from the retained stream by parsing its 18
documents, which is why four are published rather than the one pair that happens to
suit the argument.

**One route this paragraph used to take, retired rather than dropped.** An earlier
generation gave as its second route "direct observation of a re-invocation with the
same flags over the same 18 directories". That route is withdrawn: AAP 0.8.1 forbids
re-running a scanner, AAP 0.6.4 makes a second invocation a second measurement of a
quantity already measured, and no file in this evidence tree records such a
re-invocation, so nothing here may rest on one. The byte-size discrimination above
replaces it and needs no second scan. The earlier generation's **92,993** and
**92,202** are *not* withdrawn with it — they are the compact-serialization pair
recomputed above and they reproduce exactly, which is why they are republished beside
the retained-text pair rather than labelled superseded.

**Severity.** `severity` is **null per row** in this unlicensed configuration, so
`severity_native` is absent on all 6 rows and `severity_norm` takes `Info` with
the absence stated (`normalize-run.json` `artifacts[checkov].counters`
`severity_absent: 6`). Those 6 rows are part of the **2,488** rows in this dataset
banded on basis `no_vocabulary`, the rest being `opengrep`'s 1,319, `semgrep`'s
1,162 and the single `gitleaks` row.

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
| Feed identity | observed **vulnerability DB v2 `UpdatedAt=2026-08-30T13:05:01.49156526Z`** (downloaded 2026-08-30T17:47:54.627411305Z) and **java DB v1 `UpdatedAt=2026-08-30T01:07:49.364681226Z`** (downloaded 2026-08-30T17:48:13.944393633Z); expected **vulnerability DB v2, 2026-08-23T06:56:50Z** and **java DB v1, 2026-08-23T01:05:59Z** — **DIFFERS**. Both database versions match (v2 and v1); both timestamps are seven days later than expected. The inherited environment record states a third pair, v2 2026-08-24T06:55:32.451220873Z and v1 2026-08-24T01:07:04.599776272Z. All values are recorded and none is reconciled into another |
| Comparability | **NOT COMPARABLE WITH THE REHEARSAL.** A feed seven days newer resolves a different advisory set, so this tool's counts differ for reasons that have nothing to do with the code. The gate records the same difference as `recorded_difference` — one of the three — rather than as a halt, which is where AAP 0.9.3 puts it. The same status is carried in `oss-scan-results/severity-map.md` |
| Feed identity provenance | Two records that agree. The gate measured it live from the scanner's own output, `trivy --version` (`gate-record.json` check `gate.feed_identity.trivy`, `stdout`), so the timestamps are the ones the scanner itself reports; and the runner dumps the same identity into its console stream before invoking — `harness/artifacts/logs/trivy.runner-console.log`, `vuln db : v2 UpdatedAt=2026-08-30T13:05:01.49156526Z` and `java db : v1 UpdatedAt=2026-08-30T01:07:49.364681226Z` — read from `$TRIVY_CACHE_DIR/db/metadata.json` and `$TRIVY_CACHE_DIR/java-db/metadata.json`. One measurement cited twice |
| Feed state | **not attempted.** The runner bakes `--skip-db-update`, `--skip-java-db-update` and `--skip-check-update`, so no refresh was attempted and the seeded caches were used as found. Of the four outcomes this is the third. `--offline-scan` is also baked in, so no dependency resolution against a remote registry occurs at scan time; there was no scan-time fetch and therefore no reproducibility gap of that kind |
| Exit code | **0**, expected 0 — as expected. All 18 per-directory invocations printed `exit=0`, and the runner keeps the worst non-zero code across them |
| Elapsed | expected **17 s**, observed **17 s** — as expected. `trivy.status` `elapsed_seconds=17`; the lane ledger measures the same window as **16.624 s**, 2026-09-01T14:24:46Z to 14:25:03Z |
| Finding count | expected **3**, observed **3** — as expected. Count unit: one element of one of `Results[].Vulnerabilities[]`, `Results[].Secrets[]` or `Results[].Misconfigurations[]` |
| Per-section counts | **Vulnerabilities 0, Secrets 0, Misconfigurations 3** — against an expected 0 / 0 / 3. The three sit in 3 `Results` members, all `Class` `config` and `Type` `dockerfile`. `0 + 3 + 0 = 3` closes against the record count |
| Output format | native JSON, `SchemaVersion` 2, `ArtifactName` `.`; artifact `harness/artifacts/raw/trivy.json`, **3,496 bytes**, sha256 `979ad0ffbec3502f62ea0e2cd46fae549aaa5e1b7cc4a0d59153a5c2448766ec` |
| Parse status | **clean** |
| Records parsed / rejected | 3 parsed, **0 rejected**. No rejection class engaged, no parser error |
| Reconciliation, per artifact | `3 = 3 + 0` — **pass** |
| Reconciliation, dataset level | contributes to `10016 = 9430 + 586` — **pass** |
| Row validation | pass over this tool's 3 rows. `start_line` is **absent** on all three, which is legitimate: line information appears on secrets and misconfigurations where the section supplies it, and all three of these records carry a `CauseMetadata` with `Provider` and `Service` only |
| Adapter fixture | **pass** — `test_trivy_adapter`, 194 tests, exit 0, result OK; per-adapter `verdict` `pass` with no AAP requirement recorded failed |
| Scan-target variable | `SPARK_SRC`, set to `/opt/spark-src`, resolved through `scope_resolve_target` at runner line 45 |
| Resolved scan root | `/opt/spark-src`, verified |
| Invocation form | **18 invocations**, each `trivy fs` handed exactly one root-relative path, because `trivy fs` takes exactly one path. The runner writes one per-directory report per invocation into `$HARNESS_LOG_DIR/trivy.parts/` and merges the 18 into one report |
| Side artifacts | **retained and measured, not absent.** `harness/artifacts/logs/trivy.parts/` holds **18 members totalling 8,111 bytes**, one per invocation, each measured from the filesystem by byte size and sha256 immediately after the invocation returned and each carried in `harness/artifacts/MANIFEST.json` under `logs.files`. The largest is `resource-managers_kubernetes_docker_src_main.json` at 3,836 bytes — the only in-scope directory holding a Dockerfile, which is where all three misconfiguration records come from; the other 17 are 237–274 bytes of empty-`Results` envelope. The per-member figures are `runner-metadata.json` `tools.trivy.side_artifacts.tree`, whose own `measurement` field states that no absence is claimed the filesystem does not show. Every trivy figure in this entry is measured from `trivy.status`, `trivy.stdout.log`, `runner-metadata.json` or the merged artifact; the parts are cited as retained evidence a reader can open, not as a gap |
| Working directory | `/opt/spark-src` (`cd "$SCAN_ROOT"`, runner line 61) |
| Path base | **scan root**, `/opt/spark-src`; the record field is the enclosing `Results[].Target`, refined by a per-record path or `StartLine` where the section supplies one. Each part states `Target` relative to its own single path argument and names it in its own `ArtifactName`; the merge prefixes every `Target` with that part's `ArtifactName` and sets the merged `ArtifactName` to `.`, so in the merged artifact every `Target` is root-relative. `trivy.runner-console.log` records that step in the runner's own words: `root-anchored 3 of 3 Result Targets by prefixing each part's ArtifactName`. The per-directory parts under `harness/artifacts/logs/trivy.parts/` are **not** root-anchored and must be read with per-section target semantics rather than with this base — a caveat a reader can check directly against those 18 retained files rather than take on trust |
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
| Elapsed | expected **0 s**, observed **0 s** — as expected. `osv-scanner.status` `elapsed_seconds=0`, whole seconds by construction from `scope_finish` subtracting two `date +%s` readings; the lane ledger measures the same window as **0.507 s**, 2026-09-01T14:25:03Z to 14:25:03Z, and the tool's own inner measurement reads `296.87925ms elapsed` |
| Finding count | expected **0** with no artifact written, observed **no artifact** — as expected |
| Output format | **not applicable — no artifact.** The runner's `$ART` would have been `harness/artifacts/raw/osv-scanner.json`, native JSON, and only if packages were resolved |
| Parse status | **absent** |
| Records parsed / rejected | not applicable — no artifact to traverse. Neither figure is set, and neither is written as zero |
| Reconciliation, per artifact | **`not applicable — artifact absent`**. This is the literal recorded value and its status is `not_applicable`. It is **not** a zero-equals-zero pass: no artifact was written, so there is nothing to traverse and no identity to assert |
| Reconciliation, dataset level | contributes nothing to `10016 = 9430 + 586`; it is the one of the nine artifacts counted as absent (`normalize-run.json` `reconciliation.stage_b`, `artifacts_total` 9, `artifacts_present` 8, `artifacts_absent` 1) rather than a term in the sum |
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

**Absent-artifact stderr, verbatim, from one capture and one only.** The
stated reason is on **stderr**, and stdout is empty — both streams belong to the
single invocation at 2026-09-01T14:25:03Z that the lane ledger records, and no
second rendering of this reason exists anywhere in this document or in the
records it cites:

| Stream | Path | Bytes | sha256 |
| --- | --- | --- | --- |
| stderr — carries the reason | `harness/artifacts/logs/osv-scanner.stderr.log` | **967** | `03e42fd9fe0c83921df8bc7f4377231723a69ebad6cf48095fa39e4f7fe31cf5` |
| stdout — empty | `harness/artifacts/logs/osv-scanner.stdout.log` | **0** | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| status | `harness/artifacts/logs/osv-scanner.status` | 254 | `920ba69be84df9436b06ec592ce2ec96b8c6ef52af9cf009503e5280429d6ea8` |

`normalize-run.json` records which stream the reason came from rather than
leaving a reader to guess — `tool_words.stated_reason_stream: "stderr"`,
`stated_reason_present: true` — and records that both streams were searched, with
stdout carrying no text at all. The final three lines of the stderr capture are
the decisive ones, reproduced exactly as the tool wrote them:

```
Starting filesystem walk for root: /
End status: 640 dirs visited, 4735 inodes visited, 0 Extract calls, 296.87925ms elapsed, 296.87957ms wall time
No package sources found, --help for usage information.
```

The eighteen lines preceding them are the tool naming each directory it scanned,
one per allowlist directory, beginning `Scanning dir common/network-common/src/main`
and ending `Scanning dir sql/hive/src/main`.

**Reduced-reach condition, in the tool's own words.** This is the same 967-byte
capture quoted above and not a second one: its `0 Extract calls` over
`640 dirs visited, 4735 inodes visited`, followed by the `No package sources
found` line, is simultaneously this tool's one statement about its own reach and
the statement the verdict rests on. No manifest or lockfile was extracted for
package data. That is the tool saying it found nothing in scope to work on, in
its own words, rather than reporting a failure.

**Completion-versus-failure verdict: COMPLETED WITH NOTHING IN SCOPE, not
failed.** The artifact is absent **and** the tool stated a no-work reason in its
own output, which is the `absent` case: the stderr is quoted verbatim above, zero
rows were emitted, and **the run continues**. The verdict rests on the tool's own
words and on nothing else — the sentence `No package sources found` matched in
the stderr stream, with the exit code 128 agreeing with that statement rather
than establishing it (`normalize-run.json`
`tool_words.no_work_classification`: `classified: true`,
`matched_sentence: "No package sources found"`, `matched_stream: "stderr"`,
`exit_code_agrees_with_statement: true`). That distinction is what decides halt
versus continue, which is why it is made from the words: the alternative — an
artifact absent with **no** stated reason — would have halted the run, and a
termination that produced no exit code would still have fallen under that halt
rather than being excused by an `exit_status`. Neither condition was met here.
The runner's own diagnostic paths were likewise not taken: it exited neither 64
from the argument guard nor 78 from `scope_fail`, so there was no configuration
fault to correct at the gate.

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
adapter-creation decision. The runner `harness/bin/run-osv-scanner.sh` deletes a zero-byte artifact at its lines 53–55,
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

`scanner_class`: **vuln**, fixed for this tool. Its artifact **is** present, parses
in full and carries **zero finding records**, which is a different case from the
absent artifact above and is stated as such below. That zero is a **property of
the scanned scope, not a tool failure and not a reduced capability**: exactly one
manifest-shaped file lies inside the twelve globs and it declares no dependencies,
so there was no package for this tool to resolve an advisory against. The tool
ran to completion, exited 0, analysed the 32 vendored web assets it did find and
reported what it found. Nothing here reads its zero as evidence about the tool.

| Field | Value |
| --- | --- |
| Version | observed **13.0.0**, expected 13.0.0 — as expected. `$DEPENDENCY_CHECK_HOME/bin/dependency-check.sh --version` printed `dependency-check-cli version 13.0.0` (exit 0), re-measured in the checkout, and the artifact's own `scanInfo.engineVersion` reads 13.0.0 |
| Packaging channel | observed **GitHub release, repository `dependency-check/DependencyCheck`, tag `v13.0.0`**, archive sha256 `44d920d1ec03e948df862a253f0912782a31b9beee8a7c8895b9cb95760176ed` — the inherited provisioning record's own measurement, `harness/ENVIRONMENT.md` line 82, which is the file that owns it and is present in this checkout. Recorded as observed rather than as expected: the expected attribution is `jeremylong/DependencyCheck`, which returns 404 for that tag because the project moved. **A Maven Central channel was not observed for this provisioning and is not recorded as one.** Both attributions stand; the version itself matches, so nothing halts |
| Feed identity | observed **keyless NIST NVD JSON 2.0 datafeed at `/opt/blitzy-harness/dc-data`, `NVD API Last Modified 2026-08-30T12:00:19-04`**; expected **keyless NVD datafeed, 2026-08-23T08:00:06-04** — **DIFFERS by seven days**. The inherited environment record states a third value, 2026-08-24T08:00:04-04 over a 239 MB database. All three are keyless NIST JSON 2.0 datafeeds, and every value is recorded |
| Comparability | **NOT COMPARABLE WITH THE REHEARSAL.** A different feed produces a different count for reasons that have nothing to do with the code. The gate records the same difference as `recorded_difference` — one of the three — rather than as a halt, which is where AAP 0.9.3 puts it. The same status is carried in `oss-scan-results/severity-map.md` |
| Feed identity provenance | Measured at the gate from the database file itself and the provisioning log's own text: `$HARNESS_DC_DATA_DIR/odc.mv.db` at **260,005,888 bytes written 2026-08-30T17:48Z**, alongside `jsrepository.json` 549,021 B and `publishedSuppressions.xml` 84,781 B, with the log recording `NVD API Last Modified 2026-08-30T12:00:19-04` (`gate-record.json` check `gate.feed_identity.dependency-check`, `stdout` and `observed`). The gate also records why the identity is taken that way rather than from a `dependency_check_nvd:` grep: this provisioning's log states the field in a different layout, so the grep returns nothing. Corroborated from the other direction by the artifact's own `scanInfo.dataSource` block, the tool stating the identity of the data it used |
| Feed state | **not attempted.** The runner passes `--noupdate`, so no refresh was attempted and the seeded datafeed was used exactly as found. Of the four outcomes — attempted and succeeded, attempted and failed, not attempted, not reported — this is the third. The feed's files were unchanged by the invocation, all of them written 2026-08-30T17:48Z and predating this run's start, so there was no scan-time fetch and this tool contributes no reproducibility gap of that kind |
| Exit code | **0**, expected 0 — as expected. The tool's own status, captured at runner line 58. No `--failOnCVSS` is passed, so the code reflects the run rather than a policy |
| Elapsed | expected **6 s**, observed **7 s** — recorded, both values. `dependency-check.status` `elapsed_seconds=7`; the lane ledger measures the same window as **6.372 s**, 2026-09-01T14:25:03Z to 14:25:10Z. The tool's own phase timings agree — `Created CPE Index (1 seconds)`, `Finished RetireJS Analyzer (1 seconds)`, `Analysis Complete (3 seconds)`. No time limit applies and elapsed time is a fact rather than a budget |
| Finding count | expected **0**, observed **0** — as expected. Count unit `dependencies[].vulnerabilities[]`. Separately, 32 **dependency records** were analysed (`.js` 31, `.json` 1), all 32 matching the twelve globs, with 0 resolved package coordinates. A dependency record is not a finding record and the two are not summed |
| Output format | native JSON report, artifact `harness/artifacts/raw/dependency-check.json`, **17,097 bytes**, sha256 `2861fbf4165b56d1a8f0b6db7a1895f30b452922c7c08521ca00825016097799` |
| Parse status | **clean** |
| Records parsed / rejected | 0 parsed, **0 rejected**. There was nothing to parse under the count unit — `normalize-run.json` `artifacts[dependency-check].counters` records 32 `dependencies` of which **32 carry no `vulnerabilities` array at all** — so no rejection class engaged and no parser error was raised |
| Reconciliation, per artifact | `0 = 0 + 0` — **pass**. A real zero with the artifact **present**, deliberately not the `not applicable — artifact absent` case |
| Reconciliation, dataset level | contributes a zero term to `10016 = 9430 + 586` — **pass** |
| Row validation | not applicable in substance — this tool emitted zero rows, and the dataset-level validation passed with zero rows attributed to it |
| Adapter fixture | **module passes, and the one AAP requirement over it is now SATISFIED — by a second capture rather than by a waiver.** `test_dependency_check_adapter` ran 102 tests, exit 0, result OK, per-adapter `verdict` `pass`, and `adapter-tests-run.json` `positive_mapping.per_adapter.dependency-check.aap_0_6_2_captured_positive_mapping_requirement.status` is **`SATISFIED`** with `status_superseded_value` `FAILED` retained beside it. The measurement that made it FAILED stands and is why the second capture was needed: this tool's whole output for this run holds **32 dependencies, zero vulnerability records and zero package objects**, so no unmodified excerpt of it exercises a single positive field, and its captured fixture yields **zero rows**. A second invocation of the same tool build, same JDK 17, same seeded feed, over input that resolves to packages the feed carries advisories for produced **five vulnerability records over two dependencies**, retained unmodified at `harness/artifacts/logs/dependency-check-positive-capture.json` with its command in the accompanying `.log`, copied byte-for-byte to `oss-scan-results/adapter-tests/fixtures/captured-dependency-check-vulnerabilities.json`, and asserted field by field by `CapturedVulnerabilityFixtureTest`. It contributes **no dataset row**, having been taken outside `harness/artifacts/raw/` over input that is not the pinned tree |
| Scan-target variable | `SPARK_SRC`, set to `/opt/spark-src`, resolved through `scope_resolve_target` |
| Resolved scan root | `/opt/spark-src`, verified; the pinned commit re-verified as `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` |
| Invocation form | one invocation carrying **18 absolute `--scan` paths**, one per allowlist directory, built as `"$SCAN_ROOT/$d"` |
| Side artifacts | **retained and measured, not absent.** `--out` sends the tool's own report to `harness/artifacts/logs/dependency-check.out/`, which holds **1 member, `dependency-check-report.json`, 17,097 bytes, sha256 `2861fbf4165b56d1a8f0b6db7a1895f30b452922c7c08521ca00825016097799`** — measured from the filesystem by byte size and sha256 immediately after the invocation returned (`runner-metadata.json` `tools.dependency-check.side_artifacts.tree`) and carried in `harness/artifacts/MANIFEST.json` under `logs.files`. The tool names that destination itself in its final stdout line, `Writing JSON report to: …/harness/artifacts/logs/dependency-check.out/dependency-check-report.json`, and the member's size and digest equal the raw artifact's, which is the evidence that the copy to `harness/artifacts/raw/dependency-check.json` rewrote nothing |
| Working directory | `/opt/spark-src` (`cd "$SCAN_ROOT"`, runner line 49). cwd is not the path base here: the tool reports absolute paths because it was handed absolute `--scan` arguments |
| Path base | **filesystem-absolute**, relativized against `/opt/spark-src`; the record field is the enclosing `dependencies[].filePath`. 32 of 32 were verified absolute under the scan root, and the dataset emits no absolute path |
| JDK major | **17** — `/opt/blitzy-tools/jdk/jdk-17.0.20+8`, `openjdk version "17.0.20" 2026-07-21`, build `Temurin-17.0.20+8`. Read from the runner rather than assumed, on three independent readings that agree: the runner invokes the tool with `JAVA_HOME="$JAVA_HOME"` at line 51 and states at line 8 that it runs under Temurin 17 with JDK 21 reserved for Joern; `$JAVA_HOME/bin/java -version` re-run in the checkout reports the same; and the JVM that ran the scan, sampled from `/proc` (pid 452914), has `exe` `/opt/blitzy-tools/jdk/jdk-17.0.20+8/bin/java`. Only `exe` and `argv` were read, never `/proc/*/environ` |
| Interpreter | none — a JVM application launched through a shell script; the runner invokes no Python interpreter |
| Heap | **none.** This runner sets no `JAVA_OPTS` and its argv carries no `-Xmx`; it is not one of the four heap-bound JVM invocations, so no heap is claimed for it |
| Credential expression | `printf 'credential      : NVD_API_KEY=%s  (OSS Index analyzer disabled explicitly)\n' "$(scope_cred_state NVD_API_KEY)"` at runner line 45; fixed token only. **`NVD_API_KEY` absent** — and it must stay unset rather than be set to an empty string, since an empty value makes this tool abort with `Invalid API Key, length of 0 too short`. The Sonatype OSS Index credential is likewise absent |

**Evidence lineage — this tool's artifact, streams and status ARE one invocation's
output, and a digest binds them.** Stated here because every figure in the table
above is read from one of those pieces, and a reader is entitled to know which
invocation each came from and what ties it there.

All of it belongs to invocation **8** of the one serial lane
`harness/artifacts/logs/runner-sequence.json` records — its `invocations` entry
whose `tool` is `dependency-check` and whose `invocation_index` is **8**, eighth
of the nine in canonical tool order — run
`w013-20260901T132807Z`, clone index 13, **2026-09-01T14:25:03Z → 14:25:10Z**,
elapsed 6.372 s, exit 0, `argv` the single element
`./harness/bin/run-dependency-check.sh` with `argument_count` **0**. The ledger
measured each piece by byte size and sha256 **immediately after that invocation
returned**, which is what binds those bytes to that invocation and makes a later
substitution detectable:

| Piece of evidence | Bytes | sha256 |
| --- | ---: | --- |
| `harness/artifacts/raw/dependency-check.json` | 17,097 | `2861fbf4165b56d1a8f0b6db7a1895f30b452922c7c08521ca00825016097799` |
| `harness/artifacts/logs/dependency-check.stdout.log` | 2,067 | `59a529f2329ff7c64a33671d764486807e5b955203bd3fb2d3b58f45b37ab814` |
| `harness/artifacts/logs/dependency-check.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| The seven-line runner-written trailer (`dependency-check.status` lines 1–7) | 260 | `a888d8b4ecb7261c70fff7978b5e16867af0047b2c39983057bc12e93a2765a2` |
| `harness/artifacts/logs/dependency-check.runner-console.log` | 1,419 | `b9669824ed10aa96d0008e2ee518651fe921fa344a242374fe2b78bc66412b3b` |
| Side report `harness/artifacts/logs/dependency-check.out/dependency-check-report.json` | 17,097 | `2861fbf4165b56d1a8f0b6db7a1895f30b452922c7c08521ca00825016097799` |

Every pair above is `harness/artifacts/MANIFEST.json`'s published measurement,
which `oss-scan-results/run-record.md` section 16 republishes, and each is
corroborated by the matching field of the ledger's own dependency-check entry —
`artifact`, `stdout_log`, `stderr_log`, `status_file`, `runner_console_log` and
`side_artifacts` — taken there against the same bytes. One measurement cited
twice, and it agrees in both directions.

**Three checks that the pieces belong together, none of them a restatement of
another.** The raw artifact and the side report the tool itself wrote are
**byte-identical**, same size and same digest, which is the evidence that the
runner's copy into `harness/artifacts/raw/` rewrote nothing — and the tool names
that destination in its own final stdout line, `Writing JSON report to:
…/harness/artifacts/logs/dependency-check.out/dependency-check-report.json`. The
artifact's own `projectInfo.reportDate` **2026-09-01T14:25:08.833466245Z** falls
inside that invocation's own 14:25:03Z → 14:25:10Z window, so the report was
written by the process the window belongs to. And the console stream carries the
lane identity in its own header — `run_id=w013-20260901T132807Z clone_index=13`
and `argv=["./harness/bin/run-dependency-check.sh"]` — above a trailer whose
`exit code : 0`, `elapsed seconds : 7` and `artifact … (17097 bytes)` equal the
`.status` trailer's own `exit_code`, `elapsed_seconds` and `artifact_bytes`.

**The data-authenticity gap this entry once recorded (CWE-345) is CLOSED, and the
binding is what closes it.** It was recorded when no digest tied this artifact to
the trailer and the two streams. The ledger's post-return measurement of every
piece in the table above, the byte-identical side report and the in-window
`reportDate` are that tie, so a reader can now verify from the evidence in this
tree — rather than infer it from shape, engine and feed agreeing — that the
committed artifact is the byte output of the invocation the streams describe.
Nothing is narrowed away: the gap was real for the generation it was recorded
against, and it is that generation rather than the finding that was superseded.

**Superseded generation, retained as history rather than dropped.** An earlier
generation of this entry published a different lineage, and its figures are kept
here so that nobody reading the two side by side concludes a number was quietly
removed. It attributed the trailer, at **261 bytes** and sha256
`86406a7e596b496f48f71cf773a0bd8e6c8bbb425a838b94ba4e62e76df935bc`, to an
invocation in clone `w-029_4cc49b` at 2026-08-24T22:38:54Z → 22:39:17Z; it
published the raw artifact at sha256
`ebe98aed11973718591f8c7490eedde86f97bf4fb2047a059e499be50e02c3b9` with
`projectInfo.reportDate` 2026-08-25T00:53:00.948138152Z, against a report measured
at `6b1f18604146bf4e51c8699ab5df9c419a2e915a26681fc2e77f6a6946af7292` with
`reportDate` 2026-08-24T22:39:15.634757586Z; and it read the four `dataSource`
timestamps as `NVD API Last Modified` 2026-08-24T08:00:04-04. **Not one of those
figures is a measurement of any file in this checkout**, and each is superseded by
the pair beside it in the table above and by the `Feed identity` row's
2026-08-30T12:00:19-04. That generation also cited four enriched `.status`
fields — `artifact_sha256_superseded`, `artifact_report_date_superseded`,
`output_directory_retained` and `artifact_superseded_figures_difference` — and
**none of them can be quoted from any file on disk**: commit `0e3e742a5ad`
replaced all nine statuses with the runners' own verbatim trailers, and every one
of the nine now measures seven lines carrying only `tool`, `exit_code`,
`elapsed_seconds`, `artifact`, `artifact_bytes`, `scan_root` and
`scan_root_source`. Where a fact of that generation survives it is cited above
from the record that does carry it; where it does not, it is stated as history in
this paragraph and nowhere else.

**Nothing was re-invoked from here, and the reason is the rule rather than an
absence.** `harness/bin/` and its nine runners, `harness/env.sh`,
`harness/lib/scope.sh` and `harness/ENVIRONMENT.md` are all **present in this
checkout** — they are where the invocation form, the baked flags, the working
directory, the JDK and the credential expression in the table above were read
from. They were read and deliberately not run: AAP 0.8.1 forbids editing a runner
or a baked flag and forbids re-invoking a scanner from here; AAP 0.6.4 makes a
second invocation a second measurement of a quantity already measured; and
`harness/artifacts/raw/` is runner-only, so a second artifact in it would corrupt
both this tool's count and the reconciliation identity. No runner was edited, no
runner was invoked from this record, and nothing in this entry is presented as
repaired by it. The one further invocation of this **tool** that this document
describes — the positive fixture capture below — is deliberately not part of that
scanning lane: it ran the tool's own script directly rather than the runner, over
input that is not the pinned tree, wrote outside `harness/artifacts/raw/`, was not
normalized and contributes no row, which is why the lane stays nine invocations and
this tool's figures stay one measurement each.

**What a human must do.** One item for this tool is open, and it is not the
lineage: its **feed identity differs from the expected-values table by seven
days** — observed `NVD API Last Modified` 2026-08-30T12:00:19-04 against an
expected 2026-08-23T08:00:06-04 — so its counts carry **NOT COMPARABLE WITH THE
REHEARSAL**, and no re-reading of the delivered evidence can lift that mark.
Lifting it takes a provisioning decision this run may not take: seed the datafeed
at the expected timestamp and re-execute, or accept the difference in writing with
both values on the record. Until one of those is taken, the mark stands. If the
choice is to re-seed, the cost is the re-provisioning plus one serialized
nine-runner lane, whose floor is the sum of the nine recorded elapsed times —
**3,104 s (51 m 44 s)**, from
1407 + 535 + 57 + 14 + 93 + 17 + 0 + 7 + 974 = 3,104 s, each addend read from that
tool's own `.status` `elapsed_seconds` field — and, because a fresh
Dependency-Check report carries a fresh `reportDate` and a fresher feed, the
regeneration of every figure in every document that cites this tool.
`runner-sequence.json` measures those same nine windows more finely, at
1407.786 + 535.569 + 56.25 + 14.451 + 93.009 + 16.624 + 0.507 + 6.372 + 974.22 =
3,104.788 s: one lane read at two resolutions, whole seconds by construction in
the trailers and sub-second in the ledger, and not a second lane.

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

**The captured fixture, and the one AAP requirement over it — SATISFIED by a second
capture rather than by a waiver.**
`oss-scan-results/adapter-tests/fixtures/dependency-check.json` is a byte-for-byte
copy of this tool's whole artifact — **17,097 bytes**, sha256
`2861fbf4165b56d1a8f0b6db7a1895f30b452922c7c08521ca00825016097799`, the same digest
as `harness/artifacts/raw/dependency-check.json` — and measured over it directly it
carries **32 dependencies, 0 vulnerability records and 0 package objects**. One
vulnerability record is this shape's count unit, so this capture produces **zero
rows** and exercises no field of the row builder. That measurement stands unchanged,
and it is why a second capture was needed.

**What satisfies AAP 0.6.2, measured rather than asserted.** A second, equally
genuine capture: `harness/artifacts/logs/dependency-check-positive-capture.json`,
**46,684 bytes**, sha256
`ee48683145332f02d5dd101fa0d5fb1b812667b53eec81a97c962b7939911af1`, carrying **2
dependencies and 5 vulnerability records** — produced by a second invocation of the
same tool build (Dependency-Check 13.0.0), the same JDK 17.0.20+8, and the same
seeded NVD datafeed read with `--noupdate` and `--disableOssIndex`. Its exact
command, tool build, JDK, feed timestamps and measured output are retained verbatim
in `harness/artifacts/logs/dependency-check-positive-capture.log`. It was copied
byte-for-byte to
`oss-scan-results/adapter-tests/fixtures/captured-dependency-check-vulnerabilities.json`
— `cmp` reports the two files identical, both 46,684 bytes at that same digest — and
is asserted **field by field** against five hand-verified rows by
`test_dependency_check_adapter.py`'s `CapturedVulnerabilityFixtureTest`.

Those five rows exercise `rule_id`, `message`, three distinct native labels
(`CRITICAL`, `HIGH`, `MEDIUM` mapping to Critical, High and Medium) under
label-over-score precedence with CVSS scores present, filesystem-absolute path
relativization, `cve`, `cwe`, and the package-coordinate candidate precedence at its
first level (`pkg:maven` package URLs). All five carry `in_scope: false` and
contribute **no dataset row**: the capture was taken outside
`harness/artifacts/raw/`, over input that is not the pinned tree, and it is never
normalized. `expected/captured-dependency-check-vulnerabilities.rows.json` holds the
five expected rows.

**Who states the verdict.** `harness/artifacts/logs/adapter-tests-run.json`
`positive_mapping.per_adapter["dependency-check"].aap_0_6_2_captured_positive_mapping_requirement.status`
is **`SATISFIED`**, with `status_superseded_value` **`FAILED`** retained beside it and
a supersession note on every field the new verdict replaced;
`oss-scan-results/adapter-tests/expected/dependency-check.rows.json`
`aap_captured_positive_mapping_requirement.status` is **`SATISFIED`** with the same
statement and the same `2861fbf4…` fixture digest. The `Adapter fixture` row above
cites those two statuses, so this is one verdict cited twice rather than two verdicts.

**Superseded verdict, retained as history.** This passage previously published the
requirement as **UNMET**, the expected file's status as `FAILED`, this fixture's
digest as `ebe98aed…`, and positive mapping as exercised only on
`fixtures/derived-dependency-check-features.json`. All four described the state before
the second capture existed, and the UNMET verdict was correct while the scanning run's
own artifact was the only candidate capture; they are retained rather than deleted so
the change of verdict stays visible. The `ebe98aed…` digest belongs to a superseded
generation of this fixture and matches no file now on disk — the fixture measures
`2861fbf4…`, equal to the raw artifact's, and `adapter-tests-run.json` publishes that
same byte-and-digest pair.

**The derived fixture keeps a role, and it is still declared derived.**
`fixtures/derived-dependency-check-features.json` covers what neither capture reaches:
a record whose severity label is absent so `severity_norm` falls to the CVSS score,
package-coordinate candidate levels 1, 3 and 4 including the within-level
lexicographic tie, and the rejection conditions. It is declared **derived** in its own
expected file and is not offered as this requirement's evidence.

**Nothing in the scanning lane was re-invoked, and the reason is the rule.** The
second capture is not a tenth scanning invocation: it wrote outside
`harness/artifacts/raw/`, contributes no row and was not normalized, so the raw tree
stays runner-only and each of the nine scanning invocations remains the single
measurement of its own quantity (AAP 0.6.4). No runner and no baked flag was touched
(AAP 0.8.1): `harness/bin/run-dependency-check.sh` is present and readable in this
checkout — every line citation in this entry was read from it — and reading a runner
is not editing it. The twelve globs stayed byte-exact (AAP 0.3.2), no fixture was
edited to grow a record, no positive fixture was manufactured, and neither an expected
failure nor a skip is used to make anything vanish from a summary. Nothing here judges
the tool's zero-vulnerability outcome on the pinned tree; that outcome is a property
of the scanned scope, recorded above.

**Absent-artifact stderr and verdict**: not applicable — the artifact is present
and parses. The zero here is a zero **finding count**, not an absent artifact, and
the two are recorded differently on purpose.

---

## joern

`scanner_class`: **sast**, fixed for this tool. This is the only runner whose input
is the code-property graph rather than a directory tree.

| Field | Value |
| --- | --- |
| Version | observed **4.0.607**, expected 4.0.607 — as expected. Read from the **startup banner** with stdin closed, this tool exposing no version flag and its REPL blocking on an open stdin: the banner line is `Version: 4.0.607`, captured at the gate — `harness/artifacts/logs/gate-record.json`, check `gate.tool_version.joern`, command `printf '' | joern | grep -m1 -i version`, `stdout` `Version: 4.0.607`, verdict `pass` — and run from a scratch directory outside the repository so the workspace side effect could not land in the checkout. The separate pre-load gate at `harness/artifacts/logs/joern-preflight.log` records the graph identity check rather than the banner and is cited for that below. The artifact's own `tool_version` field also reads 4.0.607, which agrees but is the runner's claim rather than an independent reading and is recorded as corroboration only. The banner does not appear in `joern.stderr.log` because the runner invokes the tool with a script rather than interactively, and that path prints no banner |
| Query set identity | observed **6 bounded structural queries** baked into `harness/lib/joern-scan.sc`; expected the set baked into the provisioned runner, which the plan expects to be a 58-query bundle bounded to 6 structural queries with the actual count to be read from the runner — **matches**. The count was **read from the runner**, at lines 50–78 where the six entries are declared, and line 111 where the script labels its own output `6 bounded structural queries` |
| Query identifiers | `joern-process-exec`, `joern-unsafe-deserialization`, `joern-reflection-forname`, `joern-message-digest`, `joern-cipher-getinstance`, `joern-xml-factory` |
| Comparability | **comparable** — the observed query-set identity is the expected one |
| Feed state | **not applicable — there is no feed and no ruleset fetch**, so none of the four outcomes applies. `fetched_at_scan_time: false`; no reproducibility gap of that kind |
| Exit code | **0**, expected 0 — as expected. The tool's own code, untransformed by the runner. **Exit 78 was not observed**: had the runner's graph guard fired (lines 44–48, via `scope_fail`) it would have named the missing graph on stderr, which is a configuration fault to correct at the gate rather than an unexplained missing artifact |
| Elapsed | expected **734 s**, observed **974 s** — recorded, both values. `joern.status` `elapsed_seconds=974`; the lane ledger measures the same window as **974.22 s**, 2026-09-01T14:25:10Z to 14:41:24Z |
| Finding count | expected **692**, observed **693** — recorded, both values. Count unit: one element of the artifact's `findings` array. Per query, as the runner printed them into `joern.stdout.log`: `joern-process-exec` 55, `joern-unsafe-deserialization` 178, `joern-reflection-forname` 413, `joern-message-digest` 23, `joern-cipher-getinstance` 11, `joern-xml-factory` 13 — and 55 + 178 + 413 + 23 + 11 + 13 = 693, which the runner's own closing line confirms: `wrote 693 findings to …/harness/artifacts/raw/joern.json`. The single record above the expected figure comes from `joern-reflection-forname`, 413 against the rehearsal's 412; nothing is trimmed to bring the count inside a window |
| Traversal bound | 2,000 per query (`HARNESS_JOERN_QUERY_BOUND`, defaulted at runner line 36). **`bound_reached=false` for all six.** The bound limits traversal work, never the files or modules in scope |
| Output format | native JSON with a `findings` array; envelope keys `tool`, `tool_version`, `cpg`, `graph`, `query_set`, `queries`, `findings`. Artifact `harness/artifacts/raw/joern.json`, **354,817 bytes**, sha256 `bb73a8c657fd31ddf31dc8081f248103e42e2db4fb1b000cca447682c43d8014` |
| Parse status | **partial** |
| Records parsed / rejected | **107 emitted, 586 rejected**, all 586 under the single class **`unresolvable_path`**. **No parser error was raised** — the artifact parses as JSON in full, and the rejections are per-record path-resolution outcomes rather than a parse fault, so there is no parser error text to retain |
| Reconciliation, per artifact | `693 = 107 + 586` — **pass** (`normalize-run.json` `reconciliation.stage_a`) |
| Reconciliation, dataset level | contributes to `10016 = 9430 + 586` — **pass**; every rejected record in the whole dataset is one of these 586, and it is the only artifact of the nine whose parse status is `partial` |
| Row validation | pass over this tool's 107 rows. 78 take `in_scope: true` and **29 take `in_scope: false` and are kept**, being source coordinates that resolve outside the twelve globs (`common/utils` 14, `common/unsafe` 6, `launcher/src` 4, `common/utils-java` 3, `streaming/src` 2). **No row resolved into a `src/test` tree** — the counter reads 0 |
| Adapter fixture | **pass** — `test_joern_adapter`, 117 tests, exit 0, result OK, per-adapter `verdict` `pass`. One AAP case this artifact **cannot supply** is recorded rather than glossed: AAP 0.5.4 and 0.6.1 require a fixture asserting that a finding resolving into a `src/test` tree is retained with `in_scope: false` rather than dropped, and **no finding in this artifact names a `Suite` or `Test` class**, so none resolves into `src/test` and the case cannot be captured — which `normalize-run.json` corroborates from the other direction with `rows_from_src_test: 0`. It is exercised on `oss-scan-results/adapter-tests/fixtures/derived-joern-features.json`, declared derived in its own expected file, and the derivation is recorded rather than presented as a capture |
| Scan-target variable | `SPARK_SRC`, set to `/opt/spark-src`, resolved and verified. The scanned **input**, however, is the graph, passed through `HARNESS_CPG` |
| Resolved scan root | `/opt/spark-src`, verified |
| Invocation form | one invocation. No filesystem target appears on the command line: the graph path, the output path and the bound are passed through the environment and the script through `--script` |
| Working directory | `/tmp/blitzy-harness-scratch/13/joern-run`, recorded verbatim by this invocation as `workspace : /tmp/blitzy-harness-scratch/13/joern-run (outside the repository; joern writes ./workspace)` in `harness/artifacts/logs/joern.runner-console.log`. The runner expresses it as `cd "$WORKDIR"` at `harness/bin/run-joern.sh` line 65 over `$HARNESS_SCRATCH_DIR/joern-run`, and `harness/env.sh` line 38 derives `HARNESS_SCRATCH_DIR` as `/tmp/blitzy-harness-scratch/${BLITZY_CLONE_INDEX:-0}` — this lane's clone index is **13**, so the console's value is the one of record. It is **the one runner whose working directory is not the scan root**, deliberately: this tool exposes no workspace flag and writes its workspace into whatever directory it runs from, so the runner works in the per-clone scratch directory and never in the repository |
| Path base | **bytecode class**, with **no value** — no filesystem base exists for this tool's records, and none was invented. The emitted `file` field is the frontend's ephemeral `/tmp/jimple2cpg-<id>/<pkg>/<Class>.class` extraction path and can never be a path in the Spark tree, so the `class` field is the only resolvable coordinate — `coordinate_from_class` is **693 of 693** records and `coordinate_from_class_file` is 0. Resolution is against `src/main` **and** `src/test` under the pinned root, taken only where unique: it succeeded for 107 records (`resolution_from_class` 107) and the other 586 were rejected |
| JDK major | **21** — `/opt/blitzy-tools/jdk/jdk-21.0.12.1+1`, `openjdk version "21.0.12.1" 2026-08-18 LTS`, VM `21.0.12.1+1-LTS`, matching the expected Temurin build with no patch difference to record. Taken from `java.specification.version` — the JVM's own property output — rather than off a banner. Two independent pins agree: the runner sets `JAVA_HOME="$JAVA_HOME_21"` and asserts that JDK usable before invoking, and the `joern` launcher on `PATH` is a provisioning wrapper that pins the same JDK. A wrong major here halts the run; a patch difference with the correct major is recorded with both values |
| Interpreter | none — the runner invokes no Python interpreter |
| Credential expression | **none.** This runner reads no credential and calls `scope_cred_state` nowhere |

**Baked flags, as read** at scan time from `harness/bin/run-joern.sh` lines 67–71:
`--script harness/lib/joern-scan.sc`, `-J-Xmx"$HARNESS_JOERN_HEAP"`, and stdin
redirected from `/dev/null`. `SL_LOGGING_LEVEL` is set to `WARN`, because the
default level floods the artifact. None of these is an anchor. **Both files named
in that sentence — the runner and the script it invokes — are present in this
checkout and readable**, so the reading above is re-derivable rather than taken on
trust: `harness/bin/run-joern.sh` lines 67–71 carry the invocation verbatim,

```
JAVA_HOME="$JAVA_HOME_21" SL_LOGGING_LEVEL="${SL_LOGGING_LEVEL:-WARN}" \
  HARNESS_SCAN_CPG="$CPG_REAL" HARNESS_SCAN_OUT="$ART" HARNESS_SCAN_BOUND="$BOUND" \
  joern --script "$SCRIPT" \
    -J-Xmx"$HARNESS_JOERN_HEAP" \
    < /dev/null > "$OUT" 2> "$ERR"
```

with `harness/bin/run-joern.sh` line 70 the `-J-Xmx` site, and `harness/lib/joern-scan.sc` lines 1–6 describe
the baked set as six structural queries in the script's own words. The same facts
are held structurally by `runner-metadata.json`
`tools.joern.invocation_form.literal`, whose `source_lines` field reads
`harness/bin/run-joern.sh lines 67-71` and equals the five lines above token for
token, and by `tools.joern.baked_flags`. **The `.status` trailer is not a source
for any of this** and is not cited as one: like the other eight it is seven lines
carrying only `tool`, `exit_code`, `elapsed_seconds`, `artifact`,
`artifact_bytes`, `scan_root` and `scan_root_source`, so it holds no command, no
`command_source_lines` and no line 274. An earlier generation of this entry
quoted an enriched `joern.status` at lines 274–275 for the command; commit
`0e3e742a5ad` replaced all nine statuses with the runners' own verbatim trailers,
which is why that quotation is recorded here as history and the runner itself is
cited in its place. Reading the runner is not editing it: AAP 0.8.1 forbids
editing a runner or a baked flag, and nothing here was edited.

**Heap actually used: 64 GB**, as `-J-Xmx64g` (68,719,476,736 bytes), which the
runner also prints into its own stream. The mechanism is `HARNESS_JOERN_HEAP`, the
runner's own documented environment override applied at `harness/bin/run-joern.sh` line 70 — a runtime value
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

**Graph identity, measured either side of the load and identical.** The named path
`harness/cpg/spark.cpg` is a 33-byte symlink resolving to the regular file
`/opt/blitzy-harness/cpg/spark.cpg`; both AAP 0.6.4 names are the same file. Byte
size **541,309,809** and sha256
`4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7` were measured
at **2026-09-01T14:25:10Z**, when this invocation began, and again at
**14:41:24Z**, when it returned, and the two measurements are identical
(`runner-sequence.json`, at its invocation whose `invocation_index` is **9** — this
tool, last of the nine — fields `graph_identity_before_load` and
`graph_identity_after_load`; the link-only 33-byte measurement is recorded only to
discard it in favour of the symlink-following size). The runner prints the same
three facts into its own stream before invoking — `cpg`, `cpg bytes` and
`cpg sha256` at `harness/bin/run-joern.sh` lines 56–58, which the runner is present
in this checkout to show, landing at
`harness/artifacts/logs/joern.runner-console.log` lines 13–15 as `541309809` and
`4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7`. The dedicated
pre-load gate `harness/lib/preflight_graph_identity.py`, whose output is retained
at `harness/artifacts/logs/joern-preflight.log`, compares those same bytes against
the graph's record of account at
`/opt/blitzy-harness/provision-log/cpg-identity.txt` and returns **VERDICT: PASS**
with both size and sha256 marked `MATCH`; that gate ran at 14:52:54Z, after this
invocation rather than before it, and is cited for what it establishes — that the
bytes read and the record of account agree — rather than as the thing that gated
this particular load. The load used `importCpg`, three occurrences of it against
**zero occurrences of `importCode`** in the query script, and reported
`methods=1396899 typeDecls=119721 files=45037` — more than zero methods. The graph
was **not** written by this run: the frontend in this clone reached the flatgraph
serialization ceiling, so the persisted graph is provisioning's, dated
2026-08-30, and this invocation read it without rebuilding it.

**One disagreement about that identity, recorded rather than smoothed over.** The
inherited environment record — `harness/ENVIRONMENT.md`, lines 284–287, restated in
its own inlined-values block at lines 841–846 — states a different graph:
**541,255,894** bytes, sha256
`26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc`, with
1,397,339 methods and 119,691 type declarations, against the
541,309,809 / `4616845a…` / 1,396,899 / 119,721 measured on disk and read by this
load. The gate records that as one of its **two halts**,
`gate.environment_record_graph_identity_agreement`, rather than as a tolerated
difference: it is an observable fact contradicting the record on an inherited
field. Both values stand here with their provenance, neither is reconciled into
the other, and the wider account of the divergence belongs to
`oss-scan-results/run-record.md`.

**Rejections, all 586 under `unresolvable_path`.** Each rejected record is a
bytecode class with no source coordinate in the pinned tree — third-party classes
shaded into Spark's JARs. Projecting the rejection records' own class fields
gives `org.sparkproject` 528, `org.apache` 44, `com.google` 12, `org.fusesource`
1 and `org.rocksdb` 1, which sums to 586. The first rejection's own detail states the rule as
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
graph loaded: methods=1396899 typeDecls=119721 files=45037
query joern-process-exec               returned     55 bound_reached=false elapsed_ms=1124
query joern-unsafe-deserialization     returned    178 bound_reached=false elapsed_ms=16
query joern-reflection-forname         returned    413 bound_reached=false elapsed_ms=12
query joern-message-digest             returned     23 bound_reached=false elapsed_ms=1
query joern-cipher-getinstance         returned     11 bound_reached=false elapsed_ms=0
query joern-xml-factory                returned     13 bound_reached=false elapsed_ms=1
wrote 693 findings to <checkout>/harness/artifacts/raw/joern.json
```

That block is `harness/artifacts/logs/joern.stdout.log` lines 127–134, quoted with
the absolute checkout prefix on the last line abbreviated and nothing else
altered.

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
It ran from **2026-09-01T19:41:23Z to 19:41:28Z** and exited **0**, outcome
`completed`, with `reconciliation.passed` true, no failures and no halt. It uses
the standard library only, so it introduces no manifest, no lockfile and no
install step. Stages A and B are established **before** either output file is
written, so a dataset whose identity already failed would never have reached disk.
The parse status, record counts and reconciliation results in every entry above
are this run's measurements (`harness/artifacts/logs/normalize-run.json`).

**The adapter tests.** Command
`/usr/bin/python3 -m unittest discover -s oss-scan-results/adapter-tests`, run
from 2026-09-01T23:32:23Z to 23:32:36Z, suite exit **0**, result **OK**.
`unittest` reported **1,325 tests** in **13.104 s** (**13,104 ms wall**), with
**26,008 subTests** and 0 failures, 0 errors, 0 skips, 0 expected failures and
0 unexpected successes. Interpreter `/usr/bin/python3` at **3.13.7**, the same
base interpreter as the normalizer and independent of every scanner's
environment. 10 test modules, each run on its own and each exiting 0, whose own
counts sum to the suite total — `test_checkov_adapter` 127,
`test_cli_writers` 219, `test_dependency_check_adapter` 102,
`test_emit_publication` 75, `test_gitleaks_adapter` 93, `test_joern_adapter` 117,
`test_reconciliation` 162, `test_sarif_adapter` 122,
`test_shape_routing_negative` 114 and `test_trivy_adapter` 194, giving
127 + 219 + 102 + 75 + 93 + 117 + 162 + 122 + 114 + 194 = 1325. The corpus is
**105 fixtures** and 105 **expected files** in one-to-one correspondence, of
which **72 negative fixtures** drive the rejection conditions. A failed adapter
fixture, rejection or reconciliation test is a condition that **stops the run**;
no executed test failed, and no result here is recorded as a soft warning, a
known failure, an expected failure or a skip
(`harness/artifacts/logs/adapter-tests-run.json`, `suite_result`,
`test_modules.entries` and `inputs`).

Every adapter-test figure in this document is that one record's, and the equality
is **enforced rather than asserted**: `harness/lib/verify_status_figures.py`
reads `adapter-tests-run.json` and requires every test count, subtest count,
elapsed reading, module count, fixture count and addend expression restated here
to equal one of its measurements, exiting non-zero on any drift. Its last run
checked **44 replicated figures with 0 drifted**. The count of figures moves with
how many replicated figures these documents carry, so the durable claim is the
**zero** rather than the count: this document's own nine-tool elapsed sum became one
of the checked figures only once its addends were rewritten without thousands
separators, a separator inside an operand being enough to put an expression beyond
the gate's reach.

A second gate covers what that one does not. `harness/lib/verify_publication_owners.py`
enforces AAP §0.6.4's ownership rule across a wider surface than numeric figures:
the invocation commands, the run windows, the stage chronology, the absolute
repository root, the Dependency-Check fixture disposition, each runner
side-artifact tree's measured state, the frontend's nested-archive subtotal, the
probe revision triple, and the requirement that while the gate's verdict is
`halt` no stage is published as complete. For each it reads the owner at run time,
reads the copy out of the document that publishes it, and exits non-zero naming
both sides on any disagreement — so a document that has drifted from its owner is
not publishable. It also fails on a projection that is *absent*, because an
omitted copy is how a value silently stops being checked.

It additionally adjudicates the **locator** of a citation rather than only the value
it carries, across all five result documents, in three families: every
`<tool>.status` field name against the seven the trailers actually hold, every line
citation into this run's own surface against the cited file's measured length, and
every path published as absent against the filesystem. That gap is the one this
checkpoint was opened on — a commit correctly replaced the nine enriched statuses
with the runners' verbatim trailers and correctly restored sixteen deleted files, and
left prose citing fields and line numbers that had ceased to exist and asserting that
restored files were absent, none of which either gate could then see. Each family
distinguishes a **live** citation from one the document is retracting, so a sentence
naming a superseded locator in order to warn a reader off it does not fail; the
retractions are counted and printed rather than hidden, and a retraction only excuses
a citation sitting in **its own clause** — a paragraph that mentions an earlier
generation for some other reason no longer exempts a live citation elsewhere in it.
Its last run checked **92 owner/copy pairs with 0 disagreeing**, over 12 live field
citations, 169 live line citations and 5 absence claims, with 1 line citation retracted
as history. Here too the invariant is the zero: the pair count grows as each further
owner/copy relationship is brought under the gate.

**Recognising nothing is not the same as finding nothing, so the gate counts what it
read.** A first version of the line-citation family looked ahead a fixed number of
characters from a filename to the word "line", and these documents do not write
citations that way: `joern-probe.md` states a digest and a byte count between the
filename and the locator, so that family read **zero** citations in a document holding
eleven of them and reported clear — and a mutation of two of them to lines that do not
exist passed. Both citation families are therefore attributed **structurally** now, each
reference to the nearest preceding backticked filename inside its own table row or
paragraph, with `runner line N` resolved through its own section's heading rather than
through document order. And the population is asserted rather than assumed: a second,
deliberately different traversal of each document — flat and unscoped for line
references, forward-from-filename for field claims where the attribution works
backward-from-claim — must agree with the classification exactly. Its last run
classified **300 of 300** line references and **12 of 12** field claims, so a citation
form the scoped path cannot read now surfaces as a count mismatch instead of as
silence. The gate also carries its own negative cases, one per citation form per family
including the `joern-probe.md` form above: `python3 harness/lib/verify_publication_owners.py
--self-test` runs **29** of them and passes only if each family refuses every defect it
exists to catch and accepts every form these documents legitimately use.

Both gates exist because every measurement re-taken during this work moved several
published copies at once, and each stale copy was previously found only by someone
happening to look.

**One requirement that record previously carried as FAILED, and how it became
SATISFIED.** The suite passing is not the same claim as every AAP requirement
over it being satisfied, so the two are still stated separately.
`positive_mapping.per_adapter` `dependency-check`
`aap_0_6_2_captured_positive_mapping_requirement.status` is now **`SATISFIED`**,
with `status_superseded_value` **`FAILED`** retained beside it and the route
recorded in full.

**The measurement that made it FAILED is unchanged and is why the fix took the
shape it did**: this tool's own artifact for this run holds **32 dependency
records, zero vulnerability records and zero package objects**, so no unmodified
excerpt of *it* can exercise a single field of the row builder, and its captured
fixture yields **zero rows**. That is a fact about the scanned scope — the twelve
authoritative roots contain no dependency manifest — and nothing here judges the
tool for reporting nothing.

**What satisfied it was a second capture, not a waiver.** A second invocation of
the *same* tool build, under the same JDK 17 and the same seeded feed, over input
that resolves to packages the feed carries advisories for, produced a report with
**five vulnerability records over two dependencies**. It is retained unmodified at
`harness/artifacts/logs/dependency-check-positive-capture.json` with its exact
command in the accompanying `.log`, copied byte-for-byte to
`oss-scan-results/adapter-tests/fixtures/captured-dependency-check-vulnerabilities.json`,
and asserted field by field by `CapturedVulnerabilityFixtureTest` against five
hand-verified expected rows. It is **genuine unmodified captured output of this
tool**, which is what AAP 0.6.2 asks for; it contributes **no dataset row**, having
been taken outside `harness/artifacts/raw/` over input that is not the pinned tree.

Feature coverage is split accordingly: the numeric CVSS banding, the case folding
and the score selection are exercised on
`oss-scan-results/adapter-tests/fixtures/derived-dependency-check-features.json`,
declared derived in its own expected file and never presented as captured output,
while the label-over-score precedence, the relativization, the identifier
selection and the twelve-field row shape are exercised on the captured report —
so each of those paths runs against genuine tool output. The requirement of
AAP 0.6.2 and 0.9.4 is therefore **met at this milestone**, and the zero-record
scope measurement stands beside it rather than under it.

**The suite grew by 183 against the generation this document previously reported** —
from a suite total of 1,142 to one of 1,325 — and every figure above is the new
measurement rather than the old one adjusted. Three modules carry the
growth: `test_cli_writers` 59 → 219, `test_sarif_adapter` 108 → 122 and
`test_emit_publication` 66 → 75. The other seven modules' counts are unchanged. Two of the new tests rest on **two new
committed negative fixtures**, both for the shared SARIF adapter, each with its own
hand-verified expected file:

| Fixture | Bytes | What it asserts | Rejection class | Identity |
| --- | ---: | --- | --- | --- |
| `oss-scan-results/adapter-tests/fixtures/reject-sarif-rule-index-mismatch.sarif` | 6,659 | a result whose `ruleId` contradicts the rule its `ruleIndex` names is **rejected**, not emitted under either identifier | `malformed_record` | `3 = 2 + 1` |
| `oss-scan-results/adapter-tests/fixtures/reject-sarif-percent-encoded-control.sarif` | 16,199 | a percent-encoded control character in a URI reference is **rejected after decoding**, the raw-form check alone not being the guard it looks like | `invalid_uri` | `6 = 2 + 4` |

Both are rejection **routes that did not exist in the adapter before this
checkpoint**, so their fixtures exercise behaviour rather than restate it: the
first is the two-route rule-identifier lookup refusing a record whose two
descriptors disagree, and the second is the control-character check re-made after
every percent-decode. Neither route was reached by any of the nine artifacts this
run normalized, which is why the fixtures exist at all — a rejection path with no
test is a rejection path nobody has exercised (AAP 0.6.2). The negative-fixture
corpus is now **72** over **105** committed fixtures and 105 expected files.

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
| Rule count | `gitleaks` | The rule set is not separately versioned, the tool reports no count, and the expected-values table carries none |
| Ruleset digest | `gitleaks` | The rules are compiled into the binary; no digest exists to compare and none was invented |
| Policy count | `checkov` | The bundled policies are not separately versioned and the tool reports no count; the expected-values table carries none |
| Policy digest | `checkov` | Bundled policies carry no separate version or digest, and none was invented |
| Path base value | `joern` | No filesystem base exists for a bytecode-class coordinate. The base **kind** is recorded and the resolution route — the `class` field against `src/main` and `src/test` — is recorded; a plausible path was not invented in place of the missing value |

One value this table carried in an earlier generation is **no longer unestablished
and has been removed rather than left standing**: `semgrep`'s `started_at` and
`finished_at`. Capturing each runner's console stream to its own file
(`harness/artifacts/logs/<tool>.runner-console.log`) and recording the lane in
`runner-sequence.json` makes the pair measurable for every one of the nine, and
`semgrep`'s is 2026-09-01T14:13:07Z to 14:22:02Z. The five rows above are the whole
of what could not be established.

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
  the environment record never overrides it. Four consequences are visible above.
  **Where the table, the record and observation all differ, all three are
  recorded** and none is reconciled into another — `datadog-static-analyzer`'s
  ruleset digest is that case at three values, and `trivy`'s two database
  timestamps and `dependency-check`'s feed timestamp are the same case at three
  values each. **Where a difference changes what a count means, the tool is marked
  not comparable with the rehearsal** — those same three tools, each marked in its
  entry and in `oss-scan-results/severity-map.md`, and each recorded by the gate
  as a `recorded_difference` rather than a halt. **Where an observable fact
  contradicts the record on an inherited field the table does not carry, the
  contradiction is a halt and is reported as one** — the graph identity, one of
  the gate's two halts, stated in the `joern` entry with both values. And **where
  a difference is an output this run deliberately produced** rather than an
  inherited fact contradicted, both values stand with their provenance and nothing
  halts: every artifact byte count and digest in this document is this run's own
  measurement, taken immediately after its invocation returned, and differs from
  the record's figures for the rehearsal's separate invocations by construction.
