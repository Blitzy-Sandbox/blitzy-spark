#!/usr/bin/env python3
"""Assert that every replicated adapter-test figure equals its one authoritative measurement.

WHY THIS EXISTS
===============
AAP 0.6.4 states the rule plainly: "A count that appears in two documents must be one
measurement cited twice, never two measurements." The adapter-test suite's figures are the
worst offenders in practice, because they appear in three places for three different
reasons -- the machine-readable run record, the per-tool status document, and the run
index -- and because they change whenever a test is added. A count restated by hand drifts
the moment the suite grows, and a drifted count is indistinguishable from a measurement
nobody took.

Two counts had already drifted twice by the time this module was written: a subtest total
of 14,028 restated in the run index against an authoritative 14,035, and three different
elapsed readings (1.907 s, 1.966 s, 2.011 s) for one run. Correcting them by hand is what
produced the second drift, so the check is committed rather than performed.

THE AUTHORITATIVE SOURCE, AND WHY IT IS THE ONE IT IS
=====================================================
``harness/artifacts/logs/adapter-tests-run.json`` is written by the invocation itself and
carries the suite's own trailer verbatim. Its ``suite_result`` and ``test_modules.entries``
are therefore the measurement; every other appearance of those numbers is a citation. This
module reads the authoritative values from that file and then requires every figure it can
find elsewhere -- including elsewhere IN THAT SAME FILE, since its prose restates its own
measurements -- to equal one of them.

TWO THINGS A CITATION CHECK ALONE DOES NOT CATCH
================================================
A record that disagrees with ITSELF settles nothing, and the citation rules above cannot
see it: they compare a figure in one file against a value in another, so a record whose
prose says one total while its own arrays hold another passes every one of them. That is
not hypothetical. One edition of the run record carried three prose statements reading
"all 1142 of them" and "1142 entries for 1142 executed tests" beside its own
``suite_result.tests_run`` of 1325, a ``per_test_outcomes.entries_count`` of 1325 and a
1325-entry array, and two ``mandated_tests`` module totals of 73 and 110 against modules
that ran 114 and 162. Nothing failed, because nothing compared the record to itself.

So the second family of checks (``check_record_self_consistency``) requires the record to
agree with itself before it is allowed to adjudicate anything: the four ways it states its
own test total must be one number, the per-module breakdown and the status histogram must
sum to it, every ``mandated_tests`` module total must equal the module entry it names, each
entry's outcomes must sum to that entry's own count, and -- the rule that would have caught
the 1142 -- every test-count claim in the record's own PROSE is adjudicated rather than
trusted. A prose figure is legitimate only if it is one of the measurements the record
carries, or if it is DECLARED under ``historical_figures_quoted_in_prose`` as a figure
quoted from a superseded edition on purpose. That declaration is what separates a
deliberate quotation, which is how a correction stays visible, from a claim left behind by
a measurement that moved; an undeclared figure fails, and a declared figure that no longer
appears anywhere fails too, so the declaration cannot rot into a blanket exemption.

The third family (``check_documented_commands``) closes the other half of the same defect.
``oss-scan-results/adapter-tests/README.md`` documents the invocation a reader is told to
reproduce and states that it is the recorded one "in every argument", the one difference
being the executable. That claim was false in the same edition: the README documented
``-p 'test_*.py' -v`` while the record's ``command`` carried neither argument, even though
that record's own ``captured_streams`` held a verbose per-test stream only ``-v`` produces.
A documented command and a recorded command that differ in any argument mean the run a
reader reproduces is not the run that was measured, so the two are compared token by token
after resolving ``python3`` to the interpreter path the record names -- for the whole-suite
form, and for every per-module form, each of which must be the documented per-module
template with its pattern swapped for that module's own filename.

WHAT IT CHECKS, AND WHY EACH RULE IS SHAPED THE WAY IT IS
=========================================================
A test-count figure is legitimate at two granularities: the whole suite, or one module. So
the rule is membership in the authoritative SET rather than equality with the total -- a
document may legitimately say "98 tests" of one module. The same applies to the verbatim
trailer, which exists per module as well as for the suite.

The addend rule is deliberately two-directional. Correcting only the sum of an expression
leaves a sum its own addends do not produce, which is how ``98+81+75+112+70+88+63+119 =
695`` survived a repair. So an expression whose operands are the module counts must sum to
the suite total, AND an expression whose right-hand side is the suite total must have the
module counts as its operands. Addend expressions about anything else -- the JAR inventory's
``191 + 436 + 0 = 627``, Trivy's ``0 + 3 + 0 = 3`` -- are out of scope and are skipped by
that same operand test rather than by a hardcoded exclusion list.

Every pattern requires a leading DIGIT and forbids a following hyphen or word character.
Without the first, ``", rejection or reconciliation test"`` matches with a comma as its
"number"; without the second, ``"14 test-resource fixtures"`` and ``"test-framework
classes"`` are read as test counts. Both were observed while this module was being written.

OUTPUT AND EXIT STATUS
======================
Every figure found is printed with its file, line, rule and verdict, so the coverage is
auditable rather than implied by a violation count of zero -- a checker whose patterns match
nothing also reports no violations. The two families above print the same way, each
assertion naming what it compared, so an assertion that silently stopped being made shows
as a smaller audit rather than as a pass. Exit 0 when every figure and every assertion
agrees, 1 otherwise, with each violation named.

Usage:  python3 harness/lib/verify_status_figures.py [--quiet]
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Final

#: The record written by the invocation itself. Everything else cites it.
AUTHORITY: Final[str] = "harness/artifacts/logs/adapter-tests-run.json"

#: Files whose figures are citations of that record -- including the record's own prose.
SCANNED: Final[tuple[str, ...]] = (
    AUTHORITY,
    "oss-scan-results/tool-status.md",
    "oss-scan-results/run-record.md",
)


def repo_root() -> Path:
    """Locate the repository root from this file's own position, or HARNESS_REPO_ROOT."""
    env = os.environ.get("HARNESS_REPO_ROOT")
    if env:
        return Path(env).resolve()
    return Path(__file__).resolve().parent.parent.parent


def _int(text: str) -> int:
    """Parse a figure as written in prose, where thousands may carry commas."""
    return int(text.replace(",", ""))


class Authority:
    """The authoritative measurements, read once from the run record."""

    def __init__(self, root: Path) -> None:
        path = root / AUTHORITY
        data = json.loads(path.read_text())
        suite = data["suite_result"]
        entries = data["test_modules"]["entries"]
        inputs = data["inputs"]

        self.tests_run: int = suite["tests_run"]
        self.subtests: int = suite["subtests_recorded"]
        self.seconds: float = suite["unittest_reported_seconds"]
        self.wall_ms: int = suite["wall_clock_ms"]
        self.module_tests: list[int] = [e["tests_run"] for e in entries]
        self.module_seconds: list[float] = [e["unittest_reported_seconds"] for e in entries]
        self.modules_present: int = data["test_modules"]["modules_present"]
        self.fixtures: int = inputs["fixtures_present"]
        self.expected_files: int = inputs["expected_files_present"]
        self.negative_fixtures: int = inputs["negative_fixtures_present"]

        # The authority must agree with itself before it can adjudicate anything else.
        if sum(self.module_tests) != self.tests_run:
            raise SystemExit(
                f"{AUTHORITY} is internally inconsistent: its module counts "
                f"{self.module_tests} sum to {sum(self.module_tests)}, not {self.tests_run}. "
                f"An authority that disagrees with itself cannot settle a citation."
            )
        if len(entries) != self.modules_present:
            raise SystemExit(
                f"{AUTHORITY}: modules_present is {self.modules_present} but it carries "
                f"{len(entries)} entries."
            )

    @property
    def any_test_count(self) -> set[int]:
        """Counts a document may legitimately state: the suite's, or one module's."""
        return {self.tests_run, *self.module_tests}

    @property
    def any_seconds(self) -> set[float]:
        """Elapsed readings a document may legitimately state, per the same granularity."""
        return {self.seconds, *self.module_seconds}


# Each rule is (name, compiled pattern, checker). The checker returns None when the figure
# agrees and a human-readable reason when it does not.
_TEST_MODULES = re.compile(r"(\d[\d,]*)\*{0,2}\s+test modules?(?![-\w])")
_TESTS = re.compile(r"(\d[\d,]*)\*{0,2}\s+tests?(?![-\w])(?!\s+modules?\b)")
_QUALIFIED_TESTS = re.compile(r"\*\*(\d[\d,]*)\*\*\s+adapter and reconciliation tests?(?![-\w])")
_SUBTESTS = re.compile(r"(\d[\d,]*)\*{0,2}\s+sub[Tt]ests?(?![-\w])")
_TRAILER = re.compile(r"Ran\s+(\d[\d,]*)\s+tests?\s+in\s+([\d.]+)s")
_REPORTED = re.compile(r"([\d.]+)\s*s\s+as\s+.unittest.\s+reported it")
_WALL = re.compile(r"(\d[\d,]*)\s+ms\s+wall")
_ADDENDS = re.compile(r"((?:\d+\s*\+\s*){2,}\d+)\s*=\s*(\d[\d,]*)")
_FIXTURES = re.compile(r"(\d[\d,]*)\*{0,2}\s+fixtures(?![-\w])")
_EXPECTED = re.compile(r"(\d[\d,]*)\*{0,2}\s+expected files(?![-\w])")
_NEGATIVE = re.compile(r"(\d[\d,]*)\*{0,2}\s+negative fixtures(?![-\w])")


def _rules(a: Authority):
    """Build the rule table against the authoritative values."""

    def one_of(value, allowed, label):
        return None if value in allowed else (
            f"{label} {value} is not among the authoritative {sorted(allowed)}")

    return [
        ("test-modules", _TEST_MODULES,
         lambda m: one_of(_int(m.group(1)), {a.modules_present}, "module count")),
        ("tests", _TESTS,
         lambda m: one_of(_int(m.group(1)), a.any_test_count, "test count")),
        ("tests-qualified", _QUALIFIED_TESTS,
         lambda m: one_of(_int(m.group(1)), a.any_test_count, "test count")),
        ("subtests", _SUBTESTS,
         lambda m: one_of(_int(m.group(1)), {a.subtests}, "subtest count")),
        ("trailer", _TRAILER, lambda m: _check_trailer(a, m)),
        ("reported-seconds", _REPORTED,
         lambda m: one_of(float(m.group(1)), a.any_seconds, "reported elapsed")),
        ("wall-ms", _WALL,
         lambda m: one_of(_int(m.group(1)), {a.wall_ms}, "wall clock")),
        ("addends", _ADDENDS, lambda m: _check_addends(a, m)),
        ("fixtures", _FIXTURES,
         lambda m: one_of(_int(m.group(1)), {a.fixtures}, "fixture count")),
        ("expected-files", _EXPECTED,
         lambda m: one_of(_int(m.group(1)), {a.expected_files}, "expected-file count")),
        ("negative-fixtures", _NEGATIVE,
         lambda m: one_of(_int(m.group(1)), {a.negative_fixtures}, "negative-fixture count")),
    ]


def _check_trailer(a: Authority, match: re.Match[str]) -> str | None:
    """A trailer must be a real (count, seconds) pair the run actually produced."""
    count, seconds = _int(match.group(1)), float(match.group(2))
    pairs = {(a.tests_run, a.seconds), *zip(a.module_tests, a.module_seconds)}
    if (count, seconds) in pairs:
        return None
    return (f"trailer pair ({count}, {seconds}) is not among the authoritative pairs "
            f"{sorted(pairs)}")


def _check_addends(a: Authority, match: re.Match[str]) -> str | None:
    """Check an addend expression in BOTH directions, or skip it as out of scope.

    In scope when either the operands are the module counts or the total is the suite
    total. Anything else -- the JAR inventory's 191 + 436 + 0 = 627, a per-section count --
    is a different measurement and is not this module's to adjudicate.
    """
    operands = [int(x) for x in re.findall(r"\d+", match.group(1))]
    total = _int(match.group(2))
    is_module_operands = sorted(operands) == sorted(a.module_tests)
    is_suite_total = total == a.tests_run
    if not is_module_operands and not is_suite_total:
        return None
    problems = []
    if sum(operands) != total:
        problems.append(f"the operands sum to {sum(operands)}, not the stated {total}")
    if is_suite_total and not is_module_operands:
        problems.append(f"the operands {operands} are not the authoritative module counts "
                        f"{a.module_tests}")
    if is_module_operands and total != a.tests_run:
        problems.append(f"the stated total {total} is not the authoritative suite total "
                        f"{a.tests_run}")
    return "; ".join(problems) or None


# ---------------------------------------------------------------------------------------
# Family two: the record must agree with itself.
# ---------------------------------------------------------------------------------------

#: Prose shapes that state a count of executed tests or of recorded subTests. Each is a
#: phrasing observed in the record itself; a bare integer is deliberately NOT one of them,
#: because the record legitimately carries byte sizes, line numbers and fixture counts.
_PROSE_CLAIMS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"\b(\d[\d,]*) (?:tests|executed tests)\b"),
    re.compile(r"\b(\d[\d,]*) sub[Tt]ests\b"),
    re.compile(r"\ball (\d[\d,]*) (?:of them|carry)\b"),
    re.compile(r"\b(\d[\d,]*) entries for (\d[\d,]*) executed tests\b"),
    re.compile(r"\b(\d[\d,]*)-entry array\b"),
    re.compile(r"entries_count of (\d[\d,]*)"),
    re.compile(r"tests_run of (\d[\d,]*)"),
    re.compile(r"(?:suite )?total (?:of|is) (\d[\d,]*)"),
    re.compile(r"test count of (\d[\d,]*)"),
    re.compile(r"sub[Tt]est total (?:of|is) (\d[\d,]*)"),
    re.compile(r"\bruns (\d[\d,]*) tests\b"),
    re.compile(r"\bit ran (\d[\d,]*)\b"),
    re.compile(r"\bran (\d[\d,]*) tests\b"),
    re.compile(r"\b(\d[\d,]*) where it (?:ran|was) (\d[\d,]*)\b"),
)

#: "N entries" is a count of per-test outcomes only where the sentence says so; the same
#: two words also count negative fixtures and rejection classes elsewhere in the record.
_PROSE_ENTRIES: Final[re.Pattern[str]] = re.compile(r"\b(\d[\d,]*) entries\b")


def _strings(node, path: str = ""):
    """Yield (json path, string) for every string value in the record."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield from _strings(value, f"{path}.{key}")
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from _strings(value, f"{path}[{index}]")
    elif isinstance(node, str):
        yield path, node


def _prose_claims(record: dict):
    """Yield (json path, claimed value, the quoted text) for every test-count claim."""
    for path, text in _strings(record):
        hits: list[tuple[int, str]] = []
        for pattern in _PROSE_CLAIMS:
            for match in pattern.finditer(text):
                hits += [(_int(group), match.group(0)) for group in match.groups()]
        if "per_test_outcomes" in text:
            for match in _PROSE_ENTRIES.finditer(text):
                hits.append((_int(match.group(1)), match.group(0)))
        for value, quoted in dict.fromkeys(hits):
            yield path, value, quoted


def _line_of(raw_lines: list[str], needle: str) -> int:
    """The 1-based line carrying this text, or 0 when it spans none of them alone."""
    for number, text in enumerate(raw_lines, 1):
        if needle in text:
            return number
    return 0


def check_record_self_consistency(root: Path) -> tuple[list[str], list[str]]:
    """Require the run record to agree with itself. Returns (audit lines, violations)."""
    path = root / AUTHORITY
    raw = path.read_text()
    raw_lines = raw.splitlines()
    record = json.loads(raw)

    audit: list[str] = []
    problems: list[str] = []

    def assert_that(label: str, ok: bool, detail: str) -> None:
        audit.append(f"  {'ok' if ok else 'DRIFT':5s} {AUTHORITY} [{label}] {detail}")
        if not ok:
            problems.append(f"{AUTHORITY} [{label}]: {detail}")

    suite = record["suite_result"]
    entries = record["test_modules"]["entries"]
    outcomes = record["per_test_outcomes"]
    module_sum = sum(e["tests_run"] for e in entries)

    # 1. The four ways the record states its own test total must be one number.
    ways = {
        "suite_result.tests_run": suite["tests_run"],
        "per_test_outcomes.entries_count": outcomes["entries_count"],
        "len(per_test_outcomes.entries)": len(outcomes["entries"]),
        "per_test_outcomes.per_module_sum": outcomes["per_module_sum"],
        "per_test_outcomes.tests_run_reported_by_the_runner":
            outcomes["tests_run_reported_by_the_runner"],
        "sum(test_modules.entries[].tests_run)": module_sum,
        "suite_result.sum_of_per_module_tests_run": suite["sum_of_per_module_tests_run"],
    }
    total = suite["tests_run"]
    disagreeing = {name: value for name, value in ways.items() if value != total}
    assert_that("total-agreement", not disagreeing,
                f"the record states its test total {len(ways)} ways; "
                + (f"all equal {total}" if not disagreeing
                   else f"these disagree with suite_result.tests_run {total}: {disagreeing}"))
    assert_that("total-agreement-flags",
                outcomes["three_figures_agree"] is True
                and suite["sum_equals_suite_total"] is True,
                "three_figures_agree and sum_equals_suite_total are both true, as the "
                "figures they describe require"
                if outcomes["three_figures_agree"] and suite["sum_equals_suite_total"]
                else "three_figures_agree is "
                     f"{outcomes['three_figures_agree']} and sum_equals_suite_total is "
                     f"{suite['sum_equals_suite_total']} while the figures they describe "
                     "agree, so the flags do not describe the data")

    # 2. The per-module breakdown must sum to that total and match the entries themselves.
    per_module = outcomes["entries_per_module"]
    assert_that("entries-per-module-sum", sum(per_module.values()) == total,
                f"entries_per_module sums to {sum(per_module.values())} against a total of "
                f"{total}")
    counted: dict[str, int] = {}
    for entry in outcomes["entries"]:
        module = entry["id"].split(".", 1)[0]
        counted[module] = counted.get(module, 0) + 1
    assert_that("entries-per-module-matches-entries", counted == per_module,
                "entries_per_module equals the per-module tally of the entries themselves"
                if counted == per_module else
                f"entries_per_module {per_module} disagrees with the entries' own tally "
                f"{counted}")
    by_module = {Path(e["module"]).stem: e["tests_run"] for e in entries}
    assert_that("entries-per-module-matches-modules", counted == by_module,
                "the entries' per-module tally equals each module entry's own tests_run"
                if counted == by_module else
                f"the entries' tally {counted} disagrees with the module entries "
                f"{by_module}")

    # 3. The status histogram must account for every executed test, once.
    histogram = outcomes["status_histogram"]
    assert_that("status-histogram-sum", sum(histogram.values()) == total,
                f"status_histogram {histogram} sums to {sum(histogram.values())} against a "
                f"total of {total}")

    # 4. Each module entry's outcomes must sum to that entry's own count.
    for entry in entries:
        name = Path(entry["module"]).name
        counts = entry["outcomes"]
        assert_that(f"module-outcomes:{name}",
                    sum(counts.values()) == entry["tests_run"],
                    f"{name} outcomes {counts} sum to {sum(counts.values())} against its "
                    f"tests_run of {entry['tests_run']}")

    # 5. Every mandated_tests module total must equal the module entry it names.
    for key, block in record["mandated_tests"].items():
        if not isinstance(block, dict) or "module_total_tests_run" not in block:
            continue
        module_name = Path(block["module"]).name
        owner = next((e for e in entries if Path(e["module"]).name == module_name), None)
        if owner is None:
            assert_that(f"mandated-total:{key}", False,
                        f"names module {module_name}, which has no test_modules entry")
            continue
        stated, measured = block["module_total_tests_run"], owner["tests_run"]
        assert_that(f"mandated-total:{key}", stated == measured,
                    f"module_total_tests_run {stated} equals {module_name}'s measured "
                    f"tests_run {measured}" if stated == measured else
                    f"module_total_tests_run is {stated} while {module_name} ran "
                    f"{measured}")
        subset = block.get("tests_run")
        if isinstance(subset, int):
            assert_that(f"mandated-subset:{key}", subset <= measured,
                        f"its requirement-specific subset {subset} is within the module's "
                        f"{measured}" if subset <= measured else
                        f"its requirement-specific subset {subset} exceeds the module's "
                        f"{measured}, so it cannot be a subset of it")

    # 6. The rule that would have caught the 1142: adjudicate the record's own prose.
    legitimate = {total, suite["subtests_recorded"], outcomes["entries_count"]}
    for entry in entries:
        legitimate.add(entry["tests_run"])
        legitimate.add(entry["subtests_recorded"])
    for block in record["mandated_tests"].values():
        if isinstance(block, dict):
            for field in ("tests_run", "module_total_tests_run"):
                if isinstance(block.get(field), int):
                    legitimate.add(block[field])
    declaration = record.get("historical_figures_quoted_in_prose")
    declared: dict[int, str] = {}
    if isinstance(declaration, dict):
        for item in declaration.get("figures", []):
            declared[item["value"]] = item.get("was", "")
    claims = list(_prose_claims(record))
    assert_that("prose-claims-found", bool(claims),
                f"{len(claims)} test-count claims found in the record's prose"
                if claims else
                "no test-count claim matched any prose shape, so this family adjudicates "
                "nothing and cannot report a disagreement")
    for json_path, value, quoted in claims:
        line = _line_of(raw_lines, quoted)
        where = f"{AUTHORITY}:{line}" if line else AUTHORITY
        if value in legitimate:
            audit.append(f"  {'ok':5s} {where} [prose-claim] {quoted!r} is the measured "
                         f"{value}")
        elif value in declared:
            audit.append(f"  {'ok':5s} {where} [prose-claim] {quoted!r} is declared "
                         f"historical: {declared[value]}")
        else:
            audit.append(f"  {'DRIFT':5s} {where} [prose-claim] {quoted!r}")
            problems.append(
                f"{where} [prose-claim] {quoted!r} at {json_path}: {value} is neither one "
                f"of this run's measured counts {sorted(legitimate)} nor declared under "
                f"historical_figures_quoted_in_prose. A count in prose that no measurement "
                f"produced is the defect this rule exists for -- either the prose is stale "
                f"or the quotation is deliberate and undeclared.")

    # 7. A declaration that no longer appears anywhere is itself stale.
    prose = "\n".join(text for _, text in _strings(record))
    for value, was in declared.items():
        present = str(value) in prose or f"{value:,}" in prose
        assert_that(f"declared-figure:{value}", present,
                    f"the declared historical figure {value} ({was[:40]}) still appears in "
                    f"the prose that quotes it" if present else
                    f"the declared historical figure {value} appears nowhere in the "
                    f"record's prose, so the declaration exempts a claim that is no longer "
                    f"made and would exempt a future one silently")

    return audit, problems


# ---------------------------------------------------------------------------------------
# Family three: the documented command must be the recorded command.
# ---------------------------------------------------------------------------------------

#: The file that tells a reader what to run. Its commands are checked, not its prose.
DOCUMENTED: Final[str] = "oss-scan-results/adapter-tests/README.md"

#: A command line as the README publishes it, at the start of a line inside a code fence.
_DOCUMENTED_COMMAND: Final[re.Pattern[str]] = re.compile(
    r"^(python3 -m unittest [^\n]*)$", re.MULTILINE)

#: The discover pattern inside a documented or recorded command.
_PATTERN_ARG: Final[re.Pattern[str]] = re.compile(r"^test_[\w*]+\.py$")


def _argv(command: str) -> list[str]:
    """Split a command line into argv, honouring the quoting the documents use."""
    import shlex

    return shlex.split(command)


def check_documented_commands(root: Path) -> tuple[list[str], list[str]]:
    """Require README's commands to be the record's commands. Returns (audit, violations)."""
    record_path = root / AUTHORITY
    readme_path = root / DOCUMENTED
    audit: list[str] = []
    problems: list[str] = []

    if not readme_path.is_file():
        return audit, [f"{DOCUMENTED} does not exist, so the documented invocation cannot "
                       f"be compared with the recorded one"]

    record = json.loads(record_path.read_text())
    readme_raw = readme_path.read_text()
    readme_lines = readme_raw.splitlines()
    interpreter = record["interpreter"]["path"]

    documented = _DOCUMENTED_COMMAND.findall(readme_raw)
    suite_forms = [c for c in documented if "test_*.py" in c]
    module_forms = [c for c in documented if "test_*.py" not in c]

    def assert_that(label: str, where: str, ok: bool, detail: str) -> None:
        audit.append(f"  {'ok' if ok else 'DRIFT':5s} {where} [{label}] {detail}")
        if not ok:
            problems.append(f"{where} [{label}]: {detail}")

    assert_that("documented-commands-found", DOCUMENTED,
                len(suite_forms) == 1 and len(module_forms) == 1,
                f"{len(suite_forms)} whole-suite and {len(module_forms)} per-module command "
                f"lines published"
                + ("" if len(suite_forms) == 1 and len(module_forms) == 1
                   else "; exactly one of each is expected, and a missing one means this "
                        "family compares nothing"))
    if not suite_forms or not module_forms:
        return audit, problems

    # The whole-suite form, resolved and compared argument by argument.
    documented_argv = _argv(suite_forms[0])
    recorded_argv = _argv(record["command"])
    resolved = [interpreter] + documented_argv[1:]
    line = _line_of(readme_lines, suite_forms[0])
    same_tail = documented_argv[1:] == recorded_argv[1:]
    assert_that("suite-command", f"{DOCUMENTED}:{line}", resolved == recorded_argv,
                f"the documented invocation is the recorded one in every argument, the one "
                f"difference being the executable ({documented_argv[0]} against "
                f"{recorded_argv[0]})" if resolved == recorded_argv else
                f"the documented argv {documented_argv[1:]} is not the recorded argv "
                f"{recorded_argv[1:]}"
                + ("" if same_tail else
                   f"; the record's command is {record['command']!r} and the README's is "
                   f"{suite_forms[0]!r}, so a reader reproducing the documented run is not "
                   f"reproducing the measured one"))
    assert_that("suite-executable", AUTHORITY, recorded_argv[0] == interpreter,
                f"the recorded command names the interpreter this record measured "
                f"({interpreter})" if recorded_argv[0] == interpreter else
                f"the recorded command runs {recorded_argv[0]} while interpreter.path is "
                f"{interpreter}")

    # Every per-module form must be the documented template with its pattern swapped.
    template = _argv(module_forms[0])
    template_line = _line_of(readme_lines, module_forms[0])
    pattern_positions = [i for i, token in enumerate(template)
                         if _PATTERN_ARG.match(token)]
    assert_that("module-template", f"{DOCUMENTED}:{template_line}",
                len(pattern_positions) == 1,
                f"the documented per-module form carries exactly one discover pattern"
                if len(pattern_positions) == 1 else
                f"the documented per-module form carries {len(pattern_positions)} discover "
                f"patterns, so no unambiguous template can be taken from it")
    if len(pattern_positions) != 1:
        return audit, problems
    slot = pattern_positions[0]

    for entry in record["test_modules"]["entries"]:
        name = Path(entry["module"]).name
        expected = [interpreter] + template[1:]
        expected[slot] = name
        actual = _argv(entry["command"])
        assert_that(f"module-command:{name}", AUTHORITY, expected == actual,
                    f"its recorded command is the documented per-module form with the "
                    f"pattern {name!r}" if expected == actual else
                    f"its recorded command is {entry['command']!r}, which is not the "
                    f"documented per-module form with the pattern swapped for {name!r}; "
                    f"expected argv {expected}, recorded {actual}")

    return audit, problems


def main(argv: list[str]) -> int:
    """Check every citation, print the audit, and return the process exit status."""
    quiet = "--quiet" in argv
    root = repo_root()
    authority = Authority(root)
    rules = _rules(authority)

    checked = 0
    violations: list[str] = []
    lines: list[str] = []

    for rel in SCANNED:
        path = root / rel
        if not path.is_file():
            violations.append(f"{rel} does not exist, so its citations cannot be checked")
            continue
        for number, text in enumerate(path.read_text().splitlines(), 1):
            for name, pattern, check in rules:
                for match in pattern.finditer(text):
                    checked += 1
                    reason = check(match)
                    verdict = "ok" if reason is None else "DRIFT"
                    lines.append(f"  {verdict:5s} {rel}:{number} [{name}] "
                                 f"{match.group(0).strip()!r}")
                    if reason is not None:
                        violations.append(f"{rel}:{number} [{name}] "
                                          f"{match.group(0).strip()!r}: {reason}")

    # The record must agree with itself, and the documented invocation must be the
    # recorded one. Both families are collected before anything is printed so the audit
    # reads in one pass and a failure in one does not hide the other's coverage.
    self_audit, self_problems = check_record_self_consistency(root)
    command_audit, command_problems = check_documented_commands(root)
    violations.extend(self_problems)
    violations.extend(command_problems)
    assertions = len(self_audit) + len(command_audit)

    print("=" * 84)
    print("REPLICATED ADAPTER-TEST FIGURES vs THEIR ONE AUTHORITATIVE MEASUREMENT")
    print("=" * 84)
    print(f"  authority : {AUTHORITY}")
    print(f"  suite     : {authority.tests_run} tests, {authority.subtests} subtests, "
          f"{authority.seconds}s reported, {authority.wall_ms} ms wall")
    print(f"  modules   : {authority.module_tests} "
          f"(sum {sum(authority.module_tests)}, {authority.modules_present} present)")
    print(f"  fixtures  : {authority.fixtures} fixtures, {authority.expected_files} expected, "
          f"{authority.negative_fixtures} negative")
    print()
    if not quiet:
        print("\n".join(lines))
        print()
        print("-" * 84)
        print("THE RECORD AGAINST ITSELF -- its own prose, arrays and totals")
        print("-" * 84)
        print("\n".join(self_audit))
        print()
        print("-" * 84)
        print(f"THE DOCUMENTED INVOCATION AGAINST THE RECORDED ONE -- {DOCUMENTED}")
        print("-" * 84)
        print("\n".join(command_audit))
        print()
    print(f"  figures checked     : {checked}")
    print(f"  assertions checked  : {assertions} "
          f"({len(self_audit)} self-consistency, {len(command_audit)} command equality)")
    print(f"  drifted             : {len(violations)}")
    print()

    if checked == 0 or assertions == 0:
        print("  FAIL: no figure matched any rule, or no assertion was made. A checker")
        print("        that finds nothing reports no violations either, so an empty result")
        print("        is a failure, not a pass.")
        return 1
    if violations:
        print("=" * 84)
        print("DRIFT -- each figure below disagrees with the measurement it cites")
        print("=" * 84)
        for item in violations:
            print(f"  - {item}")
        print()
        return 1
    print("  PASS: every replicated figure equals its authoritative measurement.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
