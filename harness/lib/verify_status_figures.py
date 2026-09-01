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
nothing also reports no violations. Exit 0 when every figure agrees, 1 otherwise, with each
violation named.

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
    print(f"  figures checked : {checked}")
    print(f"  drifted         : {len(violations)}")
    print()

    if checked == 0:
        print("  FAIL: no figure matched any rule. A checker that finds nothing reports")
        print("        no violations either, so an empty result is a failure, not a pass.")
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
