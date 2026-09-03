#!/usr/bin/env python3
"""Pre-scan target gate: refuse to scan a tree that is not the pinned tree, refuse a
smoke override on the production path, refuse a harness path value a runner would
interpolate into a shell or a Python source string, and refuse to scan at all when either
artifact tree is missing.

WHY THIS EXISTS AS A SEPARATE, EXECUTABLE STEP
==============================================
AAP 0.8.1 requires that "a runner shown to resolve a tree other than ``SPARK_SRC`` halts
the run", and 0.8.1 again that the smoke override "is never a fallback" -- it "exists for
setup-time verification, and leaving it set silently scans the wrong thing". Neither
requirement can be satisfied from inside the thing that resolves the target:

* ``harness/lib/scope.sh`` lines 22-35 (``scope_resolve_target``) accept ANY ``SPARK_SRC``
  that is a directory. There is no comparison against the pinned commit, so a caller who
  points the variable at another checkout gets eighteen expanded scope roots and a full
  scan of the wrong code, with ``scan_root_source=SPARK_SRC`` recorded as though nothing
  were unusual.
* the same file takes ``HARNESS_SMOKE_TARGET`` whenever it is non-empty, and lines 43-48
  of ``scope_dirs`` then print ``.`` -- the smoke directory becomes the entire scope. The
  only statement that this must not happen on a production path is a COMMENT, at
  ``harness/env.sh`` lines 87-89. A comment binds nothing.
* ``harness/bin/run-trivy.sh`` lines 52-53 interpolate ``$TRIVY_CACHE_DIR`` into the text
  of a ``python3 -c`` program. An existing directory whose literal name closes the
  embedded string executes arbitrary Python with the process's ambient environment.
* ``harness/env.sh`` line 91 runs ``mkdir -p "$HARNESS_RAW_DIR" "$HARNESS_LOG_DIR"
  "$HARNESS_SCRATCH_DIR" 2>/dev/null || true``, and every runner sources ``env.sh`` before
  it reaches ``scope_begin``. So ``scope.sh`` lines 68-69's ``-d`` guards -- which are
  correct code implementing exactly AAP 0.8.1's missing-tree halt -- are handed trees that
  were created on the way to them, and the mandated halt cannot fire. Measured dead: a
  runner invoked with ``HARNESS_RAW_DIR`` pointing at a non-existent path completed and
  left the tree created and populated. The ``|| true`` also swallows a permission failure.

None of those four files may be edited. AAP 0.3.2 forbids runner reconfiguration, 0.8.1
states that "the line is the runner's own file and its baked flags: those are never
edited", 0.6.3 states that "no runner or harness helper is edited", 0.6.1 marks every
``harness/bin/`` entry and ``harness/ENVIRONMENT.md`` REFERENCE, and the environment
runbook repeats it for ``harness/env.sh`` and ``harness/lib/scope.sh`` by name. So the
check cannot live inside the thing it guards. It lives here instead, in the same shape as
``harness/lib/preflight_graph_identity.py``: a fail-closed gate whose exit status a
committed caller reads BEFORE the runner is reached.

WHAT MAKES IT BINDING, AND WHAT IT DOES NOT REACH
=================================================
``harness/lib/run-scanner-gated.sh`` is the committed gated entry point for all nine
runners and runs this module first; ``harness/lib/run-joern-gated.sh`` runs it as step 1
of 4, ahead of the graph-identity gate, the heap commit proof and the runner. Neither
wrapper has a branch that reaches a runner after a non-zero status here, so for an
invocation routed through either one the control is structural rather than advisory.

It is not the only route. Each ``harness/bin/run-<tool>.sh`` remains executable in its
own right -- AAP 0.8.1 requires each runner invocable directly with no arguments -- and an
invocation that does not read this exit status is not bound by it. That residual is a
property of a provisioning that may not be edited from a clone, and it is reported rather
than papered over.

THE FOUR CONTROLS, AND WHY EACH IS STRICTER THAN THE RESOLVER IT GUARDS
=======================================================================
1. THE SMOKE OVERRIDE IS REFUSED ON PRESENCE, NOT ON VALUE.
   ``scope.sh`` tests ``[ -n "${HARNESS_SMOKE_TARGET:-}" ]``, so an exported EMPTY value
   reads there as though the variable were unset. A gate whose refusal depended on the
   value being non-empty would therefore agree with the resolver about the one case where
   the resolver's own reading is surprising, and it would not be the control the
   checkpoint asks for ("reject any set override on the production path"). So this gate
   refuses whenever the name is present in the environment AT ALL, whatever it holds.

   Presence in ``os.environ`` is exactly the reachable surface, and that is not an
   approximation: a shell variable that was assigned without ``export`` reaches neither
   this gate nor a runner, both of which are child processes. What a runner can see is
   what this gate can see.

2. THE TREE'S IDENTITY IS MEASURED AGAINST A PIN AUTHORED HERE, AND THE ENVIRONMENT'S
   CLAIM IS CHECKED SEPARATELY AGAINST THE SAME PIN.
   ``SPARK_SRC_COMMIT`` is an ordinary overridable environment value
   (``harness/env.sh`` line 44). A gate that compared the measured HEAD against that
   variable alone would pass for a caller who overrode BOTH -- the wrong tree and a
   matching claim about it -- which is the whole hole. So ``PINNED_SPARK_COMMIT`` below is
   a constant of this module, the measured HEAD is compared against it, and
   ``SPARK_SRC_COMMIT`` is then required to agree with it as its own separate check. Two
   independent comparisons against one immovable value; neither can be satisfied by
   moving the other.

   The resolved repository root is compared too. A ``SPARK_SRC`` naming a SUBDIRECTORY of
   the pinned tree yields the pinned HEAD while resolving a different root, so HEAD
   equality alone would accept a scan whose scope expansion was anchored somewhere the
   allowlist does not describe.

   ``git`` itself is invoked with argument vectors (never a shell), from an ABSOLUTE
   executable path, with every ``GIT_*`` variable stripped from the child environment.
   That last point is not hygiene, it is a measured bypass: with ``GIT_DIR`` exported,
   ``git -C "$SPARK_SRC" rev-parse HEAD`` returns the HEAD of the repository ``GIT_DIR``
   names and not of the tree ``-C`` selects, so an unhardened call is caller-controllable.
   System and global configuration are excluded for the same reason.

3. EVERY HARNESS PATH VALUE A RUNNER CONSUMES MUST BE ORDINARY TEXT.
   This is the compensating control for the Trivy expression. The runner cannot be
   edited, so the value it will interpolate is validated BEFORE any runner is invoked,
   against a strict allowlist-shaped policy: absolute, and free of control characters and
   of every character that carries meaning to ``sh`` or closes a string in Python source.
   ``TRIVY_CACHE_DIR`` is the value that reaches an interpolation today; the rest are
   checked because the same class of defect in any future runner would consume them, and
   a policy applied to one variable is a policy nobody can rely on.

   The residual is stated plainly rather than implied: a caller invoking
   ``harness/bin/run-trivy.sh`` DIRECTLY with a hostile ``TRIVY_CACHE_DIR`` still executes
   it. The one-line provisioning change that would close it at the root is to pass the
   metadata path as positional ``argv`` to fixed Python code -- ``python3 -c '...'
   "$TRIVY_CACHE_DIR/db/metadata.json"`` reading ``sys.argv[1]`` -- instead of
   interpolating environment text into program source.

4. BOTH ARTIFACT TREES MUST ALREADY EXIST, AND THIS GATE NEITHER CREATES NOR CLEARS THEM.
   AAP 0.8.1: both trees "must already exist and be empty", and "this run neither creates
   nor clears them: a missing tree is a provisioning failure and a non-empty tree makes an
   earlier run's artifact indistinguishable from this one's". AAP 0.9.2 lists "either
   artifact tree missing or non-empty" among the conditions that stop the run.

   The missing half of that halt is dead where it is written, for the reason the fourth
   bullet above gives, and it cannot be repaired there. So existence is refused here:
   ``$HARNESS_RAW_DIR`` and ``$HARNESS_LOG_DIR`` are each resolved, reported, and required
   to be a directory, and a tree that is absent -- or present as something other than a
   directory -- is a REFUSAL before any runner is reached. Nothing is created and nothing
   is cleared, which is the same prohibition the defect violates: a gate that repaired the
   condition it was asked to detect would destroy the evidence of it.

   The NON-EMPTY half is deliberately NOT refused here, and the distinction matters.
   Emptiness is a statement about a single moment -- "before this run wrote anything" --
   measured once at the Stage 0 gate and owned by
   ``harness/artifacts/logs/gate-record.json``, which carries it as a live halt. This gate
   runs after that moment, on trees this run's own evidence writes into by design, so
   re-deciding emptiness here would produce a second, later, wrong answer to a question
   already answered. What is reported instead is each tree's census, with the owning
   condition named beside it.

ORDER MATTERS, AND IT IS THE ORDER A CALLER MUST OBSERVE TOO
============================================================
The character policy runs BEFORE anything consumes a value: the git checks are attempted
only once ``SPARK_SRC`` has passed the policy, and are recorded as "not attempted" when it
has not. A gate that measured first and validated afterwards would already have handed the
hostile value to a subprocess, and would then have to print it in order to explain itself.

The artifact-tree checks sit between the two: after the policy, because those two values
are path values and nothing should resolve or ``stat`` them before they are known to be
ordinary text; before the target checks, because a run whose output trees are missing must
not reach a runner at all, and a reader should see that before three git measurements.

REFUSING WITHOUT REPEATING THE HOSTILE VALUE
============================================
A refusal names the variable, the offending character class, its zero-based index, and the
value's length and sha256. It does not print the value, because this record is written
0644 into ``harness/artifacts/logs/`` and preserved verbatim, and a diagnostic that echoes
an attacker-supplied string copies it into the evidence base -- exactly the disclosure
channel the checkpoint requires closed elsewhere in the pipeline. A value that PASSES the
policy is recorded in full: it is by construction ordinary path text, AAP 0.1.3 states
that "file paths and version facts intentionally required by the AAP are not secrets", and
a record that named no path would not let a reader reproduce the check.

EVIDENCE, AND AUDITING WITHOUT DESTROYING IT
============================================
A normal run prints its report and writes two files:
``harness/artifacts/logs/sec-gate-scan-target.log`` (the console report verbatim) and
``harness/artifacts/logs/sec-gate-scan-target.json`` (the structured record: every check,
its expectation, what was observed, its verdict, and the git argument vectors with their
exit statuses). ``--check-only`` performs every measurement and prints the whole report
but writes NOTHING, so an audit run cannot overwrite the record that shows the gate
preceded an invocation -- the same discipline, and for the same reason, as
``preflight_graph_identity.py --check-only``.

EXIT STATUS
===========
``0``  every check passed; the caller may invoke the runner.
``77`` REFUSAL. A definite prohibition was measured -- a smoke override present, a tree
       that is not the pinned tree, or a path value carrying shell or source syntax. The
       caller MUST NOT invoke the runner. 77 is ``preflight_graph_identity.py``'s HALT
       convention, kept identical so one status means one thing across the harness.
``78`` CONFIGURATION FAULT. The gate could not complete a measurement, or a prerequisite
       of the harness itself is missing -- ``SPARK_SRC`` unset or not a directory, an
       expected environment value absent because ``harness/env.sh`` was never sourced, no
       usable ``git``. 78 is ``scope.sh``'s ``scope_fail`` convention for the same class of
       condition, so a fault here reads the same way as a fault there.

Both non-zero statuses forbid the scan. They are distinguished because "correct your
environment" and "you are pointed at the wrong tree" send a reader to different places.
When both occur the status is 77: a measured prohibition is decisive, and every fault is
still listed in the report and the record.

Usage:
    harness/lib/preflight_scan_target.py                # gate an invocation, write evidence
    harness/lib/preflight_scan_target.py --check-only   # measure and print, write nothing
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Final, NamedTuple

#: Exit status meaning "a prohibition was measured; do not invoke the runner". Identical
#: to preflight_graph_identity.HALT_EXIT so one number means one thing in every log.
HALT_EXIT: Final[int] = 77

#: Exit status meaning "the gate could not measure, or the harness is misconfigured".
#: Identical to scope.sh's scope_fail status (EX_CONFIG).
CONFIG_EXIT: Final[int] = 78

#: The pinned commit, AUTHORED IN THIS MODULE. The AAP names it in 0.1.1 and
#: harness/env.sh line 44 carries it as an overridable default; this copy exists so the
#: comparison cannot be satisfied by overriding the environment. See control 2 above.
PINNED_SPARK_COMMIT: Final[str] = "59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d"

#: The setup-time override that redirects every runner at one small directory.
#: harness/env.sh lines 87-89 require it left unset for a real scan, in a comment.
SMOKE_VAR: Final[str] = "HARNESS_SMOKE_TARGET"

#: Every path-valued variable harness/env.sh exports and a runner consumes, in the order
#: env.sh sets them. TRIVY_CACHE_DIR is the one that reaches a string interpolation today
#: (run-trivy.sh lines 52-53); the others are held to the same policy because a policy
#: applied to a single variable is a policy the next runner escapes.
PATH_VARS: Final[tuple[str, ...]] = (
    "SPARK_SRC",
    "HARNESS_DIR",
    "HARNESS_REPO_ROOT",
    "HARNESS_RAW_DIR",
    "HARNESS_LOG_DIR",
    "HARNESS_SCOPE_FILE",
    "HARNESS_CPG",
    "HARNESS_LIB_DIR",
    "HARNESS_SHARED_DIR",
    "HARNESS_TOOLS_DIR",
    "HARNESS_SCRATCH_DIR",
    "JAVA_HOME",
    "JAVA_HOME_21",
    "JOERN_HOME",
    "DEPENDENCY_CHECK_HOME",
    "MAVEN_HOME",
    "SCALA_HOME",
    "OPENGREP_RULES_DIR",
    "SEMGREP_RULES_DIR",
    "DD_SAST_RULES_FILE",
    "TRIVY_CACHE_DIR",
    "HARNESS_DC_DATA_DIR",
)

#: Characters refused anywhere in a path value, each with the reason it is refused. The
#: reason is what gets printed: "single quote at index 5" tells a reader nothing about why
#: the harness cares, and this record is read by whoever has to fix the environment.
FORBIDDEN_CHARS: Final[dict[str, str]] = {
    "'": "single quote -- closes a single-quoted sh word and a Python string literal",
    '"': "double quote -- closes a double-quoted sh word and a Python string literal",
    "\\": "backslash -- escape character in sh and in Python source",
    "$": "dollar -- sh parameter expansion and command substitution",
    "`": "backtick -- sh command substitution",
    ";": "semicolon -- sh command separator, and a statement separator in Python",
    "|": "pipe -- sh pipeline",
    "&": "ampersand -- sh background and AND-list",
    "(": "open parenthesis -- sh subshell, and a call expression in Python",
    ")": "close parenthesis -- sh subshell, and a call expression in Python",
    "<": "less-than -- sh input redirection",
    ">": "greater-than -- sh output redirection",
    "*": "asterisk -- sh pathname expansion",
    "?": "question mark -- sh pathname expansion",
    "[": "open bracket -- sh pathname expansion, and a subscript in Python",
    "]": "close bracket -- sh pathname expansion, and a subscript in Python",
    "{": "open brace -- sh brace expansion",
    "}": "close brace -- sh brace expansion",
    "!": "exclamation mark -- sh history expansion",
    "#": "hash -- sh and Python comment introducer",
    "~": "tilde -- sh tilde expansion",
}

#: Control characters carry their own class name because a bare codepoint in a report is
#: unreadable, and because a newline in a value breaks every line-oriented log the harness
#: writes -- scope.sh's own .status files among them.
_CONTROL_NAMES: Final[dict[int, str]] = {
    0x00: "NUL -- terminates a C string and truncates any path built from it",
    0x09: "horizontal tab -- a field separator in the harness's own records",
    0x0A: "newline -- splits one value across two lines of every line-oriented log",
    0x0D: "carriage return -- rewrites a log line in a terminal",
    0x7F: "delete",
}

#: Verdict vocabulary. Fixed, because the report, the JSON record and the exit status are
#: all derived from it and a fourth spelling would make one of the three disagree.
PASS: Final[str] = "PASS"
REFUSE: Final[str] = "REFUSE"
FAULT: Final[str] = "FAULT"


class Check(NamedTuple):
    """One measurement, with everything a reader needs to re-take it by hand."""

    check_id: str
    subject: str
    expectation: str
    observed: str
    verdict: str


class Violation(NamedTuple):
    """One character-policy violation: its class, its index, and nothing of the value."""

    index: int
    codepoint: int
    char_class: str


def repo_root() -> Path:
    """Locate the repository root without hardcoding a checkout path.

    ``HARNESS_REPO_ROOT`` wins when set, so a caller can point the gate at a specific
    checkout; otherwise the root is derived from this file's own location -- this module
    lives at ``<root>/harness/lib/``. Identical to ``preflight_graph_identity.repo_root``
    and to the technique ``harness/env.sh`` uses, so the two gates and the environment
    file cannot disagree about which checkout they are in.
    """
    env = os.environ.get("HARNESS_REPO_ROOT")
    if env:
        return Path(env).resolve()
    return Path(__file__).resolve().parent.parent.parent


def sha256_of_text(value: str) -> str:
    """Return the sha256 of ``value``'s UTF-8 bytes.

    Used to identify a refused value in the record WITHOUT reproducing it: a digest lets
    two runs be compared and lets a reader confirm which value was refused once they have
    it in front of them, while copying nothing attacker-supplied into a preserved log.
    Surrogates are permitted through ``surrogateescape`` because an environment value can
    carry bytes that are not valid UTF-8, and a gate that raised on one would fail to
    describe exactly the input most worth describing.
    """
    return hashlib.sha256(value.encode("utf-8", "surrogateescape")).hexdigest()


def git_executable() -> str | None:
    """Return an absolute, executable ``git``, or ``None``.

    Absolute by construction: the fixed system locations are tried first and a ``PATH``
    lookup only supplies a candidate, which is then required to be absolute and
    executable. The gate's measurement of the pinned tree is only as trustworthy as the
    binary that takes it, and ``PATH`` is caller-controlled.
    """
    candidates = ["/usr/bin/git", "/bin/git", "/usr/local/bin/git"]
    found = shutil.which("git")
    if found:
        candidates.append(found)
    for candidate in candidates:
        if os.path.isabs(candidate) and os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return None


def git_child_env() -> dict[str, str]:
    """Return the environment ``git`` is invoked with: no ``GIT_*``, no outside config.

    Every ``GIT_*`` variable is dropped, because ``GIT_DIR`` (or ``GIT_WORK_TREE``,
    ``GIT_COMMON_DIR``, ``GIT_OBJECT_DIRECTORY``, ...) makes ``git -C <tree> rev-parse
    HEAD`` report the HEAD of the repository the VARIABLE names rather than of the tree
    ``-C`` selects. That was measured on this host: with ``GIT_DIR`` exported the call
    against the pinned tree returned this checkout's HEAD instead. An unhardened call is
    therefore caller-controllable, and a pin check that can be told what to see is not a
    check.

    System and global configuration are excluded for the same reason -- an ``include.path``
    or an alias in a file a caller can write is input to the measurement. ``rev-parse``
    needs neither: the only system configuration on this host is git-lfs's filter
    registration, which no ``rev-parse`` consults.
    """
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env["GIT_CONFIG_GLOBAL"] = "/dev/null"
    env["GIT_TERMINAL_PROMPT"] = "0"  # never block waiting for credentials
    return env


class GitResult(NamedTuple):
    """What one ``git`` invocation did, in a form the JSON record can carry verbatim."""

    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str


def run_git(git: str, target: str, *args: str) -> GitResult:
    """Run one ``git`` command against ``target`` as an argument vector, never a shell.

    ``shell=False`` (the default for a list argv) is what makes a path containing shell
    syntax inert here: it is one ``execve`` argument and no interpreter ever sees it. The
    character policy still refuses such a path before this function is reached, because
    the RUNNER that consumes the same value afterwards does not have that property.

    Output is decoded with ``replace`` and stripped: a value that cannot be decoded must
    still be describable. Failures are returned rather than raised, so the caller can
    record the exit status and the diagnostic as evidence instead of dying on them.
    """
    argv = (git, "-C", target, *args)
    try:
        completed = subprocess.run(  # noqa: S603 - fixed absolute binary, list argv, no shell
            list(argv),
            env=git_child_env(),
            capture_output=True,
            text=True,
            errors="replace",
            timeout=120,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return GitResult(argv, -1, "", f"{type(exc).__name__}: {exc}")
    return GitResult(argv, completed.returncode, completed.stdout.strip(), completed.stderr.strip())


def policy_violations(value: str) -> list[Violation]:
    """Return every character-policy violation in ``value``, first occurrence per class.

    One entry per distinct class rather than per character: a name padded with fifty
    asterisks would otherwise produce fifty identical lines and bury the other classes it
    also violates. The index kept is the FIRST occurrence, which is where a reader looks.

    Emptiness, relativeness and a leading dash are reported by the caller: they are
    properties of the whole value rather than of a character in it, and conflating the two
    would make "index" meaningless for three of the outcomes.
    """
    seen: dict[str, Violation] = {}
    for index, char in enumerate(value):
        code = ord(char)
        if code < 0x20 or code == 0x7F:
            char_class = _CONTROL_NAMES.get(code, f"control character U+{code:04X}")
        elif char in FORBIDDEN_CHARS:
            char_class = FORBIDDEN_CHARS[char]
        else:
            continue
        seen.setdefault(char_class, Violation(index, code, char_class))
    return sorted(seen.values(), key=lambda item: item.index)


def check_smoke_override() -> Check:
    """Refuse when the smoke override is present in the environment at all.

    Presence, not truthiness: ``scope.sh`` line 23 tests ``-n``, so an exported empty
    value reads there as unset, and a gate that copied that reading would leave the
    surprising case uncovered. See control 1 in the module docstring.
    """
    if SMOKE_VAR not in os.environ:
        return Check(
            "smoke-override-absent",
            f"${SMOKE_VAR}",
            "absent from the environment (harness/env.sh lines 87-89: setup-time only)",
            "absent",
            PASS,
        )
    raw = os.environ[SMOKE_VAR]
    shape = "present and empty" if raw == "" else f"present, {len(raw)} characters"
    return Check(
        "smoke-override-absent",
        f"${SMOKE_VAR}",
        "absent from the environment (harness/env.sh lines 87-89: setup-time only)",
        f"{shape}, sha256 {sha256_of_text(raw)}; scope.sh lines 43-48 would make this "
        f"directory the entire scope and print '.' as the only scope directory",
        REFUSE,
    )


def check_path_policy() -> tuple[list[Check], dict[str, object]]:
    """Validate every harness path value against the character policy.

    Returns the checks and a machine-readable per-variable detail block. A value that
    passes is recorded in full (ordinary path text, and AAP 0.1.3 excludes paths from
    secrets); a value that is refused is recorded only by length, digest, and the class
    and index of each violation.
    """
    checks: list[Check] = []
    detail: dict[str, object] = {}
    for name in PATH_VARS:
        if name not in os.environ:
            checks.append(Check(
                f"path-policy:{name}",
                f"${name}",
                "set by harness/env.sh and holding an absolute path free of shell and "
                "source syntax",
                "not set in the environment -- harness/env.sh appears not to have been "
                "sourced, and a runner that reads this value would resolve its own default",
                FAULT,
            ))
            detail[name] = {"present": False, "verdict": FAULT}
            continue
        value = os.environ[name]
        digest = sha256_of_text(value)
        violations = policy_violations(value)
        problems: list[str] = []
        if value == "":
            problems.append("the value is empty")
        elif not value.startswith("/"):
            problems.append("the value is not absolute (does not start with '/')")
        if value.startswith("-"):
            problems.append("the value begins with '-' and would be read as an option "
                            "rather than as an operand")
        for violation in violations:
            problems.append(
                f"{violation.char_class} (U+{violation.codepoint:04X}) at index "
                f"{violation.index}")
        if not problems:
            checks.append(Check(
                f"path-policy:{name}",
                f"${name}",
                "absolute, no control characters, no shell or source syntax",
                f"{value} ({len(value)} characters, sha256 {digest})",
                PASS,
            ))
            detail[name] = {
                "present": True,
                "value": value,
                "length": len(value),
                "sha256": digest,
                "verdict": PASS,
            }
            continue
        checks.append(Check(
            f"path-policy:{name}",
            f"${name}",
            "absolute, no control characters, no shell or source syntax",
            f"REFUSED, {len(value)} characters, sha256 {digest}; " + "; ".join(problems)
            + ("; run-trivy.sh lines 52-53 interpolate this value into python3 -c source"
               if name == "TRIVY_CACHE_DIR" else ""),
            REFUSE,
        ))
        detail[name] = {
            "present": True,
            "length": len(value),
            "sha256": digest,
            "verdict": REFUSE,
            "problems": problems,
            "violations": [
                {"index": v.index, "codepoint": f"U+{v.codepoint:04X}", "class": v.char_class}
                for v in violations
            ],
        }
    return checks, detail


#: The two artifact trees AAP 0.8.1 requires to EXIST before any runner is invoked, with
#: the role each one plays after the gate. Order fixed: raw first, because it is the tree
#: whose containment rule is absolute ("nothing else is ever written into this tree").
ARTIFACT_TREE_VARS: Final[tuple[tuple[str, str], ...]] = (
    ("HARNESS_RAW_DIR", "runner-only: exactly one verbatim artifact per tool that writes "
                        "one, and nothing else, ever"),
    ("HARNESS_LOG_DIR", "this run's own evidence: per-tool streams and status, plus the "
                        "durable records for the gate, the build, the graph and "
                        "normalization"),
)


class TreeCensus(NamedTuple):
    """What one artifact tree holds, counted without listing what is in it.

    Counts rather than names: ``logs/`` holds well over a hundred files, and a report that
    enumerated them would bury the verdict it exists to deliver. The counts are what the
    emptiness condition is decided on, and they are enough for a reader to see at a glance
    whether a tree is fresh.
    """

    entries: int
    files: int
    directories: int
    other: int
    recursive_files: int


def census_of(path: Path) -> TreeCensus:
    """Count what ``path`` holds, top level and recursively. Reads only; creates nothing.

    ``os.scandir`` rather than a glob so a symlinked entry is counted as what it is, and
    ``follow_symlinks=False`` on the type tests so a link to a directory is not walked out
    of the tree it appears in -- an artifact tree's census must describe that tree.
    """
    entries = files = directories = other = 0
    recursive = 0
    # Closed explicitly rather than left to the collector: this gate runs inside runners
    # and wrappers, and a leaked directory handle in a control is the kind of defect that
    # only shows up under the load nobody tested.
    with os.scandir(path) as scan:
        for item in scan:
            entries += 1
            if item.is_dir(follow_symlinks=False):
                directories += 1
            elif item.is_file(follow_symlinks=False):
                files += 1
            else:
                other += 1
    for _, _, filenames in os.walk(path):
        recursive += len(filenames)
    return TreeCensus(entries, files, directories, other, recursive)


def check_artifact_trees() -> tuple[list[Check], dict[str, object]]:
    """Require both artifact trees to EXIST as directories before any runner is invoked.

    WHY THIS CONTROL HAS TO LIVE HERE (QA Issue 2)
    ==============================================
    AAP 0.8.1 states that both trees "must already exist and be empty" and that "this run
    neither creates nor clears them: a missing tree is a provisioning failure", and AAP
    0.9.2 lists "either artifact tree missing" among the conditions that stop the run.
    ``harness/lib/scope.sh`` lines 68-69 implement exactly that guard --
    ``[ -d "${HARNESS_RAW_DIR:-}" ] || scope_fail ...`` -- and it is correct code that can
    never fire for the condition it names, because ``harness/env.sh`` line 91 is::

        mkdir -p "$HARNESS_RAW_DIR" "$HARNESS_LOG_DIR" "$HARNESS_SCRATCH_DIR" 2>/dev/null || true

    and every runner sources ``env.sh`` before it reaches ``scope_begin``. So a missing
    tree is CREATED on the way to the guard that was supposed to refuse it, the scan
    proceeds, and the halt AAP 0.8.1 mandates is dead. It was measured dead: a
    ``run-gitleaks.sh`` invocation with ``HARNESS_RAW_DIR`` pointing at a non-existent path
    completed with the tool's ordinary findings status and left the tree created and
    populated. The ``|| true`` additionally swallows a permission failure, so a tree that
    could not be created is indistinguishable from one that was.

    Neither file may be edited from a clone -- ``harness/env.sh`` and
    ``harness/lib/scope.sh`` are REFERENCE under AAP 0.6.1/0.6.5 -- so the refusal lives
    here, ahead of every runner, exactly as the smoke and target controls do. The exact
    provisioning patch is stated in the VERDICT block's residual and in
    ``oss-scan-results/run-record.md``.

    WHAT THIS CHECK DOES NOT DO
    ===========================
    It creates nothing and clears nothing: that is the same prohibition the defect above
    violates, and a gate that fixed the condition it was asked to detect would destroy the
    evidence of it. An existing but NON-EMPTY tree does not refuse here either. Emptiness
    is AAP 0.8.1's Stage-0 gate condition, measured once "before this run wrote anything"
    and owned by ``harness/artifacts/logs/gate-record.json``, which carries it as a live
    halt; re-deciding it here from a filesystem this run has been writing to since would
    be a second, later, wrong answer to a question already answered. The census is
    reported so a reader can see the state, and the sentence beside it names the condition's
    owner.
    """
    checks: list[Check] = []
    detail: dict[str, object] = {}
    for name, role in ARTIFACT_TREE_VARS:
        check_id = f"artifact-tree:{name}"
        expectation = (f"${name} exists as a directory before any runner is invoked "
                       f"(AAP 0.8.1: a missing tree is a provisioning failure; this run "
                       f"neither creates nor clears it)")
        if name not in os.environ:
            checks.append(Check(
                check_id, f"${name}", expectation,
                "not set in the environment -- harness/env.sh lines 25-26 set it, so the "
                "environment file appears not to have been sourced and there is no tree to "
                "measure",
                FAULT))
            detail[name] = {"present": False, "verdict": FAULT, "role": role}
            continue
        value = os.environ[name]
        path = Path(value)
        record: dict[str, object] = {
            "present": True,
            "value": value,
            "role": role,
            "is_symlink": path.is_symlink(),
        }
        if path.is_symlink():
            try:
                record["symlink_target"] = os.readlink(path)
            except OSError as exc:
                record["symlink_target"] = f"unreadable: {exc}"
        try:
            exists = path.exists()
            is_dir = path.is_dir()
        except OSError as exc:
            checks.append(Check(
                check_id, f"${name}", expectation,
                f"{value} could not be examined: {type(exc).__name__}: {exc}. A tree whose "
                f"state cannot be measured is not a tree this gate will certify",
                FAULT))
            record.update({"verdict": FAULT, "error": f"{type(exc).__name__}: {exc}"})
            detail[name] = record
            continue
        record.update({"exists": exists, "is_directory": is_dir})
        if not exists:
            checks.append(Check(
                check_id, f"${name}", expectation,
                f"{value} DOES NOT EXIST. AAP 0.9.2 stops the run on a missing artifact "
                f"tree, and this run may not create it. The condition is refused HERE "
                f"because it cannot be refused where it is written: harness/env.sh line 91 "
                f"runs `mkdir -p \"$HARNESS_RAW_DIR\" \"$HARNESS_LOG_DIR\" "
                f"\"$HARNESS_SCRATCH_DIR\" 2>/dev/null || true` on every runner's source of "
                f"env.sh, so scope.sh lines 68-69's `-d` guard is passed a tree that was "
                f"created on the way to it and the mandated halt never fires. Provisioning "
                f"must leave this tree created and empty",
                REFUSE))
            record["verdict"] = REFUSE
            detail[name] = record
            continue
        if not is_dir:
            kind = ("a symlink whose target is not a directory" if path.is_symlink()
                    else "not a directory")
            checks.append(Check(
                check_id, f"${name}", expectation,
                f"{value} exists but is {kind}. An artifact tree that is not a directory "
                f"cannot receive an artifact, and `mkdir -p` on it fails, which is the one "
                f"shape of this defect scope.sh line 68-69 can still catch (exit 78). "
                f"Refused here so the outcome does not depend on which of the two got there "
                f"first",
                REFUSE))
            record["verdict"] = REFUSE
            detail[name] = record
            continue
        try:
            census = census_of(path)
        except OSError as exc:
            checks.append(Check(
                check_id, f"${name}", expectation,
                f"{value} is a directory but its contents could not be counted: "
                f"{type(exc).__name__}: {exc}",
                FAULT))
            record.update({"verdict": FAULT, "error": f"{type(exc).__name__}: {exc}"})
            detail[name] = record
            continue
        shape = (f"{value} exists and is a directory; census: {census.entries} top-level "
                 f"entr{'y' if census.entries == 1 else 'ies'} "
                 f"({census.files} file(s), {census.directories} directory(ies), "
                 f"{census.other} other), {census.recursive_files} file(s) recursively; "
                 f"role: {role}")
        if census.entries:
            shape += ("; NON-EMPTY, which this check does not refuse: emptiness is AAP "
                      "0.8.1's Stage-0 condition, measured before this run wrote anything "
                      "and owned by harness/artifacts/logs/gate-record.json, which carries "
                      "it as a live halt. Existence is what is refused here")
        else:
            shape += "; empty"
        checks.append(Check(check_id, f"${name}", expectation, shape, PASS))
        record.update({
            "verdict": PASS,
            "census": census._asdict(),
            "empty": census.entries == 0,
            "emptiness_condition_owner": "harness/artifacts/logs/gate-record.json "
                                         "(AAP 0.8.1 Stage-0 gate); not decided here",
        })
        detail[name] = record
    return checks, detail


def check_scan_target(spark_src_ok: bool) -> tuple[list[Check], list[GitResult]]:
    """Establish that the tree to be scanned is the pinned tree, or refuse.

    ``spark_src_ok`` is the character-policy verdict for ``SPARK_SRC``. When it is false
    nothing here runs: the value is not handed to a subprocess and is not printed, which
    is the ordering the module docstring commits to.

    Four independent measurements, none of which the others can substitute for:
    the directory exists; it is the root of a git work tree; its HEAD is the pin authored
    in this module; and ``$SPARK_SRC_COMMIT`` agrees with that same pin.
    """
    checks: list[Check] = []
    git_runs: list[GitResult] = []
    raw = os.environ.get("SPARK_SRC")

    if raw is None:
        checks.append(Check(
            "scan-target-set", "$SPARK_SRC",
            "set to the pinned Apache Spark clone",
            "not set -- the scan target must come from the environment (scope.sh line 29 "
            "fails the same way, with the same status)",
            FAULT))
        return checks, git_runs
    if not spark_src_ok:
        checks.append(Check(
            "scan-target-identity", "$SPARK_SRC",
            "HEAD equal to the pinned commit, measured with git",
            "NOT ATTEMPTED -- $SPARK_SRC failed the character policy, so it was neither "
            "passed to a subprocess nor printed here",
            REFUSE))
        return checks, git_runs
    if not os.path.isdir(raw):
        checks.append(Check(
            "scan-target-exists", "$SPARK_SRC",
            "an existing directory",
            f"{raw} is not a directory (scope.sh line 30 fails the same way)",
            FAULT))
        return checks, git_runs
    checks.append(Check(
        "scan-target-exists", "$SPARK_SRC", "an existing directory",
        f"{raw} is a directory", PASS))

    git = git_executable()
    if git is None:
        checks.append(Check(
            "scan-target-identity", "$SPARK_SRC",
            "HEAD equal to the pinned commit, measured with git",
            "no absolute, executable git was found, so the tree's identity could not be "
            "measured at all",
            FAULT))
        return checks, git_runs

    inside = run_git(git, raw, "rev-parse", "--is-inside-work-tree")
    git_runs.append(inside)
    if inside.returncode != 0 or inside.stdout != "true":
        checks.append(Check(
            "scan-target-work-tree", f"{raw} (via {git})",
            "the root of a git work tree, so its commit can be established",
            f"`git -C <SPARK_SRC> rev-parse --is-inside-work-tree` exited "
            f"{inside.returncode} and printed {inside.stdout!r}"
            + (f"; stderr: {inside.stderr}" if inside.stderr else "")
            + " -- a tree whose commit cannot be established is refused, because the pin "
              "is the only thing that makes a scan's findings about the intended code",
            REFUSE))
        return checks, git_runs
    checks.append(Check(
        "scan-target-work-tree", f"{raw} (via {git})",
        "the root of a git work tree", "inside a work tree: true", PASS))

    toplevel = run_git(git, raw, "rev-parse", "--show-toplevel")
    git_runs.append(toplevel)
    if toplevel.returncode != 0:
        checks.append(Check(
            "scan-target-root", "$SPARK_SRC",
            "resolves to the ROOT of the pinned work tree, not to a subdirectory of it",
            f"`git -C <SPARK_SRC> rev-parse --show-toplevel` exited {toplevel.returncode}"
            + (f"; stderr: {toplevel.stderr}" if toplevel.stderr else ""),
            REFUSE))
    else:
        want = os.path.realpath(raw)
        got = os.path.realpath(toplevel.stdout)
        if want != got:
            checks.append(Check(
                "scan-target-root", "$SPARK_SRC",
                "resolves to the ROOT of the pinned work tree, not to a subdirectory of it",
                f"the work tree's root is {got} while $SPARK_SRC resolves to {want}; a "
                f"subdirectory carries the pinned HEAD while anchoring the allowlist's "
                f"expansion somewhere the twelve globs do not describe",
                REFUSE))
        else:
            checks.append(Check(
                "scan-target-root", "$SPARK_SRC",
                "resolves to the ROOT of the pinned work tree",
                f"work-tree root and $SPARK_SRC both resolve to {got}", PASS))

    head = run_git(git, raw, "rev-parse", "HEAD")
    git_runs.append(head)
    if head.returncode != 0:
        checks.append(Check(
            "scan-target-head", f"{raw} HEAD",
            f"byte-equal to {PINNED_SPARK_COMMIT} (the pin authored in this module)",
            f"`git -C <SPARK_SRC> rev-parse HEAD` exited {head.returncode}"
            + (f"; stderr: {head.stderr}" if head.stderr else ""),
            REFUSE))
    elif head.stdout != PINNED_SPARK_COMMIT:
        checks.append(Check(
            "scan-target-head", f"{raw} HEAD",
            f"byte-equal to {PINNED_SPARK_COMMIT} (the pin authored in this module)",
            f"HEAD is {head.stdout} -- this is not the pinned tree, and every finding a "
            f"scan of it produced would be about the wrong code (AAP 0.8.1: a runner "
            f"shown to resolve a tree other than the pinned SPARK_SRC halts the run)",
            REFUSE))
    else:
        checks.append(Check(
            "scan-target-head", f"{raw} HEAD",
            f"byte-equal to {PINNED_SPARK_COMMIT} (the pin authored in this module)",
            f"HEAD is {head.stdout}", PASS))

    declared = os.environ.get("SPARK_SRC_COMMIT")
    if declared is None:
        checks.append(Check(
            "declared-commit-agrees", "$SPARK_SRC_COMMIT",
            f"equal to {PINNED_SPARK_COMMIT}, checked separately from the measured HEAD",
            "not set -- harness/env.sh line 44 sets it, so the environment file appears "
            "not to have been sourced and the environment makes no claim to check",
            FAULT))
    elif declared != PINNED_SPARK_COMMIT:
        checks.append(Check(
            "declared-commit-agrees", "$SPARK_SRC_COMMIT",
            f"equal to {PINNED_SPARK_COMMIT}, checked separately from the measured HEAD",
            f"the environment declares {declared}, which is not the pin. This check is "
            f"deliberately independent of the HEAD comparison above: a caller who "
            f"overrode both the tree and its declared commit would satisfy a gate that "
            f"compared them against each other",
            REFUSE))
    else:
        checks.append(Check(
            "declared-commit-agrees", "$SPARK_SRC_COMMIT",
            f"equal to {PINNED_SPARK_COMMIT}, checked separately from the measured HEAD",
            f"the environment declares {declared}, which is the pin", PASS))
    return checks, git_runs


def main(argv: list[str] | None = None) -> int:
    """Run every check, print the report, write the evidence, and return the status.

    ``--check-only`` suppresses both writes. Every measurement is still taken and the
    whole report is still printed; what is skipped is replacing the durable record, which
    is the evidence that the gate ran BEFORE an invocation and which an audit run would
    otherwise stamp with a later time.
    """
    args = list(sys.argv[1:] if argv is None else argv)
    check_only = "--check-only" in args
    unknown = [arg for arg in args if arg != "--check-only"]
    if unknown:
        print(
            "preflight_scan_target.py: unknown argument(s) "
            f"{' '.join(repr(a) for a in unknown)}; the only accepted form is "
            "--check-only. Refusing rather than guessing, because a gate that ignores "
            "what it was asked to do is a gate whose behaviour nobody can predict.",
            file=sys.stderr)
        return CONFIG_EXIT

    root = repo_root()
    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    report: list[str] = []

    def emit(text: str = "") -> None:
        report.append(text)
        print(text)

    checks: list[Check] = [check_smoke_override()]
    path_checks, path_detail = check_path_policy()
    checks.extend(path_checks)
    # After the character policy, because these two values are path values too and the
    # policy must clear them before anything resolves or stats them; before the target
    # checks, because a run whose artifact trees are missing must not reach a runner at
    # all and the reader should see that before the git measurements.
    tree_checks, tree_detail = check_artifact_trees()
    checks.extend(tree_checks)
    spark_ok = bool(
        isinstance(path_detail.get("SPARK_SRC"), dict)
        and path_detail["SPARK_SRC"].get("verdict") == PASS  # type: ignore[union-attr]
    )
    target_checks, git_runs = check_scan_target(spark_ok)
    checks.extend(target_checks)

    refusals = [check for check in checks if check.verdict == REFUSE]
    faults = [check for check in checks if check.verdict == FAULT]
    status = HALT_EXIT if refusals else (CONFIG_EXIT if faults else 0)
    verdict = REFUSE if refusals else (FAULT if faults else PASS)

    emit("=" * 84)
    emit("PRE-SCAN TARGET, SMOKE AND PATH-VALUE GATE")
    emit("=" * 84)
    emit()
    emit("  Refuses to invoke any scanner runner unless the tree to be scanned is the")
    emit("  pinned tree, the setup-time smoke override is absent from the environment")
    emit("  entirely, every harness path value a runner interpolates is ordinary text,")
    emit("  and both artifact trees already exist as directories. harness/lib/scope.sh")
    emit("  accepts any SPARK_SRC without comparing HEAD, takes HARNESS_SMOKE_TARGET")
    emit("  whenever it is non-empty, and holds a missing-tree guard that harness/env.sh")
    emit("  line 91's `mkdir -p ... || true` creates the trees ahead of -- and neither")
    emit("  that file nor any runner may be edited (AAP 0.3.2, 0.8.1, 0.6.3), so the")
    emit(f"  controls live here: a refusal exits {HALT_EXIT} before a caller reaches the")
    emit("  runner. Nothing is created, cleared or repaired by this gate.")
    emit()
    emit("  Gate source             : harness/lib/preflight_scan_target.py")
    emit("  Binding callers         : harness/lib/run-scanner-gated.sh (all nine runners),")
    emit("                            harness/lib/run-joern-gated.sh (step 1 of 4)")
    emit(f"  Checked at (UTC)        : {started}")
    emit(f"  Clone index             : {os.environ.get('BLITZY_CLONE_INDEX', '0')}")
    emit(f"  Repository root         : {root}")
    emit(f"  Pin authored here       : {PINNED_SPARK_COMMIT}")
    emit(f"  git executable          : {git_executable() or '<none found>'}")
    emit("  git child environment   : every GIT_* variable stripped, system and global")
    emit("                            config excluded (GIT_DIR alone would otherwise")
    emit("                            decide what HEAD the gate sees)")
    emit()
    emit("  Every check, in the order it was taken -- the character policy runs before")
    emit("  any value is handed to a subprocess or printed:")
    emit()

    width = max(len(check.check_id) for check in checks)
    for check in checks:
        emit(f"    [{check.verdict:6}] {check.check_id.ljust(width)}  {check.subject}")
        emit(f"               expected : {check.expectation}")
        for line_index, line in enumerate(_wrap(check.observed, 92)):
            emit(f"               {'observed :' if line_index == 0 else '          '} {line}")
        emit()

    if git_runs:
        emit("  git invocations taken (argument vectors, never a shell):")
        for run in git_runs:
            emit(f"    $ {' '.join(run.argv)}")
            emit(f"      exit {run.returncode}"
                 + (f", stdout {run.stdout!r}" if run.stdout else "")
                 + (f", stderr {run.stderr!r}" if run.stderr else ""))
        emit()

    emit("=" * 84)
    emit(f"VERDICT: {verdict}"
         + (f"  ({len(refusals)} refusal(s), {len(faults)} fault(s))" if status else ""))
    emit("=" * 84)
    if refusals:
        emit()
        emit(f"  REFUSED (exit {HALT_EXIT}). No runner may be invoked; nothing was scanned.")
        for check in refusals:
            emit(f"    - {check.check_id}: {check.observed}")
    if faults:
        emit()
        emit(f"  CONFIGURATION FAULT{'S' if len(faults) != 1 else ''} "
             f"(exit {CONFIG_EXIT} unless a refusal supersedes it): correct the "
             f"environment and re-run.")
        for check in faults:
            emit(f"    - {check.check_id}: {check.observed}")
    if not status:
        emit()
        emit("  The target is the pinned tree, no smoke override is present, and every")
        emit("  path value is ordinary text. A caller may invoke the runner.")
    emit()
    emit("  RESIDUAL, stated because none of it can be closed from a clone. Every file")
    emit("  named below is REFERENCE under AAP 0.6.1/0.6.5, so each defect is reported")
    emit("  with the exact patch provisioning must apply, and each is carried in")
    emit("  oss-scan-results/run-record.md. The gate above is the compensating control")
    emit("  for all three, and it compensates only for callers that read its status:")
    emit("  each harness/bin/run-<tool>.sh remains executable in its own right, and an")
    emit("  invocation that does not read this exit status is not bound by it.")
    emit()
    emit("    1. harness/bin/run-trivy.sh lines 52-53 -- CWE-94. Both lines build a")
    emit("       python3 -c program by interpolating $TRIVY_CACHE_DIR into its source")
    emit("       text, so a cache directory whose literal name closes the embedded")
    emit("       string executes arbitrary Python with the process's ambient")
    emit("       environment. Proven exploitable at this checkpoint: a crafted value")
    emit("       created a marker file while the runner still exited 0 and wrote a valid")
    emit("       artifact, so the compromise leaves no failure signal.")
    emit("       PATCH: pass the path as positional argv to fixed code instead of")
    emit("       interpolating environment text into program source --")
    emit("       python3 -c 'import json,sys;d=json.load(open(sys.argv[1]));")
    emit("       print(\"v%s UpdatedAt=%s\"%(d[\"Version\"],d[\"UpdatedAt\"]))'")
    emit("       \"$TRIVY_CACHE_DIR/db/metadata.json\" -- and the same for java-db on :53.")
    emit("       Mitigated here by the path-text policy above, which refuses every")
    emit("       shell- and source-significant character in $TRIVY_CACHE_DIR before any")
    emit("       runner is invoked, and which names that variable in its own diagnostic.")
    emit()
    emit("    2. harness/env.sh line 91 -- the missing-tree halt AAP 0.8.1 mandates is")
    emit("       dead. `mkdir -p \"$HARNESS_RAW_DIR\" \"$HARNESS_LOG_DIR\"")
    emit("       \"$HARNESS_SCRATCH_DIR\" 2>/dev/null || true` runs on every runner's")
    emit("       source of env.sh, so scope.sh lines 68-69's correct `-d` guard is")
    emit("       handed trees that were created on the way to it; and `|| true` also")
    emit("       swallows a permission failure, making a tree that could not be created")
    emit("       indistinguishable from one that was.")
    emit("       PATCH: drop the two artifact trees from the mkdir -p, keeping only")
    emit("       HARNESS_SCRATCH_DIR (which this run legitimately owns and creates), and")
    emit("       drop the `|| true`, so a failure to create the scratch directory is")
    emit("       reported rather than hidden: `mkdir -p \"$HARNESS_SCRATCH_DIR\"`.")
    emit("       Provisioning leaves raw/ and logs/ created and empty; this run neither")
    emit("       creates nor clears them. Mitigated here by the artifact-tree checks")
    emit("       above, which refuse an absent or non-directory tree before any runner.")
    emit()
    emit("    3. harness/env.sh lines 19-22 -- both preflight gates' documented override")
    emit("       is destroyed by sourcing the environment. HARNESS_DIR and")
    emit("       HARNESS_REPO_ROOT are assigned UNCONDITIONALLY while 27 other exports")
    emit("       use ${VAR:-default}, so a caller who exports HARNESS_REPO_ROOT to point")
    emit("       a gate at a specific checkout has it silently overwritten the moment")
    emit("       env.sh is sourced -- which is what every runner and every documented")
    emit("       shell does. Both gates implement and document that override")
    emit("       (preflight_scan_target.py repo_root, preflight_graph_identity.py")
    emit("       repo_root), and it works only in a shell that has NOT sourced env.sh.")
    emit("       PATCH: `export HARNESS_DIR=\"${HARNESS_DIR:-$(cd \"$(dirname")
    emit("       \"$_harness_self\")\" && pwd)}\"` and the same shape for")
    emit("       HARNESS_REPO_ROOT, matching the other 27 exports.")
    emit()

    record = {
        "gate": "harness/lib/preflight_scan_target.py",
        "findings_addressed": ["SEC-01", "SEC-03", "QA Issue 2", "QA Issue 12"],
        "checked_at_utc": started,
        "clone_index": os.environ.get("BLITZY_CLONE_INDEX", "0"),
        "repository_root": str(root),
        "pin_authored_in_module": PINNED_SPARK_COMMIT,
        "git_executable": git_executable(),
        "git_child_environment": "every GIT_* stripped; GIT_CONFIG_NOSYSTEM=1; "
                                 "GIT_CONFIG_GLOBAL=/dev/null; GIT_TERMINAL_PROMPT=0",
        "check_only": check_only,
        "verdict": verdict,
        "exit_status": status,
        "refusal_count": len(refusals),
        "fault_count": len(faults),
        "checks": [check._asdict() for check in checks],
        "path_policy": path_detail,
        "artifact_trees": tree_detail,
        "artifact_tree_policy": (
            "existence as a directory is REFUSED on absence (AAP 0.8.1/0.9.2, QA Issue 2: "
            "env.sh line 91's `mkdir -p ... || true` runs on every runner's source of "
            "env.sh, so scope.sh lines 68-69's -d guard cannot fire for a missing tree). "
            "Neither tree is created or cleared by this gate. A tree that exists and is "
            "non-empty does NOT refuse here: emptiness is the Stage-0 gate condition owned "
            "by harness/artifacts/logs/gate-record.json"
        ),
        "git_invocations": [
            {"argv": list(run.argv), "exit": run.returncode,
             "stdout": run.stdout, "stderr": run.stderr}
            for run in git_runs
        ],
        "residual": (
            "harness/bin/run-<tool>.sh remains directly executable and is not bound by "
            "this exit status, so the three defects below are compensated for rather than "
            "closed. Every file named is REFERENCE under AAP 0.6.1/0.6.5 and each patch "
            "belongs to provisioning; all three are carried in "
            "oss-scan-results/run-record.md."
        ),
        "residual_provisioning_defects": [
            {
                "id": "QA Issue 10 / SEC-03",
                "location": "harness/bin/run-trivy.sh lines 52-53",
                "class": "CWE-94 code injection",
                "defect": "$TRIVY_CACHE_DIR is interpolated into the source text of a "
                          "python3 -c program, so a directory name that closes the "
                          "embedded string executes arbitrary Python; proven exploitable "
                          "with the runner still exiting 0 and writing a valid artifact",
                "patch": "pass the metadata path as positional argv to fixed code -- "
                         "python3 -c 'import json,sys;d=json.load(open(sys.argv[1]));"
                         "print(\"v%s UpdatedAt=%s\"%(d[\"Version\"],d[\"UpdatedAt\"]))' "
                         "\"$TRIVY_CACHE_DIR/db/metadata.json\" -- and the same for "
                         "java-db on line 53",
                "compensating_control_here": "path-policy:TRIVY_CACHE_DIR refuses every "
                                             "shell- and source-significant character "
                                             "before any runner is invoked",
            },
            {
                "id": "QA Issue 2",
                "location": "harness/env.sh line 91",
                "class": "AAP 0.8.1/0.9.2 mandated halt unreachable",
                "defect": "mkdir -p \"$HARNESS_RAW_DIR\" \"$HARNESS_LOG_DIR\" "
                          "\"$HARNESS_SCRATCH_DIR\" 2>/dev/null || true runs on every "
                          "runner's source of env.sh, so scope.sh lines 68-69's -d guard "
                          "is handed trees created on the way to it and the missing-tree "
                          "halt cannot fire; || true also swallows permission failures",
                "patch": "drop the two artifact trees from the mkdir -p, keeping only "
                         "HARNESS_SCRATCH_DIR, and drop the || true: "
                         "mkdir -p \"$HARNESS_SCRATCH_DIR\"",
                "compensating_control_here": "artifact-tree:HARNESS_RAW_DIR and "
                                             "artifact-tree:HARNESS_LOG_DIR refuse an "
                                             "absent or non-directory tree, and create "
                                             "and clear nothing",
            },
            {
                "id": "QA Issue 18 / D23 / P5-1",
                "location": "harness/env.sh lines 19-22",
                "class": "configuration: a documented override destroyed by sourcing",
                "defect": "HARNESS_DIR and HARNESS_REPO_ROOT are assigned "
                          "unconditionally while 27 other exports use ${VAR:-default}, so "
                          "both preflight gates' documented HARNESS_REPO_ROOT override is "
                          "silently overwritten in exactly the shells the runners use",
                "patch": "export HARNESS_DIR=\"${HARNESS_DIR:-$(cd \"$(dirname "
                         "\"$_harness_self\")\" && pwd)}\" and the same shape for "
                         "HARNESS_REPO_ROOT",
                "compensating_control_here": "none available: a gate cannot restore a "
                                             "value the environment file overwrote before "
                                             "the gate started. Reported only, and the "
                                             "override remains usable in a shell that has "
                                             "not sourced env.sh",
            },
        ],
        "value_disclosure_policy": (
            "a value that passes the character policy is recorded in full (path text, "
            "which AAP 0.1.3 excludes from secrets); a refused value is recorded only by "
            "length, sha256 and the class and index of each violation"
        ),
    }

    logs = root / "harness/artifacts/logs"
    console_out = logs / "sec-gate-scan-target.log"
    json_out = logs / "sec-gate-scan-target.json"
    if check_only:
        print(f"--check-only: {console_out} and {json_out} were NOT written, so the "
              f"ordering evidence for a gated invocation is not overwritten by an audit run")
    else:
        try:
            logs.mkdir(parents=True, exist_ok=True)
            console_out.write_text("\n".join(report).rstrip("\n") + "\n")
            json_out.write_text(json.dumps(record, indent=2, sort_keys=False) + "\n")
        except OSError as exc:
            # A gate that cannot write its evidence has not established the ordering the
            # AAP requires logged, so it refuses rather than proceeding quietly.
            print(f"preflight_scan_target.py: could not write the evidence record: {exc}",
                  file=sys.stderr)
            return CONFIG_EXIT
        print(f"wrote {console_out} ({console_out.stat().st_size} B) and "
              f"{json_out} ({json_out.stat().st_size} B)")
    return status


def _wrap(text: str, width: int) -> list[str]:
    """Wrap ``text`` to ``width`` on word boundaries, preserving every character.

    Hand-rolled rather than ``textwrap`` so that a long unbroken token -- a digest, or a
    path -- is emitted whole on its own line instead of being split at an arbitrary
    column, which would make a reader unable to copy it back out of the report.
    """
    words = text.split()
    if not words:
        return [""]
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        if len(current) + 1 + len(word) <= width:
            current = f"{current} {word}"
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return lines


if __name__ == "__main__":
    sys.exit(main())
