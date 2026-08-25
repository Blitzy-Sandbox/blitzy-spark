"""The field-by-field test of the Gitleaks 8.30.1 native adapter.

AAP 0.6.1 gives this file its own row -- *"Asserts the path base taken from the
recorded invocation, and that no secret value reaches any field"* -- AAP 0.5.4 fixes
the behaviour it asserts, AAP 0.9.4 puts it in the definition of done, and AAP 0.9.2
makes a failure here a condition that stops the run. Two of its assertions are unique
in this folder: the **invocation-derived path base** and the **no-secret-value
invariant**.

The module under test is ``harness/lib/normalize/adapters/gitleaks.py``. Nothing under
``harness/lib/normalize/`` is written by this file, and no fixture is ever modified: a
disagreement between an adapter and a hand-verified expectation is a finding to
diagnose, not a file to overwrite.

Rules
-----
No user-specified rule governs this file. ``review_rules`` returns exactly one line,
``No user rules provided.``, and that line is the whole document -- not a truncated
read -- corroborated by AAP 0.7 and AAP 0.10.2. No rule is cited or invented here.
Enterprise best practice applies in their place and the absence is **not** licence to
lower the bar: concretely, the no-secret invariant below is asserted *structurally*,
over every one of the twelve fields of every row and against every sensitive value
present anywhere in the artifact, rather than spot-checked on the one field a reader
would think of first.

The contract under test
-----------------------
``adapt(doc, *, tool, root, tool_base, allowlist, tally)`` returns
``(rows, rejections, counters)``. The artifact is a **bare top-level JSON array**, one
element per finding, which is also the count unit ``reconcile.py`` walks. Field
sources, from AAP 0.5.4's per-shape table: ``rule_id`` from ``RuleID``; ``message``
from ``Description``, the *rule's* description; ``path`` from ``File``; ``start_line``
from ``StartLine``. ``severity_native`` is always absent because the tool defines no
severity vocabulary, and ``cwe``, ``cve`` and ``package_coordinate`` are absent
because the shape supplies none of them. ``scanner_class`` is the constant ``secret``,
fixed by AAP 0.5.4's class table.

The seventeen required assertions, and the class that owns each
--------------------------------------------------------------
:class:`PathBaseFromMetadataTests`
    1. the base comes from the recorded metadata, not from a constant
    2. change the recorded base and every resolved path changes with it
    3. one path per invocation versus several paths in one

:class:`PathShapeAndScopeTests`
    4. every emitted path is relative to the root and never absolute
    5. outside the twelve globs is kept with ``in_scope`` false; inside it is true

:class:`NoSeverityVocabularyTests`
    6. ``severity_native`` is ``None`` on every row
    7. ``severity_norm`` is ``Info`` on every row and never absent
    8. the basis is the no-vocabulary route -- stated, not assumed
    9. the tally records **no** native literal for this tool

:class:`NoSecretValueTests`
    10. ``message`` is ``Description``, never ``Secret`` and never ``Match``
    11. the structural sweep over every field of every row
    12. the committed fixtures carry no live-looking credential
    13. ``cwe``, ``cve`` and ``package_coordinate`` are ``None``

:class:`PositiveMappingTests`
    14. the row count, and every field of every row in ``FIELDS`` order
    15. ``tool`` and ``scanner_class`` on every row
    16. ``start_line`` is an integer taken from ``StartLine``

:class:`BareArrayShapeTests`
    17. a bare array is not a mapping, and an empty one is not an error

:class:`DeclaredContractTests` and :class:`FixtureInventoryTests` run before all of
them, and exist because every assertion above walks rows or fixtures: a fixture that
silently failed to parse, or an expectation file with an empty ``rows`` array, would
let a whole class pass over an empty loop. :class:`RejectionTests` covers the negative
fixtures and :class:`CallerContractTests` the faults that are raised rather than
counted.

Why the path base is the hardest thing here
-------------------------------------------
``gitleaks dir`` takes **exactly one path** and reports relative to the **process
working directory** when handed more, so the base is a property of the *invocation*
rather than of the tool. AAP 0.5.4 states both readings: *"one path per invocation
makes paths relative to that directory; several paths in one makes them relative to
the recorded working directory."* Both shapes are real -- AAP 0.2.3 describes a
historical harness that passed all eighteen scope directories to a single invocation,
and this provisioning's runner instead sets its working directory to the scan root and
hands over one root-relative directory per invocation, eighteen times, which is what
``harness/artifacts/logs/runner-metadata.json`` records.

That is why assertion 2 is the highest-value check in this file. A wrong base
mis-resolves **every** Gitleaks row while every row still looks well-formed: nothing
about a path naming a Scala source under ``core/src/main`` reveals which directory it
was reported relative to. So the base is read from the recorded metadata, and this
file proves the reading by running one document under two different recorded bases and
requiring the emitted paths to differ exactly as the bases dictate. A runner is never
edited to make one shape true; AAP 0.3.2 forbids that, and a base that differs from
expectation is a condition to record.

Two places where this file follows the recorded facts rather than a summary of them
----------------------------------------------------------------------------------
Both are stated because a reader comparing this file against a prose description of
the harness would otherwise read a deliberate choice as an error.

* **The counter key set.** The adapter publishes fifteen counters --
  ``multi_location_records``, ``non_filesystem_paths``, ``rows_in_scope``,
  ``rows_out_of_scope``, ``start_line_absent``, ``severity_absent``, one
  ``path_kind_*`` per :data:`normalize.paths.PATH_KINDS` member and one
  ``severity_basis_*`` per :data:`normalize.severity.BASIS_VALUES` member. There is
  deliberately no ``multi_valued_cwe_records`` or ``multi_valued_cve_records``,
  because a Gitleaks record carries neither identifier and a counter that could only
  ever read zero would assert nothing. This file asserts the key set the adapter
  declares in ``COUNTER_KEYS``, which is also the set every ``expected/*.rows.json``
  records.
* **The recorded invocation shape.** This file does not assert *which* of the two
  shapes the provisioned runner used. It asserts that the shape is recorded, that the
  base is taken from that record, and that both readings resolve correctly -- which is
  what survives a re-provisioning. Asserting one shape as a constant would rebuild, in
  a test, exactly the assumption the metadata exists to displace.

Rejection conditions this adapter can produce -- all six, all asserted
---------------------------------------------------------------------
``gitleaks.REJECT_CLASSES_PRODUCED`` declares six, every one a member of the closed
:data:`normalize.paths.REJECT_CLASSES` vocabulary. Five come from committed negative
fixtures and the sixth from the metadata side:

* ``malformed_record`` -- ``reject-gitleaks-malformed-record.json``
* ``missing_rule_id`` -- ``reject-gitleaks-missing-rule-id.json``
* ``missing_message`` -- ``reject-gitleaks-missing-message.json``
* ``non_integer_start_line`` -- ``reject-gitleaks-non-integer-start-line.json``
* ``absent_path`` -- ``reject-gitleaks-unresolvable-path.json``. The class is
  ``absent_path`` rather than the ``unresolvable_path`` the filename suggests, and the
  expectation file's own ``reject_class_divergence`` block sets out why. Every class
  asserted here is read from the expectation file, never inferred from a filename.
* ``unresolvable_path`` -- produced where the recorded metadata establishes no base at
  all, which no committed artifact exhibits. AAP 0.9.4 requires an assertion for every
  condition an adapter can produce *"whether or not this run's artifacts contained
  that case"*, so it is exercised from a metadata document rather than left untested.

Four conditions in that closed vocabulary this adapter **cannot** produce, with the
reason each is out of reach -- asserted in
:meth:`RejectionTests.test_conditions_this_adapter_cannot_produce` so the claim is
checkable rather than merely written down:

* ``invalid_uri`` -- there is no URI in this shape. ``File`` is a filesystem path, and
  the bounded ``uriBaseId`` chain walk belongs to the shared SARIF adapter, which this
  artifact never routes to.
* ``ambiguous_source_resolution`` -- there is no bytecode input. Resolving a class
  identifier to a source file, and rejecting the ambiguous resolution, belongs to the
  adapter for the graph-derived artifact.
* ``unformable_package_coordinate`` -- a secret finding names a code location rather
  than a package, so ``package_coordinate`` is absent **by design**. AAP 0.5.4 makes an
  unformable coordinate a rejection for a *dependency-oriented* record only, and no
  record in this shape is one.
* ``unattributable_section`` -- this shape has no sections. The whole document is one
  flat array, so there is no per-section array a record could fail to be attributed to.

What this file deliberately does not do
---------------------------------------
AAP 0.3.2 and AAP 0.8.2, in full force. It performs **no cross-tool interpretation of
any kind**: no assertion, message, comment or docstring here ranks, contrasts or
explains this tool's output against another's. It compares nothing against any
commercial scanner. It judges no finding real, important, a false positive or a
duplicate, and it deduplicates nothing. It asserts nothing about an exit code:
artifact status and exit status are independent (AAP 0.5.4), an exit code belongs to
``harness/artifacts/logs/<tool>.status`` rather than to the adapter, and the adapter
never consults one.

And it contains **no secret value**. The runner passes a redaction flag, so no
captured value reached the artifact in the first place -- but that is upstream
protection in a runner this run may not edit, and it is not the check. The sensitive
values this file sweeps for are read out of the fixtures at run time and never written
here; the one value written literally is a self-evidently synthetic sentinel that no
scanner would treat as live, present so the sweep has something it demonstrably
catches (see :meth:`NoSecretValueTests.test_the_sweep_detects_a_planted_value`).
Nowhere does this file join a path, a rule identifier and a line number into a single
colon-separated string, because that is the shape of this artifact's fingerprint
values.

How to run it
-------------
From the repository root::

    python3 -m unittest discover -s oss-scan-results/adapter-tests \\
        -p 'test_gitleaks_adapter.py' -v

Standard library only (AAP 0.4.1), so no plugin and no installed package is needed and
AAP 0.4.3 adds no dependency in any direction. The module imports nothing outside the
standard library and the ``normalize`` package, which
:meth:`DeclaredContractTests.test_no_third_party_import` asserts against this file's
own source. It runs from any working directory: every path it touches is derived from
``__file__`` or created inside a :class:`tempfile.TemporaryDirectory`.
"""

from __future__ import annotations

import ast
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

# --------------------------------------------------------------------------------- #
# The one-time sys.path bootstrap.
#
# There is deliberately no __init__.py under harness/lib/normalize/: PEP 420 implicit
# namespace packages make "from normalize import paths" work once harness/lib is on
# sys.path. cli.py owns the same two lines for its own direct-script route and its
# comment names this import route explicitly, so the two agree by construction.
#
# This file sits at <repo>/oss-scan-results/adapter-tests/, so parents[2] is the
# repository root. Guarded on absence rather than inserted unconditionally, so that
# running two test modules in one process leaves one entry rather than two.
# --------------------------------------------------------------------------------- #

REPO_ROOT = Path(__file__).resolve().parents[2]
_LIB_DIR = str(REPO_ROOT / "harness" / "lib")
if _LIB_DIR not in sys.path:
    sys.path.insert(0, _LIB_DIR)

from normalize import emit  # noqa: E402  (import follows the bootstrap by necessity)
from normalize import paths  # noqa: E402
from normalize import severity  # noqa: E402
from normalize.adapters import gitleaks  # noqa: E402

# --------------------------------------------------------------------------------- #
# Locations. Every directory below is an input and is never written to by this module;
# everything this module writes goes inside a temporary directory it owns.
# --------------------------------------------------------------------------------- #

ADAPTER_TESTS_DIR = Path(__file__).resolve().parent
FIXTURES_DIR = ADAPTER_TESTS_DIR / "fixtures"
EXPECTED_DIR = ADAPTER_TESTS_DIR / "expected"

#: The run's own recorded runner metadata -- the document ``paths.py`` resolves
#: against in production. Read here to assert that the provisioned base is honoured as
#: *recorded*, never to hard-code the value it happens to record.
RECORDED_METADATA_PATH = REPO_ROOT / "harness" / "artifacts" / "logs" / "runner-metadata.json"

# --------------------------------------------------------------------------------- #
# The twelve authoritative scope globs (AAP 0.3.1), byte-exact and in the request's
# order.
#
# Restated here independently rather than read from paths.ALLOWLIST_GLOBS: this module
# writes these twelve lines to its own allowlist file, loads them back through
# paths.load_allowlist() and then confirms the loaded tuple is what paths.py authors,
# via paths.allowlist_matches_authoritative_globs(). Loading the module's own copy and
# comparing it with itself would assert nothing.
#
# There is no exclusion line: the literal `src/test` exclusion is paths.py's, applied
# inside its in_scope matcher, and is not part of the allowlist file.
# --------------------------------------------------------------------------------- #

AUTHORITATIVE_GLOBS = (
    "core/src/main/**",
    "common/network-common/src/main/**",
    "common/network-shuffle/src/main/**",
    "common/network-yarn/src/main/**",
    "sql/catalyst/src/main/**",
    "sql/core/src/main/**",
    "sql/connect/**/src/main/**",
    "sql/hive/src/main/**",
    "sql/hive-thriftserver/src/main/**",
    "resource-managers/kubernetes/**/src/main/**",
    "resource-managers/yarn/src/main/**",
    "python/pyspark/**",
)

# --------------------------------------------------------------------------------- #
# The tool, its fixtures and its expectations
# --------------------------------------------------------------------------------- #

#: The canonical tool identifier every row carries (AAP 0.5.4).
TOOL = "gitleaks"

#: The ``scanner_class`` fixed for this tool by AAP 0.5.4's class table.
EXPECTED_SCANNER_CLASS = "secret"

#: The captured positive fixture, and the negative fixture per rejection condition.
POSITIVE_FIXTURE = "gitleaks"
NEGATIVE_FIXTURES = (
    "reject-gitleaks-unresolvable-path",
    "reject-gitleaks-missing-rule-id",
    "reject-gitleaks-missing-message",
    "reject-gitleaks-non-integer-start-line",
    "reject-gitleaks-malformed-record",
)
ALL_FIXTURES = (POSITIVE_FIXTURE, *NEGATIVE_FIXTURES)

#: The record keys whose values are sensitive by nature and must never reach a field.
#:
#: Taken from the adapter's own ``NEVER_READ_FIELDS`` declaration rather than retyped:
#: ``Secret`` and ``Match`` are the captured value, and ``Fingerprint`` is a composite
#: that in commit mode also carries a commit identity. Their *values* are read out of
#: the fixtures at run time and never written into this file.
SENSITIVE_RECORD_KEYS = ("Secret", "Match", "Fingerprint")

#: The shortest sensitive value the sweep will search for.
#:
#: A one- or two-character value -- and above all the empty string, which is a
#: substring of everything -- would make the sweep report a violation for every row
#: while testing nothing. Values below this length are skipped and counted, and the
#: sweep asserts it considered something.
MINIMUM_SWEEP_VALUE_LENGTH = 4

#: A self-evidently synthetic sentinel, planted in an in-memory record so the sweep has
#: something it demonstrably catches. Deliberately not shaped like any provider's token
#: and matched by none of :data:`LIVE_CREDENTIAL_MARKERS`.
SYNTHETIC_SENTINEL_PREFIX = "SYNTHETIC-PLACEHOLDER-NOT-A-CREDENTIAL"
SYNTHETIC_MATCH_SENTINEL = f"{SYNTHETIC_SENTINEL_PREFIX}-MATCH-0000"
SYNTHETIC_SECRET_SENTINEL = f"{SYNTHETIC_SENTINEL_PREFIX}-SECRET-0000"
SYNTHETIC_FINGERPRINT_SENTINEL = f"{SYNTHETIC_SENTINEL_PREFIX}-FINGERPRINT-0000"

#: Prefixes and delimiters that identify a live credential from a named issuer.
#:
#: The fixtures are committed to git and this tree is not ignored, so their bytes are
#: scanned for these. Each is an issuer-specific *prefix* rather than a word, so a rule
#: identifier naming a credential kind -- which several fixtures legitimately carry --
#: cannot match one.
LIVE_CREDENTIAL_MARKERS = (
    "sk_live_",
    "sk_test_",
    "pk_live_",
    "rk_live_",
    "AKIA",
    "ASIA",
    "ghp_",
    "gho_",
    "ghs_",
    "ghu_",
    "ghr_",
    "github_pat_",
    "xoxb-",
    "xoxa-",
    "xoxp-",
    "xoxr-",
    "AIza",
    "ya29.",
    "sk-ant-",
    "AccountKey=",
    "-----BEGIN",
    "PRIVATE KEY-----",
    "://admin:",
    "://root:",
)


# --------------------------------------------------------------------------------- #
# Fixture and expectation loading
# --------------------------------------------------------------------------------- #


def fixture_path(stem: str) -> Path:
    """Return the committed fixture path for ``stem``."""
    return FIXTURES_DIR / f"{stem}.json"


def expected_path(stem: str) -> Path:
    """Return the hand-verified expectation path for ``stem``."""
    return EXPECTED_DIR / f"{stem}.rows.json"


def load_fixture(stem: str) -> list:
    """Return one committed fixture, parsed.

    Read fresh on each call and never cached across tests: a cached document could be
    mutated by one test and silently change what a later one asserts, and this module
    must not modify a fixture in memory any more than on disk.
    """
    return json.loads(fixture_path(stem).read_text(encoding="utf-8"))


def load_expected(stem: str) -> dict:
    """Return one hand-verified expectation document, parsed."""
    return json.loads(expected_path(stem).read_text(encoding="utf-8"))


def sha256_of(path: Path) -> str:
    """Return the hex sha256 of a file's bytes."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


# --------------------------------------------------------------------------------- #
# The hermetic environment
# --------------------------------------------------------------------------------- #

#: Sentinel meaning "record the scan root as this tool's base value".
#:
#: A distinct object rather than ``None``, because ``None`` is itself a meaningful
#: recorded value -- it is what a base kind that establishes no base carries, and the
#: ``unresolvable_path`` assertion depends on being able to record exactly that.
SCAN_ROOT = object()


class Environment:
    """A scan root, an allowlist and runner-metadata documents, all inside a temp tree.

    Every input an ``adapt`` call needs is created here and read back through
    ``paths.py``'s own loaders, so the loaders are exercised on the same route
    ``cli.py`` uses rather than bypassed with literals.

    Nothing in the resolution path reads the filesystem: a reported path is expressed
    against the root by parts arithmetic, and ``in_scope`` is decided by matching the
    allowlist globs against the resolved string. :meth:`materialise` therefore exists
    to make that independence visible rather than to satisfy a resolver -- a test that
    passes with the referenced files present but empty cannot be depending on their
    content.

    Attributes:
        directory: The temporary directory holding everything this object created.
        root: The absolute scan root, as a string, which is what ``adapt`` takes.
        root_path: The same root as a :class:`pathlib.Path`.
        allowlist_path: The allowlist file this object wrote.
        globs: The twelve globs as ``paths.load_allowlist`` returned them.
    """

    def __init__(self, directory: Path) -> None:
        """Create the scan root and write, then load, the allowlist."""
        self.directory = directory
        self.root_path = directory / "spark-src"
        self.root_path.mkdir(parents=True, exist_ok=True)
        self.root = str(self.root_path)

        self.allowlist_path = directory / "allowlist.txt"
        # One glob per line, byte-exact, with a trailing newline and nothing else.
        self.allowlist_path.write_text(
            "".join(f"{glob}\n" for glob in AUTHORITATIVE_GLOBS), encoding="utf-8"
        )
        self.globs = paths.load_allowlist(self.allowlist_path)
        self._metadata_serial = 0

    def materialise(self, *relative_paths: str) -> tuple[Path, ...]:
        """Create each relative path under the root as an empty file.

        Returns the created paths. Deliberately empty: see the class docstring on why
        content is never what a resolution turns on.
        """
        created: list[Path] = []
        for relative in relative_paths:
            target = self.root_path / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.touch()
            created.append(target)
        return tuple(created)

    def metadata_document(
        self,
        *,
        kind: str,
        base_value: object = SCAN_ROOT,
        working_directory: str | None = None,
        invocations_per_run: int | None = None,
        target_count: int | None = None,
        record_path_field: str | None = "File",
    ) -> dict:
        """Build one runner-metadata document recording a base for this tool.

        Every argument names something the real document records for a runner, so a
        test can vary exactly one of them and leave the rest alone. ``base_value``
        defaults to the scan root and accepts ``None`` to record a base kind that
        establishes no base at all.
        """
        path_base: dict = {"kind": kind}
        path_base["value"] = self.root if base_value is SCAN_ROOT else base_value
        if record_path_field is not None:
            path_base["record_path_field"] = record_path_field
        path_base["evidence"] = (
            "Recorded by this test from the invocation it is asserting; the production "
            "document records it from the runner as invoked."
        )

        invocation_form: dict = {}
        if invocations_per_run is not None:
            invocation_form["invocations_per_run"] = invocations_per_run
        if target_count is not None:
            invocation_form["target_count"] = target_count
            invocation_form["target_passing_style"] = (
                "exactly one path per invocation"
                if invocations_per_run == target_count
                else "several paths in one invocation"
            )

        entry: dict = {
            "canonical_tool_identifier": TOOL,
            "scanner_class": EXPECTED_SCANNER_CLASS,
            "path_base": path_base,
            "resolved_scan_root": self.root,
            "invocation_form": invocation_form,
        }
        if working_directory is not None:
            entry["working_directory"] = {"path": working_directory}

        return {
            "purpose": (
                "Minimal runner metadata for the Gitleaks adapter test. Written and "
                "read inside a temporary directory; it is not the run's record."
            ),
            "spark_src": self.root,
            "tools": {TOOL: entry},
        }

    def recorded_base(self, **kwargs: object) -> paths.ToolPathBase:
        """Write a metadata document, load it through ``paths.py``, return the base.

        The document reaches ``paths.load_runner_metadata`` as a real file, so this is
        the same read path production takes; nothing here constructs a
        :class:`normalize.paths.ToolPathBase` directly, which would skip the loader
        whose correctness is half of what the base assertions are about.
        """
        self._metadata_serial += 1
        document = self.metadata_document(**kwargs)  # type: ignore[arg-type]
        location = self.directory / f"runner-metadata-{self._metadata_serial}.json"
        location.write_text(json.dumps(document, indent=1) + "\n", encoding="utf-8")
        return paths.tool_path_base(paths.load_runner_metadata(location), TOOL)

    def scan_root_base(self) -> paths.ToolPathBase:
        """The base this provisioning records: the scan root, ``File`` root-relative.

        Eighteen invocations, one root-relative directory each, with the working
        directory set to the scan root -- the shape
        ``harness/artifacts/logs/runner-metadata.json`` records and the one every
        ``expected/*.rows.json`` was derived under.
        """
        return self.recorded_base(
            kind=paths.PATH_BASE_KIND_SCAN_ROOT,
            working_directory=self.root,
            invocations_per_run=18,
            target_count=18,
        )


# --------------------------------------------------------------------------------- #
# One adaptation, and the sweep that runs over it
# --------------------------------------------------------------------------------- #


class Adaptation:
    """One ``adapt`` call's three results, the document it read and the tally it fed.

    Held together so a test asserts over one measurement rather than calling the
    adapter twice and comparing two: AAP 0.6.4 requires a count appearing twice to be
    one measurement cited twice.

    Attributes:
        document: The artifact the adapter was handed, exactly as parsed.
        rows: The dataset rows emitted, in array order.
        rejections: The :class:`normalize.paths.Rejection` records counted instead.
        counters: The adapter's own counters, over ``gitleaks.COUNTER_KEYS``.
        tally: The :class:`normalize.severity.LiteralTally` the call fed.
    """

    def __init__(
        self,
        document: object,
        rows: list,
        rejections: list,
        counters: dict,
        tally: severity.LiteralTally,
    ) -> None:
        self.document = document
        self.rows = rows
        self.rejections = rejections
        self.counters = counters
        self.tally = tally

    @property
    def record_count(self) -> int:
        """The number of elements in the top-level array -- the count unit."""
        return len(self.document)  # type: ignore[arg-type]

    @property
    def outcome_count(self) -> int:
        """Rows plus rejections: one outcome per element, never both and never neither."""
        return len(self.rows) + len(self.rejections)


def sensitive_values(document: object) -> tuple[str, ...]:
    """Return every sensitive value in ``document``, long enough to search for.

    A value from **any** record is returned rather than only the record a given row
    came from. That is deliberate and strictly stronger: rows and records do not share
    an index once a record has been rejected, and a sweep that had to map one onto the
    other could pass because the mapping was wrong rather than because nothing leaked.

    The direction of the eventual comparison matters too, and only this direction is
    sound: a sensitive value must not appear *inside* a field. The converse would fail
    legitimately, because a fingerprint composite contains the record's own path, and a
    row's path field is supposed to carry that path.

    Non-string values are rendered as compact JSON so a list or an object planted in
    one of these keys is searched for as well. Values shorter than
    :data:`MINIMUM_SWEEP_VALUE_LENGTH` are dropped: the empty string is a substring of
    every field, so keeping it would make the sweep report a violation everywhere while
    proving nothing.
    """
    collected: list[str] = []
    if not isinstance(document, list):
        return ()
    for record in document:
        if not isinstance(record, dict):
            continue
        for key in SENSITIVE_RECORD_KEYS:
            if key not in record:
                continue
            raw = record[key]
            text = raw if isinstance(raw, str) else json.dumps(raw, sort_keys=True)
            if len(text.strip()) < MINIMUM_SWEEP_VALUE_LENGTH:
                continue
            if text not in collected:
                collected.append(text)
    return tuple(collected)


def sweep_rows_for_values(rows: list, values: tuple[str, ...]) -> list[str]:
    """Return one description per (row, field, value) where a value reached a field.

    The structural form of the no-secret invariant: every one of the twelve fields of
    every row, against every value, iterating ``emit.FIELDS`` so a violation names the
    field it was found in. Returning the violations rather than asserting inside means
    the same helper proves the sweep can *fail* -- see
    :meth:`NoSecretValueTests.test_the_sweep_detects_a_planted_value`.
    """
    violations: list[str] = []
    for index, row in enumerate(rows):
        for field in emit.FIELDS:
            value = row.get(field)
            if value is None:
                continue
            text = value if isinstance(value, str) else json.dumps(value, sort_keys=True)
            for sensitive in values:
                if sensitive in text:
                    violations.append(
                        f"row {index} field {field!r} carries a value from a "
                        f"{'/'.join(SENSITIVE_RECORD_KEYS)} key"
                    )
    return violations


def sweep_rejections_for_values(rejections: list, values: tuple[str, ...]) -> list[str]:
    """Return one description per rejection whose detail or identity carried a value.

    The rejection channel needs the same guarantee as the row channel. AAP 0.5.4
    requires a parser reason retained verbatim, and a verbatim reason that interpolated
    a record's own text would turn that channel into a leak channel -- so the detail
    and every value in ``record_identity`` are swept exactly as a row's fields are.
    """
    violations: list[str] = []
    for index, rejection in enumerate(rejections):
        searchable = [rejection.reject_class, rejection.tool, rejection.detail]
        for key, value in dict(rejection.record_identity).items():
            searchable.append(str(key))
            searchable.append(
                value if isinstance(value, str) else json.dumps(value, sort_keys=True)
            )
        for text in searchable:
            for sensitive in values:
                if sensitive in text:
                    violations.append(
                        f"rejection {index} under {rejection.reject_class!r} carries a "
                        f"value from a {'/'.join(SENSITIVE_RECORD_KEYS)} key"
                    )
    return violations


def synthetic_record(
    *,
    rule_id: str = "generic-api-key",
    description: str = "Detected a Generic API Key, potentially exposing access.",
    file_value: object = "core/src/main/scala/org/apache/spark/storage/DiskStore.scala",
    start_line: object = 72,
    symlink_file: str = "",
) -> dict:
    """Return one record in the tool's own eighteen-key shape and key order.

    Used where a behaviour has no committed fixture -- a second reported location, an
    archive coordinate, a base that resolves nowhere -- because a fixture is never
    edited to reach one (AAP 0.3.2 on repairing nothing, and this folder's rule that a
    fixture is captured or derived, never adjusted to suit a test).

    The sensitive keys carry the synthetic sentinels, so any of them reaching a field
    is caught by the same sweep the captured fixtures go through.
    """
    return {
        "RuleID": rule_id,
        "Description": description,
        "StartLine": start_line,
        "EndLine": start_line,
        "StartColumn": 1,
        "EndColumn": 2,
        "Match": SYNTHETIC_MATCH_SENTINEL,
        "Secret": SYNTHETIC_SECRET_SENTINEL,
        "File": file_value,
        "SymlinkFile": symlink_file,
        "Commit": "",
        "Entropy": 1.0,
        "Author": "",
        "Email": "",
        "Date": "",
        "Message": "",
        "Tags": [],
        "Fingerprint": SYNTHETIC_FINGERPRINT_SENTINEL,
    }


# --------------------------------------------------------------------------------- #
# The shared base case
# --------------------------------------------------------------------------------- #


class GitleaksAdapterTestCase(unittest.TestCase):
    """A hermetic :class:`Environment` per test, and one way to call the adapter.

    A fresh temporary directory per test rather than one per module: the base
    assertions write several metadata documents each, and sharing a directory across
    tests would let one test's document be the one a later test accidentally loaded.
    """

    def setUp(self) -> None:
        """Create the temporary tree and the environment inside it."""
        self._temporary = tempfile.TemporaryDirectory(prefix="blitzy-gitleaks-adapter-")
        self.addCleanup(self._temporary.cleanup)
        self.environment = Environment(Path(self._temporary.name))

    def adapt(
        self,
        document: object,
        *,
        tool_base: paths.ToolPathBase | None = None,
        tool: str = TOOL,
        tally: severity.LiteralTally | None = None,
    ) -> Adaptation:
        """Call the adapter and return its three results with the tally it fed.

        ``tool_base`` defaults to the base this provisioning records, which is the base
        every committed expectation was derived under.
        """
        base = tool_base if tool_base is not None else self.environment.scan_root_base()
        recorder = tally if tally is not None else severity.LiteralTally.with_all_tools()
        rows, rejections, counters = gitleaks.adapt(
            document,
            tool=tool,
            root=self.environment.root,
            tool_base=base,
            allowlist=self.environment.globs,
            tally=recorder,
        )
        return Adaptation(document, rows, rejections, counters, recorder)

    def adapt_fixture(self, stem: str, **kwargs: object) -> Adaptation:
        """Load a committed fixture and adapt it."""
        return self.adapt(load_fixture(stem), **kwargs)  # type: ignore[arg-type]

    def assertRowsHaveTheTwelveFields(self, rows: list) -> None:
        """Assert every row carries exactly ``emit.FIELDS``, in that order.

        Order as well as membership: both output files are written by iterating the
        field list, and a row whose keys arrived in another order would still write
        correctly today while making any future dict-order-sensitive comparison wrong.
        """
        for index, row in enumerate(rows):
            with self.subTest(row=index):
                self.assertEqual(
                    list(row),
                    list(emit.FIELDS),
                    "a row must carry exactly the twelve fields, in emit.FIELDS order",
                )



# --------------------------------------------------------------------------------- #
# Guards. These run before every assertion that walks rows or fixtures, because an
# empty loop passes silently.
# --------------------------------------------------------------------------------- #


class DeclaredContractTests(unittest.TestCase):
    """What the adapter declares about itself, checked against the modules it must agree with.

    The adapter cannot import ``emit`` or ``shape`` -- AAP 0.6.4 permits it ``paths``
    and ``severity`` and nothing else -- so its ``FIELDS`` and ``SCANNER_CLASS`` are
    authored copies that have to agree *by construction*. Nothing else in the codebase
    is positioned to check that, so it is checked here, first.
    """

    def test_tool_identifier_is_canonical(self) -> None:
        """The identifier is the mechanical stem, and this module tests that tool."""
        self.assertEqual(gitleaks.TOOL, TOOL)
        self.assertIn(TOOL, severity.CANONICAL_TOOLS)
        self.assertIn(TOOL, paths.CANONICAL_TOOLS)

    def test_scanner_class_is_the_fixed_secret_class(self) -> None:
        """AAP 0.5.4's class table fixes ``secret``, and nothing varies it per record."""
        self.assertEqual(gitleaks.SCANNER_CLASS, EXPECTED_SCANNER_CLASS)

    def test_field_list_agrees_with_the_emitter(self) -> None:
        """The adapter's authored twelve are ``emit.FIELDS``, in order."""
        self.assertEqual(gitleaks.FIELDS, emit.FIELDS)
        self.assertEqual(len(emit.FIELDS), 12)

    def test_absence_permitted_fields_agree_with_the_emitter(self) -> None:
        """Absence is permitted for the same five fields on both sides (AAP 0.8.2)."""
        self.assertEqual(gitleaks.ABSENCE_PERMITTED_FIELDS, emit.OPTIONAL_FIELDS)
        self.assertNotIn("path", gitleaks.ABSENCE_PERMITTED_FIELDS)
        self.assertNotIn("severity_norm", gitleaks.ABSENCE_PERMITTED_FIELDS)

    def test_every_declared_reject_class_is_a_real_member(self) -> None:
        """Each producible class is a named member of the closed vocabulary.

        Asserted against ``paths.REJECT_CLASSES`` rather than against a list retyped
        here: a class counted under a name no status document knows about is exactly
        what the closed set exists to prevent.
        """
        self.assertEqual(len(gitleaks.REJECT_CLASSES_PRODUCED), 6)
        for reject_class in gitleaks.REJECT_CLASSES_PRODUCED:
            with self.subTest(reject_class=reject_class):
                self.assertIn(reject_class, paths.REJECT_CLASSES)
                self.assertTrue(paths.is_reject_class(reject_class))

    def test_source_and_never_read_key_sets_are_disjoint(self) -> None:
        """A key cannot be both a row's source and one the adapter never reads."""
        self.assertEqual(
            set(gitleaks.SOURCE_FIELDS) & set(gitleaks.NEVER_READ_FIELDS),
            set(),
        )
        self.assertEqual(
            gitleaks.SOURCE_FIELDS, ("RuleID", "Description", "File", "StartLine")
        )
        for key in SENSITIVE_RECORD_KEYS:
            with self.subTest(key=key):
                self.assertIn(key, gitleaks.NEVER_READ_FIELDS)

    def test_counter_key_set_is_fully_pre_initialised(self) -> None:
        """``new_counters`` returns every declared key at zero, and no other key.

        A missing key is ambiguous between "zero" and "this adapter forgot", which is
        the ambiguity a caller aggregating several artifacts cannot resolve.
        """
        counters = gitleaks.new_counters()
        self.assertEqual(tuple(counters), gitleaks.COUNTER_KEYS)
        self.assertEqual(set(counters.values()), {0})
        for kind in paths.PATH_KINDS:
            self.assertIn(f"path_kind_{kind}", counters)
        for basis in severity.BASIS_VALUES:
            self.assertIn(f"severity_basis_{basis}", counters)

    def test_no_third_party_import(self) -> None:
        """Every module this file imports is standard library or ``normalize``.

        AAP 0.4.1 permits the standard library only and AAP 0.4.3 adds no dependency in
        any direction, so this run introduces no manifest, no lockfile and no install
        step. Asserted against this file's own source, so adding an import cannot pass
        review by being invisible to the test suite.

        The source is parsed rather than scanned line by line. A textual scan for lines
        beginning ``import`` or ``from`` also matches prose in a docstring -- this
        module's own field-source sentences begin that way -- and a check that reports a
        violation for a sentence is a check nobody will keep.
        """
        permitted = {
            "__future__",
            "ast",
            "hashlib",
            "json",
            "sys",
            "tempfile",
            "unittest",
            "pathlib",
            "normalize",
        }
        tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
        observed: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    observed.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                observed.add(node.module.split(".")[0])
        self.assertTrue(observed, "the import scan found nothing, so it proved nothing")
        self.assertEqual(observed - permitted, set())


class FixtureInventoryTests(unittest.TestCase):
    """Every fixture and expectation is present, parses, and is the file it claims to be.

    This class exists so that no later assertion can pass over an empty loop. A fixture
    silently absent, an expectation whose ``rows`` array is empty, or a fixture edited
    since its expectation was derived would each let a field-by-field comparison
    succeed without comparing anything.
    """

    def test_every_fixture_is_a_non_empty_bare_array(self) -> None:
        """The shape is a bare top-level JSON array of findings (AAP 0.5.4)."""
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                self.assertTrue(fixture_path(stem).is_file())
                document = load_fixture(stem)
                self.assertIsInstance(document, list)
                self.assertGreater(len(document), 0)

    def test_every_expectation_is_present_and_names_this_tool(self) -> None:
        """Each expectation exists, parses, and fixes this tool and its class."""
        for stem in ALL_FIXTURES:
            with self.subTest(expected=stem):
                self.assertTrue(expected_path(stem).is_file())
                expected = load_expected(stem)
                self.assertEqual(expected["tool"], TOOL)
                self.assertEqual(expected["scanner_class"], EXPECTED_SCANNER_CLASS)
                self.assertEqual(tuple(expected["field_order"]), emit.FIELDS)
                self.assertIn("rows", expected)
                self.assertIn("counts", expected)
                self.assertIn("counters", expected)

    def test_fixture_digest_matches_the_expectation_that_was_derived_from_it(self) -> None:
        """The fixture is byte-for-byte the one its expectation was hand-derived from.

        Each expectation records the fixture's sha256 precisely so that a later edit is
        visible. An edited fixture means the expected values have to be re-derived, not
        that the recorded digest should be updated to match -- and either way the
        divergence must fail loudly here rather than shift what every other assertion
        in this file is comparing.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                expected = load_expected(stem)
                self.assertEqual(
                    sha256_of(fixture_path(stem)),
                    expected["fixture"]["sha256"],
                    f"{stem}.json differs from the fixture "
                    f"{stem}.rows.json was derived from",
                )

    def test_element_count_matches_the_recorded_record_count(self) -> None:
        """The count unit is the array's length, and both files agree on it.

        The left-hand side of ``raw finding records = dataset rows + rejected records``
        is a property of the document rather than of the adapter, so it is checkable
        here without adapting anything.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                expected = load_expected(stem)
                self.assertEqual(
                    len(load_fixture(stem)), expected["counts"]["raw_finding_records"]
                )

    def test_recorded_runner_metadata_is_present(self) -> None:
        """The run's own runner metadata exists and records an entry for this tool.

        A declared dependency of this file rather than an optional extra: it is the
        document ``paths.py`` resolves against in production, and
        :meth:`PathBaseFromMetadataTests.test_the_recorded_provisioned_base_is_honoured`
        reads it. Its absence is a configuration fault, so it fails here by name rather
        than surfacing later as a confusing base error.
        """
        self.assertTrue(
            RECORDED_METADATA_PATH.is_file(),
            f"{RECORDED_METADATA_PATH} is missing; the recorded path base is the input "
            "the resolver takes, and no default to the scan root substitutes for it",
        )
        document = paths.load_runner_metadata(RECORDED_METADATA_PATH)
        self.assertIn(TOOL, paths.metadata_tools(document))



# --------------------------------------------------------------------------------- #
# Assertions 1-3: the path base comes from the recorded invocation
# --------------------------------------------------------------------------------- #


class PathBaseFromMetadataTests(GitleaksAdapterTestCase):
    """The base is read from the recorded metadata, and reading it is what is proved.

    A wrong base mis-resolves every row of this tool while every row still looks
    well-formed, so no assertion in this class checks that a path *looks* plausible.
    Each one changes something the metadata records and requires the emitted paths to
    change with it -- which an implementation carrying a fixed base cannot do.

    The record under test is always written and then loaded back through
    ``paths.load_runner_metadata`` and ``paths.tool_path_base``, so the loader is on the
    same route production takes rather than bypassed by constructing a base object.
    """

    #: One record, reported relative to whichever directory the invocation anchored on.
    #: The value is deliberately the tail of a real in-scope path, so the same record
    #: resolves to a different real path under each base below.
    REPORTED_TAIL = "scala/org/apache/spark/storage/DiskStore.scala"

    def test_the_base_comes_from_the_recorded_metadata(self) -> None:
        """Assertion 1. The recorded document supplies the base, and it is used.

        Written into the temporary tree, loaded with ``paths.load_runner_metadata`` and
        read per tool with ``paths.tool_path_base``, then asserted to be what resolution
        followed. The recorded ``record_path_field`` is asserted too: it is the field
        the resolver reads the path out of, so a document naming another field would
        change which value became the row's path.
        """
        base = self.environment.recorded_base(
            kind=paths.PATH_BASE_KIND_SCAN_ROOT,
            working_directory=self.environment.root,
            invocations_per_run=18,
            target_count=18,
        )
        self.assertEqual(base.tool, TOOL)
        self.assertEqual(base.kind, paths.PATH_BASE_KIND_SCAN_ROOT)
        self.assertIn(base.kind, paths.PATH_BASE_KINDS)
        self.assertEqual(base.record_path_field, "File")
        self.assertTrue(base.has_explicit_base)
        self.assertEqual(base.base_for_relative(), self.environment.root)

        self.environment.materialise("core/src/main/" + self.REPORTED_TAIL)
        record = synthetic_record(file_value="core/src/main/" + self.REPORTED_TAIL)
        adapted = self.adapt([record], tool_base=base)

        self.assertEqual(len(adapted.rows), 1)
        self.assertEqual(adapted.rows[0]["path"], "core/src/main/" + self.REPORTED_TAIL)
        self.assertTrue(adapted.rows[0]["in_scope"])

    def test_changing_the_recorded_base_changes_every_resolved_path(self) -> None:
        """Assertion 2. The same document under two bases yields paths that differ.

        The single highest-value check in this file. Both runs are handed byte-identical
        records and differ in exactly one thing -- the directory the metadata records --
        so an adapter carrying a constant base cannot pass: it would return the same
        path twice. Both expected results are real in-scope paths, so neither run can be
        excused as the degenerate one.
        """
        under_core = self.environment.recorded_base(
            kind=paths.PATH_BASE_KIND_PER_TARGET_DIRECTORY,
            base_value=f"{self.environment.root}/core/src/main",
            invocations_per_run=1,
            target_count=1,
        )
        under_sql_core = self.environment.recorded_base(
            kind=paths.PATH_BASE_KIND_PER_TARGET_DIRECTORY,
            base_value=f"{self.environment.root}/sql/core/src/main",
            invocations_per_run=1,
            target_count=1,
        )

        document = [synthetic_record(file_value=self.REPORTED_TAIL)]
        first = self.adapt(document, tool_base=under_core)
        second = self.adapt(document, tool_base=under_sql_core)

        self.assertEqual(len(first.rows), 1)
        self.assertEqual(len(second.rows), 1)
        self.assertEqual(first.rows[0]["path"], f"core/src/main/{self.REPORTED_TAIL}")
        self.assertEqual(second.rows[0]["path"], f"sql/core/src/main/{self.REPORTED_TAIL}")
        self.assertNotEqual(
            first.rows[0]["path"],
            second.rows[0]["path"],
            "the resolved path did not follow the recorded base, which is the signature "
            "of a base hard-coded in the adapter rather than read from the metadata",
        )
        # Everything except the path is identical, which is what makes the difference
        # attributable to the base rather than to anything else about the two calls.
        for field in emit.FIELDS:
            if field == "path":
                continue
            with self.subTest(field=field):
                self.assertEqual(first.rows[0][field], second.rows[0][field])

    def test_one_path_per_invocation_anchors_on_that_directory(self) -> None:
        """Assertion 3, first branch. One target per invocation: paths are relative to it.

        The recorded invocation form is asserted as well as the resolution, because the
        two are one fact: ``invocations_per_run`` equalling the target count is what
        makes this the one-path-per-invocation shape, and the base kind recorded
        alongside it is the consequence rather than an independent choice.
        """
        target = f"{self.environment.root}/resource-managers/yarn/src/main"
        base = self.environment.recorded_base(
            kind=paths.PATH_BASE_KIND_PER_TARGET_DIRECTORY,
            base_value=target,
            invocations_per_run=1,
            target_count=1,
        )
        self.assertEqual(base.invocations_per_run, 1)
        self.assertEqual(base.base_for_relative(), target)

        adapted = self.adapt(
            [synthetic_record(file_value="java/org/apache/spark/network/yarn/Shuffle.java")],
            tool_base=base,
        )
        self.assertEqual(
            adapted.rows[0]["path"],
            "resource-managers/yarn/src/main/java/org/apache/spark/network/yarn/Shuffle.java",
        )
        self.assertTrue(adapted.rows[0]["in_scope"])

    def test_many_paths_in_one_invocation_anchors_on_the_working_directory(self) -> None:
        """Assertion 3, second branch. Several targets in one invocation: the cwd governs.

        ``gitleaks dir`` reports relative to the process working directory when handed
        more than one path, so this branch's base is the recorded working directory. The
        document deliberately records a ``path_base.value`` that is *not* the working
        directory, so the assertion distinguishes the two rather than passing because
        they happened to coincide -- which they do in this provisioning, and which is
        exactly why coinciding cannot be what a test relies on.
        """
        working_directory = f"{self.environment.root}/sql/catalyst/src/main"
        base = self.environment.recorded_base(
            kind=paths.PATH_BASE_KIND_PROCESS_WORKING_DIRECTORY,
            base_value=f"{self.environment.root}/a-directory-the-invocation-did-not-use",
            working_directory=working_directory,
            invocations_per_run=1,
            target_count=18,
        )
        self.assertEqual(base.invocations_per_run, 1)
        self.assertEqual(base.working_directory_path, working_directory)
        self.assertEqual(
            base.base_for_relative(),
            working_directory,
            "the recorded working directory must govern this branch; anchoring on the "
            "path_base value instead would mis-resolve every row of a multi-path "
            "invocation",
        )

        adapted = self.adapt(
            [synthetic_record(file_value="scala/org/apache/spark/sql/catalyst/Rule.scala")],
            tool_base=base,
        )
        self.assertEqual(
            adapted.rows[0]["path"],
            "sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/Rule.scala",
        )
        self.assertTrue(adapted.rows[0]["in_scope"])

    def test_the_two_branches_disagree_on_the_same_record(self) -> None:
        """Assertion 3, stated as the difference between the branches.

        One record, two recorded invocation shapes, two different resolved paths. This
        is what makes the branch distinction load-bearing rather than descriptive: an
        implementation that collapsed the two would return one path for both.
        """
        document = [synthetic_record(file_value=self.REPORTED_TAIL)]
        per_target = self.environment.recorded_base(
            kind=paths.PATH_BASE_KIND_PER_TARGET_DIRECTORY,
            base_value=f"{self.environment.root}/core/src/main",
            invocations_per_run=1,
            target_count=1,
        )
        working_directory = self.environment.recorded_base(
            kind=paths.PATH_BASE_KIND_PROCESS_WORKING_DIRECTORY,
            base_value=None,
            working_directory=f"{self.environment.root}/sql/hive/src/main",
            invocations_per_run=1,
            target_count=18,
        )
        first = self.adapt(document, tool_base=per_target)
        second = self.adapt(document, tool_base=working_directory)
        self.assertEqual(first.rows[0]["path"], f"core/src/main/{self.REPORTED_TAIL}")
        self.assertEqual(second.rows[0]["path"], f"sql/hive/src/main/{self.REPORTED_TAIL}")

    def test_the_recorded_provisioned_base_is_honoured(self) -> None:
        """The run's own recorded document resolves this tool's paths.

        Read from ``harness/artifacts/logs/runner-metadata.json`` rather than restated,
        and asserted for the properties that survive a re-provisioning: an entry exists
        for this tool, its base kind is a member of the declared vocabulary, it supplies
        an explicit base, it names the record field the path comes from, and it records
        the invocation the base was derived from.

        Which of the two invocation shapes was used is deliberately **not** asserted.
        AAP 0.3.2 makes a runner's reach and path base a condition to record rather than
        a defect to repair, so a provisioning that invoked the tool the other way is a
        recorded difference and not a test failure. What must hold either way is that
        the base was taken from the record -- and that resolution against it puts a
        root-relative report exactly where the record says it belongs.
        """
        document = paths.load_runner_metadata(RECORDED_METADATA_PATH)
        base = paths.tool_path_base(document, TOOL)

        self.assertEqual(base.tool, TOOL)
        self.assertIn(base.kind, paths.PATH_BASE_KINDS)
        self.assertTrue(
            base.has_explicit_base,
            "the recorded document must supply a base this resolver may anchor on; "
            "without one every record is rejected rather than defaulted to the root",
        )
        self.assertEqual(base.record_path_field, "File")
        self.assertTrue(
            base.invocation_form,
            "the base is a property of the invocation, so the invocation must be on "
            "the record beside it",
        )
        self.assertIsNotNone(base.evidence)

        # Resolution against the recorded base, expressed against the recorded root.
        recorded_root = paths.metadata_scan_root(document)
        anchor = base.base_for_relative()
        self.assertIsNotNone(anchor)
        reported = "core/src/main/scala/org/apache/spark/storage/DiskStore.scala"
        rows, rejections, _ = gitleaks.adapt(
            [synthetic_record(file_value=reported)],
            tool=TOOL,
            root=recorded_root,
            tool_base=base,
            allowlist=self.environment.globs,
            tally=severity.LiteralTally.with_all_tools(),
        )
        self.assertEqual(rejections, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0]["path"],
            paths.relativize_to_root(paths.posix_join(str(anchor), reported), recorded_root),
        )

    def test_a_recorded_base_naming_another_tool_is_refused(self) -> None:
        """A foreign path base is a caller fault, not a rejection to absorb.

        Resolving one tool's paths against another tool's recorded base would produce a
        wrong path for every row while every row still parsed, so it raises. It matters
        more for this tool than any other: its base is a property of the invocation, and
        the two possible invocation shapes anchor on different directories.
        """
        foreign = self.environment.recorded_base(kind=paths.PATH_BASE_KIND_SCAN_ROOT)
        object.__setattr__(foreign, "tool", "semgrep")
        with self.assertRaises(gitleaks.GitleaksAdapterError):
            self.adapt([synthetic_record()], tool_base=foreign)



# --------------------------------------------------------------------------------- #
# Assertions 4-5: what an emitted path may be, and what in_scope means
# --------------------------------------------------------------------------------- #


class PathShapeAndScopeTests(GitleaksAdapterTestCase):
    """Every emitted path is root-relative, and ``in_scope`` is the allowlist's answer alone.

    Two separate questions that are easy to run together and must not be. Whether a
    path is *expressible* against the root is the path question; whether it falls inside
    the twelve globs is the scope question. A row can be perfectly well-formed and out
    of scope, and such a row is **kept** -- AAP 0.9.3 -- because dropping it would
    silently change what every count means.
    """

    #: Real paths at the pin that fall outside the twelve globs. AAP 0.2.1 names all
    #: three: the pinned tree's two lockfiles and its Gemfile lock.
    OUT_OF_GLOB_PATHS = (
        "dev/package-lock.json",
        "ui-test/package-lock.json",
        "docs/Gemfile.lock",
    )

    #: Real in-scope paths, chosen for the glob forms a naive matcher gets wrong.
    #:
    #: ``paths.py`` owns the matcher and its ``**`` means zero or more directories,
    #: which neither ``fnmatch`` nor ``PurePath.match`` provides. The last three below
    #: are where that bites: two carry a mid-path ``**`` and one is a Python test module
    #: that is in scope because the exclusion is the literal ``src/test`` and it carries
    #: no such segment.
    IN_SCOPE_PATHS = (
        "core/src/main/scala/org/apache/spark/storage/DiskStore.scala",
        "python/pyspark/ml/tests/test_evaluation.py",
        "python/pyspark/pandas/groupby.py",
        "sql/connect/server/src/main/scala/org/apache/spark/sql/connect/Server.scala",
        "sql/connect/shims/src/main/scala/org/apache/spark/sql/connect/Shim.scala",
        "resource-managers/kubernetes/docker/src/main/dockerfiles/spark/Dockerfile",
        "resource-managers/kubernetes/core/volcano/src/main/scala/VolcanoFeatureStep.scala",
    )

    def test_no_emitted_path_is_absolute(self) -> None:
        """Assertion 4. Across every fixture, every path is relative to the root.

        Checked three ways, because this is the invariant AAP 0.8.2 states without
        exception: through ``paths.assert_relative_path``, which is where the rule
        lives; through ``emit.validate_rows``, which is the last gate before either
        deliverable is opened; and directly, since a leading separator is the shape a
        reader would look for.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                self.assertGreater(len(adapted.rows), 0)
                for index, row in enumerate(adapted.rows):
                    with self.subTest(row=index):
                        self.assertIsNotNone(row["path"])
                        self.assertEqual(
                            paths.assert_relative_path(row["path"]), row["path"]
                        )
                        self.assertFalse(row["path"].startswith("/"))
                        self.assertFalse(row["path"].startswith("\\"))
                emit.validate_rows(adapted.rows)

    def test_a_path_outside_the_globs_is_kept_out_of_scope(self) -> None:
        """Assertion 5, first half. Outside the twelve globs: kept, ``in_scope`` false.

        A runner legitimately reaching a file the allowlist does not cover produces a
        row, not a rejection and not a silence. Nothing here judges the finding; the
        allowlist decides one field of it.
        """
        for reported in self.OUT_OF_GLOB_PATHS:
            with self.subTest(path=reported):
                self.environment.materialise(reported)
                adapted = self.adapt([synthetic_record(file_value=reported)])
                self.assertEqual(adapted.rejections, [])
                self.assertEqual(len(adapted.rows), 1)
                self.assertEqual(adapted.rows[0]["path"], reported)
                self.assertIs(adapted.rows[0]["in_scope"], False)
                self.assertEqual(adapted.counters["rows_out_of_scope"], 1)
                self.assertEqual(adapted.counters["rows_in_scope"], 0)
                # Out of scope is not the same question as non-filesystem: the path
                # names a real file inside the root and simply falls outside the globs.
                self.assertEqual(adapted.counters["non_filesystem_paths"], 0)
                self.assertEqual(adapted.counters["path_kind_tree_file"], 1)

    def test_a_path_inside_the_globs_is_in_scope(self) -> None:
        """Assertion 5, second half, over the glob forms a naive matcher gets wrong."""
        for reported in self.IN_SCOPE_PATHS:
            with self.subTest(path=reported):
                self.environment.materialise(reported)
                adapted = self.adapt([synthetic_record(file_value=reported)])
                self.assertEqual(adapted.rejections, [])
                self.assertEqual(len(adapted.rows), 1)
                self.assertEqual(adapted.rows[0]["path"], reported)
                self.assertIs(
                    adapted.rows[0]["in_scope"],
                    True,
                    "a matcher whose ** cannot span zero or more directories drops "
                    "whole modules, and a silently dropped module looks exactly like a "
                    "module with nothing to report",
                )
                self.assertEqual(adapted.counters["rows_in_scope"], 1)

    def test_a_src_test_path_is_out_of_scope_and_kept(self) -> None:
        """The exclusion is the literal ``src/test``, and it overrides a glob match.

        ``core/src/test/...`` matches ``core/src/main/**`` on no reading, but the
        interesting case is that the exclusion is applied by ``paths.py`` rather than
        expressed in the allowlist file -- which is why this module's allowlist carries
        no exclusion line. The row is kept, as every out-of-scope row is.
        """
        reported = "core/src/test/scala/org/apache/spark/storage/DiskStoreSuite.scala"
        adapted = self.adapt([synthetic_record(file_value=reported)])
        self.assertEqual(len(adapted.rows), 1)
        self.assertEqual(adapted.rows[0]["path"], reported)
        self.assertIs(adapted.rows[0]["in_scope"], False)
        self.assertTrue(paths.contains_src_test(reported))

    def test_an_archive_member_keeps_its_serialization_and_is_counted(self) -> None:
        """A non-filesystem coordinate is expressed against the root and never dropped.

        The serialization is ``<container-relative-to-root>!<member-path>`` with ``!``
        the single separator (AAP 0.5.4). It takes ``in_scope`` false whatever its
        segments look like -- which matters precisely because this container sits under
        a scope root, so a matcher applied to the segments alone would call it in scope.
        """
        reported = "core/src/main/resources/vendor.jar!org/apache/vendored/Client.class"
        adapted = self.adapt([synthetic_record(file_value=reported)])
        self.assertEqual(adapted.rejections, [])
        self.assertEqual(len(adapted.rows), 1)
        self.assertEqual(adapted.rows[0]["path"], reported)
        self.assertIn(paths.ARCHIVE_SEPARATOR, adapted.rows[0]["path"])
        self.assertIs(adapted.rows[0]["in_scope"], False)
        self.assertEqual(adapted.counters["non_filesystem_paths"], 1)
        self.assertEqual(adapted.counters["path_kind_archive_member"], 1)

    def test_a_location_outside_the_root_keeps_its_parent_segments(self) -> None:
        """An absolute report outside the root relativizes with ``..`` preserved.

        The SARIF 2.1.0 errata forbid normalizing ``..`` out of a path, and the same
        serialization is used for every tool so that one row's ``path`` means the same
        thing whichever tool produced it. The row is kept and counted; only evidence
        about the *runner* establishes a wrong scan root (AAP 0.8.3), and a single
        coordinate outside the tree is not that evidence.
        """
        outside = str(self.environment.directory / "outside-the-root" / "config.yaml")
        adapted = self.adapt([synthetic_record(file_value=outside)])
        self.assertEqual(adapted.rejections, [])
        self.assertEqual(len(adapted.rows), 1)
        emitted = adapted.rows[0]["path"]
        self.assertTrue(emitted.startswith("../"))
        self.assertEqual(paths.assert_relative_path(emitted), emitted)
        self.assertIs(adapted.rows[0]["in_scope"], False)
        self.assertEqual(adapted.counters["non_filesystem_paths"], 1)
        self.assertEqual(adapted.counters["path_kind_outside_root"], 1)

    def test_a_second_reported_location_is_counted_and_never_substituted(self) -> None:
        """A non-empty ``SymlinkFile`` makes the record name two locations.

        AAP 0.5.4's first representation decision: the row takes the first location, the
        record still counts once, and the number carrying more than one is reported per
        tool. The row's location is the field the metadata names, and the counterpart is
        never used to fill in an absent one -- substituting it would silently change
        which location the row names.
        """
        reported = "core/src/main/scala/org/apache/spark/storage/DiskStore.scala"
        counterpart = "core/src/main/scala/org/apache/spark/storage/DiskStoreLink.scala"
        adapted = self.adapt(
            [synthetic_record(file_value=reported, symlink_file=counterpart)]
        )
        self.assertEqual(len(adapted.rows), 1)
        self.assertEqual(adapted.rows[0]["path"], reported)
        self.assertEqual(adapted.counters["multi_location_records"], 1)

        # With no File to take, the counterpart is not promoted into its place: the
        # record is rejected and counted instead.
        absent = self.adapt(
            [synthetic_record(file_value="", symlink_file=counterpart)]
        )
        self.assertEqual(absent.rows, [])
        self.assertEqual(len(absent.rejections), 1)
        self.assertEqual(absent.rejections[0].reject_class, paths.REJECT_ABSENT_PATH)
        self.assertEqual(absent.counters["multi_location_records"], 1)

    def test_path_resolution_does_not_depend_on_the_file_existing(self) -> None:
        """A resolved path is arithmetic over the reported value, not a filesystem probe.

        Asserted because it is what makes every other test in this module hermetic: the
        same reported value resolves identically whether the file is present or absent,
        so no expectation here is quietly a statement about the machine it ran on. AAP
        0.6.1 has ``run-record.md`` report the rows whose path names something that is
        not a file on disk, and that count is taken by the caller against the root --
        never by the adapter deciding a path is invalid because nothing is there.
        """
        reported = "sql/core/src/main/scala/org/apache/spark/sql/execution/Absent.scala"
        before = self.adapt([synthetic_record(file_value=reported)])
        self.environment.materialise(reported)
        after = self.adapt([synthetic_record(file_value=reported)])
        self.assertEqual(before.rows, after.rows)
        self.assertEqual(before.counters, after.counters)



# --------------------------------------------------------------------------------- #
# Assertions 6-9: the no-severity-vocabulary reference case
# --------------------------------------------------------------------------------- #


class NoSeverityVocabularyTests(GitleaksAdapterTestCase):
    """This tool defines no severity vocabulary, which makes it the reference case.

    AAP 0.5.4's native-severity table puts this tool under *"No vocabulary at all"*:
    ``severity_native`` absent, ``severity_norm`` ``Info``, *"the absence stated rather
    than a level assumed."* The distinction between stated and assumed is the whole
    point of this class. A test checking only the two field values would pass equally
    against an adapter that hard-coded ``Info`` and against one that took the documented
    no-vocabulary route, so the **basis** is asserted beside them -- that is what makes
    the statement checkable rather than true by coincidence.
    """

    def test_severity_native_is_absent_on_every_row(self) -> None:
        """Assertion 6. ``None`` on every row of every fixture, without exception."""
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                self.assertGreater(len(adapted.rows), 0)
                for index, row in enumerate(adapted.rows):
                    with self.subTest(row=index):
                        self.assertIsNone(row["severity_native"])
                self.assertEqual(
                    adapted.counters["severity_absent"], len(adapted.rows)
                )

    def test_severity_norm_is_info_on_every_row(self) -> None:
        """Assertion 7. ``Info`` on every row, and never absent.

        ``severity_norm`` is one of the two fields absence is never permitted for, so
        the vocabulary membership is asserted as well as the value: a band outside
        ``severity.SEVERITY_NORM`` would be refused downstream, and asserting only
        equality with ``Info`` would not say that.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                for index, row in enumerate(adapted.rows):
                    with self.subTest(row=index):
                        self.assertIsNotNone(row["severity_norm"])
                        self.assertIn(row["severity_norm"], severity.SEVERITY_NORM)
                        self.assertEqual(row["severity_norm"], "Info")

    def test_the_basis_is_the_no_vocabulary_route(self) -> None:
        """Assertion 8. Every row took ``BASIS_NO_VOCABULARY``, and no other basis.

        Two independent readings of the same fact, which is why both are asserted: the
        basis the tally recorded per row, and the adapter's own
        ``severity_basis_no_vocabulary`` counter. Every other ``severity_basis_*``
        counter is required to be zero -- a row that reached a mapped vocabulary would
        show up there, and a policy fixed before any output was observed says none can.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                entries = adapted.tally.entries(TOOL)
                self.assertEqual(len(entries), 1)
                self.assertEqual(entries[0].basis, severity.BASIS_NO_VOCABULARY)
                self.assertEqual(entries[0].rows, len(adapted.rows))

                counter = f"severity_basis_{severity.BASIS_NO_VOCABULARY}"
                self.assertEqual(adapted.counters[counter], len(adapted.rows))
                for basis in severity.BASIS_VALUES:
                    if basis == severity.BASIS_NO_VOCABULARY:
                        continue
                    with self.subTest(basis=basis):
                        self.assertEqual(adapted.counters[f"severity_basis_{basis}"], 0)

    def test_no_native_literal_is_recorded_for_this_tool(self) -> None:
        """Assertion 9. The tally records the absence, not an empty string and not ``INFO``.

        ``oss-scan-results/severity-map.md`` has to be able to name this tool as one
        that defines no severity vocabulary, and it renders from this tally. An entry
        carrying a fabricated literal would put a label into that document which the
        tool never emitted, and an empty-string literal would render as a blank cell
        indistinguishable from a formatting fault.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                for entry in adapted.tally.entries(TOOL):
                    with self.subTest(basis=entry.basis):
                        self.assertIsNone(
                            entry.severity_native,
                            "the tally must record no native literal for this tool, "
                            "neither an empty string nor a fabricated band name",
                        )
                        self.assertFalse(entry.unmapped)
                self.assertEqual(adapted.tally.unmapped_by_tool()[TOOL], ())
                self.assertEqual(adapted.tally.row_count(TOOL), len(adapted.rows))
                bands = adapted.tally.band_counts(TOOL)
                self.assertEqual(bands["Info"], len(adapted.rows))
                for band in severity.SEVERITY_NORM:
                    if band == "Info":
                        continue
                    with self.subTest(band=band):
                        self.assertEqual(bands[band], 0)

    def test_the_absence_is_structural_in_the_severity_policy(self) -> None:
        """The no-vocabulary result cannot carry a native literal at all.

        Asserted against ``severity.py`` directly, because this is what makes assertions
        6 and 8 more than a convention the adapter has to remember: the result object
        refuses the combination on construction, so no code path can produce a row whose
        basis says "no vocabulary" while a literal sits in the field.
        """
        result = severity.SeverityResult.absent()
        self.assertIsNone(result.severity_native)
        self.assertEqual(result.severity_norm, "Info")
        self.assertEqual(result.basis, severity.BASIS_NO_VOCABULARY)
        self.assertIsNone(result.selected_entry)
        self.assertIsNone(result.unmapped_literal)

        with self.assertRaises(severity.SeverityPolicyError):
            severity.SeverityResult(
                severity_native="INFO",
                severity_norm="Info",
                basis=severity.BASIS_NO_VOCABULARY,
            )

    def test_a_document_producing_no_row_records_no_literal(self) -> None:
        """An artifact contributing no row contributes no tally entry either.

        A tool with zero rows still reaches ``severity-map.md`` -- through the tally's
        seeded identifiers, not through an invented entry -- so the seeded tool is
        present with no entries rather than absent or carrying a zero-row literal.
        """
        adapted = self.adapt([])
        self.assertEqual(adapted.rows, [])
        self.assertEqual(adapted.tally.entries(TOOL), ())
        self.assertEqual(adapted.tally.row_count(TOOL), 0)
        self.assertIn(TOOL, adapted.tally.tools())

    def test_a_rejected_record_contributes_no_severity_literal(self) -> None:
        """Only an emitted row feeds the tally.

        A rejected record contributes no row, so counting one would report an entry
        against rows the dataset does not contain -- and the row count behind the
        statement in ``severity-map.md`` would exceed the rows in ``findings.json``.
        """
        for stem in NEGATIVE_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                self.assertGreater(len(adapted.rejections), 0)
                self.assertEqual(adapted.tally.row_count(TOOL), len(adapted.rows))
                self.assertLess(adapted.tally.row_count(TOOL), adapted.record_count)



# --------------------------------------------------------------------------------- #
# Assertions 10-13: no secret value reaches any field
# --------------------------------------------------------------------------------- #


class NoSecretValueTests(GitleaksAdapterTestCase):
    """The central prohibition of this file, asserted structurally rather than sampled.

    AAP 0.5.4: *"Gitleaks runs with redaction so a matched secret's value never enters
    an artifact, and no adapter carries a secret value into any field."* The runner does
    pass a redaction flag -- but that is upstream protection in a runner this run may
    not edit, and an invariant that depends on a flag in an unmodifiable file is not an
    invariant. What is asserted here is that the adapter **cannot** carry a value even
    if one arrived.

    Structural means what it says: every one of the twelve fields of every row, against
    every sensitive value present anywhere in the artifact, iterating ``emit.FIELDS`` so
    a violation names its field. A per-field spot check would leave eleven fields
    unexamined, and the eleventh is where an unused field becomes a smuggling route.

    No sensitive value is written into this file. The values swept for are read out of
    the fixtures at run time; the only literals here are the synthetic sentinels, which
    exist so the sweep is shown to catch something.
    """

    def test_message_is_the_rule_description(self) -> None:
        """Assertion 10. ``message`` is ``Description`` and is neither of the other two.

        The single easiest field in this artifact to get wrong. ``Description`` is the
        *rule's* description; ``Message`` is a git commit message; and ``Secret`` and
        ``Match`` are the captured value. Row order is fixture order and every record in
        the positive fixture becomes a row, so rows and records correspond one to one
        here -- which is what lets the comparison be per record rather than per set.
        """
        document = load_fixture(POSITIVE_FIXTURE)
        adapted = self.adapt(document)
        self.assertEqual(len(adapted.rows), len(document))

        for index, (row, record) in enumerate(zip(adapted.rows, document)):
            with self.subTest(row=index):
                self.assertEqual(row["message"], record["Description"].strip())
                for key in ("Secret", "Match"):
                    value = record.get(key)
                    if not isinstance(value, str) or not value.strip():
                        continue
                    with self.subTest(key=key):
                        self.assertNotEqual(row["message"], value)
                        self.assertNotIn(value, row["message"])

    def test_the_structural_sweep_finds_nothing_in_any_row(self) -> None:
        """Assertion 11. No field of any row carries any sensitive value.

        The sweep runs over every fixture, and every value from every record is searched
        for in every row -- not only the record a given row came from, because rows and
        records stop corresponding as soon as one record is rejected, and a sweep whose
        mapping was wrong would pass for the wrong reason.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                document = load_fixture(stem)
                values = sensitive_values(document)
                self.assertGreater(
                    len(values),
                    0,
                    "the sweep found no sensitive value to search for, so it proved "
                    "nothing about this fixture",
                )
                adapted = self.adapt(document)
                self.assertEqual(sweep_rows_for_values(adapted.rows, values), [])

    def test_the_structural_sweep_finds_nothing_in_any_rejection(self) -> None:
        """The rejection channel carries no sensitive value either.

        AAP 0.5.4 requires a parser reason retained verbatim, and a verbatim reason that
        interpolated the record's own text would turn that channel into a leak channel.
        The adapter renders only values whose type makes them safe -- ``None``, a bool,
        an int, a float -- and names the type alone for everything else, a string above
        all.
        """
        for stem in NEGATIVE_FIXTURES:
            with self.subTest(fixture=stem):
                document = load_fixture(stem)
                values = sensitive_values(document)
                adapted = self.adapt(document)
                self.assertGreater(len(adapted.rejections), 0)
                self.assertEqual(
                    sweep_rejections_for_values(adapted.rejections, values), []
                )

    def test_a_planted_value_is_not_carried_into_any_field(self) -> None:
        """A record whose sensitive keys are distinctive sentinels still yields a clean row.

        The positive fixtures were produced by a redacting runner, so their sensitive
        keys carry no captured value to begin with. This record does carry a distinctive
        one, so the sweep over it is a real search rather than a search for something
        that was never there.
        """
        record = synthetic_record(
            file_value="core/src/main/scala/org/apache/spark/storage/DiskStore.scala"
        )
        adapted = self.adapt([record])
        self.assertEqual(len(adapted.rows), 1)

        planted = sensitive_values([record])
        self.assertEqual(len(planted), len(SENSITIVE_RECORD_KEYS))
        self.assertEqual(sweep_rows_for_values(adapted.rows, planted), [])
        for field in emit.FIELDS:
            value = adapted.rows[0][field]
            if not isinstance(value, str):
                continue
            with self.subTest(field=field):
                self.assertNotIn(SYNTHETIC_SENTINEL_PREFIX, value)

    def test_the_sweep_detects_a_planted_value(self) -> None:
        """The sweep can fail, which is what makes its passing meaningful.

        A sweep that reported nothing because it searched nothing, compared in the wrong
        direction, or skipped the field an implementation actually used would pass every
        other assertion in this class. So the helper is run once over a row deliberately
        carrying a sentinel and required to report it, naming the field -- and run again
        over the same row with a value shorter than the minimum to confirm that a
        degenerate value is skipped rather than matching everything.
        """
        leaked = {field: None for field in emit.FIELDS}
        leaked["message"] = f"prefix {SYNTHETIC_MATCH_SENTINEL} suffix"
        violations = sweep_rows_for_values([leaked], (SYNTHETIC_MATCH_SENTINEL,))
        self.assertEqual(len(violations), 1)
        self.assertIn("message", violations[0])

        # A value below the minimum length is dropped by sensitive_values rather than
        # matching every field: the empty string is a substring of everything.
        degenerate = synthetic_record()
        degenerate["Secret"] = ""
        degenerate["Match"] = "  "
        degenerate["Fingerprint"] = SYNTHETIC_FINGERPRINT_SENTINEL
        self.assertEqual(
            sensitive_values([degenerate]), (SYNTHETIC_FINGERPRINT_SENTINEL,)
        )

    def test_the_rejection_sweep_detects_a_planted_value(self) -> None:
        """The rejection sweep can fail too, in both channels it searches.

        Once through the detail and once through a value in ``record_identity``, because
        an implementation that scrubbed one and not the other would otherwise look
        clean.
        """
        in_detail = paths.make_rejection(
            paths.REJECT_MALFORMED_RECORD,
            TOOL,
            f"the record's value was {SYNTHETIC_SECRET_SENTINEL}",
            record_index=0,
        )
        self.assertEqual(
            len(sweep_rejections_for_values([in_detail], (SYNTHETIC_SECRET_SENTINEL,))),
            1,
        )

        in_identity = paths.make_rejection(
            paths.REJECT_MALFORMED_RECORD,
            TOOL,
            "a detail that carries nothing from the record",
            record_index=0,
            reported_path=SYNTHETIC_MATCH_SENTINEL,
        )
        self.assertEqual(
            len(sweep_rejections_for_values([in_identity], (SYNTHETIC_MATCH_SENTINEL,))),
            1,
        )

    def test_the_committed_fixtures_carry_no_live_looking_credential(self) -> None:
        """Assertion 12. Every committed fixture's bytes are clean.

        These files are committed to git and this tree is not ignored -- the root
        ``.gitignore`` matches an ``artifacts/`` directory and nothing under
        ``oss-scan-results/`` -- so the invariant has to hold of the fixtures as written
        and not only of the dataset the adapter produces.

        Each marker is an issuer-specific prefix rather than a word, which is what lets
        the fixtures legitimately carry rule identifiers naming a credential kind: a rule
        identifier is the name of a detector, and none of it matches a token prefix.

        This module's own source is deliberately **not** scanned for these markers, and
        the reason is not an exemption: the marker list is written in this file, so any
        scan of it would match every marker against the constant that defines it and
        fail unconditionally. The corresponding guarantee about this file is the one it
        can actually hold -- that it quotes no sensitive value from any fixture -- and it
        is asserted in
        :meth:`test_this_file_quotes_no_sensitive_value_from_any_fixture`.

        The membership test is written with ``assertFalse`` rather than ``assertNotIn``
        so that a failure names the marker and the file instead of printing the file.
        """
        for stem in ALL_FIXTURES:
            text = fixture_path(stem).read_text(encoding="utf-8")
            for marker in LIVE_CREDENTIAL_MARKERS:
                with self.subTest(fixture=stem, marker=marker):
                    self.assertFalse(
                        marker in text,
                        f"{stem}.json carries the issuer prefix {marker!r}, which is "
                        "the shape of a live credential; a fixture is committed to git "
                        "and every placeholder in one must be self-evidently synthetic",
                    )

    def test_this_file_quotes_no_sensitive_value_from_any_fixture(self) -> None:
        """No value from a sensitive key appears in this module's own source.

        The discipline the expectation files hold, applied to the test that reads them.
        Every value the sweep searches for is loaded from a fixture at run time rather
        than written here, and this is what keeps that true as the file is edited: a
        maintainer who pasted a value into a docstring or an assertion message to make a
        failure clearer would be committing it to git, and this fails first.

        Two drafting consequences follow and both are observed above: no sentence here
        quotes the placeholder a redacting runner substitutes for a captured value, and
        nowhere is a path, a rule identifier and a line number joined into one
        colon-separated string, since that is the shape of this artifact's fingerprint
        values.
        """
        source = Path(__file__).read_text(encoding="utf-8")
        considered = 0
        for stem in ALL_FIXTURES:
            for value in sensitive_values(load_fixture(stem)):
                considered += 1
                with self.subTest(fixture=stem):
                    self.assertFalse(
                        value in source,
                        f"this file quotes a value from a "
                        f"{'/'.join(SENSITIVE_RECORD_KEYS)} key of {stem}.json; such a "
                        "value is read from the fixture at run time and never written "
                        "into a committed source file",
                    )
        self.assertGreater(
            considered, 0, "no value was checked, so this assertion proved nothing"
        )

    def test_every_synthetic_placeholder_is_self_evidently_synthetic(self) -> None:
        """The one class of literal this file does write announces itself as fake.

        A placeholder that could be mistaken for a live value would defeat the point of
        having one. Each sentinel says so in its own text, and none matches a marker
        above.
        """
        sentinels = (
            SYNTHETIC_MATCH_SENTINEL,
            SYNTHETIC_SECRET_SENTINEL,
            SYNTHETIC_FINGERPRINT_SENTINEL,
        )
        for sentinel in sentinels:
            with self.subTest(sentinel=sentinel):
                self.assertIn("NOT-A-CREDENTIAL", sentinel)
                self.assertIn("SYNTHETIC", sentinel)
                self.assertGreaterEqual(len(sentinel), MINIMUM_SWEEP_VALUE_LENGTH)
                for marker in LIVE_CREDENTIAL_MARKERS:
                    self.assertFalse(
                        marker in sentinel,
                        f"the sentinel carries the issuer prefix {marker!r}",
                    )
        self.assertEqual(len(set(sentinels)), len(sentinels))

    def test_cwe_cve_and_package_coordinate_are_absent_on_every_row(self) -> None:
        """Assertion 13. The three unused optional fields are ``None`` everywhere.

        An unused field is the obvious place for a value to be smuggled, and an absent
        coordinate here is not a rejection condition: AAP 0.5.4 makes an unformable
        package coordinate a rejection for a dependency-oriented record, and a secret
        finding names a code location rather than a package.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                for index, row in enumerate(adapted.rows):
                    with self.subTest(row=index):
                        self.assertIsNone(row["cwe"])
                        self.assertIsNone(row["cve"])
                        self.assertIsNone(row["package_coordinate"])

    def test_a_row_is_built_from_four_named_keys_and_constants(self) -> None:
        """Every emitted value traces to a declared source key or a fixed constant.

        The invariant's other half. The sweep proves no sensitive value *arrived*; this
        proves nothing beyond the four declared keys is read in the first place, which
        is what makes the guarantee structural rather than the result of a filter that
        could be relaxed. Asserted against ``gitleaks.SOURCE_FIELDS`` rather than a list
        retyped here.
        """
        document = load_fixture(POSITIVE_FIXTURE)
        adapted = self.adapt(document)
        for index, (row, record) in enumerate(zip(adapted.rows, document)):
            with self.subTest(row=index):
                self.assertEqual(row["rule_id"], record["RuleID"].strip())
                self.assertEqual(row["message"], record["Description"].strip())
                self.assertEqual(row["start_line"], record["StartLine"])
                self.assertEqual(row["path"], record["File"])
                self.assertEqual(row["tool"], TOOL)
                self.assertEqual(row["scanner_class"], EXPECTED_SCANNER_CLASS)
                self.assertIsNone(row["severity_native"])
                self.assertEqual(row["severity_norm"], "Info")
                self.assertIsInstance(row["in_scope"], bool)



# --------------------------------------------------------------------------------- #
# Assertions 14-16: the positive mapping, field by field
# --------------------------------------------------------------------------------- #


class PositiveMappingTests(GitleaksAdapterTestCase):
    """The captured fixture against its hand-verified expectation, field by field.

    The expectation was derived by reading the fixture and the authored contracts, never
    by running the adapter and recording what it printed -- its own ``description`` says
    so. That is what makes this a comparison rather than a snapshot: where the two
    disagree, the disagreement is the finding, and neither file is adjusted to make the
    other pass.

    Every comparison iterates ``emit.FIELDS``, so a failure names the field rather than
    printing two row dicts and leaving a reader to spot the difference.
    """

    def test_row_count_matches_exactly(self) -> None:
        """Assertion 14, first half. As many rows as the expectation fixes, and no more."""
        expected = load_expected(POSITIVE_FIXTURE)
        adapted = self.adapt_fixture(POSITIVE_FIXTURE)
        self.assertEqual(len(adapted.rows), len(expected["rows"]))
        self.assertEqual(len(adapted.rows), expected["counts"]["rows"])
        self.assertEqual(adapted.rejections, [])
        self.assertEqual(expected["counts"]["rejections"], 0)

    def test_every_field_of_every_row_matches_the_expectation(self) -> None:
        """Assertion 14, second half. All twelve fields of every row, in order.

        The comparison a snapshot test cannot make: each field is compared on its own,
        under a subtest naming the row index and the field, so a single wrong value is
        reported as that value rather than as an unequal pair of dicts.
        """
        expected = load_expected(POSITIVE_FIXTURE)
        adapted = self.adapt_fixture(POSITIVE_FIXTURE)
        self.assertEqual(len(adapted.rows), len(expected["rows"]))
        for index, (row, want) in enumerate(zip(adapted.rows, expected["rows"])):
            for field in emit.FIELDS:
                with self.subTest(row=index, field=field):
                    self.assertEqual(row[field], want[field])

    def test_every_row_carries_exactly_the_twelve_fields_in_order(self) -> None:
        """No thirteenth field, no missing field, and the order the emitter iterates."""
        adapted = self.adapt_fixture(POSITIVE_FIXTURE)
        self.assertRowsHaveTheTwelveFields(adapted.rows)

    def test_tool_and_scanner_class_on_every_row(self) -> None:
        """Assertion 15. Both are constants, and neither is derived from record content.

        Asserted over the negative fixtures too: a record's content decides whether it
        becomes a row, and must never decide what class the row carries.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                self.assertGreater(len(adapted.rows), 0)
                for index, row in enumerate(adapted.rows):
                    with self.subTest(row=index):
                        self.assertEqual(row["tool"], TOOL)
                        self.assertEqual(row["scanner_class"], EXPECTED_SCANNER_CLASS)

    def test_start_line_is_an_integer_taken_from_the_record(self) -> None:
        """Assertion 16. An ``int`` from ``StartLine``, never a coerced string.

        ``bool`` is excluded explicitly. Python's numeric tower makes
        ``isinstance(True, int)`` true, so a type check alone would accept ``True`` as
        line one -- and a line number that came from a boolean is not a line number.
        """
        document = load_fixture(POSITIVE_FIXTURE)
        adapted = self.adapt(document)
        for index, (row, record) in enumerate(zip(adapted.rows, document)):
            with self.subTest(row=index):
                self.assertIsInstance(row["start_line"], int)
                self.assertNotIsInstance(row["start_line"], bool)
                self.assertEqual(row["start_line"], record["StartLine"])
                self.assertGreaterEqual(row["start_line"], 1)

    def test_a_permitted_absent_start_line_becomes_a_row(self) -> None:
        """An absent ``StartLine`` is a permitted absence, not a rejection.

        Absence is permitted for this field (AAP 0.8.2), so both an absent key and an
        explicit ``null`` yield a row with ``start_line`` null and increment the
        counter that makes the number visible. Only a value that is *present and
        unusable* is the rejection condition, which
        :class:`RejectionTests` covers from its own fixture.
        """
        without_key = synthetic_record()
        del without_key["StartLine"]
        explicit_null = synthetic_record(start_line=None)

        adapted = self.adapt([without_key, explicit_null])
        self.assertEqual(adapted.rejections, [])
        self.assertEqual(len(adapted.rows), 2)
        for index, row in enumerate(adapted.rows):
            with self.subTest(row=index):
                self.assertIsNone(row["start_line"])
        self.assertEqual(adapted.counters["start_line_absent"], 2)

    def test_counters_match_the_expectation(self) -> None:
        """Every counter, over every fixture, against the recorded value.

        The counters are how ``run-record.md`` and ``tool-status.md`` state numbers the
        row fields cannot show -- a permitted absence, a second reported location, an
        out-of-scope row. Compared key by key so a failure names the counter, and the key
        set is compared as well: a counter the adapter stopped publishing would otherwise
        vanish silently from every document that reports it.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                expected = load_expected(stem)
                adapted = self.adapt_fixture(stem)
                self.assertEqual(
                    set(adapted.counters), set(expected["counters"])
                )
                for key in gitleaks.COUNTER_KEYS:
                    with self.subTest(counter=key):
                        self.assertEqual(adapted.counters[key], expected["counters"][key])

    def test_rows_are_in_fixture_order(self) -> None:
        """Array order is preserved, because both output files use it.

        No sort, no grouping by tool, no ranking and no deduplication (AAP 0.3.2).
        ``emit.py`` compares the two written files ordered row by row, so a reordering
        here would make an expectation in any other order assert something the pipeline
        does not promise.
        """
        document = load_fixture(POSITIVE_FIXTURE)
        adapted = self.adapt(document)
        self.assertEqual(
            [row["path"] for row in adapted.rows],
            [record["File"] for record in document],
        )

    def test_two_identical_records_produce_two_rows(self) -> None:
        """Nothing is deduplicated, not even two byte-identical elements.

        AAP 0.3.2 forbids deduplication outright. Two identical elements are two records
        and two rows, and the count unit is the array element rather than the distinct
        finding -- so collapsing them would break the reconciliation identity as well as
        the prohibition.
        """
        record = synthetic_record()
        adapted = self.adapt([dict(record), dict(record)])
        self.assertEqual(len(adapted.rows), 2)
        self.assertEqual(adapted.rows[0], adapted.rows[1])
        self.assertEqual(adapted.outcome_count, adapted.record_count)

    def test_rows_pass_the_emitter_and_its_measured_summary(self) -> None:
        """The rows are emittable, and the emitter's own summary agrees with them.

        ``emit.validate_rows`` enforces the schema by raising, which proves it held only
        by the absence of an exception. ``emit.validation_summary`` turns the same facts
        into numbers, so the twelve-field count, the resolved non-absent ``path`` and the
        non-absent ``severity_norm`` are asserted as measurements rather than inferred
        from nothing having gone wrong.
        """
        adapted = self.adapt_fixture(POSITIVE_FIXTURE)
        validated = emit.validate_rows(adapted.rows)
        self.assertEqual(len(validated), len(adapted.rows))
        summary = emit.validation_summary(adapted.rows)
        self.assertIsInstance(summary, dict)
        self.assertTrue(summary)


# --------------------------------------------------------------------------------- #
# The negative fixtures, and every rejection condition this adapter can produce
# --------------------------------------------------------------------------------- #


class RejectionTests(GitleaksAdapterTestCase):
    """A defective record is rejected under a named class, counted, and never coerced.

    AAP 0.5.4: *"Where a record cannot be attributed with certainty, it is rejected and
    the rejection recorded as a class with its count -- never guessed into a field."*
    Every expectation below is read from the corresponding ``expected/*.rows.json``,
    including the class name, because two of the fixtures do not produce what their
    filenames suggest:

    * ``reject-gitleaks-unresolvable-path.json`` produces ``absent_path``. Its
      expectation carries a ``reject_class_divergence`` block explaining why, and a test
      that inferred the class from the filename would fail against a correct adapter.
    * ``reject-gitleaks-malformed-record.json`` produces **two** rejections and
      ``reject-gitleaks-missing-message.json`` produces **four**. The count comes from
      the expectation, not from an assumption that a negative fixture holds one
      defective record.

    Every class is additionally asserted to be a real member of
    ``paths.REJECT_CLASSES`` and one the adapter declares it can produce, so a class
    could not be renamed on both sides at once and still pass.
    """

    def test_each_negative_fixture_produces_its_recorded_rejections(self) -> None:
        """The whole negative contract, per fixture, driven by its expectation.

        Rows, rejection count, each class by name, each detail verbatim and each record
        identity -- and the parsable records around the defective ones still becoming
        rows, which is what makes this a partial parse rather than a failed one.
        """
        for stem in NEGATIVE_FIXTURES:
            with self.subTest(fixture=stem):
                expected = load_expected(stem)
                adapted = self.adapt_fixture(stem)

                self.assertEqual(len(adapted.rows), expected["counts"]["rows"])
                self.assertEqual(
                    len(adapted.rejections), expected["counts"]["rejections"]
                )
                for index, (row, want) in enumerate(
                    zip(adapted.rows, expected["rows"])
                ):
                    for field in emit.FIELDS:
                        with self.subTest(row=index, field=field):
                            self.assertEqual(row[field], want[field])

                for index, (rejection, want) in enumerate(
                    zip(adapted.rejections, expected["rejections"])
                ):
                    with self.subTest(rejection=index):
                        self.assertEqual(rejection.reject_class, want["reject_class"])
                        self.assertIn(rejection.reject_class, paths.REJECT_CLASSES)
                        self.assertIn(
                            rejection.reject_class, gitleaks.REJECT_CLASSES_PRODUCED
                        )
                        self.assertEqual(rejection.tool, TOOL)
                        self.assertEqual(rejection.detail, want["expected_detail"])
                        self.assertEqual(
                            dict(rejection.record_identity),
                            want["expected_record_identity"],
                        )
                        self.assertEqual(
                            sorted(rejection.record_identity),
                            sorted(want["expected_record_identity_keys"]),
                        )

    def test_the_expectations_record_the_classes_the_fixtures_can_reach(self) -> None:
        """The expectation files' own class names, pinned against the ``paths`` constants.

        Every other assertion in this class takes the class from the expectation and
        compares it with what the adapter produced, which is two independent sources
        cross-checking each other. This closes the remaining gap: it names the five
        classes the committed fixtures reach using the constants themselves, so a class
        renamed on *both* sides at once -- in the adapter and in the expectation -- would
        still fail here.

        ``absent_path`` is in this set and ``unresolvable_path`` is not, which is the
        divergence between the fixture filenames and the classes they actually reach.
        """
        recorded: set[str] = set()
        for stem in NEGATIVE_FIXTURES:
            for rejection in load_expected(stem)["rejections"]:
                recorded.add(rejection["reject_class"])
        self.assertEqual(
            recorded,
            {
                paths.REJECT_MALFORMED_RECORD,
                paths.REJECT_MISSING_RULE_ID,
                paths.REJECT_MISSING_MESSAGE,
                paths.REJECT_NON_INTEGER_START_LINE,
                paths.REJECT_ABSENT_PATH,
            },
        )
        self.assertNotIn(paths.REJECT_UNRESOLVABLE_PATH, recorded)

    def test_the_rejections_by_class_totals_match(self) -> None:
        """Grouped by class, the counts are the expectation's.

        A test that only counted rejections could not tell one condition from another,
        so the grouping is what carries the assertion: a fixture whose records were all
        rejected under the wrong class would pass a bare count and fail here.
        """
        for stem in NEGATIVE_FIXTURES:
            with self.subTest(fixture=stem):
                expected = load_expected(stem)
                adapted = self.adapt_fixture(stem)
                observed: dict[str, int] = {}
                for rejection in adapted.rejections:
                    observed[rejection.reject_class] = (
                        observed.get(rejection.reject_class, 0) + 1
                    )
                self.assertEqual(observed, expected["counts"]["rejections_by_class"])

    def test_the_reconciliation_identity_holds_over_every_fixture(self) -> None:
        """``records == rows + rejections``, for every artifact including the clean one.

        The count unit is the array element, so the left-hand side is a property of the
        document rather than of the adapter. Every element yields exactly one outcome --
        one row or one rejection, never both and never neither -- so a record silently
        dropped would show up here and nowhere else.
        """
        for stem in ALL_FIXTURES:
            with self.subTest(fixture=stem):
                expected = load_expected(stem)
                adapted = self.adapt_fixture(stem)
                self.assertEqual(adapted.outcome_count, adapted.record_count)
                self.assertEqual(
                    adapted.record_count, expected["counts"]["raw_finding_records"]
                )

    def test_a_rejected_record_contributes_no_row_and_no_counter(self) -> None:
        """A rejection is not a row with fields missing, and it moves no row counter.

        The row counters decompose the rows, so their sum must be the row count. A
        rejection that had incremented one would put a number in ``run-record.md``
        describing rows the dataset does not contain.
        """
        for stem in NEGATIVE_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                self.assertEqual(
                    adapted.counters["rows_in_scope"]
                    + adapted.counters["rows_out_of_scope"],
                    len(adapted.rows),
                )
                self.assertEqual(
                    sum(
                        adapted.counters[f"path_kind_{kind}"]
                        for kind in paths.PATH_KINDS
                    ),
                    len(adapted.rows),
                )
                self.assertEqual(adapted.counters["severity_absent"], len(adapted.rows))

    def test_unresolvable_path_is_produced_where_the_metadata_supplies_no_base(self) -> None:
        """The sixth producible class, exercised from the metadata rather than a fixture.

        No committed artifact reaches ``unresolvable_path``: every record in every
        fixture is resolved or rejected as ``absent_path``. The class is reachable
        because a recorded base kind can establish no base at all, and the document's
        own instruction for that case is to reject rather than fall back -- defaulting to
        the scan root would produce a plausible path for a record whose base nobody
        established.

        AAP 0.9.4 requires an assertion for every condition an adapter can produce
        whether or not this run's artifacts contained the case, which is why this is
        asserted here instead of being left to a fixture that cannot carry it.
        """
        base = self.environment.recorded_base(
            kind=paths.PATH_BASE_KIND_NONE,
            base_value=None,
            invocations_per_run=1,
        )
        self.assertFalse(base.has_explicit_base)

        adapted = self.adapt([synthetic_record()], tool_base=base)
        self.assertEqual(adapted.rows, [])
        self.assertEqual(len(adapted.rejections), 1)
        self.assertEqual(
            adapted.rejections[0].reject_class, paths.REJECT_UNRESOLVABLE_PATH
        )
        self.assertIn(
            paths.REJECT_UNRESOLVABLE_PATH, gitleaks.REJECT_CLASSES_PRODUCED
        )
        self.assertTrue(adapted.rejections[0].detail)
        self.assertEqual(adapted.outcome_count, adapted.record_count)

    def test_every_declared_producible_class_is_exercised(self) -> None:
        """The union of what the fixtures and the metadata case reach is the declared set.

        The closing check on the negative suite. A class the adapter declares it can
        produce but nothing exercises is a rejection path nobody has run, and AAP 0.9.4
        is explicit that such a path needs an assertion regardless of what this run's
        artifacts happened to contain.
        """
        observed: set[str] = set()
        for stem in NEGATIVE_FIXTURES:
            for rejection in self.adapt_fixture(stem).rejections:
                observed.add(rejection.reject_class)

        no_base = self.environment.recorded_base(
            kind=paths.PATH_BASE_KIND_NONE, base_value=None
        )
        for rejection in self.adapt([synthetic_record()], tool_base=no_base).rejections:
            observed.add(rejection.reject_class)

        self.assertEqual(
            observed,
            set(gitleaks.REJECT_CLASSES_PRODUCED),
            "every class the adapter declares it can produce must be exercised, and no "
            "class outside that declaration may be produced",
        )

    def test_conditions_this_adapter_cannot_produce(self) -> None:
        """The four classes out of reach for this shape, asserted rather than described.

        Each is a real member of the closed vocabulary and each is absent from this
        adapter's declaration, for a reason that is a property of the artifact:

        * ``invalid_uri`` -- this shape reports a filesystem path, not a URI, so there is
          no URI to be syntactically invalid and no base chain to walk;
        * ``ambiguous_source_resolution`` -- there is no bytecode input, so no class
          identifier is resolved to a source file and none can resolve two ways;
        * ``unformable_package_coordinate`` -- a secret finding names a code location
          rather than a package, so the field is absent by design and its absence is not
          a rejection condition for a record that is not dependency-oriented;
        * ``unattributable_section`` -- the document is one flat array, so there is no
          per-section array a record could fail to be attributed to.

        Writing the reasons in a docstring alone would leave the claim unchecked; the
        membership assertions below are what make it fail if the declaration ever grows
        one of these.
        """
        out_of_reach = (
            paths.REJECT_INVALID_URI,
            paths.REJECT_AMBIGUOUS_SOURCE_RESOLUTION,
            paths.REJECT_UNFORMABLE_PACKAGE_COORDINATE,
            paths.REJECT_UNATTRIBUTABLE_SECTION,
        )
        for reject_class in out_of_reach:
            with self.subTest(reject_class=reject_class):
                self.assertIn(reject_class, paths.REJECT_CLASSES)
                self.assertNotIn(reject_class, gitleaks.REJECT_CLASSES_PRODUCED)

        # The two sets together account for the whole closed vocabulary, so nothing in
        # it is left unclassified as either producible or out of reach.
        self.assertEqual(
            set(gitleaks.REJECT_CLASSES_PRODUCED) | set(out_of_reach),
            set(paths.REJECT_CLASSES),
        )

    def test_a_rejection_detail_names_a_type_rather_than_rendering_a_string(self) -> None:
        """A record's own text never reaches a rejection reason.

        The reason has to be actionable and retained verbatim, and those two
        requirements together are what make this the interesting case: the detail must
        say enough to find the record without quoting anything the record contained. A
        string value is reduced to its type, which is why a value planted in a rejected
        record's ``RuleID`` cannot appear in the reason for rejecting it.
        """
        record = synthetic_record()
        record["RuleID"] = [SYNTHETIC_SECRET_SENTINEL]
        adapted = self.adapt([record])

        self.assertEqual(adapted.rows, [])
        self.assertEqual(len(adapted.rejections), 1)
        rejection = adapted.rejections[0]
        self.assertEqual(rejection.reject_class, paths.REJECT_MALFORMED_RECORD)
        self.assertIn("list", rejection.detail)
        self.assertFalse(
            SYNTHETIC_SENTINEL_PREFIX in rejection.detail,
            "a value from the record reached the rejection detail, which turns the "
            "reason channel into a leak channel",
        )
        self.assertEqual(
            sweep_rejections_for_values(
                adapted.rejections, sensitive_values([record])
            ),
            [],
        )

    def test_a_rejection_is_json_serialisable_under_its_class(self) -> None:
        """A rejection can be written to the run's status record without loss.

        ``normalize-run.json`` carries the per-artifact rejected counts with each class
        named, so a rejection has to survive serialisation with its class, its tool and
        its identity intact.
        """
        adapted = self.adapt_fixture(NEGATIVE_FIXTURES[0])
        self.assertGreater(len(adapted.rejections), 0)
        for index, rejection in enumerate(adapted.rejections):
            with self.subTest(rejection=index):
                serialised = json.loads(json.dumps(rejection.as_dict()))
                self.assertEqual(serialised["reject_class"], rejection.reject_class)
                self.assertEqual(serialised["tool"], TOOL)
                self.assertEqual(serialised["detail"], rejection.detail)
                self.assertEqual(
                    serialised["record_identity"], dict(rejection.record_identity)
                )


# --------------------------------------------------------------------------------- #
# Assertion 17: a bare array is not a mapping
# --------------------------------------------------------------------------------- #


class BareArrayShapeTests(GitleaksAdapterTestCase):
    """The top level is an array, and the adapter takes it as one.

    AAP 0.5.4 fixes the shape: *"a bare top-level JSON array, one element per finding"*,
    which is also the count unit the independent reconciliation traversal walks. Two
    consequences are asserted here, and they pull in opposite directions -- which is why
    both are needed.

    An **empty** array is not an error. Finding nothing is an ordinary outcome, and
    eighteen per-directory reports merged into one empty array is the ordinary shape of
    a clean scan.

    A **non-array** document raises rather than being counted as a rejection, and the
    reason is arithmetic rather than taste: the counting traversal finds zero records in
    a document that is not an array, so emitting a rejection for one would make
    ``rows + rejections`` exceed the record count and break the identity that rejection
    accounting exists to protect. It is also the same point the mandated shape-routing
    negative test makes from the other direction -- an empty result set is
    indistinguishable from a clean scan, so a malformed artifact must not be able to look
    like one.
    """

    def test_a_bare_array_needs_no_enclosing_object(self) -> None:
        """The document is handed over as a list and adapted directly."""
        document = load_fixture(POSITIVE_FIXTURE)
        self.assertIsInstance(document, list)
        adapted = self.adapt(document)
        self.assertEqual(len(adapted.rows), len(document))

    def test_an_empty_array_yields_nothing_and_no_error(self) -> None:
        """Zero rows, zero rejections, a zeroed counter set, and no exception."""
        adapted = self.adapt([])
        self.assertEqual(adapted.rows, [])
        self.assertEqual(adapted.rejections, [])
        self.assertEqual(tuple(adapted.counters), gitleaks.COUNTER_KEYS)
        self.assertEqual(set(adapted.counters.values()), {0})
        self.assertEqual(adapted.outcome_count, 0)
        self.assertEqual(adapted.record_count, 0)

    def test_an_object_top_level_raises_rather_than_rejecting(self) -> None:
        """An enclosing object is a document-level fault, and it is raised.

        Including the shape a reader might expect of a findings file -- an object with a
        ``findings`` array -- because that is the one most likely to be handed here by
        mistake, and it would otherwise parse to zero rows and look like a clean scan.
        """
        for document in ({}, {"findings": []}, {"results": [{"RuleID": "x"}]}):
            with self.subTest(document=type(document).__name__):
                with self.assertRaises(gitleaks.GitleaksAdapterError):
                    self.adapt(document)

    def test_a_scalar_or_null_top_level_raises(self) -> None:
        """Neither a scalar nor ``null`` is an array of findings.

        A string is refused explicitly rather than treated as a sequence: it *is* one in
        Python, so a length taken over it would count characters as findings.
        """
        for document in (None, 0, 1, 4.2, True, "", "[]", "not a document"):
            with self.subTest(document=repr(document)):
                with self.assertRaises(gitleaks.GitleaksAdapterError):
                    self.adapt(document)


# --------------------------------------------------------------------------------- #
# Caller faults: raised, never absorbed into a rejection count
# --------------------------------------------------------------------------------- #


class CallerContractTests(GitleaksAdapterTestCase):
    """A defective *call* stops the caller; a defective *record* is counted.

    The distinction is what keeps a rejection count meaningful. A wrong root, a foreign
    path base or the wrong tool identifier would each produce a plausible-looking
    dataset for an entire artifact, and absorbing one into a rejection count would leave
    the reconciliation identity holding over rows that are all wrong in the same
    direction.
    """

    def test_the_wrong_tool_identifier_is_refused(self) -> None:
        """The identifier is stamped into every row and fed to the tally.

        Accepting another one would attribute this artifact's rows to a different
        scanner in ``findings.json`` and in ``severity-map.md`` alike.
        """
        for wrong in ("semgrep", "trivy", "Gitleaks", "", "gitleaks "):
            with self.subTest(tool=wrong):
                with self.assertRaises(gitleaks.GitleaksAdapterError):
                    self.adapt([synthetic_record()], tool=wrong)

    def test_a_relative_root_is_refused(self) -> None:
        """A relative root cannot anchor anything, so it is refused on the call.

        Refused before any record is read rather than at the first one: a relative root
        would produce a plausible-looking wrong answer for every row, and the fault
        belongs to the call rather than to the artifact.
        """
        for root in ("relative/path", "", "."):
            with self.subTest(root=root):
                with self.assertRaises(gitleaks.GitleaksAdapterError):
                    gitleaks.adapt(
                        [synthetic_record()],
                        tool=TOOL,
                        root=root,
                        tool_base=self.environment.scan_root_base(),
                        allowlist=self.environment.globs,
                        tally=severity.LiteralTally.with_all_tools(),
                    )

    def test_a_tally_that_cannot_record_is_refused(self) -> None:
        """The tally is how a tool defining no vocabulary is named as one.

        A silently skipped tally would leave ``severity-map.md`` under-reporting with
        nothing to show that it had, so a tally that cannot record is a caller fault.
        """
        for tally in (None, object(), "a tally"):
            with self.subTest(tally=type(tally).__name__):
                with self.assertRaises(gitleaks.GitleaksAdapterError):
                    gitleaks.adapt(
                        [synthetic_record()],
                        tool=TOOL,
                        root=self.environment.root,
                        tool_base=self.environment.scan_root_base(),
                        allowlist=self.environment.globs,
                        tally=tally,
                    )

    def test_the_allowlist_is_consumed_once_into_a_tuple(self) -> None:
        """A generator allowlist would be exhausted by the first row.

        Every subsequent row would then silently take ``in_scope`` false, which is
        indistinguishable from a scope that genuinely excluded them. The adapter
        materialises the globs once, and this is the assertion that says so: two rows
        from one generator, both in scope.
        """
        in_scope_path = "core/src/main/scala/org/apache/spark/storage/DiskStore.scala"
        document = [
            synthetic_record(file_value=in_scope_path),
            synthetic_record(file_value=in_scope_path),
        ]
        rows, rejections, _ = gitleaks.adapt(
            document,
            tool=TOOL,
            root=self.environment.root,
            tool_base=self.environment.scan_root_base(),
            allowlist=(glob for glob in self.environment.globs),
            tally=severity.LiteralTally.with_all_tools(),
        )
        self.assertEqual(rejections, [])
        self.assertEqual(len(rows), 2)
        for index, row in enumerate(rows):
            with self.subTest(row=index):
                self.assertIs(row["in_scope"], True)

    def test_the_loaded_allowlist_is_the_twelve_authoritative_globs(self) -> None:
        """The scope this module writes, loads and adapts under is the authoritative one.

        Written as twelve lines, read back through ``paths.load_allowlist`` and then
        checked against what ``paths.py`` itself authors. Comparing the module's own copy
        with itself would assert nothing, which is why the twelve are restated in this
        file and the round trip is what closes the loop.
        """
        self.assertEqual(len(AUTHORITATIVE_GLOBS), 12)
        self.assertEqual(self.environment.globs, AUTHORITATIVE_GLOBS)
        self.assertTrue(
            paths.allowlist_matches_authoritative_globs(self.environment.globs)
        )
        self.assertNotIn(
            paths.SRC_TEST_MARKER,
            self.environment.allowlist_path.read_text(encoding="utf-8"),
            "the src/test exclusion belongs to the matcher in paths.py, not to the "
            "allowlist file",
        )
