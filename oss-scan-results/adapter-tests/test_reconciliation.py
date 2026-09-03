"""Assert the reconciliation identity that makes every number in the dataset checkable.

What this module tests
---------------------
One identity, stated per artifact in AAP 0.5.4::

    raw finding records = dataset rows for that tool + rejected records

The left-hand side must come from ``normalize.reconcile.count_records`` -- *"a traversal
that walks the count units and builds nothing"*.  The dataset-level assertion is the **sum
of the per-artifact identities**, and the parsed ``findings.json`` and parsed
``findings.csv`` row counts are *"compared to it separately rather than assumed equal"*.
AAP 0.6.1 gives this file its row, AAP 0.9.4 puts it in the definition of done, and
AAP 0.9.2 makes a failure here a condition that halts the run.

Why the second traversal has to be independent, and why that is asserted structurally
------------------------------------------------------------------------------------
AAP 0.5.4 names the failure mode precisely: *"A count taken from the same traversal that
builds the rows satisfies the assertion while testing nothing."*  An identity whose two
sides come from one code path is arithmetic about itself.  Observing that
``reconcile.count_records`` happens to agree with an adapter on one fixture does not
establish independence -- a delegating implementation agrees on every fixture.  So
independence is asserted against the import graph and the signatures
(:class:`CountingTraversalIndependenceTest`): ``reconcile`` may import nothing from the
``normalize`` package, and the rejection-class vocabulary must arrive as a **parameter**
rather than as an import, which is the design property that keeps it free of ``paths``.

Hermetic by construction
------------------------
Every test runs against an absolute scan root inside a
:class:`tempfile.TemporaryDirectory`, an allowlist file holding the twelve authoritative
globs, and a minimal ``runner-metadata.json`` -- all three loaded through ``paths.py``'s own
loaders, never bypassed.  No live Spark tree is read, no committed fixture is mutated, and
nothing is written outside a temporary directory: in particular this module never writes
``oss-scan-results/findings.json`` or ``oss-scan-results/findings.csv``.

The identity is root-independent, which is what makes the temporary root legitimate rather
than a convenience.  A reported path that resolves inside the root produces a row and one
that cannot be resolved produces a counted rejection; the root decides which side of the
identity a record lands on, and the identity holds either way.  Field-level correctness for
one root is each per-adapter test's subject, not this file's.

The absent artifact, and why a zero will not do
-----------------------------------------------
A tool that wrote no artifact reconciles as the exact literal
``reconcile.NOT_APPLICABLE_ABSENT`` -- the words "not applicable", an em dash (U+2014) and
"artifact absent" -- and never as ``0 = 0 + 0``, which would be a passing assertion over an
artifact nobody looked at (AAP 0.5.4).  ``osv-scanner`` is the tool expected in that state this run:
exactly one manifest-shaped file is in scope,
``core/src/main/resources/org/apache/spark/ui/static/package.json``, five lines carrying a
name, a license and a type with no dependencies block and no lockfile beside it, and there
is no ``pom.xml``, ``requirements*.txt``, ``setup.py``, ``pyproject.toml`` or JAR anywhere
in the eighteen in-scope directories -- so the tool resolves zero packages, states that
reason in its own words and exits 128 (AAP 0.2.1, AAP 0.2.2).  No OSV fixture exists in
this tree and none is expected, so the absent case is asserted synthetically, and the OSV
count unit is asserted as a **definition** rather than over an artifact.

The composer, and why its coverage lives here
--------------------------------------------
``harness/lib/normalize/cli.py`` is the only module that couples the others, and the
reconciliation identity is one of the things it composes: it takes the independent count
before an adapter runs, establishes stages A and B **before either output file is
written**, writes both files from one row list, and then establishes stage C over the two
parsed files.  None of that is observable from ``reconcile.py`` alone -- the ordering, the
halts and the exit codes belong to the composer -- so the ``Cli*`` classes at the foot of
this module assert them, driven in process over temporary inputs built from the committed
fixtures.  The eight-module test inventory is frozen, so this coverage is added here rather
than in a ninth module.

Three properties hold across every one of those classes.  Every call is an in-process call
on ``cli``, so a halt is asserted as the exception object it is -- its class, its ``reason``
from the closed ``cli.HALT_REASONS`` set, its ``exit_code`` and its serialisable details --
rather than as an exit status with a message to be pattern-matched.  Every path handed to
the normalizer, including both output files and the run record, is inside a
:class:`tempfile.TemporaryDirectory`, so nothing is written into the repository and no
committed artifact is read for anything but copying a fixture *out* of ``fixtures/``.  And
no scanner, build, graph or Spark test is invoked by any of it.

Prohibitions this module observes
---------------------------------
It performs no cross-tool interpretation of any kind (AAP 0.3.2): it counts per tool and
sums, which is arithmetic, and it never ranks tools, contrasts their coverage or accounts
for a difference between two tools' counts.  It deduplicates nothing -- two tools reporting
one location are two rows, and the identity holds with both counted.  It judges no finding.
It carries no secret value in any literal, message or docstring, this tree being committed
to git.  It edits nothing under ``harness/lib/normalize/``: a defect there is reported, not
repaired here.

Rules
-----
No user-specified rule governs this file; enterprise-standard best practice applies in its
place (AAP 0.7, AAP 0.10.2).  That absence is not licence to lower the bar -- this file is
held to the AAP's own bar, which is why the independence above is established structurally
and every mandated rejection path is asserted rather than assumed.

Running it
----------
Standard library only, no ``pytest``, and runnable from any working directory::

    python3 -m unittest discover -s oss-scan-results/adapter-tests -p 'test_reconciliation.py'
"""

from __future__ import annotations

import ast
import contextlib
import copy
import csv
import dataclasses
import hashlib
import inspect
import io
import json
import os
import shutil
import sys
import tempfile
import types
import unittest
import unittest.mock
from collections.abc import Mapping
from pathlib import Path

# --------------------------------------------------------------------------------------
# The one-time sys.path bootstrap, mirroring the two lines cli.py documents for these
# tests.  There is deliberately no __init__.py under harness/lib/normalize/: PEP 420
# implicit namespace packages make "from normalize import ..." work once harness/lib is on
# sys.path.  parents[2] of this file is the repository root, so the entry is derived from
# this file's own location rather than from the working directory -- which is what lets the
# module be discovered from the repository root and from anywhere else alike.
# --------------------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[2]
_LIB_DIR = str(REPO_ROOT / "harness" / "lib")
if _LIB_DIR not in sys.path:
    sys.path.insert(0, _LIB_DIR)

from normalize import cli, emit, paths, reconcile, severity, shape  # noqa: E402
from normalize.adapters import (  # noqa: E402
    checkov,
    dependency_check,
    gitleaks,
    joern,
    sarif,
    trivy,
)

# --------------------------------------------------------------------------------------
# Locations
# --------------------------------------------------------------------------------------

#: This tree.  Both directories are inputs and are never written to by this module.
ADAPTER_TESTS_DIR = Path(__file__).resolve().parent
FIXTURES_DIR = ADAPTER_TESTS_DIR / "fixtures"
EXPECTED_DIR = ADAPTER_TESTS_DIR / "expected"

# --------------------------------------------------------------------------------------
# The twelve authoritative scope globs (AAP 0.3.1), byte-exact and in the request's order.
#
# Written here as an independent restatement rather than read from paths.ALLOWLIST_GLOBS:
# the test writes these twelve lines to its own allowlist file, loads them back through
# paths.load_allowlist() and then confirms the loaded tuple is what paths.py authors, via
# paths.allowlist_matches_authoritative_globs().  Loading the module's own copy and
# comparing it with itself would assert nothing.  There is no exclusion line -- the
# `src/test` exclusion is paths.py's, not the allowlist's.
# --------------------------------------------------------------------------------------
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

# --------------------------------------------------------------------------------------
# The minimal runner metadata this module writes and then loads through paths.py.
#
# Each entry carries only what a resolver needs: the recorded path_base kind for that tool
# and the anchor fields where its kind has them.  The kinds are the ones
# harness/artifacts/logs/runner-metadata.json records for this provisioning -- read as
# input, never inferred from an artifact (AAP 0.5.4) -- and joern's base value is null
# because no filesystem base exists for a bytecode class.
# --------------------------------------------------------------------------------------
RECORDED_PATH_BASES = {
    "opengrep": {"kind": "scan_root", "value_is_root": True},
    "semgrep": {"kind": "scan_root", "value_is_root": True},
    "datadog-static-analyzer": {"kind": "scan_root", "value_is_root": True},
    "gitleaks": {"kind": "scan_root", "value_is_root": True, "record_path_field": "File"},
    "checkov": {
        "kind": "per_target_directory",
        "value_is_root": True,
        "anchor_fields": ["repo_file_path", "file_abs_path"],
    },
    "trivy": {"kind": "scan_root", "value_is_root": True},
    "osv-scanner": {"kind": "scan_root", "value_is_root": True},
    "dependency-check": {"kind": "filesystem_absolute", "value_is_root": True},
    "joern": {
        "kind": "bytecode_class",
        "value_is_root": False,
        "record_path_field": "class",
        "record_path_field_to_ignore": "file",
    },
}

# --------------------------------------------------------------------------------------
# A deliberately partial synthetic source index for the Joern adapter.
#
# paths.SourceIndex.from_mapping is the documented test route: it needs no live tree, so the
# resolution is decided by this mapping rather than by whatever the filesystem holds today.
# Three of the fixture's eleven classes are indexed and eight are not, so Joern contributes
# rows *and* rejections to the sweep -- which is the point. An identity asserted only over
# artifacts whose every record became a row never exercises the right-hand side.
#
# Every path below names a real file in the pinned tree; two are inside the twelve globs and
# `launcher/...` is outside them, so one Joern row legitimately carries in_scope false.
# --------------------------------------------------------------------------------------
SYNTHETIC_JOERN_INDEX_BY_FILENAME = {
    "org/apache/spark/rdd/PipedRDD": ("core/src/main/scala/org/apache/spark/rdd/PipedRDD.scala",),
    "org/apache/spark/deploy/worker/ExecutorRunner": (
        "core/src/main/scala/org/apache/spark/deploy/worker/ExecutorRunner.scala",
    ),
    "org/apache/spark/launcher/SparkLauncher": (
        "launcher/src/main/java/org/apache/spark/launcher/SparkLauncher.java",
    ),
}

# --------------------------------------------------------------------------------------
# One artifact per tool, which is what the raw tree holds: reconcile.run_stage_a refuses two
# entries for one tool, because a duplicate would double-count it in the dataset sum. The
# derived reconcile-mixed.json artifact is therefore asserted on its own (it carries the
# Opengrep shape) and is deliberately absent from this table.
#
# `module` is the adapter each artifact routes to. Nothing here decides routing: shape
# detection is shape.py's and is asserted by test_shape_routing_negative.py. This table
# records which adapter each committed fixture belongs to so the identity can be taken over
# each of them.
# --------------------------------------------------------------------------------------
FIXTURE_CASES = (
    ("opengrep", "opengrep.sarif", sarif),
    ("semgrep", "semgrep.sarif", sarif),
    ("datadog-static-analyzer", "datadog-static-analyzer.sarif", sarif),
    ("gitleaks", "gitleaks.json", gitleaks),
    ("checkov", "checkov.json", checkov),
    ("trivy", "trivy.json", trivy),
    ("dependency-check", "dependency-check.json", dependency_check),
    ("joern", "joern.json", joern),
)

#: The one tool expected to have written no artifact this run (AAP 0.2.1).
ABSENT_ARTIFACT_TOOL = "osv-scanner"

#: The derived fixture that carries both outcomes -- the only input over which the identity
#: is non-degenerate, since over an all-valid document it collapses to ``raw = rows``.
MIXED_FIXTURE = "reconcile-mixed.json"

#: The two captured artifacts whose own shape cannot exhibit a half of its count-unit rule,
#: paired with the declared-derived document that can.
#:
#: ``fixtures/checkov.json`` and ``fixtures/trivy.json`` are byte-for-byte copies of the
#: artifacts under ``harness/artifacts/raw/`` (AAP 0.6.2 -- an unmodified captured excerpt),
#: and the tool wrote a narrow document in each case: Checkov emitted only a
#: ``failed_checks`` bucket, and Trivy emitted only ``Misconfigurations`` across its three
#: config results. So the exclusion half of the Checkov rule -- a *present* passed or
#: skipped bucket contributing zero -- and the spanning half of the Trivy rule -- a unit
#: summing across more than one supported section -- have no material in the captures to be
#: asserted over. Each is therefore asserted over the ``derived-<tool>-features`` document,
#: which is declared derived in its own expected file and exists to carry exactly the cases
#: the tool's own output does not. The capture still carries the other half of each rule,
#: which is why both documents appear rather than one replacing the other.
CHECKOV_CAPTURED_FIXTURE = "checkov.json"
CHECKOV_FEATURES_FIXTURE = "derived-checkov-features.json"
TRIVY_CAPTURED_FIXTURE = "trivy.json"
TRIVY_FEATURES_FIXTURE = "derived-trivy-features.json"


class Environment:
    """The hermetic inputs every test shares: a scan root, an allowlist and runner metadata.

    All three are real files inside one temporary directory, and both configuration files
    are read back through ``paths.py``'s own loaders rather than being handed to the
    adapters as literals -- so the loaders are exercised on the same route ``cli.py`` uses.

    Attributes:
        directory: The temporary directory holding everything this object created.
        root: The absolute scan root the adapters express paths against. It need not
            contain any file: no adapter reads the tree, and the Joern resolution is driven
            by a synthetic source index instead.
        globs: The twelve authoritative globs, as ``paths.load_allowlist`` returned them.
        metadata: The runner-metadata document, as ``paths.load_runner_metadata`` returned
            it.
        source_index: The deliberately partial synthetic index the Joern adapter resolves
            against.
    """

    def __init__(self, directory: Path) -> None:
        """Create the scan root and write, then load, the allowlist and the metadata."""
        self.directory = directory
        self.root = str(directory / "spark-src")
        (directory / "spark-src").mkdir(parents=True, exist_ok=True)

        allowlist_path = directory / "allowlist.txt"
        # One glob per line, byte-exact, with a trailing newline and nothing else.
        allowlist_path.write_text(
            "".join(f"{glob}\n" for glob in AUTHORITATIVE_GLOBS), encoding="utf-8"
        )
        self.allowlist_path = allowlist_path
        self.globs = paths.load_allowlist(allowlist_path)

        metadata_path = directory / "runner-metadata.json"
        metadata_path.write_text(
            json.dumps(self._metadata_document(), indent=1) + "\n", encoding="utf-8"
        )
        self.metadata_path = metadata_path
        self.metadata = paths.load_runner_metadata(metadata_path)

        self.source_index = paths.SourceIndex.from_mapping(
            SYNTHETIC_JOERN_INDEX_BY_FILENAME,
            {},
            files_indexed=len(SYNTHETIC_JOERN_INDEX_BY_FILENAME),
        )

    def _metadata_document(self) -> dict:
        """Build the minimal document ``paths.load_runner_metadata`` accepts.

        Minimal is deliberate: this module asserts counts and the agreement of two output
        files, so the document carries the base facts a resolver needs and nothing that
        would make the test a second copy of the real record.
        """
        tools = {}
        for tool, recorded in RECORDED_PATH_BASES.items():
            path_base = {"kind": recorded["kind"]}
            if recorded["value_is_root"]:
                path_base["value"] = self.root
            else:
                path_base["value"] = None
            for key in ("anchor_fields", "record_path_field", "record_path_field_to_ignore"):
                if key in recorded:
                    path_base[key] = recorded[key]
            tools[tool] = {
                "canonical_tool_identifier": tool,
                "path_base": path_base,
                "resolved_scan_root": self.root,
            }
        return {
            "purpose": (
                "Minimal runner metadata for the reconciliation adapter test. Written and "
                "read inside a temporary directory; it is not the run's record."
            ),
            "spark_src": self.root,
            "tools": tools,
        }

    def tool_base(self, tool: str) -> paths.ToolPathBase:
        """Return one tool's recorded path base, taken from the loaded document."""
        return paths.tool_path_base(self.metadata, tool)


class Adapted:
    """One artifact's adaptation and its independent record count, measured once.

    Both sides of the identity are held here so a test asserts over one measurement rather
    than taking a second one: AAP 0.6.4 requires a count that appears twice to be one
    measurement cited twice.

    Attributes:
        tool: The canonical tool identifier.
        filename: The fixture the document was parsed from.
        document: The parsed artifact.
        rows: The dataset rows the adapter emitted, each carrying the twelve fields.
        rejections: The ``paths.Rejection`` records it counted instead.
        counters: The adapter's own counters.
        raw_records: ``reconcile.count_records`` over the same document -- the traversal
            that builds nothing.
        rejections_by_class: Rejection counts per named class, tallied exactly as
            ``cli.py`` tallies them.
    """

    __slots__ = (
        "tool",
        "filename",
        "document",
        "rows",
        "rejections",
        "counters",
        "raw_records",
        "rejections_by_class",
    )

    def __init__(self, tool, filename, document, rows, rejections, counters, raw_records):
        """Hold one artifact's two measurements and tally its rejections by class."""
        self.tool = tool
        self.filename = filename
        self.document = document
        self.rows = rows
        self.rejections = rejections
        self.counters = counters
        self.raw_records = raw_records
        by_class: dict[str, int] = {}
        for rejection in rejections:
            by_class[rejection.reject_class] = by_class.get(rejection.reject_class, 0) + 1
        self.rejections_by_class = by_class

    @property
    def artifact_counts(self) -> reconcile.ArtifactCounts:
        """Return this artifact's counts in the record ``reconcile`` takes as input."""
        return reconcile.ArtifactCounts.for_present_artifact(
            self.tool,
            raw_records=self.raw_records,
            emitted_rows=len(self.rows),
            rejected_records=len(self.rejections),
            rejections_by_class=self.rejections_by_class,
        )


#: Module-level state, built once in :func:`setUpModule` and released in
#: :func:`tearDownModule`. Held at module level because every test needs the same root, and
#: rebuilding it per test would make each test's rows depend on a different temporary path.
ENV: Environment | None = None
_TEMPORARY_DIRECTORY: tempfile.TemporaryDirectory | None = None
_ADAPTED_CACHE: dict[tuple[str, str], Adapted] = {}


def setUpModule() -> None:
    """Create the temporary scan root, allowlist and runner metadata for the whole module."""
    global ENV, _TEMPORARY_DIRECTORY
    _TEMPORARY_DIRECTORY = tempfile.TemporaryDirectory(prefix="blitzy-reconciliation-")
    ENV = Environment(Path(_TEMPORARY_DIRECTORY.name))


def tearDownModule() -> None:
    """Release the temporary directory. Nothing this module wrote survives it."""
    global ENV, _TEMPORARY_DIRECTORY
    _ADAPTED_CACHE.clear()
    ENV = None
    if _TEMPORARY_DIRECTORY is not None:
        _TEMPORARY_DIRECTORY.cleanup()
        _TEMPORARY_DIRECTORY = None


def environment() -> Environment:
    """Return the module's environment, or fail loudly if it was never built."""
    if ENV is None:  # pragma: no cover - defended, unreachable under unittest
        raise RuntimeError("setUpModule did not run: the hermetic environment is missing")
    return ENV


def load_fixture(filename: str):
    """Parse one committed fixture. The file is read and never written.

    A fresh document is returned on every call, so a test that needs to derive a variant
    can deep-copy it without any risk of a shared object being mutated between tests.
    """
    return json.loads((FIXTURES_DIR / filename).read_text(encoding="utf-8"))


def load_expected(name: str):
    """Parse one hand-verified expected file from ``expected/``."""
    return json.loads((EXPECTED_DIR / f"{name}.rows.json").read_text(encoding="utf-8"))


def adapt(tool: str, filename: str, module: types.ModuleType) -> Adapted:
    """Adapt one fixture and count its records independently, caching the result.

    The row-building call and the counting call are made here side by side so that every
    test asserts over the same pair of measurements. They remain two code paths:
    ``module.adapt`` builds rows, ``reconcile.count_records`` walks count units and builds
    nothing, and :class:`CountingTraversalIndependenceTest` is what establishes that the
    second cannot be the first in disguise.
    """
    key = (tool, filename)
    cached = _ADAPTED_CACHE.get(key)
    if cached is not None:
        return cached

    env = environment()
    document = load_fixture(filename)
    keywords = {
        "tool": tool,
        "root": env.root,
        "tool_base": env.tool_base(tool),
        "allowlist": env.globs,
        # A fresh tally per artifact: the tally is severity-map.md's input and is not part
        # of the identity, so nothing here depends on its accumulated state.
        "tally": severity.LiteralTally.with_all_tools(),
    }
    if module is joern:
        keywords["source_index"] = env.source_index
    rows, rejections, counters = module.adapt(document, **keywords)
    adapted = Adapted(
        tool=tool,
        filename=filename,
        document=document,
        rows=rows,
        rejections=rejections,
        counters=counters,
        raw_records=reconcile.count_records(tool, document),
    )
    _ADAPTED_CACHE[key] = adapted
    return adapted


def adapt_all() -> list[Adapted]:
    """Adapt every committed positive fixture, one artifact per tool, in table order."""
    return [adapt(tool, filename, module) for tool, filename, module in FIXTURE_CASES]


def adapt_mixed() -> Adapted:
    """Adapt the mixed fixture: the one artifact carrying both rows and rejections."""
    return adapt("opengrep", MIXED_FIXTURE, sarif)


#: A message carrying an embedded newline, a comma and a double quote -- the three
#: characters that make a CSV field span physical lines and need quoting. Derived for the
#: line-count assertion rather than taken from an artifact, because no committed fixture's
#: message carries a newline while the dataset's do: ``findings.csv`` holds 9,427 rows over
#: 9,436 physical lines, so a row count taken from lines is wrong by construction. It
#: carries no secret and no tool's real output.
MULTILINE_MESSAGE = (
    'the finding message continues on a second line, with a comma\n'
    'and a "quoted" fragment, and then a third line\n'
    "so that one row spans three physical lines of CSV"
)


def dataset_rows(multiline_message: bool = False) -> list:
    """Return the rows every committed artifact contributed, in artifact order.

    This is the row set the two output files are written from: one record per finding,
    twelve fields, no metadata envelope, nothing sorted, grouped or deduplicated. The rows
    are deep-copied out of the cache so no test can mutate another's input.

    Args:
        multiline_message: Replace one row's ``message`` with :data:`MULTILINE_MESSAGE`.
            The row count is unchanged, so the identity's row total still governs -- what
            changes is that the CSV then spans more physical lines than it holds rows.
    """
    rows: list = []
    for adapted in adapt_all():
        rows.extend(copy.deepcopy(adapted.rows))
    if multiline_message:
        # Not the first row: a row in the middle also shows that the rows after it keep
        # their positions, which a header-adjacent row would not.
        rows[len(rows) // 2]["message"] = MULTILINE_MESSAGE
    return rows


def coerce_csv_row(cells: dict) -> dict:
    """Coerce one parsed CSV row to the types ``findings.json`` carries (AAP 0.5.4).

    Three coercions and no others: ``start_line`` to an ``int`` or ``None``, ``in_scope``
    to a ``bool`` from the literal that was written, and every empty optional cell to
    ``None``. Written out here rather than delegated to ``emit.read_findings_csv`` so that
    the comparison is this test's own reading of the file rather than a second call to the
    code under test.
    """
    coerced = {}
    for field in emit.FIELDS:
        cell = cells[field]
        if field == "start_line":
            coerced[field] = None if cell == emit.CSV_ABSENT else int(cell)
        elif field == "in_scope":
            coerced[field] = {emit.CSV_TRUE: True, emit.CSV_FALSE: False}[cell]
        else:
            coerced[field] = None if cell == emit.CSV_ABSENT else cell
    return coerced


def parse_csv_rows(path: Path) -> list:
    """Parse ``findings.csv`` with :mod:`csv` and return its coerced data rows.

    ``newline=""`` is what :mod:`csv` requires for an embedded newline inside a quoted
    field to be read back as part of that field rather than as a row break.
    """
    with path.open("r", encoding="utf-8", newline="") as handle:
        records = [record for record in csv.reader(handle) if record]
    if not records:
        raise AssertionError(f"{path} carries no records at all, not even a header")
    header, data = records[0], records[1:]
    if tuple(header) != emit.FIELDS:
        raise AssertionError(f"{path} header is {header}, not the twelve fields in order")
    rows = []
    for index, record in enumerate(data):
        if len(record) != len(emit.FIELDS):
            raise AssertionError(
                f"{path} row {index} carries {len(record)} fields, not {len(emit.FIELDS)}"
            )
        rows.append(coerce_csv_row(dict(zip(emit.FIELDS, record))))
    return rows


def parse_json_rows(path: Path) -> list:
    """Parse ``findings.json`` and return its rows, asserting the row-only shape."""
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, list):
        raise AssertionError(
            f"{path} must be a row-only JSON array with no metadata envelope, got "
            f"{type(document).__name__}"
        )
    return document


def normalize_sibling_names() -> frozenset:
    """Return every importable name inside the ``normalize`` package, discovered on disk.

    Discovered rather than listed so a module added to the package later is covered
    without this test being edited -- a hardcoded list would silently stop protecting the
    import graph the day it went stale.
    """
    package_dir = Path(reconcile.__file__).resolve().parent
    names = {module.stem for module in package_dir.glob("*.py")}
    names -= {"reconcile"}
    names |= {module.stem for module in (package_dir / "adapters").glob("*.py")}
    names |= {"normalize", "adapters"}
    return frozenset(names)


def imported_module_names(module: types.ModuleType) -> list:
    """Return every module name ``module``'s own source imports, with relative ones marked.

    The source is parsed with :mod:`ast` rather than the module being introspected after
    the fact, because an import that fails to bind a name -- ``from x import y`` where the
    name is shadowed later -- is still an import. A relative import is returned with its
    leading dots so a caller can reject it without having to resolve it.
    """
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    found: list = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            found.append("." * (node.level or 0) + (node.module or ""))
    return found


class CountingTraversalIndependenceTest(unittest.TestCase):
    """The counting traversal is independent of row construction, structurally.

    AAP 0.5.4: the left-hand side of the identity comes from *"a traversal that walks the
    count units and builds nothing"*, because *"a count taken from the same traversal that
    builds the rows satisfies the assertion while testing nothing"*. That cannot be
    established by agreement on a fixture -- an implementation that delegated to the
    adapter would agree everywhere -- so it is established here against the import graph,
    the signatures and the one behaviour that separates counting from validating.
    """

    def test_reconcile_imports_nothing_from_the_normalize_package(self) -> None:
        """No sibling module, no relative import, no ``normalize`` import at all."""
        siblings = normalize_sibling_names()
        self.assertIn("paths", siblings, "the sibling discovery found no paths module")
        self.assertIn("severity", siblings, "the sibling discovery found no severity module")

        for name in imported_module_names(reconcile):
            with self.subTest(imported=name):
                self.assertFalse(
                    name.startswith("."),
                    "reconcile.py must not use a relative import: a relative import inside "
                    "the package is exactly the dependency this module may not have",
                )
                first_segment = name.split(".", 1)[0]
                self.assertNotIn(
                    first_segment,
                    siblings,
                    f"reconcile.py imports {name!r} from the normalize package; the "
                    "counting traversal must depend on nothing that builds rows",
                )

    def test_reconcile_namespace_holds_no_module_from_the_normalize_package(self) -> None:
        """A second, independent reading of the same property: its bound namespace."""
        for name, value in vars(reconcile).items():
            if isinstance(value, types.ModuleType):
                with self.subTest(attribute=name, module=value.__name__):
                    self.assertFalse(
                        value.__name__ == "normalize"
                        or value.__name__.startswith("normalize."),
                        f"reconcile.{name} is bound to {value.__name__}, a module of the "
                        "normalize package",
                    )

    def test_the_rejection_class_vocabulary_is_a_parameter_not_an_import(self) -> None:
        """The design property that makes the independence structural.

        ``paths.REJECT_CLASSES`` is the closed vocabulary every rejection is counted
        under, and ``reconcile`` cannot import it. So every entry point that has to
        validate a class name accepts the vocabulary as an argument, and ``cli.py`` passes
        it in.
        """
        self.assertFalse(
            hasattr(reconcile, "REJECT_CLASSES"),
            "reconcile must not carry the rejection-class vocabulary: taking it as a "
            "parameter is what keeps it free of an import from paths",
        )
        vocabulary_parameter = "reject_classes"
        signature = inspect.signature(reconcile.validate_rejection_classes)
        self.assertIn(vocabulary_parameter, signature.parameters)
        for function in (
            reconcile.reconcile_artifact,
            reconcile.run_stage_a,
            reconcile.run_three_stage_validation,
        ):
            with self.subTest(function=function.__name__):
                self.assertIn(
                    vocabulary_parameter, inspect.signature(function).parameters
                )

    def test_the_passed_vocabulary_is_the_one_that_decides(self) -> None:
        """Behavioural proof of the same thing: a restricted vocabulary is honoured.

        A class name that *is* a member of ``paths.REJECT_CLASSES`` must still be refused
        when the caller passes a vocabulary that excludes it. An implementation that
        reached for the canonical tuple itself would accept it, and the parameter would be
        decoration.
        """
        genuine_class = paths.REJECT_MISSING_MESSAGE
        self.assertIn(genuine_class, paths.REJECT_CLASSES)

        accepted = reconcile.validate_rejection_classes(
            {genuine_class: 2}, paths.REJECT_CLASSES
        )
        self.assertEqual(accepted, {genuine_class: 2})

        restricted = tuple(name for name in paths.REJECT_CLASSES if name != genuine_class)
        with self.assertRaises(reconcile.UnknownRejectionClassError):
            reconcile.validate_rejection_classes({genuine_class: 2}, restricted)

    def test_count_records_takes_a_document_and_returns_a_count(self) -> None:
        """It returns a number, never anything row-shaped, for every committed artifact."""
        signature = inspect.signature(reconcile.count_records)
        self.assertEqual(tuple(signature.parameters), ("tool", "doc"))

        for adapted in adapt_all() + [adapt_mixed()]:
            with self.subTest(tool=adapted.tool, fixture=adapted.filename):
                counted = reconcile.count_records(adapted.tool, adapted.document)
                self.assertIsInstance(counted, int)
                self.assertNotIsInstance(
                    counted,
                    bool,
                    "a boolean count would make True read as one record",
                )
                self.assertNotIsInstance(
                    counted,
                    (list, tuple, dict, set),
                    "the counting traversal must return a count, never a collection of "
                    "records: building one is what it exists not to do",
                )
                self.assertGreaterEqual(counted, 0)

    def test_count_records_counts_records_it_cannot_validate(self) -> None:
        """Counting is not validating, and the mixed fixture is where the two diverge.

        Three of its seven records are rejected by the adapter. The traversal counts all
        seven, which is only possible if it reads container keys rather than record
        content -- and is why the right-hand side of the identity has two terms.
        """
        adapted = adapt_mixed()
        expected = load_expected("reconcile-mixed")
        self.assertEqual(adapted.raw_records, expected["counts"]["raw_finding_records"])
        self.assertGreater(len(adapted.rejections), 0)
        self.assertGreater(
            adapted.raw_records,
            len(adapted.rows),
            "the traversal counted no more records than the adapter turned into rows, so "
            "this fixture no longer distinguishes counting from row construction",
        )


class CountUnitTest(unittest.TestCase):
    """One count unit per artifact shape, each asserted against a document (AAP 0.5.4).

    The unit is what the traversal walks, and getting it wrong is not visible downstream:
    a unit one level too shallow under-counts and the identity then fails for a reason
    nobody can locate, while a unit that counts the wrong container over-counts and every
    record looks dropped. Each test below states the unit as arithmetic over the document
    itself rather than as a number, so the assertion survives a fixture being extended.
    """

    def test_sarif_count_unit_is_runs_results(self) -> None:
        """``runs[].results[]``, summed across every run."""
        sarif_fixtures = [
            (tool, filename)
            for tool, filename, module in FIXTURE_CASES
            if module is sarif
        ] + [("opengrep", MIXED_FIXTURE)]

        for tool, filename in sarif_fixtures:
            with self.subTest(tool=tool, fixture=filename):
                document = load_fixture(filename)
                runs = document["runs"]
                self.assertGreater(len(runs), 0)
                expected = sum(len(run.get("results", [])) for run in runs)
                self.assertEqual(reconcile.count_records(tool, document), expected)
                self.assertIn(tool, reconcile.SARIF_TOOLS)

    def test_sarif_count_unit_spans_runs_and_tolerates_a_run_without_results(self) -> None:
        """A second run's results are counted too, and a run carrying none contributes zero.

        Built in memory: no committed SARIF fixture carries two runs, and a run with no
        results array is the ordinary shape of a clean run, so both halves of the rule need
        a document that exhibits them.
        """
        document = {
            "version": "2.1.0",
            "runs": [
                {"results": [{"ruleId": "a"}, {"ruleId": "b"}]},
                {"results": []},
                {},
            ],
        }
        self.assertEqual(reconcile.count_records("opengrep", document), 2)

    def test_gitleaks_count_unit_is_the_bare_top_level_array(self) -> None:
        """The artifact is a JSON array, so the document itself is the container."""
        document = load_fixture("gitleaks.json")
        self.assertIsInstance(document, list)
        self.assertEqual(reconcile.count_records("gitleaks", document), len(document))
        self.assertEqual(reconcile.count_records("gitleaks", []), 0)

    def test_checkov_count_unit_is_the_union_of_failed_checks_across_both_shapes(self) -> None:
        """Both output shapes, and the union across every element of the array form.

        The committed multi-framework fixture is a real captured shape whose failures all
        sit in one element, so the union is additionally asserted over a document derived
        in memory that splits the same failed checks across two elements. Without failures
        in more than one element, a implementation that read only the first element would
        pass.
        """
        self.assertEqual(reconcile.CHECKOV_COUNTED_SECTION, "failed_checks")

        object_form = load_fixture("checkov.json")
        self.assertIsInstance(object_form, dict)
        failed = object_form["results"]["failed_checks"]
        object_count = reconcile.count_records("checkov", object_form)
        self.assertEqual(object_count, len(failed))

        array_form = load_fixture("checkov-alt-shape.json")
        self.assertIsInstance(array_form, list)
        self.assertEqual(
            reconcile.count_records("checkov", array_form),
            object_count,
            "the two output shapes carry the same failed checks and must count the same",
        )

        split = copy.deepcopy(array_form)
        self.assertGreaterEqual(len(split), 2)
        self.assertGreaterEqual(
            len(failed), 2, "the fixture must carry at least two failures to be split"
        )
        head, tail = failed[:1], failed[1:]
        split[0].setdefault("results", {})["failed_checks"] = copy.deepcopy(head)
        split[1].setdefault("results", {})["failed_checks"] = copy.deepcopy(tail)
        elements_with_failures = sum(
            1 for report in split if report.get("results", {}).get("failed_checks")
        )
        self.assertGreater(
            elements_with_failures,
            1,
            "the derived document must carry failures in more than one element",
        )
        self.assertEqual(reconcile.count_records("checkov", split), object_count)

    def test_checkov_counts_failed_checks_only(self) -> None:
        """Passes, skips and parsing errors are not findings and never reach the count.

        Two documents, because the rule has two halves and no single document exhibits
        both. ``fixtures/checkov.json`` is the captured artifact and its ``results``
        object carries **only** ``failed_checks`` -- the shape the tool actually wrote
        this run -- so it is what establishes that an *absent* bucket reads as zero
        rather than raising or being counted. ``fixtures/derived-checkov-features.json``
        is the declared-derived document that carries passes, a skip and a parsing
        error, so it is what establishes that a *present* bucket is excluded. Asserting
        the exclusion over the capture alone would be vacuous, which is exactly what the
        guard below refuses to allow.
        """
        captured = load_fixture(CHECKOV_CAPTURED_FIXTURE)
        captured_results = captured["results"]
        self.assertEqual(
            set(captured_results),
            {"failed_checks"},
            "the captured artifact carries only failed checks; if that changes, the "
            "absent-bucket half of this rule needs a document that still exhibits it",
        )
        captured_failed = len(captured_results["failed_checks"])
        self.assertGreater(captured_failed, 0)
        self.assertEqual(
            reconcile.count_records("checkov", captured),
            captured_failed,
            "an absent passed_checks or skipped_checks bucket contributes zero and does "
            "not make the document unreadable",
        )

        document = load_fixture(CHECKOV_FEATURES_FIXTURE)
        results = document["results"]
        failed = len(results["failed_checks"])
        passed = len(results.get("passed_checks") or [])
        skipped = len(results.get("skipped_checks") or [])
        self.assertGreater(passed, 0, "the fixture must carry passes for this to test them")
        self.assertGreater(skipped, 0, "the fixture must carry a skip for this to test it")
        self.assertEqual(reconcile.count_records("checkov", document), failed)
        self.assertNotEqual(
            reconcile.count_records("checkov", document), failed + passed + skipped
        )

        with_parsing_errors = copy.deepcopy(document)
        with_parsing_errors["parsing_errors"] = [
            "resource-managers/kubernetes/docker/src/main/dockerfiles/spark/Dockerfile",
            "python/pyspark/sql/connect/client/core.py",
        ]
        self.assertEqual(
            reconcile.count_records("checkov", with_parsing_errors),
            failed,
            "a parsing error is status evidence for tool-status.md, not a finding",
        )

    def test_trivy_count_unit_spans_the_supported_sections(self) -> None:
        """``Results[]`` times the three supported sections, and nothing else.

        The non-empty unsupported section is a halt, and that halt is
        ``test_trivy_adapter.py``'s to assert; what belongs here is that an unsupported
        section contributes zero to the count whether it is empty or not, since counting
        one would let a dropped section pass reconciliation unnoticed.
        """
        self.assertEqual(
            reconcile.TRIVY_SUPPORTED_SECTIONS,
            ("Vulnerabilities", "Secrets", "Misconfigurations"),
        )

        # The captured artifact first, because it is what the tool wrote: three
        # config-class results holding Misconfigurations and nothing else, with neither
        # unsupported section present at all. It establishes that the unit sums the
        # supported sections it finds and that an ABSENT section contributes zero -- a
        # traversal that indexed a missing key would fail here rather than downstream.
        captured = load_fixture(TRIVY_CAPTURED_FIXTURE)
        captured_results = captured["Results"]
        captured_per_section = {
            section: sum(len(result.get(section) or []) for result in captured_results)
            for section in reconcile.TRIVY_SUPPORTED_SECTIONS
        }
        self.assertEqual(
            reconcile.count_records("trivy", captured),
            sum(captured_per_section.values()),
        )
        self.assertGreater(sum(captured_per_section.values()), 0)
        for section in ("Licenses", "ExperimentalModifiedFindings"):
            with self.subTest(captured_absent_section=section):
                self.assertTrue(
                    all(section not in result for result in captured_results),
                    f"{section} is absent from every element of the captured artifact",
                )

        # The spanning half needs a document that populates more than one section, and
        # the captured artifact populates exactly one. The declared-derived features
        # document populates Vulnerabilities, Secrets and Misconfigurations and carries
        # both unsupported sections present-and-empty, so it is what the rest of this
        # rule is asserted over.
        document = load_fixture(TRIVY_FEATURES_FIXTURE)
        results = document["Results"]
        per_section = {
            section: sum(len(result.get(section) or []) for result in results)
            for section in reconcile.TRIVY_SUPPORTED_SECTIONS
        }
        populated = [section for section, count in per_section.items() if count]
        self.assertGreaterEqual(
            len(populated),
            2,
            "the fixture must carry records in at least two sections for the unit to be "
            "shown to span them",
        )
        self.assertEqual(
            reconcile.count_records("trivy", document), sum(per_section.values())
        )

        unsupported = ("Licenses", "ExperimentalModifiedFindings")
        for section in unsupported:
            with self.subTest(section=section):
                self.assertTrue(
                    all(section in result for result in results),
                    f"the fixture must carry {section} for its emptiness to be counted",
                )
                self.assertEqual(
                    sum(len(result.get(section) or []) for result in results),
                    0,
                    f"{section} is expected present and empty in this fixture",
                )

        without_unsupported = copy.deepcopy(document)
        for result in without_unsupported["Results"]:
            for section in unsupported:
                result.pop(section, None)
        self.assertEqual(
            reconcile.count_records("trivy", without_unsupported),
            reconcile.count_records("trivy", document),
            "an empty unsupported section already contributed zero, so removing it must "
            "change nothing",
        )

        with_unsupported_records = copy.deepcopy(document)
        with_unsupported_records["Results"][0]["Licenses"] = [
            {"Name": "fixture-only license record"}
        ]
        self.assertEqual(
            reconcile.count_records("trivy", with_unsupported_records),
            reconcile.count_records("trivy", document),
            "only the three supported sections are counted; counting an unsupported one "
            "would mask the adapter's halt on it",
        )

    def test_dependency_check_count_unit_is_dependencies_vulnerabilities(self) -> None:
        """``dependencies[].vulnerabilities[]``, with a clean dependency contributing zero."""
        document = load_fixture("dependency-check.json")
        dependencies = document["dependencies"]
        expected = sum(len(entry.get("vulnerabilities") or []) for entry in dependencies)
        self.assertEqual(reconcile.count_records("dependency-check", document), expected)
        self.assertTrue(
            any(not entry.get("vulnerabilities") for entry in dependencies),
            "the fixture must carry a scanned dependency with no vulnerability for the "
            "zero-contribution half of the unit to be exercised",
        )

    def test_joern_count_unit_is_findings_not_the_per_query_counts(self) -> None:
        """``findings[]`` is the unit; the query envelope's own totals are not.

        The envelope records how many results each baked query returned, and those totals
        are far larger than the collected findings. Reading them as the unit would report a
        record count no artifact contains, so the divergence is asserted rather than
        assumed to be absent.
        """
        document = load_fixture("joern.json")
        findings = document["findings"]
        queries = document["queries"]
        self.assertGreater(len(queries), 0)
        per_query_total = 0
        for query in queries:
            self.assertIn("returned", query)
            per_query_total += query["returned"]
        self.assertNotEqual(
            per_query_total,
            len(findings),
            "the fixture's per-query totals must differ from its findings count, or this "
            "assertion cannot distinguish the two readings",
        )
        self.assertEqual(reconcile.count_records("joern", document), len(findings))

    def test_osv_scanner_count_unit_is_defined_without_an_artifact(self) -> None:
        """``results[].packages[].vulnerabilities[]`` -- per package, per source.

        Asserted over a document built in memory, because no OSV artifact exists in this
        tree and none is expected: the tool resolved zero packages and wrote nothing. The
        unit still has to be defined and asserted, so that an artifact appearing in a later
        run is counted rather than met with an untested traversal.
        """
        document = {
            "results": [
                {
                    "source": {"path": "dev/package-lock.json"},
                    "packages": [
                        {"vulnerabilities": [{"id": "GHSA-fixture-1"}, {"id": "GHSA-fix-2"}]},
                        {"vulnerabilities": []},
                        {},
                    ],
                },
                {
                    "source": {"path": "docs/Gemfile.lock"},
                    "packages": [{"vulnerabilities": [{"id": "GHSA-fixture-3"}]}],
                },
                {"source": {"path": "ui-test/package-lock.json"}, "packages": []},
            ]
        }
        self.assertEqual(reconcile.count_records("osv-scanner", document), 3)
        self.assertEqual(reconcile.count_records("osv-scanner", {"results": []}), 0)

    def test_an_unknown_tool_identifier_is_an_error_rather_than_a_zero(self) -> None:
        """Counting an unknown identifier as zero would drop a whole artifact silently."""
        with self.assertRaises(reconcile.UnknownToolError):
            reconcile.count_records("not-one-of-the-nine", {"runs": []})
        self.assertEqual(len(reconcile.CANONICAL_TOOLS), 9)


class MixedArtifactIdentityTest(unittest.TestCase):
    """The identity over the one artifact carrying both rows and rejections.

    AAP 0.6.1 requires this file to assert the identity *"over a fixture containing at least
    one rejection"*, and the reason is arithmetic: over an all-valid document the identity
    collapses to ``raw = rows``, which an implementation that silently dropped a defective
    record satisfies perfectly. ``fixtures/reconcile-mixed.json`` is the document where the
    two readings separate, and its hand-verified expected file is used as the independent
    statement of what it should produce.
    """

    def setUp(self) -> None:
        """Adapt the mixed fixture and load the expected file hand-verified from it."""
        self.adapted = adapt_mixed()
        self.expected = load_expected("reconcile-mixed")

    def test_the_identity_holds_with_rejections_on_the_right_hand_side(self) -> None:
        """``raw finding records = dataset rows + rejected records``, over both outcomes."""
        rows = len(self.adapted.rows)
        rejected = len(self.adapted.rejections)
        self.assertEqual(self.adapted.raw_records, rows + rejected)

        record = reconcile.reconcile_artifact(
            self.adapted.tool,
            self.adapted.raw_records,
            rows,
            rejected,
            rejections_by_class=self.adapted.rejections_by_class,
            reject_classes=paths.REJECT_CLASSES,
        )
        self.assertTrue(record.passed, record.detail)
        self.assertEqual(record.identity, f"{self.adapted.raw_records} = {rows} + {rejected}")
        self.assertEqual(record.status, reconcile.STATUS_PASS)
        self.assertTrue(record.artifact_present)

    def test_the_fixture_keeps_the_identity_non_degenerate(self) -> None:
        """Both sides must be non-zero and unequal, or the assertion proves less than it looks.

        With rows and rejections both positive and different, an implementation that dropped
        the defective records would report ``7 = 4 + 0`` and fail, and one that coerced them
        into rows would report ``7 = 7 + 0`` and fail. If a later edit to the fixture makes
        the two equal, that is a defect in the fixture to report rather than something to
        paper over here.
        """
        rows = len(self.adapted.rows)
        rejected = len(self.adapted.rejections)
        self.assertGreater(rows, 0)
        self.assertGreater(rejected, 0)
        self.assertNotEqual(rows, rejected)

    def test_the_counts_match_the_hand_verified_expected_file(self) -> None:
        """The measured triple equals the one hand-verified from the fixture by inspection."""
        counts = self.expected["counts"]
        self.assertEqual(self.adapted.raw_records, counts["raw_finding_records"])
        self.assertEqual(len(self.adapted.rows), counts["rows"])
        self.assertEqual(len(self.adapted.rejections), counts["rejections"])
        self.assertEqual(self.adapted.rejections_by_class, counts["rejections_by_class"])

    def test_every_rejection_is_counted_under_a_named_class(self) -> None:
        """A named class from ``paths.REJECT_CLASSES``, and the per-class counts add up.

        The second half is what stops a rejection being counted twice or lost between
        classes: if the per-class breakdown and the total ever disagree,
        ``tool-status.md`` would carry two numbers that cannot both be right.
        """
        for rejection in self.adapted.rejections:
            with self.subTest(reject_class=rejection.reject_class):
                self.assertIn(rejection.reject_class, paths.REJECT_CLASSES)
                self.assertTrue(paths.is_reject_class(rejection.reject_class))
                self.assertEqual(rejection.tool, self.adapted.tool)
        self.assertEqual(
            sum(self.adapted.rejections_by_class.values()), len(self.adapted.rejections)
        )
        self.assertGreater(
            len(self.adapted.rejections_by_class),
            1,
            "the fixture must exercise more than one class, or a rejection count could "
            "not be shown to discriminate between conditions",
        )
        # reconcile refuses a class name the caller's vocabulary does not carry, so passing
        # the observed breakdown through it is a second check on every name at once.
        self.assertEqual(
            reconcile.validate_rejection_classes(
                self.adapted.rejections_by_class, paths.REJECT_CLASSES
            ),
            self.adapted.rejections_by_class,
        )

    def test_partial_parse_emits_every_parsable_record_and_keeps_the_rest(self) -> None:
        """Partial parse is an outcome, not a failure -- and all four of its properties hold.

        Every parsable record becomes a row, every rejected record is counted under its
        class, the reason is retained verbatim in the rejection, and the status is
        ``partial`` with both counts carried rather than one of them absorbed.
        """
        disjointness = self.expected["disjointness"]
        rejected_indices = sorted(
            rejection.record_identity["result_index"]
            for rejection in self.adapted.rejections
        )
        self.assertEqual(rejected_indices, list(disjointness["rejected_result_indices"]))

        row_indices = list(disjointness["row_result_indices"])
        self.assertEqual(
            len(row_indices),
            len(self.adapted.rows),
            "the expected file names one surviving record per emitted row",
        )
        self.assertEqual(set(row_indices) & set(rejected_indices), set())
        self.assertEqual(
            sorted(row_indices + rejected_indices),
            list(range(self.adapted.raw_records)),
            "every counted record must appear exactly once as a row or as a rejection",
        )

        # The reason is kept verbatim: the expected file states stable substrings of the
        # retained text, so the assertion checks the reason without pinning whole prose.
        #
        # ``rejections`` is the ordered array the adapters return -- one element per
        # rejected record, in record order -- and the aggregates live beside it under
        # ``rejected_records`` and ``rejections_by_class``, the names
        # ``reconcile.ArtifactReconciliation`` uses for the same two values.  Both are
        # asserted here so the expectation's own shape is checked against production's
        # rather than the other way round.
        expectations = self.expected["rejections"]
        self.assertIsInstance(
            expectations,
            list,
            "the expected rejections are an ordered array, as every adapter returns and "
            "as every other expected file records",
        )
        self.assertEqual(len(expectations), len(self.adapted.rejections))
        self.assertEqual(self.expected["rejected_records"], len(self.adapted.rejections))
        self.assertEqual(
            self.expected["rejections_by_class"], self.adapted.rejections_by_class
        )
        self.assertEqual(
            sum(self.expected["rejections_by_class"].values()),
            self.expected["rejected_records"],
            "the per-class breakdown and the total are one measurement, so they add up",
        )
        self.assertEqual(
            [entry["locator"]["result_index"] for entry in expectations],
            [
                rejection.record_identity["result_index"]
                for rejection in self.adapted.rejections
            ],
            "the array is in the order the adapter produced the rejections",
        )
        for expectation in expectations:
            index = expectation["locator"]["result_index"]
            matching = [
                rejection
                for rejection in self.adapted.rejections
                if rejection.record_identity.get("result_index") == index
            ]
            with self.subTest(result_index=index):
                self.assertEqual(len(matching), 1)
                rejection = matching[0]
                self.assertEqual(rejection.reject_class, expectation["reject_class"])
                self.assertTrue(rejection.detail)
                for fragment in expectation["detail_contains"]:
                    self.assertIn(fragment, rejection.detail)

        self.assertEqual(cli.PARSE_STATUS_PARTIAL, "partial")
        self.assertIn(cli.PARSE_STATUS_PARTIAL, cli.PARSE_STATUSES)
        self.assertEqual(self.expected["arithmetic"]["parse_status"], cli.PARSE_STATUS_PARTIAL)

        record = reconcile.reconcile_artifact(
            self.adapted.tool,
            self.adapted.raw_records,
            len(self.adapted.rows),
            len(self.adapted.rejections),
            rejections_by_class=self.adapted.rejections_by_class,
            reject_classes=paths.REJECT_CLASSES,
        )
        self.assertEqual(record.emitted_rows, len(self.adapted.rows))
        self.assertEqual(record.rejected_records, len(self.adapted.rejections))
        self.assertTrue(
            record.passed,
            "a partially parsed artifact still reconciles: its rejections sit on the "
            "right-hand side rather than making the identity fail",
        )

    def test_dropping_the_rejections_breaks_the_identity(self) -> None:
        """The failure the right-hand side exists to catch is a real failure, not a shrug."""
        record = reconcile.reconcile_artifact(
            self.adapted.tool,
            self.adapted.raw_records,
            len(self.adapted.rows),
            0,
        )
        self.assertFalse(record.passed)
        self.assertEqual(record.status, reconcile.STATUS_FAIL)
        self.assertIn("identity failed", record.detail)


class PerToolAndDatasetIdentityTest(unittest.TestCase):
    """The identity per tool, then the dataset-level sum of those identities (AAP 0.5.4).

    The dataset assertion is the **sum of the per-artifact identities** rather than an
    independent recount, because a global recount can balance while two tools are wrong in
    opposite directions. Counting per tool and adding the counts up is arithmetic; nothing
    here compares one tool with another.
    """

    def setUp(self) -> None:
        """Adapt every committed positive fixture, one artifact per tool."""
        self.adapted = adapt_all()

    def test_the_fixture_table_follows_the_normalizer_artifact_order(self) -> None:
        """One artifact per tool, in the order the normalizer processes them.

        ``reconcile.run_stage_a`` refuses two entries for one tool, so the table has to be
        one per tool; following ``cli.ARTIFACT_ORDER`` keeps the row order of the files this
        test writes the same as the run's.
        """
        self.assertEqual(
            tuple(tool for tool, _, _ in FIXTURE_CASES),
            tuple(tool for tool in cli.ARTIFACT_ORDER if tool != ABSENT_ARTIFACT_TOOL),
        )
        self.assertIn(ABSENT_ARTIFACT_TOOL, cli.ARTIFACT_ORDER)
        self.assertEqual(len(cli.ARTIFACT_ORDER), 9)

    def test_the_identity_holds_for_every_committed_artifact(self) -> None:
        """Per tool: raw finding records = rows emitted + records rejected."""
        for adapted in self.adapted:
            with self.subTest(tool=adapted.tool, fixture=adapted.filename):
                rows = len(adapted.rows)
                rejected = len(adapted.rejections)
                self.assertEqual(adapted.raw_records, rows + rejected)
                record = reconcile.reconcile_artifact(
                    adapted.tool,
                    adapted.raw_records,
                    rows,
                    rejected,
                    rejections_by_class=adapted.rejections_by_class,
                    reject_classes=paths.REJECT_CLASSES,
                )
                self.assertTrue(record.passed, record.detail)
                self.assertEqual(sum(adapted.rejections_by_class.values()), rejected)

    def test_every_measured_count_matches_its_hand_verified_expected_file(self) -> None:
        """Each fixture's raw count is the one its expected file records by inspection."""
        for adapted in self.adapted:
            with self.subTest(tool=adapted.tool, fixture=adapted.filename):
                expected = load_expected(adapted.tool)["counts"]
                self.assertEqual(adapted.raw_records, expected["raw_finding_records"])
                self.assertEqual(
                    adapted.raw_records,
                    expected["rows"] + expected["rejections"],
                    "the expected file's own triple must satisfy the identity it records",
                )

    def test_at_least_one_artifact_contributes_to_each_side(self) -> None:
        """The sweep must exercise both sides, or it only ever tests ``raw = rows``.

        The Joern case is the one that carries rejections here: its resolution runs against
        a deliberately partial synthetic source index, so some of its records resolve to a
        source path and the rest are counted as rejections.
        """
        self.assertGreater(sum(len(entry.rows) for entry in self.adapted), 0)
        self.assertGreater(sum(len(entry.rejections) for entry in self.adapted), 0)
        self.assertTrue(any(entry.rejections for entry in self.adapted))

    def test_the_dataset_identity_is_the_sum_of_the_per_artifact_identities(self) -> None:
        """Stage A per tool, Stage B as their sum, with the absent tool reported as absent."""
        counts = [adapted.artifact_counts for adapted in self.adapted]
        counts.append(reconcile.ArtifactCounts.for_absent_artifact(ABSENT_ARTIFACT_TOOL))

        stage_a = reconcile.run_stage_a(counts, reject_classes=paths.REJECT_CLASSES)
        self.assertEqual(len(stage_a), len(reconcile.CANONICAL_TOOLS))
        by_tool = {record.tool: record for record in stage_a}

        for adapted in self.adapted:
            with self.subTest(tool=adapted.tool):
                record = by_tool[adapted.tool]
                self.assertTrue(record.artifact_present)
                self.assertEqual(record.raw_records, adapted.raw_records)
                self.assertEqual(record.emitted_rows, len(adapted.rows))
                self.assertEqual(record.rejected_records, len(adapted.rejections))
                self.assertTrue(record.passed, record.detail)

        stage_b = reconcile.run_stage_b(stage_a)
        present = [record for record in stage_a if record.artifact_present]
        self.assertEqual(stage_b.raw_records, sum(r.raw_records for r in present))
        self.assertEqual(stage_b.emitted_rows, sum(r.emitted_rows for r in present))
        self.assertEqual(stage_b.rejected_records, sum(r.rejected_records for r in present))
        self.assertEqual(
            stage_b.raw_records, stage_b.emitted_rows + stage_b.rejected_records
        )
        self.assertTrue(stage_b.passed, stage_b.detail)
        self.assertEqual(stage_b.artifacts_total, len(reconcile.CANONICAL_TOOLS))
        self.assertEqual(stage_b.artifacts_present, len(self.adapted))
        self.assertEqual(stage_b.absent_tools, (ABSENT_ARTIFACT_TOOL,))
        self.assertEqual(stage_b.failed_tools, ())

    def test_a_dataset_sum_cannot_hide_two_opposite_per_tool_errors(self) -> None:
        """A balancing sum with a failed per-artifact identity is still a failure.

        Constructed from counts rather than from an artifact: no fixture can be made to
        fail in one direction and another in the other, and the property being asserted is
        Stage B's verdict rather than any adapter's behaviour.
        """
        stage_a = reconcile.run_stage_a(
            [
                {"tool": "opengrep", "raw_records": 10, "emitted_rows": 9, "rejected_records": 0},
                {"tool": "semgrep", "raw_records": 10, "emitted_rows": 11, "rejected_records": 0},
            ]
        )
        stage_b = reconcile.run_stage_b(stage_a)
        self.assertEqual(stage_b.raw_records, stage_b.emitted_rows + stage_b.rejected_records)
        self.assertFalse(
            stage_b.passed,
            "the totals balance, so only the per-artifact verdicts can catch this",
        )
        self.assertEqual(set(stage_b.failed_tools), {"opengrep", "semgrep"})


class OutputFileAgreementTest(unittest.TestCase):
    """Both output files, parsed back and compared -- never counted by line (AAP 0.5.4).

    Both files are written from one validated in-memory row set, then read back from disk.
    The parsed ``findings.json`` array length and the parsed ``findings.csv`` data-row count
    are each compared with the row total separately, because inferring one from the other
    removes the check. Every file this class writes goes into a
    :class:`tempfile.TemporaryDirectory`: the deliverable dataset under
    ``oss-scan-results/`` is never touched.
    """

    def setUp(self) -> None:
        """Build the dataset rows and a temporary directory for the two output files."""
        self.rows = dataset_rows()
        self.assertGreater(len(self.rows), 0)
        self._directory = tempfile.TemporaryDirectory(prefix="blitzy-reconciliation-out-")
        self.addCleanup(self._directory.cleanup)
        self.output = Path(self._directory.name)
        self.json_path = self.output / "findings.json"
        self.csv_path = self.output / "findings.csv"

    def write(self, rows: list) -> list:
        """Write both files from one validated row set and return the validated rows."""
        return emit.write_findings(rows, self.json_path, self.csv_path)

    def test_each_parsed_row_count_is_compared_with_the_row_total_separately(self) -> None:
        """Two assertions, neither derived from the other, then the two files against each other."""
        validated = self.write(self.rows)
        expected_rows = len(validated)
        self.assertEqual(expected_rows, len(self.rows))
        # The row total is the identity's own row side: the rows every artifact emitted.
        self.assertEqual(
            expected_rows, sum(len(adapted.rows) for adapted in adapt_all())
        )

        self.assertEqual(len(parse_json_rows(self.json_path)), expected_rows)
        self.assertEqual(len(parse_csv_rows(self.csv_path)), expected_rows)

        # The same two counts taken by the module's own parsers, which is the route the run
        # itself uses. Both parse; neither counts lines.
        self.assertEqual(reconcile.count_json_rows(self.json_path), expected_rows)
        self.assertEqual(reconcile.count_csv_rows(self.csv_path), expected_rows)

    def test_stage_c_records_the_two_comparisons_and_the_files_against_each_other(self) -> None:
        """Three separate Stage C records, each naming how its count was obtained."""
        validated = self.write(self.rows)
        stage_a = reconcile.run_stage_a(
            [adapted.artifact_counts for adapted in adapt_all()],
            reject_classes=paths.REJECT_CLASSES,
        )
        stage_b = reconcile.run_stage_b(stage_a)
        self.assertEqual(stage_b.emitted_rows, len(validated))

        stage_c = reconcile.run_stage_c(stage_b, self.json_path, self.csv_path)
        self.assertEqual(
            [comparison.name for comparison in stage_c],
            [
                "findings_json_rows_vs_dataset",
                "findings_csv_rows_vs_dataset",
                "findings_json_rows_vs_findings_csv_rows",
            ],
        )
        for comparison in stage_c:
            with self.subTest(comparison=comparison.name):
                self.assertTrue(comparison.passed, comparison.detail)
                self.assertEqual(comparison.status, reconcile.STATUS_PASS)
                self.assertIn(
                    "parsed",
                    comparison.method,
                    "every recorded count must state that it came from parsing a file",
                )

    def test_the_two_files_agree_row_by_row_and_field_by_field(self) -> None:
        """Ordered comparison over ``emit.FIELDS`` after this test's own typed coercion."""
        validated = self.write(self.rows)
        json_rows = parse_json_rows(self.json_path)
        csv_rows = parse_csv_rows(self.csv_path)
        self.assertEqual(len(json_rows), len(csv_rows))

        for index, (json_row, csv_row) in enumerate(zip(json_rows, csv_rows)):
            self.assertEqual(
                tuple(json_row.keys()),
                emit.FIELDS,
                f"row {index} of findings.json is not the twelve fields in order",
            )
            for field in emit.FIELDS:
                with self.subTest(row_index=index, field=field):
                    self.assertEqual(json_row[field], csv_row[field])
                    self.assertEqual(json_row[field], validated[index][field])

        # Corroborated by the module's own typed comparison, which reports the field order
        # it iterated so a reader of normalize-run.json can see it was the twelve-field
        # contract.
        comparison = emit.compare_outputs(self.json_path, self.csv_path)
        self.assertTrue(comparison.passed, comparison.as_dict())
        self.assertEqual(comparison.field_order, emit.FIELDS)
        self.assertEqual(comparison.rows_compared, len(validated))
        self.assertEqual(comparison.fields_compared, len(validated) * len(emit.FIELDS))

    def test_the_coercion_covers_the_integer_the_boolean_and_the_absent_cell(self) -> None:
        """All three coercions are exercised by the dataset, not merely implemented."""
        self.write(self.rows)
        csv_rows = parse_csv_rows(self.csv_path)
        self.assertTrue(any(isinstance(row["start_line"], int) for row in csv_rows))
        self.assertTrue(any(row["start_line"] is None for row in csv_rows))
        self.assertTrue(all(isinstance(row["in_scope"], bool) for row in csv_rows))
        self.assertTrue(any(row["in_scope"] for row in csv_rows))
        self.assertTrue(
            any(
                row[field] is None
                for row in csv_rows
                for field in sorted(emit.OPTIONAL_FIELDS)
            )
        )

    def test_an_embedded_newline_defeats_a_line_count_but_not_a_parse(self) -> None:
        """A row spanning several physical lines, encoded as an assertion.

        A message carrying a newline makes its CSV row span several physical lines. Both
        files still parse to the same number of rows; a line tally does not, and this test
        fails if the comparison is ever taken from one.
        """
        rows = dataset_rows(multiline_message=True)
        validated = self.write(rows)
        row_total = len(validated)
        self.assertEqual(row_total, len(self.rows), "the newline row replaced no row")

        self.assertEqual(len(parse_json_rows(self.json_path)), row_total)
        self.assertEqual(len(parse_csv_rows(self.csv_path)), row_total)

        text = self.csv_path.read_text(encoding="utf-8")
        physical_lines = len(text.splitlines())
        self.assertGreater(
            physical_lines,
            row_total,
            "a row whose message spans lines must make the file longer than its row count",
        )
        naive_data_line_count = physical_lines - 1  # discounting the header line
        self.assertNotEqual(
            naive_data_line_count,
            row_total,
            "a naive line count must be demonstrably wrong here, or this test proves "
            "nothing about how the counts were taken",
        )
        self.assertIn(MULTILINE_MESSAGE, [row["message"] for row in parse_csv_rows(self.csv_path)])

    def test_absence_is_null_in_json_and_an_empty_cell_in_csv(self) -> None:
        """One absence convention, agreeing row for row, and only where absence is permitted."""
        self.write(self.rows)
        json_rows = parse_json_rows(self.json_path)
        with self.csv_path.open("r", encoding="utf-8", newline="") as handle:
            raw_records = [record for record in csv.reader(handle) if record][1:]

        observed_absences = {field: 0 for field in emit.FIELDS}
        for index, (json_row, record) in enumerate(zip(json_rows, raw_records)):
            cells = dict(zip(emit.FIELDS, record))
            for field in emit.FIELDS:
                with self.subTest(row_index=index, field=field):
                    if json_row[field] is None:
                        self.assertIn(
                            field,
                            emit.OPTIONAL_FIELDS,
                            "absence is permitted for five fields only",
                        )
                        self.assertEqual(cells[field], emit.CSV_ABSENT)
                        observed_absences[field] += 1
                    else:
                        self.assertNotEqual(cells[field], emit.CSV_ABSENT)
            self.assertIsNotNone(json_row["severity_norm"])
            self.assertIsNotNone(json_row["path"])
            self.assertIn(cells["in_scope"], (emit.CSV_TRUE, emit.CSV_FALSE))

        for field in sorted(emit.OPTIONAL_FIELDS):
            with self.subTest(field=field):
                self.assertGreater(
                    observed_absences[field],
                    0,
                    "the dataset must exercise this field's absence for the convention to "
                    "be asserted rather than assumed",
                )
        for field in emit.REQUIRED_FIELDS:
            with self.subTest(field=field):
                self.assertEqual(observed_absences[field], 0)

    def test_no_emitted_path_is_absolute_in_either_file(self) -> None:
        """Every path is expressed against the root, in both files, on every row."""
        self.write(self.rows)
        json_rows = parse_json_rows(self.json_path)
        csv_rows = parse_csv_rows(self.csv_path)
        for index, (json_row, csv_row) in enumerate(zip(json_rows, csv_rows)):
            for source, row in (("findings.json", json_row), ("findings.csv", csv_row)):
                with self.subTest(row_index=index, file=source):
                    value = row["path"]
                    self.assertIsInstance(value, str)
                    self.assertTrue(value)
                    self.assertFalse(
                        paths.is_absolute_path(value),
                        f"{source} row {index} carries the absolute path {value!r}",
                    )
                    self.assertFalse(value.startswith("/"))

    def test_nothing_is_deduplicated(self) -> None:
        """Two identical records are two rows, in both files (AAP 0.3.2).

        Deduplication is prohibited, and it is the kind of prohibition an implementation
        breaks helpfully. The duplicate here is an exact copy of a real row, which is the
        hardest case for a writer that might collapse one.
        """
        rows = dataset_rows()
        duplicated = copy.deepcopy(rows[0])
        rows.append(duplicated)
        validated = self.write(rows)
        self.assertEqual(len(validated), len(dataset_rows()) + 1)

        json_rows = parse_json_rows(self.json_path)
        csv_rows = parse_csv_rows(self.csv_path)
        self.assertEqual(len(json_rows), len(validated))
        self.assertEqual(len(csv_rows), len(validated))
        self.assertEqual(json_rows[0], json_rows[-1])
        self.assertEqual(csv_rows[0], csv_rows[-1])


class AbsentArtifactTest(unittest.TestCase):
    """A tool that wrote no artifact reconciles as a sentinel, never as ``0 = 0 + 0``.

    AAP 0.5.4 is explicit: *"For a tool that wrote no artifact the reconciliation is 'not
    applicable - artifact absent', not zero equals zero."*  Zero-equals-zero is a passing
    assertion over a document nobody opened, so it would pass just as well for a tool whose
    artifact was silently lost. The distinction matters because the dataset files are
    row-only: a tool with no row is invisible in them by construction, and
    ``tool-status.md`` is where all nine tools are accounted for.
    """

    def test_the_sentinel_is_the_exact_literal_with_its_em_dash(self) -> None:
        """The string reaches ``tool-status.md`` unaltered, punctuation included."""
        self.assertEqual(
            reconcile.NOT_APPLICABLE_ABSENT, "not applicable \u2014 artifact absent"
        )
        self.assertIn("\u2014", reconcile.NOT_APPLICABLE_ABSENT)
        self.assertNotEqual(reconcile.NOT_APPLICABLE_ABSENT, "0 = 0 + 0")

    def test_an_absent_artifact_carries_no_counts_at_all(self) -> None:
        """Not zeroes: nobody traversed a document that was never written."""
        record = reconcile.reconcile_absent_artifact(ABSENT_ARTIFACT_TOOL)
        self.assertEqual(record.tool, ABSENT_ARTIFACT_TOOL)
        self.assertFalse(record.artifact_present)
        self.assertEqual(record.identity, reconcile.NOT_APPLICABLE_ABSENT)
        self.assertEqual(record.status, reconcile.STATUS_NOT_APPLICABLE)
        self.assertIsNone(record.raw_records)
        self.assertIsNone(record.emitted_rows)
        self.assertIsNone(record.rejected_records)
        self.assertIsNone(
            record.passed,
            "an absent artifact has no identity to pass or fail, and reporting a pass "
            "would make the dataset sum look complete",
        )
        self.assertEqual(dict(record.rejections_by_class), {})

    def test_a_present_empty_artifact_is_a_different_outcome(self) -> None:
        """``0 = 0 + 0`` is a real reconciliation over a document that was traversed.

        Both cases occur in practice, so the record has to tell them apart: an artifact
        holding no record reconciles at zero and passes, while an artifact that does not
        exist has no counts at all.
        """
        empty = reconcile.reconcile_artifact("trivy", 0, 0, 0)
        absent = reconcile.reconcile_absent_artifact(ABSENT_ARTIFACT_TOOL)

        self.assertTrue(empty.artifact_present)
        self.assertEqual(empty.identity, "0 = 0 + 0")
        self.assertTrue(empty.passed)
        self.assertEqual(empty.raw_records, 0)
        self.assertEqual(empty.emitted_rows, 0)
        self.assertEqual(empty.rejected_records, 0)

        self.assertNotEqual(empty.identity, absent.identity)
        self.assertNotEqual(empty.status, absent.status)
        self.assertIsNotNone(empty.raw_records)
        self.assertIsNone(absent.raw_records)

        # The same distinction has to survive serialisation, since normalize-run.json and
        # tool-status.md are rendered from these dicts rather than from the objects.
        self.assertEqual(empty.as_dict()["raw_records"], 0)
        self.assertIsNone(absent.as_dict()["raw_records"])
        self.assertEqual(absent.as_dict()["identity"], reconcile.NOT_APPLICABLE_ABSENT)

    def test_the_sum_alone_cannot_tell_absent_from_a_counted_zero(self) -> None:
        """Which is why the record has to, and does.

        Two datasets that differ only in whether ``osv-scanner`` wrote an empty artifact or
        wrote none at all produce the *same three totals*. If the sum were the only thing
        recorded, a silently lost artifact would be indistinguishable from a tool that
        legitimately had nothing to work on. What separates them is the record: absent
        carries no counts and appears under ``absent_tools``, while a traversed empty
        artifact carries zeroes and appears under ``present_tools``.
        """
        adapted = adapt_all()
        counts = [entry.artifact_counts for entry in adapted]

        absent_stage_a = reconcile.run_stage_a(
            counts + [reconcile.ArtifactCounts.for_absent_artifact(ABSENT_ARTIFACT_TOOL)],
            reject_classes=paths.REJECT_CLASSES,
        )
        empty_stage_a = reconcile.run_stage_a(
            counts
            + [
                reconcile.ArtifactCounts.for_present_artifact(
                    ABSENT_ARTIFACT_TOOL, raw_records=0, emitted_rows=0, rejected_records=0
                )
            ],
            reject_classes=paths.REJECT_CLASSES,
        )
        absent_stage_b = reconcile.run_stage_b(absent_stage_a)
        empty_stage_b = reconcile.run_stage_b(empty_stage_a)

        self.assertEqual(absent_stage_b.raw_records, empty_stage_b.raw_records)
        self.assertEqual(absent_stage_b.emitted_rows, empty_stage_b.emitted_rows)
        self.assertEqual(absent_stage_b.rejected_records, empty_stage_b.rejected_records)
        self.assertEqual(absent_stage_b.identity, empty_stage_b.identity)

        self.assertEqual(absent_stage_b.artifacts_present, len(adapted))
        self.assertEqual(absent_stage_b.artifacts_absent, 1)
        self.assertEqual(absent_stage_b.absent_tools, (ABSENT_ARTIFACT_TOOL,))
        self.assertNotIn(ABSENT_ARTIFACT_TOOL, absent_stage_b.present_tools)

        self.assertEqual(empty_stage_b.artifacts_present, len(adapted) + 1)
        self.assertEqual(empty_stage_b.artifacts_absent, 0)
        self.assertEqual(empty_stage_b.absent_tools, ())
        self.assertIn(ABSENT_ARTIFACT_TOOL, empty_stage_b.present_tools)

        # The per-tool records are where a consumer reads the difference, so they have to
        # carry it too: the same tool, the same totals, and two verdicts that cannot be
        # confused for one another.
        absent_record = next(
            record for record in absent_stage_a if record.tool == ABSENT_ARTIFACT_TOOL
        )
        empty_record = next(
            record for record in empty_stage_a if record.tool == ABSENT_ARTIFACT_TOOL
        )
        self.assertEqual(absent_record.identity, reconcile.NOT_APPLICABLE_ABSENT)
        self.assertEqual(empty_record.identity, "0 = 0 + 0")
        self.assertIsNone(absent_record.raw_records)
        self.assertEqual(empty_record.raw_records, 0)
        self.assertIsNone(absent_record.passed)
        self.assertTrue(empty_record.passed)
        self.assertNotEqual(absent_record.status, empty_record.status)

    def test_every_one_of_the_nine_tools_is_accounted_for(self) -> None:
        """A tool nobody mentioned is reported absent rather than omitted.

        The row-only dataset files cannot show a tool that contributed no row, so Stage A
        carries an entry for all nine whether or not an artifact existed.
        """
        stage_a = reconcile.run_stage_a(
            [{"tool": "opengrep", "raw_records": 3, "emitted_rows": 3, "rejected_records": 0}]
        )
        self.assertEqual(
            tuple(record.tool for record in stage_a), reconcile.CANONICAL_TOOLS
        )
        for record in stage_a:
            if record.tool == "opengrep":
                continue
            with self.subTest(tool=record.tool):
                self.assertFalse(record.artifact_present)
                self.assertEqual(record.identity, reconcile.NOT_APPLICABLE_ABSENT)
                self.assertIsNone(record.raw_records)

    def test_claiming_rows_for_an_artifact_that_does_not_exist_is_an_error(self) -> None:
        """Records cannot come from a document that was never written."""
        with self.assertRaises(reconcile.ReconciliationError):
            reconcile.reconcile_absent_artifact(ABSENT_ARTIFACT_TOOL, emitted_rows=1)
        with self.assertRaises(reconcile.ReconciliationError):
            reconcile.reconcile_absent_artifact(ABSENT_ARTIFACT_TOOL, rejected_records=1)
        with self.assertRaises(ValueError):
            reconcile.run_stage_a(
                [{"tool": ABSENT_ARTIFACT_TOOL, "present": False, "raw_records": 0}]
            )


class ReconciliationRecordContractTest(unittest.TestCase):
    """The record carries what its consumers render, so nothing has to be inferred.

    ``oss-scan-results/tool-status.md`` renders one entry per tool from these records, and
    ``harness/artifacts/logs/normalize-run.json`` carries every reconciliation assertion
    with its result. A consumer left to derive the rejection count from a total, or to guess
    which tools were absent, would be taking a second measurement of something already
    measured -- and AAP 0.6.4 requires a number appearing twice to be one measurement cited
    twice.
    """

    def setUp(self) -> None:
        """Adapt every fixture and write both output files into a temporary directory."""
        self.adapted = adapt_all()
        self._directory = tempfile.TemporaryDirectory(prefix="blitzy-reconciliation-rec-")
        self.addCleanup(self._directory.cleanup)
        output = Path(self._directory.name)
        self.json_path = output / "findings.json"
        self.csv_path = output / "findings.csv"
        self.rows = emit.write_findings(dataset_rows(), self.json_path, self.csv_path)

    def report(self) -> reconcile.ReconciliationReport:
        """Run all three stages over the committed artifacts plus the absent tool."""
        counts = [entry.artifact_counts for entry in self.adapted]
        counts.append(reconcile.ArtifactCounts.for_absent_artifact(ABSENT_ARTIFACT_TOOL))
        return reconcile.run_three_stage_validation(
            counts,
            json_rows=self.json_path,
            csv_rows=self.csv_path,
            reject_classes=paths.REJECT_CLASSES,
        )

    def test_the_three_stage_validation_passes_over_the_committed_artifacts(self) -> None:
        """The whole pipeline in one call: per artifact, dataset, then both output files."""
        report = self.report()
        self.assertTrue(report.passed, report.failures)
        self.assertEqual(report.failures, ())
        self.assertEqual(len(report.stage_a), len(reconcile.CANONICAL_TOOLS))
        self.assertEqual(len(report.stage_c), 3)
        self.assertEqual(report.stage_b.emitted_rows, len(self.rows))

    def test_the_per_artifact_record_exposes_every_number_its_consumers_need(self) -> None:
        """The raw count, the row count, the rejection count and its per-class breakdown."""
        mixed = adapt_mixed()
        record = reconcile.reconcile_artifact(
            mixed.tool,
            mixed.raw_records,
            len(mixed.rows),
            len(mixed.rejections),
            rejections_by_class=mixed.rejections_by_class,
            reject_classes=paths.REJECT_CLASSES,
        )
        serialised = record.as_dict()
        for key in (
            "tool",
            "artifact_present",
            "raw_records",
            "emitted_rows",
            "rejected_records",
            "rejections_by_class",
            "identity",
            "passed",
            "status",
            "detail",
        ):
            with self.subTest(key=key):
                self.assertIn(key, serialised)
        self.assertEqual(serialised["raw_records"], mixed.raw_records)
        self.assertEqual(serialised["emitted_rows"], len(mixed.rows))
        self.assertEqual(serialised["rejected_records"], len(mixed.rejections))
        self.assertEqual(serialised["rejections_by_class"], mixed.rejections_by_class)
        self.assertEqual(
            sum(serialised["rejections_by_class"].values()), serialised["rejected_records"]
        )
        self.assertTrue(serialised["detail"])

    def test_the_report_is_serialisable_and_keeps_the_sentinel(self) -> None:
        """Every assertion reaches the run record, including the not-applicable verdict."""
        report = self.report()
        serialised = report.as_dict()
        self.assertEqual(set(serialised), {"passed", "failures", "stage_a", "stage_b", "stage_c"})
        # Serialisable with the standard library alone: the run record is written as JSON.
        json.dumps(serialised)

        absent = report.for_tool(ABSENT_ARTIFACT_TOOL)
        self.assertEqual(absent.identity, reconcile.NOT_APPLICABLE_ABSENT)
        self.assertIsNone(absent.raw_records)

        tools_in_record = [entry["tool"] for entry in serialised["stage_a"]["artifacts"]]
        self.assertEqual(tuple(tools_in_record), reconcile.CANONICAL_TOOLS)
        self.assertEqual(
            serialised["stage_b"]["dataset"]["absent_tools"], [ABSENT_ARTIFACT_TOOL]
        )

    def test_for_tool_returns_the_one_measurement_and_refuses_an_unknown_tool(self) -> None:
        """One measurement cited twice, never two measurements."""
        report = self.report()
        for adapted in self.adapted:
            with self.subTest(tool=adapted.tool):
                record = report.for_tool(adapted.tool)
                self.assertEqual(record.raw_records, adapted.raw_records)
                self.assertEqual(record.emitted_rows, len(adapted.rows))
                self.assertEqual(record.rejected_records, len(adapted.rejections))
        with self.assertRaises(reconcile.UnknownToolError):
            report.for_tool("not-one-of-the-nine")

    def test_a_failed_identity_is_raised_rather_than_returned_quietly(self) -> None:
        """A failed reconciliation halts the run, and the report survives the halt.

        ``raise_on_failure=False`` is how ``cli.py`` gets the report first, so the evidence
        is durable before the run stops; the error then carries that same report.
        """
        counts = [
            {"tool": "opengrep", "raw_records": 5, "emitted_rows": 4, "rejected_records": 0}
        ]
        report = reconcile.run_three_stage_validation(
            counts,
            json_rows=self.json_path,
            csv_rows=self.csv_path,
            raise_on_failure=False,
        )
        self.assertFalse(report.passed)
        self.assertTrue(report.failures)
        json.dumps(report.as_dict())
        with self.assertRaises(reconcile.ReconciliationError) as caught:
            report.raise_for_failures()
        self.assertIs(caught.exception.report, report)


# --------------------------------------------------------------------------------------
# The composer: harness/lib/normalize/cli.py
#
# Everything below drives cli.py in process over a temporary workspace. The workspace is
# a complete, hermetic input set -- a scan root with a handful of real source files, the
# twelve authoritative globs, a runner-metadata document, a raw tree holding copies of the
# committed fixtures under their canonical artifact filenames, and a log tree carrying the
# stated reasons the absent tools need -- so the normalizer runs exactly as it does in the
# harness, with every path inside a directory that is deleted when the test ends.
# --------------------------------------------------------------------------------------


#: The eight artifacts a full workspace holds, keyed by canonical tool identifier and
#: derived from the fixture table above so the two cannot drift.
CLI_PRESENT_ARTIFACTS = {tool: filename for tool, filename, _ in FIXTURE_CASES}

#: What an absent tool's own output says, in the shape AAP 0.5.4 requires the absence be
#: classified from -- the tool's own words and nothing else. This is OSV-Scanner's
#: documented sentence for a scope holding zero resolvable dependency manifests; it is
#: written into a temporary log tree so the absent branch has something to quote, and it
#: carries no secret and no scanner's real artifact.
CLI_STATED_REASON = "No package sources found, --help for usage information.\n"

#: The keys ``cli.ArtifactOutcome.as_dict`` promises its consumers, in order.
#: ``tool-status.md`` and ``severity-map.md`` are rendered from these, so a key that
#: vanished would silently empty a column of a published document.
CLI_OUTCOME_KEYS = (
    "tool",
    "scanner_class",
    "artifact_filename",
    "artifact_expected",
    "present",
    "parse_status",
    "artifact",
    "routing",
    "raw_records",
    "emitted_rows",
    "rejected_records",
    "rejections_by_class",
    "rejections",
    "counters",
    "counter_summary",
    "path_kinds",
    "runner_status",
    "network_fetch",
    "tool_words",
    "extras",
    "notes",
)


class CliWorkspace:
    """One hermetic normalizer input set inside a temporary directory.

    Built from the committed fixtures and from ``paths.py``'s own loaders: the allowlist
    and the runner metadata are written and then read back through
    :func:`normalize.paths.load_allowlist` and
    :func:`normalize.paths.load_runner_metadata`, never bypassed, so a document this
    class writes is a document the production code accepts.

    Attributes:
        directory: The temporary directory holding everything created here.
        root: The absolute scan root every emitted path is expressed against.
        raw_dir: The runner-only artifact tree, holding one copy per present fixture.
        log_dir: The per-tool stream and status tree.
        out_dir: Where the dataset and the run record are written. Created by the
            normalizer rather than here, so a test can assert it does not exist after a
            pre-write halt.
    """

    def __init__(
        self,
        directory: Path,
        present: "dict[str, str] | None" = None,
        *,
        with_sources: bool = True,
        stated_reasons: bool = True,
    ) -> None:
        """Create the workspace, copying each named fixture into the raw tree."""
        self.directory = directory
        self.present = (
            dict(CLI_PRESENT_ARTIFACTS) if present is None else dict(present)
        )
        self.environment = Environment(directory)
        self.root = Path(self.environment.root)
        self.allowlist_path = self.environment.allowlist_path
        self.metadata_path = self.environment.metadata_path
        self.raw_dir = directory / "raw"
        self.log_dir = directory / "logs"
        self.out_dir = directory / "out"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        if with_sources:
            # Real files, at the real relative locations the synthetic Joern index above
            # names, so cli.py's own source index -- built by walking this root -- can
            # resolve some of the fixture's classes and not others. An index over
            # everything would leave the rejection side of the identity unexercised.
            for candidates in SYNTHETIC_JOERN_INDEX_BY_FILENAME.values():
                for relative in candidates:
                    target = self.root / relative
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text(
                        f"// {relative}: an inert placeholder file. This tree exists so "
                        "the class-to-source index has something to resolve against.\n",
                        encoding="utf-8",
                    )

        for tool, filename in self.present.items():
            source = FIXTURES_DIR / filename
            if not source.is_file():
                raise AssertionError(f"required fixture {filename!r} is absent")
            shutil.copyfile(source, self.raw_dir / shape.artifact_filename_for(tool))

        if stated_reasons:
            for tool in shape.CANONICAL_TOOLS:
                if tool not in self.present:
                    self.state_reason(tool)

    @property
    def findings_json(self) -> Path:
        """Where the dataset's JSON half is written."""
        return self.out_dir / "findings.json"

    @property
    def findings_csv(self) -> Path:
        """Where the dataset's CSV half is written."""
        return self.out_dir / "findings.csv"

    @property
    def run_record_path(self) -> Path:
        """Where ``normalize-run.json`` is written -- on every path out, including a halt.

        Inside the log tree rather than beside the dataset, because that is the root
        ``cli.py`` holds the run record to: the dataset files belong to the repository root
        the run declares and the record belongs to ``--log-dir``, and a path outside its
        owner is refused as a fault in the invocation. A workspace that wrote it beside
        ``findings.json`` would be exercising an invocation no real run can make.
        """
        return self.log_dir / "normalize-run.json"

    def state_reason(self, tool: str, text: str = CLI_STATED_REASON) -> Path:
        """Write one tool's own words to its stderr log, and return that path."""
        path = self.log_dir / f"{tool}.stderr.log"
        path.write_text(text, encoding="utf-8")
        return path

    def write_status(self, tool: str, /, **fields: object) -> Path:
        """Write one ``<tool>.status`` file in the key=value shape ``scope_finish`` uses.

        ``tool`` is positional-only so a status *field* may legitimately be called
        ``tool`` -- which it is, because that is the first line ``scope_finish`` writes.
        """
        path = self.log_dir / f"{tool}.status"
        path.write_text(
            "".join(f"{key}={value}\n" for key, value in fields.items()),
            encoding="utf-8",
        )
        return path

    @property
    def env(self) -> "dict[str, str]":
        """The environment ``harness/env.sh`` would have exported for this workspace.

        ``HARNESS_REPO_ROOT`` is the declaration ``cli.py`` resolves the dataset's owner
        root from. Without it the owner is the repository this module is installed in, and
        every dataset path a temporary workspace names is then correctly refused as being
        outside it. Declaring the root here is what a real run does; the alternative --
        relaxing the containment check -- would remove the guard the check exists for.
        """
        return {"HARNESS_REPO_ROOT": str(self.directory)}

    def argv(self, **overrides: object) -> list:
        """Return the full argument list, with any input overridden by keyword."""
        values = {
            "raw_dir": self.raw_dir,
            "runner_metadata": self.metadata_path,
            "allowlist": self.allowlist_path,
            "log_dir": self.log_dir,
            "spark_src": self.root,
            "findings_json": self.findings_json,
            "findings_csv": self.findings_csv,
            "run_record": self.run_record_path,
        }
        values.update(overrides)
        arguments: list = []
        for destination, value in values.items():
            arguments.extend([f"--{destination.replace('_', '-')}", str(value)])
        return arguments

    def inputs(self, **overrides: object) -> cli.Inputs:
        """Resolve the workspace through the real parser and ``cli.resolve_inputs``.

        Routed through the production resolution rather than constructed directly, so a
        stage test is handed the same object the composition hands it -- an
        ``Inputs`` built by hand could carry a relative path no real run could produce.
        """
        namespace = cli.build_parser().parse_args(self.argv(**overrides))
        return cli.resolve_inputs(namespace, self.env)

    def metadata(self):
        """Return the runner metadata as ``paths.load_runner_metadata`` returns it."""
        return self.environment.metadata

    def run(self, argv: "list | None" = None) -> tuple:
        """Call ``cli.main`` in process, capturing both streams.

        Returns:
            The exit code, the captured stdout and the captured stderr. The streams are
            captured rather than silenced because the diagnostics are part of the
            contract: a halt has to name its fault on stderr as well as in the record.
        """
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            with unittest.mock.patch.dict(os.environ, self.env):
                code = cli.main(self.argv() if argv is None else argv)
        return code, stdout.getvalue(), stderr.getvalue()

    def record(self) -> dict:
        """Parse the run record the normalizer wrote."""
        return json.loads(self.run_record_path.read_text(encoding="utf-8"))


class CliTestCase(unittest.TestCase):
    """Workspace construction and halt assertions shared by every ``Cli*`` class.

    The halt helper asserts four things at once, because each on its own is satisfied by
    the wrong halt: the exception class decides the exit code a caller sees, the reason
    is what ``normalize-run.json`` publishes from a closed set, the exit code is what the
    shell acts on, and the details are what a reader diagnoses from without re-reading
    the artifact.
    """

    def workspace(self, **keywords) -> CliWorkspace:
        """Return a fresh workspace, released when the test ends."""
        handle = tempfile.TemporaryDirectory(prefix="blitzy-cli-reconcile-")
        self.addCleanup(handle.cleanup)
        return CliWorkspace(Path(handle.name), **keywords)

    @contextlib.contextmanager
    def temporary_directory(self):
        """Yield a private directory for a case that needs no whole workspace.

        Every path any of these tests writes lives under one of these: the repository is
        never written to, and nothing outside a temporary tree is created.
        """
        with tempfile.TemporaryDirectory(prefix="blitzy-cli-reconcile-dir-") as name:
            yield Path(name)

    def assertHalt(self, halt, *, reason: str, exit_code: int, expected_class=None):
        """Assert a halt's class, reason, exit code and serialisable record together."""
        expected_class = expected_class or cli.NormalizeHalt
        self.assertIsInstance(halt, expected_class)
        self.assertIn(
            halt.reason,
            cli.HALT_REASONS,
            msg=(
                f"halt reason {halt.reason!r} is outside the closed set the run record "
                "draws from, so a reader could not enumerate it"
            ),
        )
        self.assertEqual(halt.reason, reason)
        self.assertEqual(halt.exit_code, exit_code)
        self.assertTrue(halt.message.strip(), msg="a halt must carry a diagnostic")
        serialised = halt.as_dict()
        self.assertEqual(serialised["reason"], reason)
        self.assertEqual(serialised["exit_code"], exit_code)
        json.dumps(serialised)
        return halt

    def outcome_for(self, workspace: CliWorkspace, tool: str) -> cli.ArtifactOutcome:
        """Build the outcome object the composition builds before processing one artifact."""
        filename = shape.artifact_filename_for(tool)
        return cli.ArtifactOutcome(
            tool=tool,
            scanner_class=cli._scanner_class_label(tool),
            artifact_filename=filename,
            present=tool in workspace.present,
            parse_status=cli.PARSE_STATUS_FAILED,
            artifact=cli._file_record(workspace.raw_dir / filename),
        )

    def process_present(
        self,
        workspace: CliWorkspace,
        tool: str,
        *,
        artifact_path: "Path | None" = None,
        metadata=None,
        root: "str | None" = None,
        globs=None,
        source_index=None,
        outcome: "cli.ArtifactOutcome | None" = None,
    ):
        """Call ``cli._process_present_artifact`` with the workspace's own inputs."""
        outcome = outcome if outcome is not None else self.outcome_for(workspace, tool)
        rows, counts = cli._process_present_artifact(
            tool,
            artifact_path
            if artifact_path is not None
            else workspace.raw_dir / shape.artifact_filename_for(tool),
            metadata=workspace.metadata() if metadata is None else metadata,
            root=str(workspace.root) if root is None else root,
            globs=paths.ALLOWLIST_GLOBS if globs is None else globs,
            tally=severity.LiteralTally.with_all_tools(),
            source_index=source_index,
            log_dir=workspace.log_dir,
            outcome=outcome,
        )
        return rows, counts, outcome


class CliStageInputTests(CliTestCase):
    """The four stages that settle the composition's inputs before an artifact is read.

    Each one guards a fault whose consequence is a dataset that looks clean and is wrong.
    Unusable runner metadata leaves every tool's path base unknown, and a base defaulted
    to the root would make one tool's paths wrong in the same direction throughout. A scan
    root that is not the root the runners used would corrupt every path in the dataset. An
    allowlist that has drifted decides the ``in_scope`` field of every row by a policy the
    request did not specify. And a class-to-source index over zero files would reject every
    Joern record for a reason that has nothing to do with the artifact.
    """

    def test_the_runner_metadata_is_read_as_input_and_recorded_with_its_direction(self) -> None:
        """Stage 1 writes it, the normalizer reads it, and no Markdown is read at all.

        The recorded direction is the guard against the circularity AAP 0.6.4 forbids:
        ``tool-status.md`` is rendered from this metadata joined with the run's results, so
        the composer reading that Markdown back would make the pipeline its own input.
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        record: dict = {}
        document = cli._load_metadata(workspace.inputs(), record)

        self.assertEqual(paths.metadata_scan_root(document), str(workspace.root))
        entry = record["runner_metadata"]
        self.assertTrue(entry["file"]["present"])
        self.assertEqual(
            entry["file"]["bytes"], workspace.metadata_path.stat().st_size
        )
        self.assertEqual(entry["spark_src"], str(workspace.root))
        self.assertEqual(list(entry["tools_recorded"]), list(RECORDED_PATH_BASES))
        self.assertEqual(entry["tools_missing_from_metadata"], [])
        self.assertEqual(
            entry["smoke_override_live_environment"],
            "set" if os.environ.get("HARNESS_SMOKE_TARGET") else "absent",
        )
        self.assertIn("never reads tool-status.md", entry["direction"])
        json.dumps(record)

    def test_a_tool_missing_from_the_metadata_is_recorded_rather_than_assumed(self) -> None:
        """A gap in the metadata is named before any path is resolved against it."""
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        document = json.loads(workspace.metadata_path.read_text(encoding="utf-8"))
        del document["tools"]["joern"]
        trimmed = workspace.directory / "metadata-without-joern.json"
        trimmed.write_text(json.dumps(document), encoding="utf-8")

        record: dict = {}
        cli._load_metadata(workspace.inputs(runner_metadata=trimmed), record)
        self.assertEqual(
            record["runner_metadata"]["tools_missing_from_metadata"], ["joern"]
        )

    def test_unreadable_or_unusable_runner_metadata_is_a_configuration_fault(self) -> None:
        """Both routes to an unusable input exit 78, and both name the file.

        The absent file is an ``OSError`` and the wrong-shaped document is a
        ``paths.RunnerMetadataError``; they are separate branches and either one silently
        skipped would leave the other appearing to cover it.
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        absent = workspace.directory / "no-such-metadata.json"
        wrong_shape = workspace.directory / "metadata-as-an-array.json"
        wrong_shape.write_text(json.dumps([{"tools": {}}]), encoding="utf-8")

        for label, candidate in (("absent", absent), ("wrong shape", wrong_shape)):
            with self.subTest(metadata=label):
                with self.assertRaises(cli.ConfigurationFault) as raised:
                    cli._load_metadata(
                        workspace.inputs(runner_metadata=candidate), {}
                    )
                fault = self.assertHalt(
                    raised.exception,
                    reason=cli.HALT_RUNNER_METADATA,
                    exit_code=cli.EXIT_CONFIG,
                    expected_class=cli.ConfigurationFault,
                )
                self.assertEqual(fault.details["runner_metadata"], str(candidate))
                self.assertTrue(fault.details["error"])

    def test_the_scan_root_is_the_argument_checked_against_the_recorded_root(self) -> None:
        """It agrees, and the record says so with both values and the root's own state."""
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        record: dict = {}
        root = cli._resolve_scan_root(
            workspace.inputs(), workspace.metadata(), record
        )
        self.assertEqual(root, str(workspace.root))
        entry = record["scan_root"]
        self.assertEqual(entry["argument"], str(workspace.root))
        self.assertEqual(entry["runner_metadata"], str(workspace.root))
        self.assertIs(entry["agree"], True)
        self.assertIs(entry["exists"], True)
        self.assertIs(entry["is_directory"], True)
        self.assertIn("no absolute path is ever emitted", entry["note"])

    def test_a_symlinked_clone_is_the_same_root_rather_than_another_tree(self) -> None:
        """Two names for one directory agree; a different directory does not.

        The comparison is by normalised string *and* by real path, so a clone reached
        through a symlink -- which is how the provisioned graph path is reached -- is not
        mistaken for a runner that scanned somewhere else. Asserted together with the
        negative case, because a comparison that returned ``True`` for everything would
        satisfy the first half alone.
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        link = workspace.directory / "spark-src-by-another-name"
        os.symlink(workspace.root, link)

        record: dict = {}
        root = cli._resolve_scan_root(
            workspace.inputs(spark_src=link), workspace.metadata(), record
        )
        self.assertEqual(root, str(link))
        self.assertIs(record["scan_root"]["agree"], True)
        self.assertTrue(cli._same_root(str(link), str(workspace.root)))
        self.assertTrue(cli._same_root(str(workspace.root) + "/", str(workspace.root)))
        self.assertFalse(
            cli._same_root(str(workspace.directory / "elsewhere"), str(workspace.root))
        )

    def test_a_relative_scan_root_halts_before_any_path_is_resolved(self) -> None:
        """A relative root would make every row wrong in the same direction.

        Built by replacing the field on the resolved inputs, because
        ``cli.resolve_inputs`` absolutises every value and therefore cannot produce this
        state -- which is the point: the guard is defence in depth behind that resolution,
        and a caller constructing ``Inputs`` directly is the case it protects.
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        relative = dataclasses.replace(workspace.inputs(), spark_src="relative/spark-src")
        with self.assertRaises(cli.ConfigurationFault) as raised:
            cli._resolve_scan_root(relative, workspace.metadata(), {})
        fault = self.assertHalt(
            raised.exception,
            reason=cli.HALT_SCAN_ROOT_NOT_ABSOLUTE,
            exit_code=cli.EXIT_CONFIG,
            expected_class=cli.ConfigurationFault,
        )
        self.assertEqual(fault.details["scan_root"], "relative/spark-src")

    def test_a_scan_root_that_contradicts_the_metadata_halts_with_both_values(self) -> None:
        """Resolving against a root the runners did not use corrupts every path."""
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        other = workspace.directory / "another-clone"
        other.mkdir()
        with self.assertRaises(cli.ConfigurationFault) as raised:
            cli._resolve_scan_root(
                workspace.inputs(spark_src=other), workspace.metadata(), {}
            )
        fault = self.assertHalt(
            raised.exception,
            reason=cli.HALT_SCAN_ROOT_DISAGREEMENT,
            exit_code=cli.EXIT_CONFIG,
            expected_class=cli.ConfigurationFault,
        )
        self.assertEqual(fault.details["argument"], str(other))
        self.assertEqual(fault.details["runner_metadata"], str(workspace.root))

    def test_metadata_recording_no_scan_root_is_a_configuration_fault(self) -> None:
        """The root has two witnesses, and the metadata's is not optional."""
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        document = json.loads(workspace.metadata_path.read_text(encoding="utf-8"))
        del document["spark_src"]
        rootless = workspace.directory / "metadata-without-a-root.json"
        rootless.write_text(json.dumps(document), encoding="utf-8")

        with self.assertRaises(cli.ConfigurationFault) as raised:
            cli._resolve_scan_root(
                workspace.inputs(),
                paths.load_runner_metadata(rootless),
                {},
            )
        self.assertHalt(
            raised.exception,
            reason=cli.HALT_RUNNER_METADATA,
            exit_code=cli.EXIT_CONFIG,
            expected_class=cli.ConfigurationFault,
        )

    def test_the_allowlist_must_be_the_twelve_authoritative_globs(self) -> None:
        """It is read, checked, recorded with its expansion, and never rewritten."""
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        before = workspace.allowlist_path.read_bytes()
        record: dict = {}
        globs = cli._load_globs(workspace.inputs(), str(workspace.root), record)

        self.assertEqual(globs, tuple(AUTHORITATIVE_GLOBS))
        entry = record["allowlist"]
        self.assertIs(entry["matches_authoritative_globs"], True)
        self.assertEqual(entry["glob_count"], 12)
        self.assertEqual(entry["authoritative_globs"], list(paths.ALLOWLIST_GLOBS))
        self.assertIn("no licence to change the file", entry["consumers"])
        self.assertIn("in_scope: false and is kept", entry["in_scope_policy"])
        self.assertEqual(
            entry["expansion"]["expected_directory_count"],
            paths.PINNED_EXPANSION_DIRECTORIES,
        )
        self.assertEqual(
            entry["expansion"]["directory_count"],
            len(entry["expansion"]["directories"]),
        )
        self.assertEqual(
            workspace.allowlist_path.read_bytes(),
            before,
            msg="the allowlist is the authority and is never rewritten here",
        )

    def test_an_allowlist_that_has_drifted_halts_with_both_glob_lists(self) -> None:
        """A mis-scoped dataset reads exactly like a clean one, so drift stops the run."""
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        workspace.allowlist_path.write_text(
            "".join(f"{glob}\n" for glob in AUTHORITATIVE_GLOBS[:-1]), encoding="utf-8"
        )
        with self.assertRaises(cli.ConfigurationFault) as raised:
            cli._load_globs(workspace.inputs(), str(workspace.root), {})
        fault = self.assertHalt(
            raised.exception,
            reason=cli.HALT_ALLOWLIST_NOT_AUTHORITATIVE,
            exit_code=cli.EXIT_CONFIG,
            expected_class=cli.ConfigurationFault,
        )
        self.assertEqual(fault.details["observed"], list(AUTHORITATIVE_GLOBS[:-1]))
        self.assertEqual(fault.details["authoritative"], list(paths.ALLOWLIST_GLOBS))

    def test_an_absent_or_empty_allowlist_is_a_configuration_fault(self) -> None:
        """Two routes to no usable scope, both exit 78 and both name the file."""
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        absent = workspace.directory / "no-such-allowlist.txt"
        empty = workspace.directory / "empty-allowlist.txt"
        empty.write_text("", encoding="utf-8")

        for label, candidate in (("absent", absent), ("empty", empty)):
            with self.subTest(allowlist=label):
                with self.assertRaises(cli.ConfigurationFault) as raised:
                    cli._load_globs(
                        workspace.inputs(allowlist=candidate), str(workspace.root), {}
                    )
                fault = self.assertHalt(
                    raised.exception,
                    reason=cli.HALT_ALLOWLIST_UNREADABLE,
                    exit_code=cli.EXIT_CONFIG,
                    expected_class=cli.ConfigurationFault,
                )
                self.assertEqual(fault.details["allowlist"], str(candidate))

    def test_an_unmeasurable_expansion_is_recorded_rather_than_assumed(self) -> None:
        """Where the root is no directory on this host, the expansion says it was not taken.

        The weaker statement on purpose: an expansion recorded as empty would read as
        twelve globs matching nothing, which is a measurement nobody made.
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        record: dict = {}
        cli._load_globs(
            workspace.inputs(), str(workspace.directory / "not-a-directory"), record
        )
        expansion = record["allowlist"]["expansion"]
        self.assertIsNone(expansion["directories"])
        self.assertIn("Recorded rather than assumed", expansion["note"])

    def test_the_source_index_is_built_only_where_joern_wrote_an_artifact(self) -> None:
        """No Joern artifact, no index -- and the record says why rather than being silent."""
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        record: dict = {}
        index = cli._build_source_index(
            str(workspace.root), {"gitleaks": workspace.raw_dir / "gitleaks.json"}, record
        )
        self.assertIsNone(index)
        self.assertIs(record["source_index"]["built"], False)
        self.assertIn("joern wrote no artifact", record["source_index"]["reason"])

    def test_the_source_index_spans_both_source_trees_when_joern_wrote_one(self) -> None:
        """One index per run, over ``src/main`` and ``src/test``, with its statistics recorded.

        Both trees, because every ``-tests`` artifact the build produced is in the graph
        input: a Joern finding can legitimately name bytecode compiled from a test tree,
        and that row is retained with ``in_scope: false`` rather than dropped.
        """
        workspace = self.workspace(present={"joern": "joern.json"})
        record: dict = {}
        index = cli._build_source_index(
            str(workspace.root), {"joern": workspace.raw_dir / "joern.json"}, record
        )
        self.assertIsInstance(index, paths.SourceIndex)
        entry = record["source_index"]
        self.assertIs(entry["built"], True)
        self.assertEqual(entry["root"], str(workspace.root))
        self.assertEqual(entry["files_indexed"], index.statistics()["files_indexed"])
        self.assertGreater(entry["files_indexed"], 0)
        self.assertEqual(tuple(index.trees_indexed), paths.SOURCE_TREES)

    def test_a_source_index_over_no_files_is_a_configuration_fault(self) -> None:
        """An empty index would reject every Joern record for the wrong reason."""
        workspace = self.workspace(
            present={"joern": "joern.json"}, with_sources=False
        )
        with self.assertRaises(cli.ConfigurationFault) as raised:
            cli._build_source_index(
                str(workspace.root), {"joern": workspace.raw_dir / "joern.json"}, {}
            )
        fault = self.assertHalt(
            raised.exception,
            reason=cli.HALT_SOURCE_INDEX_EMPTY,
            exit_code=cli.EXIT_CONFIG,
            expected_class=cli.ConfigurationFault,
        )
        self.assertEqual(fault.details["root"], str(workspace.root))
        self.assertEqual(fault.details["statistics"]["files_indexed"], 0)


class CliRunnerEvidenceTests(CliTestCase):
    """The runner's own side records, read as evidence and never re-derived.

    ``<tool>.status`` and the two stream logs are the runner's account of what it did, and
    the composer's classifications depend on them: whether an absence is a completion or a
    failure is settled *only* by the tool's own words, and whether a process ended without
    an exit code is a status name rather than an excuse. Each helper below turns one of
    those files into a record field, and each has a failure mode that would quietly
    strengthen a claim nobody measured.
    """

    def test_a_status_file_yields_the_exit_code_as_a_fact(self) -> None:
        """The fields ``scope_finish`` writes, parsed and carried verbatim."""
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        workspace.write_status(
            "gitleaks",
            tool="gitleaks",
            exit_code=2,
            elapsed_seconds=41,
            artifact="harness/artifacts/raw/gitleaks.json",
            artifact_bytes=561,
            scan_root=workspace.root,
            scan_root_source="SPARK_SRC",
        )
        status = cli._runner_status(workspace.log_dir, "gitleaks")
        self.assertIs(status["present"], True)
        self.assertEqual(status["exit_code"], 2)
        self.assertEqual(status["exit_code_literal"], "2")
        self.assertEqual(status["exit_status"], cli.EXIT_STATUS_EXITED)
        self.assertEqual(status["elapsed_seconds"], 41)
        self.assertEqual(status["artifact_bytes_literal"], "561")
        self.assertEqual(status["scan_root"], str(workspace.root))
        self.assertEqual(status["scan_root_source"], "SPARK_SRC")
        self.assertEqual(status["fields"]["tool"], "gitleaks")

    def test_a_status_file_with_no_readable_code_is_a_timeout_not_an_unrecorded_one(self) -> None:
        """Three distinct states, and the two failure states are different facts.

        A status file carrying no readable code is a process that ended without one --
        ``timeout``, the single name AAP 0.8.1 gives it. No status file at all is
        ``unrecorded``, which is the absence of the evidence rather than a statement about
        the process. Collapsing the two would let a missing file read as a timeout.
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        unrecorded = cli._runner_status(workspace.log_dir, "gitleaks")
        self.assertIs(unrecorded["present"], False)
        self.assertEqual(unrecorded["exit_status"], cli.EXIT_STATUS_UNRECORDED)
        self.assertIsNone(unrecorded["exit_code"])

        workspace.write_status("gitleaks", tool="gitleaks", exit_code="")
        timed_out = cli._runner_status(workspace.log_dir, "gitleaks")
        self.assertEqual(timed_out["exit_status"], cli.EXIT_STATUS_TIMEOUT)
        self.assertIsNone(timed_out["exit_code"])
        self.assertEqual(timed_out["exit_code_literal"], "")

        without_a_log_tree = cli._runner_status(None, "gitleaks")
        self.assertIsNone(without_a_log_tree["path"])
        self.assertEqual(
            without_a_log_tree["exit_status"], cli.EXIT_STATUS_UNRECORDED
        )

    def test_the_tools_own_words_are_read_only_where_the_verdict_depends_on_them(self) -> None:
        """Stderr is preferred, stdout is the fallback, and neither is read unasked.

        ``with_text=False`` describes both streams without reading either: for a present
        artifact the classification does not depend on their content, and one of them can
        run to hundreds of megabytes. The recorded ``stated_reason_present`` must then be
        false even though a log exists, because nothing was read.
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        (workspace.log_dir / "gitleaks.stderr.log").write_text(
            "the words on stderr\n", encoding="utf-8"
        )
        (workspace.log_dir / "gitleaks.stdout.log").write_text(
            "the words on stdout\n", encoding="utf-8"
        )

        described = cli._tool_words(workspace.log_dir, "gitleaks", with_text=False)
        self.assertIs(described["streams"]["stderr"]["present"], True)
        self.assertIsNone(described["streams"]["stderr"]["text"])
        self.assertIs(described["stated_reason_present"], False)

        read = cli._tool_words(workspace.log_dir, "gitleaks", with_text=True)
        self.assertEqual(read["stated_reason"], "the words on stderr\n")
        self.assertEqual(read["stated_reason_stream"], "stderr")

        (workspace.log_dir / "gitleaks.stderr.log").write_text("   \n", encoding="utf-8")
        fell_back = cli._tool_words(workspace.log_dir, "gitleaks", with_text=True)
        self.assertEqual(fell_back["stated_reason"], "the words on stdout\n")
        self.assertEqual(fell_back["stated_reason_stream"], "stdout")

    def test_a_bounded_excerpt_says_it_was_cut_and_carries_its_digest(self) -> None:
        """The cap cannot lose evidence silently: the size and sha256 sit beside the text."""
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        long_text = "a" * (cli.TOOL_WORDS_EXCERPT_LIMIT + 25)
        path = workspace.state_reason("osv-scanner", long_text)

        words = cli._tool_words(workspace.log_dir, "osv-scanner", with_text=True)
        stream = words["streams"]["stderr"]
        self.assertIs(stream["text_truncated"], True)
        self.assertEqual(len(stream["text"]), cli.TOOL_WORDS_EXCERPT_LIMIT)
        self.assertEqual(stream["text_excerpt_limit"], cli.TOOL_WORDS_EXCERPT_LIMIT)
        self.assertEqual(stream["bytes"], path.stat().st_size)
        self.assertEqual(
            stream["sha256"], hashlib.sha256(path.read_bytes()).hexdigest()
        )

    def test_the_fetch_disclosure_projects_the_runners_statements_and_no_more(self) -> None:
        """Selected by field name from the runner's own record, with the weaker claim kept.

        The normalizer observes artifacts, not sockets. An empty selection means the
        runner said nothing about an invocation-time fetch, which is a strictly weaker
        statement than no fetch having occurred -- and the note must keep it weaker,
        because the stronger claim would be a fabricated measurement.
        """
        workspace = self.workspace(present={"trivy": "trivy.json"})
        status_path = workspace.write_status(
            "trivy",
            tool="trivy",
            exit_code=0,
            feed_state="not attempted",
            skip_db_update="true",
            elapsed_seconds=12,
        )
        status = cli._runner_status(workspace.log_dir, "trivy")
        disclosure = cli._network_fetch_disclosure(status)

        self.assertEqual(disclosure["source"], str(status_path))
        self.assertEqual(
            sorted(disclosure["statements"]), ["feed_state", "skip_db_update"]
        )
        self.assertEqual(disclosure["statement_count"], 2)
        self.assertEqual(disclosure["status_fields_scanned"], 5)
        self.assertIn("not a second one", disclosure["note"])
        self.assertIn("which is not the same claim as no fetch", disclosure["note"])

        silent = cli._network_fetch_disclosure(
            cli._runner_status(workspace.log_dir, "gitleaks")
        )
        self.assertEqual(silent["statements"], {})
        self.assertEqual(silent["statement_count"], 0)

    def test_every_file_the_record_names_carries_its_size_and_digest(self) -> None:
        """The two artifact trees are git-ignored, so the record must be self-describing.

        An absent file says so rather than being omitted, which is the difference between
        "this file is not there" and "nobody looked".
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        artifact = workspace.raw_dir / "gitleaks.json"
        record = cli._file_record(artifact)
        self.assertIs(record["present"], True)
        self.assertEqual(record["bytes"], artifact.stat().st_size)
        self.assertEqual(
            record["sha256"], hashlib.sha256(artifact.read_bytes()).hexdigest()
        )

        absent = cli._file_record(workspace.raw_dir / "osv-scanner.json")
        self.assertIs(absent["present"], False)
        self.assertIsNone(absent["bytes"])
        self.assertIsNone(absent["sha256"])
        self.assertEqual(absent["path"], str(workspace.raw_dir / "osv-scanner.json"))


class CliArtifactOutcomeTests(CliTestCase):
    """``cli.ArtifactOutcome`` is the per-tool record all nine tools reach.

    ``findings.json`` and ``findings.csv`` are row-only, so a tool that produced no row is
    invisible in them by construction; ``tool-status.md`` and ``severity-map.md`` are the
    authoritative inventory of all nine and are rendered from these outcomes. A field
    missing here empties a column of a published document, and an absent artifact recorded
    as a counted zero would turn "nobody looked" into "there was nothing".
    """

    def test_the_outcome_describes_itself_in_the_order_its_consumers_read(self) -> None:
        """Twenty-one keys, in a fixed order, and every mutable default its own object."""
        first = cli.ArtifactOutcome(
            tool="gitleaks",
            scanner_class="secret",
            artifact_filename="gitleaks.json",
            present=False,
            parse_status=cli.PARSE_STATUS_FAILED,
            artifact={},
        )
        second = cli.ArtifactOutcome(
            tool="checkov",
            scanner_class="misconfig",
            artifact_filename="checkov.json",
            present=False,
            parse_status=cli.PARSE_STATUS_FAILED,
            artifact={},
        )
        self.assertEqual(tuple(first.as_dict()), CLI_OUTCOME_KEYS)
        self.assertEqual(first.emitted_rows, 0)
        self.assertEqual(first.rejected_records, 0)
        self.assertIsNone(first.raw_records)
        self.assertIsNone(first.routing)
        self.assertIsNone(first.artifact_expected)

        first.notes.append("a note about the first outcome only")
        first.rejections_by_class["missing_message"] = 1
        self.assertEqual(
            second.notes,
            [],
            msg="each outcome must own its containers; a shared default would let one "
            "tool's notes appear under another's entry",
        )
        self.assertEqual(second.rejections_by_class, {})
        json.dumps(first.as_dict())

    def test_a_present_artifact_records_its_counts_routing_and_evidence(self) -> None:
        """One artifact processed, and every number the record publishes measured once.

        The independent record count is compared with ``reconcile.count_records`` over the
        same document -- the traversal that builds nothing -- and the rejection tally is
        compared with the rejections themselves, so a per-class breakdown that had drifted
        from the total would be caught rather than reported.
        """
        workspace = self.workspace(present={"checkov": "checkov.json"})
        rows, counts, outcome = self.process_present(workspace, "checkov")
        document = load_fixture("checkov.json")

        self.assertEqual(outcome.raw_records, reconcile.count_records("checkov", document))
        self.assertEqual(outcome.emitted_rows, len(rows))
        self.assertEqual(outcome.rejected_records, len(outcome.rejections))
        self.assertEqual(
            sum(outcome.rejections_by_class.values()), outcome.rejected_records
        )
        self.assertEqual(
            outcome.parse_status,
            cli.PARSE_STATUS_PARTIAL if outcome.rejections else cli.PARSE_STATUS_CLEAN,
        )
        self.assertEqual(outcome.routing["tool"], "checkov")
        self.assertEqual(outcome.routing["shape"], shape.SHAPE_NATIVE)
        self.assertIn("detection", outcome.routing)
        self.assertEqual(
            sorted(outcome.extras),
            ["never_emitted_sections", "parsing_errors", "report_summaries"],
            msg="Checkov's parsing errors are status evidence this module carries",
        )
        self.assertEqual(counts.tool, "checkov")
        self.assertIs(counts.present, True)
        self.assertEqual(counts.raw_records, outcome.raw_records)
        self.assertEqual(counts.emitted_rows, outcome.emitted_rows)
        json.dumps(outcome.as_dict())

    def test_an_absent_artifact_reconciles_as_not_applicable_rather_than_zero(self) -> None:
        """The sentinel, with its em dash, and never ``0 = 0 + 0``.

        A zero identity is a passing assertion over an artifact nobody looked at, which is
        precisely the outcome the sentinel exists to keep distinguishable. Asserted through
        stage A, because that is where the identity a consumer reads is written.
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        outcome = self.outcome_for(workspace, "osv-scanner")
        outcome.artifact_expected = False
        counts = cli._process_absent_artifact(
            "osv-scanner",
            root=str(workspace.root),
            log_dir=workspace.log_dir,
            outcome=outcome,
        )

        self.assertEqual(outcome.parse_status, cli.PARSE_STATUS_ABSENT)
        self.assertIsNone(outcome.raw_records)
        self.assertEqual(outcome.emitted_rows, 0)
        self.assertEqual(outcome.rejected_records, 0)
        self.assertIs(counts.present, False)
        self.assertIsNone(counts.raw_records)

        stage_a = {
            entry.tool: entry
            for entry in reconcile.run_stage_a(
                [counts], reject_classes=paths.REJECT_CLASSES
            )
        }
        entry = stage_a["osv-scanner"]
        self.assertEqual(entry.identity, reconcile.NOT_APPLICABLE_ABSENT)
        self.assertEqual(entry.identity, "not applicable \u2014 artifact absent")
        self.assertIsNone(entry.passed)
        self.assertTrue(
            any("not-applicable sentinel" in note for note in outcome.notes),
            msg=f"the outcome must say which reconciliation applies; got {outcome.notes}",
        )


class CliPresentArtifactProcessingTests(CliTestCase):
    """``cli._process_present_artifact``: the ordinary path, and every halt it can raise.

    Two orderings inside it are load-bearing and neither is visible from the outside. The
    independent record count is taken *before* the adapter runs, so an adapter that halts
    still leaves the artifact's true record count in the record. And the outcome is already
    in the run record's artifact list when this is called, so a halt leaves the evidence
    gathered so far behind rather than only inside the exception.
    """

    def test_the_ordinary_path_returns_rows_and_the_artifacts_counts(self) -> None:
        """A clean artifact: rows out, a status of ``clean``, and the counts to reconcile."""
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        rows, counts, outcome = self.process_present(workspace, "gitleaks")

        self.assertEqual(len(rows), outcome.emitted_rows)
        self.assertGreater(len(rows), 0)
        for row in rows:
            with self.subTest(row=row["rule_id"]):
                self.assertEqual(tuple(row), emit.FIELDS)
                self.assertEqual(row["tool"], "gitleaks")
        self.assertEqual(outcome.parse_status, cli.PARSE_STATUS_CLEAN)
        self.assertEqual(outcome.rejections, [])
        self.assertEqual(counts.rejected_records, 0)

    def test_an_unreadable_artifact_is_a_configuration_fault(self) -> None:
        """Present but unreadable is a fault to correct, not an artifact to classify.

        Driven by pointing the stage at a directory, which is the readable-shaped input a
        real run can produce: an artifact name occupied by a directory is exactly the
        condition the raw-tree enumeration reports separately.
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        outcome = self.outcome_for(workspace, "gitleaks")
        with self.assertRaises(cli.ConfigurationFault) as raised:
            self.process_present(
                workspace,
                "gitleaks",
                artifact_path=workspace.log_dir,
                outcome=outcome,
            )
        fault = self.assertHalt(
            raised.exception,
            reason=cli.HALT_ARTIFACT_UNREADABLE,
            exit_code=cli.EXIT_CONFIG,
            expected_class=cli.ConfigurationFault,
        )
        self.assertEqual(fault.details["artifact_path"], str(workspace.log_dir))
        self.assertEqual(outcome.parse_status, cli.PARSE_STATUS_FAILED)

    def test_an_artifact_that_is_not_json_halts_with_the_parser_error_located(self) -> None:
        """The parser error is retained verbatim, with its line, column and offset."""
        workspace = self.workspace(present={"trivy": "trivy.json"})
        (workspace.raw_dir / "trivy.json").write_text(
            '{"Results": [ this is not JSON', encoding="utf-8"
        )
        outcome = self.outcome_for(workspace, "trivy")
        with self.assertRaises(cli.NormalizeHalt) as raised:
            self.process_present(workspace, "trivy", outcome=outcome)
        halt = self.assertHalt(
            raised.exception,
            reason=cli.HALT_ARTIFACT_INVALID_JSON,
            exit_code=cli.EXIT_HALT,
        )
        self.assertNotIsInstance(halt, cli.ConfigurationFault)
        self.assertTrue(halt.details["parser_error"])
        self.assertIsInstance(halt.details["line"], int)
        self.assertIsInstance(halt.details["column"], int)
        self.assertIsInstance(halt.details["character_offset"], int)
        self.assertEqual(outcome.parse_status, cli.PARSE_STATUS_FAILED)

    def test_a_document_matching_no_known_shape_halts_with_the_detection_evidence(self) -> None:
        """A container the schema does not admit is a halt, never a best-effort parse.

        The detection reason arrives from ``shape.py``'s own record and is renamed on the
        way in, so the halt's ``reason`` stays the halting condition while the detector's
        reason is carried beside it. Both must be present: a halt that quoted only one
        would leave a reader unable to tell which test failed.
        """
        workspace = self.workspace(present={"checkov": "checkov.json"})
        (workspace.raw_dir / "checkov.json").write_text(
            json.dumps("a bare JSON string is neither an object nor an array"),
            encoding="utf-8",
        )
        outcome = self.outcome_for(workspace, "checkov")
        with self.assertRaises(cli.NormalizeHalt) as raised:
            self.process_present(workspace, "checkov", outcome=outcome)
        halt = self.assertHalt(
            raised.exception,
            reason=cli.HALT_UNKNOWN_ARTIFACT_SHAPE,
            exit_code=cli.EXIT_HALT,
        )
        self.assertIn("detection_reason", halt.details)
        self.assertTrue(halt.details["detection_reason"])
        self.assertEqual(outcome.parse_status, cli.PARSE_STATUS_FAILED)

    def test_an_adapter_structural_halt_is_carried_through_with_its_structure(self) -> None:
        """A non-empty unsupported section stops the run rather than being dropped.

        Reconciliation would otherwise pass while real tool output went unread, and a
        dataset short of a section reads exactly like a scan that found nothing there.
        """
        workspace = self.workspace(
            present={"trivy": "halt-trivy-unsupported-section.json"}
        )
        outcome = self.outcome_for(workspace, "trivy")
        with self.assertRaises(cli.NormalizeHalt) as raised:
            self.process_present(workspace, "trivy", outcome=outcome)
        halt = self.assertHalt(
            raised.exception,
            reason=cli.HALT_ADAPTER_STRUCTURAL,
            exit_code=cli.EXIT_HALT,
        )
        self.assertTrue(halt.details["adapter_reason"])
        self.assertTrue(halt.details["section"])
        self.assertIsInstance(halt.details["structure"], dict)
        self.assertEqual(outcome.parse_status, cli.PARSE_STATUS_FAILED)

    def test_the_independent_count_is_taken_before_the_adapter_runs(self) -> None:
        """An adapter that halted still leaves the artifact's true record count behind.

        The ordering is the whole reason the count is independent: taken afterwards, a
        structural halt would leave the record unable to say how many records the artifact
        held, and the reconciliation identity would have no left-hand side to quote.
        """
        workspace = self.workspace(
            present={"trivy": "halt-trivy-unsupported-section.json"}
        )
        outcome = self.outcome_for(workspace, "trivy")
        with self.assertRaises(cli.NormalizeHalt):
            self.process_present(workspace, "trivy", outcome=outcome)
        self.assertIsNotNone(
            outcome.raw_records,
            msg="the counting traversal must have run before the adapter halted",
        )
        self.assertEqual(
            outcome.raw_records,
            reconcile.count_records(
                "trivy", load_fixture("halt-trivy-unsupported-section.json")
            ),
        )
        self.assertEqual(outcome.emitted_rows, 0)
        self.assertIsNotNone(outcome.routing)

    def test_an_adapter_contract_fault_is_never_absorbed_into_a_rejection_count(self) -> None:
        """An adapter refusing its arguments halts; it does not become a counted rejection.

        Every adapter's contract error derives from ``ValueError``, and a rejection count
        is a statement about records. Folding one into the other would report a defect in
        the caller as a property of the scanner's output.
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        outcome = self.outcome_for(workspace, "gitleaks")
        with self.assertRaises(cli.NormalizeHalt) as raised:
            self.process_present(
                workspace, "gitleaks", root="a/relative/root", outcome=outcome
            )
        halt = self.assertHalt(
            raised.exception,
            reason=cli.HALT_ADAPTER_CONTRACT,
            exit_code=cli.EXIT_HALT,
        )
        self.assertEqual(halt.details["tool"], "gitleaks")
        self.assertTrue(halt.details["error"])
        self.assertEqual(outcome.emitted_rows, 0)
        self.assertEqual(outcome.rejected_records, 0)
        self.assertEqual(outcome.rejections_by_class, {})

    def test_a_tool_with_no_recorded_path_base_is_a_configuration_fault(self) -> None:
        """A base is read from the metadata, never inferred, and never defaulted to the root."""
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        document = json.loads(workspace.metadata_path.read_text(encoding="utf-8"))
        del document["tools"]["gitleaks"]
        without_gitleaks = workspace.directory / "metadata-without-gitleaks.json"
        without_gitleaks.write_text(json.dumps(document), encoding="utf-8")

        with self.assertRaises(cli.ConfigurationFault) as raised:
            self.process_present(
                workspace,
                "gitleaks",
                metadata=paths.load_runner_metadata(without_gitleaks),
            )
        fault = self.assertHalt(
            raised.exception,
            reason=cli.HALT_RUNNER_METADATA,
            exit_code=cli.EXIT_CONFIG,
            expected_class=cli.ConfigurationFault,
        )
        self.assertEqual(fault.details["tool"], "gitleaks")
        self.assertIn("not something to infer", fault.message)

    def test_a_runners_own_status_record_of_another_tree_halts_either_way(self) -> None:
        """A targeting fault is established from the runner's record, present artifact or not.

        Both conditions are read from ``<tool>.status``: a root that is not the one this
        dataset is expressed against, and a root whose source is the setup-time smoke
        override, which redirects every runner at one small directory and is never a
        fallback for a real scan. Each is asserted for a present artifact *and* for an
        absent one, because a runner that scanned the wrong tree wrote nothing worth
        keeping either way.
        """
        cases = {
            cli.HALT_WRONG_SCAN_ROOT_EVIDENCE: {
                "scan_root": "/somewhere/else/spark",
                "scan_root_source": "SPARK_SRC",
            },
            cli.HALT_SMOKE_OVERRIDE_EVIDENCE: {
                "scan_root_source": "HARNESS_SMOKE_TARGET",
            },
        }
        for reason, fields in cases.items():
            with self.subTest(reason=reason, artifact="present"):
                workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
                workspace.write_status(
                    "gitleaks", tool="gitleaks", exit_code=0, **fields
                )
                with self.assertRaises(cli.NormalizeHalt) as raised:
                    self.process_present(workspace, "gitleaks")
                halt = self.assertHalt(
                    raised.exception, reason=reason, exit_code=cli.EXIT_HALT
                )
                self.assertEqual(halt.details["tool"], "gitleaks")

            with self.subTest(reason=reason, artifact="absent"):
                workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
                workspace.write_status(
                    "osv-scanner", tool="osv-scanner", exit_code=128, **fields
                )
                with self.assertRaises(cli.NormalizeHalt) as raised:
                    cli._process_absent_artifact(
                        "osv-scanner",
                        root=str(workspace.root),
                        log_dir=workspace.log_dir,
                        outcome=self.outcome_for(workspace, "osv-scanner"),
                    )
                self.assertHalt(
                    raised.exception, reason=reason, exit_code=cli.EXIT_HALT
                )

    def test_a_non_zero_exit_and_a_timeout_beside_a_parsable_artifact_are_recorded(self) -> None:
        """Artifact status and exit status are independent, and both notes say so.

        Two of the nine runners exit non-zero precisely because they found something, so a
        valid artifact is never suppressed for its exit code. A status file with no
        readable code is recorded as ``timeout`` beside a present, parsable artifact --
        the condition AAP 0.9.3 records rather than halts on.
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        workspace.write_status("gitleaks", tool="gitleaks", exit_code=2)
        _, _, outcome = self.process_present(workspace, "gitleaks")
        self.assertEqual(outcome.parse_status, cli.PARSE_STATUS_CLEAN)
        self.assertTrue(
            any("used for nothing else" in note for note in outcome.notes),
            msg=f"the non-zero exit must be recorded as a fact; got {outcome.notes}",
        )

        workspace.write_status("gitleaks", tool="gitleaks", exit_code="")
        _, _, timed_out = self.process_present(workspace, "gitleaks")
        self.assertEqual(timed_out.parse_status, cli.PARSE_STATUS_CLEAN)
        self.assertTrue(
            any("exit_status is" in note for note in timed_out.notes),
            msg=f"the timeout status must be recorded; got {timed_out.notes}",
        )

    def test_an_artifact_present_against_the_metadatas_expectation_is_a_difference(self) -> None:
        """Recorded as a difference, and normalized on its own merits regardless."""
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        outcome = self.outcome_for(workspace, "gitleaks")
        outcome.artifact_expected = False
        self.process_present(workspace, "gitleaks", outcome=outcome)
        self.assertTrue(
            any("Recorded as a difference" in note for note in outcome.notes),
            msg=f"the difference must reach the record; got {outcome.notes}",
        )
        self.assertEqual(outcome.parse_status, cli.PARSE_STATUS_CLEAN)


class CliAbsentArtifactProcessingTests(CliTestCase):
    """``cli._process_absent_artifact`` classifies an absence from the tool's words alone.

    This is the one classification in the pipeline that cannot be made from the artifacts,
    because the artifact is what is missing. AAP 0.5.4 settles it *"using only the tool's
    own stated words"*: with a stated reason the tool contributes zero rows and the run
    continues, and with no stated reason the run halts -- a termination that produced no
    exit code names how the process ended and does not excuse the absence.
    """

    def test_a_stated_reason_classifies_the_absence_and_is_quoted_verbatim(self) -> None:
        """The words are carried into the record, not summarised into a status."""
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        outcome = self.outcome_for(workspace, "osv-scanner")
        counts = cli._process_absent_artifact(
            "osv-scanner",
            root=str(workspace.root),
            log_dir=workspace.log_dir,
            outcome=outcome,
        )
        self.assertEqual(outcome.parse_status, cli.PARSE_STATUS_ABSENT)
        self.assertIs(counts.present, False)
        self.assertIs(outcome.tool_words["stated_reason_present"], True)
        self.assertEqual(outcome.tool_words["stated_reason"], CLI_STATED_REASON)
        self.assertEqual(outcome.tool_words["stated_reason_stream"], "stderr")
        json.dumps(outcome.as_dict())

    def test_an_absence_with_no_stated_reason_halts_naming_where_it_looked(self) -> None:
        """Only the tool's own words can settle completion from failure, so silence halts.

        The details name every path that was consulted -- the artifact, both streams and
        the status file -- because a halt a reader cannot retrace is a halt they cannot
        act on.
        """
        workspace = self.workspace(
            present={"gitleaks": "gitleaks.json"}, stated_reasons=False
        )
        outcome = self.outcome_for(workspace, "osv-scanner")
        with self.assertRaises(cli.NormalizeHalt) as raised:
            cli._process_absent_artifact(
                "osv-scanner",
                root=str(workspace.root),
                log_dir=workspace.log_dir,
                outcome=outcome,
            )
        halt = self.assertHalt(
            raised.exception,
            reason=cli.HALT_ABSENT_WITHOUT_STATED_REASON,
            exit_code=cli.EXIT_HALT,
        )
        self.assertNotIsInstance(halt, cli.ConfigurationFault)
        self.assertEqual(
            halt.details["stderr_log"],
            str(workspace.log_dir / "osv-scanner.stderr.log"),
        )
        self.assertEqual(
            halt.details["stdout_log"],
            str(workspace.log_dir / "osv-scanner.stdout.log"),
        )
        self.assertIn("does not excuse a missing artifact", halt.details["note"])

    def test_a_termination_without_an_exit_code_does_not_excuse_the_absence(self) -> None:
        """``timeout`` names how the process ended; the words still have to be there.

        Both halves asserted over the same status file: with no stated reason it halts even
        though the status is recorded, and with the reason restored the same status becomes
        a recorded note beside a completed classification.
        """
        workspace = self.workspace(
            present={"gitleaks": "gitleaks.json"}, stated_reasons=False
        )
        workspace.write_status("osv-scanner", tool="osv-scanner", exit_code="")
        with self.assertRaises(cli.NormalizeHalt) as raised:
            cli._process_absent_artifact(
                "osv-scanner",
                root=str(workspace.root),
                log_dir=workspace.log_dir,
                outcome=self.outcome_for(workspace, "osv-scanner"),
            )
        halt = self.assertHalt(
            raised.exception,
            reason=cli.HALT_ABSENT_WITHOUT_STATED_REASON,
            exit_code=cli.EXIT_HALT,
        )
        self.assertEqual(halt.details["exit_status"], cli.EXIT_STATUS_TIMEOUT)

        workspace.state_reason("osv-scanner")
        workspace.write_status(
            "osv-scanner", tool="osv-scanner", exit_code="", artifact_bytes="MISSING"
        )
        outcome = self.outcome_for(workspace, "osv-scanner")
        outcome.artifact_expected = True
        cli._process_absent_artifact(
            "osv-scanner",
            root=str(workspace.root),
            log_dir=workspace.log_dir,
            outcome=outcome,
        )
        self.assertEqual(outcome.parse_status, cli.PARSE_STATUS_ABSENT)
        notes = " ".join(outcome.notes)
        self.assertIn("exit_status is", notes)
        self.assertIn("independently reports the artifact as MISSING", notes)
        self.assertIn("Recorded as a difference", notes)


class CliPreWriteReconciliationTests(CliTestCase):
    """``cli._reconcile_before_write``: the gate that decides whether a dataset is published.

    AAP 0.9.2 makes a failed reconciliation halt the run, and the ordering is the substance
    of the requirement rather than a detail of it: stages A and B are established *before*
    either output file is opened, so a dataset whose identity already fails never reaches
    disk. A gate that ran after the write would leave a wrong ``findings.json`` published
    beside a record admitting it is wrong.
    """

    def _counts(self, tools: Mapping[str, int]) -> list[reconcile.ArtifactCounts]:
        """Build present-artifact counts whose identity holds by construction."""
        return [
            reconcile.ArtifactCounts.for_present_artifact(
                tool, raw_records=rows, emitted_rows=rows, rejected_records=0
            )
            for tool, rows in tools.items()
        ]

    def test_the_gate_records_both_stages_and_the_vocabulary_it_was_handed(self) -> None:
        """The passing path: stages A and B in the record, and the gate saying what it checked.

        The rejection-class vocabulary is asserted to be the one ``paths.py`` owns, because
        ``reconcile.py`` deliberately imports nothing from this package -- the vocabulary
        arrives as a parameter so its independence is enforced by the import graph. A
        record naming some other vocabulary would mean the gate had been given a private
        copy that could drift.
        """
        record: dict[str, object] = {}
        stage_a, stage_b = cli._reconcile_before_write(
            self._counts({"gitleaks": 1, "checkov": 6}), record
        )
        reconciliation = record["reconciliation"]

        self.assertEqual(
            reconciliation["reject_class_vocabulary"], list(paths.REJECT_CLASSES)
        )
        self.assertEqual(
            reconciliation["not_applicable_sentinel"], reconcile.NOT_APPLICABLE_ABSENT
        )
        self.assertEqual(
            reconciliation["identity"],
            "raw finding records = dataset rows for that tool + rejected records",
        )
        gate = reconciliation["pre_write_gate"]
        self.assertEqual(gate["checked"], ["stage_a", "stage_b"])
        self.assertIs(gate["passed"], True)
        self.assertEqual(gate["failures"], [])
        self.assertIsNone(
            reconciliation["stage_c"],
            msg="stage C cannot have run: there is nothing written to count yet",
        )
        self.assertIsNone(reconciliation["passed"])
        self.assertEqual(
            {artifact.tool for artifact in stage_a},
            set(reconcile.CANONICAL_TOOLS),
            msg="stage A seeds every canonical tool so a tool with no counts is visible",
        )
        self.assertEqual(stage_b.emitted_rows, 7)

    def test_a_failing_identity_halts_before_either_output_file_exists(self) -> None:
        """The ordering, asserted the only way it can be: by looking for the files.

        The counts are made to disagree -- a row emitted that no record accounts for -- and
        the whole composition is driven through ``main``. The halt is then checked against
        the *filesystem*: neither ``findings.json`` nor ``findings.csv`` may exist, because
        a pre-write gate that wrote first would leave both behind.
        """
        record: dict[str, object] = {}
        with self.assertRaises(cli.NormalizeHalt) as raised:
            cli._reconcile_before_write(
                [
                    reconcile.ArtifactCounts.for_present_artifact(
                        "gitleaks", raw_records=1, emitted_rows=3, rejected_records=0
                    )
                ],
                record,
            )
        halt = self.assertHalt(
            raised.exception,
            reason=cli.HALT_RECONCILIATION,
            exit_code=cli.EXIT_HALT,
        )
        self.assertEqual(halt.details["stage"], "A/B")
        self.assertTrue(halt.details["failures"])
        self.assertIn("before anything was written", halt.message)
        gate = record["reconciliation"]["pre_write_gate"]
        self.assertIs(gate["passed"], False)
        self.assertEqual(gate["failures"], halt.details["failures"])

    def test_the_composition_leaves_no_output_behind_when_the_identity_fails(self) -> None:
        """End to end: a dropped row fails the identity and nothing is published.

        A capability fake discards one adapted row on its way back from the Gitleaks
        adapter -- the silent-drop defect this identity exists to catch, since the row is
        simply not there and no exception is raised. The independent traversal still counts
        the record, so the identity fails, and the assertion that matters is that both
        output paths are still absent afterwards.

        Over the whole captured artifact set rather than one artifact, because the
        composition classifies an absent artifact before it reaches the gate: a tool with
        no documented no-work statement of its own halts the run on absence, so a
        one-artifact workspace never gets as far as the identity this test is about.
        """
        workspace = self.workspace()
        real_adapt = gitleaks.adapt

        def drops_a_row(*positional: object, **keywords: object):
            rows, rejections, counters = real_adapt(*positional, **keywords)
            return rows[1:], rejections, counters

        with unittest.mock.patch.object(gitleaks, "adapt", drops_a_row):
            exit_code, _, stderr = workspace.run()

        self.assertEqual(exit_code, cli.EXIT_HALT)
        self.assertIn(cli.HALT_RECONCILIATION, stderr)
        self.assertFalse(
            workspace.findings_json.exists(),
            msg="findings.json must not exist: the gate runs before the write",
        )
        self.assertFalse(workspace.findings_csv.exists())
        record = workspace.record()
        self.assertEqual(record["halt"]["reason"], cli.HALT_RECONCILIATION)
        self.assertIs(record["reconciliation"]["pre_write_gate"]["passed"], False)
        self.assertIsNone(record["outputs"])
        self.assertIsNone(record["output_comparison"])
        self.assertIs(
            gitleaks.adapt, real_adapt, msg="the capability fake must be undone"
        )

    def test_a_reconciliation_error_and_malformed_counts_both_halt_as_reconciliation(self) -> None:
        """Two failure modes of the gate itself, kept distinct from a failed identity.

        An unknown tool identifier and a counts object of the wrong type are raised by
        ``reconcile.py`` as ``ReconciliationError`` and ``ValueError`` respectively, and both
        must arrive as a reconciliation halt rather than escaping as an unexpected error --
        an unexpected error would name the wrong condition in the record.
        """
        cases = {
            "unknown tool identifier": [
                reconcile.ArtifactCounts.for_present_artifact(
                    "not-a-tool", raw_records=1, emitted_rows=1, rejected_records=0
                )
            ],
            "must be an ArtifactCounts or a mapping": ["gitleaks: 1 row"],
        }
        for fragment, counts in cases.items():
            with self.subTest(fragment=fragment):
                record: dict[str, object] = {}
                with self.assertRaises(cli.NormalizeHalt) as raised:
                    cli._reconcile_before_write(counts, record)
                halt = self.assertHalt(
                    raised.exception,
                    reason=cli.HALT_RECONCILIATION,
                    exit_code=cli.EXIT_HALT,
                )
                self.assertIn(fragment, halt.details["error"])
                self.assertEqual(halt.details["stage"], "A/B")
                self.assertNotIn(
                    "reconciliation",
                    record,
                    msg="a gate that could not be established records no stages",
                )


class CliOutputWriteTests(CliTestCase):
    """``cli._write_outputs``: both files from one row list, then proven equal by parsing.

    Neither file is derived from the other after writing, and equality is asserted by
    parsing both and coercing the CSV cells to the types their fields carry. A row count is
    established by parsing and never by counting lines, because ``message`` fields carry
    embedded newlines: ``findings.csv`` holds 9,427 rows over 9,436 physical lines, and the
    gap widens with every multi-line message a scanner reports.
    """

    def _rows(self, workspace: "CliWorkspace") -> list[dict[str, object]]:
        """Adapt one captured artifact into dataset rows."""
        rows, _, _ = self.process_present(workspace, "gitleaks")
        return list(rows)

    def test_both_files_are_written_and_the_typed_comparison_is_recorded(self) -> None:
        """The passing path, with the comparison's own numbers asserted rather than its verdict.

        ``passed`` alone would be satisfied by a comparison that compared nothing, so the
        rows compared, the fields compared and the field order are all asserted: twelve
        fields per row, in the schema's order, over every row written.
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        rows = self._rows(workspace)
        record: dict[str, object] = {}
        comparison = cli._write_outputs(rows, workspace.inputs(), record)

        self.assertIs(comparison.passed, True)
        self.assertEqual(comparison.rows_compared, len(rows))
        self.assertEqual(comparison.fields_compared, len(rows) * len(emit.FIELDS))
        self.assertEqual(tuple(comparison.field_order), emit.FIELDS)
        self.assertIsNone(comparison.first_mismatch)
        self.assertTrue(workspace.findings_json.exists())
        self.assertTrue(workspace.findings_csv.exists())
        self.assertEqual(
            parse_json_rows(workspace.findings_json),
            parse_csv_rows(workspace.findings_csv),
            msg="this module's own independent re-parse must agree with the comparison",
        )
        self.assertEqual(record["output_comparison"], comparison.as_dict())
        outputs = record["outputs"]
        for name in ("findings_json", "findings_csv"):
            with self.subTest(output=name):
                self.assertEqual(outputs[name]["bytes"], (workspace.out_dir / (
                    "findings.json" if name == "findings_json" else "findings.csv"
                )).stat().st_size)
                self.assertRegex(outputs[name]["sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(outputs["row_validation"]["rows"], len(rows))
        self.assertIn("absence_convention", outputs)

    def test_a_row_the_schema_refuses_halts_at_the_run_level(self) -> None:
        """An absolute path is refused, and the refusal is a halt rather than a config fault.

        AAP 0.8.2 forbids an absolute path in any emitted row, including for archive members
        and other non-filesystem coordinates. The distinction from exit 78 matters: this is
        a dataset that must not be published, not a harness that needs correcting.
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        rows = self._rows(workspace)
        rows[0] = dict(rows[0], path=str(workspace.root / "core" / "Absolute.scala"))
        record: dict[str, object] = {}
        with self.assertRaises(cli.NormalizeHalt) as raised:
            cli._write_outputs(rows, workspace.inputs(), record)
        halt = self.assertHalt(
            raised.exception, reason=cli.HALT_EMIT, exit_code=cli.EXIT_HALT
        )
        self.assertNotIsInstance(halt, cli.ConfigurationFault)
        self.assertTrue(halt.details["error"])
        self.assertEqual(halt.details["findings_json"], str(workspace.findings_json))

    def test_an_unwritable_output_location_is_a_configuration_fault(self) -> None:
        """A path that cannot be written is the harness's fault, so exit 78 not exit 1."""
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        rows = self._rows(workspace)
        occupied = workspace.out_dir / "occupied-by-a-directory"
        occupied.mkdir(parents=True)
        record: dict[str, object] = {}
        with self.assertRaises(cli.ConfigurationFault) as raised:
            cli._write_outputs(
                rows, workspace.inputs(findings_json=occupied), record
            )
        fault = self.assertHalt(
            raised.exception,
            reason=cli.HALT_OUTPUT_SYMLINKED,
            exit_code=cli.EXIT_CONFIG,
            expected_class=cli.ConfigurationFault,
        )
        self.assertIn("cannot be written", fault.message)
        self.assertIn("is not a regular file", fault.message)

    def test_a_json_csv_disagreement_halts_naming_the_first_mismatch(self) -> None:
        """A genuine disagreement between the two files, produced and then detected.

        The capability fake writes both files through the real writer, tampers one CSV cell
        on disk, and returns the *real* ``emit.compare_outputs`` over the tampered pair --
        so the mismatch is discovered by the production comparison rather than asserted by
        the test. This is the defect the comparison exists for: one file carrying a value
        the other does not, with no exception raised anywhere.
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        rows = self._rows(workspace)
        real_publish_findings = emit.publish_findings

        def tampers_with_the_csv(row_sequence, json_path, csv_path, manifest_path=None):
            publication = real_publish_findings(
                row_sequence, json_path, csv_path, manifest_path=manifest_path
            )
            lines = csv_path.read_text(encoding="utf-8").splitlines(keepends=True)
            lines[1] = lines[1].replace("gitleaks", "tampered", 1)
            csv_path.write_text("".join(lines), encoding="utf-8")
            comparison = emit.compare_outputs(json_path, csv_path)
            if not comparison.passed:
                raise emit.ComparisonFailed(comparison)
            return publication

        record: dict[str, object] = {}
        with unittest.mock.patch.object(emit, "publish_findings", tampers_with_the_csv):
            with self.assertRaises(cli.NormalizeHalt) as raised:
                cli._write_outputs(rows, workspace.inputs(), record)
        halt = self.assertHalt(
            raised.exception,
            reason=cli.HALT_OUTPUT_COMPARISON,
            exit_code=cli.EXIT_HALT,
        )
        self.assertIs(halt.details["comparison"]["passed"], False)
        self.assertEqual(halt.details["comparison"]["first_mismatch"]["field"], "tool")
        self.assertIs(record["output_comparison"]["passed"], False)
        self.assertIs(
            emit.publish_findings,
            real_publish_findings,
            msg="the capability fake must be undone",
        )


class CliAfterWriteReconciliationTests(CliTestCase):
    """``cli._reconcile_after_write``: stage C, over the files as they now sit on disk.

    Stage C compares the parsed ``findings.json`` count and the parsed ``findings.csv``
    count against the stage B identity *separately*, and then against each other -- three
    comparisons, none inferred from another. The stage A and stage B objects are the ones
    measured before the write and are reused rather than recomputed, because a count in two
    places must be one measurement cited twice.
    """

    def _written(self, workspace: "CliWorkspace") -> tuple[object, object, dict]:
        """Drive the ordinary route as far as both files existing on disk."""
        rows, counts, _ = self.process_present(workspace, "gitleaks")
        record: dict[str, object] = {}
        stage_a, stage_b = cli._reconcile_before_write([counts], record)
        cli._write_outputs(rows, workspace.inputs(), record)
        return stage_a, stage_b, record

    def test_stage_c_records_three_comparisons_and_completes_the_report(self) -> None:
        """Both files counted by parsing, each against stage B, and then against each other."""
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        stage_a, stage_b, record = self._written(workspace)
        report = cli._reconcile_after_write(
            stage_a, stage_b, workspace.inputs(), record
        )

        self.assertIs(report.passed, True)
        self.assertEqual(report.failures, ())
        self.assertEqual(
            [comparison.name for comparison in report.stage_c],
            [
                "findings_json_rows_vs_dataset",
                "findings_csv_rows_vs_dataset",
                "findings_json_rows_vs_findings_csv_rows",
            ],
        )
        self.assertIs(
            report.stage_b,
            stage_b,
            msg="stage B must be the object measured before the write, not a recomputation",
        )
        self.assertEqual(tuple(report.stage_a), tuple(stage_a))
        reconciliation = record["reconciliation"]
        self.assertEqual(len(reconciliation["stage_c"]), 3)
        self.assertIs(reconciliation["passed"], True)
        self.assertEqual(reconciliation["failures"], [])

    def test_a_row_missing_from_one_output_fails_stage_c(self) -> None:
        """A silent drop after the write is caught by the counts, not by an exception.

        One row is removed from ``findings.json`` on disk. Nothing about the file is
        malformed, so only the parsed count against stage B can detect it -- which is
        exactly what stage C is for.
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        stage_a, stage_b, record = self._written(workspace)
        written = json.loads(workspace.findings_json.read_text(encoding="utf-8"))
        self.assertGreater(len(written), 0)
        workspace.findings_json.write_text(
            json.dumps(written[:-1]), encoding="utf-8"
        )

        with self.assertRaises(cli.NormalizeHalt) as raised:
            cli._reconcile_after_write(stage_a, stage_b, workspace.inputs(), record)
        halt = self.assertHalt(
            raised.exception,
            reason=cli.HALT_RECONCILIATION,
            exit_code=cli.EXIT_HALT,
        )
        self.assertEqual(halt.details["stage"], "C")
        self.assertTrue(
            any(
                "findings_json_rows_vs_dataset" in failure
                for failure in halt.details["failures"]
            ),
            msg=f"stage C must name the comparison that failed; got {halt.details}",
        )
        self.assertIs(record["reconciliation"]["passed"], False)

    def test_an_output_file_that_cannot_be_parsed_fails_stage_c_rather_than_escaping(self) -> None:
        """Both counts come from parsing, so an unparsable file fails the stage either way.

        The two files fail differently and both are asserted, because a stage that handled
        one and let the other escape as an unexpected error would name the wrong condition
        in the record. Corrupting ``findings.json`` makes the parse itself raise, and the
        stage must convert that into a reconciliation halt carrying the parser error and the
        path it could not read.
        Corrupting ``findings.csv`` does not raise -- ``csv`` reads the garbage as a header
        and reports zero data rows -- so only the comparison against stage B can catch it,
        which is the silent case the separate per-file comparison exists for.
        """
        workspace = self.workspace(present={"gitleaks": "gitleaks.json"})
        garbage = "this is not a parsable dataset"

        stage_a, stage_b, record = self._written(workspace)
        workspace.findings_json.write_text(garbage, encoding="utf-8")
        with self.assertRaises(cli.NormalizeHalt) as raised:
            cli._reconcile_after_write(stage_a, stage_b, workspace.inputs(), record)
        halt = self.assertHalt(
            raised.exception,
            reason=cli.HALT_RECONCILIATION,
            exit_code=cli.EXIT_HALT,
        )
        self.assertEqual(halt.details["stage"], "C")
        self.assertIn("cannot parse", halt.details["error"])
        self.assertIn(str(workspace.findings_json), halt.details["error"])

        stage_a, stage_b, record = self._written(workspace)
        workspace.findings_csv.write_text(garbage, encoding="utf-8")
        with self.assertRaises(cli.NormalizeHalt) as raised:
            cli._reconcile_after_write(stage_a, stage_b, workspace.inputs(), record)
        halt = self.assertHalt(
            raised.exception,
            reason=cli.HALT_RECONCILIATION,
            exit_code=cli.EXIT_HALT,
        )
        self.assertEqual(halt.details["stage"], "C")
        self.assertTrue(
            any(
                "findings_csv_rows_vs_dataset" in failure
                for failure in halt.details["failures"]
            ),
            msg=f"the CSV comparison must be the one that failed; got {halt.details}",
        )
        self.assertIs(record["reconciliation"]["passed"], False)


class CliRunRecordTests(CliTestCase):
    """``normalize-run.json``: the record every downstream document is rendered from.

    ``oss-scan-results/tool-status.md``, ``severity-map.md`` and ``run-record.md`` are
    rendered from this file joined with ``runner-metadata.json``, and this file is never
    rendered from them (AAP 0.6.4). A key missing here empties a column of a published
    document, and a record that failed to write would lose the reason for a halt.
    """

    def test_the_skeleton_carries_every_key_a_halt_might_never_reach(self) -> None:
        """A stage that never ran leaves ``null`` rather than an absent key.

        The distinction is what lets a reader tell "not reached" from "not recorded", and
        it only holds if the skeleton is built before the first stage runs.
        """
        record = cli._new_record(["--raw-dir", "/somewhere"], "2026-01-01T00:00:00Z")
        for key in (
            "inputs",
            "vocabularies",
            "runner_metadata",
            "scan_root",
            "allowlist",
            "raw_directory",
            "source_index",
            "severity_literals",
            "totals",
            "reconciliation",
            "output_comparison",
            "outputs",
            "halt",
            "exit_status",
            "finished_at_utc",
        ):
            with self.subTest(key=key):
                self.assertIn(key, record)
                self.assertIsNone(record[key])
        self.assertEqual(record["schema_version"], cli.SCHEMA_VERSION)
        self.assertEqual(record["document"], cli.RUN_RECORD_DOCUMENT)
        self.assertEqual(record["produced_by"], "harness/lib/normalize/cli.py")
        self.assertEqual(record["artifacts"], [])
        self.assertEqual(record["started_at_utc"], "2026-01-01T00:00:00Z")
        self.assertEqual(record["command"]["argv"], ["--raw-dir", "/somewhere"])
        self.assertIn(sys.executable, record["command"]["command_line"])
        self.assertEqual(record["command"]["working_directory"], os.getcwd())
        self.assertEqual(record["interpreter"], cli.interpreter_record())

    def test_the_totals_are_one_measurement_each_and_cover_every_tool_processed(self) -> None:
        """Every figure a downstream document quotes, measured here once.

        The per-tool row counts are asserted against the outcomes rather than against a
        literal, and the dataset row count against the rows themselves, so a totals block
        that had drifted from the outcomes it summarises would fail rather than be believed.
        """
        workspace = self.workspace(
            present={"gitleaks": "gitleaks.json", "checkov": "checkov.json"}
        )
        outcomes: list[cli.ArtifactOutcome] = []
        rows: list[dict] = []
        tally = paths.PathKindTally()
        for tool in ("gitleaks", "checkov"):
            artifact_rows, _, outcome = self.process_present(workspace, tool)
            rows.extend(artifact_rows)
            outcomes.append(outcome)
            cli._merge_path_kinds(tally, outcome.counters)
        totals = cli._totals_record(outcomes, rows, tally, str(workspace.root))

        self.assertEqual(totals["rows"], len(rows))
        self.assertEqual(
            totals["rows_by_tool"],
            {outcome.tool: outcome.emitted_rows for outcome in outcomes},
        )
        self.assertEqual(
            totals["raw_records_by_tool"],
            {outcome.tool: outcome.raw_records for outcome in outcomes},
        )
        self.assertEqual(totals["artifacts_present"], 2)
        self.assertEqual(totals["artifacts_absent"], 0)
        self.assertEqual(totals["path_kinds"], tally.as_dict())
        self.assertEqual(totals["non_filesystem_paths"], tally.non_filesystem)
        self.assertEqual(
            totals["non_filesystem_proportion"], tally.non_filesystem_proportion
        )
        self.assertEqual(
            list(totals["rejections_by_class"]),
            [
                reject_class
                for reject_class in paths.REJECT_CLASSES
                if reject_class in totals["rejections_by_class"]
            ],
            msg="the per-class breakdown must follow the authored vocabulary's order",
        )
        self.assertTrue(
            any("deduplicated" in note for note in totals["notes"]),
            msg="the no-cross-tool-interpretation constraint belongs in the record",
        )

    def test_a_value_json_cannot_render_appears_rather_than_aborting_the_write(self) -> None:
        """The fallback renders what it cannot serialise instead of losing the whole record.

        Every branch is asserted, because the one that matters is the last: an object with
        no route through the others is rendered as its ``repr`` so it appears in the record
        rather than raising and taking the surrounding evidence with it.
        """
        class Opaque:
            def __repr__(self) -> str:
                return "<an object with no serialisation route>"

        cases = {
            "as_dict": (
                paths.PathKindTally(),
                lambda rendered: self.assertIn("by_kind", rendered),
            ),
            "dataclass": (
                emit.Mismatch(
                    kind="value",
                    row_index=0,
                    field="tool",
                    json_value="gitleaks",
                    csv_value="tampered",
                    detail="a rendered mismatch",
                ),
                lambda rendered: self.assertEqual(rendered["field"], "tool"),
            ),
            "path": (
                Path("/tmp/a/path"),
                lambda rendered: self.assertEqual(rendered, "/tmp/a/path"),
            ),
            "set": (
                {"beta", "alpha"},
                lambda rendered: self.assertEqual(rendered, ["alpha", "beta"]),
            ),
            "tuple": (
                ("one", "two"),
                lambda rendered: self.assertEqual(rendered, ["one", "two"]),
            ),
            "repr_fallback": (
                Opaque(),
                lambda rendered: self.assertEqual(
                    rendered, "<an object with no serialisation route>"
                ),
            ),
        }
        for name, (value, check) in cases.items():
            with self.subTest(value=name):
                rendered = cli._json_default(value)
                check(rendered)
                json.dumps(rendered)

    def test_a_record_that_cannot_be_written_is_reported_and_never_raises(self) -> None:
        """Losing the reason for a halt to a second fault while writing it down is the worst outcome.

        The record is written fail-closed: the fault is reported on stderr *and* raised
        as :class:`cli.RunRecordNotPersisted`, so a run whose record was lost cannot
        report success. Swallowing it would leave a run that halted for a reason nothing
        on disk states, which is the worse of the two outcomes.
        """
        with self.temporary_directory() as directory:
            occupied = directory / "occupied"
            occupied.write_text("this is a file, not a directory", encoding="utf-8")
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                with self.assertRaises(cli.RunRecordNotPersisted) as raised:
                    cli._write_run_record(
                        occupied / "normalize-run.json", {"schema_version": "x"}
                    )
            self.assertIn("could not be written", str(raised.exception))
            self.assertIn("normalize-run.json", str(raised.exception))

    def test_the_record_is_written_with_an_indent_and_a_trailing_newline(self) -> None:
        """Written to be read and diffed, and to round-trip through a parser unchanged."""
        with self.temporary_directory() as directory:
            path = directory / "nested" / "normalize-run.json"
            record = cli._new_record([], "2026-01-01T00:00:00Z")
            record["outputs"] = {"note": "an em dash \u2014 kept as itself"}
            cli._write_run_record(path, record)
            text = path.read_text(encoding="utf-8")
            self.assertTrue(text.endswith("\n"))
            self.assertIn("\n ", text)
            self.assertIn("\u2014", text)
            self.assertEqual(json.loads(text)["outputs"], record["outputs"])


class CliEndToEndTests(CliTestCase):
    """``cli._execute`` and ``cli.main``: the composition, and the four exit codes.

    Every run here is in process over a temporary raw directory, a temporary scan root and
    temporary output paths. No scanner is invoked, no graph is built or read, no Spark
    source is executed, and nothing is written inside the repository -- which is what makes
    the whole orchestration testable at all.
    """

    def test_a_complete_run_over_every_captured_artifact_exits_zero(self) -> None:
        """The success path, asserted against the dataset and the record it published.

        Eight artifacts present and one legitimately absent, which is a complete run rather
        than a degraded one: a tool that resolves no package source writes nothing and
        states that reason in its own words, and it still holds its entry with zero rows
        rather than disappearing from the record. Its reconciliation reads
        ``reconcile.NOT_APPLICABLE_ABSENT`` and never ``0 = 0 + 0``, which would be a
        passing assertion over an artifact nobody looked at.
        """
        workspace = self.workspace()
        exit_code, stdout, stderr = workspace.run()
        self.assertEqual(exit_code, cli.EXIT_OK, msg=stderr)

        record = workspace.record()
        self.assertIsNone(record["halt"])
        self.assertEqual(record["exit_status"], {"code": 0, "outcome": "completed"})
        self.assertEqual(
            [outcome["tool"] for outcome in record["artifacts"]],
            list(cli.ARTIFACT_ORDER),
            msg="all nine tools hold an entry whether or not they produced a row",
        )
        self.assertEqual(len(record["artifacts"]), 9)
        self.assertEqual(record["totals"]["artifacts_present"], 8)
        self.assertEqual(record["totals"]["artifacts_absent"], 1)
        self.assertIs(record["reconciliation"]["passed"], True)
        self.assertIs(record["reconciliation"]["pre_write_gate"]["passed"], True)
        self.assertEqual(len(record["reconciliation"]["stage_c"]), 3)
        self.assertIs(record["output_comparison"]["passed"], True)

        json_rows = parse_json_rows(workspace.findings_json)
        csv_rows = parse_csv_rows(workspace.findings_csv)
        self.assertEqual(json_rows, csv_rows)
        self.assertEqual(len(json_rows), record["totals"]["rows"])
        self.assertGreater(len(json_rows), 0)
        for index, row in enumerate(json_rows):
            with self.subTest(row=index):
                self.assertEqual(tuple(row), emit.FIELDS)
                self.assertFalse(paths.is_absolute_path(row["path"]))
        self.assertIn(f"wrote {len(json_rows)} row(s)", stdout)
        self.assertIn("all three reconciliation stages", stdout)
        self.assertEqual(stderr, "")

    def test_the_row_order_is_the_canonical_tool_order_and_is_reproducible(self) -> None:
        """One sequence in both files, so two runs over identical artifacts agree byte for byte.

        Asserted by running the whole composition twice into different output paths and
        comparing the bytes, which is the only assertion that would catch an ordering that
        depended on a set's iteration or a directory listing.
        """
        first = self.workspace()
        self.assertEqual(first.run()[0], cli.EXIT_OK)
        second = self.workspace()
        self.assertEqual(second.run()[0], cli.EXIT_OK)
        self.assertEqual(
            first.findings_json.read_bytes(), second.findings_json.read_bytes()
        )
        self.assertEqual(
            first.findings_csv.read_bytes(), second.findings_csv.read_bytes()
        )

        rows = parse_json_rows(first.findings_json)
        appearance = []
        for row in rows:
            if not appearance or appearance[-1] != row["tool"]:
                appearance.append(row["tool"])
        self.assertEqual(
            appearance,
            [tool for tool in cli.ARTIFACT_ORDER if tool in appearance],
            msg="each tool's rows must be contiguous and in canonical order",
        )
        self.assertEqual(len(appearance), len(set(appearance)))

    def test_a_halting_condition_in_the_data_returns_one_and_names_itself(self) -> None:
        """Exit 1, the halt in the record, and the fault named on stderr.

        Driven by an absent artifact with no stated reason: only the tool's own words can
        settle completion from failure, so silence is the condition AAP 0.9.2 halts on.
        """
        workspace = self.workspace(stated_reasons=False)
        exit_code, stdout, stderr = workspace.run()

        self.assertEqual(exit_code, cli.EXIT_HALT)
        self.assertIn("halted", stderr)
        self.assertIn(cli.HALT_ABSENT_WITHOUT_STATED_REASON, stderr)
        self.assertIn("run record written to", stderr)
        self.assertEqual(stdout, "")
        record = workspace.record()
        self.assertEqual(
            record["halt"]["reason"], cli.HALT_ABSENT_WITHOUT_STATED_REASON
        )
        self.assertEqual(record["halt"]["exit_code"], cli.EXIT_HALT)
        self.assertEqual(record["exit_status"], {"code": 1, "outcome": "halted"})
        self.assertFalse(workspace.findings_json.exists())

    def test_a_configuration_fault_returns_seventy_eight_and_names_itself(self) -> None:
        """Exit 78, the fault in the record, and ``configuration fault`` on stderr.

        Driven by a missing raw directory, which is the fault a reader must be able to tell
        from a halting condition in the data: 78 says correct the harness, 1 says the
        dataset must not be published.
        """
        workspace = self.workspace()
        exit_code, stdout, stderr = workspace.run(
            workspace.argv(raw_dir=workspace.directory / "no-such-raw-tree")
        )

        self.assertEqual(exit_code, cli.EXIT_CONFIG)
        self.assertIn("configuration fault", stderr)
        self.assertIn(cli.HALT_RAW_DIRECTORY_MISSING, stderr)
        self.assertEqual(stdout, "")
        record = workspace.record()
        self.assertEqual(record["halt"]["reason"], cli.HALT_RAW_DIRECTORY_MISSING)
        self.assertEqual(record["halt"]["exit_code"], cli.EXIT_CONFIG)
        self.assertEqual(
            record["exit_status"], {"code": 78, "outcome": "configuration_fault"}
        )

    def test_a_usage_error_exits_two_before_any_record_is_written(self) -> None:
        """Exit 2 comes from argparse, ahead of the run, so no record can exist yet.

        The arguments are parsed before the record skeleton is built, which is deliberate:
        a run that was never configured has nothing to record, and a record naming a
        command that never ran would be worse than none.
        """
        workspace = self.workspace()
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as raised:
                cli.main(["--not-an-option", "value"])
        self.assertEqual(raised.exception.code, cli.EXIT_USAGE)
        self.assertIn("unrecognized arguments", stderr.getvalue())
        self.assertFalse(
            workspace.run_record_path.exists(),
            msg="a usage error precedes the record, so none may have been written",
        )

    def test_a_required_input_missing_is_a_configuration_fault_naming_every_gap(self) -> None:
        """Exit 78 with the whole list, because reporting one gap at a time wastes a run.

        The environment is emptied of every name ``cli.py`` consults for the duration of
        the call, because this case is about what happens when **nothing** supplies an
        input. Read from the ambient environment instead, a session that has sourced
        ``harness/env.sh`` supplies a real log tree and a real raw tree, and the run then
        halts on the containment of ``--run-record`` against that log tree -- a different,
        earlier fault, which would leave the gap list this case exists to assert
        unexercised. Isolating the environment is what makes the assertion mean the same
        thing whether or not the harness environment happens to be loaded.
        """
        with self.temporary_directory() as directory:
            record_path = directory / "normalize-run.json"
            stdout, stderr = io.StringIO(), io.StringIO()
            isolated = {
                name: ""
                for name in (
                    "HARNESS_REPO_ROOT",
                    "HARNESS_LOG_DIR",
                    "HARNESS_RAW_DIR",
                    "HARNESS_SCOPE_FILE",
                    "SPARK_SRC",
                    "HARNESS_SMOKE_TARGET",
                )
            }
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with unittest.mock.patch.dict(os.environ, isolated):
                    exit_code = cli.main(["--run-record", str(record_path)])
            self.assertEqual(exit_code, cli.EXIT_CONFIG)
            self.assertIn(cli.HALT_MISSING_INPUT, stderr.getvalue())
            record = json.loads(record_path.read_text(encoding="utf-8"))
            self.assertEqual(record["halt"]["reason"], cli.HALT_MISSING_INPUT)
            self.assertIsNone(
                record["inputs"],
                msg="inputs could not be resolved, so none may be recorded",
            )
            self.assertGreaterEqual(len(record["halt"]["details"]["missing"]), 1)
            self.assertEqual(record["exit_status"]["code"], cli.EXIT_CONFIG)

    def test_an_unexpected_error_is_recorded_with_its_traceback_and_returns_one(self) -> None:
        """Nothing escapes ``main`` unrecorded, and the traceback is kept for the diagnosis.

        A capability fake makes a stage raise a plain ``RuntimeError`` -- something no halt
        class covers -- and the assertion is that it is still recorded under the named
        unexpected-error reason with a traceback, rather than propagating and leaving the
        run with no record at all.
        """
        workspace = self.workspace()
        real_verify = cli._verify_vocabularies

        def raises(record):
            raise RuntimeError("a fault no halt class covers")

        with unittest.mock.patch.object(cli, "_verify_vocabularies", raises):
            exit_code, stdout, stderr = workspace.run()

        self.assertEqual(exit_code, cli.EXIT_HALT)
        self.assertIn("unexpected error", stderr)
        self.assertIn("a fault no halt class covers", stderr)
        self.assertEqual(stdout, "")
        record = workspace.record()
        self.assertEqual(record["halt"]["reason"], cli.HALT_UNEXPECTED)
        self.assertEqual(record["halt"]["exit_code"], cli.EXIT_HALT)
        chain = record["halt"]["details"]["exception_chain"]
        self.assertTrue(chain, msg="the exception chain must be recorded")
        self.assertEqual(chain[0]["exception_type"], "RuntimeError")
        # The message is DESCRIBED, never shown (SEC-04). On an unexpected error the
        # exception's own str() is composed from whatever artifact content was being
        # processed, so what the durable record carries is the evidence -- type, full
        # length, full digest -- with the fixed redaction marker in place of the text.
        # Asserting the marker alone would pass for a description that had lost the
        # evidence too, so all four members are asserted together.
        described = chain[0]["message"]
        raised = "a fault no halt class covers"
        self.assertEqual(described["excerpt"], paths.REDACTED_TEXT)
        self.assertIs(described["redacted"], True)
        self.assertIs(described["publishable"], False)
        self.assertEqual(described["character_length"], len(raised))
        self.assertEqual(
            described["sha256"], hashlib.sha256(raised.encode("utf-8")).hexdigest()
        )
        self.assertNotIn(
            raised,
            json.dumps(described),
            msg="the description must not carry the message it describes",
        )
        self.assertNotIn(
            raised,
            record["halt"]["message"],
            msg=(
                "the halt message interpolates the description, so it must not carry "
                "the exception's own text either"
            ),
        )
        self.assertTrue(
            any("test_reconciliation.py" in frame or "cli.py" in frame
                for link in chain for frame in link["frames"]),
            msg=(
                "the frames are this repository's own source and are quoted; the "
                "exception's own message is described rather than shown, because on an "
                "unexpected error it is composed from artifact content"
            ),
        )
        self.assertEqual(record["exit_status"]["outcome"], "unexpected_error")
        self.assertIs(
            cli._verify_vocabularies,
            real_verify,
            msg="the capability fake must be undone",
        )

    def test_execute_raises_where_main_converts_and_records(self) -> None:
        """The division of labour: ``_execute`` raises, ``main`` records and returns a code.

        Both halves over the same fault. ``_execute`` must let the halt out so a caller
        embedding it keeps the exception, and ``main`` must convert it to an exit code so a
        shell can act on it -- and neither may write an output file.
        """
        workspace = self.workspace(stated_reasons=False)
        record = cli._new_record([], "2026-01-01T00:00:00Z")
        with self.assertRaises(cli.NormalizeHalt) as raised:
            cli._execute(workspace.inputs(), record)
        self.assertHalt(
            raised.exception,
            reason=cli.HALT_ABSENT_WITHOUT_STATED_REASON,
            exit_code=cli.EXIT_HALT,
        )
        self.assertEqual(
            [outcome.tool for outcome in record["artifacts"]][:1],
            [cli.ARTIFACT_ORDER[0]],
            msg="the record must hold the evidence gathered before the halt",
        )
        self.assertFalse(workspace.findings_json.exists())

        exit_code, _, stderr = workspace.run()
        self.assertEqual(exit_code, cli.EXIT_HALT)
        self.assertIn(cli.HALT_ABSENT_WITHOUT_STATED_REASON, stderr)

    def test_every_stage_of_the_composition_leaves_its_fact_in_the_record(self) -> None:
        """The stages run in one order and each records before the next begins.

        Asserted over the successful run's record: vocabularies, metadata, scan root,
        allowlist, raw directory, source index, per-artifact outcomes, severity literals,
        totals, reconciliation and outputs are each non-null. A stage that recorded nothing
        would leave a published document with an empty section and no way to tell whether
        it ran.
        """
        workspace = self.workspace()
        self.assertEqual(workspace.run()[0], cli.EXIT_OK)
        record = workspace.record()
        for key in (
            "inputs",
            "vocabularies",
            "runner_metadata",
            "scan_root",
            "allowlist",
            "raw_directory",
            "source_index",
            "severity_literals",
            "totals",
            "reconciliation",
            "output_comparison",
            "outputs",
        ):
            with self.subTest(stage=key):
                self.assertIsNotNone(record[key], msg=f"{key} recorded nothing")
        self.assertTrue(record["artifacts"])
        self.assertEqual(
            record["inputs"]["findings_json"], str(workspace.findings_json)
        )
        self.assertEqual(record["interpreter"]["executable"], sys.executable)


class PathsNotOnDiskMeasurementTest(unittest.TestCase):
    """The rows whose path names nothing on disk are measured once and published.

    AAP 0.1.1 requires the run to *"count and report the rows whose path names something
    that is not a file on disk"*, and AAP 0.6.1 puts the count and proportion in
    ``run-record.md``.  It is not the path-kind tally beside it, and the difference is the
    reason both are needed: the tally classifies a path by its **form** and needs no
    filesystem, while this asks whether the thing a row names is **actually in the pinned
    tree**.  A ``tree_file`` naming a file the pin does not carry is invisible to the first
    and counted by the second.

    A zero is the expected result for this dataset, which is exactly why the denominator
    matters: ``count 0`` beside ``rows_examined 9427`` states that nothing was found among
    9,427 rows, whereas an absent field states nothing at all.
    """

    def setUp(self) -> None:
        """A real tree with one real file in it, and one valid row to vary."""
        rows = dataset_rows()
        self.assertGreater(len(rows), 0)
        self.template = copy.deepcopy(rows[0])
        self._directory = tempfile.TemporaryDirectory(prefix="blitzy-not-on-disk-")
        self.addCleanup(self._directory.cleanup)
        self.root = Path(self._directory.name) / "spark-src"
        present = self.root / "core" / "src" / "main" / "scala" / "Present.scala"
        present.parent.mkdir(parents=True, exist_ok=True)
        present.write_text("object Present\n", encoding="utf-8")
        (self.root / "core" / "src" / "main" / "resources").mkdir(parents=True)
        self.present_path = "core/src/main/scala/Present.scala"
        self.directory_path = "core/src/main/resources"

    def row(self, path: str, tool: str = "opengrep") -> dict:
        """Return the template row carrying ``path`` and ``tool``."""
        row = copy.deepcopy(self.template)
        row["path"] = path
        row["tool"] = tool
        return row

    def measure(self, *row_paths: str) -> dict:
        """Measure over one row per path supplied."""
        return cli._paths_not_on_disk(
            [self.row(path) for path in row_paths], str(self.root)
        )

    def test_a_file_that_is_there_is_not_counted_and_the_denominator_is_real(self) -> None:
        """The zero case, with the denominator that makes it readable."""
        measured = self.measure(self.present_path, self.present_path)
        self.assertEqual(measured["count"], 0)
        self.assertEqual(measured["rows_examined"], 2)
        self.assertEqual(measured["proportion"], 0.0)
        self.assertEqual(measured["examples"], [])
        self.assertEqual(measured["by_tool"], {})
        self.assertEqual(measured["root"], str(self.root))
        self.assertFalse(measured["examples_truncated"])

    def test_a_path_absent_from_the_tree_is_counted_with_its_reason(self) -> None:
        """The case the path-kind tally cannot see: a well-formed path naming nothing."""
        measured = self.measure(self.present_path, "core/src/main/scala/Gone.scala")
        self.assertEqual(measured["count"], 1)
        self.assertEqual(measured["rows_examined"], 2)
        self.assertEqual(measured["proportion"], 0.5)
        self.assertEqual(
            measured["by_reason"][cli.NOT_ON_DISK_ABSENT_FROM_TREE], 1
        )
        self.assertEqual(measured["by_tool"], {"opengrep": 1})
        self.assertEqual(
            measured["examples"],
            [
                {
                    "tool": "opengrep",
                    "path": "core/src/main/scala/Gone.scala",
                    "reason": cli.NOT_ON_DISK_ABSENT_FROM_TREE,
                }
            ],
        )

    def test_a_directory_is_present_but_not_a_regular_file(self) -> None:
        """"Exists" is not the test; "is a file" is, and the two are distinguished."""
        measured = self.measure(self.directory_path)
        self.assertEqual(measured["count"], 1)
        self.assertEqual(
            measured["by_reason"][cli.NOT_ON_DISK_NOT_A_REGULAR_FILE], 1
        )

    def test_an_archive_member_is_counted_without_being_stat_ed(self) -> None:
        """A member inside a container is not a file in the tree, however present the jar.

        Decided from the form rather than from a stat: the joined string cannot exist by
        construction, so stat-ing it would give the right answer for the wrong reason and
        would keep giving it if the separator convention ever changed.
        """
        member = "core/target/spark-core.jar!org/apache/spark/SparkConf.class"
        self.assertEqual(
            cli._not_on_disk_reason(member, self.root),
            cli.NOT_ON_DISK_ARCHIVE_MEMBER,
        )
        measured = self.measure(member)
        self.assertEqual(measured["count"], 1)
        self.assertEqual(
            measured["by_reason"][cli.NOT_ON_DISK_ARCHIVE_MEMBER], 1
        )

    def test_a_coordinate_that_escapes_the_root_is_counted_without_being_stat_ed(self) -> None:
        """Including an interior escape, which a leading-``..`` test would miss.

        Resolving these would reach outside the root this dataset is expressed against, so
        the record says the check was refused rather than that the file was missing.
        """
        for path in (
            "../outside/File.scala",
            "core/src/main/../../../../etc/passwd",
        ):
            with self.subTest(path=path):
                self.assertEqual(
                    cli._not_on_disk_reason(path, self.root),
                    cli.NOT_ON_DISK_OUTSIDE_ROOT,
                )
        measured = self.measure("../outside/File.scala")
        self.assertEqual(measured["by_reason"][cli.NOT_ON_DISK_OUTSIDE_ROOT], 1)

    def test_an_interior_dot_dot_that_stays_inside_the_root_is_still_measured(self) -> None:
        """A path whose depth never goes negative is a tree path and is tested on disk."""
        inside = "core/src/main/scala/../scala/Present.scala"
        self.assertEqual(
            cli._not_on_disk_reason(inside, self.root),
            None,
            msg="the lexical join reaches the same real file without collapsing '..'",
        )

    def test_distinct_paths_are_classified_once_and_the_verdict_reused(self) -> None:
        """The cost is one stat per distinct path, not one per row."""
        measured = self.measure(*(["core/src/main/scala/Gone.scala"] * 40))
        self.assertEqual(measured["count"], 40)
        self.assertEqual(measured["rows_examined"], 40)
        self.assertEqual(
            measured["distinct_paths_examined"],
            1,
            msg="forty rows sharing one path cost one classification",
        )

    def test_the_examples_are_bounded_in_row_order_and_say_so_when_cut(self) -> None:
        """An unbounded list would let one misconfigured tool size the run record."""
        total = cli.PATHS_NOT_ON_DISK_EXAMPLE_LIMIT + 5
        measured = self.measure(*(f"core/src/main/scala/Gone{n}.scala" for n in range(total)))
        self.assertEqual(measured["count"], total)
        self.assertEqual(
            len(measured["examples"]), cli.PATHS_NOT_ON_DISK_EXAMPLE_LIMIT
        )
        self.assertTrue(measured["examples_truncated"])
        self.assertEqual(
            [example["path"] for example in measured["examples"]],
            [f"core/src/main/scala/Gone{n}.scala" for n in range(cli.PATHS_NOT_ON_DISK_EXAMPLE_LIMIT)],
            msg="row order, so an example can be found at that position in findings.json",
        )

    def test_the_reason_vocabulary_is_closed_and_always_fully_reported(self) -> None:
        """Every reason appears in ``by_reason`` whether or not it occurred."""
        measured = self.measure(self.present_path)
        self.assertEqual(list(measured["by_reason"]), list(cli.NOT_ON_DISK_REASONS))
        self.assertEqual(len(cli.NOT_ON_DISK_REASONS), len(set(cli.NOT_ON_DISK_REASONS)))
        self.assertIn("NOT_ON_DISK_REASONS", cli.__all__)

    def test_the_measurement_is_deterministic_and_serialisable(self) -> None:
        """Two measurements over the same rows produce the same record."""
        row_paths = (
            self.present_path,
            "core/src/main/scala/Gone.scala",
            "a.jar!b/C.class",
            "../outside/File.scala",
        )
        first = self.measure(*row_paths)
        second = self.measure(*row_paths)
        self.assertEqual(first, second)
        self.assertEqual(
            json.dumps(first, sort_keys=True), json.dumps(second, sort_keys=True)
        )
        self.assertEqual(first["count"], 3)
        self.assertEqual(first["proportion"], 0.75)

    def test_the_totals_record_publishes_it_beside_the_path_kind_tally(self) -> None:
        """Both figures are in ``totals``, and neither is the other.

        ``run-record.md`` cites this measurement from ``normalize-run.json``, so the record
        is where it has to appear; a figure computed for the console and not written down is
        a figure nothing downstream can check.
        """
        rows = [self.row(self.present_path), self.row("core/src/main/scala/Gone.scala")]
        tally = paths.PathKindTally()
        tally.add_many("tree_file", 2)
        totals = cli._totals_record([], rows, tally, str(self.root))
        self.assertIn("paths_not_on_disk", totals)
        self.assertIn("path_kinds", totals)
        measured = totals["paths_not_on_disk"]
        self.assertEqual(measured["count"], 1)
        self.assertEqual(measured["rows_examined"], 2)
        self.assertEqual(
            totals["path_kinds"]["non_filesystem"],
            0,
            msg="the form-based tally sees nothing here, which is why the second figure "
            "exists",
        )
        json.dumps(totals)


class SafeDiagnosticRenderingTest(unittest.TestCase):
    """Every persisted diagnostic goes through one renderer, and it is the safe one.

    A rejection's ``detail`` and a halt's ``message`` are composed from artifact-supplied
    text -- a rule identifier, a message, a URI, a class name -- and both are *persisted*,
    into ``harness/artifacts/logs/normalize-run.json`` and from there quoted into
    ``tool-status.md``.  Rejecting a record is not protection, because a rejected record is
    still a recorded one.  Two properties therefore have to hold at the point of
    persistence: a terminal control sequence must not survive into the record, and a URI's
    userinfo -- the only place a URI may carry a credential -- must not either.

    These assertions exercise ``paths.sanitise_diagnostic``, ``paths.safe_diagnostic``,
    ``paths.sanitise_persisted`` and the ``Rejection.as_dict()`` boundary that uses them,
    and they pin the two properties that make the treatment usable rather than merely safe:
    benign prose is recorded byte-for-byte as composed, and ``\\n``/``\\t`` survive, because
    this dataset carries messages with embedded newlines by design.
    """

    HOSTILE_URI = "https://alice:s3cr3t@example.com/a/b.tf"

    def test_benign_prose_is_recorded_exactly_as_composed(self) -> None:
        """Nothing changes for the diagnostics this pipeline actually produces.

        The whole committed run record's rejection details are ordinary prose about
        ordinary paths, so the treatment must be a no-op over them: otherwise every
        hand-verified ``detail`` in ``expected/*.rows.json`` would move, and the run record
        would stop being comparable between two runs over unchanged artifacts.
        """
        composed = (
            "no unique resolution exists for org/apache/spark/sql/Row: two distinct "
            "candidates under src/main and src/test"
        )
        rendered = paths.sanitise_diagnostic(composed)
        self.assertEqual(rendered.text, composed)
        self.assertFalse(rendered.changed)
        self.assertEqual(rendered.original_length, len(composed))
        self.assertFalse(rendered.truncated)
        self.assertEqual(rendered.controls_escaped, 0)
        self.assertEqual(rendered.userinfo_redactions, 0)

    def test_uri_userinfo_is_redacted_and_the_redaction_is_counted(self) -> None:
        """The credential goes; the marker and the count say that it did."""
        rendered = paths.sanitise_diagnostic(f"uri {self.HOSTILE_URI} is not relative")
        self.assertNotIn("s3cr3t", rendered.text)
        self.assertNotIn("alice", rendered.text)
        self.assertIn(paths.USERINFO_REDACTION, rendered.text)
        self.assertIn("example.com", rendered.text)
        self.assertEqual(rendered.userinfo_redactions, 1)
        self.assertTrue(rendered.changed)

    def test_an_address_that_is_not_uri_userinfo_is_left_alone(self) -> None:
        """Redaction is anchored on URI syntax, so evidence is not removed for nothing.

        A severity source such as ``nvd@nist.gov``, an SSH shorthand such as
        ``git@host:path`` and an ordinary address in prose are all not credentials.  A
        pattern that matched on ``@`` alone would delete each of them, which would cost
        provenance the run record is required to carry.
        """
        for text in (
            "the selected score came from nvd@nist.gov",
            "remote git@github.com:apache/spark.git",
            "reported by maintainer@example.org",
            "no scheme here://but/not/a/uri",
        ):
            with self.subTest(text=text):
                rendered = paths.sanitise_diagnostic(text)
                self.assertEqual(rendered.text, text)
                self.assertEqual(rendered.userinfo_redactions, 0)

    def test_control_characters_are_escaped_but_newline_and_tab_survive(self) -> None:
        """ESC is the injection vector; newline and tab are legitimate evidence."""
        rendered = paths.sanitise_diagnostic(
            "before\x1b[2Jafter\x00nul\x7fdel\x9fc1\nkept\tkept"
        )
        self.assertIn("<U+001B>", rendered.text)
        self.assertIn("<U+0000>", rendered.text)
        self.assertIn("<U+007F>", rendered.text)
        self.assertIn("<U+009F>", rendered.text)
        self.assertNotIn("\x1b", rendered.text)
        self.assertNotIn("\x00", rendered.text)
        self.assertIn("\nkept\tkept", rendered.text)
        self.assertEqual(rendered.controls_escaped, 4)

    def test_the_length_bound_carries_the_digest_of_the_whole_value(self) -> None:
        """A cut rendering still identifies the text it was cut from.

        The digest is of the value as composed, never of the excerpt, so two records
        carrying the same oversized value are recognisable as the same value -- which is
        the only thing that makes a bounded rendering actionable.
        """
        whole = "A" * 6_000
        rendered = paths.sanitise_diagnostic(whole, limit=paths.DIAGNOSTIC_TEXT_LIMIT)
        self.assertTrue(rendered.truncated)
        self.assertEqual(rendered.original_length, 6_000)
        self.assertIn("truncated at 2000 of 6000 characters", rendered.text)
        self.assertEqual(
            rendered.sha256,
            hashlib.sha256(whole.encode("utf-8")).hexdigest(),
            msg="the digest is of the value as composed, not of the excerpt",
        )
        self.assertLess(len(rendered.text), 2_200)
        unbounded = paths.sanitise_diagnostic(whole, limit=None)
        self.assertFalse(unbounded.truncated)
        self.assertEqual(unbounded.text, whole)

    def test_a_non_string_value_is_described_by_its_type_rather_than_refused(self) -> None:
        """The values that reach the renderer are the ones whose type was wrong.

        A mapping where a string was required *is* the fault, so refusing to render it
        would lose the diagnosis; the type name is reported separately so
        ``dict from locations[0].physicalLocation`` reads as the shape fault it is.
        """
        rendered = paths.safe_diagnostic(
            {"uri": "x"}, context="locations[0].physicalLocation"
        )
        self.assertEqual(rendered.value_type, "dict")
        self.assertEqual(rendered.context, "locations[0].physicalLocation")
        self.assertIn("dict from locations[0].physicalLocation", str(rendered))
        for value in (None, 12, 4.5, True, ["a"], ("b",)):
            with self.subTest(value=value):
                described = paths.safe_diagnostic(value)
                self.assertEqual(described.value_type, type(value).__name__)
                self.assertEqual(described.character_length, len(repr(value)))
                json.dumps(described.as_dict())

    def test_a_described_value_never_shows_the_credential_it_carried(self) -> None:
        """``safe_diagnostic`` is what replaces ``{value!r}``, so it must not leak either."""
        rendered = paths.safe_diagnostic(self.HOSTILE_URI, context="artifactLocation.uri")
        self.assertNotIn("s3cr3t", rendered.excerpt)
        self.assertNotIn("s3cr3t", str(rendered))
        self.assertNotIn("s3cr3t", json.dumps(rendered.as_dict()))
        self.assertEqual(rendered.userinfo_redactions, 1)
        self.assertEqual(rendered.character_length, len(self.HOSTILE_URI))

    def test_the_rendering_is_deterministic(self) -> None:
        """Two runs over the same value produce the same record, field for field."""
        hostile = f"{self.HOSTILE_URI}\x1b[2J{'B' * 4_000}"
        first = paths.safe_diagnostic(hostile, context="ctx").as_dict()
        second = paths.safe_diagnostic(hostile, context="ctx").as_dict()
        self.assertEqual(first, second)
        self.assertEqual(
            json.dumps(first, sort_keys=True), json.dumps(second, sort_keys=True)
        )

    def test_sanitise_persisted_recurses_through_mappings_lists_and_keys(self) -> None:
        """A value added to a detail mapping later is covered without anyone remembering.

        Keys are treated too: an artifact-supplied value can reach a key -- a counter keyed
        by an observed section name -- and a control character in a JSON key is exactly as
        hostile to a reader as one in a value.
        """
        rendered = paths.sanitise_persisted(
            {
                "uri": self.HOSTILE_URI,
                "nested": {"list": ["ok", "bad\x1bhere"], "count": 3},
                "esc\x1bkey": "value",
                "flag": True,
                "absent": None,
            }
        )
        self.assertNotIn("s3cr3t", json.dumps(rendered))
        self.assertNotIn("\x1b", json.dumps(rendered))
        self.assertIn("esc<U+001B>key", rendered)
        self.assertEqual(rendered["nested"]["count"], 3)
        self.assertIs(rendered["flag"], True)
        self.assertIsNone(rendered["absent"])
        self.assertEqual(rendered["nested"]["list"][0], "ok")

    def test_a_rejection_record_is_sanitised_while_the_attribute_is_not(self) -> None:
        """The boundary is ``as_dict``, so an assertion still reads what the adapter said.

        That split is the whole reason the treatment could be applied at all: sanitising at
        each of the sites that compose a detail would rewrite the hand-verified ``detail``
        strings in every ``expected/*.rows.json`` for no security gain.
        """
        hostile = f"uri {self.HOSTILE_URI} carries \x1b[2J"
        rejection = paths.make_rejection(
            paths.REJECT_INVALID_URI, "opengrep", hostile, uri=self.HOSTILE_URI
        )
        self.assertEqual(rejection.detail, hostile)
        record = rejection.as_dict()
        self.assertNotIn("s3cr3t", json.dumps(record))
        self.assertNotIn("\x1b", json.dumps(record))
        self.assertIn("diagnostics", record)
        self.assertEqual(record["diagnostics"]["userinfo_redactions"], 1)
        self.assertEqual(record["diagnostics"]["controls_escaped"], 1)
        self.assertEqual(record["diagnostics"]["original_length"], len(hostile))

    def test_a_benign_rejection_record_carries_no_diagnostics_key(self) -> None:
        """The extra key appears only where something was changed.

        Every rejection in the committed run record is benign, so the record's shape is
        unchanged for all 585 of them -- which is what keeps two runs comparable.
        """
        record = paths.make_rejection(
            paths.REJECT_ABSENT_PATH,
            "checkov",
            "the record carries no file_path",
            check_id="CKV_DOCKER_3",
        ).as_dict()
        self.assertEqual(
            sorted(record), ["detail", "record_identity", "reject_class", "tool"]
        )
        self.assertEqual(record["detail"], "the record carries no file_path")

    def test_a_bad_limit_or_context_is_a_caller_fault(self) -> None:
        """A programming fault raises rather than being silently coerced."""
        for limit in (0, -1, "512", 1.5, True):
            with self.subTest(limit=limit):
                with self.assertRaises(paths.PathPolicyError):
                    paths.sanitise_diagnostic("text", limit=limit)
                with self.assertRaises(paths.PathPolicyError):
                    paths.safe_diagnostic("text", limit=limit)
        with self.assertRaises(paths.PathPolicyError):
            paths.sanitise_diagnostic(b"bytes")  # type: ignore[arg-type]
        with self.assertRaises(paths.PathPolicyError):
            paths.safe_diagnostic("text", context=7)  # type: ignore[arg-type]

    def test_the_renderer_is_exported_from_the_module_adapters_may_import(self) -> None:
        """AAP 0.6.4 fixes that an adapter depends only on ``paths`` and ``severity``.

        So the renderer lives in ``paths`` and is public there.  A new module would have to
        be imported by six adapters and by ``cli``, and an adapter importing ``cli`` would
        make the graph cyclic -- both were refused.
        """
        for name in (
            "DIAGNOSTIC_TEXT_LIMIT",
            "DIAGNOSTIC_VALUE_LIMIT",
            "USERINFO_REDACTION",
            "DiagnosticText",
            "SafeDiagnostic",
            "sanitise_diagnostic",
            "safe_diagnostic",
            "sanitise_persisted",
        ):
            with self.subTest(name=name):
                self.assertIn(name, paths.__all__)
                self.assertTrue(hasattr(paths, name))


class PersistedDiagnosticSafetyTest(unittest.TestCase):
    """``cli.py``'s own three persisted-string sites carry no raw artifact text.

    AAP 0.5.4 requires a halt diagnosable from the record, and the record is durable, so the
    halt is where externally supplied text most easily becomes a persisted secret or a
    persisted control sequence.  Three sites are covered here: the halt boundary
    (:meth:`cli.NormalizeHalt.as_dict` and :attr:`cli.NormalizeHalt.safe_message`), the
    adapter-contract halt whose text an adapter composed from the artifact, and the
    unexpected-error path -- the one nobody designed, and therefore the one that must not be
    the path that writes ``traceback.format_exc()`` into the record.
    """

    HOSTILE = "artifact said https://bob:t0ken@registry.example/x\x1b[2Jand more"

    def test_a_halt_record_redacts_and_escapes_its_message(self) -> None:
        """The message reaches the record through the one renderer."""
        halt = cli.NormalizeHalt(cli.HALT_UNEXPECTED, self.HOSTILE)
        record = halt.as_dict()
        self.assertNotIn("t0ken", record["message"])
        self.assertNotIn("\x1b", record["message"])
        self.assertIn(paths.USERINFO_REDACTION, record["message"])
        self.assertEqual(
            halt.message, self.HOSTILE, "the exception's own attribute is untouched"
        )

    def test_safe_message_is_what_reaches_the_terminal(self) -> None:
        """A control sequence printed to a terminal is the more immediate hazard."""
        halt = cli.NormalizeHalt(cli.HALT_UNEXPECTED, self.HOSTILE)
        self.assertEqual(halt.safe_message, halt.as_dict()["message"])
        self.assertNotEqual(halt.safe_message, halt.message)
        multiline = cli.NormalizeHalt(cli.HALT_UNEXPECTED, "one\ntwo\tthree")
        self.assertEqual(
            multiline.safe_message,
            "one\ntwo\tthree",
            msg="authored multi-line prose still reads as one message",
        )

    def test_a_halt_record_sanitises_nested_details(self) -> None:
        """Details are structures, so the treatment recurses through them."""
        halt = cli.NormalizeHalt(
            cli.HALT_UNEXPECTED,
            "message",
            details={
                "observed": {"uri": self.HOSTILE, "count": 2},
                "candidates": [self.HOSTILE, "ok"],
                "exit_code": 128,
            },
        )
        serialised = json.dumps(halt.as_dict())
        self.assertNotIn("t0ken", serialised)
        self.assertNotIn("\x1b", serialised)
        self.assertEqual(halt.as_dict()["details"]["observed"]["count"], 2)
        self.assertEqual(halt.as_dict()["details"]["exit_code"], 128)
        self.assertEqual(
            halt.details["observed"]["uri"],
            self.HOSTILE,
            msg="the exception's own details are untouched",
        )

    def test_a_halt_details_verbatim_excerpt_is_not_truncated_at_the_boundary(self) -> None:
        """Length is bounded where the evidence contract states it, not here.

        A runner's own words are bounded at :data:`cli.TOOL_WORDS_EXCERPT_LIMIT` with the
        byte size and sha256 recorded beside them, which AAP 0.5.4 requires quoted verbatim.
        Re-bounding at the boundary would cut that evidence, so the boundary redacts and
        escapes and leaves lengths alone.
        """
        words = "No package sources found, --help for usage information.\n" * 200
        halt = cli.NormalizeHalt(
            cli.HALT_ABSENT_WITHOUT_STATED_REASON, "m", details={"excerpt": words}
        )
        self.assertEqual(halt.as_dict()["details"]["excerpt"], words)
        self.assertGreater(len(words), paths.DIAGNOSTIC_TEXT_LIMIT)

    def test_the_unexpected_error_path_never_calls_traceback_format_exc(self) -> None:
        """Structural, because the hazard is a call nobody notices being reintroduced.

        ``traceback.format_exc()``'s final line is the exception's own ``str()``, which on
        an unexpected error is composed from whatever artifact content was being processed.
        The frames are safe and are rendered from ``traceback.format_tb``; the message is
        described instead.
        """
        tree = ast.parse(Path(cli.__file__).read_text(encoding="utf-8"))
        called = {
            f"{node.func.value.id}.{node.func.attr}"
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
        }
        self.assertIn("traceback.format_tb", called)
        self.assertNotIn("traceback.format_exc", called)
        self.assertNotIn("traceback.print_exc", called)

    def test_the_exception_chain_describes_each_message_rather_than_quoting_it(self) -> None:
        """The rendered chain carries a description, never the exception's own text."""
        try:
            raise ValueError(self.HOSTILE)
        except ValueError as error:
            chain = cli._safe_exception_chain(error)
        self.assertEqual(len(chain), 1)
        link = chain[0]
        self.assertEqual(link["exception_type"], "ValueError")
        self.assertIsNone(link["linked_by"])
        self.assertNotIn("t0ken", json.dumps(link))
        self.assertNotIn("\x1b", json.dumps(link))
        self.assertEqual(link["message"]["character_length"], len(self.HOSTILE))
        self.assertEqual(link["message"]["userinfo_redactions"], 1)
        self.assertTrue(link["frames"])
        self.assertFalse(link["frames_truncated"])

    def test_the_exception_chain_walks_a_cause_and_prefers_it_over_a_context(self) -> None:
        """``raise ... from`` is this module's idiom, and the artifact text is on the cause."""
        try:
            try:
                raise KeyError(self.HOSTILE)
            except KeyError as inner:
                raise cli.NormalizeHalt(cli.HALT_UNEXPECTED, "wrapper") from inner
        except cli.NormalizeHalt as error:
            chain = cli._safe_exception_chain(error)
        self.assertEqual(len(chain), 2)
        self.assertEqual(chain[0]["exception_type"], "NormalizeHalt")
        self.assertEqual(chain[1]["exception_type"], "KeyError")
        self.assertEqual(chain[1]["linked_by"], "cause")
        self.assertNotIn("t0ken", json.dumps(chain))

    def test_the_exception_chain_is_bounded_and_terminates(self) -> None:
        """A pathological chain cannot decide how large this pipeline's record is."""
        error: BaseException = ValueError("depth 0")
        for depth in range(1, 30):
            wrapper = ValueError(f"depth {depth}")
            wrapper.__cause__ = error
            error = wrapper
        chain = cli._safe_exception_chain(error)
        self.assertEqual(len(chain), cli.UNEXPECTED_ERROR_CHAIN_MAX_DEPTH)
        cyclic = ValueError("self")
        cyclic.__cause__ = cyclic
        cyclic_chain = cli._safe_exception_chain(cyclic)
        self.assertEqual(len(cyclic_chain), 2)
        self.assertIn("walk stopped", cyclic_chain[1]["note"])
        json.dumps(cyclic_chain)

    def test_the_adapter_contract_halt_describes_the_adapter_error(self) -> None:
        """An adapter composes its error text from the artifact, so the halt describes it.

        The halt is raised through the real ``cli._process_present_artifact``, with one
        adapter's ``adapt`` replaced by a function that raises the kind of contract error
        the adapters really do raise -- a ``ValueError`` whose message quotes an observed
        value.  Asserting over the real route is what makes this a test of the halt rather
        than of a rendering helper.
        """
        directory = tempfile.TemporaryDirectory(prefix="blitzy-adapter-contract-")
        self.addCleanup(directory.cleanup)
        base = Path(directory.name)
        environment = Environment(base)
        raw_dir = base / "raw"
        raw_dir.mkdir()
        artifact_path = raw_dir / "gitleaks.json"
        artifact_path.write_text("[]\n", encoding="utf-8")

        original = gitleaks.adapt

        def refusing(*_args: object, **_keywords: object) -> None:
            raise ValueError(f"observed {self.HOSTILE}")

        gitleaks.adapt = refusing  # type: ignore[assignment]
        self.addCleanup(lambda: setattr(gitleaks, "adapt", original))

        outcome = cli.ArtifactOutcome(
            tool="gitleaks",
            scanner_class="secret",
            artifact_filename="gitleaks.json",
            present=True,
            parse_status=cli.PARSE_STATUS_CLEAN,
            artifact={"path": str(artifact_path), "present": True},
        )
        with self.assertRaises(cli.NormalizeHalt) as raised:
            cli._process_present_artifact(
                "gitleaks",
                artifact_path,
                metadata=environment.metadata,
                root=environment.root,
                globs=environment.globs,
                tally=severity.LiteralTally(),
                source_index=None,
                log_dir=base,
                outcome=outcome,
            )
        halt = raised.exception
        self.assertEqual(halt.reason, cli.HALT_ADAPTER_CONTRACT)
        serialised = json.dumps(halt.as_dict())
        self.assertNotIn("t0ken", serialised)
        self.assertNotIn("\x1b", serialised)
        self.assertEqual(halt.details["error_type"], "ValueError")
        self.assertEqual(halt.details["error"]["userinfo_redactions"], 1)
        self.assertIn("ValueError", halt.as_dict()["message"])
        self.assertEqual(outcome.parse_status, cli.PARSE_STATUS_FAILED)

    def test_a_stack_trace_halt_still_leaves_the_stream_file_byte_exact(self) -> None:
        """The safe rendering governs the record, never the runner's own log file.

        AAP 0.5.4 requires both streams preserved verbatim, and the file on disk is that
        preservation.  This asserts the two treatments stay separate: the halt's message and
        details are sanitised, the file is not touched at all.
        """
        directory = tempfile.TemporaryDirectory(prefix="blitzy-stream-verbatim-")
        self.addCleanup(directory.cleanup)
        log_dir = Path(directory.name)
        trace = (
            "Traceback (most recent call last):\n"
            '  File "/opt/osv/main.go", line 1, in main\n'
            f"    connect(\x1b[2J'{self.HOSTILE}')\n"
            "RuntimeError: could not reach the OSV API\n"
        )
        (log_dir / "osv-scanner.stderr.log").write_text(trace, encoding="utf-8")
        outcome = cli.ArtifactOutcome(
            tool="osv-scanner",
            scanner_class="vuln",
            artifact_filename="osv-scanner.json",
            present=False,
            parse_status=cli.PARSE_STATUS_ABSENT,
            artifact={"path": str(log_dir / "raw" / "osv-scanner.json"), "present": False},
            artifact_expected=False,
        )
        with self.assertRaises(cli.NormalizeHalt) as raised:
            cli._process_absent_artifact(
                "osv-scanner",
                root=str(log_dir / "spark-src"),
                log_dir=log_dir,
                outcome=outcome,
            )
        halt = raised.exception
        self.assertEqual(halt.reason, cli.HALT_ABSENT_WITHOUT_NO_WORK_STATEMENT)
        self.assertNotIn("t0ken", json.dumps(halt.as_dict()))
        self.assertEqual(
            (log_dir / "osv-scanner.stderr.log").read_text(encoding="utf-8"),
            trace,
            msg="the stream file is left exactly as the runner wrote it",
        )


class StartLineBoundaryTest(unittest.TestCase):
    """A present ``start_line`` is at least 1, at the final boundary as well as upstream.

    Line numbering is one-based in every producer this dataset reads -- SARIF 2.1.0 fixes
    ``region.startLine`` at 1 for the first line, and Trivy, Gitleaks and Checkov all count
    from 1.  So ``0`` is not a small line number: it is either an absence written as a
    sentinel, which this module's own convention forbids (absence is ``null`` in JSON and an
    empty CSV field), or a producer's off-by-one.  A reader cannot tell which, so neither
    may be written.

    The adapters already reject such a record under ``non_integer_start_line``, and that is
    exactly why this is asserted at ``emit.py``'s validator: the final boundary must refuse
    independently rather than on the assumption that no adapter will ever regress.  The
    reader is held to the same rule, because a reader that accepted ``0`` would coerce it
    into an integer the JSON side cannot hold and the field-for-field comparison would then
    report a disagreement whose cause nobody could see.
    """

    def setUp(self) -> None:
        """One valid committed row to vary, and a temporary directory to write into."""
        rows = dataset_rows()
        self.assertGreater(len(rows), 0)
        self.template = copy.deepcopy(rows[0])
        self._directory = tempfile.TemporaryDirectory(prefix="blitzy-start-line-")
        self.addCleanup(self._directory.cleanup)
        self.output = Path(self._directory.name)

    def row_with(self, start_line: object) -> dict:
        """Return the template row carrying ``start_line``."""
        row = copy.deepcopy(self.template)
        row["start_line"] = start_line
        return row

    def test_line_zero_is_refused_by_the_final_validator(self) -> None:
        """The row is refused rather than written, and the message says why."""
        with self.assertRaises(emit.EmitError) as raised:
            emit.validate_rows([self.row_with(0)])
        message = str(raised.exception)
        self.assertIn("start_line", message)
        self.assertIn("at least 1", message)
        self.assertIn("one-based", message)

    def test_a_negative_line_is_refused_too(self) -> None:
        """The stricter bound cannot be reached by widening it back to non-negative."""
        for value in (-1, -100):
            with self.subTest(value=value):
                with self.assertRaises(emit.EmitError):
                    emit.validate_rows([self.row_with(value)])

    def test_line_one_is_the_accepted_boundary(self) -> None:
        """1 is the first line, so the bound is exclusive of 0 and inclusive of 1."""
        validated = emit.validate_rows([self.row_with(1)])
        self.assertEqual(validated[0]["start_line"], 1)
        absent = emit.validate_rows([self.row_with(None)])
        self.assertIsNone(absent[0]["start_line"], "absence is still None, never 0")

    def test_the_reader_refuses_a_zero_cell_and_a_leading_zero(self) -> None:
        """The reader is exactly as strict as the writer, so the two cannot disagree."""
        csv_path = self.output / "findings.csv"
        header = ",".join(emit.FIELDS)
        for cell in ("0", "01", "007"):
            with self.subTest(cell=cell):
                values = []
                for field in emit.FIELDS:
                    if field == "start_line":
                        values.append(cell)
                    elif field == "in_scope":
                        values.append(emit.CSV_TRUE)
                    else:
                        values.append("x")
                csv_path.write_text(
                    f"{header}\n{','.join(values)}\n", encoding="utf-8"
                )
                with self.assertRaises(emit.EmitError) as raised:
                    emit.read_findings_csv(csv_path)
                self.assertIn("at least 1", str(raised.exception))

    def test_a_written_row_still_round_trips_through_both_files(self) -> None:
        """The stricter bound changes nothing for a legitimate line number."""
        json_path = self.output / "findings.json"
        csv_path = self.output / "findings.csv"
        emit.write_findings([self.row_with(72)], json_path, csv_path)
        comparison = emit.compare_outputs(json_path, csv_path)
        comparison.raise_if_failed()
        self.assertEqual(emit.read_findings_csv(csv_path)[0]["start_line"], 72)


class PathKindBulkTallyTest(unittest.TestCase):
    """Path-kind counts are folded in one validated step per kind, never replayed.

    An adapter's counters are already aggregated: it reports ``path_kind_tree_file: 1322``,
    not 1,322 observations.  ``PathKindTally.add_many`` folds such a number in one step, and
    this class holds that step to the count it stands for: a bulk fold agrees with adding
    the kind one at a time, both in the per-artifact tally and in the dataset tally it is
    merged into.  Expanding an aggregate back into ``add`` calls would re-enumerate every
    resolution in the dataset to recompute a sum already known, twice over.

    Validation is the reason the fold is a method on the tally rather than a dict update in
    ``cli.py``: the kind is checked against the same closed set ``add`` checks it against,
    so the tally cannot drift from ``paths.NON_FILESYSTEM_PATH_KINDS``.
    """

    def test_add_many_is_equivalent_to_that_many_add_calls(self) -> None:
        """One fold of ``count`` equals ``count`` separate ``add`` calls, kind by kind."""
        counts = {"tree_file": 1322, "outside_root": 7, "archive_member": 3}
        replayed = paths.PathKindTally()
        for kind, count in counts.items():
            for _ in range(count):
                replayed.add(kind)
        bulk = paths.PathKindTally()
        for kind, count in counts.items():
            bulk.add_many(kind, count)
        self.assertEqual(bulk.counts, replayed.counts)
        self.assertEqual(bulk.as_dict(), replayed.as_dict())
        self.assertEqual(bulk.total, 1332)
        self.assertEqual(bulk.non_filesystem, replayed.non_filesystem)

    def test_add_many_accumulates_and_accepts_zero(self) -> None:
        """Repeated folds sum, and a zero is a no-op so a whole mapping can be folded."""
        tally = paths.PathKindTally()
        tally.add_many("tree_file", 4)
        tally.add_many("tree_file", 6)
        tally.add_many("outside_root", 0)
        self.assertEqual(tally.counts, {"tree_file": 10})
        self.assertEqual(tally.as_dict()["by_kind"]["outside_root"], 0)

    def test_add_many_validates_the_kind_against_the_closed_set(self) -> None:
        """A bulk operation must not be a way around the discriminator."""
        tally = paths.PathKindTally()
        with self.assertRaises(paths.PathPolicyError) as raised:
            tally.add_many("not_a_kind", 3)
        self.assertIn("unknown path kind", str(raised.exception))
        self.assertEqual(tally.counts, {}, "a refused kind counts nothing")

    def test_add_many_refuses_a_negative_or_non_integer_count(self) -> None:
        """A tally that can go backwards can be balanced by two opposite mistakes.

        So a negative count is refused rather than clamped to zero: a clamp keeps counting
        and leaves the reported non-filesystem proportion wrong with nothing recording why.
        A non-``int`` is refused for the same reason, ``bool`` included, since
        ``add_many(kind, True)`` reads as a flag rather than as a count of one.
        """
        tally = paths.PathKindTally()
        for count in (-1, -1322):
            with self.subTest(count=count):
                with self.assertRaises(paths.PathPolicyError) as raised:
                    tally.add_many("tree_file", count)
                self.assertIn("must not be negative", str(raised.exception))
        for count in ("3", 3.0, None, True):
            with self.subTest(count=count):
                with self.assertRaises(paths.PathPolicyError) as raised:
                    tally.add_many("tree_file", count)
                self.assertIn("must be an int", str(raised.exception))
        self.assertEqual(tally.counts, {})

    def test_the_cli_builds_and_merges_through_the_bulk_operation(self) -> None:
        """Asserted over the real ``cli`` helpers, on counters shaped like an adapter's."""
        counters = {
            "path_kind_tree_file": 1322,
            "path_kind_outside_root": 5,
            "multi_location_records": 11,
            "severity_basis_sarif_level": 900,
        }
        tally = cli._path_kind_tally(counters)
        self.assertEqual(tally.counts, {"tree_file": 1322, "outside_root": 5})
        self.assertEqual(tally.total, 1327, "no non-path-kind counter is folded in")
        total = paths.PathKindTally()
        cli._merge_path_kinds(total, counters)
        cli._merge_path_kinds(total, {"path_kind_archive_member": 2})
        self.assertEqual(
            total.counts, {"tree_file": 1322, "outside_root": 5, "archive_member": 2}
        )
        self.assertEqual(total.as_dict()["non_filesystem"], 7)

    def test_the_dataset_level_tally_is_the_sum_of_the_per_artifact_ones(self) -> None:
        """AAP 0.6.4: a count that appears twice is one measurement cited twice."""
        per_artifact = [
            {"path_kind_tree_file": 1322},
            {"path_kind_tree_file": 1162, "path_kind_outside_root": 4},
            {"path_kind_bytecode_source": 107},
        ]
        total = paths.PathKindTally()
        summed = 0
        for counters in per_artifact:
            one = cli._path_kind_tally(counters)
            summed += one.total
            cli._merge_path_kinds(total, counters)
        self.assertEqual(total.total, summed)
        self.assertEqual(total.total, 2595)


class SeverityProvenanceRecordTest(unittest.TestCase):
    """The selected score entry reaches the run record, per literal, per tool.

    AAP 0.5.4 requires that *"either way the entry used is recorded -- the label, or the
    score with its source and version"*, and AAP 0.6.4 fixes the direction that requirement
    travels in: ``cli._severity_record`` drains the tally into
    ``harness/artifacts/logs/normalize-run.json``, and ``oss-scan-results/severity-map.md``
    is written from that record. So a selection that survives the tally and is then dropped
    by the serialiser is a selection the document still cannot state.

    This class asserts the serialisation boundary itself, in the module that already drives
    ``cli.py``: that the record publishes the field list a literal is keyed on, that each
    serialised literal carries the four provenance columns, that two selections rendering one
    literal arrive as two entries, and that the whole record is JSON-serialisable and
    deterministic -- the property a regenerated document depends on.
    """

    def tally_with(self, *results: severity.SeverityResult) -> severity.LiteralTally:
        """Return a tally seeded with all nine identifiers, fed the given results."""
        tally = severity.LiteralTally.with_all_tools()
        for result in results:
            tally.record("dependency-check", result)
        return tally

    def test_the_record_publishes_the_field_list_a_literal_is_keyed_on(self) -> None:
        """``literal_key`` names the identity columns, so no consumer has to infer them."""
        record = cli._severity_record(self.tally_with())
        self.assertEqual(record["literal_key"], list(severity.LITERAL_KEY_FIELDS))
        self.assertEqual(
            record["selected_entry_policy"],
            severity.POLICY_SELECTED_ENTRY_TALLY,
            "the authored contract is quoted from severity.py rather than restated here",
        )
        self.assertEqual(
            len(record["tools"]), 9, "all nine identifiers reach the record"
        )
        self.assertEqual(
            record["policy_statements"],
            [name for name, _ in severity.POLICY_STATEMENTS],
            "the four mapping statements are unchanged by the provenance columns",
        )

    def test_two_scores_rendering_one_literal_reach_the_record_as_two_entries(self) -> None:
        """The collapse is closed at the serialisation boundary too, with sources named.

        Both results render ``severity_native`` ``"7.5"`` and band ``High`` on the
        ``cvss_score`` basis, so a record keyed on those three alone would carry one literal
        naming one source, with nothing to show the other had been counted into it.
        """
        record = cli._severity_record(
            self.tally_with(
                severity.resolve(
                    scores=[{"score": 7.5, "source": "NVD:cvssv3", "version": "3.1"}]
                ),
                severity.resolve(
                    scores=[{"score": 7.5, "source": "redhat", "version": "3.1"}]
                ),
                severity.resolve(
                    scores=[{"score": 7.5, "source": "redhat", "version": "3.1"}]
                ),
            )
        )
        literals = record["tools"]["dependency-check"]["literals"]
        self.assertEqual(len(literals), 2, f"two selections, two entries: {literals!r}")
        self.assertEqual({literal["severity_native"] for literal in literals}, {"7.5"})
        self.assertEqual(
            [
                (
                    literal["selected_source"],
                    literal["selected_version"],
                    literal["selected_score"],
                    literal["rows"],
                )
                for literal in literals
            ],
            [("NVD:cvssv3", "3.1", 7.5, 1), ("redhat", "3.1", 7.5, 2)],
            "each entry names its own source, version and score, and counts its own rows",
        )
        self.assertEqual(
            record["tools"]["dependency-check"]["rows"],
            3,
            "the row total is unaffected by the split",
        )
        self.assertEqual(record["total_rows"], 3)

    def test_every_serialised_literal_carries_every_key_column(self) -> None:
        """No literal in the record may omit a provenance column, on any basis.

        A column present on the score path and absent on the label path would make the
        document's table ragged and its absences unreadable: absent-because-nothing-was-used
        and absent-because-the-serialiser-dropped-it would look the same.
        """
        record = cli._severity_record(
            self.tally_with(
                severity.resolve(label="HIGH"),
                severity.resolve(sarif_level="error"),
                severity.resolve(scores=[{"score": 0.0, "source": "nvd"}]),
                severity.SeverityResult.unmapped("catastrophic"),
                severity.SeverityResult.absent(),
            )
        )
        literals = record["tools"]["dependency-check"]["literals"]
        self.assertEqual(len(literals), 5, "five distinct selections")
        for literal in literals:
            with self.subTest(basis=literal["basis"], native=literal["severity_native"]):
                for column in severity.LITERAL_KEY_FIELDS:
                    self.assertIn(column, literal)
        by_basis = {literal["basis"]: literal for literal in literals}
        self.assertEqual(by_basis[severity.BASIS_LABEL]["selected_label"], "HIGH")
        self.assertEqual(by_basis[severity.BASIS_SARIF_LEVEL]["selected_label"], "error")
        self.assertEqual(by_basis[severity.BASIS_CVSS_SCORE]["selected_source"], "nvd")
        for basis in (severity.BASIS_NO_VOCABULARY, severity.BASIS_UNMAPPED_LITERAL):
            for column in severity.LITERAL_KEY_FIELDS[3:]:
                self.assertIsNone(
                    by_basis[basis][column],
                    f"{basis} used nothing, so {column} states no selection",
                )
        self.assertEqual(
            [literal["severity_native"] for literal in record["tools"]["dependency-check"]["unmapped_literals"]],
            ["catastrophic"],
            "the unmapped disclosure is unchanged by the new columns",
        )

    def test_the_whole_record_is_json_serialisable_and_deterministic(self) -> None:
        """Two serialisations of one record are byte-identical, columns included.

        ``normalize-run.json`` is written once per run and compared between runs; a float or
        an optional column that serialised unstably would make that comparison useless.
        """
        tally = self.tally_with(
            severity.resolve(
                scores=[{"score": 3.200000047683716, "source": "nvd", "version": "3.1"}]
            ),
            severity.resolve(label="moderate"),
            severity.SeverityResult.absent(),
        )
        record = cli._severity_record(tally)
        first = json.dumps(record, indent=2, sort_keys=True)
        second = json.dumps(cli._severity_record(tally), indent=2, sort_keys=True)
        self.assertEqual(first, second)
        self.assertEqual(json.loads(first), record)
        literals = record["tools"]["dependency-check"]["literals"]
        rendered = {
            literal["severity_native"]: literal["selected_score"] for literal in literals
        }
        self.assertEqual(
            rendered["3.2"],
            3.200000047683716,
            "the full-precision score is what the column carries; severity_native carries "
            "the one-decimal rendering, which is why the two cannot be conflated",
        )


class SharedRowWriterContractTest(unittest.TestCase):
    """Both writers consume the same validated rows, and neither copies them.

    AAP 0.6.1 requires *"both writers consume the same validated rows"*, which is what makes
    the typed re-parse comparison an assertion about one row set rather than about two. A
    defensive copy in either writer would quietly weaken it, and a gratuitous copy of the
    9,427-row list buys nothing: the writers only read.
    """

    def setUp(self) -> None:
        """A small validated row set and a temporary directory to write into."""
        rows = dataset_rows()
        self.rows = emit.validate_rows(rows[:5])
        self._directory = tempfile.TemporaryDirectory(prefix="blitzy-shared-rows-")
        self.addCleanup(self._directory.cleanup)
        self.output = Path(self._directory.name)

    def test_the_json_writer_serialises_the_production_list_without_copying_it(self) -> None:
        """Asserted by identity: the object handed to ``json.dump`` is the caller's list."""
        captured: list[object] = []
        original = json.dump

        def recording(obj: object, *args: object, **keywords: object) -> object:
            captured.append(obj)
            return original(obj, *args, **keywords)  # type: ignore[arg-type]

        json.dump = recording  # type: ignore[assignment]
        self.addCleanup(lambda: setattr(json, "dump", original))
        with io.StringIO() as handle:
            emit._render_json(self.rows, handle)
        self.assertEqual(len(captured), 1)
        self.assertIs(
            captured[0],
            self.rows,
            msg="the validated list is serialised in place, not shallow-copied",
        )

    def test_a_foreign_sequence_is_still_materialised(self) -> None:
        """``json`` serialises only ``list`` and ``tuple``, so anything else is converted.

        Dropping the conversion entirely would raise part-way through a write and leave a
        partial staged file behind, so the copy is made conditional rather than removed.
        The seam is ``emit._render_json``, which renders into the handle the publication
        protocol opened: the staging and promotion around it are asserted by
        ``test_emit_publication``, and what is asserted here is the row set the renderer
        consumes.
        """

        class Rows:
            """A sequence that is neither a ``list`` nor a ``tuple``."""

            def __init__(self, items: list) -> None:
                self._items = items

            def __getitem__(self, index: int) -> object:
                return self._items[index]

            def __len__(self) -> int:
                return len(self._items)

        with io.StringIO() as handle:
            emit._render_json(Rows(self.rows), handle)
            rendered = handle.getvalue()
        self.assertEqual(json.loads(rendered), self.rows)
        self.assertEqual(
            sorted(entry.name for entry in self.output.iterdir()),
            [],
            msg="rendering writes into the handle it was given and nowhere else",
        )

    def test_both_writers_see_the_same_row_objects(self) -> None:
        """The shared-row contract, asserted by identity through the public entry point."""
        json_path = self.output / "findings.json"
        csv_path = self.output / "findings.csv"
        returned = emit.write_findings(self.rows, json_path, csv_path)
        self.assertEqual(returned, self.rows)
        comparison = emit.compare_outputs(json_path, csv_path)
        comparison.raise_if_failed()

if __name__ == "__main__":  # pragma: no cover - convenience for a direct run
    unittest.main()
