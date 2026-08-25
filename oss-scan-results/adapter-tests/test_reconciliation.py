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
No user-specified rules govern this file: ``review_rules`` reports "No user rules
provided.", which AAP 0.7 and AAP 0.10.2 corroborate.  Enterprise-standard best practice
applies in their place and the absence is not licence to lower the bar -- which is why the
independence above is established structurally and every mandated rejection path is
asserted rather than assumed.

Running it
----------
Standard library only, no ``pytest``, and runnable from any working directory::

    python3 -m unittest discover -s oss-scan-results/adapter-tests -p 'test_reconciliation.py'
"""

from __future__ import annotations

import ast
import copy
import csv
import inspect
import json
import sys
import tempfile
import types
import unittest
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

from normalize import cli, emit, paths, reconcile, severity  # noqa: E402
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
#: line-count assertion rather than taken from an artifact: no committed fixture's message
#: carries a newline, and the precedent dataset's did (10,178 parsed rows across 12,760
#: physical lines). It carries no secret and no tool's real output.
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
        """Passes, skips and parsing errors are not findings and never reach the count."""
        document = load_fixture("checkov.json")
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
        document = load_fixture("trivy.json")
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
        for expectation in self.expected["rejections"]["expectations"]:
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
        """The 10,178-rows-across-12,760-lines lesson, encoded as an assertion.

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


if __name__ == "__main__":  # pragma: no cover - convenience for a direct run
    unittest.main()
