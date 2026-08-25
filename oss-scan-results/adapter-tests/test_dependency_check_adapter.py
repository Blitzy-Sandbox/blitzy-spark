"""Assert the OWASP Dependency-Check native adapter field by field, and its rejections by class.

What this module tests
----------------------
``harness/lib/normalize/adapters/dependency_check.py``, the adapter for the one tool whose
canonical identifier is ``dependency-check`` (13.0.0 in this provisioning).  AAP 0.6.1 gives
this file its row -- *"Asserts absolute-path relativization, the package-coordinate candidate
precedence, and label-over-score precedence with the selected entry recorded"* -- AAP 0.5.4
fixes the behaviour, and AAP 0.9.4 puts it in the definition of done.  A failure here
**halts the run** (AAP 0.9.2), so no assertion below is weakened to pass: where an assertion
and the adapter disagree, the disagreement is the finding and the adapter is diagnosed rather
than this file relaxed.

This is the folder's **reference case** for two behaviours the AAP fixes once for every
dependency-oriented shape, and it is the only adapter whose real artifact carries measured
numeric hazards:

* the **four-level package-coordinate precedence**, each level asserted individually by its
  own test method, with the within-level lexicographic tiebreak asserted on inputs whose
  document order and lexicographic order disagree;
* **label-over-score severity**, with the entry that governed recorded -- the label, or the
  score with its source and version -- since AAP 0.5.4 requires the selection to be recorded
  and not merely a band to be produced;
* the **float32-to-float64 representation tail**: the artifact carries ``3.200000047683716``
  and ``5.300000190734863`` in ``severity``, and both are asserted to band numerically while
  no spurious precision reaches a text field.

The contract under test
-----------------------
``adapt(doc, *, tool, root, tool_base, allowlist, tally) -> (rows, rejections, counters)``,
with ``SCANNER_CLASS`` the constant ``"vuln"`` fixed by AAP 0.5.4's class table (Trivy is
that table's single per-record exception and this tool is not it).  The shape is
``dependencies[].vulnerabilities[]`` and **one vulnerability is one record** -- the same
count unit ``normalize.reconcile`` walks independently.  Field sources: ``rule_id`` from
``name``; ``message`` from ``description``; ``severity_native`` from ``severity`` with the
selected CVSS entry recorded alongside; ``path`` from the **enclosing dependency's**
filesystem-absolute ``filePath``, relativized; ``cve`` from ``name`` when CVE-shaped; ``cwe``
from ``cwes[]``; ``package_coordinate`` from the dependency's ``packages[].id``.  The
counters this file asserts by name include the four AAP 0.5.4 has reported per tool:
``multi_location_records``, ``multi_valued_cwe_records``, ``multi_valued_cve_records`` and
``non_filesystem_paths``.

``start_line`` is ``None`` on every row, on every path
-----------------------------------------------------
Dependency-Check reports at **dependency granularity** -- a vulnerable component, not a line
of code -- and this artifact shape carries no line information in any member at any depth.
The adapter names that as the constant ``_START_LINE = None`` and never synthesises one.
Two consequences are asserted rather than assumed: every row's ``start_line`` is ``None``,
and the counter ``start_line_absent`` equals the row count.

Rejection conditions this adapter can and cannot produce
--------------------------------------------------------
Every class asserted below is a **member of** ``paths.REJECT_CLASSES`` asserted by name
against the imported tuple, because a test that only counts rejections cannot tell one
condition from another.  The six this adapter can produce, in the adapter's own fixed
classification order -- shape, rule identifier, message, path, package coordinate:

======================================  ==========================================
class                                   how it arises here
======================================  ==========================================
``malformed_record``                    a ``vulnerabilities`` element that is not an
                                        object, or a non-string ``filePath``
``missing_rule_id``                     ``name`` absent, blank or not a string
``missing_message``                     ``description`` absent, blank or not a string
``absent_path``                         ``filePath`` absent or empty
``unresolvable_path``                   a recorded ``path_base`` of kind ``none``,
                                        which supplies no base to anchor on
``unformable_package_coordinate``       no candidate at any of the four levels
======================================  ==========================================

Four conditions in the closed set are **not producible** by this adapter, and each absence
is a stated fact rather than a gap:

* ``non_integer_start_line`` -- this shape carries no line information, so there is no
  ``start_line`` for a non-integer to occupy.  It is also why no
  ``reject-dependency-check-non-integer-start-line`` fixture exists in this tree.
* ``invalid_uri`` -- a SARIF ``uriBaseId`` chain fault.  This route parses no SARIF bases.
* ``ambiguous_source_resolution`` -- an ambiguous bytecode-to-source resolution.  There is
  no bytecode input on this route and ``paths.resolve_bytecode_class`` is never reached.
* ``unattributable_section`` -- this shape has no sections to attribute a record to.

Severity policy, and the one label this dataset defines
-------------------------------------------------------
The CVSS v3.1 qualitative scale (specification document section 5,
``https://www.first.org/cvss/v3.1/specification-document``) names five bands: **None** at
0.0, Low 0.1-3.9, Medium 4.0-6.9, High 7.0-8.9 and Critical 9.0-10.0.  This dataset's
``severity_norm`` vocabulary has no ``None`` label, so the standard's ``None`` band is
emitted under this dataset's own label **``Info``** -- a mapping this dataset defines and
**not** a CVSS label.  The other four labels and all four boundaries are the standard's, and
all nine boundary values are asserted below.  An advisory commonly carries several scores
from different sources -- the positive fixture's first record carries three CVSS blocks
spanning Critical, Medium and Low at once -- which is precisely why *which* entry was
selected must be recorded, and why this file asserts ``SeverityResult.basis`` and
``SeverityResult.selected_entry`` rather than the band alone.

Hermetic by construction, with two roots and a stated reason for each
--------------------------------------------------------------------
Everything this module writes goes inside one :class:`tempfile.TemporaryDirectory`: the
allowlist holding the twelve authoritative globs, two minimal ``runner-metadata.json``
documents, and a derived scan root in which only the paths a derived document actually
references are materialised.  Both configuration files are read back through ``paths.py``'s
own loaders rather than handed to the adapter as literals, so the loaders are exercised on
the route ``cli.py`` uses.  No live Spark tree is read, no committed fixture is mutated, and
nothing is written outside that directory -- in particular this module never writes
``oss-scan-results/findings.json`` or ``oss-scan-results/findings.csv``.

The two roots are not interchangeable, and neither is a convenience:

* ``FIXTURE_ROOT`` -- every ``filePath`` in a committed fixture is filesystem-absolute and
  carries ``/opt/spark-src`` as a literal prefix, so the hand-verified rows in
  ``expected/`` are stated **against that root**.  Resolution here is pure string
  arithmetic: the adapter stats nothing, so the root is a value rather than a place, and it
  is cross-checked against each expected file's own ``resolution_context.root``.
* ``DERIVED_ROOT`` -- a directory inside the temporary directory, used by the derived
  documents this module authors for the behaviours the committed fixtures do not reach.  Its
  referenced paths are materialised so the root is a real tree rather than a bare string.

Derived documents, and why they exist
-------------------------------------
Three behaviours the AAP requires asserted are not reached by any committed fixture, and
each expected file names the gap and prescribes the cover: level 3 of the coordinate
precedence (every record in the positive fixture has a ``name`` that is its own rule
identifier or is CVE-shaped, and both are refused as package names); a path that relativizes
outside the root as a plain file rather than as an archive member; and the ``no_vocabulary``
and ``unmapped_literal`` severity bases.  They are covered by documents **authored as
literals in this module**, never by editing a fixture: a committed fixture is an unmodified
capture and stays byte-identical, which this module re-checks by sha256.

How the negative fixtures are read
----------------------------------
Each of the five ``reject-dependency-check-*`` fixtures is asserted against its own
``expected/*.rows.json``, which is the authority for its row count, its rejection count and
its per-rejection class, detail and record identity.  Two readings matter and both come from
those files rather than from a summary:

* the **defective record** produces no row and exactly one counted rejection, while every
  other record in the same document still produces its row.  That is the ``partial`` parse
  AAP 0.5.4 calls a first-class outcome, and it is why three of the five expectations carry
  surviving rows and two rejections -- one defective dependency holding two vulnerabilities
  is two records, and the count unit is the record.
* the fixture named ``reject-dependency-check-unresolvable-path.json`` empties a
  ``filePath``, so its class is **``absent_path``** and not the ``unresolvable_path`` its
  slug reads like.  Asserting the slug would assert a class the module never produces.

Prohibitions this module observes
---------------------------------
It performs **no cross-tool interpretation of any kind** (AAP 0.3.2): it asserts this
adapter's rows against this adapter's contract, and nowhere compares a Dependency-Check row
with another tool's, accounts for a difference between two tools, or characterises what any
tool's output demonstrates about that tool.  It makes no comparison against Apex, Cantina or
any other scanner.  It judges no finding -- not real, not important, not a false positive,
not a duplicate -- and it deduplicates nothing.  It carries no secret value in any literal,
message or docstring, this tree being committed to git.  It edits nothing under
``harness/lib/normalize/``: a defect there is reported, never repaired from here.  It mutates
no fixture.

Rules
-----
No user-specified rule governs this file.  ``review_rules`` returns exactly one line, ``No
user rules provided.``, and that line is the complete document; AAP 0.7 and AAP 0.10.2 say
the same.  Enterprise-standard best practice applies in their place and the absence is not
licence to lower the bar -- which is why all four coordinate levels get their own test
method, the float hazards are asserted with the exact measured literals, every rejection
class is asserted by name against the imported tuple, and the field list is iterated from
``emit.FIELDS`` so a failure names the field.

Running it
----------
Standard library only, no ``pytest``, and runnable from any working directory::

    python3 -m unittest discover -s oss-scan-results/adapter-tests \\
        -p 'test_dependency_check_adapter.py' -v
"""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
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

from normalize import emit, paths, severity  # noqa: E402
from normalize.adapters import dependency_check  # noqa: E402

# --------------------------------------------------------------------------------------
# Locations.  Both directories are inputs and are never written to by this module.
# --------------------------------------------------------------------------------------
ADAPTER_TESTS_DIR = Path(__file__).resolve().parent
FIXTURES_DIR = ADAPTER_TESTS_DIR / "fixtures"
EXPECTED_DIR = ADAPTER_TESTS_DIR / "expected"

#: The canonical tool identifier, hyphenated -- the literal every row's ``tool`` field
#: carries and the key of this tool's entry in ``runner-metadata.json``.  Written here as an
#: independent restatement and asserted against ``dependency_check.TOOL``.
TOOL = "dependency-check"

#: The ``scanner_class`` every row from this adapter carries, fixed by AAP 0.5.4's class
#: table.  Restated rather than imported for the same reason.
SCANNER_CLASS = "vuln"

# --------------------------------------------------------------------------------------
# The twelve authoritative scope globs (AAP 0.3.1), byte-exact and in the request's order.
#
# An independent restatement rather than a read of paths.ALLOWLIST_GLOBS: this module writes
# these twelve lines to its own allowlist file, loads them back through
# paths.load_allowlist() and only then confirms the loaded tuple is what paths.py authors,
# via paths.allowlist_matches_authoritative_globs().  Loading the module's own copy and
# comparing it with itself would assert nothing.  There is no exclusion line here -- the
# literal `src/test` exclusion is paths.py's, not the allowlist's.
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
# The root the committed fixtures' own paths are stated against.
#
# Every filePath in every dependency-check fixture is filesystem-absolute and carries this
# value as a literal prefix, so the hand-verified rows in expected/ hold for this root and
# for no other.  It is a value rather than a place: the adapter reads no filesystem, and
# paths.py's relativization is parts arithmetic on strings.  Asserted against each expected
# file's own resolution_context.root rather than trusted.
# --------------------------------------------------------------------------------------
FIXTURE_ROOT = "/opt/spark-src"

# --------------------------------------------------------------------------------------
# This tool's recorded path base, as harness/artifacts/logs/runner-metadata.json holds it --
# read as input, never inferred from an artifact (AAP 0.5.4).  The runner passes 18 absolute
# --scan arguments in one invocation, so dependencies[].filePath is filesystem-absolute
# under the scan root and the resolution is a straight relativization.
#
# record_path_field is load-bearing rather than decorative: paths.resolve_recorded_path
# composes its absent-path detail from it, so the expected detail for an emptied filePath
# reads "the record carries no the enclosing dependencies[].filePath value, ...".  The
# doubled article is the composition of a real metadata value and is preserved as written --
# a test asserting a tidied wording would assert against a string the module never produces.
# --------------------------------------------------------------------------------------
RECORDED_PATH_BASE_KIND = "filesystem_absolute"
RECORDED_RECORD_PATH_FIELD = "the enclosing dependencies[].filePath"
RECORDED_INVOCATIONS_PER_RUN = 1

# --------------------------------------------------------------------------------------
# The committed fixtures, each with the expectation that is the authority for its rows,
# counts, counters and rejections.  Nothing here decides routing: shape detection is
# shape.py's and is asserted by test_shape_routing_negative.py.
# --------------------------------------------------------------------------------------
POSITIVE_FIXTURE = "dependency-check"

#: One entry per rejection condition this adapter can produce and for which this tree
#: carries a fixture.  The class each is expected to yield is read from its expectation
#: rather than from its slug -- the first entry is exactly why (its filePath is emptied, so
#: its class is absent_path and not the unresolvable_path the slug reads like).
REJECT_FIXTURES = (
    "reject-dependency-check-unresolvable-path",
    "reject-dependency-check-missing-rule-id",
    "reject-dependency-check-missing-message",
    "reject-dependency-check-no-package-coordinate",
    "reject-dependency-check-malformed-record",
)

#: Every committed fixture this module reads, in a stable order.
ALL_FIXTURES = (POSITIVE_FIXTURE,) + REJECT_FIXTURES

#: sha256 of each committed fixture as this module found it.  Re-checked by
#: :class:`FixtureIntegrityTest`: a fixture is an unmodified capture, and a test that
#: silently normalized one would be asserting against a shape the tool never emitted.
FIXTURE_SHA256 = {
    "dependency-check": (
        "53fb2fa91725148f1b33df951f95e8ee01ef98ec62bebb10b73bd541bf10de68"
    ),
    "reject-dependency-check-unresolvable-path": (
        "cef4785fa3afbe45a741d7b08fa4468e697ce1da0fb44562c4a6fa83a9e5cfd8"
    ),
    "reject-dependency-check-missing-rule-id": (
        "01765c370abf5be9b6ece262d4c425bcf38e59d1d5d75711e193b66204905e03"
    ),
    "reject-dependency-check-missing-message": (
        "924f3373ed918efde1a80e081c0e5738aa023b8b741c949cdb600282df47e59c"
    ),
    "reject-dependency-check-no-package-coordinate": (
        "23b6a6eacff8f1bc8062c4af1b8f476f27064ba840e983de07f8b3937264182f"
    ),
    "reject-dependency-check-malformed-record": (
        "3e8b7e17d5a82e210dc1153286c54ff096ec504dde41083540ef44b083681d90"
    ),
}

# --------------------------------------------------------------------------------------
# The rejection classes this adapter can and cannot produce, with the reason for each
# absence.  Asserted as membership in the imported paths.REJECT_CLASSES tuple: a class name
# spelled by hand and never checked against the module is a test that passes while naming
# something no code path produces.
# --------------------------------------------------------------------------------------
PRODUCIBLE_REJECT_CLASSES = (
    paths.REJECT_MALFORMED_RECORD,
    paths.REJECT_MISSING_RULE_ID,
    paths.REJECT_MISSING_MESSAGE,
    paths.REJECT_ABSENT_PATH,
    paths.REJECT_UNRESOLVABLE_PATH,
    paths.REJECT_UNFORMABLE_PACKAGE_COORDINATE,
)

UNPRODUCIBLE_REJECT_CLASSES = {
    paths.REJECT_NON_INTEGER_START_LINE: (
        "this shape carries no line information in any member at any depth, so there is no "
        "start_line for a non-integer to occupy"
    ),
    paths.REJECT_INVALID_URI: (
        "a uriBaseId chain fault is a SARIF condition; this route parses no SARIF bases"
    ),
    paths.REJECT_AMBIGUOUS_SOURCE_RESOLUTION: (
        "an ambiguous bytecode-to-source resolution needs bytecode input, and this route "
        "resolves a reported filesystem path instead"
    ),
    paths.REJECT_UNATTRIBUTABLE_SECTION: (
        "this shape has no sections, so no record can fail to be attributed to one"
    ),
}

# --------------------------------------------------------------------------------------
# The two float32-to-float64 representation artifacts the real artifact carries, exactly as
# measured, and the one-decimal text each must reach severity_native as.  The expected
# rendering is taken from expected/dependency-check.rows.json rather than invented here:
# this module asserts the authored policy, it does not author a rounding rule of its own.
# --------------------------------------------------------------------------------------
MEASURED_FLOAT_LOW = 3.200000047683716
MEASURED_FLOAT_MEDIUM = 5.300000190734863

# --------------------------------------------------------------------------------------
# The CVSS v3.1 section 5 boundaries, every one of the nine values that decides a band.
# Each is asserted through the adapter -- a record whose severity member is that number --
# rather than against severity.band_for_score alone, so the assertion covers the route a
# row actually takes.  0.0 is the standard's None band, emitted under this dataset's Info.
# --------------------------------------------------------------------------------------
CVSS_BOUNDARIES = (
    (0.0, "Info"),
    (0.1, "Low"),
    (3.9, "Low"),
    (4.0, "Medium"),
    (6.9, "Medium"),
    (7.0, "High"),
    (8.9, "High"),
    (9.0, "Critical"),
    (10.0, "Critical"),
)

# --------------------------------------------------------------------------------------
# Paths a derived document references, materialised under the derived root so that root is
# a real tree rather than a bare string.  Every one names a file that genuinely exists in
# the pinned tree, which is what keeps a derived document a statement about this tool's real
# surface: the 40 vendored front-end bundles under core/src/main/resources (30 .js and 10
# .css, verified), and the three lockfiles that are inside the root and outside the twelve
# globs.  There is no JAR anywhere in the eighteen in-scope directories, so no derived
# document invents a Maven dependency graph the scope does not contain.
# --------------------------------------------------------------------------------------
IN_SCOPE_BUNDLE = "core/src/main/resources/org/apache/spark/ui/static/jquery.cookies.2.2.0.min.js"
IN_SCOPE_MANIFEST = "core/src/main/resources/org/apache/spark/ui/static/package.json"
OUT_OF_GLOB_LOCKFILES = (
    "dev/package-lock.json",
    "ui-test/package-lock.json",
    "docs/Gemfile.lock",
)
MATERIALISED_DERIVED_PATHS = (IN_SCOPE_BUNDLE, IN_SCOPE_MANIFEST) + OUT_OF_GLOB_LOCKFILES


# --------------------------------------------------------------------------------------
# Fixture and expectation loaders
# --------------------------------------------------------------------------------------


def fixture_path(name: str) -> Path:
    """Return the path of a committed fixture, by name and without its extension."""
    return FIXTURES_DIR / f"{name}.json"


def expected_path(name: str) -> Path:
    """Return the path of a committed expectation, by the same name."""
    return EXPECTED_DIR / f"{name}.rows.json"


def load_fixture(name: str) -> dict:
    """Parse a committed fixture.

    Read-only, and never written back: a fixture is an unmodified capture of the tool's own
    output, and its bytes are what make the positive mapping a statement about the shape the
    tool emits rather than about the shape someone believed it emits.
    """
    return json.loads(fixture_path(name).read_text(encoding="utf-8"))


def load_expected(name: str) -> dict:
    """Parse a committed expectation -- the authority for that fixture's result.

    Each expectation is a hand-verified document carrying ``rows``, ``counts``, ``counters``
    and, for a negative fixture, ``rejections`` with the class, detail and record identity
    each defective record must produce.  Assertions are taken from it rather than restated,
    so a disagreement is between the adapter and a hand-verified file rather than between
    the adapter and a second guess at it.
    """
    return json.loads(expected_path(name).read_text(encoding="utf-8"))


def sha256_of(path: Path) -> str:
    """Return the sha256 of a file, read in binary so no newline handling can alter it."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


# --------------------------------------------------------------------------------------
# Derived-document builders.
#
# Every derived document is authored here as a literal rather than produced by editing a
# committed fixture, which stays byte-identical.  The builders keep a derived case readable:
# a test states only the members it is about, and the surrounding shape -- an object top
# level, a dependencies array, a filePath, a vulnerabilities array -- comes from here.
# --------------------------------------------------------------------------------------


def vulnerability(**members: object) -> dict:
    """Return one ``dependencies[].vulnerabilities[]`` element with defaults filled in.

    ``name`` and ``description`` default to usable values, because a derived case about a
    path or a coordinate must not reject at step 2 or step 3 for a reason it is not about.
    Passing either explicitly -- including as ``None`` through ``members`` -- overrides the
    default, which is how the negative derived cases are stated.
    """
    record: dict = {
        "name": "ADVISORY-DERIVED-1",
        "description": "A described condition, carried verbatim into the message field.",
    }
    record.update(members)
    return record


def dependency(
    *,
    file_path: object,
    vulnerabilities: list | object,
    packages: object = None,
    file_name: str = "derived-dependency",
    **members: object,
) -> dict:
    """Return one ``dependencies[]`` element.

    ``packages`` defaults to ``None`` and is omitted entirely when so, since a dependency
    carrying no ``packages`` member is the shape that reaches the unformable-coordinate
    rejection -- the realistic case for this tool, whose whole in-scope surface is vendored
    web assets with no manifest behind them.
    """
    element: dict = {"fileName": file_name, "filePath": file_path}
    if packages is not None:
        element["packages"] = packages
    element["vulnerabilities"] = vulnerabilities
    element.update(members)
    return element


def document(*dependencies: object, omit_dependencies: bool = False) -> dict:
    """Return a Dependency-Check report carrying ``dependencies`` and the members it needs.

    ``reportSchema``, ``scanInfo`` and ``projectInfo`` are carried because a real 13.0.0
    report carries them, even though the adapter reads only ``dependencies``: a derived
    document that dropped them would be a narrower shape than the tool emits.
    ``omit_dependencies`` builds the one document the adapter must refuse outright.
    """
    report: dict = {
        "reportSchema": "1.1",
        "scanInfo": {"engineVersion": "13.0.0"},
        "projectInfo": {"name": "derived-document-for-the-adapter-test"},
    }
    if not omit_dependencies:
        report["dependencies"] = list(dependencies)
    return report


def package_object(**members: object) -> dict:
    """Return one ``dependencies[].packages[]`` element from the members given."""
    return dict(members)


class Adapted:
    """One adaptation, held so a test asserts over a single measurement rather than retaking it.

    AAP 0.6.4 requires a count appearing twice to be one measurement cited twice, so the
    rows, the rejections, the counters and the tally that were produced together are kept
    together here.

    Attributes:
        rows: The dataset rows, each carrying exactly the twelve fields of ``emit.FIELDS``.
        rejections: The ``paths.Rejection`` records counted instead of rows.
        counters: The adapter's own counter mapping, over all of ``COUNTER_KEYS``.
        tally: The ``severity.LiteralTally`` fed once per emitted row.
    """

    def __init__(
        self,
        rows: list,
        rejections: list,
        counters: dict,
        tally: severity.LiteralTally,
    ) -> None:
        """Hold the four results of one adaptation, exactly as the adapter returned them."""
        self.rows = rows
        self.rejections = rejections
        self.counters = counters
        self.tally = tally

    @property
    def reject_classes(self) -> tuple:
        """The class of each rejection, in the order the adapter produced them."""
        return tuple(rejection.reject_class for rejection in self.rejections)

    @property
    def one_row(self) -> dict:
        """The single row, where the case produced exactly one.

        A property rather than ``rows[0]`` at each call site: a case that produced two rows
        would otherwise pass an assertion about "the" row while the second went unexamined.
        """
        if len(self.rows) != 1:
            raise AssertionError(
                f"expected exactly one row, observed {len(self.rows)}: "
                f"{[row.get('rule_id') for row in self.rows]!r}"
            )
        return self.rows[0]

    @property
    def one_rejection(self) -> paths.Rejection:
        """The single rejection, where the case produced exactly one."""
        if len(self.rejections) != 1:
            raise AssertionError(
                f"expected exactly one rejection, observed {len(self.rejections)}: "
                f"{list(self.reject_classes)!r}"
            )
        return self.rejections[0]


class Environment:
    """The hermetic inputs every test shares: an allowlist, runner metadata and two roots.

    The allowlist and the metadata are real files inside one temporary directory and are read
    back through ``paths.py``'s own loaders rather than handed to the adapter as literals, so
    the loaders are exercised on the route ``cli.py`` uses.  Nothing outside the temporary
    directory is written, and the committed fixtures are read only.

    Attributes:
        directory: The temporary directory holding everything this object created.
        globs: The twelve authoritative globs, as ``paths.load_allowlist`` returned them.
        fixture_base: The recorded path base for resolving a committed fixture, whose
            absolute paths are stated against :data:`FIXTURE_ROOT`.
        derived_root: An absolute root inside the temporary directory, for the documents this
            module authors.  Its referenced paths are materialised.
        derived_base: The recorded path base for that root.
    """

    def __init__(self, directory: Path) -> None:
        """Write and load the allowlist and both metadata documents, and build the roots."""
        self.directory = directory

        self.allowlist_path = directory / "allowlist.txt"
        # One glob per line, byte-exact, with a trailing newline and nothing else.
        self.allowlist_path.write_text(
            "".join(f"{glob}\n" for glob in AUTHORITATIVE_GLOBS), encoding="utf-8"
        )
        self.globs = paths.load_allowlist(self.allowlist_path)

        self.derived_root = str(directory / "spark-src")
        for relative in MATERIALISED_DERIVED_PATHS:
            self.materialise(relative)

        self.fixture_metadata_path, self.fixture_metadata = self._write_metadata(
            "runner-metadata-fixture.json", FIXTURE_ROOT, RECORDED_PATH_BASE_KIND
        )
        self.fixture_base = paths.tool_path_base(self.fixture_metadata, TOOL)

        self.derived_metadata_path, self.derived_metadata = self._write_metadata(
            "runner-metadata-derived.json", self.derived_root, RECORDED_PATH_BASE_KIND
        )
        self.derived_base = paths.tool_path_base(self.derived_metadata, TOOL)

    # -- inputs ---------------------------------------------------------------------- #

    def _metadata_document(self, root: str, kind: str) -> dict:
        """Build the minimal document ``paths.load_runner_metadata`` accepts for this tool.

        Minimal is deliberate.  It carries exactly the facts a resolver takes from the
        record -- the base kind, the base value, the resolved scan root, the field the base
        names and the invocation form -- and nothing that would make this a second copy of
        ``harness/artifacts/logs/runner-metadata.json``.  The values are the ones that record
        holds for this provisioning, read as input rather than inferred from an artifact.
        """
        path_base: dict = {
            "kind": kind,
            "value": root if kind != paths.PATH_BASE_KIND_NONE else None,
            "record_path_field": RECORDED_RECORD_PATH_FIELD,
        }
        return {
            "purpose": (
                "Minimal runner metadata for the dependency-check adapter test. Written and "
                "read inside a temporary directory; it is not the run's record."
            ),
            "spark_src": root,
            "tools": {
                TOOL: {
                    "canonical_tool_identifier": TOOL,
                    "scanner_class": SCANNER_CLASS,
                    "resolved_scan_root": root,
                    "invocation_form": {
                        "target_passing_style": (
                            "one absolute --scan path per allowlist directory, in a single "
                            "invocation"
                        ),
                        "invocations_per_run": RECORDED_INVOCATIONS_PER_RUN,
                    },
                    "working_directory": {"path": root, "equals_scan_root": True},
                    "path_base": path_base,
                }
            },
        }

    def _write_metadata(self, filename: str, root: str, kind: str) -> tuple:
        """Write one metadata document and return its path and the loaded mapping."""
        path = self.directory / filename
        path.write_text(
            json.dumps(self._metadata_document(root, kind), indent=1) + "\n",
            encoding="utf-8",
        )
        return path, paths.load_runner_metadata(path)

    def base_of_kind(self, kind: str) -> paths.ToolPathBase:
        """Return this tool's base as it would be with a differently recorded ``kind``.

        The route to the ``unresolvable_path`` class: a recorded kind of ``none`` supplies no
        base to anchor on, and the document's own instruction is to reject such a record
        rather than fall back to the root.  It is a property of the metadata rather than of
        the artifact, which is why it is reached by writing a metadata document and not by
        shaping a document.
        """
        _, metadata = self._write_metadata(
            f"runner-metadata-{kind}.json", self.derived_root, kind
        )
        return paths.tool_path_base(metadata, TOOL)

    def materialise(self, relative: str) -> Path:
        """Create an empty file at ``relative`` under the derived root, and return its path.

        Only the paths a derived document actually references are materialised.  The adapter
        stats nothing, so this changes no result -- it makes the derived root a real tree, so
        a reader can see that every derived coordinate names a location that exists rather
        than a string chosen to make an assertion pass.
        """
        target = Path(self.derived_root) / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            target.write_text("", encoding="utf-8")
        return target

    def derived_absolute(self, relative: str) -> str:
        """Return the absolute form of a derived-root-relative path, POSIX-spelled."""
        return f"{self.derived_root}/{relative}"

    # -- invocation ------------------------------------------------------------------ #

    def adapt(
        self,
        doc: object,
        *,
        root: str | None = None,
        tool_base: paths.ToolPathBase | None = None,
        tool: str = TOOL,
    ) -> Adapted:
        """Adapt one document and return rows, rejections, counters and the tally together.

        A fresh ``severity.LiteralTally`` per call, seeded with all nine canonical tools, so
        one case's literals can never be counted into another's -- and so the per-literal row
        counts ``severity-map.md`` reports are asserted on the same object the adapter fed.
        """
        tally = severity.LiteralTally.with_all_tools()
        rows, rejections, counters = dependency_check.adapt(
            doc,
            tool=tool,
            root=FIXTURE_ROOT if root is None else root,
            tool_base=self.fixture_base if tool_base is None else tool_base,
            allowlist=self.globs,
            tally=tally,
        )
        return Adapted(rows, rejections, counters, tally)

    def adapt_derived(self, doc: object) -> Adapted:
        """Adapt a document authored against the derived root."""
        return self.adapt(doc, root=self.derived_root, tool_base=self.derived_base)

    def adapt_fixture(self, name: str) -> Adapted:
        """Adapt one committed fixture against the root its expectation is stated for."""
        return self.adapt(load_fixture(name))


#: The one temporary directory and the one :class:`Environment` every test shares.  Built in
#: :func:`setUpModule` so the cost is paid once, and removed in :func:`tearDownModule` so
#: nothing survives the run.  Read through :func:`environment`, which fails loudly rather
#: than returning ``None`` if a caller reaches it before setup.
_TEMPORARY_DIRECTORY: tempfile.TemporaryDirectory | None = None
_ENVIRONMENT: Environment | None = None


def setUpModule() -> None:
    """Create the shared temporary directory and the hermetic environment inside it."""
    global _TEMPORARY_DIRECTORY, _ENVIRONMENT
    _TEMPORARY_DIRECTORY = tempfile.TemporaryDirectory(
        prefix="blitzy-dependency-check-adapter-test-"
    )
    _ENVIRONMENT = Environment(Path(_TEMPORARY_DIRECTORY.name))


def tearDownModule() -> None:
    """Remove the temporary directory, leaving nothing behind outside it."""
    global _TEMPORARY_DIRECTORY, _ENVIRONMENT
    _ENVIRONMENT = None
    if _TEMPORARY_DIRECTORY is not None:
        _TEMPORARY_DIRECTORY.cleanup()
        _TEMPORARY_DIRECTORY = None


def environment() -> Environment:
    """Return the shared environment, raising rather than yielding ``None``."""
    if _ENVIRONMENT is None:  # pragma: no cover - defended, not reachable in a normal run
        raise AssertionError(
            "the shared environment is absent: setUpModule did not run, so a test would "
            "otherwise assert against an unconfigured resolver"
        )
    return _ENVIRONMENT


class AdapterTestCase(unittest.TestCase):
    """Shared assertions every test class in this module reuses.

    The row-level helpers iterate ``emit.FIELDS`` rather than a list authored here, so a
    field added to the schema is covered without this file being edited, and a failure names
    the field rather than dumping two dicts.
    """

    @property
    def env(self) -> Environment:
        """The shared hermetic environment."""
        return environment()

    def assert_row_matches(self, observed: dict, expected: dict, where: str) -> None:
        """Assert one row against its expectation, field by field over ``emit.FIELDS``.

        Every one of the twelve is compared individually and the key set is compared too, so
        neither an extra field nor a missing one can pass as equality.  The order is
        ``emit.FIELDS``' order, which is the request's order.
        """
        self.assertEqual(
            sorted(observed), sorted(emit.FIELDS), f"{where}: a row is exactly the twelve fields"
        )
        self.assertEqual(
            tuple(observed), tuple(emit.FIELDS), f"{where}: the twelve fields in FIELDS order"
        )
        for field in emit.FIELDS:
            self.assertEqual(
                observed[field],
                expected[field],
                f"{where}: field {field!r} -- observed {observed[field]!r}, "
                f"expected {expected[field]!r}",
            )

    def assert_no_absolute_paths(self, rows: list, where: str) -> None:
        """Assert structurally that no row's ``path`` is absolute, a URI, or otherwise not relative.

        The rule comes from ``emit.py`` -- ``validate_rows`` refuses such a row and
        ``validation_summary`` counts it -- so this asserts the emitter's own definition
        rather than a second spelling of it.
        """
        emit.validate_rows(rows)
        summary = emit.validation_summary(rows)
        self.assertEqual(summary["absolute_paths"], 0, f"{where}: {summary['violations']!r}")
        self.assertEqual(summary["path_absent"], 0, f"{where}: path is never absent")
        self.assertEqual(
            summary["severity_norm_absent"], 0, f"{where}: severity_norm is never absent"
        )
        self.assertEqual(
            summary["rows_with_exactly_twelve_fields"],
            len(rows),
            f"{where}: every row carries exactly twelve fields",
        )
        self.assertTrue(summary["passed"], f"{where}: {summary['violations']!r}")

    def assert_reject_class_is_real(self, reject_class: str) -> None:
        """Assert a class name is a member of the closed set ``paths.REJECT_CLASSES``."""
        self.assertIn(
            reject_class,
            paths.REJECT_CLASSES,
            "a rejection class must be a member of paths.REJECT_CLASSES, the closed set of "
            f"{len(paths.REJECT_CLASSES)}",
        )
        self.assertTrue(paths.is_reject_class(reject_class))

    def assert_counters(self, observed: dict, expected: dict, where: str) -> None:
        """Assert every counter the expectation names, and that no key is missing."""
        for key, value in expected.items():
            self.assertIn(key, observed, f"{where}: counter {key!r} is absent from the result")
            self.assertEqual(
                observed[key], value, f"{where}: counter {key!r} -- expected {value!r}"
            )
        self.assertEqual(
            sorted(observed),
            sorted(dependency_check.COUNTER_KEYS),
            f"{where}: the counter key set is COUNTER_KEYS",
        )


class ContractTest(AdapterTestCase):
    """The contract this module is written against, asserted rather than assumed.

    Every later assertion rests on these: the field list being ``emit.FIELDS``, the scanner
    class being the constant AAP 0.5.4 fixes, the counter keys existing under the names the
    AAP reports, and each rejection class this module names being a real member of the closed
    set.  A test suite that assumed any of them would pass while asserting about something
    the pipeline does not have.
    """

    def test_field_list_is_the_emitter_authored_one(self) -> None:
        """The twelve fields come from ``emit.FIELDS``, and the adapter's copy agrees."""
        self.assertEqual(
            emit.FIELDS,
            (
                "tool",
                "scanner_class",
                "rule_id",
                "message",
                "severity_native",
                "severity_norm",
                "path",
                "start_line",
                "cwe",
                "cve",
                "package_coordinate",
                "in_scope",
            ),
            "emit.FIELDS is the twelve fields in the request's order (AAP 0.8.2)",
        )
        self.assertEqual(
            dependency_check.FIELDS,
            emit.FIELDS,
            "the adapter's authored copy must agree with emit.FIELDS by construction: AAP "
            "0.6.4 permits an adapter to import paths and severity only, so the duplication "
            "is required and its agreement is what has to be checked",
        )

    def test_absence_is_permitted_in_exactly_five_fields(self) -> None:
        """Absence is permitted for the five optional fields and no others (AAP 0.8.2)."""
        self.assertEqual(
            dependency_check.ABSENCE_PERMITTED_FIELDS,
            frozenset(emit.OPTIONAL_FIELDS),
            "path and severity_norm are not among them: a record whose path cannot be "
            "resolved is rejected and counted rather than emitted with a null path",
        )
        self.assertNotIn("path", dependency_check.ABSENCE_PERMITTED_FIELDS)
        self.assertNotIn("severity_norm", dependency_check.ABSENCE_PERMITTED_FIELDS)

    def test_tool_identifier_and_scanner_class_are_fixed(self) -> None:
        """``tool`` is the hyphenated identifier and ``scanner_class`` is the constant ``vuln``."""
        self.assertEqual(dependency_check.TOOL, TOOL)
        self.assertEqual(
            dependency_check.SCANNER_CLASS,
            SCANNER_CLASS,
            "AAP 0.5.4's class table fixes vuln for dependency-check; Trivy is the table's "
            "single per-record exception and this tool is not it",
        )
        self.assertIn(
            TOOL,
            severity.CANONICAL_TOOLS,
            "the identifier must be one severity.LiteralTally accepts, or no row's literal "
            "could reach severity-map.md",
        )

    def test_counter_keys_carry_the_four_the_aap_reports_per_tool(self) -> None:
        """The four counters AAP 0.5.4 has reported per tool exist, and start at zero."""
        fresh = dependency_check.new_counters()
        for key in (
            "multi_location_records",
            "multi_valued_cwe_records",
            "multi_valued_cve_records",
            "non_filesystem_paths",
        ):
            self.assertIn(key, dependency_check.COUNTER_KEYS, f"counter {key!r} is required")
            self.assertEqual(fresh[key], 0, f"counter {key!r} starts at zero")
        self.assertEqual(
            sorted(fresh),
            sorted(dependency_check.COUNTER_KEYS),
            "new_counters() pre-initialises every key, so a missing key can never be "
            "ambiguous between zero and not-measured",
        )
        for kind in paths.PATH_KINDS:
            self.assertIn(f"path_kind_{kind}", dependency_check.COUNTER_KEYS)
        for basis in severity.BASIS_VALUES:
            self.assertIn(f"severity_basis_{basis}", dependency_check.COUNTER_KEYS)

    def test_every_reject_class_this_module_names_is_real(self) -> None:
        """Each producible class is a member of ``paths.REJECT_CLASSES``, asserted by name."""
        for reject_class in PRODUCIBLE_REJECT_CLASSES:
            self.assert_reject_class_is_real(reject_class)
        self.assertEqual(
            len(set(PRODUCIBLE_REJECT_CLASSES)),
            len(PRODUCIBLE_REJECT_CLASSES),
            "the producible set carries no duplicate",
        )

    def test_the_unproducible_classes_are_real_and_stated_with_a_reason(self) -> None:
        """The four conditions this adapter cannot produce are named, not silently omitted.

        Each is a real member of the closed set -- so this is a statement about the adapter
        rather than about a misspelling -- and each carries the reason its absence is a fact.
        ``non_integer_start_line`` is the one that matters most: it is why no
        ``reject-dependency-check-non-integer-start-line`` fixture exists in this tree.
        """
        for reject_class, reason in UNPRODUCIBLE_REJECT_CLASSES.items():
            self.assert_reject_class_is_real(reject_class)
            self.assertTrue(reason, f"{reject_class!r} must state why it cannot arise")
            self.assertNotIn(
                reject_class,
                PRODUCIBLE_REJECT_CLASSES,
                f"{reject_class!r} cannot be both producible and not",
            )
        self.assertIn(paths.REJECT_NON_INTEGER_START_LINE, UNPRODUCIBLE_REJECT_CLASSES)
        self.assertFalse(
            expected_path("reject-dependency-check-non-integer-start-line").exists(),
            "no non-integer start_line expectation exists for this adapter, because this "
            "shape carries no line information for a non-integer to occupy",
        )
        self.assertFalse(
            fixture_path("reject-dependency-check-non-integer-start-line").exists(),
            "and no such fixture either",
        )

    def test_the_allowlist_loaded_from_disk_is_the_authoritative_one(self) -> None:
        """The twelve globs written here load back as the twelve ``paths.py`` authors.

        Written by this module, read back through ``paths.load_allowlist`` and only then
        compared with ``paths.allowlist_matches_authoritative_globs`` -- so the check is
        against an independent restatement rather than against itself.
        """
        self.assertEqual(len(AUTHORITATIVE_GLOBS), 12)
        self.assertEqual(self.env.globs, AUTHORITATIVE_GLOBS)
        self.assertTrue(
            paths.allowlist_matches_authoritative_globs(self.env.globs),
            "the operative allowlist must hold the twelve authoritative globs, byte-exact "
            "and in order (AAP 0.3.1)",
        )
        self.assertNotIn(
            "src/test",
            "".join(AUTHORITATIVE_GLOBS),
            "the allowlist carries no exclusion line: the literal src/test exclusion is "
            "paths.py's, applied once where it lives",
        )

    def test_the_recorded_path_base_is_read_from_metadata(self) -> None:
        """The base comes from the loaded runner metadata, and names this tool."""
        base = self.env.fixture_base
        self.assertEqual(base.tool, TOOL)
        self.assertEqual(base.kind, RECORDED_PATH_BASE_KIND)
        self.assertEqual(base.base_value, FIXTURE_ROOT)
        self.assertEqual(base.record_path_field, RECORDED_RECORD_PATH_FIELD)
        self.assertTrue(base.has_explicit_base)
        self.assertEqual(base.invocations_per_run, RECORDED_INVOCATIONS_PER_RUN)
        self.assertEqual(paths.metadata_scan_root(self.env.fixture_metadata), FIXTURE_ROOT)

    def test_another_tools_base_is_refused(self) -> None:
        """Handing this adapter another tool's recorded base is a caller fault, not a rejection.

        Resolving one tool's paths against another tool's base would produce a wrong path for
        every row while each row still looked well-formed, which is exactly what AAP 0.5.4's
        *"every base taken from the recorded runner metadata"* exists to prevent.
        """
        foreign = paths.ToolPathBase(
            tool="trivy",
            kind=paths.PATH_BASE_KIND_SCAN_ROOT,
            base_value=FIXTURE_ROOT,
            scan_root=FIXTURE_ROOT,
        )
        with self.assertRaises(dependency_check.DependencyCheckAdapterError):
            self.env.adapt(load_fixture(POSITIVE_FIXTURE), tool_base=foreign)
        with self.assertRaises(dependency_check.DependencyCheckAdapterError):
            self.env.adapt(load_fixture(POSITIVE_FIXTURE), tool="trivy")


class FixtureIntegrityTest(AdapterTestCase):
    """The committed fixtures and expectations are unmodified captures, and stay that way.

    A positive fixture is *"an unmodified excerpt captured from the tool's own output, because
    a hand-written fixture tests the adapter against the shape you believed the tool emits
    rather than the shape it emits"* (AAP 0.6.2).  This module therefore never writes to
    either directory, and re-checks by digest that it has not.
    """

    def test_every_fixture_and_expectation_is_present(self) -> None:
        """All six fixtures and all six expectations exist, and parse."""
        for name in ALL_FIXTURES:
            self.assertTrue(fixture_path(name).exists(), f"{name}.json is missing")
            self.assertTrue(expected_path(name).exists(), f"{name}.rows.json is missing")
            self.assertIsInstance(load_fixture(name), dict)
            self.assertIsInstance(load_expected(name), dict)

    def test_every_fixture_digest_is_unchanged(self) -> None:
        """Each fixture's sha256 is the one this module was written against."""
        for name, digest in FIXTURE_SHA256.items():
            self.assertEqual(
                sha256_of(fixture_path(name)),
                digest,
                f"{name}.json has changed: a fixture is an unmodified capture, so a "
                "difference is a finding to diagnose rather than a digest to update",
            )

    def test_every_expectation_states_the_root_its_rows_are_stated_against(self) -> None:
        """Each expectation's ``resolution_context`` names this module's fixture root and base.

        The cross-check that keeps :data:`FIXTURE_ROOT` from being a value this module chose:
        the fixtures' paths are absolute, so a different root would relativize every row
        differently while the rows in ``expected/`` stayed as they are.
        """
        for name in ALL_FIXTURES:
            context = load_expected(name)["resolution_context"]
            self.assertEqual(context["root"], FIXTURE_ROOT, f"{name}: root")
            self.assertEqual(
                context["path_base_kind"], RECORDED_PATH_BASE_KIND, f"{name}: path_base kind"
            )
            self.assertEqual(
                context["path_base_value"], FIXTURE_ROOT, f"{name}: path_base value"
            )

    def test_every_expectation_states_the_twelve_fields_in_order(self) -> None:
        """Each expectation's ``field_order`` is ``emit.FIELDS``, so the two cannot drift."""
        for name in ALL_FIXTURES:
            self.assertEqual(
                tuple(load_expected(name)["field_order"]),
                emit.FIELDS,
                f"{name}: field_order is emit.FIELDS",
            )


class PathResolutionTest(AdapterTestCase):
    """Every ``path`` is expressed against the ``SPARK_SRC`` root, and none is ever absolute.

    The tool reports a filesystem-absolute ``filePath`` on the **enclosing dependency**, so
    the resolution is a straight relativization against the recorded base -- with three
    shapes that are not plain relativization and are asserted individually: a location
    outside the root, keeping its ``../`` segments; an archive member, serialized with the
    single defined separator; and a location inside the root but outside the twelve globs,
    which is kept with ``in_scope: false`` rather than dropped.
    """

    def test_an_absolute_file_path_relativizes_to_a_real_in_scope_target(self) -> None:
        """A vendored bundle under ``core/src/main/resources`` resolves and is in scope.

        Asserted against a real target rather than an invented JAR path: **no JAR exists
        anywhere in the eighteen in-scope directories**, and the realistic surface this tool
        sees is the 40 vendored front-end bundles (30 ``.js`` and 10 ``.css``).
        """
        adapted = self.env.adapt_fixture(POSITIVE_FIXTURE)
        by_rule = {row["rule_id"]: row for row in adapted.rows}
        row = by_rule["jquery.cookies 2.2.0 unsafe cookie value deserialization"]
        self.assertEqual(row["path"], IN_SCOPE_BUNDLE)
        self.assertTrue(row["in_scope"], "core/src/main/** matches it")
        self.assertNotIn(FIXTURE_ROOT, row["path"], "the root prefix is not retained")

        # And the same behaviour on a derived root, so the relativization is shown to be
        # against the recorded base rather than against one hard-coded prefix.
        derived = self.env.adapt_derived(
            document(
                dependency(
                    file_path=self.env.derived_absolute(IN_SCOPE_BUNDLE),
                    packages=[package_object(id="pkg:npm/jquery.cookies@2.2.0")],
                    vulnerabilities=[vulnerability(severity="LOW")],
                )
            )
        )
        self.assertEqual(derived.one_row["path"], IN_SCOPE_BUNDLE)
        self.assertTrue(derived.one_row["in_scope"])
        self.assertEqual(derived.counters["path_kind_tree_file"], 1)
        self.assertEqual(derived.counters["non_filesystem_paths"], 0)

    def test_no_emitted_path_is_absolute_on_any_row_of_any_case(self) -> None:
        """Structurally, over every row of every committed fixture and the derived shapes.

        The rule asserted is ``emit.py``'s own -- ``validate_rows`` refuses such a row and
        ``validation_summary`` counts it -- so this is the emitter's definition rather than a
        second spelling of it.  It covers an archive member and an out-of-root coordinate too:
        AAP 0.5.4 requires that *no absolute value is ever emitted*, including for a path that
        names something other than a file in the tree.
        """
        for name in ALL_FIXTURES:
            adapted = self.env.adapt_fixture(name)
            self.assert_no_absolute_paths(adapted.rows, f"fixture {name}")
            for row in adapted.rows:
                self.assertFalse(row["path"].startswith("/"), f"{name}: {row['path']!r}")
                self.assertNotIn(
                    FIXTURE_ROOT, row["path"], f"{name}: the root prefix is not retained"
                )

        outside = self.env.adapt_derived(
            document(
                dependency(
                    file_path=str(Path(self.env.derived_root).parent / "vendor" / "bundle.js"),
                    packages=[package_object(id="pkg:npm/vendored@1.0.0")],
                    vulnerabilities=[vulnerability(severity="LOW")],
                )
            )
        )
        self.assert_no_absolute_paths(outside.rows, "derived outside-root row")

    def test_a_location_outside_the_root_keeps_its_dot_dot_segments(self) -> None:
        """``../`` is preserved rather than normalized away, and the row is kept.

        The SARIF 2.1.0 errata constraint the AAP applies to every non-filesystem and
        out-of-root coordinate: a consumer must not normalize ``..`` segments out of a path.
        So the row is kept with ``in_scope: false`` and counted in ``non_filesystem_paths``
        rather than dropped or rewritten into a path that names something else.
        """
        parent = Path(self.env.derived_root).parent
        adapted = self.env.adapt_derived(
            document(
                dependency(
                    file_path=str(parent / "vendor" / "bundle.js"),
                    packages=[package_object(id="pkg:npm/vendored@1.0.0")],
                    vulnerabilities=[vulnerability(severity="LOW")],
                )
            )
        )
        row = adapted.one_row
        self.assertEqual(
            row["path"],
            "../vendor/bundle.js",
            "one ../ segment for one level above the root, preserved verbatim",
        )
        self.assertIn("..", paths.split_segments(row["path"]))
        self.assertFalse(row["in_scope"], "a non-filesystem coordinate is never in scope")
        self.assertEqual(adapted.counters["path_kind_outside_root"], 1)
        self.assertEqual(adapted.counters["non_filesystem_paths"], 1)
        self.assertEqual(adapted.counters["rows_out_of_scope"], 1)
        self.assertEqual(len(adapted.rejections), 0, "kept, not rejected")

        # Two levels up, to show the segments are counted rather than a single '..' emitted.
        deeper = self.env.adapt_derived(
            document(
                dependency(
                    file_path=str(parent.parent / "elsewhere" / "bundle.js"),
                    packages=[package_object(id="pkg:npm/vendored@1.0.0")],
                    vulnerabilities=[vulnerability(severity="LOW")],
                )
            )
        )
        self.assertEqual(deeper.one_row["path"], "../../elsewhere/bundle.js")

        # The same preservation on real captured output: the Maven-repository coordinate in
        # the committed fixture resolves above the root and keeps both segments.
        captured = self.env.adapt_fixture("reject-dependency-check-unresolvable-path")
        outside_rows = [row for row in captured.rows if row["path"].startswith("../")]
        self.assertEqual(len(outside_rows), 1)
        self.assertTrue(
            outside_rows[0]["path"].startswith("../../root/.m2/repository/"),
            f"observed {outside_rows[0]['path']!r}",
        )
        self.assertFalse(outside_rows[0]["in_scope"])

    def test_an_archive_member_is_serialized_with_the_single_defined_separator(self) -> None:
        """``<container-relative-to-root>!<member>``, one ``!``, kept and counted.

        Dependency-Check names a member inside a container by concatenating the member onto
        the container path with a ``/`` and no separator at all, so the adapter inserts the
        one defined separator at the container boundary before delegating.  Left unsplit, that
        coordinate would relativize into an ordinary-looking path naming a file that is not on
        disk -- and one whose leading segments match an allowlist glob, so the row would take
        ``in_scope: true`` for something the tree does not contain.  Both are asserted.
        """
        captured = self.env.adapt_fixture("reject-dependency-check-unresolvable-path")
        archive_rows = [
            row for row in captured.rows if paths.ARCHIVE_SEPARATOR in row["path"]
        ]
        self.assertEqual(len(archive_rows), 2, "the fixture carries two archive members")
        for row in archive_rows:
            self.assertEqual(
                row["path"].count(paths.ARCHIVE_SEPARATOR),
                1,
                f"exactly one separator: {row['path']!r}",
            )
            container, member = row["path"].split(paths.ARCHIVE_SEPARATOR)
            self.assertTrue(
                paths.looks_like_archive_container(paths.split_segments(container)[-1]),
                f"the container component names a container: {container!r}",
            )
            self.assertTrue(member, "the member component is not empty")
            self.assertFalse(row["in_scope"], "an archive member is never in scope")
        self.assertEqual(captured.counters["archive_references_split"], 2)
        self.assertEqual(captured.counters["path_kind_archive_member"], 2)
        self.assertEqual(captured.counters["non_filesystem_paths"], 2)

        # A derived container inside the root and under an allowlisted prefix: the leading
        # segments match core/src/main/**, and the row is still out of scope.
        concatenated = (
            f"{IN_SCOPE_BUNDLE.rsplit('/', 1)[0]}/vendored-bundle.jar"
            "/META-INF/maven/example.group/example-artifact/pom.xml"
        )
        derived = self.env.adapt_derived(
            document(
                dependency(
                    file_path=self.env.derived_absolute(concatenated),
                    packages=[
                        package_object(id="pkg:maven/example.group/example-artifact@1.0.0")
                    ],
                    vulnerabilities=[vulnerability(severity="HIGH")],
                )
            )
        )
        row = derived.one_row
        self.assertEqual(
            row["path"],
            f"{IN_SCOPE_BUNDLE.rsplit('/', 1)[0]}/vendored-bundle.jar"
            "!META-INF/maven/example.group/example-artifact/pom.xml",
        )
        self.assertEqual(row["path"].count(paths.ARCHIVE_SEPARATOR), 1)
        self.assertFalse(
            row["in_scope"],
            "its leading segments match core/src/main/**, and it is still out of scope: the "
            "matcher is given the resolved path's kind, so an archive member cannot match a "
            "glob on its segments",
        )
        self.assertEqual(derived.counters["archive_references_split"], 1)
        self.assertEqual(derived.counters["path_kind_archive_member"], 1)
        self.assertEqual(derived.counters["non_filesystem_paths"], 1)
        self.assertEqual(len(derived.rejections), 0, "kept, not rejected")

    def test_a_path_inside_the_root_but_outside_the_globs_is_kept_out_of_scope(self) -> None:
        """The three real lockfiles produce kept rows with ``in_scope: false``.

        A runner legitimately reaching a manifest outside the twelve roots produces a row that
        is **kept** and counted, never dropped (AAP 0.3.2, AAP 0.9.3): only evidence about the
        *runner* establishes a wrong scan root, and an individual out-of-glob coordinate never
        does.  The three files asserted here are the three that actually exist inside the pin
        and outside the globs.
        """
        adapted = self.env.adapt_fixture(POSITIVE_FIXTURE)
        out_of_glob = [row for row in adapted.rows if not row["in_scope"]]
        self.assertEqual(len(out_of_glob), 4, "three lockfiles, one of them twice")
        for row in out_of_glob:
            self.assertIn(row["path"], OUT_OF_GLOB_LOCKFILES)
            self.assertIsNone(
                paths.matches_any_glob(row["path"], self.env.globs),
                f"{row['path']!r} matches no authoritative glob",
            )
        self.assertEqual(
            set(row["path"] for row in out_of_glob),
            set(OUT_OF_GLOB_LOCKFILES),
            "all three appear",
        )
        self.assertEqual(adapted.counters["rows_out_of_scope"], 4)
        self.assertEqual(
            adapted.counters["rows_in_scope"] + adapted.counters["rows_out_of_scope"],
            len(adapted.rows),
            "one measurement split, not a second count of the same thing",
        )
        self.assertEqual(
            adapted.counters["non_filesystem_paths"],
            0,
            "an out-of-glob lockfile is inside the root and is an ordinary tree_file: out of "
            "scope is not the same question as non-filesystem",
        )

        for lockfile in OUT_OF_GLOB_LOCKFILES:
            derived = self.env.adapt_derived(
                document(
                    dependency(
                        file_path=self.env.derived_absolute(lockfile),
                        packages=[package_object(id="pkg:npm/example-package@1.0.0")],
                        vulnerabilities=[vulnerability(severity="LOW")],
                    )
                )
            )
            self.assertEqual(derived.one_row["path"], lockfile)
            self.assertFalse(derived.one_row["in_scope"], lockfile)
            self.assertEqual(len(derived.rejections), 0, f"{lockfile} is kept, not dropped")
            self.assertEqual(derived.counters["path_kind_tree_file"], 1, lockfile)


# --------------------------------------------------------------------------------------
# The four coordinate levels, as one derived family.
#
# One builder supplies all four candidate levels and each test switches off the levels above
# the one it is about.  That is what makes "level (a) is chosen even though (b), (c) and (d)
# are also available" a checkable statement rather than an assertion about a document that
# only ever offered one candidate: the very same members are present in every case below,
# and only the higher levels are withheld.
#
# The two ecosystems are deliberately mixed-case -- 'PyPI' at level 3 and 'JavaScript' at
# level 4 -- because AAP 0.5.4 lower-cases the ecosystem and carries the name and version
# exactly as observed, so a fixture with an already-lower-case ecosystem could not tell a
# correct implementation from one that dropped the fold.
# --------------------------------------------------------------------------------------
LEVEL_A_COORDINATE = "pkg:npm/record-level-package@1.0.0"
LEVEL_B_COORDINATE = "pkg:npm/package-object-level@2.0.0"
LEVEL_C_ECOSYSTEM = "PyPI"
LEVEL_C_NAME = "Record-Level-Triple"
LEVEL_C_VERSION = "3.0.0"
LEVEL_C_COORDINATE = f"pypi:{LEVEL_C_NAME}@{LEVEL_C_VERSION}"
LEVEL_D_ECOSYSTEM = "JavaScript"
LEVEL_D_NAME = "Package-Object-Triple"
LEVEL_D_VERSION = "4.0.0"
LEVEL_D_COORDINATE = f"javascript:{LEVEL_D_NAME}@{LEVEL_D_VERSION}"

#: A CPE, which Dependency-Check also emits under ``packages[].id``.  It is **not** a package
#: URL and is never coerced into one, which is why a package object carrying only this offers
#: no candidate at level 2.
CPE_IDENTIFIER = "cpe:2.3:a:example:example-bundle:4.1.3:*:*:*:*:*:*:*"


def precedence_case(
    *,
    record_package_url: bool = True,
    package_object_package_url: bool = True,
    record_triple: bool = True,
    package_object_triple: bool = True,
    rule_id: str = "ADVISORY-DERIVED-COORDINATE",
) -> tuple:
    """Return ``(document, vulnerability, dependency)`` offering the levels not withheld.

    The vulnerability and the dependency are returned alongside the document so a test can
    call the public :func:`dependency_check.package_coordinate` seam on the very objects the
    row was built from -- which is how the *level* a coordinate was formed at is asserted, and
    not merely the value that came out.
    """
    record: dict = {"name": rule_id, "description": "A described condition.", "severity": "LOW"}
    if record_package_url:
        record["purl"] = LEVEL_A_COORDINATE
    if record_triple:
        record["ecosystem"] = LEVEL_C_ECOSYSTEM
        record["packageName"] = LEVEL_C_NAME
        record["version"] = LEVEL_C_VERSION

    package: dict = {"confidence": "HIGH"}
    if package_object_package_url:
        package["id"] = LEVEL_B_COORDINATE
    else:
        # A CPE occupies the same key, so the level is offered a value and still yields no
        # candidate -- the distinction a coercion would erase.
        package["id"] = CPE_IDENTIFIER
    if package_object_triple:
        package["ecosystem"] = LEVEL_D_ECOSYSTEM
        package["name"] = LEVEL_D_NAME
        package["version"] = LEVEL_D_VERSION

    element = dependency(
        file_path=f"{FIXTURE_ROOT}/{IN_SCOPE_BUNDLE}",
        packages=[package],
        vulnerabilities=[record],
    )
    return document(element), record, element


class PackageCoordinatePrecedenceTest(AdapterTestCase):
    """AAP 0.5.4's four candidate levels, each asserted individually and in its own method.

    The precedence is *"(a) a package URL on the record; failing that (b) a package URL on the
    enclosing package object; failing that (c) ``<ecosystem>:<name>@<version>`` from the
    record's own fields; failing that (d) the same from the enclosing package's fields"*, with
    the **lexicographically smallest** winning where several candidates sit at one level, the
    ecosystem lower-cased, and an unformable coordinate a **rejection** for this
    dependency-oriented shape rather than a null field.

    This adapter is the folder's reference implementation of that precedence, which is why
    every level is exercised here rather than only the ones a captured artifact happens to
    reach.
    """

    def test_the_four_levels_are_the_aap_precedence_in_order(self) -> None:
        """``COORDINATE_LEVELS`` is the four levels in the AAP's own order."""
        self.assertEqual(
            dependency_check.COORDINATE_LEVELS,
            (
                dependency_check.COORDINATE_LEVEL_RECORD_PACKAGE_URL,
                dependency_check.COORDINATE_LEVEL_PACKAGE_OBJECT_PACKAGE_URL,
                dependency_check.COORDINATE_LEVEL_RECORD_FIELDS,
                dependency_check.COORDINATE_LEVEL_PACKAGE_OBJECT_FIELDS,
            ),
        )

    def test_level_a_record_package_url_wins_with_every_lower_level_available(self) -> None:
        """Level (a) is chosen while (b), (c) and (d) all offer a candidate.

        The level that proves the precedence is *ordered* rather than opportunistic: an
        implementation scanning ``packages[]`` before the record would emit level (b)'s value
        and every other row would still look right.  The captured fixture carries the same
        case -- a record ``purl`` alongside a package object that also holds one.
        """
        doc, record, element = precedence_case()
        coordinate = dependency_check.package_coordinate(
            record, element, rule_id=record["name"]
        )
        self.assertIsNotNone(coordinate)
        self.assertEqual(coordinate.value, LEVEL_A_COORDINATE)
        self.assertEqual(
            coordinate.level, dependency_check.COORDINATE_LEVEL_RECORD_PACKAGE_URL
        )
        self.assertEqual(coordinate.candidates_at_level, 1)

        adapted = self.env.adapt(doc)
        self.assertEqual(adapted.one_row["package_coordinate"], LEVEL_A_COORDINATE)
        self.assertEqual(adapted.counters["package_coordinate_from_record_package_url"], 1)
        for other in (
            "package_coordinate_from_package_object_package_url",
            "package_coordinate_from_record_fields",
            "package_coordinate_from_package_object_fields",
        ):
            self.assertEqual(adapted.counters[other], 0, f"{other} must not move")

        # Captured evidence for the same level, from the tool's own output.
        captured = self.env.adapt_fixture(POSITIVE_FIXTURE)
        by_rule = {row["rule_id"]: row for row in captured.rows}
        self.assertEqual(
            by_rule["CVE-2020-28458"]["package_coordinate"],
            "pkg:npm/datatables.net@1.13.11",
            "the record's own purl, although its package object carries "
            "pkg:javascript/datatables@1.13.11",
        )
        self.assertEqual(captured.counters["package_coordinate_from_record_package_url"], 1)

    def test_level_b_package_object_package_url_when_the_record_carries_none(self) -> None:
        """Level (b) is chosen once (a) is withheld, with (c) and (d) still available."""
        doc, record, element = precedence_case(record_package_url=False)
        coordinate = dependency_check.package_coordinate(
            record, element, rule_id=record["name"]
        )
        self.assertEqual(coordinate.value, LEVEL_B_COORDINATE)
        self.assertEqual(
            coordinate.level,
            dependency_check.COORDINATE_LEVEL_PACKAGE_OBJECT_PACKAGE_URL,
        )

        adapted = self.env.adapt(doc)
        self.assertEqual(adapted.one_row["package_coordinate"], LEVEL_B_COORDINATE)
        self.assertEqual(
            adapted.counters["package_coordinate_from_package_object_package_url"], 1
        )
        self.assertEqual(adapted.counters["package_coordinate_from_record_package_url"], 0)

        # The level that carries the captured artifact: six of its eight rows.
        captured = self.env.adapt_fixture(POSITIVE_FIXTURE)
        self.assertEqual(
            captured.counters["package_coordinate_from_package_object_package_url"], 6
        )

    def test_level_c_record_fields_with_the_ecosystem_lower_cased(self) -> None:
        """Level (c) forms ``<ecosystem>:<name>@<version>`` with the ecosystem lower-cased.

        The ecosystem is folded and the name and version are carried exactly as observed,
        since a package name is case-sensitive in several ecosystems and folding it would name
        a different package.  The literal here is mixed-case (``PyPI``) precisely so a dropped
        fold fails this assertion instead of passing unnoticed.

        No row of the captured fixture reaches this level, and not because the fixture avoids
        it: a Dependency-Check vulnerability's ``name`` is the advisory identifier, so a name
        equal to the record's own rule identifier or CVE-shaped is refused as a package name,
        and every record in that artifact is one or the other.  Both refusals are asserted
        below, because without them every record would form a plausible-looking coordinate and
        the unformable-coordinate rejection this shape genuinely produces would never fire.
        """
        doc, record, element = precedence_case(
            record_package_url=False, package_object_package_url=False
        )
        coordinate = dependency_check.package_coordinate(
            record, element, rule_id=record["name"]
        )
        self.assertEqual(coordinate.value, LEVEL_C_COORDINATE)
        self.assertEqual(
            coordinate.level, dependency_check.COORDINATE_LEVEL_RECORD_FIELDS
        )
        self.assertEqual(
            coordinate.value.split(":", 1)[0],
            LEVEL_C_ECOSYSTEM.lower(),
            "the ecosystem is lower-cased",
        )
        self.assertNotEqual(
            coordinate.value.split(":", 1)[0],
            LEVEL_C_ECOSYSTEM,
            "and the observed literal was not already lower-case, so the fold is visible",
        )
        self.assertIn(LEVEL_C_NAME, coordinate.value, "the name is carried as observed")
        self.assertTrue(coordinate.value.endswith(f"@{LEVEL_C_VERSION}"))

        adapted = self.env.adapt(doc)
        self.assertEqual(adapted.one_row["package_coordinate"], LEVEL_C_COORDINATE)
        self.assertEqual(adapted.counters["package_coordinate_from_record_fields"], 1)

        # The two refusals that keep the advisory identifier out of the coordinate field.
        for refused_name in ("ADVISORY-REFUSED-AS-A-PACKAGE-NAME", "CVE-2024-6531"):
            refused_record = {
                "name": refused_name,
                "description": "A described condition.",
                "severity": "LOW",
                "ecosystem": LEVEL_C_ECOSYSTEM,
                "version": LEVEL_C_VERSION,
            }
            refused_element = dependency(
                file_path=f"{FIXTURE_ROOT}/{IN_SCOPE_BUNDLE}",
                packages=[package_object(id=CPE_IDENTIFIER)],
                vulnerabilities=[refused_record],
            )
            self.assertIsNone(
                dependency_check.package_coordinate(
                    refused_record, refused_element, rule_id=refused_name
                ),
                f"a name of {refused_name!r} is refused as a package name, so level (c) "
                "offers nothing and no level below it does either",
            )
        self.assertEqual(
            self.env.adapt_fixture(POSITIVE_FIXTURE).counters[
                "package_coordinate_from_record_fields"
            ],
            0,
            "which is why no row of the captured artifact is formed at level (c)",
        )

    def test_level_d_package_object_fields_when_no_level_above_offers_one(self) -> None:
        """Level (d) forms the triple from the enclosing package's fields, ecosystem folded.

        Reached in the captured artifact by exactly one row, for two stated reasons: its
        package object's ``id`` is a CPE, which is not a package URL and is never coerced into
        one, so level (b) offers nothing; and its ``name`` is its own rule identifier, so
        level (c) is refused.
        """
        doc, record, element = precedence_case(
            record_package_url=False,
            package_object_package_url=False,
            record_triple=False,
        )
        coordinate = dependency_check.package_coordinate(
            record, element, rule_id=record["name"]
        )
        self.assertEqual(coordinate.value, LEVEL_D_COORDINATE)
        self.assertEqual(
            coordinate.level, dependency_check.COORDINATE_LEVEL_PACKAGE_OBJECT_FIELDS
        )
        self.assertEqual(coordinate.value.split(":", 1)[0], LEVEL_D_ECOSYSTEM.lower())

        adapted = self.env.adapt(doc)
        self.assertEqual(adapted.one_row["package_coordinate"], LEVEL_D_COORDINATE)
        self.assertEqual(
            adapted.counters["package_coordinate_from_package_object_fields"], 1
        )

        captured = self.env.adapt_fixture(POSITIVE_FIXTURE)
        by_rule = {row["rule_id"]: row for row in captured.rows}
        self.assertEqual(
            by_rule["sonatype-2021-0163"]["package_coordinate"],
            "javascript:d3-flame-graph@4.1.3",
            "the ecosystem 'JavaScript' lower-cased, the name and version as observed",
        )
        self.assertEqual(
            captured.counters["package_coordinate_from_package_object_fields"], 1
        )

    def test_a_within_level_tie_takes_the_lexicographically_smallest(self) -> None:
        """Several candidates at one level: the lexicographically smallest is emitted.

        Both inputs below are constructed so **document order and lexicographic order
        disagree** -- without that the assertion could not tell the stated rule from taking
        the first thing found.  The tiebreak is over the pooled candidates of the whole level
        rather than within one package object, because taking the first entry's value would
        make the coordinate depend on producer order.
        """
        first_in_document = "pkg:npm/zeta-package@1.0.0"
        lexicographically_smallest = "pkg:maven/example.group/alpha-artifact@2.0.0"
        self.assertLess(
            lexicographically_smallest,
            first_in_document,
            "the fixture is only meaningful while the two orders disagree",
        )
        element = dependency(
            file_path=f"{FIXTURE_ROOT}/{IN_SCOPE_BUNDLE}",
            packages=[
                package_object(id=first_in_document, confidence="HIGHEST"),
                package_object(id=lexicographically_smallest, confidence="LOW"),
            ],
            vulnerabilities=[
                vulnerability(name="ADVISORY-DERIVED-TIE", severity="LOW")
            ],
        )
        record = element["vulnerabilities"][0]
        coordinate = dependency_check.package_coordinate(
            record, element, rule_id=record["name"]
        )
        self.assertEqual(coordinate.value, lexicographically_smallest)
        self.assertEqual(
            coordinate.candidates_at_level, 2, "both candidates were pooled and compared"
        )

        adapted = self.env.adapt(document(element))
        self.assertEqual(adapted.one_row["package_coordinate"], lexicographically_smallest)
        self.assertEqual(
            adapted.counters["package_coordinate_multiple_candidates_at_level"], 1
        )

        # Captured evidence: the tool's own output carries the same disagreement, with the
        # npm entry first and marked LOW confidence and the javascript entry second and HIGH,
        # so neither array order nor the confidence field decides it.
        captured = self.env.adapt_fixture(POSITIVE_FIXTURE)
        by_rule = {row["rule_id"]: row for row in captured.rows}
        self.assertEqual(
            by_rule["jquery.cookies 2.2.0 unsafe cookie value deserialization"][
                "package_coordinate"
            ],
            "pkg:javascript/jquery.cookies@2.2.0",
        )
        self.assertLess(
            "pkg:javascript/jquery.cookies@2.2.0",
            "pkg:npm/jquery.cookies@2.2.0",
            "'j' precedes 'n', and the npm entry is the one the producer put first",
        )
        self.assertEqual(
            captured.counters["package_coordinate_multiple_candidates_at_level"], 1
        )

    def test_no_formable_coordinate_is_a_counted_rejection_and_never_a_null_field(self) -> None:
        """An unformable coordinate rejects under ``unformable_package_coordinate``.

        For this **dependency-oriented** shape AAP 0.5.4 makes the absence a rejection rather
        than a row with a null field, and the class is asserted by name against
        ``paths.REJECT_CLASSES``.  It is the realistic case rather than a contrivance: the raw
        artifact's 32 dependencies resolved zero package coordinates, all of them vendored web
        assets with no manifest behind them.

        The record below is otherwise perfectly well-formed -- an object, a usable name, a
        usable description and a path that resolves cleanly inside the root and in scope -- so
        nothing but the coordinate can account for the rejection.
        """
        doc, record, element = precedence_case(
            record_package_url=False,
            package_object_package_url=False,
            record_triple=False,
            package_object_triple=False,
            rule_id="CVE-2024-6531",
        )
        self.assertIsNone(
            dependency_check.package_coordinate(record, element, rule_id=record["name"]),
            "a CPE is not a package URL and is not coerced into one",
        )

        adapted = self.env.adapt(doc)
        self.assertEqual(adapted.rows, [], "no row is emitted, not even with a null field")
        rejection = adapted.one_rejection
        self.assert_reject_class_is_real(rejection.reject_class)
        self.assertEqual(
            rejection.reject_class, paths.REJECT_UNFORMABLE_PACKAGE_COORDINATE
        )
        self.assertEqual(rejection.tool, TOOL)
        self.assertIn(
            "no package coordinate can be formed at any of the four candidate levels",
            rejection.detail,
            "the diagnostic is retained and names the condition",
        )
        self.assertEqual(rejection.record_identity["name"], "CVE-2024-6531")
        self.assertEqual(adapted.counters["package_coordinate_unformable"], 1)
        for level_counter in (
            "package_coordinate_from_record_package_url",
            "package_coordinate_from_package_object_package_url",
            "package_coordinate_from_record_fields",
            "package_coordinate_from_package_object_fields",
        ):
            self.assertEqual(adapted.counters[level_counter], 0, level_counter)
        self.assertEqual(
            adapted.counters["rows_in_scope"] + adapted.counters["rows_out_of_scope"],
            0,
            "step 5 returns before the row builder runs, so no row tally moves",
        )

    def test_a_dependency_with_no_packages_member_at_all_rejects(self) -> None:
        """The measured shape: a vendored asset with no ``packages`` member behind it.

        The dependency carries no ``packages`` key rather than an empty array, which is the
        shape a vendored bundle with no manifest actually takes, and the record's ``name`` is
        its own advisory identifier -- so no level offers a candidate.
        """
        adapted = self.env.adapt_derived(
            document(
                dependency(
                    file_path=self.env.derived_absolute(IN_SCOPE_BUNDLE),
                    vulnerabilities=[
                        vulnerability(name="ADVISORY-NO-COORDINATE", severity="LOW")
                    ],
                )
            )
        )
        self.assertEqual(adapted.rows, [])
        self.assertEqual(
            adapted.reject_classes, (paths.REJECT_UNFORMABLE_PACKAGE_COORDINATE,)
        )
        self.assertEqual(adapted.counters["package_coordinate_unformable"], 1)


class SeverityTest(AdapterTestCase):
    """Label over score, the entry that governed recorded, and the two measured hazards.

    AAP 0.5.4: *"the native label governs whenever it is in the mapped vocabulary, and a CVSS
    score is consulted only where no mapped label exists.  Either way the entry used is
    recorded -- the label, or the score with its source and version."*  So every assertion
    here reads ``SeverityResult.basis`` and ``SeverityResult.selected_entry`` and not the band
    alone: a band with no recorded selection is a band nobody can check, and an advisory
    routinely carries several scores from different sources -- the captured artifact's first
    record carries three CVSS blocks spanning Critical, Medium and Low at once.

    The seam these assertions use is the adapter's own public
    :func:`dependency_check.resolve_severity`, which is the single resolution path the row
    builder takes, so the recorded selection and the emitted row describe one decision rather
    than two.
    """

    # -- 12: the label governs ------------------------------------------------------- #

    def test_a_mapped_label_governs_over_every_coexisting_score(self) -> None:
        """A mapped label wins whatever the scores say, and the label is what is recorded.

        The captured record asserted first is the reference disagreement and a wide one: a
        ``LOW`` label beside ``cvssv3`` 9.1 (Critical), ``cvssv2`` 4.3 (Medium) and ``cvssv4``
        2.3 (Low).  An implementation reading ``cvssv3`` -- the usual choice -- emits Critical.
        """
        fixture = load_fixture(POSITIVE_FIXTURE)
        record = fixture["dependencies"][0]["vulnerabilities"][0]
        self.assertEqual(record["name"], "CVE-2024-6531", "the record this asserts about")
        candidates = dependency_check.score_candidates(record)
        self.assertEqual(
            len(candidates), 3, "three CVSS blocks are available to be consulted"
        )

        result = dependency_check.resolve_severity(record)
        self.assertEqual(result.basis, severity.BASIS_LABEL, "the label route governed")
        self.assertEqual(result.selected_entry, {"label": "LOW"}, "the label is recorded")
        self.assertEqual(result.severity_native, "LOW")
        self.assertEqual(result.severity_norm, "Low")

        adapted = self.env.adapt_fixture(POSITIVE_FIXTURE)
        by_rule = {row["rule_id"]: row for row in adapted.rows}
        self.assertEqual(by_rule["CVE-2024-6531"]["severity_native"], "LOW")
        self.assertEqual(by_rule["CVE-2024-6531"]["severity_norm"], "Low")
        self.assertEqual(
            adapted.counters["severity_basis_label"], 5, "five rows banded from a label"
        )
        self.assertEqual(
            adapted.counters["severity_score_candidates_present"],
            5,
            "five records carried a score that could have been consulted",
        )

        # A case where the two routes disagree on the band itself, so a score-first
        # implementation fails on severity_norm as well as on the recorded entry.
        derived = self.env.adapt_derived(
            document(
                dependency(
                    file_path=self.env.derived_absolute(IN_SCOPE_BUNDLE),
                    packages=[package_object(id="pkg:npm/example-package@1.0.0")],
                    vulnerabilities=[
                        vulnerability(
                            severity="LOW",
                            source="NVD",
                            cvssv3={"baseScore": 9.8, "version": "3.1"},
                        )
                    ],
                )
            )
        )
        self.assertEqual(derived.one_row["severity_native"], "LOW")
        self.assertEqual(
            derived.one_row["severity_norm"],
            "Low",
            "the label governs, so the 9.8 score does not band this row Critical",
        )
        self.assertEqual(derived.counters["severity_basis_label"], 1)
        self.assertEqual(derived.counters["severity_basis_cvss_score"], 0)
        self.assertEqual(
            derived.counters["severity_selected_score_with_source_and_version"],
            0,
            "no score entry governed, so none is recorded as having done so",
        )

    # -- 13: a score governs, and which one is recorded ------------------------------ #

    def test_with_no_mapped_label_a_score_governs_and_the_entry_is_recorded(self) -> None:
        """The selected score entry is recorded with its source and its version.

        The captured record carries no ``severity`` member at all and two CVSS blocks, so the
        candidates are consulted and ``cvssv3`` is selected over ``cvssv2`` by the documented
        order -- highest CVSS version first, version ``3.1`` above version ``2.0``.  Neither
        half of the recorded source is invented: a block named ``cvssv3`` is a version-3
        entry, and the record's own ``source`` is the provenance of the scores under it.
        """
        fixture = load_fixture(POSITIVE_FIXTURE)
        record = fixture["dependencies"][5]["vulnerabilities"][1]
        self.assertEqual(record["name"], "CVE-2020-11022")
        self.assertNotIn("severity", record, "the record carries no label to govern")

        result = dependency_check.resolve_severity(record)
        self.assertEqual(result.basis, severity.BASIS_CVSS_SCORE)
        self.assertEqual(
            result.selected_entry,
            {"score": 7.5, "source": "NVD:cvssv3", "version": "3.1"},
            "the score with its source and version, which is what AAP 0.5.4 requires "
            "recorded",
        )
        self.assertEqual(result.severity_native, "7.5")
        self.assertEqual(result.severity_norm, "High")
        self.assertFalse(
            dependency_check.severity_literal_present(record),
            "and the adapter agrees no literal was present",
        )

        adapted = self.env.adapt_fixture(POSITIVE_FIXTURE)
        by_rule = {row["rule_id"]: row for row in adapted.rows}
        self.assertEqual(by_rule["CVE-2020-11022"]["severity_native"], "7.5")
        self.assertEqual(by_rule["CVE-2020-11022"]["severity_norm"], "High")
        self.assertEqual(
            adapted.counters["severity_selected_score_with_source_and_version"],
            1,
            "exactly one row's selected entry named both a source and a version",
        )
        self.assertEqual(adapted.counters["severity_label_absent"], 1)

    def test_the_selected_score_is_chosen_by_version_and_not_by_magnitude(self) -> None:
        """Which entry governed is a decision with a stated order, so it is asserted directly.

        The same three blocks as the reference disagreement, with the label removed: the
        selection takes ``cvssv4`` because its version is highest, **not** ``cvssv3`` because
        its score is highest.  Recording the entry is what makes that visible at all -- both
        readings produce a band, and only one of them names the entry that produced it.
        """
        fixture = load_fixture(POSITIVE_FIXTURE)
        captured = fixture["dependencies"][0]["vulnerabilities"][0]
        unlabelled = {
            key: value for key, value in captured.items() if key != "severity"
        }
        candidates = dependency_check.score_candidates(unlabelled)
        self.assertEqual(
            [candidate["version"] for candidate in candidates],
            ["4.0", "3.1", "2.0"],
            "the candidates are supplied in descending major version, so the order is "
            "deterministic",
        )

        result = dependency_check.resolve_severity(unlabelled)
        self.assertEqual(result.basis, severity.BASIS_CVSS_SCORE)
        self.assertEqual(result.selected_entry["version"], "4.0")
        self.assertEqual(result.selected_entry["source"], "NVD:cvssv4")
        self.assertEqual(result.selected_entry["score"], 2.3)
        self.assertEqual(result.severity_native, "2.3")
        self.assertEqual(result.severity_norm, "Low")

    # -- 14: the nine CVSS boundary values ------------------------------------------- #

    def test_every_cvss_boundary_value_bands_as_the_standard_defines(self) -> None:
        """All nine boundaries, each asserted through the adapter rather than in isolation.

        The CVSS v3.1 section 5 scale: 9.0-10.0 Critical, 7.0-8.9 High, 4.0-6.9 Medium,
        0.1-3.9 Low and 0.0 the standard's ``None`` band, which this dataset emits under its
        own label ``Info``.  Each value is carried by a record and banded on the route a row
        actually takes, so a boundary that moved would fail here and not only in a unit of
        ``severity.band_for_score``.
        """
        self.assertEqual(len(CVSS_BOUNDARIES), 9, "nine values decide the five bands")
        for score, band in CVSS_BOUNDARIES:
            with self.subTest(score=score):
                adapted = self.env.adapt_derived(
                    document(
                        dependency(
                            file_path=self.env.derived_absolute(IN_SCOPE_BUNDLE),
                            packages=[package_object(id="pkg:npm/example-package@1.0.0")],
                            vulnerabilities=[vulnerability(severity=score, source="OSSINDEX")],
                        )
                    )
                )
                row = adapted.one_row
                self.assertEqual(row["severity_norm"], band, f"{score} bands {band}")
                self.assertEqual(
                    row["severity_native"],
                    f"{score:.1f}",
                    "a numeric severity reaches severity_native as a one-decimal rendering",
                )
                self.assertEqual(adapted.counters["severity_basis_cvss_score"], 1)
        self.assertEqual(
            severity.SEVERITY_NORM,
            ("Critical", "High", "Medium", "Low", "Info"),
            "the closed output vocabulary, most severe first, with Info where the standard "
            "names None -- a label this dataset defines and not a CVSS label",
        )
        self.assertEqual(
            severity.CVSS_BAND_TABLE,
            (
                ("Critical", 9.0, 10.0),
                ("High", 7.0, 8.9),
                ("Medium", 4.0, 6.9),
                ("Low", 0.1, 3.9),
                ("Info", 0.0, 0.0),
            ),
            "the displayed table and the comparisons that implement it must agree",
        )

    # -- 15: the measured float32-to-float64 artifacts ------------------------------- #

    def test_the_measured_float_literals_band_numerically_with_no_spurious_precision(
        self,
    ) -> None:
        """``3.200000047683716`` bands Low and ``5.300000190734863`` bands Medium.

        The artifact carries these two values verbatim: an OSS Index-sourced record has no
        label and the provider's ``float`` CVSS score is rendered into the ``severity`` field,
        so the float32-to-float64 representation tail reaches the artifact.  Two things are
        asserted, and the second is the one a careless implementation loses: the band is taken
        **numerically** from the full-precision value, and ``severity_native`` carries the
        one-decimal rendering the expectation states -- so no artifact tail reaches a text
        field.  The expected text is read from ``expected/dependency-check.rows.json`` rather
        than invented here, because this module asserts the authored policy and does not
        author a rounding rule of its own.
        """
        expectation = load_expected(POSITIVE_FIXTURE)
        expected_by_rule = {row["rule_id"]: row for row in expectation["rows"]}
        adapted = self.env.adapt_fixture(POSITIVE_FIXTURE)
        observed_by_rule = {row["rule_id"]: row for row in adapted.rows}

        for rule_id, measured, band in (
            ("sonatype-2021-0163", MEASURED_FLOAT_LOW, "Low"),
            ("sonatype-2022-6438", MEASURED_FLOAT_MEDIUM, "Medium"),
        ):
            with self.subTest(rule_id=rule_id):
                fixture_record = self._captured_record(rule_id)
                self.assertEqual(
                    fixture_record["severity"],
                    measured,
                    "the fixture carries the measured literal exactly",
                )
                self.assertIsInstance(fixture_record["severity"], float)

                # (i) the band, taken numerically from the full-precision value.
                self.assertEqual(
                    severity.band_for_score(measured),
                    band,
                    "banding is an ordered numeric comparison, never a lexical one",
                )
                self.assertEqual(observed_by_rule[rule_id]["severity_norm"], band)

                # (ii) no spurious precision in a text field, at the exact expected text.
                expected_native = expected_by_rule[rule_id]["severity_native"]
                self.assertEqual(
                    observed_by_rule[rule_id]["severity_native"], expected_native
                )
                self.assertNotEqual(
                    observed_by_rule[rule_id]["severity_native"],
                    str(measured),
                    "the stringified full-precision value must not reach severity_native",
                )
                self.assertNotIn(
                    "0000",
                    observed_by_rule[rule_id]["severity_native"],
                    "the representation tail is not carried into the text",
                )

                # The full-precision value is kept where nothing is lost: the selected entry.
                result = dependency_check.resolve_severity(fixture_record)
                self.assertEqual(result.basis, severity.BASIS_CVSS_SCORE)
                self.assertEqual(result.selected_entry["score"], measured)
                self.assertEqual(
                    result.selected_entry["source"],
                    "label",
                    "the numeric severity member is itself what was banded, so the recorded "
                    "entry names the label position rather than a CVSS block",
                )
                self.assertIsNone(result.selected_entry["version"])

        self.assertEqual(
            adapted.counters["severity_selected_score_with_source_and_version"],
            1,
            "neither float row increments it: their entries carry no version",
        )
        self.assertNotEqual(
            f"{MEASURED_FLOAT_LOW:.1f}",
            str(MEASURED_FLOAT_LOW),
            "the two spellings differ, so asserting the rendered one is a real assertion",
        )

    # -- 16: case-insensitive labels, literal preserved ------------------------------ #

    def test_a_lower_case_label_is_mapped_and_the_literal_is_preserved(self) -> None:
        """``moderate`` bands Medium while ``severity_native`` keeps the literal as written.

        The map's keys are upper-case and the lookup upper-cases the observed literal, so case
        never decides a band.  The literal is **not** folded on its way to the row, because
        ``severity-map.md`` reports observed literals with per-literal row counts and
        upper-casing here would misreport a literal the tool never wrote.
        """
        adapted = self.env.adapt_fixture(POSITIVE_FIXTURE)
        by_rule = {row["rule_id"]: row for row in adapted.rows}
        row = by_rule["jquery.cookies 2.2.0 unsafe cookie value deserialization"]
        self.assertEqual(row["severity_native"], "moderate", "as the artifact spelled it")
        self.assertEqual(row["severity_norm"], "Medium")
        self.assertIn(
            "MODERATE",
            severity.label_table(),
            "the mapped vocabulary carries the upper-case key",
        )
        self.assertNotIn(
            "moderate", severity.label_table(), "and only the upper-case one"
        )

        entries = {entry.severity_native: entry for entry in adapted.tally.entries(TOOL)}
        self.assertIn("moderate", entries, "the tally records the literal as written")
        self.assertEqual(entries["moderate"].severity_norm, "Medium")
        self.assertEqual(entries["moderate"].basis, severity.BASIS_LABEL)
        self.assertFalse(entries["moderate"].unmapped)

        # A second lower-case literal, on a different band, so the fold is not a one-off.
        for literal, band in (("critical", "Critical"), ("high", "High")):
            with self.subTest(literal=literal):
                derived = self.env.adapt_derived(
                    document(
                        dependency(
                            file_path=self.env.derived_absolute(IN_SCOPE_BUNDLE),
                            packages=[package_object(id="pkg:npm/example-package@1.0.0")],
                            vulnerabilities=[vulnerability(severity=literal)],
                        )
                    )
                )
                self.assertEqual(derived.one_row["severity_native"], literal)
                self.assertEqual(derived.one_row["severity_norm"], band)
                self.assertEqual(derived.counters["severity_basis_label"], 1)

    # -- 17: an unmapped literal, disclosed with its rows ---------------------------- #

    def test_an_unmapped_literal_bands_info_and_is_disclosed_with_its_row_count(self) -> None:
        """A literal outside every mapped vocabulary maps to ``Info`` and is listed with its rows.

        Two literals are asserted: one that is neither a label nor a number, and a CVSS
        **vector** string, which ``severity.py`` excludes from the numeric route explicitly so
        a vector can never be mistaken for a score.  Both are disclosed rather than dropped,
        which is what lets ``severity-map.md`` list them with the rows they affected.
        """
        for literal in ("PROVIDER-SPECIFIC-BAND", "CVSS:3.1/AV:N/AC:L/PR:N/UI:N"):
            with self.subTest(literal=literal):
                adapted = self.env.adapt_derived(
                    document(
                        dependency(
                            file_path=self.env.derived_absolute(IN_SCOPE_BUNDLE),
                            packages=[package_object(id="pkg:npm/example-package@1.0.0")],
                            vulnerabilities=[
                                vulnerability(name="ADVISORY-A", severity=literal),
                                vulnerability(name="ADVISORY-B", severity=literal),
                            ],
                        )
                    )
                )
                self.assertEqual(len(adapted.rows), 2)
                for row in adapted.rows:
                    self.assertEqual(row["severity_native"], literal, "disclosed as observed")
                    self.assertEqual(row["severity_norm"], "Info")
                self.assertEqual(adapted.counters["severity_basis_unmapped_literal"], 2)

                unmapped = adapted.tally.unmapped_by_tool()[TOOL]
                self.assertEqual(len(unmapped), 1, "one literal, not one entry per row")
                self.assertEqual(unmapped[0].severity_native, literal)
                self.assertEqual(unmapped[0].severity_norm, "Info")
                self.assertEqual(unmapped[0].rows, 2, "with the rows it affected")
                self.assertTrue(unmapped[0].unmapped)

        # The captured artifact carries no unmapped literal, and that is stated rather than
        # left as an untested zero: every literal in it is a mapped label or a bandable number.
        captured = self.env.adapt_fixture(POSITIVE_FIXTURE)
        self.assertEqual(captured.counters["severity_basis_unmapped_literal"], 0)
        self.assertEqual(captured.tally.unmapped_by_tool()[TOOL], ())

    def test_a_record_with_no_severity_vocabulary_states_the_absence(self) -> None:
        """A null ``severity`` and no score: ``severity_native`` absent, ``severity_norm`` Info.

        The absence is stated rather than a level assumed, and ``selected_entry`` is ``None``
        because nothing was used to derive the band -- the band came from policy.
        """
        adapted = self.env.adapt_derived(
            document(
                dependency(
                    file_path=self.env.derived_absolute(IN_SCOPE_BUNDLE),
                    packages=[package_object(id="pkg:npm/example-package@1.0.0")],
                    vulnerabilities=[vulnerability(severity=None)],
                )
            )
        )
        row = adapted.one_row
        self.assertIsNone(row["severity_native"], "absence is permitted for this field")
        self.assertEqual(row["severity_norm"], "Info")
        self.assertEqual(adapted.counters["severity_basis_no_vocabulary"], 1)
        self.assertEqual(adapted.counters["severity_label_absent"], 1)

        result = dependency_check.resolve_severity(
            {"name": "ADVISORY-DERIVED-1", "severity": None}
        )
        self.assertEqual(result.basis, severity.BASIS_NO_VOCABULARY)
        self.assertIsNone(result.selected_entry)
        self.assertIsNone(result.severity_native)
        self.assertEqual(result.severity_norm, "Info")

    # -- 18: severity_norm is never absent ------------------------------------------- #

    def test_severity_norm_is_never_absent_on_any_row(self) -> None:
        """Over every committed fixture and the derived shapes, on every row.

        ``severity_norm`` is not among the five fields absence is permitted in, and
        ``severity.py`` enforces the invariant on every construction of its result -- so this
        asserts it holds through the adapter for real captured output as well as for the
        absent-vocabulary and unmapped-literal paths, which are the two that band from policy.
        """
        for name in ALL_FIXTURES:
            adapted = self.env.adapt_fixture(name)
            for row in adapted.rows:
                self.assertIn(
                    row["severity_norm"],
                    severity.SEVERITY_NORM,
                    f"{name}: {row['rule_id']!r}",
                )
                self.assertIsNotNone(row["severity_norm"])

        for literal in (None, "PROVIDER-SPECIFIC-BAND", "moderate", 0.0, 10.0):
            with self.subTest(literal=literal):
                adapted = self.env.adapt_derived(
                    document(
                        dependency(
                            file_path=self.env.derived_absolute(IN_SCOPE_BUNDLE),
                            packages=[package_object(id="pkg:npm/example-package@1.0.0")],
                            vulnerabilities=[vulnerability(severity=literal)],
                        )
                    )
                )
                self.assertIn(adapted.one_row["severity_norm"], severity.SEVERITY_NORM)

    def test_every_row_of_the_captured_artifact_agrees_with_its_recorded_basis(self) -> None:
        """The basis counters decompose the rows exactly, so no row banded unaccountably.

        Their sum is the row count by construction, which makes the decomposition one
        measurement split rather than a second count -- the property AAP 0.6.4 requires of a
        number appearing twice.
        """
        adapted = self.env.adapt_fixture(POSITIVE_FIXTURE)
        by_basis = sum(
            adapted.counters[f"severity_basis_{basis}"] for basis in severity.BASIS_VALUES
        )
        self.assertEqual(by_basis, len(adapted.rows))
        self.assertEqual(
            adapted.counters["severity_basis_sarif_level"],
            0,
            "no native artifact takes the SARIF level route",
        )
        self.assertEqual(adapted.tally.row_count(TOOL), len(adapted.rows))
        self.assertEqual(
            sum(adapted.tally.band_counts(TOOL).values()),
            len(adapted.rows),
            "the tally is fed once per emitted row, so its per-literal counts are row counts",
        )

    # -- helper ---------------------------------------------------------------------- #

    def _captured_record(self, rule_id: str) -> dict:
        """Return the captured vulnerability whose ``name`` is ``rule_id``.

        Read from the fixture rather than reconstructed, so an assertion about a measured
        literal is made against the bytes the tool wrote.
        """
        for element in load_fixture(POSITIVE_FIXTURE)["dependencies"]:
            for record in element.get("vulnerabilities") or ():
                if isinstance(record, dict) and record.get("name") == rule_id:
                    return record
        raise AssertionError(f"no captured record named {rule_id!r}")


def count_records(doc: dict) -> int:
    """Count ``dependencies[].vulnerabilities[]`` elements, building nothing.

    A deliberately row-free traversal, written here rather than shared with the row builder:
    AAP 0.5.4 names the failure mode exactly -- *"a count taken from the same traversal that
    builds the rows satisfies the assertion while testing nothing"*.  ``normalize.reconcile``
    owns the pipeline's independent count and ``test_reconciliation.py`` asserts it; this is
    a second traversal so that the identity below is checked in this module too, and it reads
    only the containers it needs in order to walk: an element it can make no sense of still
    counts as one record, and a dependency that is not an object or whose ``vulnerabilities``
    is not an array contributes nothing.
    """
    total = 0
    for element in doc.get("dependencies") or ():
        if not isinstance(element, dict):
            continue
        records = element.get("vulnerabilities")
        if isinstance(records, list):
            total += len(records)
    return total


class PositiveMappingTest(AdapterTestCase):
    """The captured positive fixture, asserted field by field against its hand-verified rows.

    The positive fixture is an unmodified capture of the tool's own output, and the expectation
    beside it was derived by reading that output and the authored contracts -- never by running
    the adapter and recording what it printed.  So this class is the one place the two meet, and
    a disagreement is a finding to diagnose rather than a file to overwrite.
    """

    def test_the_row_count_matches_the_expectation_exactly(self) -> None:
        """Eight records, eight rows, no rejection -- and the identity holds."""
        expectation = load_expected(POSITIVE_FIXTURE)
        adapted = self.env.adapt_fixture(POSITIVE_FIXTURE)
        self.assertEqual(len(adapted.rows), expectation["counts"]["rows"])
        self.assertEqual(len(adapted.rejections), expectation["counts"]["rejections"])
        raw = count_records(load_fixture(POSITIVE_FIXTURE))
        self.assertEqual(
            raw,
            expectation["counts"]["raw_finding_records"],
            "the independent count agrees with the hand-verified one",
        )
        self.assertEqual(
            raw,
            len(adapted.rows) + len(adapted.rejections),
            "raw finding records = dataset rows + rejected records",
        )

    def test_every_row_matches_field_by_field_over_the_twelve_fields(self) -> None:
        """Each row asserted against its expectation for all twelve fields, in order.

        The field list is iterated from ``emit.FIELDS`` rather than authored here, so a failure
        names the field that differs instead of printing two dicts -- and the row's key set and
        key order are asserted too, so neither an extra field nor a missing one passes as
        equality.  Document order is asserted by comparing row for row: ``dependencies[]`` in
        order and, within each, ``vulnerabilities[]`` in order, which is the order both output
        files use.
        """
        expectation = load_expected(POSITIVE_FIXTURE)
        adapted = self.env.adapt_fixture(POSITIVE_FIXTURE)
        self.assertEqual(len(adapted.rows), len(expectation["rows"]))
        for index, (observed, expected) in enumerate(
            zip(adapted.rows, expectation["rows"])
        ):
            with self.subTest(row_index=index, rule_id=expected["rule_id"]):
                self.assert_row_matches(observed, expected, f"row {index}")
        self.assert_no_absolute_paths(adapted.rows, "the positive fixture")

    def test_every_counter_matches_the_expectation(self) -> None:
        """All thirty-three counters, including the four the AAP reports per tool."""
        expectation = load_expected(POSITIVE_FIXTURE)
        adapted = self.env.adapt_fixture(POSITIVE_FIXTURE)
        self.assert_counters(
            adapted.counters, expectation["counters"], "the positive fixture"
        )
        self.assertEqual(len(expectation["counters"]), len(dependency_check.COUNTER_KEYS))

    def test_tool_and_scanner_class_are_stamped_on_every_row(self) -> None:
        """``tool`` is the hyphenated identifier and ``scanner_class`` is ``vuln``, everywhere.

        Asserted over every row of every committed fixture rather than over one: the class is a
        constant fixed in advance by AAP 0.5.4's table, not something read off a record, so a
        single row would not show that no code path derives it from content.
        """
        for name in ALL_FIXTURES:
            adapted = self.env.adapt_fixture(name)
            for row in adapted.rows:
                self.assertEqual(row["tool"], TOOL, f"{name}: {row['rule_id']!r}")
                self.assertEqual(
                    row["scanner_class"], SCANNER_CLASS, f"{name}: {row['rule_id']!r}"
                )

    def test_the_dependency_carrying_no_vulnerabilities_key_contributes_nothing(self) -> None:
        """A clean dependency is neither a row nor a rejection -- it is the ordinary shape.

        The captured fixture's eighth dependency is the one manifest-shaped file in scope,
        ``core/src/main/resources/org/apache/spark/ui/static/package.json``, and it carries no
        ``vulnerabilities`` member at all.  Emitting a row per dependency would inflate every
        count in the dataset, so the absence is counted rather than passed over in silence.
        """
        fixture = load_fixture(POSITIVE_FIXTURE)
        clean = fixture["dependencies"][7]
        self.assertTrue(clean["filePath"].endswith(IN_SCOPE_MANIFEST))
        self.assertNotIn("vulnerabilities", clean)

        adapted = self.env.adapt_fixture(POSITIVE_FIXTURE)
        self.assertEqual(adapted.counters["dependencies"], 8)
        self.assertEqual(adapted.counters["dependencies_without_vulnerabilities_array"], 1)
        self.assertEqual(adapted.counters["dependencies_skipped_non_mapping"], 0)
        self.assertNotIn(
            IN_SCOPE_MANIFEST,
            [row["path"] for row in adapted.rows],
            "no row names it, because it carries no record",
        )

    def test_an_empty_report_is_a_clean_report_and_not_an_error(self) -> None:
        """``{"dependencies": []}`` yields no rows, no rejections and no complaint.

        The expected outcome for this tool on this scope: nothing in the twelve authoritative
        globs resolves to a package -- exactly one manifest-shaped file is in scope, five lines
        carrying a name, a license and a type with no dependencies block, and there is no
        ``pom.xml``, ``requirements*.txt``, ``setup.py``, ``pyproject.toml`` or JAR anywhere in
        the eighteen in-scope directories.  A report with nothing to say is not a failure.
        """
        adapted = self.env.adapt(document())
        self.assertEqual(adapted.rows, [])
        self.assertEqual(adapted.rejections, [])
        self.assertEqual(adapted.counters["dependencies"], 0)
        self.assertEqual(
            sorted(adapted.counters), sorted(dependency_check.COUNTER_KEYS)
        )
        self.assertEqual(set(adapted.counters.values()), {0})


class IdentifierAndLineTest(AdapterTestCase):
    """``cve``, ``cwe`` and ``start_line``: one value per field, and one absence by policy."""

    def test_cve_is_populated_only_when_the_name_is_cve_shaped(self) -> None:
        """``cve`` comes from ``name`` when CVE-shaped and is absent otherwise.

        Dependency-Check also reports non-CVE advisory identifiers -- a GHSA identifier, a
        Sonatype identifier, a RetireJS description -- and those belong in ``rule_id`` alone,
        so a name matching nothing yields ``None`` rather than being copied across.  Both
        directions are asserted, on captured rows and on derived ones.
        """
        adapted = self.env.adapt_fixture(POSITIVE_FIXTURE)
        by_rule = {row["rule_id"]: row for row in adapted.rows}
        self.assertEqual(by_rule["CVE-2024-6531"]["cve"], "CVE-2024-6531", "CVE-shaped")
        self.assertIsNone(
            by_rule["sonatype-2021-0163"]["cve"], "a Sonatype identifier is not a CVE"
        )
        self.assertIsNone(
            by_rule["jquery.cookies 2.2.0 unsafe cookie value deserialization"]["cve"],
            "and neither is a RetireJS advisory description",
        )
        self.assertEqual(adapted.counters["cve_absent"], 3)

        for name, expected_cve in (
            ("GHSA-example-shaped-identifier", None),
            ("CVE-2018-10237", "CVE-2018-10237"),
        ):
            with self.subTest(name=name):
                derived = self.env.adapt_derived(
                    document(
                        dependency(
                            file_path=self.env.derived_absolute(IN_SCOPE_BUNDLE),
                            packages=[package_object(id="pkg:npm/example-package@1.0.0")],
                            vulnerabilities=[vulnerability(name=name, severity="LOW")],
                        )
                    )
                )
                self.assertEqual(derived.one_row["rule_id"], name, "rule_id keeps it either way")
                self.assertEqual(derived.one_row["cve"], expected_cve)

    def test_cve_selection_is_by_ascending_year_then_sequence(self) -> None:
        """Where a name carries several CVE identifiers, the lowest is emitted and counted.

        The pair below is chosen so **numeric and lexicographic order disagree**: numerically
        9999 precedes 10000, while as text ``CVE-2024-10000`` precedes ``CVE-2024-9999``.
        Without that disagreement the assertion could not tell the stated ordering from a
        string sort.
        """
        numerically_first = "CVE-2024-9999"
        lexicographically_first = "CVE-2024-10000"
        self.assertLess(
            lexicographically_first,
            numerically_first,
            "the two orders must disagree for this to assert anything",
        )
        name = f"{lexicographically_first} superseded by {numerically_first}"
        selected, distinct = dependency_check.select_cve(name)
        self.assertEqual(selected, numerically_first, "ascending numeric, year then sequence")
        self.assertEqual(distinct, 2)

        adapted = self.env.adapt_derived(
            document(
                dependency(
                    file_path=self.env.derived_absolute(IN_SCOPE_BUNDLE),
                    packages=[package_object(id="pkg:npm/example-package@1.0.0")],
                    vulnerabilities=[vulnerability(name=name, severity="LOW")],
                )
            )
        )
        self.assertEqual(adapted.one_row["cve"], numerically_first)
        self.assertEqual(adapted.counters["multi_valued_cve_records"], 1)
        self.assertEqual(
            self.env.adapt_fixture(POSITIVE_FIXTURE).counters["multi_valued_cve_records"],
            0,
            "no captured name carries more than one, which is why the derived case exists",
        )

    def test_cwe_selection_is_by_ascending_numeric_identifier(self) -> None:
        """One ``cwe`` per row, the lowest numeric identifier, with the multi-valued count.

        The captured record carries ``["CWE-1321", "CWE-915"]``, where numeric and
        lexicographic order disagree -- 915 is the lower number while ``CWE-1321`` is the
        earlier text, and it is also the one the producer put first.  So this single assertion
        separates the stated ordering from both a string sort and a take-the-first reading.
        """
        self.assertLess("CWE-1321", "CWE-915", "as text, the higher number sorts first")
        adapted = self.env.adapt_fixture(POSITIVE_FIXTURE)
        by_rule = {row["rule_id"]: row for row in adapted.rows}
        self.assertEqual(by_rule["CVE-2020-28458"]["cwe"], "CWE-915")
        self.assertEqual(
            self._captured_cwes("CVE-2020-28458"),
            ["CWE-1321", "CWE-915"],
            "the producer order the fixture carries",
        )
        self.assertEqual(adapted.counters["multi_valued_cwe_records"], 1)

        selected, distinct, without_identifier = dependency_check.select_cwe(
            ["CWE-1321", "CWE-915"]
        )
        self.assertEqual((selected, distinct, without_identifier), ("CWE-915", 2, 0))

    def test_a_cwe_entry_carrying_no_identifier_yields_no_cwe(self) -> None:
        """``NVD-CWE-noinfo`` contributes no identifier, and the absence is counted.

        Dependency-Check emits it for a vulnerability whose weakness is unknown.  It is
        counted rather than turned into a fabricated ``CWE-noinfo``, which is why a reader
        seeing ``cwe: null`` on that row can tell policy from a defect.
        """
        adapted = self.env.adapt_fixture(POSITIVE_FIXTURE)
        by_rule = {row["rule_id"]: row for row in adapted.rows}
        self.assertEqual(self._captured_cwes("CVE-2024-49761"), ["NVD-CWE-noinfo"])
        self.assertIsNone(by_rule["CVE-2024-49761"]["cwe"])
        self.assertEqual(adapted.counters["cwe_entries_without_identifier"], 1)
        self.assertEqual(adapted.counters["cwe_absent"], 2)
        self.assertEqual(
            dependency_check.select_cwe(["NVD-CWE-noinfo", "NVD-CWE-Other"]),
            (None, 0, 2),
        )

    def test_start_line_is_absent_on_every_row_of_every_case(self) -> None:
        """``start_line`` is ``None`` throughout, and ``start_line_absent`` equals the rows.

        Dependency-Check reports at dependency granularity -- a vulnerable component, not a
        line of code -- and this shape carries no line information in any member at any depth,
        so synthesising one would be inference.  It is also why the non-integer ``start_line``
        rejection is unreachable for this adapter and no such fixture exists.
        """
        for name in ALL_FIXTURES:
            adapted = self.env.adapt_fixture(name)
            for row in adapted.rows:
                self.assertIsNone(row["start_line"], f"{name}: {row['rule_id']!r}")
            self.assertEqual(
                adapted.counters["start_line_absent"],
                len(adapted.rows),
                f"{name}: the counter equals the row count by design",
            )
        self.assertIsNone(dependency_check._START_LINE)

    def test_a_multi_location_record_takes_the_primary_file_path_and_counts_once(self) -> None:
        """``relatedDependencies`` is the one multi-location shape, and the row takes the primary.

        AAP 0.5.4: the row takes the first location, the record still counts once, and the
        number of records carrying more than one is reported per tool.  No captured dependency
        carries the array, so the case is derived -- and the captured zero is asserted too, so
        the counter is shown to move only when the shape is present.
        """
        related = self.env.derived_absolute("dev/package-lock.json")
        adapted = self.env.adapt_derived(
            document(
                dependency(
                    file_path=self.env.derived_absolute(IN_SCOPE_BUNDLE),
                    packages=[package_object(id="pkg:npm/example-package@1.0.0")],
                    vulnerabilities=[vulnerability(severity="LOW")],
                    relatedDependencies=[
                        {"filePath": related, "fileName": "package-lock.json"}
                    ],
                )
            )
        )
        self.assertEqual(
            adapted.one_row["path"], IN_SCOPE_BUNDLE, "the primary filePath, not the related one"
        )
        self.assertEqual(len(adapted.rows), 1, "the record still counts once")
        self.assertEqual(adapted.counters["multi_location_records"], 1)
        self.assertEqual(
            self.env.adapt_fixture(POSITIVE_FIXTURE).counters["multi_location_records"],
            0,
            "no captured dependency carries the array",
        )

    def _captured_cwes(self, rule_id: str) -> list:
        """Return the ``cwes`` array the captured record whose ``name`` is ``rule_id`` carries."""
        for element in load_fixture(POSITIVE_FIXTURE)["dependencies"]:
            for record in element.get("vulnerabilities") or ():
                if isinstance(record, dict) and record.get("name") == rule_id:
                    return list(record.get("cwes") or ())
        raise AssertionError(f"no captured record named {rule_id!r}")


class NegativeFixtureTest(AdapterTestCase):
    """One fixture per rejection condition this adapter can produce, each asserted by class.

    AAP 0.6.2 requires a negative fixture and an assertion for **every** rejection condition an
    exercised adapter can produce, *"present whether or not this run's artifacts contained the
    case"* -- because a rejection path with no test is a rejection path nobody has exercised.

    Each is asserted against its own expectation, which is the authority for its row count, its
    rejection count and each rejection's class, diagnostic and record identity.  Two readings
    come from those files rather than from a summary, and both matter:

    * the defective **record** produces no row and exactly one counted rejection, while every
      other record in the same document still produces its row.  Three of the five carry two
      rejections, because one defective dependency holding two vulnerabilities is two records
      and the count unit is the record.
    * the fixture whose slug reads ``unresolvable-path`` empties a ``filePath``, so its class is
      ``absent_path``.  Asserting the slug would assert a class the module never produces, which
      is exactly why the class is read from the expectation and checked against
      ``paths.REJECT_CLASSES``.
    """

    def test_each_negative_fixture_produces_its_expected_rejections(self) -> None:
        """Class, count, diagnostic and record identity, per rejection, per fixture."""
        for name in REJECT_FIXTURES:
            with self.subTest(fixture=name):
                expectation = load_expected(name)
                adapted = self.env.adapt_fixture(name)
                expected_rejections = expectation["rejections"]

                self.assertEqual(
                    len(adapted.rejections),
                    expectation["counts"]["rejections"],
                    f"{name}: rejection count",
                )
                self.assertEqual(len(adapted.rejections), len(expected_rejections))

                observed_by_class: dict = {}
                for index, (observed, expected) in enumerate(
                    zip(adapted.rejections, expected_rejections)
                ):
                    self.assert_reject_class_is_real(expected["reject_class"])
                    self.assertEqual(
                        observed.reject_class,
                        expected["reject_class"],
                        f"{name}: rejection {index} class",
                    )
                    self.assertEqual(observed.tool, TOOL, f"{name}: rejection {index} tool")
                    self.assertEqual(
                        observed.detail,
                        expected["expected_detail"],
                        f"{name}: rejection {index} diagnostic, retained verbatim",
                    )
                    self.assertTrue(observed.detail, "an empty diagnostic is the catch-all "
                                    "AAP 0.5.4 forbids")
                    self.assertEqual(
                        dict(observed.record_identity),
                        expected["expected_record_identity"],
                        f"{name}: rejection {index} record identity",
                    )
                    observed_by_class[observed.reject_class] = (
                        observed_by_class.get(observed.reject_class, 0) + 1
                    )

                self.assertEqual(
                    observed_by_class,
                    expectation["counts"]["rejections_by_class"],
                    f"{name}: rejections counted under their named classes",
                )

    def test_each_negative_fixture_still_emits_every_parsable_row(self) -> None:
        """A partial parse is a first-class outcome: the valid records still become rows.

        AAP 0.5.4: every parsable record is emitted, every rejected record counted under a named
        class, and the parse status is ``partial`` with both counts recorded.  So a defective
        record must not abort the artifact -- and each surviving row is asserted field by field
        against the expectation, not merely counted.
        """
        for name in REJECT_FIXTURES:
            with self.subTest(fixture=name):
                expectation = load_expected(name)
                adapted = self.env.adapt_fixture(name)
                self.assertEqual(
                    len(adapted.rows), expectation["counts"]["rows"], f"{name}: row count"
                )
                self.assertGreater(
                    len(adapted.rows),
                    0,
                    f"{name}: the artifact is not abandoned because one record is defective",
                )
                for index, (observed, expected) in enumerate(
                    zip(adapted.rows, expectation["rows"])
                ):
                    self.assert_row_matches(observed, expected, f"{name} row {index}")
                self.assert_no_absolute_paths(adapted.rows, name)

    def test_the_reconciliation_identity_holds_for_every_fixture(self) -> None:
        """``raw finding records = dataset rows + rejected records``, per artifact.

        The left-hand side comes from :func:`count_records`, a traversal that builds nothing,
        and is cross-checked against the hand-verified figure in the expectation -- so the
        identity is asserted between two independently arrived-at numbers rather than within one.
        """
        for name in ALL_FIXTURES:
            with self.subTest(fixture=name):
                expectation = load_expected(name)
                adapted = self.env.adapt_fixture(name)
                raw = count_records(load_fixture(name))
                self.assertEqual(
                    raw,
                    expectation["counts"]["raw_finding_records"],
                    f"{name}: the independent count agrees with the hand-verified one",
                )
                self.assertEqual(
                    raw,
                    len(adapted.rows) + len(adapted.rejections),
                    f"{name}: rows {len(adapted.rows)} + rejections "
                    f"{len(adapted.rejections)}",
                )

    def test_every_counter_matches_for_every_negative_fixture(self) -> None:
        """All thirty-three counters, per negative fixture, including those that must not move.

        The counters a rejected record leaves untouched are the point: a record rejected at step
        5 never reaches the row builder, so no path-kind, in-scope, severity-basis or coordinate
        level counter moves for it, and a reader tallying those numbers can see that.
        """
        for name in REJECT_FIXTURES:
            with self.subTest(fixture=name):
                expectation = load_expected(name)
                adapted = self.env.adapt_fixture(name)
                self.assert_counters(adapted.counters, expectation["counters"], name)

    def test_a_malformed_record_diagnostic_names_the_type_that_arrived(self) -> None:
        """The ``malformed_record`` diagnostic names the observed type, which is what makes it
        actionable.

        Unchecked, a non-object element raises: ``(4.3).get('name')`` is an ``AttributeError``.
        Correct handling turns that into a counted rejection, and the diagnostic names the type
        so a reader can find the element rather than guess at it.
        """
        adapted = self.env.adapt_fixture("reject-dependency-check-malformed-record")
        self.assertEqual(
            adapted.reject_classes,
            (paths.REJECT_MALFORMED_RECORD, paths.REJECT_MALFORMED_RECORD),
        )
        details = [rejection.detail for rejection in adapted.rejections]
        self.assertIn("is a float, not an object", details[0])
        self.assertIn("is a str, not an object", details[1])
        for rejection in adapted.rejections:
            self.assertIn(
                "dependency_index",
                rejection.record_identity,
                "the identity locates the element in the artifact again",
            )
            self.assertIn("vulnerability_index", rejection.record_identity)

    def test_the_dependency_level_malformations_contribute_no_record(self) -> None:
        """A dependency that is not an object, or whose ``vulnerabilities`` is not an array, is
        not a record.

        Counted rather than rejected: a dependency is not a record, so neither shape can produce
        a row or a rejection, and the counters are what show the zero was observed.
        """
        expectation = load_expected("reject-dependency-check-malformed-record")
        adapted = self.env.adapt_fixture("reject-dependency-check-malformed-record")
        self.assertEqual(
            adapted.counters["dependencies_skipped_non_mapping"],
            expectation["counters"]["dependencies_skipped_non_mapping"],
        )
        self.assertEqual(
            adapted.counters["dependencies_without_vulnerabilities_array"],
            expectation["counters"]["dependencies_without_vulnerabilities_array"],
        )

    def test_the_three_step_two_and_three_shapes_each_reject_with_their_own_reason(self) -> None:
        """Absent, blank and non-string all reject, and the diagnostic distinguishes which.

        ``missing_rule_id`` and ``missing_message`` each cover three shapes, so the class alone
        does not say what was wrong with the record.  The diagnostic does, which is why it is
        retained verbatim and asserted here rather than summarised.
        """
        cases = (
            ("name", None, paths.REJECT_MISSING_RULE_ID, "carries no name"),
            ("name", "   ", paths.REJECT_MISSING_RULE_ID, "is empty or whitespace only"),
            ("name", 17, paths.REJECT_MISSING_RULE_ID, "is a int, not a string"),
            (
                "description",
                None,
                paths.REJECT_MISSING_MESSAGE,
                "carries no description",
            ),
            (
                "description",
                "",
                paths.REJECT_MISSING_MESSAGE,
                "is empty or whitespace only",
            ),
            (
                "description",
                ["not", "a", "string"],
                paths.REJECT_MISSING_MESSAGE,
                "is a list, not a string",
            ),
        )
        for field, value, reject_class, fragment in cases:
            with self.subTest(field=field, value=value):
                record = vulnerability(severity="LOW")
                record[field] = value
                adapted = self.env.adapt_derived(
                    document(
                        dependency(
                            file_path=self.env.derived_absolute(IN_SCOPE_BUNDLE),
                            packages=[package_object(id="pkg:npm/example-package@1.0.0")],
                            vulnerabilities=[record],
                        )
                    )
                )
                self.assertEqual(adapted.rows, [], "no row for a record with no identity")
                rejection = adapted.one_rejection
                self.assert_reject_class_is_real(rejection.reject_class)
                self.assertEqual(rejection.reject_class, reject_class)
                self.assertIn(fragment, rejection.detail)

    def test_an_unresolvable_path_rejects_when_the_metadata_supplies_no_base(self) -> None:
        """``unresolvable_path`` is reachable through the recorded metadata, and is asserted there.

        A recorded ``path_base.kind`` of ``none`` supplies no base to anchor on, and the
        document's own instruction is to reject such a record rather than fall back to the root.
        It is a property of the runner metadata rather than of the artifact, which is why this
        case is reached by writing a metadata document and not by shaping one.  The class is the
        sixth this adapter can produce and would otherwise go unexercised by any fixture.
        """
        base = self.env.base_of_kind(paths.PATH_BASE_KIND_NONE)
        self.assertFalse(base.has_explicit_base)
        adapted = self.env.adapt(
            document(
                dependency(
                    file_path=self.env.derived_absolute(IN_SCOPE_BUNDLE),
                    packages=[package_object(id="pkg:npm/example-package@1.0.0")],
                    vulnerabilities=[vulnerability(severity="LOW")],
                )
            ),
            root=self.env.derived_root,
            tool_base=base,
        )
        self.assertEqual(adapted.rows, [])
        rejection = adapted.one_rejection
        self.assert_reject_class_is_real(rejection.reject_class)
        self.assertEqual(rejection.reject_class, paths.REJECT_UNRESOLVABLE_PATH)
        self.assertIn("path_base.kind 'none'", rejection.detail)

    def test_a_non_string_file_path_is_a_malformed_record(self) -> None:
        """A ``filePath`` that is not a string is classified by ``paths.py``, in one place.

        The adapter passes the value through rather than classifying it itself, so the class and
        the diagnostic come from the module that owns path resolution.
        """
        adapted = self.env.adapt_derived(
            document(
                dependency(
                    file_path=1234,
                    packages=[package_object(id="pkg:npm/example-package@1.0.0")],
                    vulnerabilities=[vulnerability(severity="LOW")],
                )
            )
        )
        self.assertEqual(adapted.rows, [])
        rejection = adapted.one_rejection
        self.assertEqual(rejection.reject_class, paths.REJECT_MALFORMED_RECORD)
        self.assertIn(RECORDED_RECORD_PATH_FIELD, rejection.detail)
        self.assertIn("is a int, not a string", rejection.detail)

    def test_every_producible_class_is_exercised_by_this_module(self) -> None:
        """The six classes this adapter can produce are each reached by a test above.

        Asserted as a set rather than trusted to a reading of the file: a condition whose test
        was removed would otherwise leave the coverage claim standing.  The five committed
        fixtures supply four of the six, and the two remaining -- ``unresolvable_path`` and the
        non-string ``filePath`` form of ``malformed_record`` -- are supplied by the derived cases
        in this class.
        """
        observed: set = set()
        for name in REJECT_FIXTURES:
            observed.update(self.env.adapt_fixture(name).reject_classes)

        derived_cases = (
            (
                self.env.adapt_derived(
                    document(
                        dependency(
                            file_path=self.env.derived_absolute(IN_SCOPE_BUNDLE),
                            vulnerabilities=[
                                vulnerability(name="ADVISORY-NO-COORDINATE", severity="LOW")
                            ],
                        )
                    )
                )
            ),
            self.env.adapt(
                document(
                    dependency(
                        file_path=self.env.derived_absolute(IN_SCOPE_BUNDLE),
                        packages=[package_object(id="pkg:npm/example-package@1.0.0")],
                        vulnerabilities=[vulnerability(severity="LOW")],
                    )
                ),
                root=self.env.derived_root,
                tool_base=self.env.base_of_kind(paths.PATH_BASE_KIND_NONE),
            ),
        )
        for adapted in derived_cases:
            observed.update(adapted.reject_classes)

        self.assertEqual(
            observed,
            set(PRODUCIBLE_REJECT_CLASSES),
            "every class this adapter can produce is exercised, and no class it cannot "
            "produce was produced",
        )
        for reject_class in UNPRODUCIBLE_REJECT_CLASSES:
            self.assertNotIn(reject_class, observed)


class CallerFaultTest(AdapterTestCase):
    """A defective *call* is raised; a defective *record* is counted.  The two never merge.

    A rejection describes a record inside an artifact and is counted and carried on from; a
    caller fault -- the wrong tool identifier, a relative root, a document that is not a
    Dependency-Check report -- stops the caller.  Keeping them apart is what stops a wiring
    mistake from being absorbed into a rejection count that still reconciles.
    """

    def test_a_document_that_is_not_this_report_shape_is_refused(self) -> None:
        """Three shapes are refused outright rather than walked to zero rows.

        An empty result set is indistinguishable from a clean scan, and for this tool a clean
        scan is the *expected* outcome -- so a silent zero here would be invisible.  A report
        whose ``dependencies`` array is merely **empty** is not this case and is asserted
        elsewhere as the legitimate clean report it is.
        """
        for doc, description in (
            ([], "a JSON array top level"),
            (document(omit_dependencies=True), "no dependencies member"),
            ({"dependencies": {}}, "a dependencies member that is not an array"),
        ):
            with self.subTest(description=description):
                with self.assertRaises(dependency_check.DependencyCheckAdapterError):
                    self.env.adapt(doc)

    def test_a_relative_root_is_refused_rather_than_producing_wrong_paths(self) -> None:
        """Every path from this tool is absolute and relativized, so a relative root is refused.

        Accepting one would produce a plausible-looking wrong answer for every row of the
        artifact, which is far harder to notice than an error at the call.
        """
        with self.assertRaises(dependency_check.DependencyCheckAdapterError):
            self.env.adapt(load_fixture(POSITIVE_FIXTURE), root="relative/root")

    def test_a_tally_that_cannot_record_is_refused(self) -> None:
        """Every emitted row's literal must reach ``severity-map.md``, so the tally is required.

        Checked by capability rather than by class, so a test double is as acceptable as a
        ``severity.LiteralTally`` -- and ``None`` is not, because a silently skipped tally would
        leave that document under-reporting with nothing to show it had.
        """
        for tally in (None, object()):
            with self.subTest(tally=type(tally).__name__):
                with self.assertRaises(dependency_check.DependencyCheckAdapterError):
                    dependency_check.adapt(
                        load_fixture(POSITIVE_FIXTURE),
                        tool=TOOL,
                        root=FIXTURE_ROOT,
                        tool_base=self.env.fixture_base,
                        allowlist=self.env.globs,
                        tally=tally,
                    )

    def test_an_exhausted_allowlist_cannot_silently_put_every_row_out_of_scope(self) -> None:
        """The globs are materialised once, so a generator cannot be consumed by the first row.

        A generator would leave every row after the first with an empty scope, which reads
        exactly like a scan that found nothing in scope.  Passing one is legitimate; being
        exhausted by it is not.
        """
        adapted = self.env.adapt(
            load_fixture(POSITIVE_FIXTURE),
            root=FIXTURE_ROOT,
            tool_base=self.env.fixture_base,
        )
        generator_adapted = dependency_check.adapt(
            load_fixture(POSITIVE_FIXTURE),
            tool=TOOL,
            root=FIXTURE_ROOT,
            tool_base=self.env.fixture_base,
            allowlist=(glob for glob in AUTHORITATIVE_GLOBS),
            tally=severity.LiteralTally.with_all_tools(),
        )
        self.assertEqual(
            [row["in_scope"] for row in generator_adapted[0]],
            [row["in_scope"] for row in adapted.rows],
            "the same scope decision for every row, whether the globs arrived as a tuple or "
            "as a generator",
        )


if __name__ == "__main__":  # pragma: no cover - convenience for a direct run
    unittest.main(verbosity=2)
