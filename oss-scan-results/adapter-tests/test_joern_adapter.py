"""Assert ``harness/lib/normalize/adapters/joern.py`` field by field, and both gaps it closes.

What this module tests
---------------------
The adapter for ``joern.json`` -- the only artifact in this tree whose shape is the
**harness's own collector output** rather than a tool-native format, and the only adapter
that must *correct* an upstream resolution without editing the upstream file.  AAP 0.6.1
gives this file its row: *"Asserts unique resolution against src/main and src/test, that a
test-JAR finding is retained with in_scope: false, and that an ambiguous or unresolvable
path becomes a counted rejection rather than a row."*  AAP 0.5.4 fixes the behaviour,
AAP 0.9.4 puts it in the definition of done, and AAP 0.9.2 makes a failure here a
condition that **halts the run**.

The two gaps, and why each needs its own assertion
--------------------------------------------------
*Gap 1 -- the collector indexes* ``src/main`` *only.*  Every ``-tests`` artifact the build
emitted is in the graph input (AAP 0.5.1 retains *"main artifacts, pre-shade and shaded
siblings, classifier artifacts and ``-tests`` artifacts"*), so a finding can legitimately
name bytecode compiled from a test tree.  AAP 0.5.4 requires the adapter to resolve
against ``src/main`` **and** ``src/test``, and requires a finding that resolves into
``src/test`` to be **retained with** ``in_scope: false`` -- *"a test-JAR finding kept out of
scope rather than dropped"*.  AAP 0.9.3 repeats it among the conditions that are recorded
and do not stop the run.  A dropped test-tree row and a retained one are indistinguishable
in a row count, so the retention is asserted on a **named row**: its path, its
``in_scope`` value and its presence.

*Gap 2 -- the collector resolves ambiguity first-writer-wins.*  AAP 0.5.4 requires the
resolution be taken *"only where it is unique"* and the ambiguous and the unresolvable to
be **rejected**.  A first-wins pick is not a smaller error than a wrong path: it is the
same error with nothing to show it happened.  So the ambiguous case is asserted twice --
positively, that exactly the expected rejections arrive under a rejection class named
against ``paths.REJECT_CLASSES``; and negatively, that **no** colliding candidate reaches
any field of any row, which is the half a naive implementation fails while every count
still adds up.

The boundary between the two outcomes is the one thing this file exists to keep sharp: an
**ambiguous** coordinate is a rejection, and a coordinate that resolves **uniquely** into
somewhere unwanted is a kept row with ``in_scope: false``.  A fixture that asserted only
the first would accept "reject the inconvenient" as though it were "reject the ambiguous",
and those are not the same instruction.

The count unit
--------------
``findings[]``: one element is one record (AAP 0.5.4).  ``queries[].count`` in the
documented shape and ``queries[].returned`` in this provisioning's shape are the
collector's **own** per-query tallies and are neither the raw count nor a substitute for
it.  Every committed fixture keeps the two numbers different -- the positive fixture
excerpts eleven of the raw artifact's findings while carrying its ``queries`` array
unchanged, so the tallies sum to 692 against eleven records -- which is what makes the
assertion non-vacuous: an implementation counting the tallies would produce 692 against
eleven rows and fail loudly rather than agree silently.

Hermetic by construction
------------------------
This adapter resolves a bytecode class coordinate **against the filesystem**, so a root is
not optional here as it is for the SARIF fixtures.  Every test therefore builds its own
absolute root inside a :class:`tempfile.TemporaryDirectory` and materialises there the
exact relative paths the expected rows name -- in **both** trees, ``src/main`` and
``src/test`` -- each carrying a one-line type declaration where the declaration key scheme
is what resolves it.  The three ambiguity collisions the expected files record are
materialised as genuine collisions rather than simulated, so the rejection is produced by
the resolver rather than arranged by the test.

The scaffold is a scaffold: a comment and a declaration line, written inside a temporary
directory and deleted with it.  **No Spark file is read at test time, no Spark test suite
is executed and no Spark source is modified** -- materialising an empty
``core/src/test/...`` file inside a temporary directory is a test fixture, not a change to
Spark's tree, and every write this module performs is inside that directory.  The expected
rows are root-*relative*, which is what makes a temporary root legitimate rather than a
convenience: the index is keyed on paths relative to the root it was built over, so the
same coordinates resolve to the same relative paths whether the root is the pinned
checkout or this one.  ``cli.py`` runs the identical resolution against ``$SPARK_SRC`` in
the live run.

Nothing here starts a JVM, loads a code-property graph or reads ``harness/cpg/spark.cpg``.
It is a pure data-shape test over an already-parsed document.

Rejection conditions this adapter can produce, and the ones it cannot
--------------------------------------------------------------------
One negative fixture per condition it can produce, present whether or not this run's own
artifact contained the case (AAP 0.6.2): ``unresolvable_path``,
``ambiguous_source_resolution`` -- unique to this adapter -- ``missing_rule_id``,
``missing_message``, ``non_integer_start_line`` and ``malformed_record``.

Three members of ``paths.REJECT_CLASSES`` are **structurally unreachable** here, and each
is named with its reason rather than left as an untested gap:

* ``invalid_uri`` and the ``uriBaseId`` chain faults -- this shape carries no SARIF base
  map and no URI at all, so there is no chain to walk, cycle or exceed;
* ``unformable_package_coordinate`` -- that class covers a *dependency-oriented* record.
  A Joern finding names a bytecode call site rather than a package, so
  ``package_coordinate`` is absent **by design** on every row and its absence is
  explicitly not a rejection;
* ``unattributable_section`` -- this shape has no finding sections to attribute a record
  to; ``scanner_class`` is fixed at ``sast`` for the whole artifact by AAP 0.5.4's class
  table.

``absent_path`` is reachable -- through a coordinate that is absent, blank or the
collector's ``<unknown>`` sentinel -- and is asserted here on synthetic documents, since
no committed fixture carries one.

Prohibitions this module observes
---------------------------------
No cross-tool interpretation of any kind (AAP 0.3.2).  In particular **nothing here
characterises what Joern can express that a rule-based scanner cannot**: that question
belongs to the Stage 5 capability probe under ``queries/joern/`` and
``oss-scan-results/joern-probe.md``, which is one of the run's two deliberate second
appearances -- it writes outside ``harness/artifacts/raw/`` and contributes no dataset row,
and folding the two together would corrupt both counts.  No comparison against Apex,
Cantina or any scanner.  No finding is judged real, important, a false positive or a
duplicate, and nothing is deduplicated.  No secret value appears in any literal, message
or docstring, this tree being committed to git.  No fixture is mutated and nothing under
``harness/lib/normalize/`` is edited: a defect there is reported, never repaired here.
``harness/lib/joern_collect.py`` is never imported -- the collector is provisioned rather
than version-controlled, and this module asserts against its **artifact contract** alone.
Nothing here loads a code-property graph, so the forbidden second-JVM loader appears
nowhere in this file; ``importCpg`` is the probe's only sanctioned loader and the probe
owns it.

Rules
-----
No user-specified rules govern this file.  ``review_rules`` returns exactly one line,
``No user rules provided.``, and that line is the complete document -- corroborated by
AAP 0.7 and AAP 0.10.2.  Enterprise-standard best practice applies in their place and the
absence is **not** licence to lower the bar, which is why the retained ``src/test`` row and
the ambiguity rejection are asserted **positively** rather than implied, and why every
rejection class is asserted by name against the module that owns the vocabulary rather
than by counting rejections.

Running it
----------
Standard library only, no ``pytest``, and runnable from any working directory::

    python3 -m unittest discover -s oss-scan-results/adapter-tests -p 'test_joern_adapter.py'
"""

from __future__ import annotations

import ast
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

# --------------------------------------------------------------------------------------
# The one-time sys.path bootstrap, mirroring the two lines cli.py documents for these
# tests.  There is deliberately no __init__.py under harness/lib/normalize/ or in this
# directory: PEP 420 implicit namespace packages make "from normalize import ..." work once
# harness/lib is on sys.path.  parents[2] of this file is the repository root, so the entry
# is derived from this file's own location rather than from the working directory -- which
# is what lets the module be discovered from the repository root and from anywhere else
# alike.
# --------------------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[2]
_LIB_DIR = str(REPO_ROOT / "harness" / "lib")
if _LIB_DIR not in sys.path:
    sys.path.insert(0, _LIB_DIR)

from normalize import cli, emit, paths, severity  # noqa: E402
from normalize.adapters import joern  # noqa: E402

# --------------------------------------------------------------------------------------
# Locations.  Both directories are inputs and are never written to by this module.
# --------------------------------------------------------------------------------------
ADAPTER_TESTS_DIR = Path(__file__).resolve().parent
FIXTURES_DIR = ADAPTER_TESTS_DIR / "fixtures"
EXPECTED_DIR = ADAPTER_TESTS_DIR / "expected"

#: The canonical tool identifier under test (AAP 0.5.4's class table).
TOOL = "joern"

#: Its scanner class, fixed per tool by that same table with Trivy the single exception.
SCANNER_CLASS = "sast"

# --------------------------------------------------------------------------------------
# The twelve authoritative scope globs (AAP 0.3.1), byte-exact and in the request's order.
#
# Restated here rather than read from paths.ALLOWLIST_GLOBS: the test writes these twelve
# lines to its own allowlist file, loads them back through paths.load_allowlist() and then
# confirms the loaded tuple is what paths.py authors, through
# paths.allowlist_matches_authoritative_globs().  Loading the module's own copy and
# comparing it with itself would assert nothing.  There is no exclusion line -- the literal
# `src/test` exclusion lives in paths.in_scope, not in the allowlist.
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
# The joern entry this module writes into its own minimal runner-metadata.json.
#
# Read as *input* from harness/artifacts/logs/runner-metadata.json and restated here, never
# inferred from an artifact (AAP 0.5.4: "every base taken from the recorded runner
# metadata").  The base value is null because no filesystem base exists for a bytecode
# class: the runner queries a graph the frontend built over staged JARs, so the emitted
# `file` member is by construction the frontend's extraction path for a .class member and
# can never be a path in the Spark tree.  `class` is therefore the only resolvable
# coordinate, and `file` is named as the member to ignore.
# --------------------------------------------------------------------------------------
RECORDED_PATH_BASE = {
    "kind": "bytecode_class",
    "value": None,
    "record_path_field": "class",
    "record_path_field_to_ignore": "file",
}

# --------------------------------------------------------------------------------------
# The collector's own resolution explanation: exactly three values, and none of them may
# reach a dataset field (AAP 0.5.4).  Taken from paths.py for the two it names, so the two
# modules cannot spell them differently, and asserted against the adapter's own tuple.
# --------------------------------------------------------------------------------------
PATH_RESOLUTION_LITERALS = (
    paths.BASIS_SOURCE_INDEX_FILENAME,
    paths.BASIS_SOURCE_INDEX_DECLARATION,
    paths.COLLECTOR_UNRESOLVED_BYTECODE_ONLY,
)

#: The five ``joern.``-prefixed identifiers the documented collector emits.
DOCUMENTED_RULE_IDS = (
    "joern.process-launch-site",
    "joern.java-deserialization-site",
    "joern.reflective-class-load",
    "joern.weak-hash-algorithm",
    "joern.rpc-handler-reaches-process-launch",
)

#: The six this provisioning's baked query set emits (harness/lib/joern-scan.sc).
PROVISIONED_RULE_IDS = (
    "joern-process-exec",
    "joern-unsafe-deserialization",
    "joern-reflection-forname",
    "joern-message-digest",
    "joern-cipher-getinstance",
    "joern-xml-factory",
)

#: The members carried into a rejection's identity as context and into **no** row.
CONTEXT_MEMBERS = ("method_full_name", "method", "callee", "file")

# --------------------------------------------------------------------------------------
# The scaffold tree.
#
# One entry per source path an expected row names, plus the pairs that make the three
# recorded collisions genuine.  Every path is a real relative path in the pinned tree, and
# every one is materialised inside a TemporaryDirectory -- never in the checkout.
#
# `declares` is the list of top-level type names the file's single declaration line
# announces, which is what paths.build_source_index's declaration scheme keys on.  It is
# not decoration: two of the positive fixture's eleven rows resolve on the declaration
# scheme *alone*, because no file is named after the class the finding reports --
# ProcessBuilderLike is declared in DriverRunner.scala and ObjectInputStreamWithLoader in
# Checkpoint.scala.  A filename-only index loses both silently, and a silently lost row is
# indistinguishable from a class with nothing to report.
#
# The scaffold is deliberately not Spark source: each file is a comment and a declaration
# line.  Nothing in this module reads Spark's own files, and nothing it writes survives the
# test run.
# --------------------------------------------------------------------------------------
SCAFFOLD = (
    # --- resolutions the positive fixture's rows name -------------------------------
    ("launcher/src/main/java/org/apache/spark/launcher/SparkLauncher.java", ("SparkLauncher",)),
    ("core/src/main/scala/org/apache/spark/rdd/PipedRDD.scala", ("PipedRDD",)),
    (
        "core/src/main/scala/org/apache/spark/deploy/worker/ExecutorRunner.scala",
        ("ExecutorRunner",),
    ),
    # DriverRunner.scala declares both its own type and ProcessBuilderLike, which is the
    # declaration-only resolution for ProcessBuilderLike$$anon$3.
    (
        "core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala",
        ("DriverRunner", "ProcessBuilderLike"),
    ),
    (
        "sql/connect/common/src/main/scala/org/apache/spark/sql/connect/common/UdfPacket.scala",
        ("UdfPacket",),
    ),
    (
        "common/network-yarn/src/main/java/org/apache/spark/network/yarn/YarnShuffleService.java",
        ("YarnShuffleService",),
    ),
    # Checkpoint.scala declares ObjectInputStreamWithLoader: the second declaration-only
    # resolution.
    (
        "streaming/src/main/scala/org/apache/spark/streaming/Checkpoint.scala",
        ("Checkpoint", "ObjectInputStreamWithLoader"),
    ),
    (
        "sql/hive-thriftserver/src/main/java/org/apache/hive/service/CookieSigner.java",
        ("CookieSigner",),
    ),
    (
        "sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/"
        "ExpressionImplUtils.java",
        ("ExpressionImplUtils",),
    ),
    (
        "sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/xml/StaxXmlParserUtils.scala",
        ("StaxXmlParserUtils",),
    ),
    # --- the src/test tree: gap 1's proof, and the retained row ----------------------
    (
        "core/src/test/scala/org/apache/spark/deploy/master/MasterSuite.scala",
        ("MasterSuite",),
    ),
    # --- resolutions the negative fixtures' rows name --------------------------------
    ("core/src/main/scala/org/apache/spark/deploy/master/Master.scala", ("Master",)),
    (
        "common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/"
        "OneForOneBlockPusher.java",
        ("OneForOneBlockPusher",),
    ),
    # An anchor from this file's own source_files, used by the synthetic null-path case.
    ("core/src/main/scala/org/apache/spark/storage/DiskStore.scala", ("DiskStore",)),
    # --- collision 1: two files share one filename key, declaring disjoint types ------
    (
        "sql/api/src/main/scala/org/apache/spark/sql/catalyst/expressions/rows.scala",
        ("GenericRow", "GenericRowWithSchema"),
    ),
    (
        "sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/rows.scala",
        ("BaseGenericInternalRow", "GenericInternalRow"),
    ),
    # --- collision 2: unique by filename, two claimants once declarations are read ----
    # sql/connect/shims really does ship stub declarations of core's types for client-only
    # builds, so the collision is structural rather than incidental -- and it is the case
    # that separates a union reading from an implementation that merely orders its lookups.
    ("core/src/main/scala/org/apache/spark/SparkContext.scala", ("SparkContext",)),
    (
        "sql/connect/shims/src/main/scala/org/apache/spark/shims.scala",
        ("SparkContext", "SparkConf"),
    ),
    # --- collision 3: two test trees, visible only because gap 1 is closed ------------
    (
        "core/src/test/scala/org/apache/spark/SparkContextSuite.scala",
        ("SparkContextSuite",),
    ),
    (
        "hadoop-cloud/src/test/scala/org/apache/spark/SparkContextSuite.scala",
        ("SparkContextSuite",),
    ),
)

#: A class key no scaffold file claims, so it resolves to nothing.  The third-party class
#: shaded into Spark's JARs that the unresolvable fixture reports.
UNRESOLVABLE_CLASS_KEY = "org/sparkproject/guava/hash/MessageDigestHashFunction"

#: The committed positive fixture, in this provisioning's collector shape.
POSITIVE_FIXTURE = "joern.json"

#: One negative fixture per rejection condition this adapter can produce (AAP 0.6.2), with
#: the class each is expected to produce.  The class is restated from paths.py's constants
#: rather than as a bare string, so a typo cannot invent a class the vocabulary lacks; the
#: expected file's own recorded class name is asserted against it as well.
NEGATIVE_FIXTURES = (
    ("reject-joern-unresolvable-path", paths.REJECT_UNRESOLVABLE_PATH),
    ("reject-joern-ambiguous-path", paths.REJECT_AMBIGUOUS_SOURCE_RESOLUTION),
    ("reject-joern-missing-rule-id", paths.REJECT_MISSING_RULE_ID),
    ("reject-joern-missing-message", paths.REJECT_MISSING_MESSAGE),
    ("reject-joern-non-integer-start-line", paths.REJECT_NON_INTEGER_START_LINE),
    ("reject-joern-malformed-record", paths.REJECT_MALFORMED_RECORD),
)

#: Every committed joern fixture, positive first.
ALL_FIXTURE_STEMS = ("joern",) + tuple(stem for stem, _ in NEGATIVE_FIXTURES)

#: The three rejection classes this shape cannot produce, each with the reason.  Asserted
#: absent from every fixture's rejections, so "cannot happen" is a measured claim.
UNREACHABLE_REJECT_CLASSES = {
    paths.REJECT_INVALID_URI: (
        "this shape carries no URI and no SARIF base map, so there is no reference to "
        "parse and no uriBaseId chain to walk, cycle or exceed"
    ),
    paths.REJECT_UNFORMABLE_PACKAGE_COORDINATE: (
        "that class covers a dependency-oriented record. A Joern finding names a bytecode "
        "call site rather than a package, so package_coordinate is absent by design on "
        "every row and its absence is explicitly not a rejection"
    ),
    paths.REJECT_UNATTRIBUTABLE_SECTION: (
        "this shape has no finding sections to attribute a record to; scanner_class is "
        "fixed at sast for the whole artifact by AAP 0.5.4's class table"
    ),
}

#: Secret prefixes the run scans staged content for (AAP 0.5.4's credential handling).  This
#: module asserts its own source and the fixtures it reads carry none.
SECRET_PATTERNS = (
    "sk_live_",
    "sk_test_",
    "pk_live_",
    "AKIA",
    "ASIA",
    "ghp_",
    "gho_",
    "ghs_",
    "github_pat_",
    "xoxb-",
    "xoxa-",
    "xoxp-",
    "AIza",
    "BEGIN RSA PRIVATE KEY",
    "BEGIN PRIVATE KEY",
    "BEGIN OPENSSH PRIVATE KEY",
)


# --------------------------------------------------------------------------------------
# Reading the committed inputs.  Read-only, always: a fixture is captured tool output and
# an expected file is a hand-verified derivation from it, and mutating either would make
# the test agree with itself.
# --------------------------------------------------------------------------------------


def read_json(path: Path) -> object:
    """Parse a committed JSON input, leaving the file untouched."""
    return json.loads(path.read_text(encoding="utf-8"))


def load_fixture(stem: str) -> dict:
    """Return one committed fixture document, parsed.

    The document is parsed fresh on every call, so a test that reads a member cannot
    affect another test through a shared mutable object.
    """
    document = read_json(FIXTURES_DIR / f"{stem}.json")
    if not isinstance(document, dict):
        raise AssertionError(
            f"fixture {stem}.json must be a JSON object carrying a findings array; "
            f"observed {type(document).__name__}"
        )
    return document


def load_expected(stem: str) -> dict:
    """Return one hand-verified expected result, parsed."""
    document = read_json(EXPECTED_DIR / f"{stem}.rows.json")
    if not isinstance(document, dict):
        raise AssertionError(
            f"expected/{stem}.rows.json must be a JSON object; observed "
            f"{type(document).__name__}"
        )
    return document


def sha256_of(path: Path) -> str:
    """Return the hex sha256 of a file's bytes."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def independent_record_count(document: object) -> int:
    """Count ``findings[]`` elements, building nothing.

    The independent half of ``raw finding records = dataset rows + rejected records``.  It
    reads exactly one container key and takes its length: no row is built, no field is
    extracted, no path is resolved and no severity is mapped.  AAP 0.5.4 names the failure
    mode this avoids -- *"A count taken from the same traversal that builds the rows
    satisfies the assertion while testing nothing."*

    A ``str`` is never treated as an array, since ``len()`` over one would count characters
    as findings.  An absent, empty or non-array ``findings`` counts zero and is not an
    error, which is how the adapter reads it too.
    """
    if not isinstance(document, dict):
        return 0
    findings = document.get("findings")
    if isinstance(findings, (str, bytes, bytearray)) or not isinstance(findings, list):
        return 0
    return len(findings)


def per_query_tally_sum(document: object) -> int:
    """Sum the collector's own per-query tallies -- the number that is *not* the count unit.

    ``count`` in the documented shape and ``returned`` in this provisioning's shape.
    Computed only so a test can assert it **differs** from the record count: without the
    divergence the count-unit assertion would hold for an implementation that used the
    wrong number.
    """
    if not isinstance(document, dict):
        return 0
    queries = document.get("queries")
    if not isinstance(queries, list):
        return 0
    total = 0
    for entry in queries:
        if not isinstance(entry, dict):
            continue
        for member in ("count", "returned"):
            value = entry.get(member)
            if isinstance(value, int) and not isinstance(value, bool):
                total += value
                break
    return total


# --------------------------------------------------------------------------------------
# The hermetic environment: a scan root with both source trees, an allowlist and runner
# metadata.  All three are real files inside one temporary directory, and both
# configuration files are read back through paths.py's own loaders rather than handed to
# the adapter as literals -- so the loaders are exercised on the same route cli.py uses.
# --------------------------------------------------------------------------------------


class Environment:
    """Everything one test class shares: a scan root, an allowlist and a path base.

    Attributes:
        directory: The temporary directory holding everything this object created.
        root_path: The scan root as a :class:`pathlib.Path`.
        root: The same root as an absolute string, which is what ``adapt`` takes.
        globs: The twelve authoritative globs, as ``paths.load_allowlist`` returned them.
        allowlist_path: Where those globs were written.
        metadata: The runner-metadata document, as ``paths.load_runner_metadata``
            returned it.
        metadata_path: Where that document was written.
        tool_base: ``joern``'s :class:`normalize.paths.ToolPathBase`, taken from the
            loaded document rather than constructed directly.
        scaffold_paths: Every relative path materialised under the root, in scaffold
            order.
    """

    def __init__(self, directory: Path) -> None:
        """Materialise the tree, then write and load the allowlist and the metadata."""
        self.directory = directory
        self.root_path = directory / "spark-src"
        self.root_path.mkdir(parents=True, exist_ok=True)
        self.root = str(self.root_path)

        self.scaffold_paths = tuple(relative for relative, _ in SCAFFOLD)
        for relative, declares in SCAFFOLD:
            self._materialise(relative, declares)

        self.allowlist_path = directory / "allowlist.txt"
        # One glob per line, byte-exact, with a trailing newline and nothing else.  No
        # exclusion line: the literal `src/test` rule is paths.in_scope's, not the file's.
        self.allowlist_path.write_text(
            "".join(f"{glob}\n" for glob in AUTHORITATIVE_GLOBS), encoding="utf-8"
        )
        self.globs = paths.load_allowlist(self.allowlist_path)

        self.metadata_path = directory / "runner-metadata.json"
        self.metadata_path.write_text(
            json.dumps(self._metadata_document(), indent=1) + "\n", encoding="utf-8"
        )
        self.metadata = paths.load_runner_metadata(self.metadata_path)
        self.tool_base = paths.tool_path_base(self.metadata, TOOL)

    def _materialise(self, relative: str, declares: tuple[str, ...]) -> None:
        """Create one scaffold file: a provenance comment and one declaration per type.

        ``//`` comments and a bare ``class X {}`` are valid in both Scala and Java, and
        both are what ``paths._DECLARATION_PATTERNS`` matches per language.  The file is a
        key for the source index, not a copy of Spark's source.
        """
        target = self.root_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "// Test scaffold written by test_joern_adapter.py inside a temporary",
            "// directory. It exists so the source index can key this path. It is not",
            "// Apache Spark source and it is deleted with the temporary directory.",
        ]
        lines.extend(f"class {name} {{}}" for name in declares)
        target.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _metadata_document(self) -> dict:
        """Build the minimal document ``paths.load_runner_metadata`` accepts.

        Minimal is deliberate.  It carries the base facts a resolver needs -- the pinned
        root, joern's recorded ``path_base`` with its coordinate member and its member to
        ignore -- and nothing that would make this test a second copy of the run's record.
        AAP 0.6.4 fixes the direction: Stage 1 writes the metadata, the normalizer reads it
        as input, and ``tool-status.md`` is rendered from it afterwards.
        """
        return {
            "purpose": (
                "Minimal runner metadata for the Joern adapter test. Written and read "
                "inside a temporary directory; it is not the run's record."
            ),
            "spark_src": self.root,
            "tools": {
                TOOL: {
                    "canonical_tool_identifier": TOOL,
                    "scanner_class": SCANNER_CLASS,
                    "resolved_scan_root": self.root,
                    "path_base": dict(RECORDED_PATH_BASE),
                }
            },
        }

    # -- derived expectations, computed from the scaffold spec rather than from the index -

    def expected_filename_keys(self) -> dict[str, tuple[str, ...]]:
        """The ``by_filename`` index this scaffold implies, derived from the spec.

        Derived from :data:`SCAFFOLD` by the same rule ``paths.source_index_key``
        documents -- the package directory is everything after
        ``<module>/src/{main,test}/<language>/`` -- so the assertion compares two
        independently produced answers rather than the index with itself.
        """
        keys: dict[str, list[str]] = {}
        for relative, _ in SCAFFOLD:
            keyed = paths.source_index_key(relative)
            if keyed is None:  # pragma: no cover - every scaffold path is on a source root
                raise AssertionError(
                    f"scaffold path {relative!r} carries no src/{{main,test}}/<language>/ "
                    "prefix, so no index key can be derived from it"
                )
            package, stem = keyed
            key = f"{package}/{stem}" if package else stem
            keys.setdefault(key, []).append(relative)
        return {key: tuple(value) for key, value in keys.items()}

    def expected_declaration_keys(self) -> dict[str, tuple[str, ...]]:
        """The ``by_decl`` index this scaffold implies, derived from the spec."""
        keys: dict[str, list[str]] = {}
        for relative, declares in SCAFFOLD:
            keyed = paths.source_index_key(relative)
            if keyed is None:  # pragma: no cover - see expected_filename_keys
                raise AssertionError(f"scaffold path {relative!r} is not on a source root")
            package, _ = keyed
            for declared in declares:
                key = f"{package}/{declared}" if package else declared
                keys.setdefault(key, []).append(relative)
        return {key: tuple(value) for key, value in keys.items()}


class Adapted:
    """One document's adaptation, measured once and shared by every assertion over it.

    Both sides of the reconciliation identity are held here so a test asserts over one
    measurement rather than taking a second: AAP 0.6.4 requires a count that appears twice
    to be one measurement cited twice.

    Attributes:
        document: The parsed artifact handed to the adapter.
        rows: The dataset rows it emitted, each carrying the twelve fields in order.
        rejections: The :class:`normalize.paths.Rejection` records it counted instead.
        counters: Its counter mapping.
        tally: The :class:`normalize.severity.LiteralTally` it fed, one record per row.
        raw_records: ``findings[]`` counted by the independent traversal above.
    """

    __slots__ = ("document", "rows", "rejections", "counters", "tally", "raw_records")

    def __init__(self, document: dict, environment: Environment, **keywords: object) -> None:
        """Adapt ``document`` against ``environment`` and take the independent count."""
        self.document = document
        self.tally = severity.LiteralTally.with_all_tools()
        self.rows, self.rejections, self.counters = joern.adapt(
            document,
            tool=TOOL,
            root=environment.root,
            tool_base=environment.tool_base,
            allowlist=environment.globs,
            tally=self.tally,
            **keywords,
        )
        self.raw_records = independent_record_count(document)

    @property
    def paths_emitted(self) -> tuple[str, ...]:
        """Every emitted row's ``path`` field, in row order."""
        return tuple(row["path"] for row in self.rows)

    @property
    def reject_classes(self) -> tuple[str, ...]:
        """Every rejection's class, in rejection order."""
        return tuple(rejection.reject_class for rejection in self.rejections)

    def rejections_by_class(self) -> dict[str, int]:
        """Rejection counts per named class, tallied as ``cli.py`` tallies them."""
        counts: dict[str, int] = {}
        for rejection in self.rejections:
            counts[rejection.reject_class] = counts.get(rejection.reject_class, 0) + 1
        return counts

    def parse_status(self) -> str:
        """The parse status ``cli.py`` would assign: partial where a record was rejected."""
        return cli.PARSE_STATUS_PARTIAL if self.rejections else cli.PARSE_STATUS_CLEAN


class HermeticRootTestCase(unittest.TestCase):
    """A test class with its own temporary scan root, allowlist and runner metadata.

    A fresh :class:`Environment` per class rather than per module: the tree is twenty small
    files, so rebuilding it costs nothing measurable and no test can observe a mutation
    another made.  ``addClassCleanup`` removes the directory whether the class passed or
    failed, so nothing this module writes outlives the run.
    """

    environment: Environment

    @classmethod
    def setUpClass(cls) -> None:
        """Create the temporary directory and build the environment inside it."""
        temporary = tempfile.TemporaryDirectory(prefix="blitzy-joern-adapter-test-")
        cls.addClassCleanup(temporary.cleanup)
        cls.environment = Environment(Path(temporary.name))

    def adapt_fixture(self, stem: str, **keywords: object) -> Adapted:
        """Adapt one committed fixture against this class's environment."""
        return Adapted(load_fixture(stem), self.environment, **keywords)

    def adapt_document(self, document: dict, **keywords: object) -> Adapted:
        """Adapt a document authored in the test itself -- never a mutated fixture."""
        return Adapted(document, self.environment, **keywords)

    def assertRowsMatchExpected(
        self,
        rows: list[dict],
        expected_rows: list[dict],
        *,
        label: str,
    ) -> None:
        """Assert two row sequences agree field by field, over ``emit.FIELDS`` in order.

        The field list is iterated from the authored constant rather than from either row's
        keys, so a failure names the field, a missing key fails rather than being skipped,
        and an extra key is caught by the key-set assertion.  Row order is asserted as
        given: no sort, no grouping and no deduplication (AAP 0.3.2).
        """
        self.assertEqual(
            len(rows),
            len(expected_rows),
            f"{label}: row count differs from expected/{label}.rows.json",
        )
        for index, (produced, expected) in enumerate(zip(rows, expected_rows)):
            with self.subTest(fixture=label, row=index):
                self.assertEqual(
                    tuple(produced),
                    emit.FIELDS,
                    f"{label} row {index}: the row's keys are not emit.FIELDS in order",
                )
                self.assertEqual(
                    set(expected),
                    set(emit.FIELDS),
                    f"{label} row {index}: the expected row does not carry exactly the "
                    "twelve fields",
                )
                for field in emit.FIELDS:
                    with self.subTest(field=field):
                        self.assertEqual(
                            produced[field],
                            expected[field],
                            f"{label} row {index}: field {field!r} differs",
                        )


#: The five counters whose value is a property of the tree the index was built over rather
#: than of the artifact.  Every expected file records them as measured over the pinned
#: checkout -- 6,759 files, 6,755 filename keys, 15,230 declaration keys, 4 and 107
#: ambiguous -- so under this module's own root they are asserted against an expectation
#: derived from the scaffold spec instead.  Every other counter is artifact-determined and
#: is compared with the expected file verbatim.
ROOT_DEPENDENT_COUNTERS = (
    "source_index_files_indexed",
    "source_index_by_filename_keys",
    "source_index_by_decl_keys",
    "source_index_ambiguous_by_filename",
    "source_index_ambiguous_by_decl",
)


# --------------------------------------------------------------------------------------
# The collector's artifact contract, and the count unit that rests on it
# --------------------------------------------------------------------------------------


class CollectorArtifactContractTest(HermeticRootTestCase):
    """The contract this adapter is written against, asserted rather than assumed.

    There is no published Joern output format to look up: ``harness/bin/run-joern.sh``
    loads the graph and runs a baked query set, and that script writes the artifact.  Two
    shapes of it exist -- the one this provisioning writes and the one the specification
    documents -- and the adapter reads both, which is what AAP 0.1.1 requires by
    *"detecting each artifact's shape rather than assuming it"*.
    """

    def test_scanner_class_is_sast_and_fixed_in_advance(self) -> None:
        """Assertion 12a: ``tool`` is ``joern`` and ``scanner_class`` is ``sast``.

        Fixed per tool by AAP 0.5.4's class table, with Trivy the single exception, so it is
        a constant of the artifact rather than something derived from a record.
        """
        self.assertEqual(joern.SCANNER_CLASS, SCANNER_CLASS)
        self.assertEqual(joern.TOOL, TOOL)
        adapted = self.adapt_fixture("joern")
        self.assertTrue(adapted.rows, "the positive fixture must produce rows")
        for index, row in enumerate(adapted.rows):
            with self.subTest(row=index):
                self.assertEqual(row["tool"], TOOL)
                self.assertEqual(row["scanner_class"], SCANNER_CLASS)

    def test_field_order_is_the_emitter_s_field_order(self) -> None:
        """The adapter's authored field tuple agrees with ``emit.FIELDS`` by construction.

        The adapter may import only ``paths`` and ``severity`` (AAP 0.6.4), so it cannot
        import ``emit.FIELDS`` and keeps an authored copy.  A copy that agrees "by
        construction" is a claim, and this is the check on it.
        """
        self.assertEqual(joern.FIELDS, emit.FIELDS)
        self.assertEqual(joern.ABSENCE_PERMITTED_FIELDS, emit.OPTIONAL_FIELDS)
        self.assertNotIn(
            "path",
            emit.OPTIONAL_FIELDS,
            "path is never absent from a row (AAP 0.8.2), so it must not be optional",
        )
        self.assertNotIn("severity_norm", emit.OPTIONAL_FIELDS)

    def test_cli_routes_the_joern_key_to_this_adapter(self) -> None:
        """``cli.py``'s registry resolves ``joern`` to the module under test."""
        self.assertIs(cli.ADAPTER_REGISTRY[TOOL], joern)
        self.assertTrue(callable(joern.adapt))

    def test_count_unit_is_findings_not_the_per_query_tallies(self) -> None:
        """The reconciliation unit is ``len(findings)``, never the collector's own tallies.

        Asserted on a fixture where the two numbers **differ**: the positive fixture
        excerpts eleven of the raw artifact's findings while carrying its ``queries`` array
        unchanged, so the tallies sum to 692.  Without that divergence an implementation
        using the wrong number would satisfy the identity and the assertion would test
        nothing (AAP 0.5.4).
        """
        adapted = self.adapt_fixture("joern")
        tallies = per_query_tally_sum(adapted.document)
        self.assertNotEqual(
            tallies,
            adapted.raw_records,
            "the fixture must keep the per-query tallies and the record count different, "
            "or this assertion is vacuous",
        )
        self.assertEqual(adapted.raw_records, 11)
        self.assertEqual(tallies, 692)
        self.assertEqual(
            len(adapted.rows) + len(adapted.rejections),
            adapted.raw_records,
            "raw finding records must equal dataset rows plus rejected records",
        )
        self.assertNotIn(
            tallies,
            (len(adapted.rows), len(adapted.rows) + len(adapted.rejections)),
            "no published figure may coincide with the per-query tally sum",
        )

    def test_the_adapter_publishes_no_substitute_for_the_independent_count(self) -> None:
        """No counter carries the record count, so the identity's left side stays external.

        A plausible substitute published by the traversal that builds the rows is how the
        requirement for an independent count would quietly be lost (AAP 0.5.4).
        """
        adapted = self.adapt_fixture("joern")
        for key in ("findings", "records", "raw", "returned", "queries_returned"):
            self.assertNotIn(key, adapted.counters)
        forbidden = {
            "findings_walked",
            "records_walked",
            "raw_finding_records",
            "envelope_queries_returned",
        }
        self.assertEqual(forbidden & set(adapted.counters), set())

    def test_count_unit_holds_over_every_committed_fixture(self) -> None:
        """``raw = rows + rejections`` over each fixture, against the independent count."""
        for stem in ALL_FIXTURE_STEMS:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                expected = load_expected(stem)
                self.assertEqual(
                    adapted.raw_records,
                    expected["counts"]["raw_finding_records"],
                    "the independent count disagrees with the expected raw record count",
                )
                self.assertEqual(
                    len(adapted.rows) + len(adapted.rejections),
                    adapted.raw_records,
                )
                self.assertEqual(len(adapted.rows), expected["counts"]["rows"])
                self.assertEqual(
                    len(adapted.rejections), expected["counts"]["rejections"]
                )
                self.assertEqual(
                    adapted.rejections_by_class(),
                    expected["counts"]["rejections_by_class"],
                )

    def test_rule_identifiers_pass_through_unchanged(self) -> None:
        """Emitted ``rule_id`` values come from the artifact and are never re-derived.

        Both vocabularies are exercised: the six identifiers this provisioning's baked
        query set emits under ``query_id``, and the five ``joern.``-prefixed literals the
        documented collector emits under ``rule_id``.
        """
        observed: set[str] = set()
        for stem in ALL_FIXTURE_STEMS:
            adapted = self.adapt_fixture(stem)
            document_identifiers = {
                finding.get("rule_id") or finding.get("query_id")
                for finding in adapted.document["findings"]
                if isinstance(finding, dict)
            }
            for index, row in enumerate(adapted.rows):
                with self.subTest(fixture=stem, row=index):
                    self.assertIn(
                        row["rule_id"],
                        document_identifiers,
                        "the emitted rule_id is not a value the artifact carried",
                    )
                    observed.add(row["rule_id"])
        for identifier in DOCUMENTED_RULE_IDS:
            with self.subTest(identifier=identifier):
                self.assertIn(identifier, joern.KNOWN_RULE_IDS)
        for identifier in PROVISIONED_RULE_IDS:
            with self.subTest(identifier=identifier):
                self.assertIn(identifier, joern.KNOWN_RULE_IDS)
        self.assertTrue(
            observed & set(DOCUMENTED_RULE_IDS),
            "the documented collector's identifiers must be exercised",
        )
        self.assertTrue(
            observed & set(PROVISIONED_RULE_IDS),
            "this provisioning's identifiers must be exercised",
        )

    def test_an_unlisted_rule_identifier_is_not_filtered(self) -> None:
        """An identifier outside the known set is a legitimate finding, not a drop.

        AAP 0.4.2 has the baked query bundle's composition *read from the runner*, so a
        query set newer than the adapter's constant must still produce rows -- dropping one
        would silently shrink the tool's count.
        """
        unlisted = "joern.query-added-after-this-constant-was-written"
        self.assertNotIn(unlisted, joern.KNOWN_RULE_IDS)
        adapted = self.adapt_document(
            {
                "tool": TOOL,
                "queries": [{"id": unlisted, "count": 9}],
                "findings": [
                    {
                        "rule_id": unlisted,
                        "message": "a call site reported by a query this constant predates",
                        "path": "core/src/main/scala/org/apache/spark/storage/DiskStore.scala",
                        "start_line": 72,
                        "method_full_name": "org.apache.spark.storage.DiskStore.put:void()",
                        "class_file": "org/apache/spark/storage/DiskStore.class",
                        "path_resolution": paths.BASIS_SOURCE_INDEX_FILENAME,
                    }
                ],
            }
        )
        self.assertEqual(adapted.rejections, [])
        self.assertEqual(len(adapted.rows), 1)
        self.assertEqual(adapted.rows[0]["rule_id"], unlisted)

    def test_the_path_resolution_vocabulary_is_the_documented_three(self) -> None:
        """``path_resolution`` takes exactly three values, spelled as ``paths.py`` spells them."""
        self.assertEqual(joern.COLLECTOR_EXPLANATIONS, PATH_RESOLUTION_LITERALS)
        self.assertEqual(
            PATH_RESOLUTION_LITERALS,
            ("source-index-filename", "source-index-declaration", "unresolved-bytecode-only"),
        )

    def test_the_documented_shape_carries_no_severity_member(self) -> None:
        """The documented collector emits no severity field at all, on any record.

        Which is why those fixtures take the no-vocabulary path while this provisioning's
        shape -- whose every query defines a label -- takes the label path.  Both halves of
        AAP 0.5.4's *"joern unless a query defines one"* are live, and which applies is a
        property of the artifact rather than of the adapter.
        """
        for stem, _ in NEGATIVE_FIXTURES:
            with self.subTest(fixture=stem):
                document = load_fixture(stem)
                for index, finding in enumerate(document["findings"]):
                    if isinstance(finding, dict):
                        self.assertNotIn("severity", finding, f"record {index}")
        provisioned = load_fixture("joern")
        self.assertTrue(
            all(
                isinstance(finding, dict) and "severity" in finding
                for finding in provisioned["findings"]
            ),
            "every record of this provisioning's shape states its query's severity",
        )

    def test_the_envelope_produces_no_rows(self) -> None:
        """Envelope members are metadata: they reach counters, never the dataset.

        Asserted by handing the adapter an artifact that carries the whole envelope and an
        **empty** findings array: the envelope figures are surfaced and no row is produced.
        An absent, empty or non-array findings value is not an error, which is how the
        independent traversal reads it too, so the two agree on zero.
        """
        envelope = {
            "tool": TOOL,
            "cpg_path": "harness/cpg/spark.cpg",
            "generated_at": "2026-08-24T22:58:56Z",
            "cpg_methods": 1397339,
            "cpg_typedecls": 119691,
            "source_index_size": 6759,
            "declaration_index_size": 15230,
            "queries": [{"id": DOCUMENTED_RULE_IDS[0], "count": 55}],
            "findings": [],
        }
        adapted = self.adapt_document(envelope)
        self.assertEqual(adapted.rows, [])
        self.assertEqual(adapted.rejections, [])
        self.assertEqual(adapted.raw_records, 0)
        self.assertEqual(adapted.counters["envelope_graph_methods"], 1397339)
        self.assertEqual(adapted.counters["envelope_queries_declared"], 1)
        self.assertEqual(
            adapted.counters["envelope_collector_source_index_size"], 6759
        )
        for shape in ({}, {"findings": None}, {"findings": "not an array"}):
            with self.subTest(document=shape):
                degenerate = self.adapt_document(dict(shape))
                self.assertEqual(degenerate.rows, [])
                self.assertEqual(degenerate.rejections, [])
                self.assertEqual(degenerate.raw_records, 0)


# --------------------------------------------------------------------------------------
# The positive fixture, field by field
# --------------------------------------------------------------------------------------


class PositiveFixtureTest(HermeticRootTestCase):
    """``fixtures/joern.json`` against ``expected/joern.rows.json``, field by field.

    The fixture is captured output rather than a hand-written approximation of it: ten of
    its eleven findings are byte-identical members of the raw artifact's 692, and the
    envelope and the whole ``queries`` array are byte-identical too.  AAP 0.6.2 gives the
    reason -- *"a hand-written fixture tests the adapter against the shape you believed the
    tool emits rather than the shape it emits."*  The eleventh, the ``MasterSuite`` record,
    is derived, because the captured output cannot supply a test-tree finding: the runbook
    excludes every ``-tests`` JAR from the graph input, so no test-tree class reached the
    graph this artifact was queried from.  Nothing is asserted about its call site; the
    record is derived and no finding is judged in either direction.
    """

    def setUp(self) -> None:
        """Adapt the fixture once per test, against this class's hermetic root."""
        self.adapted = self.adapt_fixture("joern")
        self.expected = load_expected("joern")

    def test_row_count_matches_the_expected_file(self) -> None:
        """Eleven records, eleven rows, no rejection."""
        self.assertEqual(len(self.adapted.rows), len(self.expected["rows"]))
        self.assertEqual(self.adapted.rejections, [])
        self.assertEqual(self.expected["counts"]["rows"], 11)
        self.assertEqual(self.expected["counts"]["rejections"], 0)

    def test_every_row_field_by_field_in_emit_field_order(self) -> None:
        """Assertion 11: every field of every row, iterated from ``emit.FIELDS`` in order.

        The field list comes from the authored constant rather than from either row's keys,
        so a failure names the field that differs instead of printing two dictionaries.
        """
        self.assertRowsMatchExpected(
            self.adapted.rows, self.expected["rows"], label="joern"
        )

    def test_rows_pass_the_emitter_s_own_validation(self) -> None:
        """``emit.validate_rows`` accepts them, which is the no-absolute-path check itself.

        The emitter refuses a leading slash, a Windows drive prefix and every URI form, so
        putting the rows through it asserts AAP 0.8.2's *"No absolute path is ever
        emitted"* through the code that enforces it rather than through a second opinion.
        """
        validated = emit.validate_rows(self.adapted.rows)
        self.assertEqual(len(validated), len(self.adapted.rows))
        for index, row in enumerate(validated):
            with self.subTest(row=index):
                self.assertEqual(tuple(row), emit.FIELDS)

    def test_no_emitted_path_is_absolute_or_a_uri(self) -> None:
        """Assertion 13b: no emitted ``path`` is absolute, on any row.

        Asserted directly as well as through ``emit.validate_rows``, since the emitter is a
        dependency of the same run and a shared defect would otherwise pass both times.
        """
        for index, row in enumerate(self.adapted.rows):
            with self.subTest(row=index):
                path = row["path"]
                self.assertIsInstance(path, str)
                self.assertTrue(path)
                self.assertFalse(path.startswith("/"), path)
                self.assertFalse(path.startswith("\\"), path)
                self.assertNotIn("://", path)
                self.assertFalse(Path(path).is_absolute(), path)
                self.assertNotIn(self.environment.root, path)

    def test_cwe_cve_and_package_coordinate_are_absent_on_every_row(self) -> None:
        """Assertion 12b: ``cwe``, ``cve`` and ``package_coordinate`` are ``None`` on every row.

        A Joern finding names a call site, not a weakness entry and not a package.

        The absence of ``package_coordinate`` is explicitly **not** a rejection here:
        ``unformable_package_coordinate`` covers a dependency-oriented record and this
        shape is not one.
        """
        for index, row in enumerate(self.adapted.rows):
            with self.subTest(row=index):
                self.assertIsNone(row["cwe"])
                self.assertIsNone(row["cve"])
                self.assertIsNone(row["package_coordinate"])
        self.assertNotIn(
            paths.REJECT_UNFORMABLE_PACKAGE_COORDINATE, self.adapted.reject_classes
        )

    def test_start_line_is_the_integer_the_finding_reported(self) -> None:
        """Assertion 13a: ``start_line`` is the integer the finding reported, unchanged.

        Taken from the ``line`` member of each record of this provisioning's shape, and
        compared as a sequence so a transposition between rows would fail too.
        """
        reported = [finding["line"] for finding in self.adapted.document["findings"]]
        self.assertEqual([row["start_line"] for row in self.adapted.rows], reported)
        for index, row in enumerate(self.adapted.rows):
            with self.subTest(row=index):
                self.assertIsInstance(row["start_line"], int)
                self.assertNotIsInstance(row["start_line"], bool)
                self.assertGreater(row["start_line"], 0)
        self.assertEqual(self.adapted.counters["start_line_absent"], 0)
        self.assertEqual(self.adapted.counters["start_line_from_line"], 11)
        self.assertEqual(self.adapted.counters["start_line_from_start_line"], 0)

    def test_in_scope_decomposition_sums_to_the_row_count(self) -> None:
        """``rows_in_scope`` plus ``rows_out_of_scope`` is one measurement split, not two."""
        counters = self.adapted.counters
        self.assertEqual(counters["rows_in_scope"], 8)
        self.assertEqual(counters["rows_out_of_scope"], 3)
        self.assertEqual(
            counters["rows_in_scope"] + counters["rows_out_of_scope"],
            len(self.adapted.rows),
        )
        self.assertEqual(
            sum(1 for row in self.adapted.rows if row["in_scope"]),
            counters["rows_in_scope"],
        )

    def test_artifact_determined_counters_match_the_expected_file(self) -> None:
        """Every counter that is a property of the artifact, compared verbatim.

        The five index counters are excluded and asserted separately: their value is a
        property of the tree the index was built over, and every expected file records them
        as measured over the pinned checkout.
        """
        expected_counters = self.expected["counters"]
        self.assertEqual(set(expected_counters), set(joern.COUNTER_KEYS))
        self.assertEqual(set(self.adapted.counters), set(joern.COUNTER_KEYS))
        for key in joern.COUNTER_KEYS:
            if key in ROOT_DEPENDENT_COUNTERS:
                continue
            with self.subTest(counter=key):
                self.assertEqual(
                    self.adapted.counters[key],
                    expected_counters[key],
                    f"counter {key!r} differs from the expected file",
                )

    def test_index_counters_describe_the_index_actually_built(self) -> None:
        """The index counters describe this root, derived from the scaffold spec.

        A rejection count is only interpretable beside the index that produced it, so the
        adapter publishes the index's shape.  The expectation here comes from
        :data:`SCAFFOLD` through the same keying rule ``paths.source_index_key`` documents,
        so two independently produced answers are compared rather than the index with
        itself.
        """
        counters = self.adapted.counters
        filename_keys = self.environment.expected_filename_keys()
        declaration_keys = self.environment.expected_declaration_keys()
        self.assertEqual(counters["source_index_supplied"], 0)
        self.assertEqual(counters["source_index_declarations_read"], 1)
        self.assertEqual(counters["source_index_files_indexed"], len(SCAFFOLD))
        self.assertEqual(counters["source_index_by_filename_keys"], len(filename_keys))
        self.assertEqual(counters["source_index_by_decl_keys"], len(declaration_keys))
        self.assertEqual(
            counters["source_index_ambiguous_by_filename"],
            sum(1 for value in filename_keys.values() if len(set(value)) > 1),
        )
        self.assertEqual(
            counters["source_index_ambiguous_by_decl"],
            sum(1 for value in declaration_keys.values() if len(set(value)) > 1),
        )
        # The scaffold must actually contain ambiguity, or the gap-2 assertions below
        # would be testing a resolver that never met a collision.
        self.assertGreaterEqual(counters["source_index_ambiguous_by_filename"], 2)
        self.assertGreaterEqual(counters["source_index_ambiguous_by_decl"], 2)

    def test_an_injected_index_is_recorded_as_supplied(self) -> None:
        """A caller-supplied index is reported as such, and resolves the same way.

        ``cli.py`` builds the index once for a run and injects it; the counter is what
        makes the difference between a measured index and an injected one visible.
        """
        injected = paths.build_source_index(self.environment.root)
        adapted = self.adapt_fixture("joern", source_index=injected)
        self.assertEqual(adapted.counters["source_index_supplied"], 1)
        self.assertEqual(adapted.paths_emitted, self.adapted.paths_emitted)

    def test_every_path_kind_is_bytecode_source(self) -> None:
        """A resolution from the source index names a real file, so none is non-filesystem."""
        counters = self.adapted.counters
        self.assertEqual(counters["path_kind_bytecode_source"], len(self.adapted.rows))
        self.assertEqual(counters["non_filesystem_paths"], 0)
        for kind in ("tree_file", "outside_root", "archive_member"):
            with self.subTest(kind=kind):
                self.assertEqual(counters[f"path_kind_{kind}"], 0)

    def test_the_two_shared_counters_are_published_and_the_other_two_are_structural(
        self,
    ) -> None:
        """AAP 0.5.4 reports four counters per tool; two are structurally absent here.

        ``multi_location_records`` and ``non_filesystem_paths`` are published.
        ``multi_valued_cwe_records`` and ``multi_valued_cve_records`` are not members of
        this adapter's counter set at all, because ``cwe`` and ``cve`` are ``None`` on every
        row by contract -- the count is structurally zero and manufacturing an identifier
        from a class name or a query identifier would be inference.  Asserted rather than
        assumed, so "structurally absent" is a measured claim rather than an omission.
        """
        self.assertIn("multi_location_records", self.adapted.counters)
        self.assertIn("non_filesystem_paths", self.adapted.counters)
        self.assertNotIn("multi_valued_cwe_records", joern.COUNTER_KEYS)
        self.assertNotIn("multi_valued_cve_records", joern.COUNTER_KEYS)
        reported = self.expected["aap_reported_counters"]
        self.assertTrue(reported["multi_location_records"]["published_by_the_adapter"])
        self.assertTrue(reported["non_filesystem_paths"]["published_by_the_adapter"])
        self.assertFalse(reported["multi_valued_cwe_records"]["published_by_the_adapter"])
        self.assertFalse(reported["multi_valued_cve_records"]["published_by_the_adapter"])
        self.assertEqual(self.adapted.counters["multi_location_records"], 0)

    def test_a_record_carrying_several_locations_is_counted_once(self) -> None:
        """The row takes the first location, the record counts once, the number is reported.

        This shape names one call site per finding, so the counter stays at zero over every
        committed fixture -- which is exactly why it is exercised here on an authored
        document rather than left unasserted.
        """
        adapted = self.adapt_document(
            {
                "tool": TOOL,
                "findings": [
                    {
                        "rule_id": DOCUMENTED_RULE_IDS[0],
                        "message": "a record a future collector might write with two "
                        "locations",
                        "start_line": 276,
                        "class_file": "org/apache/spark/deploy/worker/DriverRunner.class",
                        "locations": [{"index": 0}, {"index": 1}],
                        "path_resolution": paths.BASIS_SOURCE_INDEX_FILENAME,
                    }
                ],
            }
        )
        self.assertEqual(len(adapted.rows), 1)
        self.assertEqual(adapted.raw_records, 1)
        self.assertEqual(adapted.counters["multi_location_records"], 1)

    def test_the_parse_status_for_a_clean_artifact_is_clean(self) -> None:
        """No rejection means ``clean``; ``partial`` is the negative fixtures' status."""
        self.assertEqual(self.adapted.parse_status(), cli.PARSE_STATUS_CLEAN)


# --------------------------------------------------------------------------------------
# Gap 1: resolution against BOTH src/main and src/test
# --------------------------------------------------------------------------------------

#: The row of the positive fixture that resolves into a test tree, and its coordinates.
SRC_TEST_ROW_INDEX = 7
SRC_TEST_ROW_PATH = "core/src/test/scala/org/apache/spark/deploy/master/MasterSuite.scala"
SRC_MAIN_ROW_INDEX = 1
SRC_MAIN_ROW_PATH = "core/src/main/scala/org/apache/spark/rdd/PipedRDD.scala"

#: The two coordinates that resolve on the declaration scheme alone, with the file each
#: declares them in.  No file is named after either class, so a filename-only index loses
#: both silently.
DECLARATION_ONLY_RESOLUTIONS = (
    (
        "org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3",
        "core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala",
    ),
    (
        "org.apache.spark.streaming.ObjectInputStreamWithLoader",
        "streaming/src/main/scala/org/apache/spark/streaming/Checkpoint.scala",
    ),
)


class BothSourceTreesTest(HermeticRootTestCase):
    """Gap 1: the collector indexes ``src/main`` only, and the adapter closes that.

    Every ``-tests`` artifact the build emitted is in the graph input, so a finding can
    legitimately name bytecode compiled from a test tree.  AAP 0.5.4 requires the
    resolution to span both trees and requires a ``src/test`` resolution to be **retained**
    with ``in_scope: false``; AAP 0.9.3 lists that retention among the conditions that are
    recorded and do not stop the run.
    """

    def test_the_index_spans_both_trees(self) -> None:
        """``paths.SOURCE_TREES`` is both trees, and the built index holds a test-tree path."""
        self.assertEqual(paths.SOURCE_TREES, ("main", "test"))
        index = paths.build_source_index(self.environment.root)
        self.assertEqual(index.trees_indexed, ("main", "test"))
        candidates = index.candidates("org/apache/spark/deploy/master/MasterSuite")
        self.assertEqual(candidates, (SRC_TEST_ROW_PATH,))
        statistics = index.statistics()
        self.assertEqual(statistics["files_indexed"], len(SCAFFOLD))
        self.assertTrue(statistics["declarations_read"])

    def test_a_src_main_class_resolves_uniquely_and_is_in_scope(self) -> None:
        """Assertion 1: a unique ``src/main`` resolution is a row with ``in_scope`` true."""
        adapted = self.adapt_fixture("joern")
        row = adapted.rows[SRC_MAIN_ROW_INDEX]
        self.assertEqual(row["path"], SRC_MAIN_ROW_PATH)
        self.assertIs(row["in_scope"], True)
        self.assertEqual(
            paths.matches_any_glob(row["path"], self.environment.globs),
            "core/src/main/**",
        )
        self.assertEqual(adapted.counters["resolution_from_class"], len(adapted.rows))

    def test_a_src_test_resolution_is_retained_with_in_scope_false(self) -> None:
        """Assertion 2: the mandated retention, asserted on the named row.

        A dropped test-tree row and a retained one are indistinguishable in a row count, so
        the row's presence, its path and its ``in_scope`` value are each asserted.
        """
        adapted = self.adapt_fixture("joern")
        row = adapted.rows[SRC_TEST_ROW_INDEX]
        self.assertEqual(row["path"], SRC_TEST_ROW_PATH)
        self.assertIn("src/test", row["path"])
        self.assertIs(row["in_scope"], False)
        self.assertEqual(row["rule_id"], "joern-reflection-forname")
        self.assertEqual(row["start_line"], 218)
        # Retained, and visible as retained: this counter is the only place the number
        # appears, and it is what separates a kept row from a dropped one.
        self.assertEqual(adapted.counters["rows_from_src_test"], 1)
        self.assertEqual(
            sum(1 for candidate in adapted.paths_emitted if "src/test" in candidate), 1
        )

    def test_the_src_test_exclusion_is_the_literal_one_and_it_governs_in_scope(self) -> None:
        """The exclusion is literal, lives in ``paths.py``, and overrides a glob match.

        Delegated rather than re-implemented, and for a second reason:
        ``python/pyspark/**`` holds 832 test modules carrying **no** ``src/test`` segment,
        so all of them are in scope.  A hand-rolled "is this a test?" heuristic would
        wrongly exclude a fifth of the in-scope file count.
        """
        self.assertTrue(paths.contains_src_test(SRC_TEST_ROW_PATH))
        self.assertFalse(
            paths.in_scope(SRC_TEST_ROW_PATH, self.environment.globs),
            "a src/test path is out of scope",
        )
        for module in (
            "python/pyspark/tests/test_appsubmit.py",
            "python/pyspark/sql/tests/test_functions.py",
            "python/pyspark/ml/tests/test_pipeline.py",
        ):
            with self.subTest(module=module):
                self.assertFalse(paths.contains_src_test(module))
                self.assertTrue(
                    paths.in_scope(module, self.environment.globs),
                    "a python/pyspark test module carries no src/test segment and is in "
                    "scope",
                )
        # The allowlist file is the authority and holds exactly the twelve globs.
        self.assertTrue(
            paths.allowlist_matches_authoritative_globs(self.environment.globs)
        )
        self.assertEqual(self.environment.globs, AUTHORITATIVE_GLOBS)
        self.assertNotIn(
            "src/test",
            "".join(self.environment.globs),
            "the exclusion is paths.py's, not a line in the allowlist",
        )

    def test_a_null_collector_path_still_resolves_from_the_class(self) -> None:
        """Assertion 3: gap 1 actually closing, on a record the collector could not place.

        ``findings[].path`` is the collector's own answer and is ``null`` where it had
        none -- correct collector behaviour, not a defect, since it indexes ``src/main``
        only.  The adapter's own resolution spans both trees, so the row carries a resolved
        path rather than inheriting the collector's absence.  This is the assertion that
        distinguishes the adapter from the collector.

        The document is authored here rather than adapted from a fixture: no committed
        fixture carries an explicit ``"path": null`` beside a resolvable coordinate, and
        mutating a fixture to produce one is forbidden.
        """
        document = {
            "tool": TOOL,
            "cpg_path": "harness/cpg/spark.cpg",
            "queries": [
                {"id": DOCUMENTED_RULE_IDS[2], "count": 412},
                {"id": DOCUMENTED_RULE_IDS[3], "count": 23},
            ],
            "findings": [
                {
                    "rule_id": DOCUMENTED_RULE_IDS[2],
                    "message": "reflective class loading (Class.forName)",
                    "path": None,
                    "start_line": 218,
                    "method_full_name": (
                        "org.apache.spark.deploy.master.MasterSuite.$anonfun$new$29:void()"
                    ),
                    "class_file": "org/apache/spark/deploy/master/MasterSuite.class",
                    "path_resolution": paths.COLLECTOR_UNRESOLVED_BYTECODE_ONLY,
                },
                {
                    "rule_id": DOCUMENTED_RULE_IDS[3],
                    "message": "message digest construction (algorithm chosen at this "
                    "call site)",
                    "path": None,
                    "start_line": 72,
                    "method_full_name": "org.apache.spark.storage.DiskStore.put:void()",
                    "class_file": "org/apache/spark/storage/DiskStore.class",
                    "path_resolution": paths.COLLECTOR_UNRESOLVED_BYTECODE_ONLY,
                },
            ],
        }
        adapted = self.adapt_document(document)
        self.assertEqual(adapted.rejections, [])
        self.assertEqual(len(adapted.rows), 2)
        self.assertEqual(
            adapted.paths_emitted,
            (
                SRC_TEST_ROW_PATH,
                "core/src/main/scala/org/apache/spark/storage/DiskStore.scala",
            ),
        )
        # The src/test resolution is retained and out of scope; the src/main one is in.
        self.assertIs(adapted.rows[0]["in_scope"], False)
        self.assertIs(adapted.rows[1]["in_scope"], True)
        # The resolution is this run's own, not the collector's: the collector supplied
        # none, so collector_path_used stays at zero.
        self.assertEqual(adapted.counters["resolution_from_class"], 2)
        self.assertEqual(adapted.counters["collector_path_used"], 0)
        self.assertEqual(adapted.counters["collector_path_corroborated"], 0)
        self.assertEqual(adapted.counters["rows_from_src_test"], 1)
        self.assertEqual(adapted.counters["collector_explanation_present"], 2)

    def test_the_declaration_scheme_is_not_optional(self) -> None:
        """Two coordinates resolve on the declaration scheme alone, and must still resolve.

        ``ProcessBuilderLike`` is declared in ``DriverRunner.scala`` and
        ``ObjectInputStreamWithLoader`` in ``Checkpoint.scala``: no file is named after
        either class, so a filename-only index finds nothing and the row disappears without
        a rejection to show it.
        """
        index = paths.build_source_index(self.environment.root)
        filename_only = paths.build_source_index(
            self.environment.root, read_declarations=False
        )
        for coordinate, expected_path in DECLARATION_ONLY_RESOLUTIONS:
            with self.subTest(coordinate=coordinate):
                key = paths.class_key(coordinate)
                self.assertEqual(index.candidates(key), (expected_path,))
                self.assertEqual(
                    index.basis_for(key), paths.BASIS_SOURCE_INDEX_DECLARATION
                )
                self.assertEqual(
                    filename_only.candidates(key),
                    (),
                    "a filename-only index must be shown to lose this resolution, or the "
                    "declaration scheme is not what is being tested",
                )
        adapted = self.adapt_fixture("joern")
        self.assertIn(DECLARATION_ONLY_RESOLUTIONS[0][1], adapted.paths_emitted)
        self.assertIn(DECLARATION_ONLY_RESOLUTIONS[1][1], adapted.paths_emitted)

    def test_the_class_key_reduction_collapses_companion_anonymous_and_nested_names(
        self,
    ) -> None:
        """A key built from a coordinate matches one built from a path, and both truncate at ``$``.

        The same reduction the collector performs, which is what lets a key built here match
        one built there -- and the reason ``ProcessBuilderLike$$anon$3`` and
        ``UdfPacket$`` resolve at all.
        """
        cases = (
            ("org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3",
             "org/apache/spark/deploy/worker/ProcessBuilderLike"),
            ("org/apache/spark/deploy/worker/ProcessBuilderLike$$anon$3.class",
             "org/apache/spark/deploy/worker/ProcessBuilderLike"),
            ("org.apache.spark.sql.connect.common.UdfPacket$",
             "org/apache/spark/sql/connect/common/UdfPacket"),
            ("org/apache/spark/deploy/master/Master.class",
             "org/apache/spark/deploy/master/Master"),
            ("/tmp/jimple2cpg-13348921788793719165/org/apache/spark/rdd/PipedRDD.class",
             "org/apache/spark/rdd/PipedRDD"),
        )
        for coordinate, expected_key in cases:
            with self.subTest(coordinate=coordinate):
                self.assertEqual(paths.class_key(coordinate), expected_key)


# --------------------------------------------------------------------------------------
# Gap 2: a resolution is taken only where it is unique
# --------------------------------------------------------------------------------------

#: The class keys the ambiguity fixture reaches, in fixture record order, each claimed by
#: two distinct source files in the scaffold exactly as it is at the pin.
AMBIGUOUS_CLASS_KEYS = (
    "org/apache/spark/sql/catalyst/expressions/rows",
    "org/apache/spark/SparkContext",
    "org/apache/spark/SparkContextSuite",
)


class UniqueResolutionOnlyTest(HermeticRootTestCase):
    """Gap 2: the collector resolves ambiguity first-writer-wins; the adapter refuses to.

    AAP 0.5.4 requires the resolution be taken *"only where it is unique"* and the ambiguous
    and the unresolvable to be rejected.  Closing gap 1 *increases* ambiguity -- collision 3
    below exists only because the index spans ``src/test`` -- which is why closing gap 2 is
    mandatory rather than optional.
    """

    def setUp(self) -> None:
        """Adapt the ambiguity fixture once per test."""
        self.adapted = self.adapt_fixture("reject-joern-ambiguous-path")
        self.expected = load_expected("reject-joern-ambiguous-path")

    def test_each_collision_is_genuine_in_this_root(self) -> None:
        """Two distinct claimants per key, produced by the index rather than arranged.

        The rejection has to come from the resolver meeting a real collision.  A test that
        asserted the rejection without establishing the collision would pass against a
        resolver that rejected everything.
        """
        index = paths.build_source_index(self.environment.root)
        for key in AMBIGUOUS_CLASS_KEYS:
            with self.subTest(key=key):
                candidates = index.candidates(key)
                self.assertEqual(
                    len(candidates), 2, f"{key!r} must have exactly two claimants"
                )
                self.assertEqual(len(set(candidates)), 2, "the two must be distinct paths")
        # Collision 2 is the load-bearing one: unique by filename, two claimants only once
        # declarations are read.  An implementation that merely ordered its lookups would
        # resolve it silently to core, and only the union reading sees two.
        filename_only = paths.build_source_index(
            self.environment.root, read_declarations=False
        )
        self.assertEqual(
            len(filename_only.candidates("org/apache/spark/SparkContext")),
            1,
            "collision 2 must be unique under the filename scheme alone",
        )

    def test_an_ambiguous_resolution_is_a_counted_rejection_by_name(self) -> None:
        """Assertion 4: three rejections, each under the named class, and no row for them."""
        expected_rejections = self.expected["rejections"]
        self.assertEqual(len(self.adapted.rejections), len(expected_rejections))
        self.assertEqual(
            self.adapted.rejections_by_class(),
            {paths.REJECT_AMBIGUOUS_SOURCE_RESOLUTION: 3},
        )
        for index, (produced, expected) in enumerate(
            zip(self.adapted.rejections, expected_rejections)
        ):
            with self.subTest(rejection=index):
                # The class is asserted against the module that owns the vocabulary, not
                # against a bare string: a test that only counts rejections cannot tell one
                # condition from another.
                self.assertEqual(
                    produced.reject_class, paths.REJECT_AMBIGUOUS_SOURCE_RESOLUTION
                )
                self.assertEqual(produced.reject_class, expected["reject_class"])
                self.assertIn(produced.reject_class, paths.REJECT_CLASSES)
                self.assertEqual(produced.tool, TOOL)
                self.assertEqual(
                    produced.record_identity["finding_index"], expected["finding_index"]
                )
                self.assertEqual(
                    produced.record_identity["class_key"], AMBIGUOUS_CLASS_KEYS[index]
                )
                # The detail names the collision rather than the count: a reader who cannot
                # see which files claimed the key cannot check the rejection.
                self.assertIn(AMBIGUOUS_CLASS_KEYS[index], produced.detail)
                self.assertIn("2 distinct source files", produced.detail)
                for candidate in paths.build_source_index(
                    self.environment.root
                ).candidates(AMBIGUOUS_CLASS_KEYS[index]):
                    self.assertIn(candidate, produced.detail)

    def test_no_row_carries_a_colliding_candidate(self) -> None:
        """Assertion 6: the first-wins answer must not survive anywhere in the rows.

        A buggy adapter could reject a record *and* emit a row for it -- rejecting on the
        ambiguity while a second path emitted the collector's answer -- and the class, the
        count and the identity would all still look right.  So the candidate paths are
        compared against every field of every row, not only against ``path``.
        """
        forbidden = tuple(
            self.expected["no_row_carries_a_colliding_candidate"][
                "candidate_paths_that_must_not_appear"
            ]
        )
        self.assertEqual(len(forbidden), 6)
        index = paths.build_source_index(self.environment.root)
        for key in AMBIGUOUS_CLASS_KEYS:
            for candidate in index.candidates(key):
                self.assertIn(
                    candidate,
                    forbidden,
                    "every claimant of a colliding key must be in the forbidden set",
                )
        for row_index, row in enumerate(self.adapted.rows):
            for candidate in forbidden:
                with self.subTest(row=row_index, candidate=candidate):
                    self.assertNotEqual(row["path"], candidate)
                    for field in emit.FIELDS:
                        value = row[field]
                        if isinstance(value, str):
                            self.assertNotIn(candidate, value)
        # The collector's own first-wins answer for record 0 is in the fixture, and it is
        # the sql/api candidate.  Its presence there is what makes the refusal meaningful.
        first_wins = self.adapted.document["findings"][0]["path"]
        self.assertIn(first_wins, forbidden)
        self.assertNotIn(first_wins, self.adapted.paths_emitted)

    def test_the_rejected_records_contribute_to_no_per_row_counter(self) -> None:
        """No half-emission: a rejected record moves no row-scoped counter.

        An implementation that rejected a record after counting it would show as an
        asymmetry here rather than in the row count.
        """
        counters = self.adapted.counters
        rows = len(self.adapted.rows)
        self.assertEqual(counters["path_kind_bytecode_source"], rows)
        self.assertEqual(counters["severity_absent"], rows)
        self.assertEqual(counters["resolution_from_class"], rows)
        self.assertEqual(counters["rows_in_scope"] + counters["rows_out_of_scope"], rows)
        self.assertEqual(counters["start_line_from_start_line"], rows)
        # The rejected records were still *read*: their rule identifiers and coordinate
        # members are counted, which is how the traversal proves it walked all six.
        self.assertEqual(counters["rule_id_from_rule_id"], self.adapted.raw_records)
        self.assertEqual(
            counters["coordinate_from_class_file"], self.adapted.raw_records
        )
        self.assertEqual(counters["coordinate_from_class"], 0)

    def test_the_ambiguity_survives_a_collector_supplied_path(self) -> None:
        """A collector path never breaks the tie -- that would reinstate the silent guess.

        Authored here because it is the one shape a fixture cannot force: the fixture's own
        record 0 already carries a collector path, and this asserts the same refusal where
        that path is one this run could itself have resolved.
        """
        adapted = self.adapt_document(
            {
                "tool": TOOL,
                "findings": [
                    {
                        "rule_id": DOCUMENTED_RULE_IDS[2],
                        "message": "reflective class loading (Class.forName)",
                        "path": "core/src/main/scala/org/apache/spark/SparkContext.scala",
                        "start_line": 19,
                        "class_file": "org/apache/spark/SparkContext.class",
                        "path_resolution": paths.BASIS_SOURCE_INDEX_FILENAME,
                    }
                ],
            }
        )
        self.assertEqual(adapted.rows, [])
        self.assertEqual(len(adapted.rejections), 1)
        self.assertEqual(
            adapted.rejections[0].reject_class,
            paths.REJECT_AMBIGUOUS_SOURCE_RESOLUTION,
        )
        self.assertEqual(adapted.counters["collector_path_used"], 0)

    def test_an_unresolvable_class_is_a_counted_rejection(self) -> None:
        """Assertion 5: nothing on disk claims the key, so the record is rejected.

        The ordinary outcome for a third-party class shaded into Spark's JARs.  It is a
        counted rejection rather than a row with an invented path, and rather than a row
        with a null one.
        """
        adapted = self.adapt_fixture("reject-joern-unresolvable-path")
        expected = load_expected("reject-joern-unresolvable-path")
        index = paths.build_source_index(self.environment.root)
        self.assertEqual(
            index.candidates(UNRESOLVABLE_CLASS_KEY),
            (),
            "the key must genuinely resolve to nothing in this root",
        )
        self.assertEqual(len(adapted.rejections), 1)
        rejection = adapted.rejections[0]
        self.assertEqual(rejection.reject_class, paths.REJECT_UNRESOLVABLE_PATH)
        self.assertEqual(
            rejection.reject_class, expected["rejections"][0]["reject_class"]
        )
        self.assertIn(rejection.reject_class, paths.REJECT_CLASSES)
        self.assertEqual(rejection.record_identity["class_key"], UNRESOLVABLE_CLASS_KEY)
        self.assertIn("src/main", rejection.detail)
        self.assertIn("src/test", rejection.detail)
        self.assertIn("MessageDigestHashFunction", rejection.detail)
        self.assertNotIn(
            UNRESOLVABLE_CLASS_KEY,
            adapted.paths_emitted,
            "an unresolvable coordinate must not be emitted as though it were a path",
        )

    def test_an_absent_or_sentinel_coordinate_is_a_counted_rejection(self) -> None:
        """``absent_path`` is reachable and asserted, though no committed fixture carries it.

        Three routes: no coordinate member at all, a blank one, and the collector's
        ``<unknown>`` sentinel -- written where a method has no enclosing type declaration,
        so it names the *absence* of a coordinate rather than a class to look up.  Keying an
        index on the literal would turn a stated absence into an ordinary lookup miss and
        lose why the record failed, which is why the sentinel earns its own route.

        **Recorded divergence, asserted as observed rather than repaired.**  The sentinel
        route retains the collector's ``path_resolution`` in the rejection detail; the
        absent and blank routes do not, because
        ``normalize.paths.resolve_bytecode_class``'s earliest branch builds its
        :class:`normalize.paths.Rejection` without composing the explanation in.  AAP 0.5.4
        has the explanation *retained in the rejection record* for an unmappable bytecode
        path, so this is a narrow gap in that branch rather than a fault in this adapter.
        Nothing here edits ``harness/lib/normalize/``: the behaviour is asserted as it
        stands and the divergence is stated.  What matters for the confinement requirement
        holds on every route -- the explanation reaches no dataset field, because no row is
        produced at all.
        """
        routes = (
            ("absent", None, False),
            ("blank", "   ", False),
            ("sentinel", joern.COLLECTOR_UNKNOWN_CLASS, True),
        )
        for label, coordinate, explanation_retained in routes:
            with self.subTest(coordinate=label):
                finding = {
                    "rule_id": DOCUMENTED_RULE_IDS[0],
                    "message": "external process launch reachable in bytecode",
                    "start_line": 276,
                    "path_resolution": paths.COLLECTOR_UNRESOLVED_BYTECODE_ONLY,
                }
                if coordinate is not None:
                    finding["class_file"] = coordinate
                adapted = self.adapt_document({"tool": TOOL, "findings": [finding]})
                self.assertEqual(adapted.rows, [])
                self.assertEqual(len(adapted.rejections), 1)
                rejection = adapted.rejections[0]
                self.assertEqual(rejection.reject_class, paths.REJECT_ABSENT_PATH)
                self.assertIn(rejection.reject_class, paths.REJECT_CLASSES)
                self.assertEqual(rejection.tool, TOOL)
                # Every route names the ignored ephemeral member, which is what makes the
                # rejection diagnosable: no coordinate remains once `file` is excluded.
                self.assertIn("file", rejection.detail)
                if explanation_retained:
                    self.assertIn(
                        paths.COLLECTOR_UNRESOLVED_BYTECODE_ONLY, rejection.detail
                    )
                    self.assertIn(joern.COLLECTOR_UNKNOWN_CLASS, rejection.detail)
                else:
                    self.assertNotIn(
                        paths.COLLECTOR_UNRESOLVED_BYTECODE_ONLY, rejection.detail
                    )

    def test_path_is_never_absent_from_a_row(self) -> None:
        """Assertion 7: absence is permitted for five fields, and ``path`` is not one.

        A record whose coordinate cannot be resolved is rejected and counted, never emitted
        with a null path.  Swept over every committed fixture rather than over one.
        """
        self.assertEqual(
            emit.OPTIONAL_FIELDS,
            frozenset(
                {"severity_native", "start_line", "cwe", "cve", "package_coordinate"}
            ),
        )
        for stem in ALL_FIXTURE_STEMS:
            adapted = self.adapt_fixture(stem)
            for index, row in enumerate(adapted.rows):
                with self.subTest(fixture=stem, row=index):
                    self.assertIsInstance(row["path"], str)
                    self.assertTrue(row["path"].strip())
                    self.assertIsNotNone(row["severity_norm"])
                    self.assertIn(row["severity_norm"], severity.SEVERITY_NORM)


# --------------------------------------------------------------------------------------
# The collector's explanation, and every other member that reaches no dataset field
# --------------------------------------------------------------------------------------


class CollectorExplanationConfinementTest(HermeticRootTestCase):
    """``path_resolution`` is retained in a rejection and reaches no dataset field.

    AAP 0.5.4: *"any collector explanation for an unmappable bytecode path is retained in
    the rejection record, not in a dataset field."*  The twelve-field schema makes that
    structural, and this asserts it: an over-helpful implementation would smuggle the
    explanation into ``message``, which is the one field where it would look plausible.
    """

    def test_the_rejection_retains_the_collector_explanation(self) -> None:
        """Assertion 8a: the explanation is in the rejection the resolver built.

        Asserted on the unresolvable and the ambiguous fixtures, which are the two whose
        rejections come from ``paths.resolve_bytecode_class`` with the explanation composed
        in, and against the value the offending record itself carries rather than against a
        literal chosen here.
        """
        for stem in ("reject-joern-unresolvable-path", "reject-joern-ambiguous-path"):
            adapted = self.adapt_fixture(stem)
            self.assertTrue(adapted.rejections)
            for rejection in adapted.rejections:
                finding_index = rejection.record_identity["finding_index"]
                record = adapted.document["findings"][finding_index]
                explanation = record["path_resolution"]
                with self.subTest(fixture=stem, finding=finding_index):
                    self.assertIn(explanation, PATH_RESOLUTION_LITERALS)
                    self.assertIn(
                        f"collector path_resolution: {explanation}",
                        rejection.detail,
                        "the collector's own explanation must be retained verbatim in the "
                        "rejection detail",
                    )
                    # And it is retained there rather than as an identity key, so a reader
                    # of tool-status.md finds it in the sub-reason it explains.
                    self.assertNotIn("path_resolution", rejection.record_identity)

    def test_no_row_field_carries_a_path_resolution_literal(self) -> None:
        """Assertion 8b: swept over all twelve fields of every row of every fixture."""
        for stem in ALL_FIXTURE_STEMS:
            adapted = self.adapt_fixture(stem)
            for row_index, row in enumerate(adapted.rows):
                for field in emit.FIELDS:
                    value = row[field]
                    if not isinstance(value, str):
                        continue
                    for literal in PATH_RESOLUTION_LITERALS:
                        with self.subTest(fixture=stem, row=row_index, field=field):
                            self.assertNotEqual(value, literal)
                            self.assertNotIn(literal, value)
                    with self.subTest(fixture=stem, row=row_index, field=field):
                        self.assertNotIn("path_resolution", value)

    def test_all_three_path_resolution_literals_are_exercised(self) -> None:
        """Assertion 9: the vocabulary is covered by the fixtures rather than assumed."""
        observed: set[str] = set()
        for stem in ALL_FIXTURE_STEMS:
            document = load_fixture(stem)
            for finding in document["findings"]:
                if isinstance(finding, dict) and "path_resolution" in finding:
                    observed.add(finding["path_resolution"])
        self.assertEqual(observed, set(PATH_RESOLUTION_LITERALS))

    def test_the_context_members_reach_no_row_field(self) -> None:
        """``method_full_name``, ``method``, ``callee`` and ``file`` are diagnostics only.

        Each is carried into a rejection's identity so the rejection is diagnosable, and
        into no row: the schema is exactly twelve fields.  Compared by substring against
        every field of every row -- ``message`` included -- rather than by inspection.
        """
        self.assertEqual(joern.CONTEXT_FIELDS, CONTEXT_MEMBERS)
        for stem in ALL_FIXTURE_STEMS:
            adapted = self.adapt_fixture(stem)
            values = {
                finding[member]
                for finding in adapted.document["findings"]
                if isinstance(finding, dict)
                for member in CONTEXT_MEMBERS
                if isinstance(finding.get(member), str)
            }
            self.assertTrue(values, f"{stem}: the fixture must carry context members")
            for row_index, row in enumerate(adapted.rows):
                for field in emit.FIELDS:
                    value = row[field]
                    if not isinstance(value, str):
                        continue
                    for context_value in values:
                        with self.subTest(fixture=stem, row=row_index, field=field):
                            self.assertNotIn(context_value, value)

    def test_the_ephemeral_file_member_is_never_read_as_a_path(self) -> None:
        """The frontend's extraction path is excluded by the metadata, not merely ignored.

        The runner metadata names ``file`` as ``record_path_field_to_ignore`` because it is
        by construction a ``/tmp/jimple2cpg-<id>/...`` extraction path for a ``.class``
        member.  Reading it as a coordinate would relativize a temporary directory into a
        plausible-looking wrong answer for every row.
        """
        self.assertEqual(self.environment.tool_base.record_path_field, "class")
        self.assertEqual(self.environment.tool_base.record_path_field_to_ignore, "file")
        self.assertEqual(
            self.environment.tool_base.kind, paths.PATH_BASE_KIND_BYTECODE_CLASS
        )
        self.assertFalse(
            self.environment.tool_base.has_explicit_base,
            "no filesystem base exists for a bytecode class, so no fallback base may be "
            "recorded for this tool",
        )
        adapted = self.adapt_fixture("joern")
        for row_index, row in enumerate(adapted.rows):
            with self.subTest(row=row_index):
                self.assertNotIn("jimple2cpg", row["path"])
                self.assertNotIn("/tmp/", row["path"])
                self.assertFalse(row["path"].endswith(".class"))
        # A record whose only coordinate is the ignored member resolves nothing and is
        # rejected, rather than resolving that member's /tmp path.
        rejected = self.adapt_document(
            {
                "tool": TOOL,
                "findings": [
                    {
                        "query_id": PROVISIONED_RULE_IDS[0],
                        "message": "external process launch reachable in bytecode",
                        "line": 115,
                        "file": "/tmp/jimple2cpg-13348921788793719165/org/apache/spark/"
                        "rdd/PipedRDD.class",
                    }
                ],
            }
        )
        self.assertEqual(rejected.rows, [])
        self.assertEqual(len(rejected.rejections), 1)
        self.assertEqual(
            rejected.rejections[0].reject_class, paths.REJECT_ABSENT_PATH
        )


# --------------------------------------------------------------------------------------
# Severity: both halves of "joern unless a query defines one"
# --------------------------------------------------------------------------------------


class SeverityPolicyTest(HermeticRootTestCase):
    """AAP 0.5.4 places joern under *"No vocabulary at all ... unless a query defines one"*.

    Both halves are live and which applies is a property of the artifact, not of the
    adapter.  The documented collector emits no severity member, so its records take the
    no-vocabulary path; this provisioning's six baked queries each declare a label, so its
    records take the label path.  Nothing is synthesised in either direction: ``Info`` is
    never written directly and a native label is never manufactured from a rule identifier.
    """

    def test_the_documented_shape_takes_the_no_vocabulary_path(self) -> None:
        """Assertion 10: ``severity_native`` ``None``, ``severity_norm`` ``Info``, and the
        basis *states* the absence rather than a level being assumed."""
        for stem, _ in NEGATIVE_FIXTURES:
            adapted = self.adapt_fixture(stem)
            expected = load_expected(stem)
            with self.subTest(fixture=stem):
                for index, row in enumerate(adapted.rows):
                    self.assertIsNone(row["severity_native"], f"row {index}")
                    self.assertEqual(row["severity_norm"], severity.INFO, f"row {index}")
                    self.assertEqual(row["severity_norm"], "Info")
                self.assertEqual(
                    adapted.counters["severity_basis_no_vocabulary"], len(adapted.rows)
                )
                self.assertEqual(adapted.counters["severity_absent"], len(adapted.rows))
                self.assertEqual(adapted.counters["severity_from_record_label"], 0)
                self.assertEqual(
                    adapted.counters["severity_basis_label"],
                    expected["counters"]["severity_basis_label"],
                )

    def test_the_no_vocabulary_basis_is_the_named_constant(self) -> None:
        """The basis is ``severity.BASIS_NO_VOCABULARY``, asserted through the module.

        Taken from ``severity.py``'s own resolver on the same input the adapter hands it --
        an absent label -- so the basis is asserted against the policy that owns it rather
        than against a string spelled here.
        """
        result = severity.resolve(label=None)
        self.assertEqual(result.basis, severity.BASIS_NO_VOCABULARY)
        self.assertEqual(result.basis, "no_vocabulary")
        self.assertIsNone(result.severity_native)
        self.assertEqual(result.severity_norm, severity.INFO)
        self.assertIsNone(result.selected_entry)
        self.assertIsNone(result.unmapped_literal)
        self.assertIn(result.basis, severity.BASIS_VALUES)

    def test_the_tally_records_the_absence_rather_than_a_fabricated_literal(self) -> None:
        """No ``"INFO"`` literal is invented for a tool whose records state no severity.

        ``severity-map.md`` lists every observed literal with the rows it affected, and a
        fabricated one would put a literal in that document against rows the dataset does
        not contain.  The absence is recorded as an absence: one entry whose
        ``severity_native`` is ``None``.
        """
        adapted = self.adapt_fixture("reject-joern-non-integer-start-line")
        entries = adapted.tally.entries(TOOL)
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertIsNone(entry.severity_native)
        self.assertEqual(entry.severity_norm, severity.INFO)
        self.assertEqual(entry.basis, severity.BASIS_NO_VOCABULARY)
        self.assertFalse(entry.unmapped)
        self.assertEqual(entry.rows, len(adapted.rows))
        for candidate in entries:
            self.assertNotEqual(candidate.severity_native, "INFO")
            self.assertNotEqual(candidate.severity_native, "Info")
        # The tally is fed once per emitted row, never per record: a rejected record
        # contributes no row and so no literal.
        self.assertEqual(adapted.tally.row_count(TOOL), len(adapted.rows))
        self.assertEqual(adapted.tally.total_rows(), len(adapted.rows))

    def test_the_provisioning_shape_maps_the_label_its_query_defined(self) -> None:
        """A query that defines a severity supplies a label, mapped by ``severity.py``.

        The label governs whenever it is in the mapped vocabulary and no score is consulted,
        which is the label-over-score precedence ``severity-map.md`` states.  The mapping is
        asserted against ``severity.resolve`` on the same literal, so this test cannot
        disagree with the policy document.
        """
        adapted = self.adapt_fixture("joern")
        for index, (row, finding) in enumerate(
            zip(adapted.rows, adapted.document["findings"])
        ):
            with self.subTest(row=index):
                literal = finding["severity"]
                self.assertEqual(row["severity_native"], literal)
                expected = severity.resolve(label=literal)
                self.assertEqual(row["severity_norm"], expected.severity_norm)
                self.assertEqual(expected.basis, severity.BASIS_LABEL)
        self.assertEqual(
            adapted.counters["severity_from_record_label"], len(adapted.rows)
        )
        self.assertEqual(adapted.counters["severity_absent"], 0)
        bands = adapted.tally.band_counts(TOOL)
        self.assertEqual(bands["High"], 5)
        self.assertEqual(bands["Medium"], 6)
        self.assertEqual(bands["Info"], 0)

    def test_severity_norm_is_never_absent_on_any_row_of_any_fixture(self) -> None:
        """One of the two never-absent fields, the other being ``path``."""
        for stem in ALL_FIXTURE_STEMS:
            adapted = self.adapt_fixture(stem)
            for index, row in enumerate(adapted.rows):
                with self.subTest(fixture=stem, row=index):
                    self.assertIn(row["severity_norm"], severity.SEVERITY_NORM)
                    self.assertIsNotNone(row["severity_norm"])


# --------------------------------------------------------------------------------------
# One negative fixture per rejection condition this adapter can produce
# --------------------------------------------------------------------------------------


class NegativeFixtureTest(HermeticRootTestCase):
    """The six rejection conditions, each asserted by class name rather than by count.

    AAP 0.6.2 requires a negative fixture per condition each exercised adapter can produce,
    *"present whether or not this run's own artifacts contained the case"* -- a rejection
    path with no test is a rejection path nobody has exercised.  Every fixture also carries
    well-formed neighbours, so each one asserts the second half of partial parse: the
    adapter did not abandon the artifact, and every parsable record still became a row.
    """

    def test_each_negative_fixture_produces_exactly_its_expected_rejection(self) -> None:
        """Class, count, locator, identity and detail, per fixture, against its expected file."""
        for stem, expected_class in NEGATIVE_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                expected = load_expected(stem)
                expected_rejections = expected["rejections"]

                self.assertEqual(len(adapted.rejections), len(expected_rejections))
                self.assertEqual(
                    adapted.rejections_by_class(),
                    expected["counts"]["rejections_by_class"],
                )
                self.assertIn(expected_class, paths.REJECT_CLASSES)
                self.assertIn(
                    expected_class,
                    paths.REJECT_CLASS_DESCRIPTIONS,
                    "every asserted class must carry a description for tool-status.md",
                )

                for index, (produced, record) in enumerate(
                    zip(adapted.rejections, expected_rejections)
                ):
                    with self.subTest(rejection=index):
                        self.assertEqual(produced.reject_class, expected_class)
                        self.assertEqual(
                            produced.reject_class, record["reject_class"]
                        )
                        self.assertEqual(produced.tool, TOOL)
                        self.assertEqual(
                            produced.record_identity["finding_index"],
                            record["finding_index"],
                        )
                        # The identity is the locator, and its *shape* is part of the
                        # contract: a path rejection carries the resolver's three extra
                        # keys, and a rejection raised before step 4 does not.
                        self.assertEqual(
                            sorted(produced.record_identity),
                            sorted(record["expected_record_identity_keys"]),
                        )
                        self.assertEqual(
                            dict(produced.record_identity),
                            record["expected_record_identity"],
                        )
                        # The detail is the sub-reason AAP 0.5.4 requires, and it is
                        # retained verbatim rather than reworded.
                        self.assertEqual(produced.detail, record["expected_detail"])
                        self.assertTrue(produced.detail.strip())

    def test_each_negative_fixture_emits_its_well_formed_neighbours(self) -> None:
        """Partial parse: every parsable record becomes a row, field for field."""
        for stem, _ in NEGATIVE_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                expected = load_expected(stem)
                self.assertRowsMatchExpected(
                    adapted.rows, expected["rows"], label=stem
                )
                validated = emit.validate_rows(adapted.rows)
                self.assertEqual(len(validated), len(adapted.rows))
                self.assertEqual(adapted.parse_status(), cli.PARSE_STATUS_PARTIAL)
                self.assertEqual(
                    expected["counts"]["parse_status"], cli.PARSE_STATUS_PARTIAL
                )

    def test_the_offending_record_produces_no_row(self) -> None:
        """The rejected record contributes a rejection and nothing else.

        Asserted through the identity and through the record indices: the indices the rows
        account for and the indices the rejections account for are disjoint and together
        exhaust ``range(raw)``.  A record that both rejected and emitted would break the
        second half while the first still held.
        """
        for stem, _ in NEGATIVE_FIXTURES:
            with self.subTest(fixture=stem):
                adapted = self.adapt_fixture(stem)
                rejected_indices = {
                    rejection.record_identity["finding_index"]
                    for rejection in adapted.rejections
                }
                self.assertEqual(len(rejected_indices), len(adapted.rejections))
                self.assertEqual(
                    len(adapted.rows) + len(rejected_indices), adapted.raw_records
                )
                self.assertTrue(
                    rejected_indices.issubset(range(adapted.raw_records))
                )

    def test_the_artifact_determined_counters_match_each_expected_file(self) -> None:
        """Every counter that is a property of the artifact, per negative fixture."""
        for stem, _ in NEGATIVE_FIXTURES:
            adapted = self.adapt_fixture(stem)
            expected_counters = load_expected(stem)["counters"]
            self.assertEqual(set(expected_counters), set(joern.COUNTER_KEYS))
            for key in joern.COUNTER_KEYS:
                if key in ROOT_DEPENDENT_COUNTERS:
                    continue
                with self.subTest(fixture=stem, counter=key):
                    self.assertEqual(adapted.counters[key], expected_counters[key])

    def test_every_condition_this_adapter_can_produce_has_a_fixture(self) -> None:
        """The six conditions are covered, and the classes it cannot produce are named.

        ``invalid_uri`` needs a URI and a SARIF base map, of which this shape has neither;
        ``unformable_package_coordinate`` covers a dependency-oriented record, which this is
        not; ``unattributable_section`` needs finding sections, and ``scanner_class`` is
        fixed for the whole artifact here.  Each is asserted absent from every fixture's
        rejections, so "cannot happen" is measured rather than asserted in prose alone.
        """
        covered = {expected_class for _, expected_class in NEGATIVE_FIXTURES}
        self.assertEqual(len(covered), len(NEGATIVE_FIXTURES))
        self.assertEqual(
            covered,
            {
                paths.REJECT_UNRESOLVABLE_PATH,
                paths.REJECT_AMBIGUOUS_SOURCE_RESOLUTION,
                paths.REJECT_MISSING_RULE_ID,
                paths.REJECT_MISSING_MESSAGE,
                paths.REJECT_NON_INTEGER_START_LINE,
                paths.REJECT_MALFORMED_RECORD,
            },
        )
        # absent_path has no committed fixture and is asserted on authored documents in
        # UniqueResolutionOnlyTest, so it is reachable-and-covered rather than unreachable.
        reachable = covered | {paths.REJECT_ABSENT_PATH}
        self.assertEqual(
            set(paths.REJECT_CLASSES) - reachable, set(UNREACHABLE_REJECT_CLASSES)
        )
        for unreachable, reason in UNREACHABLE_REJECT_CLASSES.items():
            with self.subTest(unreachable=unreachable):
                self.assertTrue(reason.strip(), "each must be named with its reason")
                for stem in ALL_FIXTURE_STEMS:
                    adapted = self.adapt_fixture(stem)
                    self.assertNotIn(unreachable, adapted.reject_classes)

    def test_a_non_string_message_is_malformed_rather_than_missing(self) -> None:
        """The two message faults are different classes, and a reader acts differently on each.

        "This collector omitted the text" and "this artifact is not shaped as expected" are
        not the same condition.  The committed fixture covers the empty-message route; the
        non-string route is authored here because no fixture carries one.
        """
        base = {
            "rule_id": DOCUMENTED_RULE_IDS[4],
            "path": "core/src/main/scala/org/apache/spark/deploy/master/Master.scala",
            "start_line": 417,
            "class_file": "org/apache/spark/deploy/master/Master.class",
            "path_resolution": paths.BASIS_SOURCE_INDEX_FILENAME,
        }
        for label, message, expected_class in (
            ("absent", None, paths.REJECT_MISSING_MESSAGE),
            ("null", None, paths.REJECT_MISSING_MESSAGE),
            ("blank", "   ", paths.REJECT_MISSING_MESSAGE),
            ("non-string", {"text": "an object where a string belongs"},
             paths.REJECT_MALFORMED_RECORD),
        ):
            with self.subTest(message=label):
                finding = dict(base)
                if label != "absent":
                    finding["message"] = message
                adapted = self.adapt_document({"tool": TOOL, "findings": [finding]})
                self.assertEqual(adapted.rows, [])
                self.assertEqual(len(adapted.rejections), 1)
                self.assertEqual(
                    adapted.rejections[0].reject_class, expected_class
                )

    def test_a_start_line_below_one_and_a_boolean_are_the_same_class(self) -> None:
        """``non_integer_start_line`` covers three routes, each named in the detail.

        A non-integer type, ``True``/``False`` -- which Python's numeric tower would
        otherwise admit as ``1`` and ``0`` -- and a value below one, since a line is
        numbered from one.  The committed fixture covers the string route; the other two are
        authored here.  An **absent** or explicitly null ``start_line`` is not this
        condition: absence is permitted for that field.
        """
        base = {
            "rule_id": DOCUMENTED_RULE_IDS[4],
            "message": "rpc handler reaches driver launch in bytecode",
            "path": "core/src/main/scala/org/apache/spark/deploy/master/Master.scala",
            "class_file": "org/apache/spark/deploy/master/Master.class",
            "path_resolution": paths.BASIS_SOURCE_INDEX_FILENAME,
        }
        for label, value in (("boolean", True), ("zero", 0), ("negative", -1)):
            with self.subTest(start_line=label):
                adapted = self.adapt_document(
                    {"tool": TOOL, "findings": [dict(base, start_line=value)]}
                )
                self.assertEqual(adapted.rows, [])
                self.assertEqual(len(adapted.rejections), 1)
                self.assertEqual(
                    adapted.rejections[0].reject_class,
                    paths.REJECT_NON_INTEGER_START_LINE,
                )
        for label, finding in (
            ("absent", dict(base)),
            ("null", dict(base, start_line=None)),
        ):
            with self.subTest(start_line=label):
                adapted = self.adapt_document({"tool": TOOL, "findings": [finding]})
                self.assertEqual(adapted.rejections, [])
                self.assertEqual(len(adapted.rows), 1)
                self.assertIsNone(adapted.rows[0]["start_line"])
                self.assertEqual(adapted.counters["start_line_absent"], 1)


# --------------------------------------------------------------------------------------
# Caller faults: raised, never absorbed into a rejection count
# --------------------------------------------------------------------------------------


class CallerContractTest(HermeticRootTestCase):
    """A defective *call* stops the caller; a defective *record* is counted and carried on.

    The distinction matters most for a non-object document: returning zero rows for one
    would produce an empty result set, and an empty result set is indistinguishable from a
    clean scan -- the failure mode the mandated shape-routing negative test exists to
    prevent.
    """

    def _keywords(self, **overrides: object) -> dict:
        """The keyword arguments a well-formed call makes, with overrides applied."""
        keywords = {
            "tool": TOOL,
            "root": self.environment.root,
            "tool_base": self.environment.tool_base,
            "allowlist": self.environment.globs,
            "tally": severity.LiteralTally.with_all_tools(),
        }
        keywords.update(overrides)
        return keywords

    def test_a_well_formed_call_succeeds(self) -> None:
        """The baseline, so every fault below is shown to be the fault under test."""
        rows, rejections, counters = joern.adapt(
            load_fixture("joern"), **self._keywords()
        )
        self.assertEqual(len(rows), 11)
        self.assertEqual(rejections, [])
        self.assertEqual(set(counters), set(joern.COUNTER_KEYS))

    def test_a_document_that_is_not_an_object_raises(self) -> None:
        """Raised rather than answered with zero rows, which would look like a clean scan."""
        for document in ([], "a string", 7, None, ()):
            with self.subTest(document=type(document).__name__):
                with self.assertRaises(joern.JoernAdapterError):
                    joern.adapt(document, **self._keywords())

    def test_another_tool_s_identifier_raises(self) -> None:
        """The identifier is checked rather than trusted, so a mis-route cannot be silent."""
        with self.assertRaises(joern.JoernAdapterError):
            joern.adapt(load_fixture("joern"), **self._keywords(tool="semgrep"))

    def test_a_relative_root_raises(self) -> None:
        """A relative root anchors nothing and would make every row wrong the same way."""
        for root in ("spark-src", "./spark-src", ""):
            with self.subTest(root=root):
                with self.assertRaises(joern.JoernAdapterError):
                    joern.adapt(load_fixture("joern"), **self._keywords(root=root))

    def test_a_path_base_for_another_tool_or_another_kind_raises(self) -> None:
        """The base must be joern's own, and its kind must be ``bytecode_class``.

        A filesystem base for this tool does not exist to fall back on, so a record
        describing one means the call is wrong rather than the artifact.
        """
        foreign = paths.ToolPathBase(
            tool="gitleaks",
            kind=paths.PATH_BASE_KIND_SCAN_ROOT,
            base_value=self.environment.root,
            scan_root=self.environment.root,
        )
        wrong_kind = paths.ToolPathBase(
            tool=TOOL,
            kind=paths.PATH_BASE_KIND_SCAN_ROOT,
            base_value=self.environment.root,
            scan_root=self.environment.root,
        )
        for label, base in (("foreign tool", foreign), ("wrong kind", wrong_kind)):
            with self.subTest(base=label):
                with self.assertRaises(joern.JoernAdapterError):
                    joern.adapt(
                        load_fixture("joern"), **self._keywords(tool_base=base)
                    )

    def test_a_string_allowlist_or_a_tally_that_cannot_record_raises(self) -> None:
        """A string would be iterated character by character; a null tally would under-report."""
        with self.assertRaises(joern.JoernAdapterError):
            joern.adapt(
                load_fixture("joern"), **self._keywords(allowlist="core/src/main/**")
            )
        with self.assertRaises(joern.JoernAdapterError):
            joern.adapt(load_fixture("joern"), **self._keywords(tally=None))

    def test_a_source_index_that_is_not_one_raises(self) -> None:
        """An injected index is checked, since a mapping would silently resolve nothing."""
        with self.assertRaises(joern.JoernAdapterError):
            joern.adapt(
                load_fixture("joern"),
                **self._keywords(),
                source_index={"org/apache/spark/rdd/PipedRDD": ("x",)},
            )

    def test_the_metadata_loader_refuses_a_document_with_no_tools(self) -> None:
        """A missing base is a hard error, never a silent default to the scan root.

        AAP 0.6.1: *"Guessing a base is exactly how every row for that tool gets a wrong
        path."*  Written into the temporary directory, never over the run's own record.
        """
        empty = self.environment.directory / "runner-metadata-empty.json"
        empty.write_text(json.dumps({"spark_src": self.environment.root}), encoding="utf-8")
        with self.assertRaises(paths.RunnerMetadataError):
            paths.load_runner_metadata(empty)
        without_joern = self.environment.directory / "runner-metadata-no-joern.json"
        without_joern.write_text(
            json.dumps(
                {
                    "spark_src": self.environment.root,
                    "tools": {"gitleaks": {"path_base": {"kind": "scan_root"}}},
                }
            ),
            encoding="utf-8",
        )
        document = paths.load_runner_metadata(without_joern)
        with self.assertRaises(paths.RunnerMetadataError):
            paths.tool_path_base(document, TOOL)


# --------------------------------------------------------------------------------------
# Hygiene: the committed inputs, this module's own source, and the prohibitions
# --------------------------------------------------------------------------------------


# The checks in this section read this module's own **structure**, never its prose.
#
# The distinction is not stylistic.  A test that greps its own source text for a forbidden
# token fails the moment the file explains the prohibition -- and worse, it fails for
# reasons that have nothing to do with the property being asserted, which is how a hygiene
# check gets deleted rather than fixed.  Every assertion below therefore works from the
# parsed syntax tree: the import statements actually present, the string constants in
# executable positions with docstrings excluded, the identifiers actually used, and the
# receiver expression in front of every call that touches the filesystem.  Naming a
# prohibition in a docstring is documentation; performing it is a violation, and only the
# second is what these tests look for.

#: Every module this file may import: the standard library members AAP 0.4.1 names, plus the
#: normalizer modules its schema lists as dependencies.  Anything else is a third-party
#: import, which AAP 0.4.3 forbids -- this run introduces no manifest, no lockfile and no
#: install step, and the normalizer and its tests run on the base interpreter.
PERMITTED_IMPORTS = frozenset(
    {
        "__future__",
        "ast",
        "hashlib",
        "json",
        "pathlib",
        "sys",
        "tempfile",
        "unittest",
        "normalize",
    }
)

#: Modules this file must not import, and the reason the list is worth asserting: without
#: any of them no subprocess can be started, no JVM launched and no arbitrary path opened.
#: "Nothing here loads a code-property graph" then holds structurally rather than as a
#: statement about the code as currently written.
CAPABILITY_MODULES_NEVER_IMPORTED = (
    "subprocess",
    "os",
    "shutil",
    "socket",
    "urllib",
    "ctypes",
    "multiprocessing",
    "pickle",
)

#: The provisioned collector, which is never imported: it is not version-controlled, and
#: this module asserts against its artifact contract alone.
PROVISIONED_COLLECTOR_MODULE = "joern_collect"

#: The two graph loaders, each split in half.  ``importCpg`` is the probe's and the other is
#: forbidden run-wide; both are assembled at the point of use so that neither literal is
#: planted in this file by the very assertion that checks for its absence.
LOADER_HALVES = (("import", "Cpg"), ("import", "Code"))

#: ``pathlib`` attributes that create or change something on disk.
WRITE_ATTRIBUTES = frozenset(
    {
        "write_text",
        "write_bytes",
        "writelines",
        "mkdir",
        "touch",
        "unlink",
        "rmdir",
        "rename",
        "replace",
        "symlink_to",
        "hardlink_to",
        "chmod",
    }
)

#: The only receiver expressions this module may name in front of one of those, each derived
#: from the :class:`tempfile.TemporaryDirectory` its test class owns.  Asserted as an
#: equality rather than a subset: a new write target must be added here deliberately, and a
#: documented one that disappears is a change worth noticing too.
PERMITTED_WRITE_RECEIVERS = frozenset(
    {
        "target",  # one scaffold file, under Environment.root_path
        "target.parent",  # its package directory
        "self.root_path",  # the scan root itself
        "self.allowlist_path",  # the twelve globs
        "self.metadata_path",  # the minimal runner metadata
        "empty",  # a metadata document with no tools mapping
        "without_joern",  # a metadata document carrying another tool only
    }
)

#: ``pathlib`` attributes that read a file's content, and the only receivers permitted in
#: front of one.  This is the assertion that makes "no graph is loaded" measurable rather
#: than asserted: the module reads its own source, the committed fixtures and the committed
#: expected files, and nothing else -- so the graph, the runner metadata the live run writes
#: and every Spark source file are all out of reach by construction.
READ_ATTRIBUTES = frozenset({"read_text", "read_bytes"})
PERMITTED_READ_RECEIVERS = frozenset(
    {
        "path",  # read_json / sha256_of, given a committed input path
        "Path(__file__).resolve()",  # this module's own source
        "FIXTURES_DIR / f'{stem}.json'",  # one committed fixture
        "EXPECTED_DIR / f'{stem}.rows.json'",  # one committed expected result
    }
)


def docstring_constant_ids(tree: ast.Module) -> frozenset[int]:
    """Identify every string constant that is a docstring rather than executable content.

    Returned as identities rather than values, so a docstring is excluded at the position it
    occupies without also excluding an identical string used in code somewhere else.
    """
    holders = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
    identities: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, holders) and node.body:
            first = node.body[0]
            if (
                isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)
            ):
                identities.add(id(first.value))
    return frozenset(identities)


def named_table_constant_ids(tree: ast.Module, name: str) -> frozenset[int]:
    """Identify the string constants belonging to one module-level table.

    Used to exclude :data:`SECRET_PATTERNS` from the credential sweep below.  The table *is*
    the list of things being searched for, so scanning it would report itself -- and a check
    that always fails is a check that gets removed.
    """
    identities: set[int] = set()
    for node in ast.walk(tree):
        targets = getattr(node, "targets", ())
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in targets
        ):
            for inner in ast.walk(node.value):
                if isinstance(inner, ast.Constant) and isinstance(inner.value, str):
                    identities.add(id(inner))
    return frozenset(identities)


def code_string_constants(tree: ast.Module, *, excluded: frozenset[int]) -> tuple[str, ...]:
    """Every string constant in an executable position, docstrings and exclusions removed."""
    return tuple(
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in excluded
    )


def imported_top_level_modules(tree: ast.Module) -> frozenset[str]:
    """The top-level module of every import statement this file actually executes."""
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module.split(".")[0])
    return frozenset(modules)


def identifiers_used(tree: ast.Module) -> frozenset[str]:
    """Every name, attribute and definition identifier the module mentions in code."""
    identifiers: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            identifiers.add(node.id)
        elif isinstance(node, ast.Attribute):
            identifiers.add(node.attr)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            identifiers.add(node.name)
        elif isinstance(node, ast.keyword) and node.arg:
            identifiers.add(node.arg)
        elif isinstance(node, ast.alias):
            identifiers.add((node.asname or node.name).split(".")[0])
    return frozenset(identifiers)


def filesystem_call_receivers(
    tree: ast.Module, attributes: frozenset[str]
) -> frozenset[str]:
    """The receiver expression in front of every call to one of ``attributes``.

    The expression is returned as source text, which is what makes the whitelist readable: a
    reviewer can see that every write this module performs is addressed to a path derived
    from a temporary directory without tracing the call graph.
    """
    return frozenset(
        ast.unparse(node.func.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in attributes
    )


def builtin_calls(tree: ast.Module, names: frozenset[str]) -> frozenset[str]:
    """Which of ``names`` this module calls as a bare builtin."""
    return frozenset(
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in names
    )



class HygieneTest(HermeticRootTestCase):
    """The committed inputs are unchanged, and this module observes its own prohibitions.

    Two halves, and both are needed.  The **structural** half reads this file's parsed syntax
    tree: what it imports, which string constants sit in executable positions, and what
    stands in front of every call that touches the filesystem.  The **runtime** half builds
    the same hermetic environment every other class here builds and confirms that everything
    it created really does live inside a temporary directory and outside this repository --
    which is the claim the structural whitelist is a proxy for, checked directly.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Build the hermetic environment, then read and parse this module's own source."""
        super().setUpClass()
        cls.source = Path(__file__).resolve().read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source)
        cls.excluded_constants = docstring_constant_ids(cls.tree) | named_table_constant_ids(
            cls.tree, "SECRET_PATTERNS"
        )
        cls.code_constants = code_string_constants(
            cls.tree, excluded=cls.excluded_constants
        )
        cls.imported = imported_top_level_modules(cls.tree)
        cls.identifiers = identifiers_used(cls.tree)

    def test_every_fixture_is_unchanged(self) -> None:
        """Each fixture's sha256 equals the digest its expected file records.

        A fixture is captured tool output; mutating one would make the adapter agree with a
        shape the tool never emitted.  Asserted rather than trusted, and asserted against the
        expected file rather than against a digest written here, so the two documents are
        cross-checked instead of one being taken on faith.
        """
        for stem in ALL_FIXTURE_STEMS:
            with self.subTest(fixture=stem):
                fixture_path = FIXTURES_DIR / f"{stem}.json"
                self.assertTrue(fixture_path.is_file())
                recorded = load_expected(stem)["fixture"]
                self.assertEqual(recorded["path"].split("/")[-1], f"{stem}.json")
                self.assertEqual(sha256_of(fixture_path), recorded["sha256"])
                self.assertEqual(fixture_path.stat().st_size, recorded["bytes"])
                self.assertEqual(
                    independent_record_count(load_fixture(stem)), recorded["findings"]
                )

    def test_no_expected_row_value_is_absolute_or_a_uri(self) -> None:
        """Every expected row field is root-relative, so no row can encode a scan root.

        Scoped to the ``rows`` arrays and to the recorded candidate paths.  The
        ``/findings/0`` strings elsewhere in an expected file are JSON pointers into the
        fixture rather than filesystem paths, and are deliberately not treated as such.
        """
        for stem in ALL_FIXTURE_STEMS:
            expected = load_expected(stem)
            for index, row in enumerate(expected["rows"]):
                for field, value in row.items():
                    if not isinstance(value, str):
                        continue
                    with self.subTest(fixture=stem, row=index, field=field):
                        self.assertFalse(value.startswith("/"), value)
                        self.assertFalse(value.startswith("\\"), value)
                        self.assertNotIn("://", value)
                        self.assertFalse(Path(value).is_absolute(), value)
        candidates = load_expected("reject-joern-ambiguous-path")[
            "no_row_carries_a_colliding_candidate"
        ]["candidate_paths_that_must_not_appear"]
        for candidate in candidates:
            with self.subTest(candidate=candidate):
                self.assertFalse(candidate.startswith("/"))
                self.assertFalse(Path(candidate).is_absolute())

    def test_this_module_imports_only_the_standard_library_and_its_dependencies(
        self,
    ) -> None:
        """No third-party import, and no ``pytest``: it runs under ``python3 -m unittest``.

        Read from the import statements themselves rather than matched against the source
        text.  A regex over the text is not merely fragile here, it is wrong: a docstring
        line beginning with the word *from* satisfies it and contributes a module name that
        was never imported.
        """
        self.assertTrue(self.imported)
        self.assertEqual(
            self.imported - PERMITTED_IMPORTS,
            frozenset(),
            "this module imports something outside its dependency whitelist",
        )
        for module_name in ("pytest", "nose", "hypothesis", "pytest_mock"):
            with self.subTest(module=module_name):
                self.assertNotIn(module_name, self.imported)
        # And every permitted entry that is actually needed is present, so the whitelist is
        # a description of this file rather than an aspiration.
        for module_name in ("json", "unittest", "tempfile", "pathlib", "normalize", "ast"):
            with self.subTest(module=module_name):
                self.assertIn(module_name, self.imported)

    def test_this_module_never_imports_the_provisioned_collector(self) -> None:
        """``harness/lib/joern_collect.py`` is provisioned, not version-controlled.

        It is never imported and never edited; this module asserts against its **artifact
        contract** alone, which is why every case here is a document rather than a call into
        the collector.  Asserted over the import statements and the identifiers actually
        used, so naming the collector in prose -- as the docstrings here do, to say it is not
        imported -- is not mistaken for importing it.
        """
        self.assertNotIn(PROVISIONED_COLLECTOR_MODULE, self.imported)
        self.assertNotIn(PROVISIONED_COLLECTOR_MODULE, self.identifiers)
        self.assertNotIn(PROVISIONED_COLLECTOR_MODULE, sys.modules)

    def test_this_module_loads_no_graph_and_starts_no_process(self) -> None:
        """The probe's constraint, and the reason it holds here without being enforced.

        Neither graph loader appears in this module's executable content, and neither could
        be reached anyway: nothing that can start a process or open an arbitrary path is
        imported.  The Joern runner and the Stage 5 probe each hold a large heap and are
        deliberately sequenced apart (AAP 0.5.1); this file is a pure data-shape test over an
        already-parsed document and must not become a third claimant on that memory.
        """
        for prefix, suffix in LOADER_HALVES:
            loader = prefix + suffix
            with self.subTest(loader=loader):
                self.assertNotIn(loader, self.identifiers)
                for constant in self.code_constants:
                    self.assertFalse(
                        loader in constant,
                        f"the graph loader {loader} appears in an executable string constant",
                    )
        for module_name in CAPABILITY_MODULES_NEVER_IMPORTED:
            with self.subTest(module=module_name):
                self.assertNotIn(
                    module_name,
                    self.imported,
                    "importing this would give the module the capability to start a "
                    "process or open an arbitrary path",
                )
        self.assertEqual(
            builtin_calls(self.tree, frozenset({"open", "exec", "eval", "compile"})),
            frozenset(),
            "no builtin that opens a file or executes code is called",
        )

    def test_no_secret_pattern_reaches_this_module_or_its_fixtures(self) -> None:
        """This tree is committed to git, so no credential-shaped value may reach it.

        The module's own contribution is checked over its executable string constants with
        :data:`SECRET_PATTERNS` itself excluded -- that table is the list of things being
        searched for, so including it would make the check report itself and fail always,
        which is how a hygiene check ends up deleted instead of fixed.  The committed
        fixtures and expected files are checked over their whole text, since none of them has
        any reason to name a credential prefix at all.
        """
        for pattern in SECRET_PATTERNS:
            with self.subTest(pattern=pattern):
                for constant in self.code_constants:
                    self.assertFalse(
                        pattern in constant,
                        f"a credential-shaped value appears in an executable string "
                        f"constant of this module: {pattern}",
                    )
        for stem in ALL_FIXTURE_STEMS:
            fixture_text = (FIXTURES_DIR / f"{stem}.json").read_text(encoding="utf-8")
            expected_text = (EXPECTED_DIR / f"{stem}.rows.json").read_text(
                encoding="utf-8"
            )
            for pattern in SECRET_PATTERNS:
                with self.subTest(fixture=stem, pattern=pattern):
                    self.assertFalse(
                        pattern in fixture_text,
                        f"fixtures/{stem}.json carries {pattern}",
                    )
                    self.assertFalse(
                        pattern in expected_text,
                        f"expected/{stem}.rows.json carries {pattern}",
                    )
        # Gitleaks runs with redaction and no adapter carries a secret value into a field,
        # so no emitted row may carry one either.
        for stem in ALL_FIXTURE_STEMS:
            adapted = self.adapt_fixture(stem)
            for index, row in enumerate(adapted.rows):
                for field in emit.FIELDS:
                    value = row[field]
                    if not isinstance(value, str):
                        continue
                    for pattern in SECRET_PATTERNS:
                        with self.subTest(fixture=stem, row=index, field=field):
                            self.assertFalse(pattern in value)

    def test_every_class_this_module_asserts_is_a_real_rejection_class(self) -> None:
        """The vocabulary is closed and owned by ``paths.py``; nothing here invents a name."""
        asserted = {expected_class for _, expected_class in NEGATIVE_FIXTURES}
        asserted.add(paths.REJECT_ABSENT_PATH)
        asserted |= set(UNREACHABLE_REJECT_CLASSES)
        for reject_class in asserted:
            with self.subTest(reject_class=reject_class):
                self.assertTrue(paths.is_reject_class(reject_class))
                self.assertIn(reject_class, paths.REJECT_CLASSES)
        self.assertEqual(len(paths.REJECT_CLASSES), 10)

    def test_the_scaffold_spec_is_internally_consistent(self) -> None:
        """Every scaffold path is on a source root, unique, relative and in a known tree.

        The scaffold is what every resolution in this module is measured against, so a
        duplicate entry or a path off a source root would quietly change what a test means.
        """
        seen: set[str] = set()
        for relative, declares in SCAFFOLD:
            with self.subTest(path=relative):
                self.assertNotIn(relative, seen, "duplicate scaffold entry")
                seen.add(relative)
                self.assertFalse(relative.startswith("/"))
                self.assertFalse(Path(relative).is_absolute())
                self.assertIsNotNone(paths.source_index_key(relative))
                self.assertTrue(declares, "each file must declare at least one type")
                self.assertTrue(
                    relative.endswith(paths.SOURCE_EXTENSIONS),
                    "only .scala and .java are indexed",
                )
        self.assertTrue(
            any("src/test" in relative for relative, _ in SCAFFOLD),
            "the scaffold must materialise a test tree, or gap 1 is untested",
        )
        self.assertTrue(any("src/main" in relative for relative, _ in SCAFFOLD))

    def test_this_module_writes_only_inside_a_temporary_directory(self) -> None:
        """Nothing this module writes leaves the temporary directory it owns.

        The structural half: every filesystem write is addressed to one of a small set of
        receiver expressions, each derived from that directory, and every read is addressed
        to this file's own source or to a committed input.  The equality is deliberate -- a
        new write target has to be added to the whitelist on purpose.

        The runtime half: the directory really is under the system temporary location and
        really is outside this repository, and every path the environment created is inside
        it.  Materialising ``core/src/test/...`` inside a temporary directory is a test
        scaffold rather than a modification of Spark's tree, and this is what makes that
        distinction a measured fact.
        """
        self.assertEqual(
            filesystem_call_receivers(self.tree, WRITE_ATTRIBUTES),
            PERMITTED_WRITE_RECEIVERS,
            "a filesystem write is addressed to a receiver outside the whitelist",
        )
        self.assertEqual(
            filesystem_call_receivers(self.tree, READ_ATTRIBUTES),
            PERMITTED_READ_RECEIVERS,
            "a file is read from a receiver outside the whitelist",
        )

        temporary_root = Path(tempfile.gettempdir()).resolve()
        directory = self.environment.directory.resolve()
        repository = REPO_ROOT.resolve()
        self.assertTrue(
            directory.is_relative_to(temporary_root),
            f"{directory} is not under the system temporary directory",
        )
        self.assertFalse(
            directory.is_relative_to(repository),
            f"{directory} is inside the repository checkout",
        )

        created = [
            self.environment.root_path,
            self.environment.allowlist_path,
            self.environment.metadata_path,
        ]
        created.extend(
            self.environment.root_path / relative
            for relative in self.environment.scaffold_paths
        )
        self.assertEqual(len(created), len(SCAFFOLD) + 3)
        for candidate in created:
            with self.subTest(path=str(candidate)):
                self.assertTrue(candidate.exists())
                resolved = candidate.resolve()
                self.assertTrue(resolved.is_relative_to(directory))
                self.assertFalse(resolved.is_relative_to(repository))
        # And the committed input directories hold exactly the joern files this module reads:
        # no eighth fixture and no eighth expected result appeared beside them, which is the
        # observable consequence of writing nothing here.  Other adapters' files share both
        # directories and are deliberately left out of the comparison.
        self.assertEqual(
            sorted(
                entry.name
                for entry in FIXTURES_DIR.iterdir()
                if entry.is_file() and entry.name.split(".")[0] in ALL_FIXTURE_STEMS
            ),
            sorted(f"{stem}.json" for stem in ALL_FIXTURE_STEMS),
        )
        self.assertEqual(
            sorted(
                entry.name
                for entry in EXPECTED_DIR.iterdir()
                if entry.is_file() and entry.name.split(".")[0] in ALL_FIXTURE_STEMS
            ),
            sorted(f"{stem}.rows.json" for stem in ALL_FIXTURE_STEMS),
        )
