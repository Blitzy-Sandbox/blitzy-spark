"""Field-by-field assertions over the Trivy 0.74.0 native adapter.

What this module tests
---------------------
``harness/lib/normalize/adapters/trivy.py``, the one adapter in this dataset whose
``scanner_class`` varies **row by row** and the only one carrying a structural
**halt**.  AAP 0.6.1 gives this file its row -- *"Asserts scanner_class derives from
the section array, never from record content, and that a non-empty unsupported
finding section halts rather than being dropped"* -- AAP 0.5.4 fixes the behaviour it
asserts, and AAP 0.9.4 puts it in the definition of done.  A failure here is one of
the conditions that stops the run (AAP 0.9.2), so nothing below is weakened to make a
test pass.

No user-specified rule governs this file; enterprise-standard best practice applies in
its place (AAP 0.7, AAP 0.10.2).  That absence is not licence to lower the bar, which
here means three concrete things: the halt is asserted by its exact exception type
rather than by a bare ``Exception``, ``scanner_class`` is asserted per row against the
section the record was read from rather than in aggregate, and every rejection is
asserted by its **class name** taken from :data:`normalize.paths.REJECT_CLASSES` rather
than by a rejection count.  A test that only counts rejections cannot tell one
condition from another.

Why the unsupported-section halt has to be a halt and not a warning
-------------------------------------------------------------------
This is the whole justification for stopping the run, and it is the reason
:class:`~normalize.adapters.trivy.UnsupportedTrivySection` is asserted by type here
rather than tolerated:

**A silently dropped finding section would let reconciliation pass while real tool
output vanished.**  The reconciliation identity is
``raw finding records = dataset rows + rejected records``, and its left-hand side is
``reconcile._count_trivy``, whose count unit is ``Results[]`` x the **three
supported** sections.  An ignored ``Licenses`` array is therefore absent from *both*
sides: the identity would balance exactly -- over a count unit that never saw the
dropped array -- while four records of real tool output left no trace in the dataset,
in the counters, or in the reconciliation.  A drop that unbalances the identity is
caught by the identity; a drop invisible to both sides is caught by nothing, and an
empty or silently-reduced result set is indistinguishable from a clean scan.  Hence
AAP 0.5.4's requirement that the run stop with the observed structure quoted, and
hence :class:`UnsupportedSectionStopsTheRunTest` below, which also asserts the
counterfactual arithmetic that would have balanced.

Rejection conditions this adapter can produce, one negative fixture each
-----------------------------------------------------------------------
Eight of the ten classes in :data:`normalize.paths.REJECT_CLASSES` are reachable from
a Trivy artifact, and every one has a committed fixture asserted below whether or not
the measured artifact contained the case (AAP 0.9.4): ``absent_path``,
``unresolvable_path``, ``missing_rule_id``, ``missing_message``,
``non_integer_start_line``, ``unattributable_section``,
``unformable_package_coordinate`` and ``malformed_record``.

The two sets are asserted to **partition** the closed vocabulary by
:class:`RejectionClassPartitionTest`: every class this shape can produce is named with
the fixture that produces it, every class it cannot is named with the reason, and the
two together are asserted equal to :data:`normalize.paths.REJECT_CLASSES` with no
remainder on either side.  A class that fell out of both sets -- reachable but
untested -- is what that assertion exists to catch, and it is why the count and the
class names in the list above are asserted against the measured sets rather than only
written down.

Conditions this adapter **cannot** produce, and why
---------------------------------------------------
Stated so their absence is a recorded fact rather than a gap:

* **a cyclic, over-deep or invalid** ``uriBaseId`` **chain** (``invalid_uri``, and the
  chain terminal cases) -- a Trivy native report carries no SARIF base map at all.
  There is no ``uri``, no ``uriBaseId`` and no ``originalUriBaseIds`` for a chain to
  be walked over, so no chain can cycle, exceed a depth or be syntactically invalid.
  Those belong to the shared SARIF adapter and are asserted by
  ``test_sarif_adapter.py``;
* **an ambiguous bytecode-to-source resolution** (``ambiguous_source_resolution``) --
  this adapter resolves reported paths, never bytecode.  Its input is a filesystem
  report, there is no class identifier to resolve against ``src/main`` and
  ``src/test``, and therefore nothing that two source files could both claim.  That
  belongs to the Joern adapter and is asserted by ``test_joern_adapter.py``.

Both are asserted absent from every committed fixture's rejections rather than only
argued for in prose, so "cannot happen" is a measured claim.

Hermetic by construction
------------------------
Every test runs against an absolute scan root inside one
:class:`tempfile.TemporaryDirectory`, an allowlist file holding the twelve
authoritative globs, and minimal ``runner-metadata.json`` documents -- all loaded
through ``paths.py``'s own loaders (:func:`normalize.paths.load_allowlist`,
:func:`normalize.paths.load_runner_metadata`, :func:`normalize.paths.tool_path_base`)
rather than fabricated, so the loaders are exercised on exactly the route ``cli.py``
uses.  No file is materialised inside that root: no code path under test reads the
tree, which is what makes the temporary root legitimate rather than a convenience.

Nothing outside that directory is written.  Every committed fixture is **read** and
never modified: :func:`load_fixture` re-parses the file on each call and every
derived variant is a :func:`copy.deepcopy`, and :class:`FixtureIntegrityTest` records
each fixture's sha256 at module setup and asserts it again afterwards.  This module
writes no deliverable -- in particular never ``oss-scan-results/findings.json`` or
``findings.csv``.

Presence is observed, never assumed
-----------------------------------
``fixtures/trivy.json`` is a byte-for-byte copy of the runner's own artifact,
``harness/artifacts/raw/trivy.json`` (AAP 0.6.2).  :class:`CapturedFixtureProvenanceTest`
is the one class that reads the artifact, and it reads it to prove that provenance: the
two files' bytes, lengths and digests, then every ``Results`` element and every
``Misconfigurations`` record under canonical comparison, then the envelope member by
member.  Every other assertion in this module is over a committed fixture, so the
corpus stands on its own even where the artifact tree is absent.

Absence is reported, never skipped.  A missing fixture is a blocking gap raised by
:class:`FixtureCorpusTest`, and a missing raw artifact is an explicit failure naming
the path rather than a skip -- a skipped test is a green suite that asserted nothing.

Prohibitions this module observes (AAP 0.3.2, AAP 0.8.2)
--------------------------------------------------------
**No cross-tool interpretation of any kind.**  Trivy's three sections are not
compared with any other tool's coverage, in an assertion or in a comment; no other
tool's rows, counts or capabilities appear anywhere in this file.  Nothing is compared
against Apex, Cantina or any other scanner.  No finding is judged real, important, a
false positive or a duplicate, and nothing is deduplicated: two records naming the
same package at the same version are two rows and this file adds no remark about
them.  **No secret value appears in any fixture, literal, assertion message or
docstring** -- this tree is committed to git, whose only relevant ignore line matches
``artifacts/`` (``.gitignore:31``), so the guarantee is asserted structurally by
:class:`SecretRedactionTest` rather than left incidental.  Nothing under
``harness/lib/normalize/`` is edited by this file; a defect found there is reported,
never repaired here.

Imports and the ``sys.path`` bootstrap
--------------------------------------
Standard library only -- no ``pytest``, no third-party package, no manifest and no
install step (AAP 0.4.1, AAP 0.4.3) -- and the module runs under
``python3 -m unittest`` from any working directory.

Public API
    Every :class:`unittest.TestCase` below, discovered by ``unittest``.  The module's
    own helpers -- :class:`Environment`, :func:`load_fixture`, :func:`load_expected`,
    :func:`adapt_fixture`, :func:`section_walk` -- are shared fixtures for those
    tests, not a contract for anything outside this file.
"""

import copy
import dataclasses
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

# --------------------------------------------------------------------------------------
# The one-time sys.path bootstrap.
#
# There is deliberately no __init__.py under harness/lib/normalize/: PEP 420 implicit
# namespace packages make "from normalize import ..." work once harness/lib is on
# sys.path. cli.py owns the same two lines for its own direct-script route and its
# docstring states that the test modules under oss-scan-results/adapter-tests/ reach
# these modules with them.
#
# This file sits at <repo>/oss-scan-results/adapter-tests/, so parents[2] is the
# repository root. The entry is derived from this file's own resolved location rather
# than from the working directory, which is what makes discovery work from the
# repository root and from anywhere else alike. It is inserted once and only if absent,
# so repeated imports and a discovery run that already put it there leave one entry.
# --------------------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[2]
_LIB_DIR = str(REPO_ROOT / "harness" / "lib")
if _LIB_DIR not in sys.path:
    sys.path.insert(0, _LIB_DIR)

from normalize import emit, paths, reconcile, severity  # noqa: E402
from normalize.adapters import trivy  # noqa: E402

# --------------------------------------------------------------------------------------
# Locations. Both directories are inputs and are never written to by this module.
# --------------------------------------------------------------------------------------
ADAPTER_TESTS_DIR = Path(__file__).resolve().parent
FIXTURES_DIR = ADAPTER_TESTS_DIR / "fixtures"
EXPECTED_DIR = ADAPTER_TESTS_DIR / "expected"

# --------------------------------------------------------------------------------------
# The twelve authoritative scope globs (AAP 0.3.1), byte-exact and in the request's
# order, with no exclusion line: the literal `src/test` exclusion is paths.py's, not the
# allowlist's.
#
# Restated here independently rather than read from paths.ALLOWLIST_GLOBS. The test
# writes these twelve lines to its own allowlist file, loads them back through
# paths.load_allowlist() and then confirms the loaded tuple is what paths.py authors, via
# paths.allowlist_matches_authoritative_globs(). Loading the module's own copy and
# comparing it with itself would assert nothing.
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

#: The canonical tool identifier every row this adapter emits must carry.
TOOL = "trivy"

#: The primary positive fixture: a byte-for-byte capture of this run's own Trivy
#: artifact, ``harness/artifacts/raw/trivy.json``.  The whole report is 3,496 bytes, so
#: the capture needs no excerpting at all and its sha256 equals the raw artifact's --
#: the strongest provenance available for a fixture (AAP 0.6.2, which requires the
#: primary positive fixture to be an unmodified captured excerpt of the tool's own
#: output).  It carries three ``Misconfigurations`` records and nothing else:
#: ``CapturedFixtureProvenanceTest`` asserts the identity against the raw artifact, and
#: the sections it does not contain are covered by ``FEATURES_FIXTURE`` below.
POSITIVE_FIXTURE = "trivy.json"

#: The raw artifact ``POSITIVE_FIXTURE`` is captured from, repository-root-relative.
#: Read by ``CapturedFixtureProvenanceTest`` and by nothing else; no test writes to it.
RAW_ARTIFACT_RELPATH = "harness/artifacts/raw/trivy.json"

#: The derived companion to the capture: an authored multi-section document, carrying a
#: ``derived-`` name so no fixture claims captured provenance it does not have.  It
#: exists because the raw artifact contains no ``Vulnerabilities`` and no ``Secrets``
#: section, so the capture cannot exercise scanner_class variation, section-dependent
#: ``start_line``, secret redaction, multi-valued CWE/CVE selection or any
#: package-coordinate level.  Its expected file declares it derived, states separately
#: where its shape and its records came from, and enumerates the feature cases it exists
#: to carry.  Every assertion that needs those features runs against this document;
#: every assertion about provenance runs against the capture.
FEATURES_FIXTURE = "derived-trivy-features.json"

#: The expected-row stems for the two positive fixtures, so a test can name the pair it
#: is running against as data rather than as a literal in two places.
POSITIVE_EXPECTED_STEM = "trivy"
FEATURES_EXPECTED_STEM = "derived-trivy-features"

#: The fixture whose two appended elements each hold a non-empty unsupported finding
#: section. Its expected file records a stop rather than rows.
UNSUPPORTED_SECTION_FIXTURE = "halt-trivy-unsupported-section.json"

#: One committed halt fixture per member of ``trivy.HALT_REASONS``, keyed by the reason
#: constant it raises, so the closed-set check in ``HaltReasonCoverageTest`` reads the
#: mapping rather than a hand-kept list.  ``validate_finding_sections`` checks the four
#: conditions in the order ``HALT_REASONS`` states them, so each fixture is built to
#: reach its own condition without tripping an earlier one -- which is what makes the
#: raised reason attributable to the structure the fixture names.
HALT_FIXTURE_BY_REASON = {
    trivy.HALT_UNSUPPORTED_SECTION: "halt-trivy-unsupported-section",
    trivy.HALT_UNKNOWN_SECTION: "halt-trivy-unknown-section",
    trivy.HALT_SECTION_NOT_AN_ARRAY: "halt-trivy-section-not-an-array",
    trivy.HALT_DECLARED_FINDINGS_UNHELD: "halt-trivy-declared-findings-unheld",
}

#: One negative fixture per rejection condition this adapter can produce (AAP 0.9.4).
#: The stems are the fixture and expected filenames alike, which is this folder's
#: convention: fixtures/<stem>.json against expected/<stem>.rows.json.
#:
#: ``reject-trivy-absent-path`` is the ``absent_path`` case.  A ``Results[]`` element
#: whose ``Target`` is absent, null or blank reaches ``paths.resolve_trivy_path``, which
#: hands it to ``resolve_recorded_path``; that returns ``paths.REJECT_ABSENT_PATH``
#: because ``path`` is not one of the five fields absence is permitted for (AAP 0.8.2).
#: The condition is reachable for this adapter and therefore carries a fixture, and
#: :class:`RejectionClassPartitionTest` asserts the producible and unreachable sets
#: partition ``paths.REJECT_CLASSES`` exactly.
REJECT_FIXTURE_STEMS = (
    "reject-trivy-absent-path",
    "reject-trivy-unresolvable-path",
    "reject-trivy-missing-rule-id",
    "reject-trivy-missing-message",
    "reject-trivy-non-integer-start-line",
    "reject-trivy-unattributable-section",
    "reject-trivy-no-package-coordinate",
    "reject-trivy-malformed-record",
    "reject-trivy-negative-start-line",
    "reject-trivy-boolean-start-line",
)

#: The rejection class each of those fixtures asserts, so the mapping from fixture to
#: class is data rather than a string buried in a test. Each value is asserted to be a
#: literal member of paths.REJECT_CLASSES and to equal the module's own constant.
REJECT_CLASS_BY_STEM = {
    "reject-trivy-absent-path": paths.REJECT_ABSENT_PATH,
    "reject-trivy-unresolvable-path": paths.REJECT_UNRESOLVABLE_PATH,
    "reject-trivy-missing-rule-id": paths.REJECT_MISSING_RULE_ID,
    "reject-trivy-missing-message": paths.REJECT_MISSING_MESSAGE,
    "reject-trivy-non-integer-start-line": paths.REJECT_NON_INTEGER_START_LINE,
    "reject-trivy-unattributable-section": paths.REJECT_UNATTRIBUTABLE_SECTION,
    "reject-trivy-no-package-coordinate": paths.REJECT_UNFORMABLE_PACKAGE_COORDINATE,
    "reject-trivy-malformed-record": paths.REJECT_MALFORMED_RECORD,
    "reject-trivy-negative-start-line": paths.REJECT_NON_INTEGER_START_LINE,
    "reject-trivy-boolean-start-line": paths.REJECT_NON_INTEGER_START_LINE,
}

#: The two rejection classes a Trivy native artifact cannot produce, each with the
#: structural reason it cannot. The other half of the partition
#: :class:`RejectionClassPartitionTest` asserts over ``paths.REJECT_CLASSES``: this
#: mapping plus the classes in :data:`REJECT_CLASS_BY_STEM` must together be the closed
#: vocabulary exactly, with nothing left over on either side.
#:
#: Each reason is a claim about the artifact's shape rather than about this corpus, which
#: is why each is also asserted to appear in no committed fixture's rejections. "This
#: shape has no URI" is falsifiable; "our fixtures happen not to contain one" is not, and
#: would let a class drift from unreachable to merely untested without anything noticing.
UNREACHABLE_REJECT_CLASSES = {
    paths.REJECT_INVALID_URI: (
        "a Trivy native report carries no SARIF base map: no uri, no uriBaseId and no "
        "originalUriBaseIds. There is no reference to parse and no chain to walk, so "
        "none can be syntactically invalid, cycle or exceed a depth. Those terminals "
        "belong to the shared SARIF adapter and are asserted by test_sarif_adapter.py"
    ),
    paths.REJECT_AMBIGUOUS_SOURCE_RESOLUTION: (
        "this adapter resolves reported filesystem paths, never bytecode. There is no "
        "class identifier to resolve against src/main and src/test and therefore "
        "nothing two source files could both claim. That class belongs to the Joern "
        "adapter and is asserted by test_joern_adapter.py"
    ),
}

#: The one fixture that is an unmerged per-directory part rather than the merged
#: artifact, and must be read with per_section_target semantics and an explicit
#: section base.
#:
#: Every other Trivy fixture states ArtifactName "." with each Target already prefixed
#: by its scope directory -- the merged shape run-trivy.sh writes. This one states its
#: own scope directory as ArtifactName with Targets relative to it, which is the shape
#: of one of the eighteen per-directory reports the runner wrote under
#: logs/trivy.parts/ -- reports that are ABSENT from this checkout, so this fixture is
#: authored to that shape rather than captured from one of them.
#: runner-metadata.json records of those parts that they are not root-anchored, and
#: paths.resolve_trivy_path states the same from the resolver's side: a caller reading
#: one "passes a per_section_target base and a section_base; passing neither is a
#: rejection rather than a silent reading against the root." The base is therefore read
#: as input, never inferred from the artifact (AAP 0.5.4).
PER_SECTION_TARGET_STEMS = frozenset({"reject-trivy-unresolvable-path"})

#: The fixture whose outcome is reached through the public section-attribution seam,
#: trivy.adapt_record, because adapt's own iteration is section-bound by construction
#: and cannot hand a record a section outside SUPPORTED_SECTIONS. The adapter's own
#: docstring says the branch "is exercised by calling this function directly, which is
#: what the adapter test does".
SEAM_STEM = "reject-trivy-unattributable-section"

#: The record that seam hands over under a foreign section key, as the triple
#: (result_index, section-in-the-document, record_index), and the key it is handed
#: under. "Packages" is a member of trivy.KNOWN_NON_FINDING_KEYS and is physically
#: present as a non-empty object array on the very element the record sits in, so it is
#: the realistic mis-read rather than an invented string.
SEAM_DESIGNATED_RECORD = (3, "Vulnerabilities", 0)
SEAM_SECTION_KEY = "Packages"

#: The two path-base readings this module needs, both written into runner metadata
#: documents and loaded back through paths.py. The kinds are the ones
#: harness/artifacts/logs/runner-metadata.json records for this provisioning: scan_root
#: for the merged artifact, and per_section_target for the runner-written parts, which
#: are absent from this checkout.
BASE_KIND_MERGED = paths.PATH_BASE_KIND_SCAN_ROOT
BASE_KIND_PART = paths.PATH_BASE_KIND_PER_SECTION_TARGET

#: The label vocabulary and the band each literal takes, restated independently of
#: severity.py so the comparison is against a written policy rather than against the
#: module's own copy of it (AAP 0.5.4's severity table; the matching is
#: case-insensitive).
EXPECTED_LABEL_BANDS = {
    "CRITICAL": "Critical",
    "HIGH": "High",
    "MODERATE": "Medium",
    "MEDIUM": "Medium",
    "LOW": "Low",
    "NEGLIGIBLE": "Info",
    "INFO": "Info",
    "INFORMATIONAL": "Info",
    "UNKNOWN": "Info",
    "NONE": "Info",
}

#: The five bands severity_norm may take, in severity order.
EXPECTED_SEVERITY_NORM = ("Critical", "High", "Medium", "Low", "Info")

#: The five fields absence is permitted for, and no others (AAP 0.8.2). Restated so the
#: invariant is asserted against a written list as well as against emit.py's own.
EXPECTED_OPTIONAL_FIELDS = frozenset(
    {"severity_native", "start_line", "cwe", "cve", "package_coordinate"}
)


class Environment:
    """The hermetic inputs every test shares: a scan root, an allowlist, two path bases.

    All three are real files inside one temporary directory, and both configuration
    files are read back through ``paths.py``'s own loaders rather than handed to the
    adapter as literals -- so the loaders are exercised on the same route ``cli.py``
    uses.  Nothing is materialised inside the scan root: no code path under test reads
    the tree, so a root that exists and is empty is exactly as good as a populated one
    and considerably easier to reason about.

    Two runner-metadata documents are written, not one, because two of this folder's
    Trivy fixtures are different artifacts.  The merged report the runner writes is
    ``scan_root``-based; the per-directory parts the runner wrote -- absent from this
    checkout -- are ``per_section_target``
    and carry no single base, which is the whole subject of the unresolvable-path
    fixture.  Reading one with the other's base is what the metadata's own note about
    the parts exists to prevent.

    Attributes:
        directory: The temporary directory holding everything this object created.
        root: The absolute scan root every path is expressed against.
        allowlist_path: The allowlist file written here, then loaded.
        globs: The twelve authoritative globs, as ``paths.load_allowlist`` returned
            them.
        metadata_paths: The two metadata documents written, by base kind.
        metadata: The two loaded metadata documents, by base kind.
    """

    def __init__(self, directory: Path) -> None:
        """Create the scan root and write, then load, the allowlist and the metadata."""
        self.directory = directory
        root_path = directory / "spark-src"
        root_path.mkdir(parents=True, exist_ok=True)
        self.root = str(root_path)

        # One glob per line, byte-exact, a trailing newline and nothing else. No
        # exclusion line: the `src/test` exclusion is paths.py's.
        self.allowlist_path = directory / "allowlist.txt"
        self.allowlist_path.write_text(
            "".join(f"{glob}\n" for glob in AUTHORITATIVE_GLOBS), encoding="utf-8"
        )
        self.globs = paths.load_allowlist(self.allowlist_path)

        self.metadata_paths: dict[str, Path] = {}
        self.metadata: dict[str, Any] = {}
        for kind in (BASE_KIND_MERGED, BASE_KIND_PART):
            location = directory / f"runner-metadata-{kind}.json"
            location.write_text(
                json.dumps(self._metadata_document(kind), indent=1) + "\n",
                encoding="utf-8",
            )
            self.metadata_paths[kind] = location
            self.metadata[kind] = paths.load_runner_metadata(location)

    def _metadata_document(self, kind: str) -> dict[str, Any]:
        """Build the minimal document ``paths.load_runner_metadata`` accepts.

        Minimal is deliberate.  The document carries the base facts a resolver needs
        -- the recorded kind, its value where the kind has one, the resolved scan root
        and the invocation shape -- and nothing that would make this test a second copy
        of the run's own record.  ``runner-metadata.json`` is the normalizer's *input*
        and is never generated from an artifact (AAP 0.6.4).

        A ``per_section_target`` base records no value on purpose: that kind supplies no
        single base, which is why ``ToolPathBase.base_for_relative()`` returns ``None``
        for it and why a relative per-record path under it is a counted rejection rather
        than a silent reading against the root.
        """
        path_base: dict[str, Any] = {"kind": kind}
        if kind == BASE_KIND_MERGED:
            path_base["value"] = self.root
            path_base["evidence"] = (
                "The merged artifact's Targets are root-relative: the runner invokes "
                "trivy fs once per scope directory and its merge step prefixes every "
                "Target with that part's own ArtifactName."
            )
        else:
            path_base["value"] = None
            path_base["evidence"] = (
                "A per-directory part is not root-anchored: each states its "
                "Target relative to the single path its invocation was given, so it is "
                "read with a section base rather than against the root."
            )
        return {
            "purpose": (
                "Minimal runner metadata for the Trivy adapter test. Written and read "
                "inside a temporary directory; it is not the run's record."
            ),
            "spark_src": self.root,
            "tools": {
                TOOL: {
                    "canonical_tool_identifier": TOOL,
                    "script_classification": "runner",
                    "path_base": path_base,
                    "resolved_scan_root": self.root,
                    "invocation_form": {
                        "target_passing_style": (
                            "exactly one root-relative path per invocation, one "
                            "invocation per scope directory"
                        ),
                        "invocations_per_run": len(AUTHORITATIVE_GLOBS),
                    },
                    "working_directory": {"path": self.root},
                }
            },
        }

    def tool_base(self, kind: str = BASE_KIND_MERGED) -> paths.ToolPathBase:
        """Return the tool's recorded path base, taken from the loaded document."""
        return paths.tool_path_base(self.metadata[kind], TOOL)


#: Module-level state, built once in :func:`setUpModule` and released in
#: :func:`tearDownModule`. Held at module level because every test needs the same root:
#: rebuilding it per test would make each test's rows depend on a different temporary
#: path for no gain.
ENV: Environment | None = None
_TEMPORARY_DIRECTORY: tempfile.TemporaryDirectory | None = None

#: Every fixture this module requires, and its sha256 as read at module setup. The
#: digests are taken once so :class:`FixtureIntegrityTest` can prove no test mutated a
#: committed file, and are measured rather than written down: the assertion is that they
#: do not change during the run, not that they equal a value recorded here.
_FIXTURE_DIGESTS: dict[str, str] = {}


def halt_fixture_stems() -> tuple[str, ...]:
    """The halt fixture stems in ``trivy.HALT_REASONS`` order, one per reason.

    Ordered by the closed reason tuple rather than alphabetically, so the sequence
    mirrors the order ``validate_finding_sections`` checks the four conditions in and a
    reader can see that each fixture reaches its own condition.
    """
    return tuple(HALT_FIXTURE_BY_REASON[reason] for reason in trivy.HALT_REASONS)


def required_fixture_names() -> tuple[str, ...]:
    """Every fixture filename this module reads, in a stable order."""
    return (
        POSITIVE_FIXTURE,
        FEATURES_FIXTURE,
        *(f"{stem}.json" for stem in halt_fixture_stems()),
        *(f"{stem}.json" for stem in REJECT_FIXTURE_STEMS),
    )


def required_expected_names() -> tuple[str, ...]:
    """Every hand-verified expected filename this module reads, in a stable order."""
    return (
        f"{POSITIVE_EXPECTED_STEM}.rows.json",
        f"{FEATURES_EXPECTED_STEM}.rows.json",
        *(f"{stem}.rows.json" for stem in halt_fixture_stems()),
        *(f"{stem}.rows.json" for stem in REJECT_FIXTURE_STEMS),
    )


def _sha256(location: Path) -> str:
    """Return the hex sha256 of a file, read as bytes."""
    return hashlib.sha256(location.read_bytes()).hexdigest()


def setUpModule() -> None:
    """Create the temporary scan root and configuration, and digest every fixture."""
    global ENV, _TEMPORARY_DIRECTORY
    _TEMPORARY_DIRECTORY = tempfile.TemporaryDirectory(prefix="blitzy-trivy-adapter-")
    ENV = Environment(Path(_TEMPORARY_DIRECTORY.name))
    _FIXTURE_DIGESTS.clear()
    for name in required_fixture_names():
        location = FIXTURES_DIR / name
        if location.is_file():
            _FIXTURE_DIGESTS[name] = _sha256(location)


def tearDownModule() -> None:
    """Release the temporary directory. Nothing this module wrote survives it."""
    global ENV, _TEMPORARY_DIRECTORY
    ENV = None
    if _TEMPORARY_DIRECTORY is not None:
        _TEMPORARY_DIRECTORY.cleanup()
        _TEMPORARY_DIRECTORY = None


def environment() -> Environment:
    """Return the module's environment, or fail loudly if it was never built."""
    if ENV is None:  # pragma: no cover - defended, unreachable under unittest
        raise RuntimeError(
            "setUpModule did not run: the hermetic environment is missing, so no "
            "assertion below would be measuring the adapter against a known root"
        )
    return ENV


def load_fixture(name: str) -> Any:
    """Parse one committed fixture. The file is read and never written.

    A fresh document is returned on every call, so a test that derives a variant can
    deep-copy it with no risk of a shared object being mutated between tests.

    Raises:
        FileNotFoundError: Naming the missing fixture as a blocking gap. A fixture this
            module needs and cannot find is reported, never skipped: a skipped test is
            a green suite that asserted nothing (AAP 0.9.4).
    """
    location = FIXTURES_DIR / name
    if not location.is_file():
        raise FileNotFoundError(
            f"blocking gap: the Trivy adapter test requires the committed fixture "
            f"{location} and it is absent. The adapter's behaviour cannot be asserted "
            "without it, and this is reported rather than skipped."
        )
    return json.loads(location.read_text(encoding="utf-8"))


def load_expected(stem: str) -> Any:
    """Parse one hand-verified expected file from ``expected/``.

    Raises:
        FileNotFoundError: Naming the missing expectation, for the same reason.
    """
    location = EXPECTED_DIR / f"{stem}.rows.json"
    if not location.is_file():
        raise FileNotFoundError(
            f"blocking gap: the Trivy adapter test requires the hand-verified "
            f"expectation {location} and it is absent. Values asserted against nothing "
            "are not assertions, and this is reported rather than skipped."
        )
    return json.loads(location.read_text(encoding="utf-8"))


class Record:
    """One element of one supported finding section, with where it was read from.

    The section is carried alongside the record because the section is what decides the
    row's ``scanner_class``, and a walk that forgot it would be unable to state the
    expectation this file exists to assert.

    Attributes:
        result_index: The enclosing element's index in ``Results[]``.
        section: The section key the record was read from.
        record_index: The record's index within that section array.
        record: The record itself, as parsed.
        target: The enclosing ``Results[].Target``.
        element: The enclosing ``Results[]`` element.
    """

    __slots__ = ("result_index", "section", "record_index", "record", "target", "element")

    def __init__(
        self,
        result_index: int,
        section: str,
        record_index: int,
        record: Any,
        element: Any,
    ) -> None:
        """Hold one record and the coordinates that identify it."""
        self.result_index = result_index
        self.section = section
        self.record_index = record_index
        self.record = record
        self.element = element
        self.target = element.get("Target") if isinstance(element, dict) else None

    @property
    def pointer(self) -> str:
        """The JSON pointer naming this record, as the expected files spell it."""
        return f"/Results/{self.result_index}/{self.section}/{self.record_index}"

    @property
    def expected_scanner_class(self) -> str:
        """The class this record's row must take: its section's, and nothing else."""
        return trivy.SUPPORTED_SECTIONS[self.section]


def section_walk(document: Any) -> list[Record]:
    """Return every supported-section record, in the order ``adapt`` walks them.

    ``Results[]`` in document order, the three sections in the fixed order of
    :data:`trivy.SUPPORTED_SECTIONS` within each element, and each section's elements
    in order.  Section names are taken from that mapping rather than restated, so a
    change to the adapter's section set is visible here rather than silently untested.

    This walk is what makes the ``scanner_class`` expectation independent of the
    adapter: it reads which array each record sits in straight out of the document, so
    the assertion compares the emitted class against the document's own structure and
    not against another reading of the adapter's output.
    """
    found: list[Record] = []
    results = document.get("Results") if isinstance(document, dict) else None
    if not isinstance(results, list):
        return found
    for result_index, element in enumerate(results):
        if not isinstance(element, dict):
            continue
        for section in trivy.SUPPORTED_SECTIONS:
            records = element.get(section)
            if not isinstance(records, list):
                continue
            for record_index, record in enumerate(records):
                found.append(
                    Record(result_index, section, record_index, record, element)
                )
    return found


class Adapted:
    """One document's adaptation: its rows, rejections, counters and severity tally.

    Held together so a test asserts over one measurement rather than adapting the same
    document twice and comparing two.  A count that appears in two assertions must be
    one measurement cited twice (AAP 0.6.4).

    Attributes:
        document: The document adapted, as parsed or derived.
        rows: The dataset rows emitted, each carrying the twelve fields.
        rejections: The :class:`normalize.paths.Rejection` records counted instead.
        counters: The adapter's own counter mapping.
        tally: The :class:`normalize.severity.LiteralTally` fed once per emitted row.
        records: The document's supported-section records, in walk order.
    """

    __slots__ = ("document", "rows", "rejections", "counters", "tally", "records")

    def __init__(
        self,
        document: Any,
        rows: list[dict[str, Any]],
        rejections: list[paths.Rejection],
        counters: dict[str, int],
        tally: severity.LiteralTally,
    ) -> None:
        """Hold one adaptation and re-walk its document for the section expectation."""
        self.document = document
        self.rows = rows
        self.rejections = rejections
        self.counters = counters
        self.tally = tally
        self.records = section_walk(document)

    @property
    def rejections_by_class(self) -> dict[str, int]:
        """Rejection counts per named class, tallied as ``cli.py`` tallies them."""
        by_class: dict[str, int] = {}
        for rejection in self.rejections:
            by_class[rejection.reject_class] = by_class.get(rejection.reject_class, 0) + 1
        return by_class

    @property
    def raw_records(self) -> int:
        """The number of supported-section records the document holds.

        Counted by re-walking the document here rather than taken from the adapter's
        output: this is the left-hand side of the per-artifact identity, and taking it
        from the traversal that built the rows would satisfy the assertion while testing
        nothing (AAP 0.5.4).  The dataset-level identity and its own independent
        traversal, ``reconcile.count_records``, are ``test_reconciliation.py``'s
        subject; what is asserted here is that this adapter emits exactly one outcome
        per record it walked.
        """
        return len(self.records)


def adapt_document(
    document: Any,
    *,
    base_kind: str = BASE_KIND_MERGED,
    section_base: str | None = None,
) -> Adapted:
    """Adapt one document through ``trivy.adapt`` and hold the result.

    The keyword set is the uniform adapter entry point every module in this package
    exposes, so nothing here is Trivy-specific except the base kind a particular
    artifact shape requires and the section base that kind needs.

    A fresh tally per call: the tally is ``severity-map.md``'s input and no assertion
    here depends on state accumulated across documents.
    """
    env = environment()
    tally = severity.LiteralTally.with_all_tools()
    rows, rejections, counters = trivy.adapt(
        document,
        tool=TOOL,
        root=env.root,
        tool_base=env.tool_base(base_kind),
        allowlist=env.globs,
        tally=tally,
        section_base=section_base,
    )
    return Adapted(document, rows, rejections, counters, tally)


def adapt_fixture(stem: str) -> Adapted:
    """Adapt one committed fixture under the reading its own shape requires.

    The merged artifact is read against the recorded ``scan_root`` base.  The one
    unmerged per-directory part is read with a ``per_section_target`` base and a section
    base taken from the document's own ``ArtifactName`` -- the directory the invocation
    that produced it was given.  Both readings come from the recorded metadata and
    neither is inferred from the artifact's content.
    """
    document = load_fixture(f"{stem}.json")
    if stem in PER_SECTION_TARGET_STEMS:
        return adapt_document(
            document,
            base_kind=BASE_KIND_PART,
            section_base=document.get("ArtifactName"),
        )
    return adapt_document(document)


def adapt_through_seam(
    document: Any,
    *,
    designated: tuple[int, str, int],
    section_key: Any,
) -> Adapted:
    """Reproduce ``adapt``'s walk, handing one record a section key of our choosing.

    ``adapt``'s iteration is section-bound by construction -- it chooses one of
    :data:`trivy.SUPPORTED_SECTIONS`, counts the array and passes that section name
    down -- so no record it walks can arrive with a section outside the three.  The
    unattributable-section branch is therefore reached the way the adapter's own
    docstring says it is reached: by calling :func:`trivy.adapt_record` directly.

    Everything else about the walk is ``adapt``'s: pass one runs
    :func:`trivy.validate_finding_sections` over every element first, one counter
    mapping is threaded through, ``records_<section>`` is incremented by the length of
    each array at the moment the array is chosen -- which is what keeps the count unit
    identical to the counting traversal's -- and the element that held no record is
    counted.  Only the designated record's ``section`` argument differs, which is what
    isolates the guard from everything else the adapter does.
    """
    env = environment()
    tool_base = env.tool_base(BASE_KIND_MERGED)
    tally = severity.LiteralTally.with_all_tools()
    counters = trivy.new_counters()

    # Pass one, unchanged: a non-empty unsupported section anywhere would stop the run
    # before a single row existed, so this is a precondition of the expectation rather
    # than a detail.
    trivy.validate_finding_sections(document, tool=TOOL, counters=counters)

    rows: list[dict[str, Any]] = []
    rejections: list[paths.Rejection] = []
    results = document.get("Results")
    for result_index, element in enumerate(
        results if isinstance(results, list) else []
    ):
        counters[trivy.COUNTER_RESULTS] += 1
        if not isinstance(element, dict):
            counters[trivy.COUNTER_RESULTS_SKIPPED_NON_MAPPING] += 1
            continue
        held_a_record = False
        for section in trivy.SUPPORTED_SECTIONS:
            section_records = element.get(section)
            if not isinstance(section_records, list) or not section_records:
                continue
            held_a_record = True
            counters[f"{trivy.COUNTER_RECORDS_PREFIX}{section.lower()}"] += len(
                section_records
            )
            for record_index, record in enumerate(section_records):
                handed = (
                    section_key
                    if (result_index, section, record_index) == designated
                    else section
                )
                outcome = trivy.adapt_record(
                    record,
                    section=handed,
                    target=element.get("Target"),
                    tool=TOOL,
                    root=env.root,
                    tool_base=tool_base,
                    globs=env.globs,
                    tally=tally,
                    counters=counters,
                    packages=element.get("Packages") or (),
                    ecosystem=element.get("Type"),
                    result_index=result_index,
                    record_index=record_index,
                )
                if isinstance(outcome, paths.Rejection):
                    rejections.append(outcome)
                else:
                    rows.append(outcome)
        if not held_a_record:
            counters[trivy.COUNTER_RESULTS_WITHOUT_SUPPORTED_SECTION] += 1
    return Adapted(document, rows, rejections, counters, tally)


def derived(name: str) -> Any:
    """Return a deep copy of one committed fixture, for a variant to be built on.

    Named rather than inlined so that every derivation in this file is visibly a copy.
    A fixture is an input and is never mutated: :func:`load_fixture` re-parses it on
    every call and this returns a copy of that fresh parse, so a variant cannot reach
    the file on disk or another test's document.
    """
    return copy.deepcopy(load_fixture(name))


class TrivyAdapterTestCase(unittest.TestCase):
    """Shared assertions every Trivy test in this module holds a row to.

    The schema invariants are asserted on **every** row of **every** document adapted
    here, not only on the positive fixture's: a row that reached the dataset from a
    negative fixture's surviving records is as much a dataset row as any other.
    """

    maxDiff = None

    def assertRowSchema(self, row: Any, *, where: str) -> None:
        """Assert one row satisfies the twelve-field schema and its two never-absent fields.

        Iterates :data:`normalize.emit.FIELDS` -- the single authored constant -- so no
        thirteenth field, no renamed field and no reordering can pass unnoticed, and
        asserts per field so a failure names the field rather than dumping two dicts.
        """
        self.assertIsInstance(row, dict, msg=f"{where}: a row must be a mapping")
        self.assertEqual(
            list(row),
            list(emit.FIELDS),
            msg=(
                f"{where}: a row carries exactly the twelve fields of emit.FIELDS, in "
                "that order, present-with-None rather than omitted"
            ),
        )
        for field in emit.FIELDS:
            with self.subTest(where=where, field=field):
                value = row[field]
                if field in EXPECTED_OPTIONAL_FIELDS:
                    continue
                self.assertIsNotNone(
                    value,
                    msg=(
                        f"{field} is one of emit.REQUIRED_FIELDS and is never absent; "
                        "absence is permitted only for severity_native, start_line, "
                        "cwe, cve and package_coordinate"
                    ),
                )
        self.assertEqual(row["tool"], TOOL, msg=f"{where}: every row names its tool")
        self.assertIn(
            row["scanner_class"],
            trivy.SCANNER_CLASSES,
            msg=(
                f"{where}: scanner_class is one of the three classes Trivy's sections "
                "map to"
            ),
        )
        self.assertIn(
            row["severity_norm"],
            EXPECTED_SEVERITY_NORM,
            msg=f"{where}: severity_norm is one of the five bands and is never absent",
        )
        self.assertIsInstance(
            row["in_scope"], bool, msg=f"{where}: in_scope is a boolean, not a string"
        )
        start_line = row["start_line"]
        if start_line is not None:
            self.assertIsInstance(
                start_line,
                int,
                msg=f"{where}: start_line is an integer or absent, never a string",
            )
            self.assertNotIsInstance(
                start_line, bool, msg=f"{where}: a boolean is not a line number"
            )
        self.assertRelativePath(row["path"], where=where)

    def assertRelativePath(self, value: Any, *, where: str) -> None:
        """Assert an emitted path is present, relative and expressed against the root.

        No absolute path is ever emitted, *including* for archive members and other
        non-filesystem coordinates (AAP 0.8.2).  ``paths.assert_relative_path`` is the
        emitter's own check and is used here rather than a hand-rolled one, so the two
        cannot drift apart.
        """
        self.assertIsInstance(value, str, msg=f"{where}: path is a string")
        self.assertTrue(value, msg=f"{where}: path is never empty")
        self.assertFalse(
            paths.is_absolute_path(value),
            msg=f"{where}: no emitted path is absolute -- observed {value!r}",
        )
        # Raises PathPolicyError on an absolute path, a drive prefix or a URI scheme.
        paths.assert_relative_path(value)
        self.assertNotIn(
            environment().root,
            value,
            msg=f"{where}: an emitted path never carries the scan root",
        )

    def assertRowsEqualExpected(
        self, rows: list[dict[str, Any]], expected_rows: list[Any], *, where: str
    ) -> None:
        """Assert the emitted rows equal the hand-verified rows, field by field.

        The count first, then every field of every row through
        :data:`normalize.emit.FIELDS` with the row index and field name in the subTest,
        so a failure names exactly which field of which row moved.  Comparing the two
        lists whole would report one opaque mismatch for a single changed character.
        """
        self.assertEqual(
            len(rows),
            len(expected_rows),
            msg=(
                f"{where}: the adapter emitted {len(rows)} row(s) and the hand-verified "
                f"expectation states {len(expected_rows)}"
            ),
        )
        for index, (row, expected_row) in enumerate(zip(rows, expected_rows)):
            self.assertRowSchema(row, where=f"{where} row {index}")
            self.assertEqual(
                list(expected_row),
                list(emit.FIELDS),
                msg=(
                    f"{where} row {index}: the expected row itself carries the twelve "
                    "fields in emit.FIELDS order"
                ),
            )
            for field in emit.FIELDS:
                with self.subTest(where=where, row=index, field=field):
                    self.assertEqual(
                        row[field],
                        expected_row[field],
                        msg=f"{where} row {index}: field {field}",
                    )

    def assertCountersEqualExpected(
        self, adapted: Adapted, expected: Any, *, where: str
    ) -> None:
        """Assert the adapter's counters equal the expected file's, key by key.

        The counter key set is asserted too: it must be exactly
        :data:`trivy.COUNTER_KEYS`, in that order, so a counter added to the adapter
        without an expectation is visible rather than silently unasserted.
        """
        self.assertEqual(
            tuple(adapted.counters),
            trivy.COUNTER_KEYS,
            msg=f"{where}: the counter mapping's keys are exactly trivy.COUNTER_KEYS",
        )
        expected_counters = expected["counters"]
        self.assertEqual(
            sorted(expected_counters),
            sorted(adapted.counters),
            msg=f"{where}: the expected file covers every counter the adapter returns",
        )
        for key in trivy.COUNTER_KEYS:
            with self.subTest(where=where, counter=key):
                self.assertEqual(
                    adapted.counters[key],
                    expected_counters[key],
                    msg=f"{where}: counter {key}",
                )

    def assertOneOutcomePerRecord(self, adapted: Adapted, *, where: str) -> None:
        """Assert every record walked became exactly one row or exactly one rejection.

        The per-artifact half of ``raw finding records = dataset rows + rejected
        records``.  Nothing is dropped and nothing is emitted twice: a filtered record
        would balance no identity, and a duplicated one would be a row the artifact has
        no record for.
        """
        self.assertEqual(
            adapted.raw_records,
            len(adapted.rows) + len(adapted.rejections),
            msg=(
                f"{where}: {adapted.raw_records} record(s) walked must yield "
                f"{len(adapted.rows)} row(s) + {len(adapted.rejections)} rejection(s)"
            ),
        )


class FixtureCorpusTest(TrivyAdapterTestCase):
    """The corpus this module asserts over is present, and its absence is reported.

    Presence is observed rather than assumed.  Every assertion in this module runs over
    a committed fixture, so one that is silently absent would leave the behaviour it
    covers unasserted while the suite stayed green: a missing fixture is a blocking gap
    rather than a test to skip.
    """

    def test_every_fixture_this_module_reads_is_committed(self) -> None:
        """Each required fixture exists, is a file and parses as a JSON object."""
        for name in required_fixture_names():
            with self.subTest(fixture=name):
                location = FIXTURES_DIR / name
                self.assertTrue(
                    location.is_file(),
                    msg=(
                        f"blocking gap: {location} is absent, so the behaviour it "
                        "covers cannot be asserted. Reported, not skipped."
                    ),
                )
                self.assertIsInstance(
                    json.loads(location.read_text(encoding="utf-8")),
                    dict,
                    msg=f"{name}: a Trivy native artifact's top level is an object",
                )

    def test_every_expected_file_this_module_reads_is_committed(self) -> None:
        """Each hand-verified expectation exists and names this adapter."""
        for name in required_expected_names():
            with self.subTest(expected=name):
                location = EXPECTED_DIR / name
                self.assertTrue(
                    location.is_file(),
                    msg=f"blocking gap: {location} is absent. Reported, not skipped.",
                )
                document = json.loads(location.read_text(encoding="utf-8"))
                self.assertEqual(
                    document["adapter"],
                    "harness/lib/normalize/adapters/trivy.py",
                    msg=f"{name}: the expectation names the adapter under test",
                )
                self.assertEqual(
                    document["tool"], TOOL, msg=f"{name}: and the tool under test"
                )

    def test_the_scan_root_is_temporary_empty_and_outside_the_repository(self) -> None:
        """Nothing asserted here can depend on the working checkout or on a scanned tree.

        The pinned tree is cloned outside this repository and is neither built nor
        scanned from here, so a test whose outcome depended on a tree being present would
        pass or fail for a reason that has nothing to do with the adapter.  The root is a
        temporary directory that exists and holds nothing, which is sufficient because no
        code path under test reads the tree.
        """
        env = environment()
        root = Path(env.root)
        self.assertTrue(root.is_absolute(), msg="the root is absolute, as adapt requires")
        self.assertTrue(root.is_dir(), msg="and it exists")
        self.assertEqual(
            list(root.iterdir()), [], msg="and holds no file: resolution reads no tree"
        )
        self.assertNotEqual(
            root, REPO_ROOT, msg="the root is never the repository being worked in"
        )
        self.assertFalse(
            root.is_relative_to(REPO_ROOT),
            msg="nor anywhere inside it, so no assertion can reach a committed file",
        )

    def test_every_rejection_condition_has_its_own_fixture(self) -> None:
        """Every condition this adapter can produce is covered by a committed fixture.

        The mapping is asserted in both directions: every stem has a class and every
        class named is a literal member of :data:`normalize.paths.REJECT_CLASSES`,
        equal to the module's own constant rather than to a string spelled by hand.

        Coverage is asserted as *onto* rather than as a bijection. Three fixtures reach
        ``non_integer_start_line`` -- a boolean, a negative integer and a non-numeric
        ``StartLine`` -- because those are three routes into one class and a corpus with
        one of them cannot show that the adapter treats them alike. What must not happen
        is a class with NO fixture, and that is what the set equality below states.
        """
        self.assertEqual(
            sorted(REJECT_CLASS_BY_STEM),
            sorted(REJECT_FIXTURE_STEMS),
            msg="every negative fixture stem is mapped to exactly one class",
        )
        for stem, reject_class in sorted(REJECT_CLASS_BY_STEM.items()):
            with self.subTest(stem=stem):
                self.assertIn(
                    reject_class,
                    paths.REJECT_CLASSES,
                    msg=f"{reject_class!r} is a member of the closed set of ten",
                )
                self.assertTrue(
                    paths.is_reject_class(reject_class),
                    msg="and paths.py itself recognises it",
                )
        self.assertEqual(
            set(REJECT_CLASS_BY_STEM.values()),
            set(paths.REJECT_CLASSES) - set(UNREACHABLE_REJECT_CLASSES),
            msg="the committed fixtures cover every producible class and no other",
        )


class FixtureIntegrityTest(TrivyAdapterTestCase):
    """Every committed fixture is read and never modified.

    A fixture is the input and the expected file is the assertion; the dependency runs
    one way.  A test that repaired a fixture to make itself pass would have tested the
    fixture rather than the adapter, so the digests taken at module setup are asserted
    again here.
    """

    def test_no_fixture_changed_on_disk_during_this_run(self) -> None:
        """Each fixture's sha256 is what it was when the module was set up."""
        self.assertEqual(
            sorted(_FIXTURE_DIGESTS),
            sorted(required_fixture_names()),
            msg="a digest was taken for every fixture this module reads",
        )
        for name, digest in sorted(_FIXTURE_DIGESTS.items()):
            with self.subTest(fixture=name):
                self.assertEqual(
                    _sha256(FIXTURES_DIR / name),
                    digest,
                    msg=f"{name} was modified during the run; fixtures are read-only",
                )

    def test_loading_a_fixture_twice_yields_independent_documents(self) -> None:
        """Two loads share no object, so a derived variant cannot reach another test."""
        first = load_fixture(POSITIVE_FIXTURE)
        second = load_fixture(POSITIVE_FIXTURE)
        self.assertEqual(first, second, msg="the same file parses to the same document")
        self.assertIsNot(first, second, msg="but not to the same object")
        first["Results"][0]["Target"] = "mutated-in-this-test-only"
        self.assertNotEqual(
            first["Results"][0]["Target"],
            second["Results"][0]["Target"],
            msg="mutating one load leaves the other untouched",
        )
        self.assertEqual(
            load_fixture(POSITIVE_FIXTURE)["Results"][0]["Target"],
            second["Results"][0]["Target"],
            msg="and leaves the file on disk untouched",
        )


class CapturedFixtureProvenanceTest(TrivyAdapterTestCase):
    """The primary positive fixture is this run's own artifact, asserted against it.

    AAP 0.6.2 requires the primary positive fixture to be *an unmodified captured
    excerpt of the tool's own output*, because "a hand-written fixture tests the adapter
    against the shape you believed the tool emits rather than the shape it emits".  A
    digest recorded in the fixture's own expected file cannot establish that: the tree
    owns both files, so the pair is self-consistent whatever the fixture contains.  The
    only external witness is the artifact under ``harness/artifacts/raw/``, and these
    assertions read it.

    ``harness/artifacts/raw/trivy.json`` is 3,496 bytes, so the capture is the whole
    report rather than an excerpt of it -- which makes the strongest form of the claim
    available: identical bytes, hence an identical sha256.  Every weaker structural
    comparison is asserted as well, because byte equality alone would pass vacuously if
    someone later replaced *both* files together, whereas the per-element and per-record
    comparisons state what specifically must survive: each ``Results`` element and each
    ``Misconfigurations`` record as an object, and each envelope member.

    The raw artifact is opened read-only and never written.  Nothing under
    ``harness/artifacts/raw/`` is this module's to modify (AAP 0.3.2), and a missing raw
    artifact is reported as a blocking gap rather than skipped: a skipped provenance
    test is exactly the state this class exists to make impossible.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Locate the raw artifact and read both files once, as bytes and as documents."""
        super().setUpClass()
        cls.raw_location = REPO_ROOT / RAW_ARTIFACT_RELPATH
        cls.fixture_location = FIXTURES_DIR / POSITIVE_FIXTURE

    def raw_bytes(self) -> bytes:
        """Return the raw artifact's bytes, failing loudly if it is absent.

        Reported rather than skipped, and the message names the path so the reader knows
        what to restore.  ``adapter-tests-run.json`` records this module's outcomes, and
        a skip there would read as coverage that never ran.
        """
        if not self.raw_location.is_file():
            self.fail(
                f"blocking gap: the raw artifact {self.raw_location} is absent, so the "
                f"provenance of fixtures/{POSITIVE_FIXTURE} cannot be established "
                "against the tool's own output. AAP 0.6.2 requires the primary positive "
                "fixture to be a capture of that artifact, and this is reported rather "
                "than skipped."
            )
        return self.raw_location.read_bytes()

    def test_the_raw_artifact_this_fixture_is_captured_from_exists(self) -> None:
        """The external witness is present, is a file, and parses as a Trivy report."""
        data = self.raw_bytes()
        self.assertGreater(len(data), 0, msg="the raw artifact is not an empty file")
        document = json.loads(data.decode("utf-8"))
        self.assertIsInstance(
            document, dict, msg="a Trivy native artifact's top level is an object"
        )
        self.assertIn(
            "Results",
            document,
            msg="and the artifact carries the Results member the count unit is taken from",
        )

    def test_the_captured_fixture_is_the_raw_artifact_byte_for_byte(self) -> None:
        """Identical bytes and therefore an identical sha256: the strongest claim.

        Not "an excerpt that agrees in the parts we checked" -- the same file.  A single
        byte of difference fails here, including a re-indentation or an added trailing
        newline, either of which would make the fixture an edited document rather than a
        capture.
        """
        raw = self.raw_bytes()
        fixture = self.fixture_location.read_bytes()
        self.assertEqual(
            len(fixture),
            len(raw),
            msg=(
                f"fixtures/{POSITIVE_FIXTURE} is {len(fixture)} bytes and "
                f"{RAW_ARTIFACT_RELPATH} is {len(raw)}; a capture is the same length"
            ),
        )
        self.assertEqual(
            hashlib.sha256(fixture).hexdigest(),
            hashlib.sha256(raw).hexdigest(),
            msg="the two digests are equal, so the fixture is the artifact",
        )
        self.assertEqual(fixture, raw, msg="and the bytes themselves are equal")

    def test_the_expected_file_records_the_raw_artifacts_own_digest(self) -> None:
        """The recorded provenance names the artifact and its real digest.

        The digest in the expected file is checked against the *raw artifact*, not only
        against the fixture, so the recorded provenance is verified against the external
        witness rather than against the tree's own copy of it.
        """
        raw = self.raw_bytes()
        block = load_expected(POSITIVE_EXPECTED_STEM)["fixture"]
        self.assertEqual(block["kind"], "captured")
        self.assertEqual(block["captured_from"], RAW_ARTIFACT_RELPATH)
        self.assertEqual(
            block["sha256"],
            hashlib.sha256(raw).hexdigest(),
            msg="the recorded sha256 is the raw artifact's",
        )
        self.assertEqual(
            block["bytes"], len(raw), msg="and the recorded byte count is its length"
        )

    def test_every_results_element_is_the_raw_artifacts_element_object_for_object(
        self,
    ) -> None:
        """Element by element, compared canonically rather than by dict identity.

        ``json.dumps(..., sort_keys=True)`` makes the comparison independent of member
        order, so a fixture that reordered members while keeping content would still be
        recognised as carrying the same records -- and one that changed a Target, a
        Severity or a MisconfSummary would not.
        """
        raw = json.loads(self.raw_bytes().decode("utf-8"))
        fixture = load_fixture(POSITIVE_FIXTURE)
        self.assertEqual(
            len(fixture["Results"]),
            len(raw["Results"]),
            msg="the capture carries every Results element the artifact does",
        )
        for index, (captured, original) in enumerate(
            zip(fixture["Results"], raw["Results"])
        ):
            with self.subTest(result_index=index):
                self.assertEqual(
                    json.dumps(captured, sort_keys=True),
                    json.dumps(original, sort_keys=True),
                    msg=f"/Results/{index} is the artifact's element unchanged",
                )

    def test_every_misconfiguration_record_is_the_raw_artifacts_record(self) -> None:
        """Record by record, the unit reconciliation counts, compared canonically.

        The element comparison above already covers these, and they are asserted again
        on their own because the record is the count unit: this is the assertion that
        fails by name if a finding record were added, dropped or edited inside an
        element that otherwise still matched.
        """
        raw = json.loads(self.raw_bytes().decode("utf-8"))
        fixture = load_fixture(POSITIVE_FIXTURE)
        section = "Misconfigurations"
        pairs = 0
        for index, (captured, original) in enumerate(
            zip(fixture["Results"], raw["Results"])
        ):
            captured_records = captured.get(section) or []
            original_records = original.get(section) or []
            with self.subTest(result_index=index):
                self.assertEqual(
                    len(captured_records),
                    len(original_records),
                    msg=f"/Results/{index}/{section} holds the artifact's record count",
                )
            for record_index, (one, other) in enumerate(
                zip(captured_records, original_records)
            ):
                pairs += 1
                with self.subTest(result_index=index, record_index=record_index):
                    self.assertEqual(
                        json.dumps(one, sort_keys=True),
                        json.dumps(other, sort_keys=True),
                        msg=(
                            f"/Results/{index}/{section}/{record_index} is the "
                            "artifact's record unchanged"
                        ),
                    )
        self.assertEqual(
            pairs,
            load_expected(POSITIVE_EXPECTED_STEM)["counts"]["raw_finding_records"],
            msg=(
                "and the number of records compared is the count unit the expectation "
                "reconciles against, so no record escaped this comparison"
            ),
        )

    def test_every_envelope_member_is_the_raw_artifacts(self) -> None:
        """The report envelope too: schema version, tool version, id, time, artifact.

        The envelope is what a reader uses to tell one run's artifact from another's, so
        an authored fixture is most easily recognised by an envelope that belongs to no
        run.  Each member is compared individually, and the member set is compared as a
        whole so neither file may carry a top-level member the other does not.
        """
        raw = json.loads(self.raw_bytes().decode("utf-8"))
        fixture = load_fixture(POSITIVE_FIXTURE)
        self.assertEqual(
            sorted(fixture),
            sorted(raw),
            msg="the two documents carry exactly the same top-level members",
        )
        for member in sorted(member for member in raw if member != "Results"):
            with self.subTest(member=member):
                self.assertEqual(
                    json.dumps(fixture[member], sort_keys=True),
                    json.dumps(raw[member], sort_keys=True),
                    msg=f"the envelope member {member} is the artifact's",
                )
        recorded = load_expected(POSITIVE_EXPECTED_STEM)["fixture"]
        self.assertEqual(recorded["schema_version"], raw["SchemaVersion"])
        self.assertEqual(recorded["trivy_version"], raw["Trivy"]["Version"])
        self.assertEqual(recorded["report_id"], raw["ReportID"])
        self.assertEqual(recorded["created_at"], raw["CreatedAt"])
        self.assertEqual(recorded["artifact_name"], raw["ArtifactName"])
        self.assertEqual(recorded["artifact_type"], raw["ArtifactType"])

    def test_the_derived_fixture_is_declared_derived_and_is_not_the_raw_artifact(
        self,
    ) -> None:
        """The companion document states what it is, and is provably not a capture.

        The two roles are separated rather than traded off: the capture carries the
        provenance and the derived document carries the feature cases the raw artifact
        does not contain.  This asserts the second half of that -- the derived file is
        declared derived, names what it was derived from, and is not the artifact -- so
        no reader can take it for a capture, and the module cannot quietly drift back to
        asserting provenance against an authored document.
        """
        raw = self.raw_bytes()
        derived_bytes = (FIXTURES_DIR / FEATURES_FIXTURE).read_bytes()
        self.assertNotEqual(
            derived_bytes, raw, msg="the derived document is not the raw artifact"
        )
        self.assertNotEqual(
            hashlib.sha256(derived_bytes).hexdigest(),
            hashlib.sha256(raw).hexdigest(),
            msg="and does not share its digest",
        )
        block = load_expected(FEATURES_EXPECTED_STEM)["fixture"]
        self.assertEqual(
            block["kind"], "derived", msg="its expected file declares it derived"
        )
        provenance = block["derived_from"]
        self.assertIsInstance(
            provenance,
            dict,
            msg=(
                "and states its provenance as separately checkable parts rather than "
                "as one sentence"
            ),
        )
        for part in ("shape_source", "record_source", "history", "not_a_capture"):
            with self.subTest(part=part):
                self.assertIsInstance(provenance[part], str)
                self.assertTrue(
                    provenance[part].strip(),
                    msg=f"the {part} half of the provenance is stated",
                )
        self.assertIn(
            RAW_ARTIFACT_RELPATH,
            provenance["shape_source"],
            msg="the shape it was modelled on names the raw artifact",
        )
        self.assertIn(
            RAW_ARTIFACT_RELPATH,
            provenance["record_source"],
            msg="and the record provenance states the records are not from it",
        )
        self.assertEqual(
            block["sha256"],
            hashlib.sha256(derived_bytes).hexdigest(),
            msg="and records its own digest, which is not the artifact's",
        )
        self.assertNotIn(
            "captured_from",
            block,
            msg=(
                "and claims no capture: a derived document that also claimed to be "
                "captured would be the provenance defect this split removed"
            ),
        )

    def test_the_capture_carries_the_sections_the_expectation_says_it_does(self) -> None:
        """What the artifact does and does not contain, asserted rather than assumed.

        The features fixture exists because the raw artifact has no ``Vulnerabilities``
        and no ``Secrets`` section.  If a later run's artifact did carry them, this
        assertion fails and the split would be reconsidered on evidence -- rather than
        the module continuing to route feature assertions at a derived document for a
        reason that had stopped being true.
        """
        raw = json.loads(self.raw_bytes().decode("utf-8"))
        observed = {section: 0 for section in trivy.SUPPORTED_SECTIONS}
        for element in raw["Results"]:
            for section in trivy.SUPPORTED_SECTIONS:
                value = element.get(section)
                if isinstance(value, list):
                    observed[section] += len(value)
        self.assertEqual(
            observed,
            load_expected(POSITIVE_EXPECTED_STEM)["fixture"]["records_by_section"],
            msg="the per-section record split is the expectation's, read from the artifact",
        )
        self.assertEqual(
            observed["Vulnerabilities"],
            0,
            msg=(
                "the raw artifact carries no vulnerability record, which is why the "
                "coordinate and CVSS cases run against the derived features document"
            ),
        )
        self.assertEqual(
            observed["Secrets"],
            0,
            msg="and no secret record, which is why redaction is asserted there too",
        )


class ContractConstantsTest(TrivyAdapterTestCase):
    """The constants the assertions below key on are the modules' own, and agree.

    Every assertion in this file reads its section names, field order, classes and
    rejection vocabulary from a module constant rather than restating them.  These
    tests are what make that worth doing: they establish that the constants say what
    the assertions assume, so a change to one of them fails here with its own message
    instead of surfacing as a puzzling row mismatch twenty tests later.
    """

    def test_the_field_order_is_emits_and_the_adapter_agrees_with_it(self) -> None:
        """``emit.FIELDS`` is the twelve fields in order, and ``trivy.FIELDS`` matches.

        The adapter cannot import ``emit`` -- each adapter depends on ``paths`` and
        ``severity`` and on nothing else (AAP 0.6.4) -- so its copy must agree by
        construction, and this is where that is checked.
        """
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
            msg="the twelve fields, in the request's order (AAP 0.8.2)",
        )
        self.assertEqual(
            trivy.FIELDS,
            emit.FIELDS,
            msg="the adapter's authored copy agrees with emit.py's constant",
        )
        self.assertEqual(
            emit.OPTIONAL_FIELDS,
            EXPECTED_OPTIONAL_FIELDS,
            msg="absence is permitted for exactly five fields",
        )
        self.assertEqual(
            trivy.ABSENCE_PERMITTED_FIELDS,
            EXPECTED_OPTIONAL_FIELDS,
            msg="and the adapter's copy of that set agrees",
        )
        self.assertNotIn(
            "path", emit.OPTIONAL_FIELDS, msg="path is never absent (AAP 0.5.4)"
        )
        self.assertNotIn(
            "severity_norm", emit.OPTIONAL_FIELDS, msg="nor is severity_norm"
        )

    def test_the_three_supported_sections_map_to_the_three_classes(self) -> None:
        """``SUPPORTED_SECTIONS`` is the section-to-class mapping, in walk order."""
        self.assertEqual(
            dict(trivy.SUPPORTED_SECTIONS),
            {
                "Vulnerabilities": "vuln",
                "Secrets": "secret",
                "Misconfigurations": "misconfig",
            },
            msg="AAP 0.5.4's class table for the one tool whose class is per record",
        )
        self.assertEqual(
            tuple(trivy.SUPPORTED_SECTIONS),
            ("Vulnerabilities", "Secrets", "Misconfigurations"),
            msg="the insertion order is the iteration order and therefore the row order",
        )
        self.assertEqual(
            trivy.SCANNER_CLASSES,
            ("vuln", "secret", "misconfig"),
            msg="derived from the mapping so it cannot drift from it",
        )

    def test_the_class_is_resolved_from_a_section_name_and_nothing_else(self) -> None:
        """``scanner_class_for_section`` answers for the three and refuses everything else.

        The resolver is a total function of the section **name**: it takes no record, so
        there is no record content it could consult.  Anything outside the three -- a
        non-finding member, an unsupported finding section's name, a misspelling, a
        non-string -- yields ``None``, which is what becomes an ``unattributable_section``
        rejection rather than a guessed class.
        """
        for section, expected_class in trivy.SUPPORTED_SECTIONS.items():
            with self.subTest(section=section):
                self.assertEqual(
                    trivy.scanner_class_for_section(section), expected_class
                )
        for foreign in (
            SEAM_SECTION_KEY,
            *trivy.UNSUPPORTED_FINDING_SECTIONS,
            "vulnerabilities",
            "Vulnerability",
            "",
            None,
            7,
            ["Vulnerabilities"],
        ):
            with self.subTest(section=foreign):
                self.assertIsNone(
                    trivy.scanner_class_for_section(foreign),
                    msg="a section outside the three establishes no class",
                )

    def test_there_is_no_module_level_scanner_class_constant(self) -> None:
        """The adapter exposes no fixed class, unlike every sibling adapter.

        A single ``SCANNER_CLASS`` constant here would be the very defect the
        section-bound design exists to prevent, so its absence is asserted rather than
        assumed: it is the structural reason a content sniff has nothing to fall back on.
        """
        self.assertFalse(
            hasattr(trivy, "SCANNER_CLASS"),
            msg=(
                "Trivy's class is per record, taken from the section array; a "
                "module-level constant would contradict that by construction"
            ),
        )

    def test_the_unsupported_finding_sections_are_the_two_report_go_declares(self) -> None:
        """``Licenses`` and ``ExperimentalModifiedFindings``, and they are not supported.

        Both are members of Trivy 0.74.0's ``Result`` struct, so both are shapes this
        artifact can legitimately carry -- which is exactly why each is validated empty
        rather than ignored.
        """
        self.assertEqual(
            trivy.UNSUPPORTED_FINDING_SECTIONS,
            ("Licenses", "ExperimentalModifiedFindings"),
            msg="the two finding sections 0.74.0 can emit that this dataset drops",
        )
        for section in trivy.UNSUPPORTED_FINDING_SECTIONS:
            with self.subTest(section=section):
                self.assertNotIn(
                    section,
                    trivy.SUPPORTED_SECTIONS,
                    msg="an unsupported section is not one of the three",
                )
                self.assertIn(
                    section,
                    trivy.RESULT_KNOWN_KEYS,
                    msg="but it is a known Result member, not an unknown key",
                )

    def test_the_rejection_vocabulary_and_the_stop_reasons_are_disjoint(self) -> None:
        """A stop is not a rejection, and no name belongs to both vocabularies.

        The nearest neighbours in this adapter's contract are one line apart: a record
        no section claims is a counted rejection under ``unattributable_section``, while
        a section structure that cannot be read at all stops the run.  A test that
        accepted either for either would have stopped testing the distinction.
        """
        self.assertTrue(
            set(trivy.HALT_REASONS).isdisjoint(paths.REJECT_CLASSES),
            msg="no stop reason is a rejection class, and no rejection class is a reason",
        )
        self.assertIn(
            trivy.HALT_UNSUPPORTED_SECTION,
            trivy.HALT_REASONS,
            msg="the unsupported-section reason is one of the four",
        )
        self.assertNotIn(
            trivy.HALT_UNSUPPORTED_SECTION,
            paths.REJECT_CLASSES,
            msg="and it is not a class anything may be counted under",
        )
        self.assertIn(
            paths.REJECT_UNATTRIBUTABLE_SECTION,
            paths.REJECT_CLASSES,
            msg="while its nearest neighbour is a counted class",
        )
        self.assertTrue(
            issubclass(trivy.UnsupportedTrivySection, Exception),
            msg="the stop is an Exception",
        )
        self.assertFalse(
            issubclass(trivy.UnsupportedTrivySection, trivy.TrivyAdapterError),
            msg=(
                "and deliberately not the module's caller-fault error: a caller "
                "catching argument faults must not be able to swallow it"
            ),
        )

    def test_the_allowlist_loaded_from_this_test_is_the_authoritative_twelve(self) -> None:
        """The twelve globs written here are the twelve ``paths.py`` authors, in order.

        Written independently and read back through the real loader, so the comparison
        is between the request's globs and the module's -- not between the module's copy
        and itself.
        """
        env = environment()
        self.assertEqual(
            env.globs, AUTHORITATIVE_GLOBS, msg="the loader preserved them byte-exact"
        )
        self.assertTrue(
            paths.allowlist_matches_authoritative_globs(env.globs),
            msg="and they are the twelve authoritative globs paths.py states",
        )
        self.assertEqual(len(env.globs), 12, msg="twelve, neither widened nor narrowed")
        self.assertNotIn(
            paths.SRC_TEST_MARKER,
            "".join(env.globs),
            msg="the allowlist carries no exclusion line; the exclusion is paths.py's",
        )

    def test_the_recorded_path_bases_are_the_two_kinds_the_metadata_declares(self) -> None:
        """Both readings load through ``paths.py`` and behave as their kind requires."""
        env = environment()
        merged = env.tool_base(BASE_KIND_MERGED)
        part = env.tool_base(BASE_KIND_PART)
        self.assertEqual(merged.tool, TOOL)
        self.assertEqual(merged.kind, paths.PATH_BASE_KIND_SCAN_ROOT)
        self.assertEqual(
            merged.base_for_relative(),
            env.root,
            msg="the merged artifact's Targets anchor on the scan root",
        )
        self.assertTrue(merged.has_explicit_base)
        self.assertEqual(part.kind, paths.PATH_BASE_KIND_PER_SECTION_TARGET)
        self.assertIsNone(
            part.base_for_relative(),
            msg=(
                "a per-directory part supplies no single base, which is why a relative "
                "per-record path under it is a counted rejection rather than a silent "
                "reading against the root"
            ),
        )
        self.assertEqual(
            part.scan_root, env.root, msg="the root is still recorded for the part"
        )


class CallerContractTest(TrivyAdapterTestCase):
    """The six argument validators, one precise negative each, plus a passing control.

    :func:`trivy.adapt` runs six validators before it walks anything, and every one
    raises :class:`trivy.TrivyAdapterError` rather than returning a sentinel.  Each is
    asserted here with the fault it names quoted from the raised message, because a bare
    ``assertRaises`` would pass on any of the six -- and on a ``TypeError`` from a
    keyword typo -- while telling a reader nothing about which contract was broken.

    A caller fault is deliberately **not** a rejection.  A rejection describes a
    defective record inside an artifact and is counted and carried past; these describe a
    defective *call* or a structurally impossible artifact, and they stop the caller.  So
    every test here asserts that nothing was returned at all, and none of these faults
    appears in any rejection count anywhere in this module.

    The control matters as much as the negatives.  ``test_a_well_formed_call_raises
    _nothing`` runs the same argument set with no substitution, so each negative is
    attributable to the one value it changed rather than to the way this class assembles
    a call.

    One documented non-error is covered too: over a document whose ``Results`` is absent
    or null this adapter counts ``results_absent_or_null`` and must not raise.  That is
    defence in depth for a direct call rather than a routed case -- ``shape.py``'s
    envelope refuses both documents, so neither arrives here through ``shape.route`` --
    and it sits beside the negative for ``Results`` present as a non-array, which must
    raise.  The pair is what shows the line is drawn where ``_validated_document``'s
    docstring says it is.
    """

    #: The six validators the public entry runs, in the order it runs them. Iterated by
    #: the closed-set test so a seventh validator added to ``adapt`` arrives with a
    #: failure naming it rather than silently untested.
    VALIDATORS = (
        "_validated_tool",
        "_validated_root",
        "_validated_tool_base",
        "_validated_allowlist",
        "_validated_tally",
        "_validated_document",
    )

    def well_formed(self) -> dict[str, Any]:
        """Every argument :func:`trivy.adapt` requires, all of them valid.

        Built fresh per call: a tally accumulates, and a test that substituted one
        argument must not inherit another test's recorder.
        """
        env = environment()
        return {
            "doc": load_fixture(POSITIVE_FIXTURE),
            "tool": TOOL,
            "root": env.root,
            "tool_base": env.tool_base(),
            "allowlist": env.globs,
            "tally": severity.LiteralTally.with_all_tools(),
        }

    def call(self, **overrides: Any) -> Any:
        """Call ``trivy.adapt`` with the well-formed set and the overrides applied."""
        arguments = self.well_formed()
        arguments.update(overrides)
        document = arguments.pop("doc")
        return trivy.adapt(document, **arguments)

    def assertRefused(self, fragments: tuple[str, ...], **overrides: Any) -> str:
        """Assert the call raises ``TrivyAdapterError`` naming the fault, and return it.

        The fragments are asserted individually so a failure says which part of the
        message was missing, and the message is asserted non-trivial so a validator that
        raised with an empty string could not pass.
        """
        with self.assertRaises(trivy.TrivyAdapterError) as caught:
            self.call(**overrides)
        message = str(caught.exception)
        self.assertGreater(
            len(message.strip()),
            20,
            msg="a caller fault has to say what was wrong in words",
        )
        for fragment in fragments:
            self.assertIn(
                fragment,
                message,
                msg=f"the message must name the fault: {fragment!r} missing",
            )
        self.assertIsInstance(caught.exception, ValueError)
        self.assertNotIsInstance(
            caught.exception,
            trivy.UnsupportedTrivySection,
            msg=(
                "an argument fault is not a structural halt: the two are caught by "
                "different callers and mean different things"
            ),
        )
        return message

    def test_a_well_formed_call_raises_nothing(self) -> None:
        """The control. Without it every negative below could be an assembly error."""
        rows, rejections, counters = self.call()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rejections, [])
        self.assertEqual(set(counters), set(trivy.COUNTER_KEYS))
        self.assertEqual(counters[trivy.COUNTER_RESULTS], 3)
        self.assertEqual(counters[trivy.COUNTER_RESULTS_ABSENT], 0)

    def test_validated_tool_refuses_a_non_string_and_another_tools_identifier(
        self,
    ) -> None:
        """The tool must be a string, and must be this adapter's own identifier.

        ``cli.py``'s registry calls every adapter with the same keyword set, so a
        mis-keyed registry entry has to fail on the call rather than stamp ``trivy`` onto
        another tool's records -- which would be invisible in the dataset, since the row
        shape is identical.
        """
        for label, value in (
            ("null", None),
            ("number", 7),
            ("list", [TOOL]),
            ("bytes", b"trivy"),
        ):
            with self.subTest(shape=label):
                self.assertRefused(
                    ("tool must be a canonical tool identifier string", "observed"),
                    tool=value,
                )

        for other in ("checkov", "gitleaks", "dependency-check", "Trivy", "trivy "):
            with self.subTest(tool=other):
                message = self.assertRefused(
                    (repr(other), "is not the tool this adapter serves", repr(TOOL)),
                    tool=other,
                )
                self.assertIn(
                    "One adapter per",
                    message,
                    msg="and says why one adapter per native shape is the rule",
                )

    def test_validated_root_refuses_a_non_path_bytes_empty_and_relative_root(
        self,
    ) -> None:
        """Four faults, four messages, and the relative root is the dangerous one.

        A relative root cannot anchor anything, and accepting one would produce a
        plausible-looking wrong path for every row rather than an error anybody notices.
        """
        for label, value in (("number", 7), ("null", None), ("list", ["/tmp"])):
            with self.subTest(shape=label):
                self.assertRefused(
                    (
                        "root must be a str or an os.PathLike naming the SPARK_SRC root",
                        "observed",
                    ),
                    root=value,
                )

        self.assertRefused(
            ("root must be a text path, not bytes", "guess an encoding"),
            root=b"/tmp/spark-src",
        )
        self.assertRefused(("root must not be empty",), root="")
        for relative in ("spark-src", "./spark-src", "../spark-src"):
            with self.subTest(root=relative):
                self.assertRefused(
                    (
                        "root must be an absolute path to express a reported path "
                        "against",
                        repr(relative),
                    ),
                    root=relative,
                )

        # And a PathLike is accepted, so the refusals above are about the value rather
        # than about the type the caller happened to use.
        rows, rejections, _ = self.call(root=Path(environment().root))
        self.assertEqual(len(rows), 3)
        self.assertEqual(rejections, [])

    def test_validated_tool_base_refuses_a_non_base_and_another_tools_base(self) -> None:
        """The base is the per-tool view over the runner metadata, and it is checked.

        Handing this adapter another tool's recorded base would resolve every path
        against the wrong base while every row still looked well-formed -- exactly the
        failure AAP 0.5.4's "every base taken from the recorded runner metadata"
        prevents, and one no field-level assertion could catch.
        """
        env = environment()
        for label, value in (
            ("null", None),
            ("string", env.root),
            ("dict", {"kind": BASE_KIND_MERGED, "value": env.root}),
        ):
            with self.subTest(shape=label):
                self.assertRefused(
                    (
                        "tool_base must be a paths.ToolPathBase built from the runner "
                        "metadata",
                        "observed",
                    ),
                    tool_base=value,
                )

        for other in ("checkov", "gitleaks", "opengrep"):
            with self.subTest(tool=other):
                foreign = dataclasses.replace(env.tool_base(), tool=other)
                self.assertEqual(foreign.tool, other)
                message = self.assertRefused(
                    (f"tool_base names {other!r}", f"but the artifact is {TOOL!r}"),
                    tool_base=foreign,
                )
                self.assertIn("wrong path for every row", message)

    def test_validated_allowlist_refuses_a_string_a_non_iterable_and_a_bad_glob(
        self,
    ) -> None:
        """A single string, a non-iterable, and an entry that is not a non-empty string.

        The string case is the one worth having: a string is iterable, so it would be
        consumed character by character and every row would silently take
        ``in_scope: false`` with nothing raised at all.
        """
        for label, value in (("str", "core/src/main/**"), ("bytes", b"core/**")):
            with self.subTest(shape=label):
                self.assertRefused(
                    (
                        "allowlist must be an iterable of glob strings, not a single "
                        "string",
                        "character by character",
                    ),
                    allowlist=value,
                )

        for label, value in (("null", None), ("number", 7)):
            with self.subTest(shape=label):
                self.assertRefused(
                    (
                        "allowlist must be an iterable of glob strings from "
                        "paths.load_allowlist()",
                        "observed",
                    ),
                    allowlist=value,
                )

        globs = list(environment().globs)
        for index, bad in ((0, ""), (1, None), (2, 7), (len(globs) - 1, ["core/**"])):
            with self.subTest(entry=index, value=bad):
                broken = list(globs)
                broken[index] = bad
                self.assertRefused(
                    (
                        f"allowlist entry {index} must be a non-empty glob string",
                        repr(bad),
                    ),
                    allowlist=broken,
                )

        # A generator is materialised rather than exhausted by the first row: the check
        # that makes the tuple() in the validator load-bearing.
        rows, rejections, _ = self.call(allowlist=(glob for glob in globs))
        self.assertEqual(len(rows), 3)
        self.assertEqual(rejections, [])
        self.assertTrue(all(row["in_scope"] for row in rows))

    def test_validated_tally_refuses_anything_that_cannot_record(self) -> None:
        """Capability, not class: a double with a callable ``record`` is accepted.

        ``None`` is refused because every row's native literal has to reach
        ``severity-map.md``, and a silently skipped tally would leave that document
        under-reporting with nothing to show it had.
        """
        for label, value in (
            ("null", None),
            ("number", 7),
            ("string", "tally"),
            ("dict", {"record": "not callable"}),
            ("object with a non-callable record", type("T", (), {"record": 3})()),
        ):
            with self.subTest(shape=label):
                self.assertRefused(
                    (
                        "tally must expose a callable record(tool, result)",
                        "severity.LiteralTally",
                        "observed",
                    ),
                    tally=value,
                )

        class Recorder:
            """A minimal double: the capability and nothing else."""

            def __init__(self) -> None:
                self.calls: list[tuple[str, Any]] = []

            def record(self, tool: str, result: Any) -> None:
                self.calls.append((tool, result))

        double = Recorder()
        rows, rejections, _ = self.call(tally=double)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rejections, [])
        self.assertEqual(
            len(double.calls),
            len(rows),
            msg="one record(tool, result) per emitted row, and the double received them",
        )
        self.assertEqual({tool for tool, _ in double.calls}, {TOOL})

    def test_validated_document_refuses_a_non_object_top_level(self) -> None:
        """``report.go``'s ``Report`` is a struct, so a bare array or scalar is a mis-route.

        Shape detection belongs to ``shape.py``; this adapter owns the structural
        validation of what it walks, and says so in the message.
        """
        for label, value in (
            ("array", [{"Results": []}]),
            ("string", "{}"),
            ("number", 2),
            ("null", None),
            ("boolean", True),
        ):
            with self.subTest(shape=label):
                message = self.assertRefused(
                    ("a Trivy report's top level is an object", "observed"),
                    doc=value,
                )
                self.assertIn(
                    "Shape detection belongs to shape.py",
                    message,
                    msg="the message names where shape detection does belong",
                )

    def test_validated_document_refuses_results_present_as_a_non_array(self) -> None:
        """Present-but-not-an-array is refused; absent or null is not.

        Counting a non-array ``Results`` as zero records would agree with
        ``reconcile._count_trivy`` and reconcile cleanly while reporting a malformed
        artifact as a clean scan -- and an empty result set is indistinguishable from a
        clean scan.  That is why this one is a raise rather than a count.
        """
        for label, value in (
            ("object", {"0": {"Target": "core/src/main/scala/x.scala"}}),
            ("string", "[]"),
            ("number", 3),
            ("boolean", False),
        ):
            with self.subTest(shape=label):
                document = derived(POSITIVE_FIXTURE)
                document["Results"] = value
                message = self.assertRefused(("not an array",), doc=document)
                self.assertIn("Results", message)
                self.assertIn(
                    "reconcile cleanly while reporting a malformed artifact as a clean "
                    "scan",
                    message,
                    msg="and states the failure mode the refusal prevents",
                )

    def test_results_absent_or_null_is_counted_and_never_raised(self) -> None:
        """The documented non-error, asserted as an outcome rather than as an absence.

        Defence in depth rather than a routed case.  ``shape.py``'s envelope requires
        ``Results`` to be present **and** to be a JSON array -- AAP 0.5.4 names the count
        unit ``Results[]`` and halts on an artifact matching no known native shape rather
        than best-effort parsing it -- so a ``trivy.json`` whose ``Results`` is absent or
        null never reaches this adapter through ``shape.route``; that refusal is owned by
        ``fixtures/near-trivy-results-null.json`` and its expected file.  What is asserted
        here is the direct call: this adapter still returns rather than raising, and it
        counts the absence so it is visible in ``normalize-run.json`` rather than
        indistinguishable from a report nobody read.  The empty **array** below is the
        ordinary routed case, and it is a different count.
        """
        for label, prepare in (
            ("absent", lambda document: document.pop("Results", None)),
            ("null", lambda document: document.__setitem__("Results", None)),
        ):
            with self.subTest(shape=label):
                document = derived(POSITIVE_FIXTURE)
                prepare(document)
                rows, rejections, counters = self.call(doc=document)
                self.assertEqual(rows, [])
                self.assertEqual(rejections, [])
                self.assertEqual(
                    counters[trivy.COUNTER_RESULTS_ABSENT],
                    1,
                    msg="counted, so the emptiness is reported rather than inferred",
                )
                self.assertEqual(counters[trivy.COUNTER_RESULTS], 0)
                self.assertEqual(
                    reconcile.count_records(TOOL, document),
                    0,
                    msg="and the independent traversal agrees: nothing was dropped",
                )

        # An empty array is a third ordinary case, and is not the absent one.
        document = derived(POSITIVE_FIXTURE)
        document["Results"] = []
        rows, rejections, counters = self.call(doc=document)
        self.assertEqual((rows, rejections), ([], []))
        self.assertEqual(
            counters[trivy.COUNTER_RESULTS_ABSENT],
            0,
            msg="a stated empty array is not an absent member and is not counted as one",
        )

    def test_every_validator_the_entry_runs_has_a_negative_here(self) -> None:
        """The validator set is closed, and each one is reachable through the public entry.

        The declared set is read off the module rather than listed by hand, so a seventh
        validator added to ``adapt`` arrives with a failure naming it instead of being
        untested: a hand-kept list is a list nothing updates.  The invocation order is
        asserted against ``adapt``'s own source for the same reason -- a negative for a
        later validator can only be attributed to it if no earlier one fires first.
        """
        source = Path(trivy.__file__).read_text(encoding="utf-8")
        declared = {
            name
            for name in dir(trivy)
            if name.startswith("_validated_") and callable(getattr(trivy, name))
        }
        self.assertEqual(
            declared,
            set(self.VALIDATORS),
            msg="every _validated_* helper the module defines is covered by this class",
        )
        for name in self.VALIDATORS:
            with self.subTest(validator=name):
                validator = getattr(trivy, name)
                self.assertTrue(callable(validator))
                self.assertTrue(
                    (validator.__doc__ or "").strip(),
                    msg="each states its contract",
                )
                self.assertIn(
                    f"{name}(",
                    source,
                    msg="and is called rather than merely defined",
                )

        # Every one is invoked by the public entry, in the order it appears here.
        entry = source.split("def adapt(", 1)[1]
        positions = [entry.index(f"{name}(") for name in self.VALIDATORS]
        self.assertEqual(
            positions,
            sorted(positions),
            msg=(
                "the order asserted here is the order adapt runs them, so a negative "
                "for a later validator cannot be masked by an earlier one"
            ),
        )

    def test_no_caller_fault_is_ever_absorbed_into_a_rejection(self) -> None:
        """A ``TrivyAdapterError`` is raised, never counted, and returns nothing.

        The two outcomes are not interchangeable: a rejection leaves the run going with
        a counted record, and a caller fault stops it.  Asserting that no committed
        fixture's rejections carry a caller-fault class is what keeps the boundary from
        eroding into a catch-all.
        """
        for stem in REJECT_FIXTURE_STEMS:
            adapted = adapt_negative(stem)
            for rejection in adapted.rejections:
                with self.subTest(fixture=stem, reject_class=rejection.reject_class):
                    self.assertIn(rejection.reject_class, paths.REJECT_CLASSES)

        # And the raise really does return nothing: no partial row list escapes.
        captured: Any = "untouched"
        try:
            captured = self.call(tool="checkov")
        except trivy.TrivyAdapterError:
            pass
        self.assertEqual(
            captured,
            "untouched",
            msg="a refused call assigns nothing, so no partial result can be used",
        )
        self.assertFalse(
            issubclass(trivy.TrivyAdapterError, trivy.UnsupportedTrivySection),
            msg="the caller fault is not a halt",
        )
        self.assertFalse(
            issubclass(trivy.UnsupportedTrivySection, trivy.TrivyAdapterError),
            msg=(
                "and the halt is not a caller fault. The two hierarchies are disjoint on "
                "purpose, as the halt's own docstring states: a caller catching argument "
                "faults must not be able to swallow a condition the run is required to "
                "stop on"
            ),
        )
        self.assertTrue(issubclass(trivy.TrivyAdapterError, ValueError))
        self.assertTrue(issubclass(trivy.UnsupportedTrivySection, Exception))
        self.assertFalse(
            issubclass(trivy.UnsupportedTrivySection, ValueError),
            msg=(
                "nor is the halt a ValueError: catching argument errors broadly must not "
                "reach it either"
            ),
        )


class PositiveMappingContract:
    """The field-for-field contract every positive fixture is held to.

    A mixin rather than a ``TestCase``, so ``unittest`` collects it only through the
    two concrete classes below and no assertion runs twice under a name that hides
    which document it ran against.  Each concrete class names its fixture and its
    hand-verified expectation, and every assertion here reads both from the class --
    nothing in this contract is written for one document.

    The two documents are deliberately different in kind.  The capture is the runner's
    own artifact byte for byte, so it establishes that the adapter maps what Trivy
    actually emitted; the derived features document carries the vulnerability, secret,
    coordinate and multi-identifier cases the raw artifact does not contain, so it
    establishes that the adapter maps the rest of the shape Trivy can emit.  Holding
    both to the same contract is what makes the pair a pair: a change that fits one
    document and breaks the other fails here rather than in one document's private
    test.

    Each expectation was derived by reading its fixture and the authored contracts
    rather than by recording what the adapter printed.  Where the two disagree, the
    disagreement is the finding: it is diagnosed, never papered over by editing either
    file.

    Attributes:
        FIXTURE: The fixture filename this concrete class adapts.
        EXPECTED_STEM: The expected-file stem holding its hand-verified rows.
    """

    #: Set by each concrete class. Left unset here so a subclass that forgets fails
    #: loudly in ``setUpClass`` instead of silently adapting some default document.
    FIXTURE: str = ""
    EXPECTED_STEM: str = ""

    @classmethod
    def setUpClass(cls) -> None:
        """Adapt this class's fixture once, and load its expectation."""
        super().setUpClass()
        if not cls.FIXTURE or not cls.EXPECTED_STEM:  # pragma: no cover - defended
            raise RuntimeError(
                f"{cls.__name__} inherits the positive-mapping contract without naming "
                "a fixture and an expected stem, so it would assert nothing"
            )
        cls.adapted = adapt_document(load_fixture(cls.FIXTURE))
        cls.expected = load_expected(cls.EXPECTED_STEM)

    def test_the_fixture_and_expectation_name_each_other(self) -> None:
        """The expectation under test describes the fixture under test, not another.

        Both documents in this pair state nine-or-three rows of their own, so a
        concrete class wired to the wrong expectation would fail with a row-count
        mismatch that reads like an adapter defect.  This assertion makes the wiring
        itself the thing that fails.
        """
        recorded = self.expected["fixture"]["path"]
        self.assertEqual(
            recorded,
            f"oss-scan-results/adapter-tests/fixtures/{self.FIXTURE}",
            msg="the expectation's fixture block names the document being adapted",
        )
        self.assertEqual(
            _sha256(FIXTURES_DIR / self.FIXTURE),
            self.expected["fixture"]["sha256"],
            msg=(
                f"{self.EXPECTED_STEM}.rows.json records the sha256 of the fixture on "
                "disk; a mismatch means one of the pair moved without the other"
            ),
        )
        self.assertEqual(
            (FIXTURES_DIR / self.FIXTURE).stat().st_size,
            self.expected["fixture"]["bytes"],
            msg="and its byte count",
        )
        self.assertIn(
            self.expected["fixture"]["kind"],
            ("captured", "derived"),
            msg=(
                "every positive fixture states whether it is a capture of the tool's "
                "own output or a derived document, so no reader has to infer it"
            ),
        )

    def test_the_row_count_is_the_expected_files_exactly(self) -> None:
        """Assertion 1: neither a row more nor a row fewer."""
        self.assertEqual(
            len(self.adapted.rows),
            len(self.expected["rows"]),
            msg="the emitted row count equals the hand-verified count",
        )
        self.assertEqual(
            len(self.adapted.rows),
            self.expected["counts"]["rows"],
            msg="and the count the expectation states in its own counts block",
        )
        self.assertEqual(
            self.adapted.rejections,
            [],
            msg="the positive fixture rejects nothing: every record becomes a row",
        )

    def test_every_row_matches_field_by_field_over_the_twelve_fields(self) -> None:
        """Assertions 2 and 3: every field of every row, iterating ``emit.FIELDS``."""
        self.assertRowsEqualExpected(
            self.adapted.rows, self.expected["rows"], where=self.FIXTURE
        )
        for index, row in enumerate(self.adapted.rows):
            with self.subTest(row=index):
                self.assertEqual(
                    row["tool"], TOOL, msg="assertion 3: tool is trivy on every row"
                )

    def test_the_rows_satisfy_the_schema_the_emitter_enforces(self) -> None:
        """Every row survives ``emit.validate_rows`` unchanged and in order.

        The emitter is the last line of defence on the two never-absent fields, so
        putting the rows through it here proves the adapter's output is writable as a
        dataset rather than merely equal to a list of dicts.
        """
        validated = emit.validate_rows(self.adapted.rows)
        self.assertEqual(
            validated,
            self.adapted.rows,
            msg="validation neither reorders, drops nor rewrites a row",
        )

    def test_one_outcome_per_record_walked(self) -> None:
        """Every record walked reached exactly one outcome: nothing dropped, none twice.

        The counts are the expectation's own rather than a number written here, so the
        same assertion holds for a three-record capture and a nine-record derived
        document without either being special-cased.
        """
        self.assertOneOutcomePerRecord(self.adapted, where=self.FIXTURE)
        self.assertEqual(
            self.adapted.raw_records,
            self.expected["counts"]["raw_finding_records"],
            msg="the record count matches the expectation's own count unit",
        )
        self.assertEqual(
            {
                section: sum(
                    1 for record in self.adapted.records if record.section == section
                )
                for section in trivy.SUPPORTED_SECTIONS
            },
            self.expected["fixture"]["records_by_section"],
            msg="and the per-section split is the expectation's",
        )

    def test_the_counters_are_the_expected_files(self) -> None:
        """Every counter, including the four AAP 0.5.4 has reported per tool."""
        self.assertCountersEqualExpected(
            self.adapted, self.expected, where=self.FIXTURE
        )
        reported = self.expected["aap_reported_counters"]
        for key in (
            trivy.COUNTER_MULTI_LOCATION,
            trivy.COUNTER_MULTI_VALUED_CWE,
            trivy.COUNTER_MULTI_VALUED_CVE,
            trivy.COUNTER_NON_FILESYSTEM_PATHS,
        ):
            with self.subTest(counter=key):
                self.assertEqual(self.adapted.counters[key], reported[key])

    def test_no_emitted_path_is_absolute_and_every_path_is_present(self) -> None:
        """Assertion 7, on every row: relative to the root, never absolute, never absent."""
        for index, row in enumerate(self.adapted.rows):
            with self.subTest(row=index):
                self.assertRelativePath(row["path"], where=f"row {index}")
        for index, expected_row in enumerate(self.expected["rows"]):
            with self.subTest(expected_row=index):
                self.assertFalse(
                    paths.is_absolute_path(expected_row["path"]),
                    msg="no expected value in this file is an absolute path either",
                )

    def test_the_path_comes_from_the_enclosing_target_refined_where_the_section_supplies_one(
        self,
    ) -> None:
        """The Target is the base; only ``Vulnerabilities`` supplies a per-record path.

        ``DetectedSecret`` and ``DetectedMisconfiguration`` declare no path field, so a
        secret and a misconfiguration take their Target unrefined -- and a
        misconfiguration's ``CauseMetadata.Resource`` names a resource rather than a
        file, so reading one as a path would be inference.  How many records refine
        their Target is read from the expectation's own refinement counter rather than
        written here: the derived features document has exactly one such record,
        through ``PkgPath``, and the capture -- three misconfigurations and no
        vulnerability -- has none, so the same assertion covers both the refining and
        the unrefined case.
        """
        refinements = 0
        for row, record in zip(self.adapted.rows, self.adapted.records):
            with self.subTest(pointer=record.pointer):
                declared = record.record.get("PkgPath")
                if record.section != "Vulnerabilities":
                    self.assertIsNone(
                        declared,
                        msg=(
                            "a secret and a misconfiguration declare no path field, so "
                            "neither can refine its Target"
                        ),
                    )
                if isinstance(declared, str) and declared:
                    refinements += 1
                    self.assertEqual(
                        row["path"],
                        declared,
                        msg="a per-record path refines the Target",
                    )
                else:
                    self.assertEqual(
                        row["path"],
                        record.target,
                        msg="and without one the Target is the whole coordinate",
                    )
        self.assertEqual(
            refinements,
            self.adapted.counters[trivy.COUNTER_PER_RECORD_PATH_REFINEMENTS],
            msg="and the adapter counted the refinements this walk found",
        )
        self.assertEqual(
            refinements,
            self.expected["counters"][trivy.COUNTER_PER_RECORD_PATH_REFINEMENTS],
            msg=(
                "and the hand-verified expectation states the same number, so the "
                "walk, the adapter and the expectation agree on it"
            ),
        )

    def test_every_rows_class_is_the_section_its_record_sits_in(self) -> None:
        """``scanner_class`` is the enclosing array's class, on every row of this document.

        :class:`ScannerClassFromSectionTest` proves this at depth against the derived
        features document, which is the only one of the two carrying more than one
        section.  It is asserted here as well so the capture -- the document that is
        this run's actual Trivy output -- is held to the same rule rather than having
        its class taken on trust from a fixture it does not resemble.
        """
        self.assertEqual(
            len(self.adapted.rows),
            len(self.adapted.records),
            msg="a positive fixture rejects nothing, so rows and records align",
        )
        for row, record in zip(self.adapted.rows, self.adapted.records):
            with self.subTest(pointer=record.pointer):
                self.assertEqual(
                    row["scanner_class"],
                    trivy.SUPPORTED_SECTIONS[record.section],
                    msg=(
                        f"{record.pointer} sits in {record.section}, so its class is "
                        "that section's and nothing about its content can change it"
                    ),
                )
                self.assertIn(
                    row["scanner_class"],
                    trivy.SCANNER_CLASSES,
                    msg="and the class is a member of the adapter's closed set",
                )


class CapturedPositiveMappingTest(PositiveMappingContract, TrivyAdapterTestCase):
    """The contract above, against the byte-for-byte capture of this run's artifact.

    ``fixtures/trivy.json`` is ``harness/artifacts/raw/trivy.json`` copied whole --
    3,496 bytes, same sha256 -- so what this class asserts is that the adapter maps the
    output Trivy actually produced on this run.  Three ``Misconfigurations`` records,
    three rows, no rejection.  :class:`CapturedFixtureProvenanceTest` establishes the
    identity with the raw artifact; this class establishes the mapping.
    """

    FIXTURE = POSITIVE_FIXTURE
    EXPECTED_STEM = POSITIVE_EXPECTED_STEM

    def test_this_fixture_is_declared_a_capture_of_the_runs_own_artifact(self) -> None:
        """The expectation states it is captured, and from where."""
        block = self.expected["fixture"]
        self.assertEqual(block["kind"], "captured", msg="declared a capture")
        self.assertEqual(
            block["captured_from"],
            RAW_ARTIFACT_RELPATH,
            msg="and names the raw artifact it was captured from",
        )
        self.assertTrue(
            block["capture_is_byte_for_byte"],
            msg="and states the capture is the whole artifact, not an excerpt",
        )


class DerivedFeaturesPositiveMappingTest(PositiveMappingContract, TrivyAdapterTestCase):
    """The same contract, against the derived multi-section features document.

    The raw artifact carries no ``Vulnerabilities`` and no ``Secrets`` section, so the
    capture cannot exercise scanner_class variation, section-dependent ``start_line``,
    secret redaction, multi-valued identifier selection or any package-coordinate
    level.  This document carries all of them, and every assertion in this module that
    needs one of those features runs against it.  It is explicitly derived rather than
    captured, and its expectation says so in its own fixture block -- which is what
    keeps AAP 0.6.2's provenance requirement and this module's feature coverage from
    being traded off against each other.
    """

    FIXTURE = FEATURES_FIXTURE
    EXPECTED_STEM = FEATURES_EXPECTED_STEM

    def test_this_fixture_is_declared_derived_and_names_what_it_carries(self) -> None:
        """The expectation states it is derived, from what, and why it exists."""
        block = self.expected["fixture"]
        self.assertEqual(block["kind"], "derived", msg="declared derived, not captured")
        self.assertIn(
            "derived_from",
            block,
            msg="and names the document it was derived from",
        )
        cases = block["why_this_derived_fixture_exists"][
            "feature_cases_only_this_fixture_carries"
        ]
        self.assertTrue(
            cases, msg="and enumerates the feature cases it exists to carry"
        )
        for case in cases:
            with self.subTest(case=case[:60]):
                self.assertIsInstance(case, str)
                self.assertTrue(case.strip(), msg="each case is stated, not blank")
        self.assertNotEqual(
            _sha256(FIXTURES_DIR / self.FIXTURE),
            _sha256(REPO_ROOT / RAW_ARTIFACT_RELPATH),
            msg=(
                "and it is not the raw artifact: a derived document that happened to "
                "equal the capture would make the split meaningless"
            ),
        )


#: A record carrying every one of the three sections' rule-identifier fields at once,
#: plus a package coordinate so the dependency-oriented section cannot reject it, plus
#: both line members so a line is available whichever section reads it.
#:
#: Its purpose is to be handed to the adapter three times, byte-identical, under three
#: different sections. Identical content that yields three different classes is the
#: strongest available statement that the class comes from the section: no sniff of any
#: kind can distinguish three calls whose record is the same object's copy.
AMBIGUOUS_RECORD: dict[str, Any] = {
    "VulnerabilityID": "CVE-2021-3749",
    "RuleID": "jwt-token",
    "ID": "DS-0031",
    "Title": "a record whose content names all three sections' identifier fields",
    "Severity": "HIGH",
    "StartLine": 11,
    "CauseMetadata": {"StartLine": 22},
    "PkgName": "example",
    "InstalledVersion": "1.0.0",
    "PkgIdentifier": {"PURL": "pkg:npm/example@1.0.0"},
}

#: The rule-identifier field each section reads, and therefore the identifier the record
#: above yields under each. Restated from AAP 0.5.4's trivy row so the per-section choice
#: is asserted rather than assumed to follow from the class.
RULE_ID_FIELD_BY_SECTION = {
    "Vulnerabilities": "VulnerabilityID",
    "Secrets": "RuleID",
    "Misconfigurations": "ID",
}


class ScannerClassFromSectionTest(TrivyAdapterTestCase):
    """``scanner_class`` derives from the section array, never from record content.

    Trivy is the single tool in AAP 0.5.4's class table whose class is not fixed, and a
    content sniff is the failure this adapter's whole shape exists to prevent: a
    ``Result`` element can carry three finding arrays at once, and a secret and a
    misconfiguration have overlapping-looking fields, so a sniff yields
    plausible-but-wrong classes that **no reconciliation check would catch**.  Every
    row's class is therefore asserted against the array the record sits in, read
    straight out of the document.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Adapt the derived features document once, and hold its section walk.

        The capture carries one section, so it cannot show a class varying with the
        array; this document carries all three.  :class:`CapturedPositiveMappingTest`
        holds the capture to the same per-row rule.
        """
        cls.adapted = adapt_document(load_fixture(FEATURES_FIXTURE))

    def test_every_rows_class_is_the_class_its_enclosing_array_dictates(self) -> None:
        """Assertion 4, per row, with the section names read from the module constant."""
        self.assertEqual(
            len(self.adapted.rows),
            len(self.adapted.records),
            msg="this fixture rejects nothing, so rows and records align",
        )
        for row, record in zip(self.adapted.rows, self.adapted.records):
            with self.subTest(pointer=record.pointer, section=record.section):
                self.assertEqual(
                    row["scanner_class"],
                    record.expected_scanner_class,
                    msg=(
                        f"the record at {record.pointer} was read from "
                        f"{record.section}[], which trivy.SUPPORTED_SECTIONS maps to "
                        f"{record.expected_scanner_class!r}"
                    ),
                )
                self.assertEqual(
                    row["scanner_class"],
                    trivy.scanner_class_for_section(record.section),
                    msg="and the module's own resolver agrees on the section's class",
                )

    def test_the_class_totals_are_the_section_totals(self) -> None:
        """Each class carries exactly as many rows as its section held records."""
        for section, klass in trivy.SUPPORTED_SECTIONS.items():
            with self.subTest(section=section):
                from_records = sum(
                    1 for record in self.adapted.records if record.section == section
                )
                from_rows = sum(
                    1 for row in self.adapted.rows if row["scanner_class"] == klass
                )
                self.assertEqual(from_records, from_rows)
                self.assertEqual(
                    from_rows,
                    self.adapted.counters[f"{trivy.COUNTER_ROWS_CLASS_PREFIX}{klass}"],
                    msg="and the adapter's own per-class counter agrees",
                )
        self.assertEqual(
            {row["scanner_class"] for row in self.adapted.rows},
            set(trivy.SCANNER_CLASSES),
            msg="all three classes appear, so no section is silently unexercised",
        )

    def test_the_record_whose_content_names_the_wrong_section_still_follows_its_array(
        self,
    ) -> None:
        """Assertion 5: the falsifying record, without which assertion 4 proves nothing.

        ``/Results/0/Misconfigurations/0`` is a Dockerfile misconfiguration whose every
        piece of prose points at the secret scanner: its ``Title`` is about secrets
        passed via build-args, its ``Message`` names an exposed environment variable and
        its ``Resolution`` talks about secret mounts.  Any implementation that keyed the
        class off record content -- a search for a word, a guess from the identifier, a
        try-all-three read of ``VulnerabilityID`` then ``RuleID`` then ``ID`` -- would
        class this row ``secret`` and still satisfy every other assertion in this file,
        because no other record's prose disagrees with its array.

        The misleading content is asserted to be present first.  A falsifier that stopped
        misleading would silently stop falsifying.
        """
        record = self.adapted.records[0]
        self.assertEqual(record.pointer, "/Results/0/Misconfigurations/0")
        self.assertEqual(record.section, "Misconfigurations")
        prose = " ".join(
            str(record.record.get(key, ""))
            for key in ("Title", "Message", "Resolution")
        ).lower()
        self.assertIn(
            "secret",
            prose,
            msg=(
                "the falsifier only falsifies while its content points at another "
                "section; this record's own prose must still name secrets"
            ),
        )
        self.assertEqual(
            self.adapted.rows[0]["scanner_class"],
            "misconfig",
            msg=(
                "it is an element of Results[0].Misconfigurations[], and the section "
                "array is the sole source of the class. If this row ever reads secret, "
                "the derivation has been inverted"
            ),
        )

    def test_the_corollary_runs_the_other_way_on_the_secret_whose_identifier_is_a_format(
        self,
    ) -> None:
        """A ``jwt-token`` record is a secret because of its array, not its identifier."""
        record = next(
            item for item in self.adapted.records if item.section == "Secrets"
        )
        self.assertEqual(record.record["RuleID"], "jwt-token")
        row = self.adapted.rows[self.adapted.records.index(record)]
        self.assertEqual(
            row["scanner_class"],
            "secret",
            msg="from Secrets[], not from an identifier that names a token format",
        )
        self.assertEqual(row["rule_id"], "jwt-token")

    def test_one_identical_record_takes_three_classes_under_three_sections(self) -> None:
        """The same content, three sections, three classes -- and three identifiers.

        Handed through :func:`trivy.adapt_record`, the public section-attribution seam.
        Nothing about the record changes between the three calls, so any implementation
        that consulted content would have to return the same class three times.  The
        rule identifier changing with the section is asserted alongside, because it is
        the same principle applied to a different field: the field is chosen by section
        (AAP 0.5.4), not by whichever identifier happens to be present.
        """
        env = environment()
        for section, klass in trivy.SUPPORTED_SECTIONS.items():
            with self.subTest(section=section):
                counters = trivy.new_counters()
                outcome = trivy.adapt_record(
                    copy.deepcopy(AMBIGUOUS_RECORD),
                    section=section,
                    target="core/src/main/scala/org/apache/spark/SparkContext.scala",
                    tool=TOOL,
                    root=env.root,
                    tool_base=env.tool_base(),
                    globs=env.globs,
                    tally=severity.LiteralTally.with_all_tools(),
                    counters=counters,
                    ecosystem="npm",
                )
                self.assertNotIsInstance(
                    outcome,
                    paths.Rejection,
                    msg="the record is complete under every section, so it is a row",
                )
                self.assertEqual(
                    outcome["scanner_class"],
                    klass,
                    msg=f"{section}[] maps to {klass!r} and content decides nothing",
                )
                self.assertEqual(
                    outcome["rule_id"],
                    AMBIGUOUS_RECORD[RULE_ID_FIELD_BY_SECTION[section]],
                    msg=(
                        "and the rule identifier is read from the field this section "
                        f"declares, {RULE_ID_FIELD_BY_SECTION[section]}"
                    ),
                )

    def test_a_content_hint_planted_in_a_derived_document_changes_no_class(self) -> None:
        """A CVE in a misconfiguration's title, and a ``RuleID`` on a vulnerability.

        The two shapes an implementation is most likely to sniff, planted in a derived
        copy of the fixture rather than in the fixture itself.  Both rows keep the class
        their array dictates.
        """
        document = derived(FEATURES_FIXTURE)
        misconfiguration = document["Results"][0]["Misconfigurations"][0]
        misconfiguration["Title"] = "CVE-2021-3749 mentioned inside a misconfiguration"
        vulnerability = document["Results"][3]["Vulnerabilities"][0]
        vulnerability["RuleID"] = "generic-api-key"
        vulnerability["Match"] = "****"
        adapted = adapt_document(document)
        rows_by_pointer = {
            record.pointer: row for row, record in zip(adapted.rows, adapted.records)
        }
        self.assertEqual(
            rows_by_pointer["/Results/0/Misconfigurations/0"]["scanner_class"],
            "misconfig",
            msg="a CVE in the prose does not make a misconfiguration a vulnerability",
        )
        self.assertEqual(
            rows_by_pointer["/Results/0/Misconfigurations/0"]["message"],
            misconfiguration["Title"],
            msg="the planted title did reach the message, so the record was read",
        )
        self.assertEqual(
            rows_by_pointer["/Results/3/Vulnerabilities/0"]["scanner_class"],
            "vuln",
            msg="and a RuleID-looking field does not make a vulnerability a secret",
        )
        self.assertEqual(
            rows_by_pointer["/Results/3/Vulnerabilities/0"]["rule_id"],
            vulnerability["VulnerabilityID"],
            msg="the identifier still comes from the field this section declares",
        )


class StartLineSectionDependenceTest(TrivyAdapterTestCase):
    """``start_line`` is section-dependent, and its absence is stated rather than filled.

    ``report.go`` puts ``StartLine`` on a secret and inside a misconfiguration's
    ``CauseMetadata``, and declares none at all on a vulnerability.  So one document
    proves both halves without a single synthesized absence -- and the absent half is
    asserted explicitly, because an implementation that defaulted to ``0`` or ``1``
    would satisfy a mere "is an integer or None" check on every row that has a line.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Adapt the derived features document once for the whole class."""
        cls.adapted = adapt_document(load_fixture(FEATURES_FIXTURE))

    def test_a_vulnerability_row_carries_no_line_at_all(self) -> None:
        """Assertion 6, the half that catches a default: ``None``, not ``0`` and not ``1``."""
        vulnerability_rows = [
            (row, record)
            for row, record in zip(self.adapted.rows, self.adapted.records)
            if record.section == "Vulnerabilities"
        ]
        self.assertTrue(
            vulnerability_rows, msg="the fixture carries vulnerability records to assert on"
        )
        for row, record in vulnerability_rows:
            with self.subTest(pointer=record.pointer):
                self.assertIsNone(
                    row["start_line"],
                    msg=(
                        "report.go declares no line member on a DetectedVulnerability, "
                        "so the field is absent -- not zero, not one, and not the "
                        "enclosing package's location"
                    ),
                )
                self.assertNotIn(
                    "StartLine",
                    record.record,
                    msg="and the record itself states none, which is why",
                )

    def test_a_secret_and_a_misconfiguration_row_carry_the_line_they_state(self) -> None:
        """The present half, read from the member each section declares."""
        seen = set()
        for row, record in zip(self.adapted.rows, self.adapted.records):
            if record.section == "Vulnerabilities":
                continue
            with self.subTest(pointer=record.pointer):
                if record.section == "Secrets":
                    stated = record.record.get("StartLine")
                else:
                    stated = (record.record.get("CauseMetadata") or {}).get("StartLine")
                self.assertIsInstance(
                    stated, int, msg="the fixture states a line for this record"
                )
                self.assertEqual(
                    row["start_line"],
                    stated,
                    msg="and the row carries that line, unmodified",
                )
                seen.add(record.section)
        self.assertEqual(
            seen,
            {"Secrets", "Misconfigurations"},
            msg="both line-bearing sections are exercised, not just one",
        )

    def test_the_absence_count_is_the_number_of_vulnerability_records(self) -> None:
        """The adapter counted the absences this walk found, rather than assuming them."""
        absences = sum(
            1 for row in self.adapted.rows if row["start_line"] is None
        )
        self.assertEqual(
            absences,
            self.adapted.counters[trivy.COUNTER_START_LINE_ABSENT],
            msg="rows with no line and the counter agree",
        )
        self.assertEqual(
            absences,
            sum(1 for record in self.adapted.records if record.section == "Vulnerabilities"),
            msg="and every one of them is a vulnerability record",
        )
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_START_LINE_ZERO],
            0,
            msg="no record here states a zero line, so none was read as an absence",
        )

    def test_a_stated_zero_line_is_read_as_an_absence_and_not_as_line_zero(self) -> None:
        """A derived secret stating ``StartLine`` 0 yields a row with no line.

        Zero is not a line number, and the artifact stating one is a stated absence
        rather than a fault: the record is emitted with ``start_line`` absent and the
        reading is counted, so it is visible rather than silently rounded into 1 or
        rejected.
        """
        document = derived(FEATURES_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][1])]
        document["Results"][0]["Secrets"] = [
            copy.deepcopy(document["Results"][0]["Secrets"][0])
        ]
        document["Results"][0]["Secrets"][0]["StartLine"] = 0
        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 1)
        self.assertEqual(adapted.rejections, [], msg="a stated zero is not a rejection")
        self.assertIsNone(
            adapted.rows[0]["start_line"], msg="and it is not line zero either"
        )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_START_LINE_ZERO],
            1,
            msg="the reading is counted, so it is never silent",
        )


class MultiLocationRecordTest(TrivyAdapterTestCase):
    """The first-location rule, and the counter AAP 0.5.4 requires reported per tool.

    Where a record names more than one location the row takes the **first**, the record
    still counts **once**, and the number of such records is reported.  For Trivy that
    case is a misconfiguration whose ``CauseMetadata.Occurrences`` holds more than one
    entry: ``ftypes.CauseMetadata`` is the only member of any of the three sections that
    carries a list of locations, and a vulnerability and a secret each name exactly one.

    No committed fixture drives ``Occurrences`` above one -- the run's own artifact
    carries none at all -- so nothing exercised the increment and deleting it left the
    suite green.  These documents drive it, and the counter is asserted by **value**
    rather than by ``assertGreater``, because a single record with three occurrences and
    three records with one each are different artifacts that a bare "more than zero"
    check could not tell apart.

    The paired assertion matters as much: ``Occurrences`` feeds the count and nothing
    else.  ``start_line`` stays the record's own ``CauseMetadata.StartLine``, never an
    occurrence's, because reading a line out of an occurrence would be choosing a
    location the record did not put first.
    """

    def occurrence(self, start_line: int, name: str) -> dict[str, Any]:
        """One ``CauseMetadata.Occurrences`` entry, in ``ftypes``' own shape."""
        return {
            "Resource": name,
            "Filename": "resource-managers/kubernetes/docker/src/main/dockerfiles"
            "/spark/Dockerfile",
            "Location": {"StartLine": start_line, "EndLine": start_line + 2},
        }

    def with_occurrences(
        self, counts: tuple[int, ...], *, stated_line: int | None = None
    ) -> Any:
        """The capture with ``counts[i]`` occurrences on element ``i``'s record.

        A count of zero leaves ``Occurrences`` absent rather than writing an empty
        array, so the absent and the present-and-empty shapes are both represented
        across the cases below.
        """
        document = derived(POSITIVE_FIXTURE)
        for index, count in enumerate(counts):
            cause = document["Results"][index]["Misconfigurations"][0]["CauseMetadata"]
            if stated_line is not None:
                cause["StartLine"] = stated_line
            if count == 0:
                continue
            cause["Occurrences"] = [
                self.occurrence(10 + (position * 37), f"stage-{index}-{position}")
                for position in range(count)
            ]
        return document

    def test_a_record_with_several_occurrences_is_counted_once_by_value(self) -> None:
        """One record, three occurrences: the counter is exactly 1 and the rows are 3."""
        document = self.with_occurrences((3, 0, 0))
        adapted = adapt_document(document)
        self.assertEqual(
            adapted.counters[trivy.COUNTER_MULTI_LOCATION],
            1,
            msg="one record named more than one location, so the counter is 1",
        )
        self.assertEqual(
            len(adapted.rows),
            3,
            msg="the record still produced exactly one row, not one per occurrence",
        )
        self.assertEqual(adapted.rejections, [])
        self.assertEqual(
            len(adapted.rows) + len(adapted.rejections),
            adapted.raw_records,
            msg="and it still counts once on both sides of the identity",
        )
        self.assertEqual(
            len(
                document["Results"][0]["Misconfigurations"][0]["CauseMetadata"][
                    "Occurrences"
                ]
            ),
            3,
            msg="three occurrences went in, and the counter counted records not entries",
        )

    def test_the_counter_is_the_number_of_records_not_the_number_of_locations(
        self,
    ) -> None:
        """Two multi-location records among three: the counter is 2, never 5.

        The falsifying pair for an implementation that added ``len(Occurrences)`` instead
        of one, which would report 5 for this document.
        """
        adapted = adapt_document(self.with_occurrences((3, 1, 2)))
        self.assertEqual(adapted.counters[trivy.COUNTER_MULTI_LOCATION], 2)
        self.assertEqual(len(adapted.rows), 3)

        every = adapt_document(self.with_occurrences((2, 2, 2)))
        self.assertEqual(
            every.counters[trivy.COUNTER_MULTI_LOCATION],
            3,
            msg="all three records named two locations each",
        )
        self.assertEqual(len(every.rows), 3)

    def test_one_occurrence_absent_and_empty_all_leave_the_counter_at_zero(self) -> None:
        """The three shapes that name exactly one location, asserted individually.

        ``max(1, len(...))`` is what makes an empty array and an absent member behave as
        one location rather than as zero, and an implementation using a bare ``len`` on a
        record with an empty ``Occurrences`` would report a location count of zero -- not
        greater than one, so still no increment, but the guard is asserted here so the
        reading is deliberate rather than incidental.
        """
        for label, prepare in (
            ("absent", lambda cause: cause.pop("Occurrences", None)),
            ("empty array", lambda cause: cause.__setitem__("Occurrences", [])),
            (
                "one entry",
                lambda cause: cause.__setitem__(
                    "Occurrences", [self.occurrence(41, "only")]
                ),
            ),
            ("null", lambda cause: cause.__setitem__("Occurrences", None)),
            (
                "not an array",
                lambda cause: cause.__setitem__("Occurrences", {"0": "one"}),
            ),
        ):
            with self.subTest(shape=label):
                document = derived(POSITIVE_FIXTURE)
                for element in document["Results"]:
                    prepare(element["Misconfigurations"][0]["CauseMetadata"])
                adapted = adapt_document(document)
                self.assertEqual(
                    adapted.counters[trivy.COUNTER_MULTI_LOCATION],
                    0,
                    msg=f"{label} names one location, so nothing is counted",
                )
                self.assertEqual(len(adapted.rows), 3)
                self.assertEqual(adapted.rejections, [])

    def test_a_vulnerability_and_a_secret_never_reach_the_multi_location_count(
        self,
    ) -> None:
        """Only a misconfiguration can carry a list of locations, and that is asserted.

        ``Occurrences`` planted on a vulnerability and on a secret must be ignored: the
        count is section-bound, and an implementation that read ``CauseMetadata``
        regardless of section would report locations for records whose shape cannot
        carry them.
        """
        document = derived(FEATURES_FIXTURE)
        planted = 0
        for element in document["Results"]:
            for section in ("Vulnerabilities", "Secrets"):
                for record in element.get(section) or []:
                    record["CauseMetadata"] = {
                        "Occurrences": [
                            self.occurrence(11, "a"),
                            self.occurrence(22, "b"),
                            self.occurrence(33, "c"),
                        ]
                    }
                    planted += 1
        self.assertGreater(planted, 0, msg="the features document has both sections")
        adapted = adapt_document(document)
        self.assertEqual(
            adapted.counters[trivy.COUNTER_MULTI_LOCATION],
            0,
            msg=(
                "not one of the planted lists is counted: only Misconfigurations names "
                "a location list in report.go"
            ),
        )

    def test_the_row_keeps_the_records_own_line_and_not_an_occurrences(self) -> None:
        """``Occurrences`` feeds the counter and nothing else.

        The record states line 62; its three occurrences state 10, 47 and 84.  The row
        must carry 62 -- an implementation that took the first occurrence's line would
        emit 10 and would satisfy every "is an integer" check in this module.
        """
        document = self.with_occurrences((3, 0, 0), stated_line=62)
        occurrences = document["Results"][0]["Misconfigurations"][0]["CauseMetadata"][
            "Occurrences"
        ]
        occurrence_lines = [entry["Location"]["StartLine"] for entry in occurrences]
        self.assertEqual(occurrence_lines, [10, 47, 84])
        self.assertNotIn(62, occurrence_lines, msg="no occurrence states the row's line")

        adapted = adapt_document(document)
        self.assertEqual(adapted.counters[trivy.COUNTER_MULTI_LOCATION], 1)
        self.assertEqual(
            adapted.rows[0]["start_line"],
            62,
            msg="the record's own CauseMetadata.StartLine, not an occurrence's",
        )
        for row in adapted.rows:
            with self.subTest(path=row["path"]):
                self.assertNotIn(
                    row["start_line"],
                    occurrence_lines,
                    msg="no row took a line from the occurrence list",
                )

    def test_the_multi_location_counter_is_one_of_the_four_aap_reports(self) -> None:
        """The counter is the AAP's own key, and it is initialised for every call.

        Asserted against the module's constant and against ``new_counters()`` rather
        than spelled here, so a renamed counter fails on the name instead of silently
        counting into a key no report reads.
        """
        self.assertEqual(trivy.COUNTER_MULTI_LOCATION, "multi_location_records")
        self.assertIn(trivy.COUNTER_MULTI_LOCATION, trivy.COUNTER_KEYS)
        self.assertEqual(trivy.new_counters()[trivy.COUNTER_MULTI_LOCATION], 0)
        expectation = load_expected(POSITIVE_EXPECTED_STEM)["aap_reported_counters"]
        self.assertIn(
            trivy.COUNTER_MULTI_LOCATION,
            expectation,
            msg="and the expected files report it per tool",
        )
        self.assertEqual(
            expectation[trivy.COUNTER_MULTI_LOCATION],
            0,
            msg=(
                "the captured artifact carries no Occurrences at all, which is why the "
                "increment needed a document of its own to be exercised"
            ),
        )


class SeverityMappingTest(TrivyAdapterTestCase):
    """``severity_native`` is the literal as observed; ``severity_norm`` is policy.

    The policy is fixed before any output is observed (AAP 0.5.4): a native label in the
    mapped vocabulary governs, a CVSS score is consulted only where no mapped label
    exists, and a literal outside every mapped vocabulary maps to ``Info`` and is
    **listed with the rows it affected**.  ``severity_norm`` is never absent.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Adapt the derived features document once for the whole class."""
        cls.adapted = adapt_document(load_fixture(FEATURES_FIXTURE))
        cls.expected = load_expected(FEATURES_EXPECTED_STEM)

    def test_the_label_vocabulary_is_the_one_the_policy_states(self) -> None:
        """Ten literals, five bands, restated here independently of ``severity.py``."""
        self.assertEqual(
            severity.label_table(),
            EXPECTED_LABEL_BANDS,
            msg="the label table is the written policy, neither wider nor narrower",
        )
        self.assertEqual(
            severity.SEVERITY_NORM,
            EXPECTED_SEVERITY_NORM,
            msg="and the five bands are the standard scale with None emitted as Info",
        )

    def test_every_row_keeps_the_literal_and_takes_the_band_the_map_gives_it(self) -> None:
        """Assertion 8: the raw ``Severity`` literal, and its band by the label map."""
        for row, record in zip(self.adapted.rows, self.adapted.records):
            with self.subTest(pointer=record.pointer):
                stated = record.record.get("Severity")
                self.assertEqual(
                    row["severity_native"],
                    stated,
                    msg="severity_native is the literal as observed, not upper-cased",
                )
                self.assertEqual(
                    row["severity_norm"],
                    EXPECTED_LABEL_BANDS[str(stated).upper()],
                    msg="and the band is the label map's, matched case-insensitively",
                )
                self.assertIsNotNone(
                    row["severity_norm"], msg="severity_norm is never absent"
                )

    def test_the_label_governs_even_where_the_record_also_carries_scores(self) -> None:
        """Label over score: four records here state both, and the label decides all four.

        The score tables are present and counted -- so the test is not passing merely
        because nothing was there to consult -- and no row's band came from one.
        """
        with_scores = [
            record for record in self.adapted.records if record.record.get("CVSS")
        ]
        self.assertEqual(
            len(with_scores),
            self.adapted.counters[trivy.COUNTER_SEVERITY_CVSS_ENTRIES_PRESENT],
            msg="the records carrying a score table are counted",
        )
        self.assertTrue(with_scores, msg="and there is at least one to assert over")
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_SEVERITY_BASIS_PREFIX + "cvss_score"],
            0,
            msg="yet no band was taken from a score, because every label is mapped",
        )
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_SEVERITY_BASIS_PREFIX + "label"],
            len(self.adapted.rows),
            msg="every row's band came from its label",
        )

    def test_the_tally_records_every_literal_with_the_rows_it_affected(self) -> None:
        """The tally is ``severity-map.md``'s input, and it is fed once per emitted row."""
        entries = self.adapted.tally.entries(TOOL)
        self.assertEqual(
            sum(entry.rows for entry in entries),
            len(self.adapted.rows),
            msg="one tally record per emitted row, no more and no fewer",
        )
        counted: dict[str, int] = {}
        for row in self.adapted.rows:
            literal = str(row["severity_native"])
            counted[literal] = counted.get(literal, 0) + 1
        self.assertEqual(
            {entry.severity_native: entry.rows for entry in entries},
            counted,
            msg="and each literal's row count is the number of rows carrying it",
        )
        self.assertEqual(
            [entry for entry in entries if entry.unmapped],
            [],
            msg=(
                "no literal in this fixture is unmapped: UNKNOWN looks unmapped and is "
                "in fact in the vocabulary, which is why the unmapped case is asserted "
                "on a derived document instead"
            ),
        )

    def test_a_literal_outside_the_vocabulary_maps_to_info_and_is_disclosed(self) -> None:
        """Assertion 8's second half, on a derived document.

        The features fixture carries no unmapped literal -- its ``UNKNOWN`` is mapped --
        so the case is exercised on a copy.  An unmapped literal is banded ``Info``,
        never dropped and never guessed at, and it is recorded in the tally **as
        unmapped with its row count**, which is what AAP 0.5.4 requires
        ``severity-map.md`` to list.
        """
        document = derived(FEATURES_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][0])]
        unmapped_literal = "SEVERE"
        self.assertNotIn(
            unmapped_literal,
            severity.label_table(),
            msg="the literal is genuinely outside the mapped vocabulary",
        )
        for record in document["Results"][0]["Misconfigurations"]:
            record["Severity"] = unmapped_literal
        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 2)
        for index, row in enumerate(adapted.rows):
            with self.subTest(row=index):
                self.assertEqual(
                    row["severity_native"],
                    unmapped_literal,
                    msg="the literal is retained exactly as observed",
                )
                self.assertEqual(
                    row["severity_norm"],
                    "Info",
                    msg="and an unmapped literal is banded Info by policy",
                )
        unmapped_entries = [
            entry for entry in adapted.tally.entries(TOOL) if entry.unmapped
        ]
        self.assertEqual(
            len(unmapped_entries), 1, msg="one unmapped literal, one tally entry"
        )
        self.assertEqual(unmapped_entries[0].severity_native, unmapped_literal)
        self.assertEqual(unmapped_entries[0].severity_norm, "Info")
        self.assertEqual(
            unmapped_entries[0].basis,
            severity.BASIS_UNMAPPED_LITERAL,
            msg="recorded under the unmapped basis, so severity-map.md can list it",
        )
        self.assertEqual(
            unmapped_entries[0].rows,
            2,
            msg="with the number of rows it affected, not merely that it occurred",
        )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_SEVERITY_BASIS_PREFIX + "unmapped_literal"],
            2,
            msg="and the adapter counted both rows under that basis",
        )

    def test_the_label_map_is_case_insensitive(self) -> None:
        """``medium``, ``Moderate`` and ``critical`` band as their upper-case forms do."""
        for literal, band in (
            ("medium", "Medium"),
            ("Moderate", "Medium"),
            ("critical", "Critical"),
            ("negligible", "Info"),
            ("Informational", "Info"),
        ):
            with self.subTest(literal=literal):
                document = derived(FEATURES_FIXTURE)
                document["Results"] = [copy.deepcopy(document["Results"][0])]
                document["Results"][0]["Misconfigurations"] = [
                    copy.deepcopy(document["Results"][0]["Misconfigurations"][0])
                ]
                document["Results"][0]["MisconfSummary"] = {
                    "Successes": 24,
                    "Failures": 1,
                }
                document["Results"][0]["Misconfigurations"][0]["Severity"] = literal
                adapted = adapt_document(document)
                self.assertEqual(len(adapted.rows), 1)
                self.assertEqual(
                    adapted.rows[0]["severity_native"],
                    literal,
                    msg="the literal is not normalised into the row",
                )
                self.assertEqual(
                    adapted.rows[0]["severity_norm"],
                    band,
                    msg="but the lookup that banded it was case-insensitive",
                )

    def test_a_score_governs_only_where_no_mapped_label_exists(self) -> None:
        """With the label removed, the band comes from the score and the score is recorded.

        The rendered score becomes ``severity_native``, so the dataset states what was
        used rather than leaving a reader to guess which of the record's several score
        entries decided the band.
        """
        document = derived(FEATURES_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][2])]
        document["Results"][0]["Vulnerabilities"] = [
            copy.deepcopy(document["Results"][0]["Vulnerabilities"][0])
        ]
        record = document["Results"][0]["Vulnerabilities"][0]
        del record["Severity"]
        self.assertIn("CVSS", record, msg="the record still carries its score table")
        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 1)
        row = adapted.rows[0]
        self.assertEqual(
            row["severity_native"],
            "7.5",
            msg="the score entry used is rendered into the field, to one decimal",
        )
        self.assertEqual(
            row["severity_norm"], "High", msg="7.5 falls in the 7.0-8.9 band"
        )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_SEVERITY_BASIS_PREFIX + "cvss_score"],
            1,
            msg="and the basis is recorded as the score rather than a label",
        )
        entry = adapted.tally.entries(TOOL)[0]
        self.assertEqual(entry.basis, severity.BASIS_CVSS_SCORE)
        self.assertFalse(entry.unmapped, msg="a banded score is not an unmapped literal")

    def test_a_record_with_no_vocabulary_at_all_states_the_absence(self) -> None:
        """No label and no score: ``severity_native`` absent, ``severity_norm`` still Info.

        The absence is stated rather than a level being assumed, which is the whole
        distinction between this case and the unmapped one above.
        """
        document = derived(FEATURES_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][1])]
        document["Results"][0]["Secrets"] = [
            copy.deepcopy(document["Results"][0]["Secrets"][0])
        ]
        del document["Results"][0]["Secrets"][0]["Severity"]
        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 1)
        self.assertIsNone(
            adapted.rows[0]["severity_native"],
            msg="absence is permitted for severity_native and is not filled in",
        )
        self.assertEqual(
            adapted.rows[0]["severity_norm"],
            "Info",
            msg="severity_norm is never absent, so the band is Info by policy",
        )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_SEVERITY_ABSENT],
            1,
            msg="and the absence is counted rather than inferred from a null",
        )


    # ---------------------------------------------------------------- #
    # Which score entry was selected, and from where.
    #
    # AAP 0.5.4 requires that the entry used be *recorded* -- "the label, or the
    # score with its source and version".  A rendered score in
    # ``severity_native`` states the number and says nothing about which of an
    # advisory's several entries produced it, and research established that an
    # advisory commonly carries scores from several sources.  So these assert the
    # resolved ``SeverityResult.selected_entry`` itself, over candidate lists the
    # adapter built, with competing entries arranged so that selecting by the
    # wrong rule produces a different answer.
    # ---------------------------------------------------------------- #

    def resolved(self, record: Any) -> severity.SeverityResult:
        """Resolve one record's severity through the adapter's own candidate builder.

        ``trivy._score_candidates`` is called directly, and deliberately: it is the
        function that turns a Trivy ``CVSS`` table into the ``{score, source, version}``
        candidates ``severity.resolve`` reads, and asserting ``resolve`` over a
        hand-written candidate list would test ``severity.py`` against a shape this
        adapter might not actually produce.  Every assertion below is tied back to the
        public route as well: the band it selects is compared with the band the adapter
        put in the row for the same record.
        """
        return severity.resolve(
            label=record.get("Severity"), scores=trivy._score_candidates(record) or None
        )

    def one_vulnerability(self, cvss: Any, *, label: Any = None) -> Any:
        """A single-record document whose one vulnerability carries ``cvss``.

        Derived from the features fixture's own vulnerability so every other field is
        real tool output; only the score table, and the label where one is asked for, are
        this test's.
        """
        document = derived(FEATURES_FIXTURE)
        element = copy.deepcopy(document["Results"][2])
        element["Vulnerabilities"] = [copy.deepcopy(element["Vulnerabilities"][0])]
        record = element["Vulnerabilities"][0]
        record["CVSS"] = cvss
        if label is None:
            record.pop("Severity", None)
        else:
            record["Severity"] = label
        document["Results"] = [element]
        return document

    def test_a_tie_between_two_sources_is_broken_lexicographically(self) -> None:
        """Two sources, the same version, the same score: the smaller source name wins.

        Both entries render ``7.5``, so the row cannot show which was used and only
        ``selected_entry`` can.  ``redhat`` is written **first** in the table and
        ``ghsa`` second, so an implementation selecting the first entry in document
        order, or the last, picks ``redhat`` and fails here.  The score and the band are
        identical either way, which is exactly why a test that asserted only those would
        survive the mutation.
        """
        document = self.one_vulnerability(
            {
                "redhat": {"V3Score": 7.5, "V3Vector": "CVSS:3.1/AV:N/AC:L"},
                "ghsa": {"V3Score": 7.5, "V3Vector": "CVSS:3.1/AV:N/AC:L"},
            }
        )
        record = document["Results"][0]["Vulnerabilities"][0]
        self.assertEqual(
            list(record["CVSS"]),
            ["redhat", "ghsa"],
            msg="document order is redhat then ghsa, the reverse of lexicographic",
        )

        result = self.resolved(record)
        self.assertEqual(result.basis, severity.BASIS_CVSS_SCORE)
        self.assertIsNotNone(result.selected_entry)
        self.assertEqual(
            result.selected_entry,
            {"score": 7.5, "source": "ghsa:V3Score", "version": "3"},
            msg=(
                "the entry used is recorded in full -- score, source and version -- and "
                "the source is the lexicographically smaller of the two"
            ),
        )
        self.assertEqual(result.severity_native, "7.5")
        self.assertEqual(result.severity_norm, "High")

        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 1)
        self.assertEqual(adapted.rows[0]["severity_native"], result.severity_native)
        self.assertEqual(adapted.rows[0]["severity_norm"], result.severity_norm)
        self.assertEqual(
            adapted.counters[trivy.COUNTER_SEVERITY_CVSS_ENTRIES_PRESENT],
            1,
            msg="and the adapter counted the record as carrying score entries",
        )

    def test_a_tie_within_one_source_is_broken_by_version_not_by_score(self) -> None:
        """A higher score at a lower version loses: version precedence comes first.

        ``nvd:V2Score`` is 10.0 and ``nvd:V3Score`` is 4.0.  Selecting by score magnitude
        gives 10.0 and a Critical band; the documented order gives the version-3 entry,
        4.0 and Medium.  The two answers differ in the band as well as in the entry, so
        this case falsifies both a wrong selection and a wrong band.
        """
        document = self.one_vulnerability({"nvd": {"V2Score": 10.0, "V3Score": 4.0}})
        record = document["Results"][0]["Vulnerabilities"][0]

        result = self.resolved(record)
        self.assertEqual(
            result.selected_entry,
            {"score": 4.0, "source": "nvd:V3Score", "version": "3"},
            msg="the higher CVSS version governs, whatever the scores are",
        )
        self.assertEqual(result.severity_norm, "Medium")
        self.assertNotEqual(
            result.severity_norm,
            "Critical",
            msg="which 10.0 would have given, and is the mutation this case catches",
        )
        adapted = adapt_document(document)
        self.assertEqual(adapted.rows[0]["severity_native"], "4.0")
        self.assertEqual(adapted.rows[0]["severity_norm"], "Medium")

    def test_version_four_outranks_a_far_higher_version_three_score(self) -> None:
        """``V40Score`` 0.1 beats ``V3Score`` 9.9, and the recorded version says so.

        The most extreme form of the same rule, kept separate because the three score
        fields are a closed list in the adapter and a reader should see all three
        exercised: the selected entry is the version-4 one and the band is Low.
        """
        document = self.one_vulnerability(
            {
                "zulu": {"V3Score": 9.9},
                "alpha": {"V40Score": 0.1},
            }
        )
        record = document["Results"][0]["Vulnerabilities"][0]
        result = self.resolved(record)
        self.assertEqual(
            result.selected_entry,
            {"score": 0.1, "source": "alpha:V40Score", "version": "4"},
        )
        self.assertEqual(result.severity_norm, "Low")
        self.assertEqual(adapt_document(document).rows[0]["severity_native"], "0.1")

    def test_every_score_field_the_adapter_reads_is_recorded_with_its_version(
        self,
    ) -> None:
        """The three fields, their three major versions, and one source each.

        Asserted per field in isolation, so the version a field maps to is checked
        rather than inferred from the tie-breaking cases above.  The recorded version is
        the **major** the field name states and nothing more: ``V3Score`` does not say
        whether the score is 3.0 or 3.1, and writing ``3.1`` would supply precision the
        artifact never stated.
        """
        for field, major, band in (
            ("V40Score", "4", "High"),
            ("V3Score", "3", "High"),
            ("V2Score", "2", "High"),
        ):
            with self.subTest(field=field):
                document = self.one_vulnerability({"nvd": {field: 7.5}})
                record = document["Results"][0]["Vulnerabilities"][0]
                result = self.resolved(record)
                self.assertEqual(
                    result.selected_entry,
                    {"score": 7.5, "source": f"nvd:{field}", "version": major},
                )
                self.assertEqual(result.severity_norm, band)
                self.assertEqual(
                    adapt_document(document).rows[0]["severity_norm"], band
                )

    def test_a_mapped_label_governs_and_the_selected_entry_is_the_label(self) -> None:
        """With a label present the entry recorded is the label, not any score.

        The other half of AAP 0.5.4's "record which entry was used": where the label
        governs, ``selected_entry`` must be the label shape rather than a score, so a
        reader of ``severity-map.md`` can tell the two bases apart.  The scores here
        would band Critical, and the label bands Low, so a precedence error is visible in
        the band as well as in the entry.
        """
        document = self.one_vulnerability({"nvd": {"V3Score": 9.8}}, label="LOW")
        record = document["Results"][0]["Vulnerabilities"][0]
        result = self.resolved(record)
        self.assertEqual(
            result.selected_entry,
            {"label": "LOW"},
            msg="the label shape, with no score, source or version",
        )
        self.assertEqual(result.basis, severity.BASIS_LABEL)
        self.assertEqual(result.severity_norm, "Low")

        adapted = adapt_document(document)
        self.assertEqual(adapted.rows[0]["severity_native"], "LOW")
        self.assertEqual(adapted.rows[0]["severity_norm"], "Low")
        self.assertEqual(
            adapted.counters[trivy.COUNTER_SEVERITY_CVSS_ENTRIES_PRESENT],
            1,
            msg=(
                "the score entries are still counted as present -- the record carried "
                "them -- while the label is what governed"
            ),
        )
        self.assertEqual(adapted.counters[trivy.COUNTER_SEVERITY_LABEL_PRESENT], 1)

    def test_an_unreadable_or_empty_score_table_selects_nothing(self) -> None:
        """A table that yields no candidate falls through to the absence, not to a guess.

        Four shapes: no table, a table that is not an object, a source whose entry is not
        an object, and an entry whose score fields are all null.  Each must leave the
        record with no vocabulary at all rather than banding something.
        """
        for label, table in (
            ("absent", None),
            ("not an object", ["nvd"]),
            ("entry not an object", {"nvd": "7.5"}),
            ("all fields null", {"nvd": {"V3Score": None, "V2Score": None}}),
            ("no known field", {"nvd": {"V4Vector": "CVSS:4.0/AV:N"}}),
        ):
            with self.subTest(shape=label):
                document = self.one_vulnerability(table)
                record = document["Results"][0]["Vulnerabilities"][0]
                if table is None:
                    record.pop("CVSS", None)
                result = self.resolved(record)
                self.assertIsNone(
                    result.selected_entry,
                    msg="nothing was used, so nothing is recorded as used",
                )
                self.assertEqual(result.basis, severity.BASIS_NO_VOCABULARY)
                self.assertIsNone(result.severity_native)
                self.assertEqual(
                    result.severity_norm,
                    "Info",
                    msg="severity_norm is never absent; the band comes from policy",
                )
                adapted = adapt_document(document)
                self.assertIsNone(adapted.rows[0]["severity_native"])
                self.assertEqual(
                    adapted.counters[trivy.COUNTER_SEVERITY_ABSENT],
                    1,
                    msg="and the absence is counted rather than inferred from a null",
                )


class IdentifierSelectionTest(TrivyAdapterTestCase):
    """``cwe`` and ``cve`` carry one value each, chosen by ascending numeric identifier.

    The ordering is over the integer after the prefix -- year then sequence for a CVE --
    which is total, so no tie arises and no producer-order tiebreak is needed.  The
    number of records carrying more than one value is reported per tool rather than
    lost.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Adapt the derived features document once for the whole class."""
        cls.adapted = adapt_document(load_fixture(FEATURES_FIXTURE))

    def test_the_multi_valued_record_takes_the_smallest_numeric_identifier(self) -> None:
        """Assertion 9, on the fixture's own record whose two orders disagree.

        Its ``CweIDs`` are ``CWE-1333`` then ``CWE-400``.  Lexicographically the first
        is smaller; numerically the second is, and the second is what the row carries.
        An implementation that sorted the strings, or that took the producer's first
        entry, would emit ``CWE-1333`` here.
        """
        multi_valued = [
            (row, record)
            for row, record in zip(self.adapted.rows, self.adapted.records)
            if isinstance(record.record.get("CweIDs"), list)
            and len(record.record["CweIDs"]) > 1
        ]
        self.assertEqual(
            len(multi_valued),
            1,
            msg="the fixture carries exactly one multi-valued CweIDs record",
        )
        row, record = multi_valued[0]
        identifiers = record.record["CweIDs"]
        self.assertEqual(identifiers, ["CWE-1333", "CWE-400"])
        self.assertEqual(
            min(identifiers),
            "CWE-1333",
            msg="lexicographic order would choose the other one",
        )
        self.assertEqual(
            row["cwe"],
            "CWE-400",
            msg="ascending numeric order chooses 400 over 1333",
        )
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_MULTI_VALUED_CWE],
            1,
            msg="and the multi-valued record is counted, so the choice is visible",
        )

    def test_a_single_valued_record_carries_that_value_and_counts_as_single(self) -> None:
        """One identifier in, the same identifier out; absence where the record states none."""
        for row, record in zip(self.adapted.rows, self.adapted.records):
            with self.subTest(pointer=record.pointer):
                identifiers = record.record.get("CweIDs")
                if not identifiers:
                    self.assertIsNone(
                        row["cwe"], msg="no CweIDs member means no cwe is invented"
                    )
                elif len(identifiers) == 1:
                    self.assertEqual(row["cwe"], identifiers[0])
                else:
                    self.assertIn(row["cwe"], identifiers)

    def test_three_identifiers_out_of_order_still_yield_the_smallest(self) -> None:
        """A derived record whose three values put the numeric minimum last but one."""
        document = derived(FEATURES_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][3])]
        document["Results"][0]["Vulnerabilities"] = [
            copy.deepcopy(document["Results"][0]["Vulnerabilities"][0])
        ]
        identifiers = ["CWE-1004", "CWE-79", "CWE-200"]
        document["Results"][0]["Vulnerabilities"][0]["CweIDs"] = identifiers
        adapted = adapt_document(document)
        self.assertEqual(
            min(identifiers),
            "CWE-1004",
            msg="lexicographic order would choose 1004",
        )
        self.assertEqual(
            adapted.rows[0]["cwe"], "CWE-79", msg="and numeric order chooses 79"
        )
        self.assertEqual(adapted.counters[trivy.COUNTER_MULTI_VALUED_CWE], 1)

    def test_the_cve_field_is_the_vulnerability_id_where_it_is_cve_shaped(self) -> None:
        """``cve`` comes from ``VulnerabilityID`` alone, so it holds one value or none."""
        for row, record in zip(self.adapted.rows, self.adapted.records):
            with self.subTest(pointer=record.pointer):
                identifier = record.record.get("VulnerabilityID")
                if isinstance(identifier, str) and identifier.startswith("CVE-"):
                    self.assertEqual(
                        row["cve"],
                        identifier,
                        msg="a CVE-shaped identifier reaches the cve field",
                    )
                    self.assertEqual(
                        row["rule_id"],
                        identifier,
                        msg="and remains the row's rule identifier as well",
                    )
                else:
                    self.assertIsNone(
                        row["cve"],
                        msg=(
                            "a non-CVE identifier is not coerced into the cve field: "
                            "NSWG-ECO-519 is a rule identifier, not a CVE"
                        ),
                    )

    def test_no_record_can_carry_two_cves_so_the_counter_is_zero(self) -> None:
        """The multi-valued CVE counter is 0 here, and the reason is structural.

        ``cve`` is collected from ``VulnerabilityID``, which holds exactly one
        identifier, so no Trivy record can carry two.  The counter is asserted at zero
        together with that reason, rather than left as an unexplained zero a reader
        would have to take on trust.
        """
        self.assertEqual(self.adapted.counters[trivy.COUNTER_MULTI_VALUED_CVE], 0)
        for record in self.adapted.records:
            with self.subTest(pointer=record.pointer):
                identifier = record.record.get("VulnerabilityID")
                self.assertNotIsInstance(
                    identifier,
                    list,
                    msg="the member is a single identifier, never an array",
                )


class PackageCoordinateTest(TrivyAdapterTestCase):
    """One canonical package coordinate, by a defined candidate precedence.

    A package URL on the record; failing that one on the enclosing package object;
    failing that ``<ecosystem>:<name>@<version>`` from the record's own fields; failing
    that the same from the enclosing package's.  Where none can be formed the field is
    absent -- and for a dependency-oriented record that absence is a rejection with its
    own fixture, which :class:`RejectedRecordTest` asserts.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Adapt the derived features document once for the whole class."""
        cls.adapted = adapt_document(load_fixture(FEATURES_FIXTURE))

    def test_a_record_purl_is_taken_verbatim(self) -> None:
        """Level 1: the record's own ``PkgIdentifier.PURL``, unmodified."""
        seen = 0
        for row, record in zip(self.adapted.rows, self.adapted.records):
            purl = (record.record.get("PkgIdentifier") or {}).get("PURL")
            if not purl:
                continue
            seen += 1
            with self.subTest(pointer=record.pointer):
                self.assertEqual(row["package_coordinate"], purl)
        self.assertEqual(
            seen,
            self.adapted.counters[trivy.COUNTER_COORDINATE_RECORD_PURL],
            msg="and the level is counted, so the route each row took is visible",
        )
        self.assertGreater(seen, 0, msg="the fixture exercises this level")

    def test_an_enclosing_package_purl_is_the_second_candidate(self) -> None:
        """Level 2: the record states no PURL, and its ``PkgID`` matches a ``Packages`` entry.

        This is ahead of the ecosystem coordinate, so a record with a matching inventory
        entry takes that entry's PURL rather than a composed string.
        """
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_COORDINATE_PACKAGE_PURL],
            1,
            msg="exactly one row in this fixture resolves at level 2",
        )
        def inventory_of(record: Record) -> dict[str, Any]:
            """The enclosing element's package inventory, keyed by its ``ID``."""
            return {
                entry["ID"]: entry
                for entry in (record.element.get("Packages") or [])
                if isinstance(entry, dict) and entry.get("ID")
            }

        matched = [
            (row, record)
            for row, record in zip(self.adapted.rows, self.adapted.records)
            if record.record.get("PkgID")
            and not (record.record.get("PkgIdentifier") or {}).get("PURL")
            and (inventory_of(record).get(record.record["PkgID"]) or {}).get(
                "Identifier", {}
            ).get("PURL")
        ]
        self.assertEqual(
            len(matched),
            1,
            msg=(
                "exactly one record states no PURL of its own while its PkgID matches "
                "an inventory entry that states one"
            ),
        )
        row, record = matched[0]
        entry = inventory_of(record)[record.record["PkgID"]]
        self.assertEqual(
            row["package_coordinate"],
            entry["Identifier"]["PURL"],
            msg="the enclosing package's PURL, not a composed coordinate",
        )

    def test_the_ecosystem_coordinate_is_composed_with_a_lower_cased_ecosystem(self) -> None:
        """Level 3: ``<ecosystem>:<name>@<version>`` from the record's own fields."""
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_COORDINATE_RECORD_FIELDS],
            1,
            msg="exactly one row in this fixture resolves at level 3",
        )
        composed = [
            (row, record)
            for row, record in zip(self.adapted.rows, self.adapted.records)
            if row["package_coordinate"]
            and not str(row["package_coordinate"]).startswith("pkg:")
        ]
        self.assertEqual(len(composed), 1)
        row, record = composed[0]
        self.assertEqual(
            row["package_coordinate"],
            "{}:{}@{}".format(
                str(record.element["Type"]).lower(),
                record.record["PkgName"],
                record.record["InstalledVersion"],
            ),
            msg="the enclosing element's Type is the ecosystem, lower-cased",
        )

    def test_an_upper_cased_ecosystem_is_lower_cased_in_the_coordinate(self) -> None:
        """The lower-casing is asserted where it can be seen, on a derived document."""
        document = derived(FEATURES_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][3])]
        document["Results"][0]["Type"] = "BUNDLER"
        document["Results"][0]["Vulnerabilities"] = [
            copy.deepcopy(document["Results"][0]["Vulnerabilities"][1])
        ]
        adapted = adapt_document(document)
        self.assertEqual(
            adapted.rows[0]["package_coordinate"],
            "bundler:rexml@3.3.8",
            msg="the ecosystem is lower-cased and the rest is taken as stated",
        )

    def test_the_enclosing_packages_fields_are_the_fourth_candidate(self) -> None:
        """Level 4, on a derived document: the record supplies neither name nor version.

        The features fixture never reaches this level because levels 1 to 3 resolve for
        every record that gets there, so the level is exercised on a copy whose
        inventory entry carries a name and a version but no PURL.
        """
        document = derived(FEATURES_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][2])]
        element = document["Results"][0]
        element["Packages"] = [
            {"ID": "handlebars@4.5.3", "Name": "handlebars", "Version": "4.5.3"}
        ]
        record = copy.deepcopy(element["Vulnerabilities"][1])
        for member in ("PkgName", "InstalledVersion", "PkgIdentifier"):
            record.pop(member, None)
        element["Vulnerabilities"] = [record]
        adapted = adapt_document(document)
        self.assertEqual(adapted.rejections, [], msg="a coordinate was formed, so no reject")
        self.assertEqual(
            adapted.rows[0]["package_coordinate"],
            "npm:handlebars@4.5.3",
            msg="composed from the enclosing package's own fields",
        )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_COORDINATE_PACKAGE_FIELDS],
            1,
            msg="and counted at level 4, distinctly from the other three",
        )

    def test_a_section_that_supplies_no_coordinate_leaves_the_field_absent(self) -> None:
        """A secret and a misconfiguration name no package, and none is invented."""
        absent = 0
        for row, record in zip(self.adapted.rows, self.adapted.records):
            if record.section == "Vulnerabilities":
                continue
            with self.subTest(pointer=record.pointer):
                self.assertIsNone(
                    row["package_coordinate"],
                    msg=(
                        "absence is permitted for this field, and a finding about a "
                        "file is not a finding about a package"
                    ),
                )
                absent += 1
        self.assertEqual(
            absent,
            self.adapted.counters[trivy.COUNTER_COORDINATE_ABSENT],
            msg="the absences are counted rather than merely null",
        )

    def test_the_four_levels_and_the_absences_account_for_every_row(self) -> None:
        """The precedence counters sum to the row count: every row took exactly one route."""
        total = sum(
            self.adapted.counters[key]
            for key in (
                trivy.COUNTER_COORDINATE_RECORD_PURL,
                trivy.COUNTER_COORDINATE_PACKAGE_PURL,
                trivy.COUNTER_COORDINATE_RECORD_FIELDS,
                trivy.COUNTER_COORDINATE_PACKAGE_FIELDS,
                trivy.COUNTER_COORDINATE_ABSENT,
            )
        )
        self.assertEqual(
            total,
            len(self.adapted.rows),
            msg="one route per row, so no row's coordinate is unaccounted for",
        )


    # ---------------------------------------------------------------- #
    # The same-level tiebreak.
    #
    # AAP 0.5.4: "Where several candidates sit at one level, the
    # lexicographically smallest wins."  Every committed fixture has at most one
    # candidate at whichever level it resolves at, so nothing exercised the
    # tiebreak and an implementation taking the first candidate in document order
    # -- or the last -- passed every assertion in this class.  These cases put
    # competing candidates at ONE level, ordered so document order and
    # lexicographic order disagree.
    # ---------------------------------------------------------------- #

    def single_vulnerability(
        self, *, record_changes: dict[str, Any], packages: Any = None
    ) -> Any:
        """A one-record document whose vulnerability carries exactly ``record_changes``.

        Every coordinate-bearing member the features fixture's own record states is
        removed first, so the level a case resolves at is the level it sets up rather
        than one inherited from the capture.
        """
        document = derived(FEATURES_FIXTURE)
        element = copy.deepcopy(document["Results"][2])
        element["Vulnerabilities"] = [copy.deepcopy(element["Vulnerabilities"][0])]
        record = element["Vulnerabilities"][0]
        for member in ("PkgIdentifier", "PURL", "PkgName", "InstalledVersion", "PkgID"):
            record.pop(member, None)
        record.update(record_changes)
        if packages is None:
            element.pop("Packages", None)
        else:
            element["Packages"] = packages
        document["Results"] = [element]
        return document

    def test_level_one_breaks_a_tie_lexicographically_not_by_document_order(
        self,
    ) -> None:
        """Two record-level package URLs: the smaller string wins, not the first stated.

        The direct ``PURL`` member is written first and states ``pkg:maven/org.z/zeta``;
        ``PkgIdentifier.PURL`` is written second and states ``pkg:maven/org.a/alpha``.
        An implementation preferring the direct member, the first key, or the last key
        picks ``zeta`` and fails here.  Both are level-1 candidates, so the counter is
        the same either way and only the value distinguishes them.
        """
        larger = "pkg:maven/org.z/zeta@9.9"
        smaller = "pkg:maven/org.a/alpha@1.0"
        document = self.single_vulnerability(
            record_changes={
                "PURL": larger,
                "PkgIdentifier": {"PURL": smaller, "UID": "0f1e2d3c"},
            }
        )
        record = document["Results"][0]["Vulnerabilities"][0]
        self.assertEqual(
            [key for key in record if key in ("PURL", "PkgIdentifier")],
            ["PURL", "PkgIdentifier"],
            msg="document order states the larger candidate first",
        )
        self.assertLess(smaller, larger, msg="and the smaller one is the later key")

        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 1)
        self.assertEqual(
            adapted.rows[0]["package_coordinate"],
            smaller,
            msg="the lexicographically smallest of the level's candidates",
        )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_COORDINATE_RECORD_PURL],
            1,
            msg="and it resolved at level 1, which both candidates share",
        )
        self.assertEqual(adapted.counters[trivy.COUNTER_COORDINATE_ABSENT], 0)

    def test_level_two_breaks_a_tie_across_three_matched_packages(self) -> None:
        """Three inventory entries match; the winner is neither first nor last.

        With two candidates a wrong rule could still be right by accident, so the
        smallest is placed in the **middle** of document order: first-wins gives ``mid``,
        last-wins gives ``zzz``, and only the lexicographic rule gives ``aaa``.
        """
        document = self.single_vulnerability(
            record_changes={"PkgID": "org.example:shared@4.2"},
            packages=[
                {
                    "ID": "org.example:shared@4.2",
                    "Name": "shared",
                    "Version": "4.2",
                    "Identifier": {"PURL": "pkg:maven/org.m/mid@2.0"},
                },
                {
                    "ID": "org.example:shared@4.2",
                    "Name": "shared",
                    "Version": "4.2",
                    "Identifier": {"PURL": "pkg:maven/org.a/aaa@1.0"},
                },
                {
                    "ID": "org.example:shared@4.2",
                    "Name": "shared",
                    "Version": "4.2",
                    "Identifier": {"PURL": "pkg:maven/org.z/zzz@3.0"},
                },
            ],
        )
        stated = [
            package["Identifier"]["PURL"]
            for package in document["Results"][0]["Packages"]
        ]
        self.assertEqual(
            stated,
            [
                "pkg:maven/org.m/mid@2.0",
                "pkg:maven/org.a/aaa@1.0",
                "pkg:maven/org.z/zzz@3.0",
            ],
        )
        self.assertEqual(
            sorted(stated)[0],
            stated[1],
            msg="the smallest candidate is the middle one in document order",
        )

        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 1)
        self.assertEqual(
            adapted.rows[0]["package_coordinate"],
            "pkg:maven/org.a/aaa@1.0",
            msg="neither the first nor the last stated candidate",
        )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_COORDINATE_PACKAGE_PURL],
            1,
            msg="resolved at level 2, since the record states no PURL of its own",
        )
        self.assertEqual(adapted.counters[trivy.COUNTER_COORDINATE_RECORD_PURL], 0)

    def test_level_four_breaks_a_tie_across_packages_matched_by_id(self) -> None:
        """Two inventory entries share the record's ``PkgID`` and differ in name.

        Reachable only because matching is by ``PkgID``: matching by name and version
        would force every matched entry to compose the same coordinate, so there would be
        nothing to break.  The record itself states no name or version, which is what
        makes level 3 fail and level 4 the one that decides.  The smaller composed string
        is second in document order.
        """
        document = self.single_vulnerability(
            record_changes={"PkgID": "org.example:shared@4.2"},
            packages=[
                {"ID": "org.example:shared@4.2", "Name": "zeta", "Version": "9.9"},
                {"ID": "org.example:shared@4.2", "Name": "alpha", "Version": "1.0"},
            ],
        )
        element = document["Results"][0]
        self.assertEqual(element["Type"], "npm", msg="the ecosystem the Type states")
        record = element["Vulnerabilities"][0]
        self.assertNotIn("PkgName", record, msg="so level 3 cannot be formed")
        self.assertNotIn("InstalledVersion", record)

        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 1)
        self.assertEqual(
            adapted.rows[0]["package_coordinate"],
            "npm:alpha@1.0",
            msg=(
                "the lexicographically smallest composed coordinate, with the ecosystem "
                "lower-cased, and not the first entry stated"
            ),
        )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_COORDINATE_PACKAGE_FIELDS],
            1,
            msg="resolved at level 4",
        )
        for earlier in (
            trivy.COUNTER_COORDINATE_RECORD_PURL,
            trivy.COUNTER_COORDINATE_PACKAGE_PURL,
            trivy.COUNTER_COORDINATE_RECORD_FIELDS,
        ):
            with self.subTest(counter=earlier):
                self.assertEqual(
                    adapted.counters[earlier],
                    0,
                    msg="every earlier level failed, which is why level 4 decided",
                )

    def test_a_higher_level_candidate_wins_however_large_its_string(self) -> None:
        """The tiebreak is within a level and never across levels.

        The record's own package URL sorts after every level-2 and level-4 candidate the
        same document offers, and it still wins: an implementation that sorted all
        candidates from all levels together would pick the level-2 entry and fail here.
        """
        document = self.single_vulnerability(
            record_changes={
                "PkgIdentifier": {"PURL": "pkg:maven/org.z/zzz-record@9.9"},
                "PkgID": "org.example:shared@4.2",
            },
            packages=[
                {
                    "ID": "org.example:shared@4.2",
                    "Name": "aaa",
                    "Version": "0.1",
                    "Identifier": {"PURL": "pkg:maven/org.a/aaa-package@0.1"},
                }
            ],
        )
        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 1)
        self.assertEqual(
            adapted.rows[0]["package_coordinate"],
            "pkg:maven/org.z/zzz-record@9.9",
            msg="level 1 governs, whatever the lower levels offer",
        )
        self.assertLess(
            "pkg:maven/org.a/aaa-package@0.1",
            adapted.rows[0]["package_coordinate"],
            msg="and the losing candidate really does sort first",
        )
        self.assertEqual(adapted.counters[trivy.COUNTER_COORDINATE_RECORD_PURL], 1)
        self.assertEqual(adapted.counters[trivy.COUNTER_COORDINATE_PACKAGE_PURL], 0)


class SecretRedactionTest(TrivyAdapterTestCase):
    """No secret value reaches any dataset field, and none is in this tree.

    A secret record's ``message`` is its ``Match``, which Trivy emits **already
    redacted**, and no other member of a secret record is read -- ``Code`` carries the
    surrounding source lines and is never touched.  This tree is committed to git, whose
    only relevant ignore line matches ``artifacts/``, so the guarantee is asserted
    structurally rather than left to inspection.
    """

    def test_every_match_in_every_fixture_is_wholly_redacted(self) -> None:
        """Each ``Match`` this corpus carries is a run of asterisks and nothing else.

        Asserted by character content rather than by appearance: a value that merely
        looked redacted would be a value in a committed file.
        """
        examined = 0
        for name in required_fixture_names():
            document = load_fixture(name)
            for record in section_walk(document):
                if record.section != "Secrets":
                    continue
                match = record.record.get("Match")
                if match is None:
                    continue
                with self.subTest(fixture=name, pointer=record.pointer):
                    self.assertIsInstance(match, str)
                    self.assertEqual(
                        set(match),
                        {"*"},
                        msg="the whole value is asterisks, so it discloses nothing",
                    )
                    examined += 1
        self.assertGreater(
            examined, 0, msg="the corpus does carry redacted matches to assert over"
        )

    def test_a_secret_rows_message_is_the_redacted_match_and_no_other_member(self) -> None:
        """The message is the ``Match`` where one exists, and the ``Title`` where none does."""
        adapted = adapt_document(load_fixture(FEATURES_FIXTURE))
        secrets = [
            (row, record)
            for row, record in zip(adapted.rows, adapted.records)
            if record.section == "Secrets"
        ]
        self.assertEqual(len(secrets), 2, msg="both secret records are exercised")
        for row, record in secrets:
            with self.subTest(pointer=record.pointer):
                match = record.record.get("Match")
                if match is not None:
                    self.assertEqual(
                        row["message"],
                        match,
                        msg="the redacted Match is emitted as-is",
                    )
                    self.assertEqual(set(row["message"]), {"*"})
                else:
                    self.assertEqual(
                        row["message"],
                        record.record["Title"],
                        msg="and with no Match the Title is the message",
                    )

    def test_no_field_of_any_row_carries_a_secret_bearing_member(self) -> None:
        """No row field holds any textual member of a secret record beyond the ones read.

        The members this adapter reads from a secret record are ``RuleID``, ``Title``,
        ``Match``, ``Severity`` and ``StartLine``.  Every **textual** member outside that
        set is asserted absent from every field of every row, so a future record shape
        carrying a value under a new key cannot leak through a field already covered
        here.  Numeric members are excluded on purpose: ``EndLine`` legitimately equals
        ``StartLine`` on a single-line match, and a coincidence of integers is not a
        disclosure -- a secret is text.
        """
        adapted = adapt_document(load_fixture(FEATURES_FIXTURE))
        examined = 0
        for row, record in zip(adapted.rows, adapted.records):
            if record.section != "Secrets":
                continue
            unread = {
                key: value
                for key, value in record.record.items()
                if key not in ("RuleID", "Title", "Match", "Severity", "StartLine")
                and isinstance(value, str)
                and value
            }
            for key, value in unread.items():
                examined += 1
                for field in emit.FIELDS:
                    with self.subTest(pointer=record.pointer, member=key, field=field):
                        self.assertNotEqual(
                            row[field],
                            value,
                            msg=f"the unread member {key} reached no dataset field",
                        )
        self.assertGreater(
            examined,
            0,
            msg="the fixture's secrets do carry unread textual members to assert over",
        )

    def test_a_planted_code_excerpt_reaches_no_field(self) -> None:
        """A derived secret carrying an unread member: the row is unchanged by it.

        The planted value is an obvious placeholder rather than anything resembling a
        credential, since this assertion lives in a committed file.
        """
        document = derived(FEATURES_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][1])]
        placeholder = "REDACTED_PLACEHOLDER_NEVER_A_REAL_VALUE"
        for record in document["Results"][0]["Secrets"]:
            record["Code"] = {"Lines": [{"Content": placeholder}]}
            record["Secret"] = placeholder
        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 2)
        for index, row in enumerate(adapted.rows):
            for field in emit.FIELDS:
                with self.subTest(row=index, field=field):
                    self.assertNotIn(
                        placeholder,
                        str(row[field]),
                        msg="a member the adapter does not read cannot reach a field",
                    )


class UnsupportedSectionStopsTheRunTest(TrivyAdapterTestCase):
    """A non-empty unsupported finding section stops the run, quoting what it observed.

    Why this is a stop and not a warning, stated once because it is the whole
    justification: **a silently dropped section would let reconciliation pass while real
    tool output vanished.**  The count unit of the independent counting traversal is
    ``Results[]`` x the three *supported* sections, so an ignored ``Licenses`` array is
    absent from both sides of ``raw records = rows + rejections``.  The identity would
    balance exactly -- over a count unit that never saw the dropped array -- while four
    records of real tool output left no trace in the dataset, the counters or the
    reconciliation.  A drop that unbalances the identity is caught by the identity; a
    drop invisible to both sides is caught by nothing, and an empty or silently-reduced
    result set is indistinguishable from a clean scan.  That counterfactual arithmetic
    is itself asserted below, so the reasoning is checkable rather than merely stated.

    The three deliberate non-stops are asserted too: an unsupported member that is
    present and **empty**, one that is absent, and one holding a scalar.  Validated
    empty is the requirement, not validated absent -- and an implementation that stopped
    on the members' mere presence would fail on the features fixture, where both are
    present and empty on all six elements.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Load the fixture and its expectation once for the whole class."""
        cls.expected = load_expected("halt-trivy-unsupported-section")

    def offending_element(self, section: str) -> Any:
        """Return a copy of the fixture element whose ``section`` array is non-empty."""
        for element in load_fixture(UNSUPPORTED_SECTION_FIXTURE)["Results"]:
            if element.get(section):
                return copy.deepcopy(element)
        raise AssertionError(  # pragma: no cover - the fixture carries both
            f"blocking gap: no element of {UNSUPPORTED_SECTION_FIXTURE} holds a "
            f"non-empty {section} array, so that member cannot be covered"
        )

    def document_offending_only_in(self, section: str) -> Any:
        """Return the features fixture with one offending element appended.

        Appending puts the offence in the **last** element, which is what establishes
        that the validation pass runs over every element rather than stopping at the
        first: a defect in the last element must stop the run as surely as one in the
        first, or a partial dataset would be produced from the elements already walked.
        """
        document = derived(FEATURES_FIXTURE)
        document["Results"] = document["Results"] + [self.offending_element(section)]
        return document

    def test_a_non_empty_licenses_array_stops_the_run(self) -> None:
        """Assertion 12, over the committed fixture, by the exact exception type."""
        with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
            adapt_document(load_fixture(UNSUPPORTED_SECTION_FIXTURE))
        observed = self.expected["halt"]["observed_attributes"]
        error = caught.exception
        self.assertEqual(error.reason, trivy.HALT_UNSUPPORTED_SECTION)
        self.assertEqual(error.reason, observed["reason"])
        self.assertEqual(error.section, observed["section"])
        self.assertEqual(error.target, observed["target"])
        self.assertEqual(error.result_index, observed["result_index"])
        self.assertEqual(error.element_count, observed["element_count"])
        self.assertEqual(error.note, observed["note"])
        self.assertEqual(
            dict(error.structure),
            observed["structure"],
            msg="the observed structure is the expectation's, key for key",
        )

    def test_a_non_empty_experimental_modified_findings_array_stops_the_run(self) -> None:
        """Assertion 13: the second member, non-empty on its own evidence.

        In the committed fixture this array is never reached -- the ``Licenses`` element
        precedes it and raises first -- so it is exercised on a derived document that
        carries only this offence.  Recorded rather than skipped: a reader comparing the
        fixture against the stop would otherwise take the unmentioned array for one the
        validator missed.
        """
        section = "ExperimentalModifiedFindings"
        with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
            adapt_document(self.document_offending_only_in(section))
        error = caught.exception
        self.assertEqual(error.section, section)
        self.assertEqual(error.reason, trivy.HALT_UNSUPPORTED_SECTION)
        self.assertEqual(
            error.element_count, 1, msg="the one object the derived array holds"
        )
        self.assertEqual(
            error.result_index,
            len(load_fixture(FEATURES_FIXTURE)["Results"]),
            msg="the offence is in the appended last element, and was still reached",
        )

    def test_every_unsupported_section_is_covered_by_a_raising_case(self) -> None:
        """Assertion 15: iterate the constant, so a new member without a case is visible.

        A section added to :data:`trivy.UNSUPPORTED_FINDING_SECTIONS` with no fixture
        element to raise on fails here by name, rather than silently enlarging the set of
        sections nothing exercises.
        """
        covered: dict[str, int] = {}
        for section in trivy.UNSUPPORTED_FINDING_SECTIONS:
            with self.subTest(section=section):
                with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
                    adapt_document(self.document_offending_only_in(section))
                self.assertEqual(
                    caught.exception.section,
                    section,
                    msg="the stop names the member it was raised on",
                )
                covered[section] = caught.exception.element_count or 0
        self.assertEqual(
            sorted(covered),
            sorted(trivy.UNSUPPORTED_FINDING_SECTIONS),
            msg="every member of the constant has a raising case",
        )
        for section, count in covered.items():
            with self.subTest(section=section):
                self.assertGreater(
                    count, 0, msg="and each raising case was raised on a non-empty array"
                )

    def test_an_empty_unsupported_section_does_not_stop_the_run(self) -> None:
        """Assertion 14, first half: validated empty is the requirement.

        Both members are present and empty on every element of the features fixture, so
        the passing case is asserted there, and then on the three further empty shapes
        the adapter documents as non-stops: an empty object, a null and a scalar.  None
        of them holds a finding record to drop.
        """
        document = load_fixture(FEATURES_FIXTURE)
        for element in document["Results"]:
            for section in trivy.UNSUPPORTED_FINDING_SECTIONS:
                self.assertEqual(
                    element[section],
                    [],
                    msg="the fixture carries both members present and empty",
                )
        self.assertIsNone(
            trivy.validate_finding_sections(document, tool=TOOL),
            msg="so the validation returns rather than raising",
        )
        self.assertEqual(len(adapt_document(document).rows), 9)
        for label, value in (("empty object", {}), ("null", None), ("scalar", 0)):
            with self.subTest(shape=label):
                variant = derived(FEATURES_FIXTURE)
                for element in variant["Results"]:
                    for section in trivy.UNSUPPORTED_FINDING_SECTIONS:
                        element[section] = value
                self.assertIsNone(trivy.validate_finding_sections(variant, tool=TOOL))
                self.assertEqual(
                    len(adapt_document(variant).rows),
                    9,
                    msg="and every row is still emitted",
                )

    def test_an_absent_unsupported_section_does_not_stop_the_run(self) -> None:
        """Assertion 14, second half: the members are optional, and absence is ordinary."""
        document = derived(FEATURES_FIXTURE)
        for element in document["Results"]:
            for section in trivy.UNSUPPORTED_FINDING_SECTIONS:
                element.pop(section, None)
                self.assertNotIn(section, element)
        self.assertIsNone(trivy.validate_finding_sections(document, tool=TOOL))
        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 9)
        self.assertEqual(adapted.rejections, [])

    def test_the_captured_artifact_is_the_unaltered_witness_that_absence_is_ordinary(
        self,
    ) -> None:
        """The run's own artifact carries neither member, and normalizes without a stop.

        The test above establishes the rule on a document this module edited.  This one
        establishes it on a document nobody edited: ``fixtures/trivy.json`` is
        ``harness/artifacts/raw/trivy.json`` byte for byte, and Trivy wrote no
        ``Licenses`` and no ``ExperimentalModifiedFindings`` member on any of its three
        elements.  An implementation that required either member to be present -- the
        mirror-image defect of one that halts on their presence -- would stop on this
        run's real output, and that is what this asserts cannot happen.
        """
        document = load_fixture(POSITIVE_FIXTURE)
        for index, element in enumerate(document["Results"]):
            for section in trivy.UNSUPPORTED_FINDING_SECTIONS:
                with self.subTest(result_index=index, section=section):
                    self.assertNotIn(
                        section,
                        element,
                        msg="the captured artifact states no unsupported finding member",
                    )
        self.assertIsNone(
            trivy.validate_finding_sections(document, tool=TOOL),
            msg="validation returns on the run's own artifact rather than raising",
        )
        adapted = adapt_document(document)
        self.assertEqual(
            len(adapted.rows),
            len(load_expected(POSITIVE_EXPECTED_STEM)["rows"]),
            msg="and every captured record became its hand-verified row",
        )
        self.assertEqual(adapted.rejections, [], msg="with nothing rejected")

    def test_a_non_empty_object_under_an_unsupported_key_also_stops_the_run(self) -> None:
        """The member's name says it holds findings, so non-empty content of any shape stops.

        An object is not a shape Trivy emits under either key, so its contents cannot be
        claimed to have been read.  The stop carries no element count -- the value is not
        an array -- and carries a note saying so, which is how a halt report tells the two
        apart.
        """
        document = derived(FEATURES_FIXTURE)
        document["Results"][0]["Licenses"] = {"Name": "a licence object, not an array"}
        with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
            adapt_document(document)
        error = caught.exception
        self.assertEqual(error.reason, trivy.HALT_UNSUPPORTED_SECTION)
        self.assertEqual(error.section, "Licenses")
        self.assertIsNone(
            error.element_count, msg="an object has no element count to report"
        )
        self.assertTrue(error.note, msg="and the shape difference is noted instead")

    def test_the_exception_quotes_the_observed_structure(self) -> None:
        """Assertion 16: the message names the section and carries the structure excerpt."""
        with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
            adapt_document(load_fixture(UNSUPPORTED_SECTION_FIXTURE))
        error = caught.exception
        message = str(error)
        self.assertIn(
            error.section,
            message,
            msg="the offending section is named in the message a report quotes",
        )
        self.assertIn(TOOL, message, msg="and so is the tool")
        self.assertIn(
            str(error.result_index),
            message,
            msg="and the element's index, so the record can be found again",
        )
        self.assertIn(
            error.structure_excerpt,
            message,
            msg="the observed structure is in the message verbatim",
        )
        self.assertTrue(error.structure, msg="and it is not empty")
        record = error.as_dict()
        self.assertEqual(record["tool"], TOOL)
        self.assertEqual(record["reason"], trivy.HALT_UNSUPPORTED_SECTION)
        self.assertEqual(record["section"], error.section)
        self.assertEqual(record["message"], message)
        json.dumps(record)  # the run record must be serialisable as it stands

    def test_the_quoted_structure_carries_keys_and_types_but_no_value(self) -> None:
        """The excerpt is shape, not content -- which is what makes it safe to publish.

        These logs are preserved verbatim, so a structure that quoted values would put
        whatever the offending records held into a published file.  Every string value of
        every offending record is asserted absent from the message, while its keys are
        asserted present.
        """
        document = load_fixture(UNSUPPORTED_SECTION_FIXTURE)
        with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
            adapt_document(document)
        message = str(caught.exception)
        offending = document["Results"][caught.exception.result_index][
            caught.exception.section
        ]
        for index, record in enumerate(offending):
            for key, value in record.items():
                with self.subTest(element=index, member=key):
                    self.assertIn(
                        key, message, msg="the member's name is part of the structure"
                    )
                    if isinstance(value, str) and value:
                        self.assertNotIn(
                            value,
                            message,
                            msg="but its value is not, at any length",
                        )

    def test_the_stop_is_not_a_counted_rejection(self) -> None:
        """It is raised, never returned, and its reason is no rejection class.

        A rejection is counted under a named class and the run continues, which is
        exactly the outcome this condition rules out.  The expectation records no rows
        and no rejections for this fixture, and both are asserted empty.
        """
        with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
            adapt_document(load_fixture(UNSUPPORTED_SECTION_FIXTURE))
        error = caught.exception
        self.assertNotIsInstance(error, paths.Rejection)
        self.assertIn(error.reason, trivy.HALT_REASONS)
        self.assertNotIn(error.reason, paths.REJECT_CLASSES)
        self.assertFalse(paths.is_reject_class(error.reason))
        self.assertEqual(self.expected["rows"], [], msg="the expectation states no row")
        self.assertEqual(
            self.expected["rejections"], [], msg="and no rejection either"
        )
        self.assertEqual(self.expected["outcome"], "halt")

    def test_the_counterfactual_that_a_skip_would_have_balanced(self) -> None:
        """The arithmetic a tolerated skip would have satisfied, measured rather than argued.

        The fixture's first six elements are the features fixture's, so a skipping
        implementation would emit that fixture's nine rows, reject nothing, and satisfy
        ``9 == 9 + 0`` against a supported-section count of nine -- while the four
        records in the two unsupported arrays left no trace anywhere.  Both numbers are
        measured here, which is what makes the justification in this class's docstring a
        checkable claim rather than a stated one.
        """
        document = load_fixture(UNSUPPORTED_SECTION_FIXTURE)
        supported_records = len(section_walk(document))
        positive = adapt_document(load_fixture(FEATURES_FIXTURE))
        self.assertEqual(
            supported_records,
            self.expected["counts"]["raw_finding_records"],
            msg="the supported-section count over this fixture is the expectation's",
        )
        self.assertEqual(
            supported_records,
            len(positive.rows) + len(positive.rejections),
            msg="so a skip would have balanced the identity exactly",
        )
        dropped = sum(
            len(element.get(section) or [])
            for element in document["Results"]
            for section in trivy.UNSUPPORTED_FINDING_SECTIONS
        )
        self.assertEqual(
            dropped,
            4,
            msg="while these four records would have vanished without trace",
        )
        self.assertEqual(
            self.expected["guarded_failure"]["arithmetic_if_the_guard_were_removed"][
                "records_that_would_vanish"
            ],
            dropped,
            msg="which is the number the expectation records for the counterfactual",
        )


def adapt_negative(stem: str) -> Adapted:
    """Adapt one negative fixture the way its own expected file prescribes.

    Seven of the eight go through ``trivy.adapt``.  The unattributable-section fixture
    goes through the public seam instead, because ``adapt``'s iteration is section-bound
    by construction and cannot produce that condition -- which is a property of the
    adapter's design, not a convenience of this test.
    """
    if stem == SEAM_STEM:
        return adapt_through_seam(
            load_fixture(f"{stem}.json"),
            designated=SEAM_DESIGNATED_RECORD,
            section_key=SEAM_SECTION_KEY,
        )
    return adapt_fixture(stem)


def expected_rejection_section(entry: Any) -> Any:
    """The section a rejection was produced under, as the expected file spells it.

    The seam's expectation carries two section keys because they differ there and only
    there: the document says ``Vulnerabilities``, the call says ``Packages``, and the
    rejection reports the one it was given.
    """
    if "section_passed_to_adapt_record" in entry:
        return entry["section_passed_to_adapt_record"]
    return entry["section"]


class StructuralHaltContract:
    """The contract every one of ``trivy.HALT_REASONS``' four reasons is held to.

    ``validate_finding_sections`` raises :class:`trivy.UnsupportedTrivySection` on four
    distinct structural conditions, checked in the order ``HALT_REASONS`` declares them.
    Each has one committed fixture that isolates its own condition, one hand-verified
    expected file recording the stop rather than rows, and one concrete class below.
    :class:`HaltReasonCoverageTest` iterates the closed tuple and fails by name if any of
    the three is missing, so a fifth reason added to the adapter cannot arrive untested.

    A mixin rather than a ``TestCase``: ``unittest`` collects it only through the
    concrete classes, each of which names its reason and its fixture stem and asserts
    nothing that is written for one document.

    What the contract asserts, for every reason:

    * the reason is the module's own constant and a member of the closed tuple;
    * the expectation names the fixture under test and records its digest;
    * the adapter raises with **every** recorded attribute equal -- reason, section,
      target, result index, element count, note and the whole structure mapping -- so a
      test cannot pass on "something raised";
    * the composed message names the reason and quotes the structural excerpt;
    * the same halt comes out of ``validate_finding_sections`` called directly, which is
      what establishes it is pass one and that ``adapt`` propagates it untouched;
    * every condition checked **before** this one is absent from the fixture, so the
      halt is attributable to this reason rather than to an earlier branch that happened
      to fire;
    * the independent counting traversal returns the recorded raw count, and the
      counterfactual identity in ``guarded_failure`` balances -- which is the whole
      reason a halt rather than a skip is required;
    * the outcome is not a member of ``paths.REJECT_CLASSES``.

    Attributes:
        REASON: The member of ``trivy.HALT_REASONS`` this class covers.
        FIXTURE_STEM: The committed fixture and expected stem isolating that condition.
    """

    #: Set by each concrete class; left unset so a subclass that forgets fails loudly.
    REASON: str = ""
    FIXTURE_STEM: str = ""

    @classmethod
    def setUpClass(cls) -> None:
        """Load the expectation once, and hold the fixture's filename."""
        super().setUpClass()
        if not cls.REASON or not cls.FIXTURE_STEM:  # pragma: no cover - defended
            raise RuntimeError(
                f"{cls.__name__} inherits the structural-halt contract without naming a "
                "reason and a fixture stem, so it would assert nothing"
            )
        cls.expected = load_expected(cls.FIXTURE_STEM)
        cls.fixture_name = f"{cls.FIXTURE_STEM}.json"

    # -- helpers ---------------------------------------------------------------------

    @property
    def observed(self) -> Any:
        """The attributes this reason's expected file records for the raised halt."""
        return self.expected["halt"]["observed_attributes"]

    def document(self) -> Any:
        """A fresh parse of this class's committed fixture."""
        return load_fixture(self.fixture_name)

    def raised_by_adapt(self, document: Any = None) -> trivy.UnsupportedTrivySection:
        """Adapt a document and return the halt it raised, failing if it raised none."""
        with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
            adapt_document(self.document() if document is None else document)
        return caught.exception

    # -- the contract ----------------------------------------------------------------

    def test_the_reason_is_the_modules_own_constant_in_the_closed_tuple(self) -> None:
        """The reason under test is ``trivy``'s, not a string spelled in this file."""
        self.assertIn(
            self.REASON,
            trivy.HALT_REASONS,
            msg="the reason is a member of the closed set of four",
        )
        self.assertEqual(
            self.expected["halt"]["reason"],
            self.REASON,
            msg="and the expectation records the same reason",
        )
        constant = self.expected["halt"]["reason_constant"]
        self.assertTrue(
            constant.startswith("trivy."),
            msg="the expectation names the constant in the module it belongs to",
        )
        self.assertEqual(
            getattr(trivy, constant.split(".", 1)[1]),
            self.REASON,
            msg=f"{constant} is the module attribute holding this reason",
        )
        self.assertEqual(
            self.expected["outcome"], "halt", msg="and the outcome is a halt, not rows"
        )
        self.assertEqual(self.expected["rows"], [], msg="with no row expected")
        self.assertEqual(self.expected["rejections"], [], msg="and no rejection")
        self.assertEqual(self.expected["counts"]["rows"], 0)
        self.assertEqual(self.expected["counts"]["rejections"], 0)

    def test_the_expectation_names_this_fixture_and_records_its_digest(self) -> None:
        """The pair is wired to each other, so neither can drift alone."""
        block = self.expected["fixture"]
        self.assertEqual(
            block["path"],
            f"oss-scan-results/adapter-tests/fixtures/{self.fixture_name}",
        )
        location = FIXTURES_DIR / self.fixture_name
        self.assertEqual(
            _sha256(location), block["sha256"], msg="the recorded sha256 is the file's"
        )
        self.assertEqual(location.stat().st_size, block["bytes"])

    def test_the_adapter_raises_with_every_recorded_attribute(self) -> None:
        """Reason, section, target, index, count, note and structure -- all of them.

        Asserting the type alone would pass on any of the four reasons firing for any
        structure, which is how a halt test becomes a test that something went wrong
        somewhere.  Every attribute the exception carries is compared, and the structure
        mapping is compared whole rather than by one of its keys.
        """
        error = self.raised_by_adapt()
        self.assertEqual(error.reason, self.REASON)
        self.assertEqual(error.reason, self.observed["reason"])
        self.assertEqual(error.section, self.observed["section"])
        self.assertEqual(error.target, self.observed["target"])
        self.assertEqual(error.result_index, self.observed["result_index"])
        self.assertEqual(error.element_count, self.observed["element_count"])
        self.assertEqual(error.note, self.observed["note"])
        self.assertEqual(
            dict(error.structure),
            self.observed["structure"],
            msg="the observed structure is the expectation's, key for key",
        )
        self.assertEqual(
            error.as_dict()["tool"],
            self.observed["tool"],
            msg="and the dict form the run record would carry names this tool",
        )

    def test_the_structure_diagnostics_describe_shape_and_never_content(self) -> None:
        """The excerpt is quotable: JSON types, key names, counts -- no value.

        AAP 0.5.4 requires the halt to quote the observed structure, and equally
        requires that no adapter carry a value into a published field.  The structure is
        therefore asserted to be describable -- every leaf a JSON type name or a count --
        rather than merely present.
        """
        error = self.raised_by_adapt()
        structure = dict(error.structure)
        self.assertIn("json_type", structure, msg="the excerpt names the JSON type")
        self.assertIn(
            structure["json_type"],
            ("array", "object", "string", "number", "boolean", "null"),
            msg="in JSON's own vocabulary",
        )
        self.assertEqual(
            error.structure_excerpt,
            json.dumps(structure, sort_keys=False, default=str),
            msg="the excerpt is the compact rendering of that same mapping",
        )
        self.assertIn(
            error.structure_excerpt,
            str(error),
            msg="and the message quotes it verbatim, so the fault is legible",
        )
        self.assertIn(self.REASON, str(error), msg="the message names the reason")
        self.assertIn(TOOL, str(error), msg="and the tool")

    def test_the_same_halt_comes_out_of_the_validation_pass_directly(self) -> None:
        """It is pass one, and ``adapt`` propagates it untouched.

        Calling the validator directly and comparing the two exceptions is what
        establishes both halves: the condition is decided before any row is built, and
        ``adapt`` neither wraps it, downgrades it to a rejection, nor re-raises it with
        different attributes.
        """
        with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
            trivy.validate_finding_sections(self.document(), tool=TOOL)
        direct = caught.exception
        through_adapt = self.raised_by_adapt()
        for attribute in ("reason", "section", "target", "result_index", "element_count"):
            with self.subTest(attribute=attribute):
                self.assertEqual(
                    getattr(direct, attribute),
                    getattr(through_adapt, attribute),
                    msg=f"{attribute} is the same whichever entry point raised",
                )
        self.assertEqual(dict(direct.structure), dict(through_adapt.structure))
        self.assertEqual(str(direct), str(through_adapt))
        self.assertNotIsInstance(
            through_adapt,
            trivy.TrivyAdapterError,
            msg=(
                "and the halt is not a caller fault: a caller catching TrivyAdapterError "
                "must not be able to swallow it"
            ),
        )

    def test_every_condition_checked_before_this_one_is_absent_from_the_fixture(
        self,
    ) -> None:
        """The halt is attributable to this reason, not to an earlier branch.

        ``validate_finding_sections`` checks the four conditions in the order
        ``HALT_REASONS`` states them and raises on the first it meets, so a fixture that
        also tripped an earlier condition would raise that one instead -- and a test
        asserting only "it raised" would still pass.  Each earlier condition is
        therefore shown absent by walking the fixture, and the expectation's own
        ``halt_conditions_excluded`` block is required to say so too.
        """
        position = trivy.HALT_REASONS.index(self.REASON)
        earlier = trivy.HALT_REASONS[:position]
        excluded = self.expected["halt"]["halt_conditions_excluded"]
        elements = [
            element
            for element in self.document()["Results"]
            if isinstance(element, dict)
        ]
        self.assertTrue(elements, msg="the fixture carries at least one element")
        for reason in earlier:
            with self.subTest(earlier_reason=reason):
                self.assertIn(
                    reason,
                    excluded,
                    msg=(
                        f"the expectation states why {reason} does not apply, so the "
                        "exclusion is recorded rather than assumed"
                    ),
                )
        if trivy.HALT_UNSUPPORTED_SECTION in earlier:
            for index, element in enumerate(elements):
                for section in trivy.UNSUPPORTED_FINDING_SECTIONS:
                    with self.subTest(result_index=index, section=section):
                        self.assertFalse(
                            element.get(section),
                            msg=(
                                "no unsupported finding member is non-empty, so "
                                "condition 1 cannot be what fired"
                            ),
                        )
        if trivy.HALT_UNKNOWN_SECTION in earlier:
            for index, element in enumerate(elements):
                for key, value in element.items():
                    if key in trivy.RESULT_KNOWN_KEYS:
                        continue
                    with self.subTest(result_index=index, member=key):
                        self.assertFalse(
                            isinstance(value, list)
                            and any(isinstance(item, dict) for item in value),
                            msg=(
                                f"the unknown member {key!r} is not a non-empty array of "
                                "objects, so condition 2 cannot be what fired"
                            ),
                        )
        if trivy.HALT_SECTION_NOT_AN_ARRAY in earlier:
            for index, element in enumerate(elements):
                for section in trivy.SUPPORTED_SECTIONS:
                    if section not in element:
                        continue
                    value = element[section]
                    with self.subTest(result_index=index, section=section):
                        self.assertTrue(
                            value is None or isinstance(value, list),
                            msg=(
                                "every supported section present is an array or null, so "
                                "condition 3 cannot be what fired"
                            ),
                        )

    def test_the_independent_traversal_returns_the_recorded_raw_count(self) -> None:
        """And the counterfactual identity balances, which is why a skip is not enough.

        ``reconcile.count_records`` walks the artifact without building a row, so the
        number it returns is what a dataset built by a defective implementation would be
        reconciled against.  For every one of these four conditions that number leaves
        the fault invisible: the identity balances while the artifact's output is not
        fully represented.  That arithmetic is stated in each expectation's
        ``guarded_failure`` block and asserted here against the traversal itself.
        """
        counted = reconcile.count_records(TOOL, self.document())
        self.assertEqual(
            counted,
            self.expected["counts"]["raw_finding_records"],
            msg="the independent traversal returns the recorded raw record count",
        )
        arithmetic = self.expected["guarded_failure"][
            "arithmetic_if_the_guard_were_removed"
        ]
        self.assertEqual(
            arithmetic["raw_finding_records"],
            counted,
            msg="the counterfactual uses that same count",
        )
        self.assertEqual(
            arithmetic["raw_finding_records"],
            arithmetic["rows"] + arithmetic["rejections"],
            msg=(
                "and it balances: a defective implementation would satisfy the "
                "reconciliation identity, which is why the halt and not the identity is "
                "what catches this"
            ),
        )
        self.assertTrue(arithmetic["holds"], msg="stated as holding, and it does")

    def test_this_outcome_is_never_a_counted_rejection(self) -> None:
        """A halt is not a rejection class, and the expectation may not name one."""
        self.assertNotIn(
            self.REASON,
            paths.REJECT_CLASSES,
            msg="the reason is not one of the ten rejection classes",
        )
        self.assertFalse(paths.is_reject_class(self.REASON))
        not_a_rejection = self.expected["halt"]["not_a_rejection"]
        self.assertFalse(not_a_rejection["is_reject_class"])
        self.assertIsNone(not_a_rejection["reject_class"])
        self.assertEqual(
            self.expected["counts"]["rejections_by_class"],
            {},
            msg="and no class is counted for it",
        )


class UnsupportedFindingSectionHaltTest(StructuralHaltContract, TrivyAdapterTestCase):
    """Condition 1: a known finding section this dataset does not support, non-empty.

    :class:`UnsupportedSectionStopsTheRunTest` covers this reason at depth -- both
    members of ``UNSUPPORTED_FINDING_SECTIONS``, the object form, the three non-stops and
    the counterfactual.  This class exists so the reason is held to the same contract as
    the other three and is reachable from the closed-set iteration, rather than being the
    one reason whose coverage lives in a differently-shaped test.
    """

    REASON = trivy.HALT_UNSUPPORTED_SECTION
    FIXTURE_STEM = "halt-trivy-unsupported-section"


class UnknownFindingSectionHaltTest(StructuralHaltContract, TrivyAdapterTestCase):
    """Condition 2: a member outside ``RESULT_KNOWN_KEYS`` holding findings.

    A member this adapter has never heard of, carrying a non-empty array of objects, is
    read as a finding section it cannot map rather than as metadata to walk past.  The
    fixture puts one on the **last** of the capture's three elements, so the assertion
    also establishes that the validation pass reaches every element.

    The boundary matters as much as the condition: the same unknown key holding an
    object, or holding an array of strings, must **not** halt, or the adapter would stop
    on the next metadata field Trivy adds.  Both non-halting shapes are asserted below.
    """

    REASON = trivy.HALT_UNKNOWN_SECTION
    FIXTURE_STEM = "halt-trivy-unknown-section"

    def test_the_offending_member_is_outside_the_known_result_keys(self) -> None:
        """The member named by the halt is genuinely unknown, and its note says which."""
        section = self.observed["section"]
        self.assertNotIn(
            section,
            trivy.RESULT_KNOWN_KEYS,
            msg=f"{section!r} is outside the known Result members",
        )
        element = self.document()["Results"][self.observed["result_index"]]
        self.assertIn(section, element, msg="and the fixture element carries it")
        self.assertEqual(
            len(element[section]),
            self.observed["element_count"],
            msg="whose element count is the one the halt reports",
        )
        error = self.raised_by_adapt()
        for known in trivy.RESULT_KNOWN_KEYS:
            with self.subTest(known_member=known):
                self.assertIn(
                    known,
                    error.note,
                    msg=(
                        "the note lists every known member, so a reader can see what the "
                        "adapter does recognise"
                    ),
                )

    def test_an_unknown_member_holding_an_object_does_not_stop_the_run(self) -> None:
        """Metadata, not findings: an unknown member holding an object does not halt.

        The policy is narrow and is exactly AAP 0.5.4's: a **non-empty finding-shaped
        array** outside the three supported sections stops the run with the observed
        structure quoted, and nothing else does.  A value that is a JSON object holds no
        such array, whatever member name carries it, so it is walked past as metadata.
        This asserts that policy and claims nothing about Trivy's schema being
        object-only outside the finding sections -- ``Result`` also declares non-finding
        arrays, and ``trivy.RESULT_KNOWN_KEYS`` names the ones 0.74.0 emits so they never
        reach this boundary.  The section is removed and replaced with an object of the
        same key name, so the only difference from the raising document is the JSON type
        of the value.
        """
        document = self.document()
        section = self.observed["section"]
        document["Results"][self.observed["result_index"]][section] = {
            "SchemaHint": "an object under the same key name",
            "Count": 2,
        }
        self.assertIsNone(
            trivy.validate_finding_sections(document, tool=TOOL),
            msg="an unknown object is metadata and does not halt",
        )
        adapted = adapt_document(document)
        self.assertEqual(
            len(adapted.rows),
            self.expected["counts"]["raw_finding_records"],
            msg="and every supported record still becomes a row",
        )
        self.assertEqual(adapted.rejections, [], msg="with nothing rejected")

    def test_an_unknown_member_holding_an_array_of_strings_does_not_stop_the_run(
        self,
    ) -> None:
        """Not finding-shaped: every finding section Trivy emits is an array of objects.

        A list of references or of names under an unknown key holds no record this
        adapter could have mapped, so it is not the condition.  ``_is_object_array``'s
        ``any`` rather than ``all`` is asserted too: a mixed array is at least partly
        finding-shaped and does halt.
        """
        section = self.observed["section"]
        index = self.observed["result_index"]
        document = self.document()
        document["Results"][index][section] = ["a-string", "another-string"]
        self.assertIsNone(
            trivy.validate_finding_sections(document, tool=TOOL),
            msg="an array of scalars is not finding-shaped and does not halt",
        )
        self.assertEqual(
            len(adapt_document(document).rows),
            self.expected["counts"]["raw_finding_records"],
            msg="and every supported record still becomes a row",
        )
        mixed = self.document()
        mixed["Results"][index][section] = ["a-string", {"ID": "TF-0003"}]
        error = self.raised_by_adapt(mixed)
        self.assertEqual(
            error.reason,
            trivy.HALT_UNKNOWN_SECTION,
            msg="a mixed array carries at least one object and does halt",
        )
        self.assertEqual(error.element_count, 2, msg="counting every element it holds")

    def test_an_empty_array_under_an_unknown_member_does_not_stop_the_run(self) -> None:
        """Nothing to drop: an empty array under any key holds no record."""
        document = self.document()
        document["Results"][self.observed["result_index"]][self.observed["section"]] = []
        self.assertIsNone(
            trivy.validate_finding_sections(document, tool=TOOL),
            msg="an empty unknown array holds no finding to lose",
        )

    def test_the_offence_is_reached_in_the_last_element_and_in_the_first(self) -> None:
        """The pass runs over every element, whichever one carries the fault.

        The committed fixture's offence is on the last element, so a validator that
        stopped after the first would miss it.  Moving the same member to the first
        element must raise the same reason at index 0, which is what shows the fixture's
        position is a property of the fixture rather than of the branch.
        """
        section = self.observed["section"]
        last = self.observed["result_index"]
        self.assertEqual(
            last,
            len(self.document()["Results"]) - 1,
            msg="the committed fixture offends in the last element",
        )
        error = self.raised_by_adapt()
        self.assertEqual(error.result_index, last)

        moved = self.document()
        offending = moved["Results"][last].pop(section)
        moved["Results"][0][section] = offending
        first = self.raised_by_adapt(moved)
        self.assertEqual(first.reason, trivy.HALT_UNKNOWN_SECTION)
        self.assertEqual(
            first.result_index, 0, msg="and the same member on the first element raises there"
        )
        self.assertEqual(
            first.target,
            moved["Results"][0]["Target"],
            msg="naming that element's own Target",
        )


class SupportedSectionNotAnArrayHaltTest(StructuralHaltContract, TrivyAdapterTestCase):
    """Condition 3: a supported section present as something other than array or null.

    This is the branch that exists specifically to stop malformed output from
    reconciling as a clean scan.  ``reconcile._count_trivy`` reads a non-array member as
    zero records and the adapter's own walk would read it as zero too, so **both** sides
    of the reconciliation identity agree on a number that is wrong -- the one condition
    the identity structurally cannot catch.  The fixture carries two record-shaped
    objects under ``Vulnerabilities`` keyed ``"0"`` and ``"1"``, which is what a lossy
    array-to-object transform produces.

    The boundary is asserted alongside: ``null`` and absent are **not** this condition,
    because neither holds a record to lose.
    """

    REASON = trivy.HALT_SECTION_NOT_AN_ARRAY
    FIXTURE_STEM = "halt-trivy-section-not-an-array"

    def test_the_offending_member_is_a_supported_section_of_the_wrong_type(self) -> None:
        """The fault is the type, not the name: the key is one of the three."""
        section = self.observed["section"]
        self.assertIn(
            section,
            trivy.SUPPORTED_SECTIONS,
            msg="the member is a supported section, so this is not the unknown-key branch",
        )
        value = self.document()["Results"][self.observed["result_index"]][section]
        self.assertNotIsInstance(value, list, msg="and it is not an array")
        self.assertIsNotNone(value, msg="nor null, which would be the passing case")
        self.assertIsNone(
            self.observed["element_count"],
            msg=(
                "so no element count is reported: there is no array whose elements could "
                "be counted, and null rather than 0 is what says so"
            ),
        )

    def test_a_null_supported_section_does_not_stop_the_run(self) -> None:
        """Null holds no record, so both sides of the identity agree truthfully."""
        document = self.document()
        document["Results"][self.observed["result_index"]][self.observed["section"]] = None
        self.assertIsNone(
            trivy.validate_finding_sections(document, tool=TOOL),
            msg="a null supported section is explicitly not this condition",
        )
        adapted = adapt_document(document)
        self.assertEqual(
            len(adapted.rows),
            self.expected["counts"]["raw_finding_records"],
            msg="and every supported record still becomes a row",
        )
        self.assertEqual(adapted.rejections, [])

    def test_an_absent_supported_section_does_not_stop_the_run(self) -> None:
        """Absence is the capture's own shape and must remain ordinary."""
        document = self.document()
        document["Results"][self.observed["result_index"]].pop(self.observed["section"])
        self.assertIsNone(
            trivy.validate_finding_sections(document, tool=TOOL),
            msg="an absent supported section is ordinary output",
        )

    def test_every_non_array_shape_raises_this_reason_with_its_own_type_named(
        self,
    ) -> None:
        """A string, a number and a boolean each halt, and the note names the type.

        The committed fixture's object is the realistic malformation; these three
        establish that the branch is about not-an-array rather than about objects, and
        that the note quotes the observed JSON type so a reader knows what they are
        looking at.
        """
        section = self.observed["section"]
        index = self.observed["result_index"]
        for label, value in (
            ("string", "2 vulnerabilities"),
            ("number", 2),
            ("boolean", True),
        ):
            with self.subTest(json_type=label):
                document = self.document()
                document["Results"][index][section] = value
                error = self.raised_by_adapt(document)
                self.assertEqual(error.reason, trivy.HALT_SECTION_NOT_AN_ARRAY)
                self.assertEqual(error.section, section)
                self.assertEqual(error.result_index, index)
                self.assertIsNone(error.element_count)
                self.assertIn(
                    label,
                    error.note,
                    msg="the note names the JSON type actually observed",
                )
                self.assertEqual(
                    dict(error.structure)["json_type"],
                    label,
                    msg="and so does the structural excerpt",
                )

    def test_each_of_the_three_supported_sections_reaches_this_branch(self) -> None:
        """Iterate ``SUPPORTED_SECTIONS``, so no section is covered by inheritance.

        The branch loops the three sections, and a fourth section added to the constant
        with no case would otherwise be unexercised.  Each is malformed on its own, with
        the other two left as the fixture has them.
        """
        index = self.observed["result_index"]
        covered: set[str] = set()
        for section in trivy.SUPPORTED_SECTIONS:
            with self.subTest(section=section):
                document = self.document()
                document["Results"][index].pop(self.observed["section"], None)
                document["Results"][index][section] = {"0": {"ID": "X-1"}}
                error = self.raised_by_adapt(document)
                self.assertEqual(error.reason, trivy.HALT_SECTION_NOT_AN_ARRAY)
                self.assertEqual(error.section, section)
                covered.add(error.section)
        self.assertEqual(
            covered,
            set(trivy.SUPPORTED_SECTIONS),
            msg="every supported section has a raising case",
        )

    def test_the_counting_traversal_reads_the_malformed_section_as_zero(self) -> None:
        """The two code paths fail identically, which is why the identity cannot help.

        Asserted rather than argued: the independent traversal returns the same count for
        this fixture as for a document with the member removed entirely, so the
        malformation is invisible to reconciliation on both sides.
        """
        with_malformation = reconcile.count_records(TOOL, self.document())
        without = self.document()
        without["Results"][self.observed["result_index"]].pop(self.observed["section"])
        self.assertEqual(
            with_malformation,
            reconcile.count_records(TOOL, without),
            msg=(
                "the counting traversal cannot tell the malformed artifact from one that "
                "never carried the member, so only the halt records the difference"
            ),
        )


class DeclaredFindingsUnheldHaltTest(StructuralHaltContract, TrivyAdapterTestCase):
    """Condition 4: ``MisconfSummary`` declares failures no supported section holds.

    The subtlest of the four, because nothing is dropped: the record is already absent
    from the artifact, and a dataset built from the records alone reconciles perfectly
    while reporting two failures where the artifact declares three.  The fixture empties
    ``Results[1]``'s ``Misconfigurations`` array and leaves its summary declaring one
    failure -- the present-and-empty shape, which an implementation checking only whether
    the member exists would miss.

    Deliberately **not** a count comparison: ``--include-non-failures`` puts passing
    checks in the same array, so a section holding more records than the declared failure
    count is ordinary and is asserted to continue.
    """

    REASON = trivy.HALT_DECLARED_FINDINGS_UNHELD
    FIXTURE_STEM = "halt-trivy-declared-findings-unheld"

    def test_the_contradiction_is_internal_to_the_fixture(self) -> None:
        """The element's own summary declares a failure its section does not hold."""
        element = self.document()["Results"][self.observed["result_index"]]
        summary = element["MisconfSummary"]
        self.assertGreater(
            summary["Failures"],
            0,
            msg="the element declares at least one failure",
        )
        self.assertEqual(
            element["Misconfigurations"],
            [],
            msg="and its Misconfigurations array holds no record",
        )
        self.assertEqual(
            self.observed["element_count"],
            0,
            msg="which is the stated zero the halt reports, rather than null",
        )
        self.assertIn(
            str(summary["Failures"]),
            self.observed["note"],
            msg="and the note quotes the declared count",
        )

    def test_an_absent_misconfigurations_member_raises_the_same_reason(self) -> None:
        """The removed-key variant of the same contradiction, with its own note.

        The committed fixture carries the present-and-empty shape because it is the one
        a membership check would miss.  Removing the member entirely is the other shape,
        and it must reach the same reason -- with the note naming ``null`` rather than
        ``array``, which is how a reader tells the two apart.
        """
        document = self.document()
        document["Results"][self.observed["result_index"]].pop("Misconfigurations")
        error = self.raised_by_adapt(document)
        self.assertEqual(error.reason, trivy.HALT_DECLARED_FINDINGS_UNHELD)
        self.assertEqual(error.section, "Misconfigurations")
        self.assertEqual(error.result_index, self.observed["result_index"])
        self.assertEqual(error.element_count, 0)
        self.assertIn(
            "null",
            error.note,
            msg="the note names the observed type, which is null with the member removed",
        )
        self.assertNotEqual(
            error.note,
            self.observed["note"],
            msg="so the two shapes are distinguishable in the halt report",
        )

    def test_more_records_than_declared_failures_does_not_stop_the_run(self) -> None:
        """``--include-non-failures`` output is ordinary, not a fault.

        A count comparison would halt here, which is why the adapter does not make one.
        The element declares one failure and is given three records, two of them passing
        checks; the run continues and every record becomes a row.
        """
        document = self.document()
        index = self.observed["result_index"]
        template = copy.deepcopy(self.document()["Results"][0]["Misconfigurations"][0])
        passing_one = copy.deepcopy(template)
        passing_one["Status"] = "PASS"
        passing_two = copy.deepcopy(template)
        passing_two["Status"] = "PASS"
        document["Results"][index]["Misconfigurations"] = [
            copy.deepcopy(template),
            passing_one,
            passing_two,
        ]
        self.assertEqual(document["Results"][index]["MisconfSummary"]["Failures"], 1)
        self.assertIsNone(
            trivy.validate_finding_sections(document, tool=TOOL),
            msg="three records against one declared failure is ordinary output",
        )
        self.assertEqual(
            len(adapt_document(document).rows),
            self.expected["counts"]["raw_finding_records"] + 3,
            msg="and every record in the section becomes a row",
        )

    def test_a_zero_or_absent_failure_count_does_not_stop_the_run(self) -> None:
        """No declared failure, nothing unheld -- on every shape the count can take."""
        index = self.observed["result_index"]
        for label, summary in (
            ("zero failures", {"Successes": 26, "Failures": 0}),
            ("no Failures member", {"Successes": 26}),
            ("a null Failures member", {"Successes": 26, "Failures": None}),
            ("a non-integer Failures member", {"Successes": 26, "Failures": "1"}),
            ("a boolean Failures member", {"Successes": 26, "Failures": True}),
        ):
            with self.subTest(summary=label):
                document = self.document()
                document["Results"][index]["MisconfSummary"] = dict(summary)
                self.assertIsNone(
                    trivy.validate_finding_sections(document, tool=TOOL),
                    msg=(
                        "the condition needs a positive integer failure count; anything "
                        "else declares nothing and cannot leave a finding unheld"
                    ),
                )

    def test_an_unreadable_summary_is_counted_and_does_not_stop_the_run(self) -> None:
        """A ``MisconfSummary`` that is not an object increments a counter and continues.

        A branch that counts and continues is easily mistaken for one that stops, so the
        distinction is asserted: the counter rises by exactly one and no halt is raised,
        even though the element's now-unreadable summary sits beside an empty
        ``Misconfigurations`` array -- the very element the committed fixture halts on.
        """
        document = self.document()
        index = self.observed["result_index"]
        document["Results"][index]["MisconfSummary"] = "26 successes, 1 failure"
        counters = {key: 0 for key in trivy.COUNTER_KEYS}
        self.assertIsNone(
            trivy.validate_finding_sections(document, tool=TOOL, counters=counters),
            msg="an unreadable summary declares nothing and does not halt",
        )
        self.assertEqual(
            counters[trivy.COUNTER_MISCONF_SUMMARY_UNREADABLE],
            1,
            msg="it is recorded rather than passed over in silence",
        )
        adapted = adapt_document(document)
        self.assertEqual(
            adapted.counters[trivy.COUNTER_MISCONF_SUMMARY_UNREADABLE],
            1,
            msg="and adapt's own counters carry the same observation",
        )
        self.assertEqual(
            len(adapted.rows),
            self.expected["counts"]["raw_finding_records"],
            msg="while every record the artifact does hold still becomes a row",
        )

    def test_the_declared_failures_across_the_document_exceed_the_records_present(
        self,
    ) -> None:
        """The arithmetic the expectation states, read from the fixture itself.

        Three declared failures, two records: the discrepancy is a property of the
        document rather than a claim of this test, and it is what makes the identity's
        silence total -- no record is dropped, so nothing is missing from either side of
        it.
        """
        declared = 0
        records = 0
        for element in self.document()["Results"]:
            summary = element.get("MisconfSummary") or {}
            failures = summary.get("Failures")
            if isinstance(failures, int) and not isinstance(failures, bool):
                declared += failures
            records += len(element.get("Misconfigurations") or [])
        arithmetic = self.expected["guarded_failure"][
            "arithmetic_if_the_guard_were_removed"
        ]
        self.assertEqual(declared, arithmetic["declared_failures_across_the_document"])
        self.assertEqual(records, arithmetic["failure_records_present"])
        self.assertEqual(
            declared - records,
            arithmetic["unheld_declared_failures"],
            msg="one declared failure the artifact carries no record of",
        )
        self.assertEqual(
            arithmetic["records_that_would_vanish"],
            0,
            msg=(
                "and nothing would vanish, which is exactly why this condition needs an "
                "artifact-level stop: there is no missing record for a count to notice"
            ),
        )


class HaltReasonCoverageTest(TrivyAdapterTestCase):
    """Every member of the closed ``trivy.HALT_REASONS`` tuple has a behavioural case.

    Iterating the constant rather than listing the reasons is the whole point: a fifth
    condition added to ``validate_finding_sections`` fails here by name, with its own
    message, instead of arriving with no fixture, no expectation and no test and being
    invisible.  The check is behavioural rather than declarative -- each reason's fixture
    is adapted and required to raise *that* reason -- so a fixture that stopped reaching
    its condition would fail here too.
    """

    def halt_contract_classes(self) -> dict[str, type]:
        """Every concrete class in this module built on the structural-halt contract."""
        found: dict[str, type] = {}
        for value in list(globals().values()):
            if (
                isinstance(value, type)
                and issubclass(value, StructuralHaltContract)
                and issubclass(value, unittest.TestCase)
                and value.REASON
            ):
                found[value.REASON] = value
        return found

    def test_the_reason_tuple_is_closed_and_each_member_is_distinct(self) -> None:
        """Four reasons, each a distinct non-empty string, each a module attribute."""
        self.assertEqual(
            len(set(trivy.HALT_REASONS)),
            len(trivy.HALT_REASONS),
            msg="no reason appears twice",
        )
        for reason in trivy.HALT_REASONS:
            with self.subTest(reason=reason):
                self.assertIsInstance(reason, str)
                self.assertTrue(reason.strip(), msg="each reason is a stated name")
                self.assertNotIn(
                    reason,
                    paths.REJECT_CLASSES,
                    msg="and no halt reason is also a rejection class",
                )

    def test_every_reason_has_a_committed_fixture_and_expectation(self) -> None:
        """The mapping covers the tuple exactly, in both directions."""
        self.assertEqual(
            sorted(HALT_FIXTURE_BY_REASON),
            sorted(trivy.HALT_REASONS),
            msg=(
                "every halt reason is mapped to one fixture stem and no stem is mapped "
                "to a reason the adapter does not declare"
            ),
        )
        self.assertEqual(
            len(set(HALT_FIXTURE_BY_REASON.values())),
            len(trivy.HALT_REASONS),
            msg="no two reasons share a fixture",
        )
        for reason, stem in sorted(HALT_FIXTURE_BY_REASON.items()):
            with self.subTest(reason=reason):
                fixture = FIXTURES_DIR / f"{stem}.json"
                expectation = EXPECTED_DIR / f"{stem}.rows.json"
                self.assertTrue(
                    fixture.is_file(),
                    msg=(
                        f"blocking gap: {fixture} is absent, so the halt reason {reason} "
                        "has no committed input. Reported, not skipped."
                    ),
                )
                self.assertTrue(
                    expectation.is_file(),
                    msg=f"blocking gap: {expectation} is absent for reason {reason}",
                )
                document = load_expected(stem)
                self.assertEqual(document["outcome"], "halt")
                self.assertEqual(document["halt"]["reason"], reason)
                self.assertEqual(document["rows"], [])
                self.assertEqual(document["rejections"], [])

    def test_every_reason_is_raised_by_its_own_fixture(self) -> None:
        """The behavioural half: adapt each fixture, and require its own reason.

        This is what makes the coverage claim more than bookkeeping.  A reason whose
        fixture stopped reaching its condition -- because an earlier condition began
        firing first, say -- fails here even though the fixture, the expectation and the
        test class all still exist.
        """
        raised: dict[str, str] = {}
        for reason, stem in sorted(HALT_FIXTURE_BY_REASON.items()):
            with self.subTest(reason=reason):
                with self.assertRaises(trivy.UnsupportedTrivySection) as caught:
                    adapt_document(load_fixture(f"{stem}.json"))
                raised[caught.exception.reason] = stem
                self.assertEqual(
                    caught.exception.reason,
                    reason,
                    msg=(
                        f"fixtures/{stem}.json must raise {reason}; it raised "
                        f"{caught.exception.reason} instead, so that reason's fixture no "
                        "longer isolates its own condition"
                    ),
                )
        self.assertEqual(
            sorted(raised),
            sorted(trivy.HALT_REASONS),
            msg="every reason in the closed tuple was raised by exactly one fixture",
        )

    def test_every_reason_has_a_concrete_test_class_in_this_module(self) -> None:
        """A reason with no class is a reason whose attributes nobody asserted."""
        classes = self.halt_contract_classes()
        self.assertEqual(
            sorted(classes),
            sorted(trivy.HALT_REASONS),
            msg=(
                "every halt reason is covered by exactly one concrete class built on "
                "StructuralHaltContract; a reason missing from this list has no test "
                "asserting the attributes its halt carries"
            ),
        )
        for reason, klass in sorted(classes.items()):
            with self.subTest(reason=reason):
                self.assertEqual(
                    klass.FIXTURE_STEM,
                    HALT_FIXTURE_BY_REASON[reason],
                    msg="and each class names the fixture stem mapped to its reason",
                )

    def test_the_exception_carries_only_a_reason_the_module_declares(self) -> None:
        """A reason outside the closed tuple is a caller fault, not a halt.

        The constructor refuses it with ``TrivyAdapterError``, so a typo cannot invent a
        fifth reason at run time and quietly become a halt nobody has a fixture for.
        """
        with self.assertRaises(trivy.TrivyAdapterError) as caught:
            trivy.UnsupportedTrivySection(
                "a_reason_the_module_does_not_declare",
                section="Findings",
                target="a-target",
                result_index=0,
            )
        message = str(caught.exception)
        self.assertIn("unknown halt reason", message)
        for reason in trivy.HALT_REASONS:
            with self.subTest(reason=reason):
                self.assertIn(
                    reason, message, msg="and the refusal names the closed set"
                )


class RejectedRecordTest(TrivyAdapterTestCase):
    """One negative fixture per rejection condition, asserted by class name and count.

    A defective record is **rejected and counted**, never dropped and never coerced into
    a row: dropping it breaks ``raw records = rows + rejections`` and coercing it puts a
    value in the dataset that this pipeline, not Trivy, decided on.  Each fixture's
    surviving records are asserted to still become rows, so a condition that rejected
    everything would fail here rather than look like a pass.

    The class is what carries each assertion.  A test that only counted rejections could
    not tell one condition from another, so every class is compared against a literal
    member of :data:`normalize.paths.REJECT_CLASSES` taken from the module.
    """

    def test_every_negative_fixture_emits_exactly_its_expected_rows(self) -> None:
        """The surviving records still become rows, field for field."""
        for stem in REJECT_FIXTURE_STEMS:
            with self.subTest(fixture=stem):
                adapted = adapt_negative(stem)
                expected = load_expected(stem)
                self.assertRowsEqualExpected(
                    adapted.rows, expected["rows"], where=stem
                )
                self.assertEqual(
                    len(adapted.rows),
                    expected["counts"]["rows"],
                    msg="and the count the expectation states in its counts block",
                )
                self.assertGreater(
                    len(adapted.rows),
                    0,
                    msg=(
                        "a negative fixture that rejected everything would assert "
                        "nothing about the records it should have kept"
                    ),
                )

    def test_every_negative_fixture_counts_its_rejections_under_the_named_class(
        self,
    ) -> None:
        """The class, the count, and the absence of every other class.

        Comparing the whole per-class mapping asserts both that the condition fired and
        that no other condition did -- which a bare rejection count could not.
        """
        for stem in REJECT_FIXTURE_STEMS:
            with self.subTest(fixture=stem):
                adapted = adapt_negative(stem)
                expected = load_expected(stem)
                self.assertEqual(
                    adapted.rejections_by_class,
                    expected["counts"]["rejections_by_class"],
                    msg="the classes and their counts, with no other class present",
                )
                self.assertEqual(
                    len(adapted.rejections),
                    expected["counts"]["rejections"],
                    msg="and the total the expectation states",
                )
                expected_class = REJECT_CLASS_BY_STEM[stem]
                self.assertEqual(
                    sorted(adapted.rejections_by_class),
                    [expected_class],
                    msg=f"{stem} exercises exactly {expected_class!r}",
                )
                self.assertEqual(
                    expected_class,
                    getattr(paths, f"REJECT_{expected_class.upper()}"),
                    msg=(
                        "the class asserted is the module's own constant, not a string "
                        "spelled by hand in this test"
                    ),
                )

    def test_every_rejection_names_its_record_and_retains_its_diagnostic(self) -> None:
        """Each rejection identifies the record it came from and says why, in words.

        The identity is enough to find the record in the artifact again and is never the
        record itself: a rejection carries no dataset fields and no value from the
        record, which is what keeps a secret out of a record that is written to the run's
        own log.
        """
        for stem in REJECT_FIXTURE_STEMS:
            adapted = adapt_negative(stem)
            expected = load_expected(stem)
            self.assertEqual(
                len(adapted.rejections),
                len(expected["rejections"]),
                msg=f"{stem}: one expectation per rejection",
            )
            for index, (rejection, entry) in enumerate(
                zip(adapted.rejections, expected["rejections"])
            ):
                with self.subTest(fixture=stem, rejection=index):
                    self.assertIsInstance(rejection, paths.Rejection)
                    self.assertEqual(rejection.tool, TOOL)
                    self.assertEqual(rejection.reject_class, entry["reject_class"])
                    self.assertIsInstance(rejection.detail, str)
                    self.assertTrue(
                        rejection.detail,
                        msg="an empty detail is the catch-all AAP 0.5.4 forbids",
                    )
                    identity = dict(rejection.record_identity)
                    self.assertEqual(
                        identity["result_index"],
                        entry["result_index"],
                        msg="the enclosing element's index",
                    )
                    self.assertEqual(
                        identity["record_index"],
                        entry["record_index"],
                        msg="the record's index within its section",
                    )
                    self.assertEqual(
                        identity["section"],
                        expected_rejection_section(entry),
                        msg="and the section the rejection was produced under",
                    )
                    self.assertIn(
                        "Target",
                        identity,
                        msg="with the artifact's own spelling of the Target",
                    )
                    json.dumps(rejection.as_dict())

    def test_no_rejected_record_became_a_row_and_none_was_dropped(self) -> None:
        """The per-artifact identity holds over every negative fixture."""
        for stem in REJECT_FIXTURE_STEMS:
            with self.subTest(fixture=stem):
                adapted = adapt_negative(stem)
                expected = load_expected(stem)
                self.assertOneOutcomePerRecord(adapted, where=stem)
                self.assertEqual(
                    adapted.raw_records,
                    expected["counts"]["raw_finding_records"],
                    msg="the record count is the expectation's count unit",
                )
                self.assertEqual(
                    len(adapted.rows) + len(adapted.rejections),
                    expected["counts"]["rows"] + expected["counts"]["rejections"],
                    msg="rows plus rejections is what the expectation states",
                )

    def test_the_counters_are_the_expected_files_for_every_negative_fixture(self) -> None:
        """Every counter of every negative fixture, key by key."""
        for stem in REJECT_FIXTURE_STEMS:
            with self.subTest(fixture=stem):
                self.assertCountersEqualExpected(
                    adapt_negative(stem), load_expected(stem), where=stem
                )

    def test_the_rejected_records_own_section_still_decided_a_class_where_it_had_one(
        self,
    ) -> None:
        """A rejected record still has a section; what it does not have is a row.

        The class is bound at step 2 of the classification order, before the identifier,
        the message, the path, the line and the coordinate -- so every rejection except
        the unattributable one carries the section its class would have come from.
        """
        for stem in REJECT_FIXTURE_STEMS:
            if stem == SEAM_STEM:
                continue
            with self.subTest(fixture=stem):
                adapted = adapt_negative(stem)
                for rejection in adapted.rejections:
                    section = rejection.record_identity["section"]
                    self.assertIn(
                        section,
                        trivy.SUPPORTED_SECTIONS,
                        msg="the record was read from a supported section",
                    )
                    self.assertIsNotNone(
                        trivy.scanner_class_for_section(section),
                        msg="whose class was never in doubt",
                    )

    def test_the_absent_path_rejection_is_the_class_that_paths_py_owns(self) -> None:
        """The ``absent_path`` condition, asserted by class, identity and reason.

        ``path`` is not one of the five fields absence is permitted for (AAP 0.8.2), so
        a record whose enclosing ``Target`` states nothing is rejected and counted
        rather than emitted with a null path or given the scan root.  The class name is
        read from :mod:`normalize.paths` rather than spelled here, and the identity is
        asserted key by key because it is what a reader uses to find the record in the
        artifact again.
        """
        stem = "reject-trivy-absent-path"
        adapted = adapt_negative(stem)
        expected = load_expected(stem)

        self.assertEqual(len(adapted.rejections), 1)
        rejection = adapted.rejections[0]
        self.assertIs(
            type(rejection.reject_class),
            str,
            msg="a class is a string from the closed vocabulary, not an enum member",
        )
        self.assertEqual(
            rejection.reject_class,
            paths.REJECT_ABSENT_PATH,
            msg="the module's own constant, not a hand-spelled copy of it",
        )
        self.assertIn(paths.REJECT_ABSENT_PATH, paths.REJECT_CLASSES)
        self.assertTrue(paths.is_reject_class(rejection.reject_class))
        self.assertNotEqual(
            rejection.reject_class,
            paths.REJECT_UNRESOLVABLE_PATH,
            msg=(
                "absent and unresolvable are neighbouring classes and this fixture is "
                "the first: nothing was stated, so nothing failed to resolve"
            ),
        )

        entry = expected["rejections"][0]
        identity = dict(rejection.record_identity)
        self.assertEqual(
            sorted(identity),
            sorted(entry["identity_keys_expected"].keys() - {"note"}),
            msg="the identity carries exactly the keys the expectation lists",
        )
        for key, value in entry["identity_keys_expected"].items():
            if key == "note":
                continue
            with self.subTest(identity_key=key):
                self.assertEqual(identity[key], value)
        self.assertIn(
            "reported_path",
            identity,
            msg=(
                "the resolver records the value it could not resolve, and an empty "
                "string is a value: a key set to '' is not a key that was never set"
            ),
        )
        self.assertEqual(identity["Target"], "")
        self.assertEqual(identity["rule_id"], "DS-0026")

        field_name = environment().tool_base().record_path_field or "path"
        self.assertIn(
            f"carries no {field_name} value",
            rejection.detail,
            msg=(
                "the detail names the field it looked in, taking that name from the "
                "runner metadata rather than from a literal in this test"
            ),
        )
        for fragment in entry["detail_expected"]["required_substrings"]:
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, rejection.detail)
        self.assertNotIn(
            "resolve",
            rejection.detail,
            msg="nothing was resolved, so the detail must not describe a resolution",
        )

    def test_the_absent_path_fixture_changes_only_the_target_it_claims_to(self) -> None:
        """The derived fixture differs from the capture in one member and no other.

        A negative fixture that changed two things could not attribute its rejection to
        either.  The comparison is structural rather than a byte diff, so a
        reformatting of the file cannot mask a second change.
        """
        stem = "reject-trivy-absent-path"
        capture = load_fixture(POSITIVE_FIXTURE)
        fixture = load_fixture(f"{stem}.json")
        expectation = load_expected(stem)

        self.assertEqual(
            expectation["fixture"]["kind"],
            "derived",
            msg="and it says so: only fixtures/trivy.json claims to be a capture",
        )
        self.assertEqual(
            expectation["fixture"]["derived_from"]["fixture"],
            f"oss-scan-results/adapter-tests/fixtures/{POSITIVE_FIXTURE}",
        )
        self.assertEqual(
            set(capture) ^ set(fixture),
            set(),
            msg="the envelope members are the capture's, none added and none removed",
        )
        for member in capture:
            if member == "Results":
                continue
            with self.subTest(member=member):
                self.assertEqual(capture[member], fixture[member])
        self.assertEqual(len(fixture["Results"]), len(capture["Results"]))
        differing = []
        for index, (before, after) in enumerate(
            zip(capture["Results"], fixture["Results"])
        ):
            if json.dumps(before, sort_keys=True) != json.dumps(after, sort_keys=True):
                differing.append(index)
        self.assertEqual(
            differing,
            [expectation["rejections"][0]["result_index"]],
            msg="exactly one element differs, and it is the one the expectation names",
        )
        element_before = capture["Results"][differing[0]]
        element_after = fixture["Results"][differing[0]]
        for member in element_before:
            if member == "Target":
                continue
            with self.subTest(member=member):
                self.assertEqual(
                    json.dumps(element_before[member], sort_keys=True),
                    json.dumps(element_after[member], sort_keys=True),
                    msg="the record itself is the capture's, unchanged",
                )
        self.assertTrue(element_before["Target"].strip())
        self.assertEqual(element_after["Target"], "")

    def test_every_shape_of_a_missing_target_reaches_the_same_class(self) -> None:
        """Blank, whitespace-only, null and removed all reject; a non-string does not.

        Four shapes of the same omission, and the neighbouring class on the other side
        of the line.  Committing four near-identical fixtures would assert nothing the
        first does not; asserting the four shapes against one fixture's document is what
        shows the guard is ``if not value.strip()`` rather than ``if value is None``.
        """
        stem = "reject-trivy-absent-path"
        pointer = load_expected(stem)["rejections"][0]["result_index"]

        for label, mutate in (
            ("blank string", lambda element: element.__setitem__("Target", "")),
            ("whitespace only", lambda element: element.__setitem__("Target", " \t ")),
            ("null", lambda element: element.__setitem__("Target", None)),
            ("removed key", lambda element: element.pop("Target", None)),
        ):
            with self.subTest(shape=label):
                document = derived(POSITIVE_FIXTURE)
                mutate(document["Results"][pointer])
                adapted = adapt_document(document)
                self.assertEqual(len(adapted.rejections), 1)
                self.assertEqual(
                    adapted.rejections[0].reject_class,
                    paths.REJECT_ABSENT_PATH,
                    msg=f"{label} is the same omission and takes the same class",
                )
                self.assertEqual(len(adapted.rows), 2)
                self.assertOneOutcomePerRecord(adapted, where=label)

        for label, value in (
            ("number", 7),
            ("object", {"path": "core/src/main/scala/x.scala"}),
            ("array", ["core/src/main/scala/x.scala"]),
            ("boolean", True),
        ):
            with self.subTest(shape=label):
                document = derived(POSITIVE_FIXTURE)
                document["Results"][pointer]["Target"] = value
                adapted = adapt_document(document)
                self.assertEqual(len(adapted.rejections), 1)
                self.assertEqual(
                    adapted.rejections[0].reject_class,
                    paths.REJECT_MALFORMED_RECORD,
                    msg=(
                        f"a Target present as a {label} is a malformed artifact rather "
                        "than an omission, and the two classes tell a reader to do "
                        "different things"
                    ),
                )

    def test_the_absent_path_rejection_balances_the_independent_count(self) -> None:
        """``raw finding records = rows + rejections``, the left side counted separately.

        :func:`normalize.reconcile.count_records` never reads a ``Target``, so a record
        with no coordinate still counts as one raw record -- which is exactly why the
        identity catches a silent drop.  The counter signature is asserted alongside it:
        ``records_misconfigurations`` counts the record as walked while
        ``rows_class_misconfig`` does not count it as emitted, and that difference of
        one is what a rejection looks like in the counters.
        """
        stem = "reject-trivy-absent-path"
        document = load_fixture(f"{stem}.json")
        adapted = adapt_negative(stem)
        expected = load_expected(stem)

        independent = reconcile.count_records(TOOL, document)
        self.assertEqual(
            independent,
            expected["counts"]["raw_finding_records"],
            msg="the count unit is the expectation's, taken from the counting traversal",
        )
        self.assertEqual(independent, 3)
        self.assertEqual(
            independent,
            len(adapted.rows) + len(adapted.rejections),
            msg=expected["counts"]["reconciliation_identity"],
        )
        self.assertEqual(f"{independent} == {len(adapted.rows)} + "
                         f"{len(adapted.rejections)}",
                         expected["counts"]["reconciliation_arithmetic"])
        self.assertEqual(
            adapted.rejections_by_class,
            {paths.REJECT_ABSENT_PATH: 1},
            msg="one class, one rejection, and no other class present",
        )

        counters = adapted.counters
        walked = f"{trivy.COUNTER_RECORDS_PREFIX}misconfigurations"
        emitted = f"{trivy.COUNTER_ROWS_CLASS_PREFIX}misconfig"
        self.assertEqual(counters[walked], 3)
        self.assertEqual(counters[emitted], 2)
        self.assertEqual(
            counters[walked] - counters[emitted],
            len(adapted.rejections),
            msg="walked minus emitted is the rejection count, per section",
        )
        for key in (
            trivy.COUNTER_SEVERITY_LABEL_PRESENT,
            trivy.COUNTER_START_LINE_ABSENT,
            trivy.COUNTER_COORDINATE_ABSENT,
            f"{trivy.COUNTER_PATH_KIND_PREFIX}{paths.PATH_KIND_TREE_FILE}",
            trivy.COUNTER_ROWS_IN_SCOPE,
        ):
            with self.subTest(counter=key):
                self.assertEqual(
                    counters[key],
                    2,
                    msg=(
                        "every counter downstream of the path step counts the two rows "
                        "only: the rejected record returns at step 5 and reaches none "
                        "of them"
                    ),
                )
        self.assertEqual(counters[trivy.COUNTER_PER_RECORD_PATH_REFINEMENTS], 0)

    def test_a_stated_target_makes_the_same_record_a_row(self) -> None:
        """The falsifying control: the record is a constant and the Target is the variable.

        The same DS-0026 record is a row on the other two elements of this fixture, and
        restoring the emptied Target makes it a row too.  Without this the rejection
        could be attributed to anything about the record.
        """
        stem = "reject-trivy-absent-path"
        pointer = load_expected(stem)["rejections"][0]["result_index"]
        fixture = load_fixture(f"{stem}.json")
        capture = load_fixture(POSITIVE_FIXTURE)

        restored = derived(f"{stem}.json")
        restored["Results"][pointer]["Target"] = capture["Results"][pointer]["Target"]
        adapted = adapt_document(restored)
        self.assertEqual(
            adapted.rejections,
            [],
            msg="with a Target stated, the record has a coordinate and becomes a row",
        )
        self.assertEqual(len(adapted.rows), 3)
        self.assertRowsEqualExpected(
            adapted.rows,
            load_expected(POSITIVE_EXPECTED_STEM)["rows"],
            where="the absent-path fixture with its Target restored",
        )

        records = {
            json.dumps(element["Misconfigurations"][0], sort_keys=True)
            for element in fixture["Results"]
        }
        self.assertEqual(
            len(records),
            1,
            msg=(
                "all three elements hold the identical DS-0026 record, so the record "
                "cannot be what distinguishes the rejected one from the two rows"
            ),
        )

    def test_the_unattributable_branch_is_reached_only_through_the_seam(self) -> None:
        """The same record is a row under its own section and a rejection under any other.

        This is the falsifying pair the condition needs.  ``adapt`` over the same
        document produces no rejection at all, because its iteration is section-bound;
        handing the record its own section produces a ``vuln`` row; handing it a key
        outside the three produces the rejection.  An implementation that classed the
        record from its ``RuleID``, its ``ID``, its ``Match``, its ``CauseMetadata`` or
        the enclosing element's ``Class`` would fail this.
        """
        env = environment()
        document = load_fixture(f"{SEAM_STEM}.json")
        result_index, document_section, record_index = SEAM_DESIGNATED_RECORD
        element = document["Results"][result_index]
        record = element[document_section][record_index]

        section_bound = adapt_document(load_fixture(f"{SEAM_STEM}.json"))
        self.assertEqual(
            section_bound.rejections,
            [],
            msg="adapt cannot reach the branch: every record arrives with a real section",
        )
        self.assertEqual(len(section_bound.rows), section_bound.raw_records)

        def hand_over(section: Any) -> Any:
            """Hand the designated record to the seam under ``section``."""
            return trivy.adapt_record(
                copy.deepcopy(record),
                section=section,
                target=element.get("Target"),
                tool=TOOL,
                root=env.root,
                tool_base=env.tool_base(),
                globs=env.globs,
                tally=severity.LiteralTally.with_all_tools(),
                counters=trivy.new_counters(),
                packages=element.get("Packages") or (),
                ecosystem=element.get("Type"),
                result_index=result_index,
                record_index=record_index,
            )

        own = hand_over(document_section)
        self.assertNotIsInstance(
            own, paths.Rejection, msg="under its own section the record is a row"
        )
        self.assertEqual(own["scanner_class"], "vuln")

        for foreign in (
            SEAM_SECTION_KEY,
            "Class",
            "vulnerabilities",
            *trivy.UNSUPPORTED_FINDING_SECTIONS,
            None,
            7,
        ):
            with self.subTest(section=foreign):
                outcome = hand_over(foreign)
                self.assertIsInstance(
                    outcome,
                    paths.Rejection,
                    msg="under any other key it is a counted rejection, never a row",
                )
                self.assertEqual(
                    outcome.reject_class, paths.REJECT_UNATTRIBUTABLE_SECTION
                )

    def test_the_seams_rejection_carries_the_four_key_identity_and_its_reason(self) -> None:
        """Attribution precedes the identifier step, so the identity carries no ``rule_id``.

        The detail says what happened and, crucially, that record content is never used
        to establish a class -- the sentence a reader needs in order to see that the
        rejection is a design decision rather than a lookup that failed.
        """
        adapted = adapt_negative(SEAM_STEM)
        self.assertEqual(len(adapted.rejections), 1)
        rejection = adapted.rejections[0]
        self.assertEqual(
            sorted(rejection.record_identity),
            sorted(("result_index", "section", "record_index", "Target")),
            msg="four keys, and no rule_id: step 2 precedes step 3",
        )
        self.assertEqual(rejection.record_identity["section"], SEAM_SECTION_KEY)
        self.assertIn("is not one of this artifact's finding sections", rejection.detail)
        self.assertIn(
            "record content is never used to establish one", rejection.detail
        )

    def test_the_unmerged_part_is_read_with_the_base_its_shape_requires(self) -> None:
        """The three readings of the per-directory part, and why only one asserts anything.

        The metadata records that the runner-written parts, absent from this checkout, are
        not root-anchored, and ``paths.resolve_trivy_path`` requires a ``per_section_target``
        base with a section base for one.  Measuring all three readings is what shows the
        chosen one is the metadata's rather than the one that happens to produce a
        rejection: read against the scan root the fixture yields four rows and no rejection
        at all, and with no section base every record fails for the same reason and none
        survives.
        """
        stem = "reject-trivy-unresolvable-path"
        self.assertIn(stem, PER_SECTION_TARGET_STEMS)
        document = load_fixture(f"{stem}.json")
        section_base = document["ArtifactName"]
        self.assertNotEqual(
            section_base,
            ".",
            msg="the part states its own scope directory, not the merged '.'",
        )

        prescribed = adapt_document(
            load_fixture(f"{stem}.json"),
            base_kind=BASE_KIND_PART,
            section_base=section_base,
        )
        self.assertEqual(len(prescribed.rows), 3)
        self.assertEqual(len(prescribed.rejections), 1)
        self.assertEqual(
            prescribed.rejections[0].reject_class, paths.REJECT_UNRESOLVABLE_PATH
        )

        without_base = adapt_document(
            load_fixture(f"{stem}.json"), base_kind=BASE_KIND_PART
        )
        self.assertEqual(
            without_base.rows,
            [],
            msg="with no section base the Target half rejects every record",
        )
        self.assertEqual(len(without_base.rejections), without_base.raw_records)

        as_merged = adapt_document(load_fixture(f"{stem}.json"))
        self.assertEqual(
            len(as_merged.rejections),
            0,
            msg=(
                "and read against the scan root it rejects nothing, silently dropping "
                "the ArtifactName prefix -- the reading the metadata's note exists to "
                "prevent"
            ),
        )
        self.assertEqual(len(as_merged.rows), as_merged.raw_records)

    def test_the_unresolvable_rejection_is_the_per_record_half_of_the_resolution(
        self,
    ) -> None:
        """The Target resolved; the per-record path had no base to resolve against.

        Both halves are named in the detail, which is what distinguishes this from a
        record whose path merely resolved somewhere unwanted -- that one is a kept row
        with ``in_scope`` false.
        """
        adapted = adapt_negative("reject-trivy-unresolvable-path")
        rejection = adapted.rejections[0]
        self.assertEqual(rejection.reject_class, paths.REJECT_UNRESOLVABLE_PATH)
        self.assertIn(
            paths.PATH_BASE_KIND_PER_SECTION_TARGET,
            rejection.detail,
            msg="the detail names the base kind that supplied no base",
        )
        self.assertIn(
            "supplies no base",
            rejection.detail,
            msg="and says so in words a reader can act on",
        )


class RejectionClassPartitionTest(TrivyAdapterTestCase):
    """The closed rejection vocabulary, partitioned into what this shape can and cannot do.

    :data:`normalize.paths.REJECT_CLASSES` is a closed set of ten.  For each member this
    module states one of exactly two things: the committed fixture that produces it for
    a Trivy artifact, or the structural reason a Trivy artifact cannot produce it.  The
    two sets are asserted to be disjoint and to cover the vocabulary with no remainder,
    which is what makes "this adapter cannot produce that" a measured claim rather than
    an assumption nobody rechecked.

    The failure mode it guards is a class falling out of both sets with nothing looking
    for it.  ``absent_path`` shows how narrow the margin is: the adapter reaches it from
    a missing, null or blank ``Results[].Target`` through
    :func:`normalize.paths.resolve_trivy_path`, so a producible set that omitted it and a
    reasons map that did not mention it would leave a reachable class untested while
    every count still balanced.  A class that falls out of both sets fails here, and a
    tenth class added to ``paths.py`` arrives with a failure naming it rather than
    silently untested.
    """

    def producible_classes(self) -> set[str]:
        """The classes this module drives with a committed fixture."""
        return set(REJECT_CLASS_BY_STEM.values())

    def test_the_two_sets_partition_the_closed_vocabulary_exactly(self) -> None:
        """Disjoint, and together the whole of ``paths.REJECT_CLASSES``."""
        producible = self.producible_classes()
        unreachable = set(UNREACHABLE_REJECT_CLASSES)
        vocabulary = set(paths.REJECT_CLASSES)

        self.assertEqual(
            producible & unreachable,
            set(),
            msg="a class cannot be both driven by a fixture and unreachable",
        )
        self.assertEqual(
            producible | unreachable,
            vocabulary,
            msg=(
                "every member of the closed vocabulary is accounted for: the "
                "difference names the classes that are neither tested nor explained"
            ),
        )
        self.assertEqual(
            vocabulary - producible,
            unreachable,
            msg="stated the other way round, so a missing entry cannot cancel out",
        )
        self.assertEqual(len(paths.REJECT_CLASSES), 10)
        self.assertEqual(len(producible), 8)
        self.assertEqual(len(unreachable), 2)
        self.assertEqual(
            len(set(paths.REJECT_CLASSES)),
            len(paths.REJECT_CLASSES),
            msg="the vocabulary itself carries no duplicate",
        )

    def test_every_producible_class_names_one_fixture_and_that_fixture_produces_it(
        self,
    ) -> None:
        """Every producible class is named by a fixture, and every fixture actually fires.

        A mapping that named a fixture which produced a different class would pass a
        set-equality check and assert nothing, so the class is read back off the
        adapter's own output for every entry.

        The relation is onto, not one-to-one: a class may be reached by more than one
        committed route (``non_integer_start_line`` is reached by three), and what is
        asserted is that the classes the fixtures claim are exactly the producible set --
        no class left uncovered, and no fixture claiming a class this shape cannot produce.
        """
        producible = self.producible_classes()
        self.assertEqual(
            set(REJECT_CLASS_BY_STEM.values()),
            set(producible),
            msg="the fixtures' classes are exactly the producible set",
        )
        self.assertEqual(
            sorted(REJECT_CLASS_BY_STEM),
            sorted(REJECT_FIXTURE_STEMS),
            msg="the stems and the class mapping cover the same fixtures",
        )
        for stem, expected_class in sorted(REJECT_CLASS_BY_STEM.items()):
            with self.subTest(fixture=stem, reject_class=expected_class):
                self.assertTrue(
                    paths.is_reject_class(expected_class),
                    msg="the class is one paths.py owns",
                )
                self.assertEqual(
                    expected_class,
                    getattr(paths, f"REJECT_{expected_class.upper()}"),
                    msg="and equals the module's own constant of that name",
                )
                self.assertTrue(
                    (FIXTURES_DIR / f"{stem}.json").is_file(),
                    msg=f"fixtures/{stem}.json is committed",
                )
                adapted = adapt_negative(stem)
                self.assertEqual(
                    sorted(adapted.rejections_by_class),
                    [expected_class],
                    msg="the fixture produces exactly the class it is mapped to",
                )
                self.assertEqual(
                    load_expected(stem)["counts"]["rejections_by_class"],
                    adapted.rejections_by_class,
                    msg="and the hand-verified expectation states the same",
                )

    def test_every_unreachable_class_is_named_with_a_reason_and_never_appears(
        self,
    ) -> None:
        """The reason is prose a reader can check, and the absence is measured.

        Each unreachable class is asserted to appear in no rejection of any committed
        fixture -- the two positive documents, the four halt documents insofar as they
        reach the record pass, and all eight negatives.  A reason alone would be an
        assertion about intent; this is an assertion about output.
        """
        for unreachable, reason in sorted(UNREACHABLE_REJECT_CLASSES.items()):
            with self.subTest(unreachable=unreachable):
                self.assertTrue(
                    paths.is_reject_class(unreachable),
                    msg="an unreachable class is still a real class",
                )
                self.assertIsInstance(reason, str)
                self.assertGreater(
                    len(reason.strip()),
                    60,
                    msg=(
                        "the reason has to say why the shape cannot produce it, which "
                        "a few words cannot"
                    ),
                )
                self.assertNotIn(
                    "fixture",
                    reason,
                    msg=(
                        "the reason must be about the artifact's shape, never about "
                        "which cases this corpus happens to contain"
                    ),
                )

        for stem in (POSITIVE_EXPECTED_STEM, FEATURES_EXPECTED_STEM):
            fixture = (
                POSITIVE_FIXTURE if stem == POSITIVE_EXPECTED_STEM else FEATURES_FIXTURE
            )
            with self.subTest(fixture=fixture):
                adapted = adapt_document(load_fixture(fixture))
                self.assertEqual(
                    adapted.rejections,
                    [],
                    msg="a positive fixture rejects nothing at all",
                )
        for stem in REJECT_FIXTURE_STEMS:
            adapted = adapt_negative(stem)
            for unreachable in UNREACHABLE_REJECT_CLASSES:
                with self.subTest(fixture=stem, unreachable=unreachable):
                    self.assertNotIn(unreachable, adapted.rejections_by_class)

    def test_no_reject_class_is_also_a_halt_reason_or_a_path_kind(self) -> None:
        """Three closed vocabularies, no overlap, and none of them invented here.

        A rejection is a counted record-level outcome, a halt stops the run, and a path
        kind describes a resolved coordinate.  A name shared between two of them would
        make a reader unable to tell which outcome a value described, and would let one
        vocabulary's coverage assertion be satisfied by another's member.
        """
        rejections = set(paths.REJECT_CLASSES)
        halts = set(trivy.HALT_REASONS)
        kinds = set(paths.PATH_KINDS)
        self.assertEqual(rejections & halts, set())
        self.assertEqual(rejections & kinds, set())
        self.assertEqual(halts & kinds, set())
        for name in sorted(rejections | halts | kinds):
            with self.subTest(name=name):
                self.assertIsInstance(name, str)
                self.assertTrue(name.strip())
                self.assertEqual(name, name.strip())

    def test_the_module_docstrings_claim_matches_the_measured_partition(self) -> None:
        """The prose at the top of this file is asserted, not merely written.

        A docstring that names which classes are covered is documentation a reader
        trusts, and a reader has no way to check it against the adapter.  Holding the
        count and every class name to the measured producible and unreachable sets is
        what keeps the prose from drifting from the code: a class that became reachable,
        or one dropped from the list, fails here rather than misinforming silently.
        """
        docstring = __doc__ or ""
        self.assertIn(
            "Eight of the ten classes",
            docstring,
            msg="the count in the docstring is the size of the producible set",
        )
        self.assertEqual(len(self.producible_classes()), 8)
        for reject_class in sorted(self.producible_classes()):
            with self.subTest(reject_class=reject_class):
                self.assertIn(
                    f"``{reject_class}``",
                    docstring,
                    msg="every covered class is named in the docstring's list",
                )
        for unreachable in sorted(UNREACHABLE_REJECT_CLASSES):
            with self.subTest(unreachable=unreachable):
                self.assertIn(f"``{unreachable}``", docstring)


class KeptRowTest(TrivyAdapterTestCase):
    """Rows that are kept rather than rejected, asserted so a tidy-up cannot drop them.

    A path that resolves somewhere the allowlist does not cover is **not** a defect: a
    runner legitimately reaching a manifest outside the twelve roots produces a row with
    ``in_scope: false``, and that row is kept and counted (AAP 0.3.2, AAP 0.9.3).  The
    same holds for a non-filesystem coordinate.  Only evidence about the *runner*
    establishes a wrong scan root; an individual coordinate outside the tree is a
    coordinate, not a targeting fault.

    ``in_scope`` is decided by the allowlist alone.  What a runner read is a separate
    question, and this adapter filters nothing on either basis.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Adapt the derived features document once for the whole class."""
        cls.adapted = adapt_document(load_fixture(FEATURES_FIXTURE))

    def test_a_manifest_outside_the_twelve_globs_is_kept_with_in_scope_false(self) -> None:
        """Assertion 17: four rows from two real lockfiles, kept and marked out of scope.

        ``dev/package-lock.json`` and ``docs/Gemfile.lock`` are real files in the pinned
        tree and outside the twelve authoritative globs.  Nothing in scope resolves to a
        package -- exactly one manifest-shaped file is in scope and it carries no
        dependencies block -- so a vulnerability row from a manifest the runner reached
        outside the globs is the realistic case rather than an edge one.
        """
        env = environment()
        out_of_scope = [row for row in self.adapted.rows if not row["in_scope"]]
        self.assertEqual(
            len(out_of_scope),
            4,
            msg="four rows are out of scope, and all four are present rather than dropped",
        )
        self.assertEqual(
            sorted({row["path"] for row in out_of_scope}),
            ["dev/package-lock.json", "docs/Gemfile.lock"],
            msg="the two lockfiles outside the twelve globs",
        )
        for index, row in enumerate(out_of_scope):
            with self.subTest(row=index, path=row["path"]):
                self.assertIsNone(
                    paths.matches_any_glob(row["path"], env.globs),
                    msg="the path matches no glob, which is why in_scope is false",
                )
                self.assertFalse(
                    paths.in_scope(row["path"], env.globs),
                    msg="and the scope predicate itself agrees",
                )
                self.assertRowSchema(row, where=f"out-of-scope row {index}")
                self.assertIsNotNone(
                    row["rule_id"],
                    msg="a kept row is a complete row, not a stub",
                )

    def test_an_in_scope_row_matched_a_glob_and_the_split_is_counted(self) -> None:
        """The other five rows match a glob, and the adapter's counters state the split."""
        env = environment()
        in_scope_rows = [row for row in self.adapted.rows if row["in_scope"]]
        self.assertEqual(len(in_scope_rows), 5)
        for index, row in enumerate(in_scope_rows):
            with self.subTest(row=index, path=row["path"]):
                self.assertIsNotNone(
                    paths.matches_any_glob(row["path"], env.globs),
                    msg="an in-scope row names a path one of the twelve globs covers",
                )
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_ROWS_IN_SCOPE], len(in_scope_rows)
        )
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_ROWS_OUT_OF_SCOPE],
            len(self.adapted.rows) - len(in_scope_rows),
        )
        self.assertEqual(
            self.adapted.counters[trivy.COUNTER_ROWS_IN_SCOPE]
            + self.adapted.counters[trivy.COUNTER_ROWS_OUT_OF_SCOPE],
            len(self.adapted.rows),
            msg="every row is on one side of the split and none is unaccounted for",
        )

    def test_the_scope_predicate_agrees_with_every_row_the_adapter_emitted(self) -> None:
        """``in_scope`` is the allowlist's verdict on the emitted path, on every row."""
        env = environment()
        for index, row in enumerate(self.adapted.rows):
            with self.subTest(row=index, path=row["path"]):
                self.assertEqual(
                    row["in_scope"],
                    paths.in_scope(row["path"], env.globs),
                    msg="decided by the allowlist alone, and by nothing else",
                )

    def test_an_archive_member_is_serialized_with_one_separator_and_kept(self) -> None:
        """Assertion 18: ``<container-relative-to-root>!<member>``, kept and counted.

        Derived, because no Target in the committed fixture names a container -- the
        fixture's ``Node.js`` is an ecosystem label and ``.js`` is not an archive
        extension, so nothing there produces this serialization.  The container is a real
        in-tree path under one of the twelve globs, which is what makes the ``in_scope``
        result attributable to the coordinate's **kind** rather than to the container
        being out of scope.
        """
        container = "python/pyspark/lib/example-archive.zip"
        member = "example/module.py"
        self.assertIsNotNone(
            paths.matches_any_glob(container, environment().globs),
            msg="the container itself is inside the twelve globs",
        )
        document = derived(FEATURES_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][4])]
        element = document["Results"][0]
        element["Target"] = container
        element["Vulnerabilities"][0]["PkgPath"] = member
        adapted = adapt_document(document)

        self.assertEqual(len(adapted.rows), 1, msg="the row is kept, not rejected")
        self.assertEqual(adapted.rejections, [])
        row = adapted.rows[0]
        self.assertEqual(
            row["path"],
            f"{container}{paths.ARCHIVE_SEPARATOR}{member}",
            msg="the container relative to the root, one separator, then the member",
        )
        self.assertEqual(
            row["path"].count(paths.ARCHIVE_SEPARATOR),
            1,
            msg="'!' is the single separator between container and member",
        )
        self.assertRelativePath(row["path"], where="archive-member row")
        self.assertFalse(
            row["in_scope"],
            msg=(
                "a non-filesystem coordinate is never in scope, and the rule is applied "
                "before the globs: on its segments alone this path would match one"
            ),
        )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_NON_FILESYSTEM_PATHS],
            1,
            msg="and it is counted, so the reported proportion can include it",
        )
        self.assertEqual(
            adapted.counters[
                f"{trivy.COUNTER_PATH_KIND_PREFIX}{paths.PATH_KIND_ARCHIVE_MEMBER}"
            ],
            1,
            msg="under the archive-member kind rather than as a tree file",
        )
        self.assertTrue(
            paths.is_non_filesystem_kind(paths.PATH_KIND_ARCHIVE_MEMBER),
            msg="which paths.py classifies as a non-filesystem coordinate",
        )

    def test_a_path_inside_a_test_tree_is_kept_with_in_scope_false(self) -> None:
        """The ``src/test`` exclusion is literal, and it excludes without dropping.

        A path containing ``src/test`` is out of scope and the exclusion overrides a
        positive glob match -- but the row is kept, exactly as a row outside the globs is
        kept.  Asserted here because "out of scope" and "dropped" are the two readings
        this dataset must never conflate.
        """
        env = environment()
        # Under python/pyspark/**, so the glob matches and the exclusion is what decides.
        # A Scala test tree such as core/src/test/... would be out of scope for a second
        # reason -- no glob covers it at all -- and would therefore establish nothing
        # about which of the two rules did the work.
        target = "python/pyspark/example/src/test/example_module.py"
        self.assertTrue(
            paths.contains_src_test(target),
            msg="the path does carry the literal marker",
        )
        document = derived(FEATURES_FIXTURE)
        document["Results"] = [copy.deepcopy(document["Results"][1])]
        document["Results"][0]["Target"] = target
        adapted = adapt_document(document)
        self.assertEqual(len(adapted.rows), 2, msg="both records are kept")
        self.assertEqual(adapted.rejections, [])
        for index, row in enumerate(adapted.rows):
            with self.subTest(row=index):
                self.assertEqual(row["path"], target)
                self.assertFalse(
                    row["in_scope"],
                    msg="the exclusion overrides the glob match that would otherwise hold",
                )
        self.assertEqual(
            adapted.counters[trivy.COUNTER_ROWS_OUT_OF_SCOPE],
            2,
            msg="and both are counted out of scope rather than removed",
        )
        self.assertIsNotNone(
            paths.matches_any_glob(target, env.globs),
            msg="the glob would have matched, which is what makes the override the rule",
        )

    def test_a_target_that_names_no_file_is_still_expressed_against_the_root(self) -> None:
        """The fixture's ``Node.js`` Target names an ecosystem, and no row escapes the root.

        Its record refines the Target with a ``PkgPath``, so the emitted path is the
        manifest that package was found in -- a real in-scope file.  What is asserted here
        is the invariant that holds regardless: no emitted path is absolute, and none is
        the ecosystem label itself.
        """
        ecosystem_targets = [
            record for record in self.adapted.records if record.target == "Node.js"
        ]
        self.assertEqual(
            len(ecosystem_targets), 1, msg="the fixture carries one such Target"
        )
        row = self.adapted.rows[self.adapted.records.index(ecosystem_targets[0])]
        self.assertEqual(
            row["path"],
            "core/src/main/resources/org/apache/spark/ui/static/package.json",
            msg="the per-record path refined a Target that names no file",
        )
        self.assertTrue(
            row["in_scope"],
            msg="and that manifest is the one manifest-shaped file inside the globs",
        )
        self.assertFalse(
            paths.looks_like_archive_container("Node.js"),
            msg="'.js' is not an archive extension, so no member serialization arises",
        )
        for index, candidate in enumerate(self.adapted.rows):
            with self.subTest(row=index):
                self.assertNotEqual(
                    candidate["path"],
                    "Node.js",
                    msg="an ecosystem label is never emitted as a path",
                )


if __name__ == "__main__":  # pragma: no cover - exercised through unittest discovery
    unittest.main(verbosity=2)
