"""The mandated negative test of the normalizer's shape detection and routing.

Requirement (AAP 0.5.4, verbatim): the mandated negative test *"asserts the direction
that actually goes wrong -- that a native artifact does **not** route to the SARIF
adapter -- because a permissive detector that accepts a native file as SARIF produces an
empty result set rather than an error, and an empty result set is indistinguishable from
a clean scan."* AAP 0.6.1 gives this file its own row, AAP 0.9.4 lists it in the
definition of done, and AAP 0.9.2 makes a failure here a condition that stops the run.

Why the detector needs an assertion of its own
----------------------------------------------
The failure this file guards is silent. A detector that accepted ``gitleaks.json`` as
SARIF would not raise: it would look for ``runs[].results[]``, find nothing, and report
success with zero rows. Nothing downstream can tell that apart from a scan that found
nothing -- ``findings.json`` is row-only, so a tool contributing no row is invisible in
it by construction (AAP 0.5.4). The only place the difference can be established is
here, at the detector, against artifacts of every shape the contract admits.

The contract under test -- exactly two conditions
-------------------------------------------------
A document is SARIF when it carries ``version == "2.1.0"`` **together with** a ``runs``
array; AAP 0.5.4: *"those two together are the test."* The conjunction is why the
near-miss fixtures are the substance of this file rather than decoration: each fails
exactly one half, so an implementation that checks only ``version`` is caught by
``near-sarif-runs-only.json`` and one that checks only ``runs`` is caught by
``near-sarif-version-only.json``. Neither fixture alone pins the conjunction.

The eight required assertions, and the class that owns each
-----------------------------------------------------------
1. ``is_sarif`` is conjunctive .................. :class:`IsSarifConjunctionTests`
2. a native artifact never routes to SARIF ...... :class:`NativeArtifactRoutingTests`
3. a native artifact routes to its own key ...... :class:`NativeArtifactRoutingTests`
4. the three SARIF producers share one key ...... :class:`SarifProducerRoutingTests`
5. ``route()`` returns a string key, not a module :class:`RoutingDecisionTypeTests`
6. an ``osv-scanner`` key exists ................ :class:`AdapterTableCompletenessTests`
7. an unknown shape halts, by name .............. :class:`UnknownShapeHaltTests`
8. detection is content-based, not filename-based :class:`ContentNotFilenameTests`

:class:`FixtureInventoryTests` precedes them: a fixture that failed to parse, or a
fixture silently absent, would let every assertion above pass over an empty loop.

How to run it
-------------
From the repository root::

    python3 -m unittest discover -s oss-scan-results/adapter-tests \\
        -p 'test_shape_routing_negative.py' -v

That exact command is what ``harness/artifacts/logs/adapter-tests-run.json`` records and
``oss-scan-results/adapter-tests/README.md`` echoes. It needs no installed package and no
plugin: standard library only (AAP 0.4.1), and AAP 0.4.3 adds no dependency in any
direction, so no third-party test runner is imported and none is required.

What this file deliberately does not do
---------------------------------------
It compares *shapes*, never tools. No assertion, message, comment or docstring here
ranks, contrasts or explains one tool's output against another's -- AAP 0.3.2 forbids
cross-tool interpretation of any kind, and AAP 0.8.2 restates it. No finding is judged
real, important, a false positive or a duplicate; nothing is deduplicated; no result is
compared against any commercial scanner. No secret value appears anywhere in this file:
this tree is committed to git, since ``.gitignore:31`` ignores only ``artifacts/``. The
fixtures are read and never written, and nothing under ``harness/lib/normalize/`` is
edited from here -- a defect this file reveals there is reported, not repaired.

No user-specified rule governs this file; enterprise-standard best practice applies in
its place (AAP 0.7, AAP 0.10.2). That absence is expressly not licence to lower the bar
-- this file is held to the AAP's own bar, so every assertion below names the exact key
or the exact exception type rather than settling for something truthy, and no negative
assertion is softened into a smoke test.
"""

from __future__ import annotations

# Standard library only (AAP 0.4.1), and only these:
#   json        -- parse a fixture without mutating it;
#   sys         -- the sys.path bootstrap, and the sys.modules snapshot proving routing
#                  imports no adapter;
#   types       -- ModuleType, so "a string key, not a module" is asserted against the
#                  actual module type rather than against a stand-in for it;
#   unittest    -- the runner, so the suite needs no third-party plugin;
#   unittest.mock -- capability fakes for the CLI classes at the foot of this file: the
#                  only way to drive a vocabulary disagreement or an adapter module
#                  without a callable entry point, both of which are programming faults
#                  the authored modules cannot exhibit on their own;
#   pathlib     -- fixture locations derived from __file__, so nothing depends on the
#                  working directory the runner happened to start in;
#   argparse    -- the parser's own types, so the option contract is asserted against
#                  argparse rather than against a reimplementation of it;
#   contextlib  -- redirect the CLI's stdout/stderr diagnostics into a buffer instead of
#                  the test runner's console, so they can be asserted on;
#   dataclasses -- the frozen-instance error and the field list of ``cli.Inputs``;
#   io          -- the buffers those diagnostics are redirected into;
#   os          -- absolute-path arithmetic for the input-resolution assertions;
#   platform    -- the running interpreter's version, read independently of ``cli``;
#   shutil      -- copy a committed fixture into a temporary raw tree, never the reverse;
#   tempfile    -- every directory the CLI classes hand the normalizer.
import argparse
import contextlib
import dataclasses
import io
import hashlib
import json
import os
import platform
import shutil
import sys
import tempfile
import types
import unittest
import unittest.mock
from collections.abc import Mapping
from pathlib import Path

# --------------------------------------------------------------------------------------
# The one-time sys.path bootstrap.
#
# There is deliberately no __init__.py under harness/lib/normalize/: PEP 420 implicit
# namespace packages make "from normalize import shape" work once harness/lib is on
# sys.path. cli.py owns the same two lines for its own direct-script route (see its
# bootstrap comment), and this file mirrors them rather than assuming an installed
# package, because nothing installs this tree -- AAP 0.4.3 adds no dependency in any
# direction and there is no manifest, no lockfile and no install step.
#
# This file sits at <repo>/oss-scan-results/adapter-tests/, so parents[2] is the
# repository root and the entry is derived from __file__ rather than from the working
# directory: the suite therefore imports identically whether it is discovered from the
# repository root or from anywhere else on the filesystem. The membership guard makes
# repeated imports idempotent -- unittest discovery imports sibling test modules that
# perform the same insertion, and a second copy of one path entry is noise that outlives
# the run in sys.path.
# --------------------------------------------------------------------------------------
_THIS_FILE = Path(__file__).resolve()
_TESTS_DIR = _THIS_FILE.parent
REPO_ROOT = _THIS_FILE.parents[2]
_LIB_DIR = str(REPO_ROOT / "harness" / "lib")
if _LIB_DIR not in sys.path:
    sys.path.insert(0, _LIB_DIR)

from normalize import (  # noqa: E402  (imports follow the bootstrap by necessity)
    cli,
    emit,
    paths,
    reconcile,
    severity,
    shape,
)

#: Where the captured and authored artifacts live. Derived from this file's location,
#: never from the working directory.
FIXTURES_DIR = _TESTS_DIR / "fixtures"

#: Where each fixture's hand-verified expected outcome lives.
EXPECTED_DIR = _TESTS_DIR / "expected"

#: The two authored near-miss documents. Each satisfies exactly one half of the SARIF
#: conjunction, so together they pin it; neither is a substitute for the other.
NEAR_MISS_FIXTURES = (
    "near-sarif-version-only.json",
    "near-sarif-runs-only.json",
)

#: The fall-through: a document matching neither the SARIF shape nor any known native
#: shape, whose required outcome is a halt (AAP 0.5.4).
UNKNOWN_SHAPE_FIXTURE = "unknown-shape.json"

#: Checkov's second top-level form -- an array of per-framework report objects rather
#: than a single report object. AAP 0.5.4 requires both forms handled, so both must
#: detect non-SARIF. Named separately because it is not one of the nine artifact
#: filenames: it is the ``checkov.json`` artifact in its other shape.
CHECKOV_ALT_SHAPE_FIXTURE = "checkov-alt-shape.json"

#: A plausible Trivy 0.74.0 filesystem report whose ``Results`` member is present and
#: **null**. It is the one document over which Trivy's two authorities could disagree:
#: Trivy is written in Go, which marshals an unset slice as ``null`` rather than as
#: ``[]``, so this is a shape a real writer can emit. AAP 0.5.4 names the count unit
#: ``Results[]``, halts on an artifact matching no known native shape rather than
#: best-effort parsing it, and rejects rather than infers -- so the contract refuses it,
#: and it must be refused by the envelope predicate and by the declared native signature
#: alike. Its sibling control is ``{"Results": []}``, a clean report that must route.
NEAR_TRIVY_RESULTS_NULL_FIXTURE = "near-trivy-results-null.json"

#: The two authored empty containers, presented under *recognised* artifact filenames.
#: They are the documents a router keying on the filename alone would admit, and they are
#: a pair on purpose: the object satisfies no tool's envelope and halts under all nine
#: names, while the array is gitleaks' whole envelope and routes under that one name.
#: Together they establish that the check keys on the container AAP 0.5.4's per-shape
#: table names, never on whether anything is inside it -- an over-strict router that
#: refused an empty report would stop the run over a tool that found nothing.
EMPTY_OBJECT_FIXTURE = "halt-shape-empty-object.json"
EMPTY_ARRAY_FIXTURE = "halt-shape-empty-array.json"

#: The tool expected to write no artifact at all, so no fixture bears its name.
#: OSV-Scanner exits 128 with "No package sources found" over a scope holding zero
#: resolvable dependency manifests (AAP 0.2.1): the one manifest-shaped file in scope,
#: core/src/main/resources/org/apache/spark/ui/static/package.json, carries a name, a
#: license and a type and no dependencies block. Its adapter key is still asserted
#: present -- see :class:`AdapterTableCompletenessTests` -- because the key's absence,
#: not the artifact's, is what would turn a legitimately written artifact into a halt.
TOOL_WITHOUT_A_FIXTURE = "osv-scanner"


#: The prefix the authored malformed-document fixtures carry. They cannot be named for
#: the artifacts they imitate -- ``fixtures/checkov.json`` is already the captured
#: positive fixture -- so each is named ``malformed-<canonical tool>.json`` and is routed
#: under the *artifact* filename its writer uses. That indirection is asserted rather
#: than assumed in :class:`MalformedKnownArtifactHaltTests`: the fixture's own name
#: resolves to no tool, so routing under it would halt for the *name* reason and the
#: signature assertion would pass over the wrong condition.
MALFORMED_FIXTURE_PREFIX = "malformed-"


def _malformed_known_fixtures() -> tuple[tuple[str, str, str], ...]:
    """Return ``(fixture name, canonical tool, artifact filename)`` per malformed fixture.

    Derived from ``shape.NATIVE_SIGNATURE_TOOLS`` rather than written out, for the same
    reason :func:`_artifact_fixture_names` is: the six signature-bearing writers have one
    authority in ``harness/lib/normalize/shape.py``, and a second list here would keep
    passing after that table changed.

    A signature-bearing writer with no malformed fixture is excluded here and asserted on
    in :class:`MalformedKnownArtifactHaltTests`, so an absent fixture surfaces as one
    named failure rather than as a silently shorter loop. Exactly one such writer is
    expected: ``osv-scanner`` writes no artifact at all (AAP 0.5.4), so there is no
    document of its to malform.
    """
    entries = []
    for tool in shape.NATIVE_SIGNATURE_TOOLS:
        fixture_name = f"{MALFORMED_FIXTURE_PREFIX}{tool}.json"
        if (FIXTURES_DIR / fixture_name).is_file():
            entries.append((fixture_name, tool, shape.artifact_filename_for(tool)))
    return tuple(entries)


#: The authored malformed documents: a real writer's envelope with the one container its
#: native signature requires broken. Each must halt under that writer's artifact name.
MALFORMED_KNOWN_FIXTURES = _malformed_known_fixtures()

#: The smallest legitimate document per signature-bearing writer -- the shape each writes
#: when it finds nothing -- with the reason it is legitimate. These are asserted to
#: **route**, not to halt, and they are the reason the halts above are safe: a signature
#: strict enough to reject an empty finding set would stop every clean scan, which is a
#: louder failure than the silent one this file exists to prevent but a failure all the
#: same. They are authored inline rather than committed as fixtures because each is one
#: or two keys wide: a file holding ``[]`` documents nothing, while the tuple below can
#: carry the sentence explaining why the document is legitimate.
LEGITIMATE_EMPTY_DOCUMENTS: tuple[tuple[str, object, str], ...] = (
    (
        "gitleaks",
        [],
        "gitleaks writes a bare JSON array, so its clean scan is the empty array itself",
    ),
    (
        "checkov",
        {"results": {"failed_checks": []}},
        "a single-report document whose failed_checks is empty; this dataset emits "
        "failed checks only, so a framework that failed nothing still reports",
    ),
    (
        "checkov",
        {"results": {}},
        "the same single-report form with failed_checks absent entirely, which the "
        "signature deliberately does not require: it reads nothing inside results",
    ),
    (
        "checkov",
        [{"results": {"failed_checks": []}}],
        "the multi-framework form AAP 0.5.4 requires handled -- an array of report "
        "objects -- carrying one framework that failed nothing",
    ),
    (
        "trivy",
        {"Results": []},
        "a report whose scan resolved no target at all; the finding sections live "
        "inside the elements, so an empty array is a complete report",
    ),
    (
        "osv-scanner",
        {"results": []},
        "the document OSV-Scanner would write over a scope holding no resolvable "
        "package source, asserted here because its adapter key exists whether or not "
        "the artifact does",
    ),
    (
        "dependency-check",
        {"dependencies": []},
        "a report that resolved no dependency, which is the expected honest outcome "
        "for a scope carrying no dependency manifest",
    ),
    (
        "joern",
        {"findings": []},
        "a query set that matched nothing; no envelope member is required, so the "
        "collector may add one without invalidating its own artifact",
    ),
)


def _artifact_fixture_names(*, sarif_producers: bool) -> tuple[str, ...]:
    """Return the artifact filenames to exercise, in canonical tool order.

    Derived from ``shape.CANONICAL_TOOLS``, ``shape.SARIF_PRODUCERS`` and
    ``shape.artifact_filename_for`` rather than written out here. The nine identifiers
    and nine filenames have one authority -- ``harness/lib/normalize/shape.py`` -- and a
    second copy in a test is a copy that drifts: it would keep passing after the module
    it tests changed, which is the one failure a test must not have.

    A tool whose fixture is missing from disk is *excluded here and asserted on* in
    :class:`FixtureInventoryTests`, so a vanished fixture surfaces as one named failure
    rather than as a silently shorter loop in every assertion below.
    """
    names = []
    for tool in shape.CANONICAL_TOOLS:
        if (tool in shape.SARIF_PRODUCERS) is not sarif_producers:
            continue
        filename = shape.artifact_filename_for(tool)
        if (FIXTURES_DIR / filename).is_file():
            names.append(filename)
    return tuple(names)


#: The three artifacts whose runners write SARIF 2.1.0. Positive controls: a detector
#: that rejected everything would satisfy every negative assertion in this file, so the
#: positives are what make the negatives mean something.
SARIF_ARTIFACT_FIXTURES = _artifact_fixture_names(sarif_producers=True)

#: The native artifacts present on disk -- the subjects of the mandated negative
#: direction. Five of the six non-SARIF-producing tools write one; OSV-Scanner does not.
NATIVE_ARTIFACT_FIXTURES = _artifact_fixture_names(sarif_producers=False)


def _every_non_sarif_fixture() -> tuple[str, ...]:
    """Return every fixture required to detect non-SARIF, in a stable order.

    The five native artifacts, Checkov's alternative top-level form, both near-miss
    documents, the fall-through document and the Trivy near-miss whose ``Results`` is
    null. ``checkov-alt-shape.json`` is included when present and its presence is
    asserted in :class:`FixtureInventoryTests`, so the inclusion is visible rather than
    conditional in effect.
    """
    names = list(NATIVE_ARTIFACT_FIXTURES)
    if (FIXTURES_DIR / CHECKOV_ALT_SHAPE_FIXTURE).is_file():
        names.append(CHECKOV_ALT_SHAPE_FIXTURE)
    names.extend(NEAR_MISS_FIXTURES)
    names.append(UNKNOWN_SHAPE_FIXTURE)
    names.append(NEAR_TRIVY_RESULTS_NULL_FIXTURE)
    return tuple(names)


#: Every document in this folder that must not be detected as SARIF.
NON_SARIF_FIXTURES = _every_non_sarif_fixture()


# --------------------------------------------------------------------------------------
# Shared loading and subject construction
# --------------------------------------------------------------------------------------


class ShapeTestCase(unittest.TestCase):
    """Loading and subject helpers shared by every class below.

    Three properties matter and each is a decision rather than a convenience:

    * **A missing fixture fails, it never skips.** The folder brief is explicit -- do not
      fabricate a fixture and do not skip the test; report the missing fixture as a
      blocking gap. A skipped negative test reports green, which is the same silent
      success the whole file exists to prevent.
    * **A fixture is never mutated.** Every load re-reads the file and parses it fresh,
      so no test can hand a mutated document to the next one. Nothing here writes to
      ``fixtures/`` at all, and the run leaves every fixture's sha256 unchanged.
    * **The subject passed to routing is repository-relative.** ``route`` echoes the
      caller's subject into the halt it raises, and the expected files under
      ``expected/`` record that subject as a repository-relative path. Passing an
      absolute path would both contradict those records and put a machine-specific path
      into a comparison, so the subject is always ``path.relative_to(REPO_ROOT)``.
    """

    def fixture_path(self, name: str) -> Path:
        """Return the path of fixture *name*, failing loudly when it is absent."""
        path = FIXTURES_DIR / name
        if not path.is_file():
            self.fail(
                f"blocking gap: required fixture {name!r} is absent from "
                f"{FIXTURES_DIR.relative_to(REPO_ROOT).as_posix()}. This test asserts "
                "over captured and authored artifacts and must not fabricate one, and "
                "it must not be skipped: a skipped negative test reports green over an "
                "assertion nobody made."
            )
        return path

    def load_fixture(self, name: str) -> object:
        """Parse fixture *name* and return the document, asserting well-formedness.

        Well-formedness is asserted rather than assumed: a fixture that stopped parsing
        would exercise none of the assertions that follow, and the resulting error would
        read as a detector failure rather than as a broken input. Malformed input is a
        different condition entirely, owned by this folder's
        ``reject-*-malformed-record`` fixtures inside an adapter that already owns its
        artifact.
        """
        path = self.fixture_path(name)
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            self.fail(
                f"blocking gap: fixture {name!r} is not well-formed JSON "
                f"({error.__class__.__name__} at line {error.lineno} column "
                f"{error.colno}). Shape detection cannot be exercised over a document "
                "that does not parse, and this failure is a broken fixture rather than "
                "a detector defect."
            )

    def load_expected(self, name: str) -> dict:
        """Parse the hand-verified expected outcome for fixture *name*.

        The expected file is the authority this test is checked against, and the
        cross-check runs in both directions: the values recorded there are asserted
        against the fixture they describe, so neither can drift from the other without a
        named failure.
        """
        expected_name = f"{Path(name).stem}.rows.json"
        path = EXPECTED_DIR / expected_name
        if not path.is_file():
            self.fail(
                f"blocking gap: expected outcome {expected_name!r} is absent from "
                f"{EXPECTED_DIR.relative_to(REPO_ROOT).as_posix()}; the hand-verified "
                f"expectation for fixture {name!r} is what this test is checked against."
            )
        document = json.loads(path.read_text(encoding="utf-8"))
        self.assertIsInstance(
            document,
            dict,
            msg=f"expected outcome {expected_name!r} must be a JSON object",
        )
        return document

    def subject(self, name: str) -> str:
        """Return the repository-relative path string to route fixture *name* under."""
        return self.fixture_path(name).relative_to(REPO_ROOT).as_posix()

    def require(self, document: dict, *keys: str) -> object:
        """Return ``document[keys[0]][keys[1]]...``, failing when a key is absent.

        A missing key in an expected file is a drift between two authored documents and
        is reported as such, rather than absorbed by a default that would quietly stop
        asserting the thing the key carried.
        """
        cursor: object = document
        for depth, key in enumerate(keys):
            if not isinstance(cursor, dict) or key not in cursor:
                trail = " -> ".join(keys[: depth + 1])
                self.fail(
                    f"blocking gap: the expected file does not carry {trail!r}; this "
                    "test asserts against that recorded value and cannot substitute a "
                    "default for it."
                )
            cursor = cursor[key]
        return cursor

    def assertNoAdapterImportedBy(self, description: str, call) -> object:
        """Run *call* and assert it imported no adapter module, returning its result.

        ``shape.py`` imports no adapter at all -- routing names one by string key and
        ``cli.py`` resolves the key to a callable -- so an adapter cannot have run as a
        side effect of a routing call. The snapshot is taken around the call rather than
        read once at the end, which is what makes the assertion hold no matter what
        else the same interpreter has already imported: a sibling test module in the
        same discovery run legitimately imports every adapter.
        """
        before = {name for name in sys.modules if name.startswith(shape.ADAPTER_PACKAGE)}
        result = call()
        after = {name for name in sys.modules if name.startswith(shape.ADAPTER_PACKAGE)}
        self.assertEqual(
            after - before,
            set(),
            msg=(
                f"{description} imported an adapter module. Routing must name an "
                "adapter by string key and import none: a detector that reaches into "
                "the adapter layer cannot be exercised in isolation, and an adapter "
                "invoked during detection could produce rows from a document whose "
                "shape was never established."
            ),
        )
        return result



# --------------------------------------------------------------------------------------
# The inventory, asserted before anything is detected
# --------------------------------------------------------------------------------------


class FixtureInventoryTests(ShapeTestCase):
    """Assert the inventory this file iterates is the inventory it is meant to iterate.

    Every assertion in the classes below loops over a derived tuple of fixture names. A
    fixture missing from disk shortens that tuple, and a loop over a shorter tuple still
    passes -- so the inventory is established here, once, as named assertions. This is
    the only class whose failure means "the inputs are wrong" rather than "the detector
    is wrong", and telling those two apart is worth a class of its own.
    """

    def test_exactly_one_canonical_tool_has_no_artifact_fixture(self) -> None:
        """Eight of the nine tools have a fixture; the ninth is OSV-Scanner.

        Derived from ``shape.CANONICAL_TOOLS`` and ``shape.artifact_filename_for``, so
        the inventory is checked against the authored table rather than against a list
        repeated here. OSV-Scanner's absence is expected and stated: it writes no
        artifact over a scope holding zero resolvable dependency manifests, so there is
        no output of its to capture. Any *other* tool missing a fixture is a blocking
        gap, and OSV-Scanner acquiring one would mean an artifact was captured for a
        tool this run expects to write none.
        """
        missing = tuple(
            tool
            for tool in shape.CANONICAL_TOOLS
            if not (FIXTURES_DIR / shape.artifact_filename_for(tool)).is_file()
        )
        self.assertEqual(
            missing,
            (TOOL_WITHOUT_A_FIXTURE,),
            msg=(
                "the set of canonical tools without an artifact fixture must be exactly "
                f"({TOOL_WITHOUT_A_FIXTURE!r},). A different tool missing one is a "
                "blocking gap in this folder; OSV-Scanner having one would contradict "
                "the expectation that it writes no artifact."
            ),
        )
        self.assertEqual(
            len(SARIF_ARTIFACT_FIXTURES),
            len(shape.SARIF_PRODUCERS),
            msg=(
                "every SARIF-producing tool must have a positive-control fixture: "
                f"{SARIF_ARTIFACT_FIXTURES} against producers "
                f"{tuple(sorted(shape.SARIF_PRODUCERS))}"
            ),
        )
        self.assertEqual(
            len(NATIVE_ARTIFACT_FIXTURES),
            len(shape.CANONICAL_TOOLS) - len(shape.SARIF_PRODUCERS) - 1,
            msg=(
                "the native artifact fixtures must cover every non-SARIF-producing tool "
                f"except {TOOL_WITHOUT_A_FIXTURE!r}: got {NATIVE_ARTIFACT_FIXTURES}"
            ),
        )

    def test_authored_near_miss_and_fall_through_fixtures_are_present(self) -> None:
        """Both near-miss documents, the fall-through and the Trivy near-miss exist.

        The near-miss pair is what pins the conjunction, the fall-through is what
        exercises the halt, and ``near-trivy-results-null.json`` is what pins one native
        writer's envelope against its own declared signature. Losing any one of the four
        would leave a whole branch of the contract unasserted while the suite still
        reported green.
        """
        for name in (
            *NEAR_MISS_FIXTURES,
            UNKNOWN_SHAPE_FIXTURE,
            NEAR_TRIVY_RESULTS_NULL_FIXTURE,
        ):
            with self.subTest(fixture=name):
                self.assertTrue(
                    (FIXTURES_DIR / name).is_file(),
                    msg=f"blocking gap: authored fixture {name!r} is absent",
                )

    def test_checkov_alternative_top_level_form_is_present(self) -> None:
        """Checkov's array-of-reports form is present and is exercised.

        Checkov is the one artifact whose top level changes with its content -- a single
        report object, or an array of per-framework report objects (AAP 0.5.4 requires
        both handled). Both must detect non-SARIF, so both are exercised, and the
        alternative form's presence is asserted here rather than left to a conditional
        that would quietly stop exercising it.
        """
        self.assertTrue(
            (FIXTURES_DIR / CHECKOV_ALT_SHAPE_FIXTURE).is_file(),
            msg=(
                f"blocking gap: {CHECKOV_ALT_SHAPE_FIXTURE!r} is absent, so Checkov's "
                "array-of-reports top-level form would go unexercised"
            ),
        )
        self.assertIn(
            CHECKOV_ALT_SHAPE_FIXTURE,
            NON_SARIF_FIXTURES,
            msg="Checkov's alternative top-level form must be exercised as non-SARIF",
        )

    def test_every_exercised_fixture_is_well_formed_json(self) -> None:
        """Every fixture this file reads parses, and parses to an object or an array.

        Both container forms are legitimate -- ``gitleaks.json`` and Checkov's
        multi-framework form are top-level arrays -- so the assertion is that the top
        level is one of the two, never that it is a mapping. A scalar top level is a
        third thing entirely, which ``shape.py`` classifies under its own halt reason.
        """
        for name in (*SARIF_ARTIFACT_FIXTURES, *NON_SARIF_FIXTURES):
            with self.subTest(fixture=name):
                document = self.load_fixture(name)
                self.assertIsInstance(
                    document,
                    (dict, list),
                    msg=(
                        f"fixture {name!r} must parse to a JSON object or a JSON array; "
                        f"got {type(document).__name__}"
                    ),
                )

    def test_bare_array_fixtures_are_bare_arrays(self) -> None:
        """The two array-topped fixtures really are arrays.

        This is the sharpest native-shape subject in the folder: a detector that assumed
        a mapping and called ``.get`` on the document would raise on these rather than
        return ``False``, and an exception from the detector is a different failure from
        a correct negative. The assertion below establishes that these fixtures actually
        present that hazard, so the ``False``-rather-than-raise assertion in
        :class:`IsSarifConjunctionTests` is exercising it.
        """
        for name in (shape.artifact_filename_for("gitleaks"), CHECKOV_ALT_SHAPE_FIXTURE):
            with self.subTest(fixture=name):
                self.assertIsInstance(
                    self.load_fixture(name),
                    list,
                    msg=f"fixture {name!r} is expected to carry a top-level JSON array",
                )


# --------------------------------------------------------------------------------------
# Required assertion 1 -- is_sarif is conjunctive
# --------------------------------------------------------------------------------------


class IsSarifConjunctionTests(ShapeTestCase):
    """``is_sarif`` is the conjunction of both conditions and nothing looser.

    AAP 0.5.4: a document is SARIF when it carries ``version == "2.1.0"`` together with
    a ``runs`` array, and those two together are the test. The positive controls come
    first, because a detector that returned ``False`` for everything would satisfy every
    negative assertion in this file.
    """

    def test_sarif_producer_artifacts_detect_as_sarif(self) -> None:
        """Each of the three SARIF artifacts detects as SARIF."""
        for name in SARIF_ARTIFACT_FIXTURES:
            with self.subTest(fixture=name):
                self.assertIs(
                    shape.is_sarif(self.load_fixture(name)),
                    True,
                    msg=(
                        f"fixture {name!r} carries version {shape.SARIF_VERSION!r} and a "
                        f"{shape.SARIF_RUNS_KEY} array and must detect as SARIF; a "
                        "detector that rejects everything satisfies every negative "
                        "assertion in this file while detecting nothing correctly"
                    ),
                )

    def test_native_artifacts_do_not_detect_as_sarif(self) -> None:
        """None of the five native artifacts detects as SARIF."""
        for name in NATIVE_ARTIFACT_FIXTURES:
            with self.subTest(fixture=name):
                self.assertIs(
                    shape.is_sarif(self.load_fixture(name)),
                    False,
                    msg=(
                        f"fixture {name!r} is a native artifact and must not detect as "
                        "SARIF. A native document accepted as SARIF does not raise: the "
                        "shared adapter finds no runs[].results[] to walk and reports "
                        "success with zero rows, and an empty result set is "
                        "indistinguishable from a clean scan (AAP 0.5.4)"
                    ),
                )

    def test_checkov_alternative_top_level_form_does_not_detect_as_sarif(self) -> None:
        """Checkov's array-of-reports form detects non-SARIF, like its object form."""
        self.assertIs(
            shape.is_sarif(self.load_fixture(CHECKOV_ALT_SHAPE_FIXTURE)),
            False,
            msg=(
                f"fixture {CHECKOV_ALT_SHAPE_FIXTURE!r} is Checkov's array-of-reports "
                "top-level form and must not detect as SARIF; both of that artifact's "
                "top-level forms are required to detect non-SARIF"
            ),
        )

    def test_near_miss_documents_do_not_detect_as_sarif(self) -> None:
        """Neither near-miss document detects as SARIF.

        Each satisfies one half of the conjunction, so each is accepted by a detector
        that checks only that half. Together they are the pair that pins it.
        """
        for name in NEAR_MISS_FIXTURES:
            with self.subTest(fixture=name):
                self.assertIs(
                    shape.is_sarif(self.load_fixture(name)),
                    False,
                    msg=(
                        f"fixture {name!r} satisfies exactly one half of the SARIF "
                        "conjunction and must not detect as SARIF; a detector that "
                        "checks one half accepts it"
                    ),
                )

    def test_bare_json_array_returns_false_rather_than_raising(self) -> None:
        """A top-level array returns ``False``; it does not raise.

        ``gitleaks.json`` is a bare JSON array, so an unguarded ``doc.get(...)`` in the
        detector would raise ``AttributeError`` here. A raise is not a correct negative:
        it would surface as a detector defect at a point where the contract requires a
        decision, and the same code path over Checkov's array form would fail the same
        way. ``assertIs(..., False)`` asserts the decision, and reaching the assertion
        at all asserts that no exception escaped.
        """
        for name in (shape.artifact_filename_for("gitleaks"), CHECKOV_ALT_SHAPE_FIXTURE):
            with self.subTest(fixture=name):
                document = self.load_fixture(name)
                self.assertIsInstance(document, list)
                self.assertIs(
                    shape.is_sarif(document),
                    False,
                    msg=(
                        f"fixture {name!r} carries a top-level JSON array; the detector "
                        "must decide False rather than raise on it"
                    ),
                )


    def test_near_miss_version_only_fails_only_the_runs_half(self) -> None:
        """``near-sarif-version-only.json`` passes the version half and fails runs.

        Naming which half fails matters: a regression that satisfied this test by
        failing the *other* half would have changed the document's meaning while the
        assertion still passed. The evidence comes from ``shape.detection_evidence``,
        which reports the two field checks separately and takes its conjunction from
        ``is_sarif`` itself, so the evidence cannot disagree with the decision.

        The ``runs`` key here is *present* and merely of the wrong type. That is the
        point: a detector written as ``"runs" in doc`` or ``bool(doc.get("runs"))``
        accepts this document, while the ``isinstance(..., list)`` test rejects it.
        """
        name = "near-sarif-version-only.json"
        evidence = shape.detection_evidence(self.load_fixture(name))
        self.assertIs(
            evidence["version_matches"],
            True,
            msg=f"fixture {name!r} is expected to carry the exact version string",
        )
        self.assertIs(
            evidence["runs_is_array"],
            False,
            msg=(
                f"fixture {name!r} is expected to carry a {shape.SARIF_RUNS_KEY} value "
                "that is not an array; that is the half it fails"
            ),
        )
        self.assertEqual(
            evidence["runs_observed_type"],
            "object",
            msg=(
                f"fixture {name!r} is expected to carry its {shape.SARIF_RUNS_KEY} as a "
                "JSON object -- present, and of the wrong type, so a presence test "
                "accepts it and a type test does not"
            ),
        )
        self.assertIs(evidence["is_sarif"], False, msg=f"{name!r} must not be SARIF")

    def test_near_miss_runs_only_fails_only_the_version_half(self) -> None:
        """``near-sarif-runs-only.json`` passes the runs half and fails version.

        This is the half an implementer is most likely to relax, and the fixture is
        built to defeat every relaxation: its version is a near miss rather than a
        missing or obviously wrong value, so a prefix test, a substring test, a
        split-on-hyphen test and any semantic-version comparison that treats a
        pre-release suffix as ignorable all accept it. Only exact string equality
        rejects it, which is what ``shape.py`` performs.

        The document also carries a populated ``runs`` array, so a detector that
        accepted it would emit plausible rows rather than none -- a wrong number that
        reads as a working parse. Both failure modes are worse than an error and neither
        raises.
        """
        name = "near-sarif-runs-only.json"
        document = self.load_fixture(name)
        evidence = shape.detection_evidence(document)
        self.assertIs(
            evidence["runs_is_array"],
            True,
            msg=f"fixture {name!r} is expected to carry a real {shape.SARIF_RUNS_KEY} array",
        )
        self.assertIs(
            evidence["version_matches"],
            False,
            msg=f"fixture {name!r} is expected to fail the version half of the test",
        )
        observed_version = evidence["version_observed"]
        self.assertIsInstance(observed_version, str)
        self.assertNotEqual(
            observed_version,
            shape.SARIF_VERSION,
            msg="the fixture's version must differ from the accepted version",
        )
        self.assertTrue(
            observed_version.startswith(shape.SARIF_VERSION),
            msg=(
                f"fixture {name!r} is expected to carry a version that a prefix "
                "comparison would wrongly accept; without that property the fixture "
                "stops exercising the exact-equality requirement"
            ),
        )
        self.assertIs(evidence["is_sarif"], False, msg=f"{name!r} must not be SARIF")

    def test_fall_through_document_fails_both_halves(self) -> None:
        """``unknown-shape.json`` fails both halves and matches no native signature.

        It is not a probe of the conjunction -- the near-miss pair is that -- but the
        fall-through: a document imitating nothing in the contract. Its top level
        carries neither key at all, so no relaxation of either half has anything to
        read.
        """
        document = self.load_fixture(UNKNOWN_SHAPE_FIXTURE)
        evidence = shape.detection_evidence(document)
        self.assertIs(evidence["version_matches"], False)
        self.assertIs(evidence["runs_is_array"], False)
        self.assertIs(
            evidence["is_sarif"],
            False,
            msg=f"fixture {UNKNOWN_SHAPE_FIXTURE!r} must fail both halves of the test",
        )
        self.assertIsInstance(document, dict)
        self.assertNotIn(shape.SARIF_VERSION_KEY, document)
        self.assertNotIn(shape.SARIF_RUNS_KEY, document)

    def test_expected_files_agree_with_the_fixtures_they_describe(self) -> None:
        """Each expected file's recorded detection values match its own fixture.

        The three expected files under ``expected/`` record the observed version, which
        half of the conjunction was met, the top-level keys and the top-level length for
        the document they describe. Asserting those against the fixture makes the pair
        checkable in both directions: neither the fixture nor its hand-verified
        expectation can change without the other, and a disagreement is reported here
        rather than discovered later as an unexplained result.
        """
        for name in (*NEAR_MISS_FIXTURES, UNKNOWN_SHAPE_FIXTURE):
            with self.subTest(fixture=name):
                document = self.load_fixture(name)
                expected = self.load_expected(name)
                evidence = shape.detection_evidence(document)

                self.assertIs(
                    self.require(expected, "shape_detection", "is_sarif"),
                    False,
                    msg=f"the expected file for {name!r} must record is_sarif False",
                )
                self.assertEqual(
                    self.require(expected, "shape_detection", "version_condition_met"),
                    evidence["version_matches"],
                    msg=(
                        f"the expected file for {name!r} records a different verdict on "
                        "the version half than the detector reports"
                    ),
                )
                self.assertEqual(
                    self.require(expected, "shape_detection", "runs_condition_met"),
                    evidence["runs_is_array"],
                    msg=(
                        f"the expected file for {name!r} records a different verdict on "
                        "the runs half than the detector reports"
                    ),
                )
                self.assertEqual(
                    self.require(expected, "shape_detection", "version_observed"),
                    evidence["version_observed"],
                    msg=(
                        f"the expected file for {name!r} records a version literal that "
                        "is not the one the fixture carries"
                    ),
                )
                self.assertEqual(
                    list(self.require(expected, "halt", "observed_attributes", "top_level_keys")),
                    [str(key) for key in evidence["top_level_key_excerpts"]],
                    msg=(
                        f"the expected file for {name!r} records top-level keys that are "
                        "not the fixture's, in order. Compared against the excerpts "
                        "rather than against evidence['top_level_keys']: an observed key "
                        "is artifact-supplied text, so the evidence carries each one as a "
                        "bounded redacted description -- type, length, sha256, excerpt -- "
                        "and the excerpt is the key itself where nothing needed escaping"
                    ),
                )
                self.assertEqual(
                    self.require(
                        expected, "halt", "observed_attributes", "top_level_length"
                    ),
                    evidence["top_level_length"],
                    msg=(
                        f"the expected file for {name!r} records a top-level length that "
                        "is not the fixture's"
                    ),
                )
                self.assertEqual(
                    self.require(expected, "counts", "rows"),
                    0,
                    msg=f"the expected outcome for {name!r} is zero rows",
                )
                self.assertEqual(
                    self.require(expected, "counts", "rejections"),
                    0,
                    msg=f"the expected outcome for {name!r} is zero rejections",
                )
                self.assertEqual(
                    self.require(expected, "rows"),
                    [],
                    msg=(
                        f"the expected outcome for {name!r} carries no rows: the "
                        "pipeline stops, it does not look and find nothing"
                    ),
                )



# --------------------------------------------------------------------------------------
# Required assertions 2 and 3 -- the mandated negative, and the positive it implies
# --------------------------------------------------------------------------------------


class NativeArtifactRoutingTests(ShapeTestCase):
    """A native artifact never routes to SARIF, and always routes to its own adapter.

    Required assertion 2 is the mandated direction (AAP 0.5.4): a native artifact must
    **not** route to the shared SARIF adapter. Required assertion 3 is its complement --
    it is not enough that a native artifact avoids the SARIF adapter, it must reach the
    one adapter that can read it -- and the two together close the gap a single
    assertion leaves: a router that sent every native artifact to some *other* wrong
    adapter would satisfy assertion 2 alone.
    """

    def routed(self, name: str):
        """Route fixture *name* under its own artifact filename and return the decision.

        Routed under a repository-relative path rather than a bare filename, because
        that is the form the run resolves in practice -- ``route_artifact`` takes the
        path of the artifact in ``harness/artifacts/raw/`` -- and it exercises the
        filename component being taken out of a path rather than compared whole.
        """
        return shape.route_artifact(self.subject(name), self.load_fixture(name))

    def test_no_native_artifact_routes_to_the_shared_sarif_adapter(self) -> None:
        """The mandated negative: no native artifact reaches the SARIF adapter.

        AAP 0.5.4 names this direction because it is the one that fails silently. A
        native artifact accepted as SARIF raises nothing: the shared adapter walks
        ``runs[].results[]``, finds neither, and reports success with zero rows. The
        dataset files are row-only, so a tool contributing no row is invisible in them,
        and the run's reconciliation identity balances at zero on both sides. Every
        downstream signal reads exactly like a clean scan.
        """
        for name in NATIVE_ARTIFACT_FIXTURES:
            with self.subTest(fixture=name):
                decision = self.routed(name)
                self.assertNotEqual(
                    decision.adapter,
                    shape.SHARED_SARIF_ADAPTER,
                    msg=(
                        f"native artifact {name!r} routed to the shared SARIF adapter "
                        f"{shape.SHARED_SARIF_ADAPTER!r}. This is the mandated negative "
                        "direction: the SARIF adapter would find no runs[].results[] in "
                        "this document, emit zero rows and report success, and an empty "
                        "result set is indistinguishable from a clean scan (AAP 0.5.4)"
                    ),
                )
                self.assertEqual(
                    decision.shape,
                    shape.SHAPE_NATIVE,
                    msg=(
                        f"native artifact {name!r} must be decided {shape.SHAPE_NATIVE!r}"
                    ),
                )
                self.assertIs(
                    decision.is_sarif_shape,
                    False,
                    msg=f"native artifact {name!r} must not carry the SARIF shape",
                )

    def test_each_native_artifact_routes_to_its_own_adapter_key(self) -> None:
        """Each native artifact reaches the adapter key ``shape.py`` names for its tool.

        The expected key is taken from ``shape.adapter_module_for`` rather than written
        out here, so the spelling asserted is the spelling the module defines -- the
        hyphen-to-underscore split between canonical tool identifiers and adapter module
        keys is real (``dependency-check`` writes ``dependency-check.json`` and is read
        by ``dependency_check``), and a test that guessed at it would either fail for the
        wrong reason or pass while asserting a spelling nothing uses.
        """
        for name in NATIVE_ARTIFACT_FIXTURES:
            with self.subTest(fixture=name):
                tool = shape.resolve_tool(name)
                self.assertIsNotNone(
                    tool,
                    msg=f"fixture {name!r} must be one of the nine artifact filenames",
                )
                decision = self.routed(name)
                self.assertEqual(
                    decision.tool,
                    tool,
                    msg=(
                        f"artifact {name!r} identifies its writing runner, so the "
                        f"decision's tool must be {tool!r}"
                    ),
                )
                self.assertEqual(
                    decision.adapter,
                    shape.adapter_module_for(tool),
                    msg=(
                        f"native artifact {name!r} must route to the adapter key "
                        f"shape.py names for {tool!r}"
                    ),
                )
                self.assertIn(
                    decision.adapter,
                    shape.ADAPTER_MODULES,
                    msg=(
                        f"the adapter key for {name!r} must be one of the authored "
                        f"module keys {shape.ADAPTER_MODULES}"
                    ),
                )

    def test_native_adapter_keys_are_distinct_per_tool(self) -> None:
        """The five native artifacts route to five different adapters.

        The routing table is deliberately not an identity function in one direction --
        three tools collapse onto ``sarif`` -- and deliberately injective in the other:
        each native artifact has its own reader, because each carries its own record
        location, count unit and field sources. Two native artifacts sharing an adapter
        would mean one of them was being read by a parser written for the other's shape.
        """
        adapters = [self.routed(name).adapter for name in NATIVE_ARTIFACT_FIXTURES]
        self.assertEqual(
            len(set(adapters)),
            len(NATIVE_ARTIFACT_FIXTURES),
            msg=(
                "each native artifact must route to its own adapter key; got "
                f"{adapters} for {NATIVE_ARTIFACT_FIXTURES}"
            ),
        )

    def test_native_routing_carries_the_tools_authored_scanner_class(self) -> None:
        """The decision carries the ``scanner_class`` the authored table fixes per tool.

        Fixed per tool with Trivy the single exception, whose class is decided per record
        from the section array it was read from (AAP 0.5.4). ``shape.py`` hands the Trivy
        adapter a sentinel rather than a plausible default, and that sentinel refuses to
        be stringified -- so this test compares it by identity and never interpolates a
        ``scanner_class`` into a message.
        """
        for name in NATIVE_ARTIFACT_FIXTURES:
            with self.subTest(fixture=name):
                tool = shape.resolve_tool(name)
                decision = self.routed(name)
                expected_class = shape.scanner_class_for(tool)
                if shape.is_per_record(expected_class):
                    self.assertTrue(
                        decision.scanner_class_is_per_record,
                        msg=(
                            f"{tool!r} is the one tool whose scanner_class is decided "
                            "per record, so the decision must carry the sentinel"
                        ),
                    )
                else:
                    self.assertFalse(
                        decision.scanner_class_is_per_record,
                        msg=f"{tool!r} has a fixed scanner_class, not a per-record one",
                    )
                    self.assertIn(
                        decision.scanner_class,
                        shape.SCANNER_CLASSES,
                        msg=(
                            f"the scanner_class for {tool!r} must be one of the four "
                            f"authored literals {shape.SCANNER_CLASSES}"
                        ),
                    )
                self.assertIs(
                    decision.scanner_class,
                    expected_class,
                    msg=(
                        f"the decision for {name!r} must carry the scanner_class the "
                        "authored table fixes for its tool"
                    ),
                )

    def test_native_routing_echoes_the_subject_it_was_given(self) -> None:
        """The decision names the artifact it was made about, unchanged.

        ``route`` never rewrites the subject: the run record and any halt report quote
        what the caller passed. Asserting it here keeps the record traceable to a file
        and keeps an absolute path from reaching a comparison -- the subject this test
        passes is repository-relative throughout.
        """
        for name in NATIVE_ARTIFACT_FIXTURES:
            with self.subTest(fixture=name):
                subject = self.subject(name)
                decision = shape.route_artifact(subject, self.load_fixture(name))
                self.assertEqual(
                    decision.artifact_path,
                    subject,
                    msg=f"the decision for {name!r} must echo the subject it was given",
                )
                self.assertFalse(
                    Path(decision.artifact_path).is_absolute(),
                    msg="no absolute path is passed to routing or recorded by this test",
                )


# --------------------------------------------------------------------------------------
# Required assertion 4 -- three producers, one adapter
# --------------------------------------------------------------------------------------


class SarifProducerRoutingTests(ShapeTestCase):
    """The three SARIF producers collapse onto the one shared adapter.

    The artifact-stem-to-adapter map is deliberately **not** an identity function: six
    adapters serve nine tools, and there is no ``opengrep``, ``semgrep`` or
    ``datadog_static_analyzer`` adapter to route to. One shared adapter is what makes the
    three producers' rows comparable field for field, since a per-producer adapter would
    be three implementations of one specification.
    """

    def test_all_three_sarif_artifacts_route_to_the_same_shared_key(self) -> None:
        """Every SARIF artifact routes to ``shape.SHARED_SARIF_ADAPTER``, and to one key."""
        adapters = set()
        for name in SARIF_ARTIFACT_FIXTURES:
            with self.subTest(fixture=name):
                decision = shape.route_artifact(
                    self.subject(name), self.load_fixture(name)
                )
                self.assertEqual(
                    decision.shape,
                    shape.SHAPE_SARIF,
                    msg=f"artifact {name!r} must be decided {shape.SHAPE_SARIF!r}",
                )
                self.assertIs(decision.is_sarif_shape, True)
                self.assertEqual(
                    decision.adapter,
                    shape.SHARED_SARIF_ADAPTER,
                    msg=(
                        f"artifact {name!r} must route to the shared SARIF adapter "
                        f"{shape.SHARED_SARIF_ADAPTER!r}"
                    ),
                )
                adapters.add(decision.adapter)
        self.assertEqual(
            adapters,
            {shape.SHARED_SARIF_ADAPTER},
            msg=(
                "the three SARIF producers must collapse onto exactly one adapter key; "
                f"got {sorted(adapters)}"
            ),
        )

    def test_no_per_producer_sarif_adapter_exists(self) -> None:
        """No adapter key is named after an individual SARIF producer.

        The map is not an identity function, and this is the assertion that says so
        directly: were a per-producer key to appear, the shared adapter would have
        acquired a sibling and the three producers' rows would stop being one
        implementation's output.
        """
        for tool in sorted(shape.SARIF_PRODUCERS):
            with self.subTest(tool=tool):
                self.assertEqual(
                    shape.adapter_module_for(tool),
                    shape.SHARED_SARIF_ADAPTER,
                    msg=f"{tool!r} must map to the shared adapter, not to its own",
                )
                self.assertNotIn(
                    tool.replace("-", "_"),
                    shape.ADAPTER_MODULES,
                    msg=(
                        f"no adapter module key may be named after the producer {tool!r}"
                    ),
                )



# --------------------------------------------------------------------------------------
# Required assertion 5 -- a string key, never a module
# --------------------------------------------------------------------------------------


class RoutingDecisionTypeTests(ShapeTestCase):
    """Routing names an adapter by string key and imports none.

    This is a structural property rather than a behavioural one, and it is asserted for
    a specific reason: it is what keeps the detector's import graph acyclic and lets
    this file exercise routing for all nine tools without importing six adapters. A
    later refactor that resolved the key to a module inside ``shape.py`` would keep every
    other assertion in this file passing while reintroducing the cycle -- so the property
    is pinned here, where its loss is a named failure.
    """

    def all_fixtures(self) -> tuple[str, ...]:
        """Every artifact-named fixture, SARIF and native alike."""
        return (*SARIF_ARTIFACT_FIXTURES, *NATIVE_ARTIFACT_FIXTURES)

    def test_route_returns_a_string_adapter_key_not_a_module(self) -> None:
        """``RoutingDecision.adapter`` is a ``str`` and is not a module object."""
        for name in self.all_fixtures():
            with self.subTest(fixture=name):
                decision = shape.route_artifact(
                    self.subject(name), self.load_fixture(name)
                )
                self.assertIsInstance(
                    decision.adapter,
                    str,
                    msg=(
                        f"the adapter identifier for {name!r} must be a string key; a "
                        "module object here would mean the detector had imported the "
                        "adapter layer it exists to stay independent of"
                    ),
                )
                self.assertNotIsInstance(
                    decision.adapter,
                    types.ModuleType,
                    msg=f"the adapter identifier for {name!r} must not be a module",
                )
                self.assertFalse(
                    hasattr(decision.adapter, "__spec__"),
                    msg=(
                        f"the adapter identifier for {name!r} carries a module's import "
                        "machinery, so it is a module in all but name"
                    ),
                )
                self.assertEqual(
                    decision.adapter_module_name,
                    f"{shape.ADAPTER_PACKAGE}.{decision.adapter}",
                    msg=(
                        "the importable module name must be composed from the key, "
                        "which is a string operation and not an import"
                    ),
                )
                self.assertIsInstance(decision.adapter_module_name, str)

    def test_routing_every_artifact_imports_no_adapter_module(self) -> None:
        """A full routing pass over every artifact imports nothing under the adapters.

        Asserted around the calls rather than read once at the end, so the result holds
        regardless of what the same interpreter imported earlier: a sibling test module
        in the same discovery run legitimately imports every adapter, and an assertion
        written against the absolute contents of ``sys.modules`` would fail there for a
        reason that has nothing to do with this property.
        """

        def route_everything() -> list[str]:
            return [
                shape.route_artifact(self.subject(name), self.load_fixture(name)).adapter
                for name in self.all_fixtures()
            ]

        adapters = self.assertNoAdapterImportedBy(
            "routing every artifact fixture", route_everything
        )
        self.assertEqual(
            len(adapters),
            len(self.all_fixtures()),
            msg="every artifact fixture must yield exactly one adapter key",
        )

    def test_the_detector_module_holds_no_adapter_module_reference(self) -> None:
        """``shape.py``'s own namespace holds no module from the adapter package.

        The complement of the assertion above: routing importing nothing is a statement
        about the call, and this is the statement about the module. An adapter imported
        at module scope would be present here even though no routing call had performed
        the import.
        """
        offenders = sorted(
            attribute
            for attribute, value in vars(shape).items()
            if isinstance(value, types.ModuleType)
            and getattr(value, "__name__", "").startswith(shape.ADAPTER_PACKAGE)
        )
        self.assertEqual(
            offenders,
            [],
            msg=(
                "the detector must import no adapter module at module scope; found "
                f"{offenders}"
            ),
        )


# --------------------------------------------------------------------------------------
# Required assertion 6 -- the table covers all nine tools, including the silent one
# --------------------------------------------------------------------------------------


class AdapterTableCompletenessTests(ShapeTestCase):
    """Every canonical tool has an adapter key, including the one writing no artifact.

    The ``osv-scanner`` entry exists precisely so that a legitimately written OSV
    artifact cannot fall into the halt path: its runner passes an output path
    unconditionally, so the artifact *can* exist, and an artifact present but unmapped
    would stop the run for a tool doing exactly what it was configured to do. This run
    expects no such artifact -- exit 128 over a scope holding zero resolvable dependency
    manifests -- so the key's presence is asserted and no OSV document is loaded.
    """

    def test_the_osv_scanner_key_is_present_in_the_adapter_map(self) -> None:
        """``osv-scanner`` has an artifact filename, an adapter key and a class."""
        self.assertIn(
            TOOL_WITHOUT_A_FIXTURE,
            shape.CANONICAL_TOOLS,
            msg=f"{TOOL_WITHOUT_A_FIXTURE!r} must be one of the nine canonical tools",
        )
        self.assertIn(
            TOOL_WITHOUT_A_FIXTURE,
            shape.ADAPTER_MODULE_BY_TOOL,
            msg=(
                f"{TOOL_WITHOUT_A_FIXTURE!r} must carry an adapter key even though it "
                "is expected to write no artifact: an artifact present but unmapped "
                "would halt the run for a tool behaving exactly as configured"
            ),
        )
        adapter = shape.adapter_module_for(TOOL_WITHOUT_A_FIXTURE)
        self.assertIsInstance(adapter, str)
        self.assertIn(
            adapter,
            shape.ADAPTER_MODULES,
            msg=f"the adapter key for {TOOL_WITHOUT_A_FIXTURE!r} must be an authored key",
        )
        self.assertNotEqual(
            adapter,
            shape.SHARED_SARIF_ADAPTER,
            msg=f"{TOOL_WITHOUT_A_FIXTURE!r} is not a SARIF producer",
        )
        filename = shape.artifact_filename_for(TOOL_WITHOUT_A_FIXTURE)
        self.assertEqual(
            shape.resolve_tool(filename),
            TOOL_WITHOUT_A_FIXTURE,
            msg="the artifact filename must resolve back to its tool",
        )
        self.assertIn(shape.scanner_class_for(TOOL_WITHOUT_A_FIXTURE), shape.SCANNER_CLASSES)

    def test_no_osv_scanner_artifact_is_loaded_by_this_file(self) -> None:
        """No OSV document is read here, because none is expected to exist.

        The key's presence is the assertion; the artifact's absence is the expectation.
        Fabricating an OSV fixture to exercise the key would test this folder's
        invention rather than the run's output, and the folder brief forbids fabricating
        a fixture.
        """
        filename = shape.artifact_filename_for(TOOL_WITHOUT_A_FIXTURE)
        self.assertFalse(
            (FIXTURES_DIR / filename).exists(),
            msg=(
                f"no {filename!r} fixture is expected in this folder: the tool writes no "
                "artifact over a scope holding zero resolvable dependency manifests, so "
                "there is no output of its to capture"
            ),
        )
        self.assertNotIn(filename, SARIF_ARTIFACT_FIXTURES)
        self.assertNotIn(filename, NATIVE_ARTIFACT_FIXTURES)

    def test_every_canonical_tool_maps_to_an_adapter_and_an_artifact(self) -> None:
        """All nine tools are covered by both authored tables, with no gap.

        The nine-tool inventory lives in ``tool-status.md`` and ``severity-map.md``, not
        in the row-only dataset files, and it stays complete for a tool that produced no
        row. The same completeness is required of these tables: a tool missing from
        either one is a tool whose artifact could not be read if it appeared.
        """
        for tool in shape.CANONICAL_TOOLS:
            with self.subTest(tool=tool):
                self.assertIn(tool, shape.ARTIFACT_FILENAME_BY_TOOL)
                self.assertIn(tool, shape.ADAPTER_MODULE_BY_TOOL)
                self.assertIn(tool, shape.SCANNER_CLASS_BY_TOOL)
                self.assertEqual(
                    shape.resolve_tool(shape.artifact_filename_for(tool)),
                    tool,
                    msg=f"the artifact filename for {tool!r} must resolve back to it",
                )
        self.assertEqual(
            len(shape.CANONICAL_TOOLS),
            len(set(shape.CANONICAL_TOOLS)),
            msg="the canonical tool identifiers must be distinct",
        )
        self.assertEqual(
            len(shape.ARTIFACT_FILENAMES),
            len(set(shape.ARTIFACT_FILENAMES)),
            msg="the artifact filenames must be distinct",
        )



# --------------------------------------------------------------------------------------
# Required assertion 7 -- an unknown shape halts, and halts by name
# --------------------------------------------------------------------------------------


class UnknownShapeHaltTests(ShapeTestCase):
    """An artifact matching neither SARIF nor a known native shape halts.

    AAP 0.5.4 requires a halt rather than a best-effort parse, and AAP 0.9.2 lists the
    condition among those that stop the run. The requirement is a *named* halt: an
    escaping ``KeyError`` or ``TypeError`` from a parse that was attempted anyway is a
    failure, not a pass. Such an accident carries no reason, names no artifact and
    quotes no observed structure, cannot be told apart from a defect in the normalizer
    itself, and might not raise at all against a slightly different unrecognised
    document. Every assertion below therefore names ``shape.UnknownArtifactShape`` and
    checks the exact type, never ``Exception``.
    """

    def halt_for(self, name: str) -> shape.UnknownArtifactShape:
        """Route fixture *name* under its own path and return the halt it raises."""
        with self.assertRaises(shape.UnknownArtifactShape) as caught:
            shape.route_artifact(self.subject(name), self.load_fixture(name))
        return caught.exception

    def test_the_fall_through_document_raises_the_named_halt(self) -> None:
        """``unknown-shape.json`` raises exactly ``shape.UnknownArtifactShape``."""
        error = self.halt_for(UNKNOWN_SHAPE_FIXTURE)
        self.assertIs(
            type(error),
            shape.UnknownArtifactShape,
            msg=(
                "the halt must be exactly shape.UnknownArtifactShape. Asserting a "
                "supertype would pass on the failure this expectation forbids: an "
                "incidental exception from a parse that was attempted anyway"
            ),
        )
        self.assertIsInstance(error, Exception)

    def test_the_halt_is_neither_a_key_error_nor_a_type_error(self) -> None:
        """The halt is a decision to stop, not an accident that happened to stop.

        The two forbidden types are named explicitly. They are mutually exclusive with
        the halt under ``isinstance`` in both directions, so a test that names the halt
        already excludes them -- and asserting that mutual exclusivity here keeps it true
        of the class hierarchy rather than only of this one raise.
        """
        error = self.halt_for(UNKNOWN_SHAPE_FIXTURE)
        for forbidden in (KeyError, TypeError):
            with self.subTest(forbidden=forbidden.__name__):
                self.assertNotIsInstance(
                    error,
                    forbidden,
                    msg=(
                        f"a {forbidden.__name__} escaping a half-attempted parse is a "
                        "failure: it carries no reason, names no artifact and quotes no "
                        "observed structure"
                    ),
                )
                self.assertFalse(
                    issubclass(forbidden, shape.UnknownArtifactShape),
                    msg=f"{forbidden.__name__} must not satisfy the halt's type",
                )
                self.assertFalse(
                    issubclass(shape.UnknownArtifactShape, forbidden),
                    msg=f"the halt must not be a kind of {forbidden.__name__}",
                )

    def test_both_routing_entry_points_halt(self) -> None:
        """``route`` and ``route_artifact`` both raise, from the shape layer itself.

        A halt synthesised by a caller after the fact would leave the detector's own
        fall-through path untested, so the exception must come from the module that owns
        the decision -- and from both of its entry points, since the run reaches one of
        them per artifact.
        """
        document = self.load_fixture(UNKNOWN_SHAPE_FIXTURE)
        subject = self.subject(UNKNOWN_SHAPE_FIXTURE)
        with self.assertRaises(shape.UnknownArtifactShape) as by_route:
            shape.route(subject, document)
        with self.assertRaises(shape.UnknownArtifactShape) as by_route_artifact:
            shape.route_artifact(subject, document)
        self.assertEqual(by_route.exception.reason, by_route_artifact.exception.reason)
        self.assertIs(type(by_route.exception), shape.UnknownArtifactShape)
        self.assertIs(type(by_route_artifact.exception), shape.UnknownArtifactShape)

    def test_the_halt_names_the_unrecognized_name_reason(self) -> None:
        """The reason is the name reason, and is not the container reason.

        ``shape.py`` names two, and which one is attributed matters: this document's top
        level is a JSON object, which is a supported container, so the halt is
        attributable to a name that resolves to no tool. Both facts are asserted, since a
        halt that reported the wrong reason would send a reader looking for the wrong
        defect.
        """
        error = self.halt_for(UNKNOWN_SHAPE_FIXTURE)
        self.assertEqual(
            error.reason,
            shape.REASON_UNRECOGNIZED_ARTIFACT_NAME,
            msg="the halt must be attributed to the unrecognised artifact name",
        )
        self.assertNotEqual(
            error.reason,
            shape.REASON_UNSUPPORTED_DOCUMENT_TYPE,
            msg=(
                "the container reason needs a top level that is neither a JSON object "
                "nor a JSON array; this document is an object"
            ),
        )

    def test_the_halt_quotes_the_observed_structure(self) -> None:
        """The halt carries everything a report needs to quote, without re-reading.

        Each value is checked against the fixture itself and against the hand-verified
        expected file, so the halt's attributes, the document and the record all agree.
        ``sarif_detected`` is asserted ``False`` because it is what distinguishes this
        failure from the other one the same exception reports -- a document that *is*
        valid SARIF appearing under a name the raw artifact tree must never contain.
        """
        document = self.load_fixture(UNKNOWN_SHAPE_FIXTURE)
        expected = self.load_expected(UNKNOWN_SHAPE_FIXTURE)
        recorded = self.require(expected, "halt", "observed_attributes")
        error = self.halt_for(UNKNOWN_SHAPE_FIXTURE)

        self.assertEqual(error.artifact_path, self.subject(UNKNOWN_SHAPE_FIXTURE))
        self.assertEqual(error.artifact_path, recorded["artifact_path"])
        self.assertEqual(error.stem, UNKNOWN_SHAPE_FIXTURE)
        self.assertEqual(error.stem, recorded["stem"])
        self.assertEqual(error.top_level_type, "object")
        self.assertEqual(error.top_level_type, recorded["top_level_type"])
        self.assertEqual(error.python_type, type(document).__name__)
        self.assertEqual(error.python_type, recorded["python_type"])
        self.assertIsNone(error.version)
        self.assertIsNone(recorded["version"])
        self.assertEqual([str(key) for key in error.top_level_keys], list(document))
        self.assertEqual(
            [str(key) for key in error.top_level_keys], list(recorded["top_level_keys"])
        )
        self.assertEqual(error.top_level_length, len(document))
        self.assertEqual(error.top_level_length, recorded["top_level_length"])
        self.assertIs(
            error.sarif_detected,
            False,
            msg=(
                "this document is not SARIF under any relaxation of the test, so the "
                "valid-SARIF-under-an-unexpected-name case does not apply"
            ),
        )
        self.assertIs(error.sarif_detected, recorded["sarif_detected"])

        details = error.details()
        self.assertEqual(details["reason"], shape.REASON_UNRECOGNIZED_ARTIFACT_NAME)
        self.assertEqual(details["expected_artifacts"], list(shape.ARTIFACT_FILENAMES))
        json.dumps(details)  # the halt record must be serialisable for the run record

    def test_the_near_miss_documents_halt_under_their_own_names(self) -> None:
        """Both near-miss documents halt, each quoting the version it carries.

        They are not artifact names, so neither resolves to a tool and neither reaches an
        adapter. Their expected files record the same outcome, and the version each halt
        quotes is what tells the two apart in a report.
        """
        for name in NEAR_MISS_FIXTURES:
            with self.subTest(fixture=name):
                document = self.load_fixture(name)
                expected = self.load_expected(name)
                error = self.halt_for(name)
                self.assertIs(type(error), shape.UnknownArtifactShape)
                self.assertEqual(error.reason, shape.REASON_UNRECOGNIZED_ARTIFACT_NAME)
                self.assertEqual(
                    error.reason, self.require(expected, "halt", "reason")
                )
                self.assertIs(error.sarif_detected, False)
                self.assertEqual(error.version, document.get(shape.SARIF_VERSION_KEY))
                self.assertEqual(
                    error.version,
                    self.require(expected, "halt", "observed_attributes", "version"),
                    msg=(
                        f"the halt for {name!r} must quote the version the fixture "
                        "carries, which is what distinguishes the pair in a report"
                    ),
                )
                self.assertEqual(
                    self.require(expected, "outcome"),
                    "halt",
                    msg=f"the recorded outcome for {name!r} is a halt",
                )

    def test_a_halting_route_invokes_no_adapter(self) -> None:
        """Nothing under the adapter package is imported by a halting routing call.

        The halt precedes adapter selection entirely: no adapter is named, none is
        imported and none is called, so nothing inside the document is read beyond its
        top level -- no field is mapped, no severity is mapped, no path is resolved and
        no scope predicate is evaluated.
        """
        expected = self.load_expected(UNKNOWN_SHAPE_FIXTURE)
        self.assertIs(
            self.require(expected, "adapter_invocation", "any_adapter_invoked"),
            False,
            msg="the recorded expectation is that no adapter is invoked",
        )
        self.assertIsNone(self.require(expected, "adapter"))

        for name in (UNKNOWN_SHAPE_FIXTURE, *NEAR_MISS_FIXTURES):
            with self.subTest(fixture=name):
                document = self.load_fixture(name)
                subject = self.subject(name)

                def attempt_to_route() -> None:
                    with self.assertRaises(shape.UnknownArtifactShape):
                        shape.route_artifact(subject, document)

                self.assertNoAdapterImportedBy(
                    f"the halting routing call for {name!r}", attempt_to_route
                )

    def test_the_halt_is_not_a_counted_rejection(self) -> None:
        """The outcome is a halt, not one of the normalizer's rejection classes.

        A rejection is a per-record outcome inside an adapter that already owns its
        artifact; these documents never reach an adapter, so there is no record to reject
        and nothing to count under any class. The three expected files state that
        explicitly and require it asserted programmatically over the rejection
        vocabulary rather than by inspection.

        The rejection vocabulary lives in ``normalize.paths``, and it is imported *here*
        rather than at module scope on purpose: this file's module-level import surface is
        the detector alone, which is the same isolation
        :class:`RoutingDecisionTypeTests` pins. A local import keeps that true while
        still asserting the non-membership over the authored tuple.
        """
        from normalize import paths  # local by design; see the docstring above

        self.assertEqual(
            len(paths.REJECT_CLASSES),
            len(set(paths.REJECT_CLASSES)),
            msg="the rejection classes must be a set of distinct names",
        )
        candidates = (
            "halt",
            shape.UnknownArtifactShape.__name__,
            shape.REASON_UNRECOGNIZED_ARTIFACT_NAME,
            shape.REASON_UNSUPPORTED_DOCUMENT_TYPE,
        )
        for candidate in candidates:
            with self.subTest(candidate=candidate):
                self.assertNotIn(
                    candidate,
                    paths.REJECT_CLASSES,
                    msg=(
                        f"{candidate!r} must not be a rejection class: a shape halt "
                        "stops the run, it is not a record counted under a class"
                    ),
                )
                self.assertFalse(
                    paths.is_reject_class(candidate),
                    msg=f"{candidate!r} must not be recognised as a rejection class",
                )
        for name in (UNKNOWN_SHAPE_FIXTURE, *NEAR_MISS_FIXTURES):
            with self.subTest(fixture=name):
                expected = self.load_expected(name)
                self.assertEqual(self.require(expected, "outcome"), "halt")
                self.assertEqual(self.require(expected, "counts", "rejections"), 0)

    def test_the_recorded_near_miss_keys_are_exactly_the_fixtures_keys(self) -> None:
        """The fall-through fixture's account of itself is exhaustive and matches it.

        ``expected/unknown-shape.rows.json`` records one entry per top-level key, each
        with the matcher that would wrongly accept it and a ``must_match`` of ``false``.
        Asserting the key list both ways -- nothing recorded that the fixture lacks,
        nothing in the fixture unaccounted for -- is what keeps that record from drifting
        into a description of a document that no longer exists.
        """
        document = self.load_fixture(UNKNOWN_SHAPE_FIXTURE)
        expected = self.load_expected(UNKNOWN_SHAPE_FIXTURE)
        entries = self.require(expected, "near_miss_keys")
        self.assertIsInstance(entries, list)
        recorded_keys = [entry["key"] for entry in entries]
        self.assertEqual(
            recorded_keys,
            list(document),
            msg=(
                "the recorded near-miss keys must be exactly the fixture's top-level "
                "keys, in document order"
            ),
        )
        for entry in entries:
            with self.subTest(key=entry["key"]):
                self.assertIs(
                    entry["must_match"],
                    False,
                    msg=(
                        f"no top-level key of {UNKNOWN_SHAPE_FIXTURE!r} may cause a "
                        "match: a fuzzy or substring key match finds nothing iterable "
                        "and reports zero rows, which reads as a clean scan"
                    ),
                )



# --------------------------------------------------------------------------------------
# Required assertion 8 -- the shape follows the document, never the filename
# --------------------------------------------------------------------------------------


class ContentNotFilenameTests(ShapeTestCase):
    """The shape decision follows the document; the writer's identity follows the name.

    Both halves are deliberate and neither is a compromise of the other. The *shape* is
    decided by the two conditions and nothing else -- not ``$schema``, not a ``.sarif``
    extension, not ``tool.driver`` -- so a mis-named artifact cannot silently enter the
    wrong reader. The *writer* is taken from the artifact filename, because a native
    document is never fingerprinted to guess which tool produced it (AAP 0.5.4): the
    runner that wrote the file is the only fact that identifies it.

    The asymmetry a reader will meet, and why it is not a compromise of either half: a
    SARIF document reaches the shared adapter whatever name it arrives under, because the
    shape it satisfies is the whole authorisation for that reader. A *native* document
    arriving under a SARIF producer's name reaches no adapter at all -- it halts with
    ``sarif-producer-artifact-not-sarif`` -- because the runner that wrote it writes SARIF
    and nothing else, so there is no native adapter keyed by that runner to route it to,
    and adopting the adapter of whichever tool the content resembles would attribute the
    records to a tool that did not produce them. AAP 0.5.4 requires an artifact matching
    neither the SARIF shape nor its own runner's native shape to halt rather than be
    parsed best-effort, and the same rule closes the case where the *named* native
    signature is the one the content fails.
    """

    def test_a_native_document_under_a_sarif_artifact_name_halts(self) -> None:
        """A ``.sarif``-named artifact carrying a native document halts, adapting nothing.

        The filename claims SARIF and the extension agrees with it; the document does not
        satisfy the conjunction. Neither reading rescues it: handing it to the shared
        adapter would walk a document with no ``runs`` at all and report zero rows, and
        adopting the native adapter the content resembles would file those records under a
        tool that never wrote them. So the shape decision still follows the document -- the
        halt records the observed structure and ``sarif_detected`` false -- and the writer
        still follows the name, which is what the halt is raised about.
        """
        for native_name in NATIVE_ARTIFACT_FIXTURES:
            document = self.load_fixture(native_name)
            for claimed_name in SARIF_ARTIFACT_FIXTURES:
                with self.subTest(document=native_name, claimed=claimed_name):
                    with self.assertRaises(shape.UnknownArtifactShape) as raised:
                        shape.route(claimed_name, document)
                    halt = raised.exception
                    self.assertEqual(
                        halt.reason,
                        shape.REASON_SARIF_PRODUCER_NOT_SARIF,
                        msg=(
                            f"the document from {native_name!r} presented under "
                            f"{claimed_name!r} must halt as a SARIF producer's artifact "
                            "that is not SARIF, rather than reach any adapter"
                        ),
                    )
                    self.assertIn(halt.reason, shape.HALT_REASONS)
                    self.assertIs(
                        halt.sarif_detected,
                        False,
                        msg=(
                            "the shape decision still follows the document: a .sarif "
                            "extension is a naming convention rather than a shape test"
                        ),
                    )
                    self.assertEqual(
                        halt.stem,
                        shape.resolve_tool(claimed_name),
                        msg=(
                            "the halt names the runner whose artifact this is, which is "
                            "the half of the decision that does follow the name"
                        ),
                    )

    def test_a_sarif_document_under_a_native_artifact_name_is_decided_sarif(self) -> None:
        """A ``.json``-named artifact carrying a SARIF document is decided SARIF.

        This is the converse, and it is the sharper of the two: for these names the
        native routing table says something *different* from ``sarif``, so the adapter as
        well as the shape has to follow the content. A router keying the adapter off the
        name would hand a SARIF document to a native parser, which would find none of
        the keys it expects and report zero rows.
        """
        for sarif_name in SARIF_ARTIFACT_FIXTURES:
            document = self.load_fixture(sarif_name)
            for claimed_name in NATIVE_ARTIFACT_FIXTURES:
                with self.subTest(document=sarif_name, claimed=claimed_name):
                    claimed_tool = shape.resolve_tool(claimed_name)
                    table_adapter = shape.adapter_module_for(claimed_tool)
                    self.assertNotEqual(
                        table_adapter,
                        shape.SHARED_SARIF_ADAPTER,
                        msg=(
                            f"{claimed_name!r} must be a name whose table entry differs "
                            "from the shared adapter, or this test asserts nothing"
                        ),
                    )
                    decision = shape.route(claimed_name, document)
                    self.assertEqual(
                        decision.shape,
                        shape.SHAPE_SARIF,
                        msg=(
                            f"the document from {sarif_name!r} presented under "
                            f"{claimed_name!r} must be decided {shape.SHAPE_SARIF!r}"
                        ),
                    )
                    self.assertEqual(
                        decision.adapter,
                        shape.SHARED_SARIF_ADAPTER,
                        msg=(
                            f"a SARIF document presented under {claimed_name!r} must "
                            f"reach the shared SARIF adapter rather than "
                            f"{table_adapter!r}: the content decides"
                        ),
                    )
                    self.assertEqual(
                        decision.tool,
                        claimed_tool,
                        msg="the writing runner is still identified by the name",
                    )

    def test_a_near_miss_document_under_a_sarif_artifact_name_stays_native(self) -> None:
        """A near-miss document under a real producer name is never decided SARIF.

        Each fixture satisfies exactly one half of the conjunction. Presented under a
        recognised SARIF producer's name it halts as that producer's artifact not being
        SARIF -- so the assertion this test exists for is that ``sarif_detected`` is false
        on the halt: a detector that relaxed the conjunction to one half would flip that
        flag and route the document to the shared adapter, and nothing else in the module
        would notice.
        """
        for name in NEAR_MISS_FIXTURES:
            document = self.load_fixture(name)
            for claimed_name in SARIF_ARTIFACT_FIXTURES:
                with self.subTest(document=name, claimed=claimed_name):
                    with self.assertRaises(shape.UnknownArtifactShape) as raised:
                        shape.route(claimed_name, document)
                    halt = raised.exception
                    self.assertEqual(
                        halt.reason,
                        shape.REASON_SARIF_PRODUCER_NOT_SARIF,
                        msg=(
                            f"the near-miss document from {name!r} must never reach the "
                            f"shared adapter under {claimed_name!r}"
                        ),
                    )
                    self.assertIs(
                        halt.sarif_detected,
                        False,
                        msg=(
                            f"the near-miss document from {name!r} satisfies one half of "
                            "the conjunction and must still not be detected as SARIF, "
                            "under any name"
                        ),
                    )

    def test_a_native_document_under_another_native_name_keeps_the_named_adapter(self) -> None:
        """A native document routes by the name that wrote it, not by its content.

        The complement of the two tests above, and the reason routing is keyed by the
        writer at all: native shapes are not distinguishable from one another by a safe
        test, so guessing a tool from content would be a fingerprint rather than a fact.
        The named artifact's own signature is therefore what governs -- a document that
        satisfies it reaches the adapter its *name* claims, and one that does not halts on
        that signature rather than being re-keyed to whichever tool it resembles. The raw
        artifact tree is what makes the naming safe: it holds one artifact per tool that
        writes one and nothing else ever.
        """
        first, second = NATIVE_ARTIFACT_FIXTURES[0], NATIVE_ARTIFACT_FIXTURES[1]
        document = self.load_fixture(first)
        expected = shape.native_signature_for(shape.resolve_tool(second))
        if shape.matches_native_signature(shape.resolve_tool(second), document):
            decision = shape.route(second, document)
            self.assertEqual(decision.shape, shape.SHAPE_NATIVE)
            self.assertEqual(
                decision.adapter,
                shape.adapter_module_for(shape.resolve_tool(second)),
                msg=(
                    "a native document routes to the adapter its artifact name names; "
                    "content never selects among the native adapters"
                ),
            )
            self.assertNotEqual(
                decision.adapter,
                shape.SHARED_SARIF_ADAPTER,
                msg="and it still never reaches the SARIF adapter",
            )
            return
        with self.assertRaises(shape.UnknownArtifactShape) as raised:
            shape.route(second, document)
        halt = raised.exception
        self.assertEqual(
            halt.reason,
            shape.REASON_NATIVE_SHAPE_UNRECOGNIZED,
            msg=(
                f"the document from {first!r} does not satisfy {second!r}'s native "
                "signature, so it halts on that signature rather than being routed by "
                "resemblance to the tool that did write it"
            ),
        )
        self.assertEqual(halt.signature_tool, expected.tool)
        self.assertTrue(
            halt.expected_signature,
            msg="the halt quotes what the named artifact's shape must be",
        )
        self.assertIs(halt.sarif_detected, False)


# --------------------------------------------------------------------------------------
# The composer's half of routing -- harness/lib/normalize/cli.py
#
# shape.py names an adapter by string key and imports none; cli.py holds the registry
# that inverts that direction, and it is the only module that turns a key into a callable
# (AAP 0.6.4). So the routing contract is only half asserted by the classes above: the
# key is decided there, and *which module answers to that key* is decided here. The
# classes below assert that second half, together with the command-line surface that
# feeds it -- the parser, the input resolution, the interpreter record, the vocabulary
# gate the composition settles first, and the raw-tree boundary that decides which
# artifacts are routed at all.
#
# Three properties hold across every class below and are decisions rather than habits:
#
# * **In process, never as a subprocess.** Every call is a direct call on ``cli``, so a
#   halt is asserted as the exception object it is -- its class, its ``reason`` from the
#   closed ``cli.HALT_REASONS`` set, its ``exit_code`` and its serialisable details --
#   rather than as an exit status with a message to be pattern-matched. No scanner is
#   invoked, no graph is read and no Spark source is executed.
# * **Every path is inside a TemporaryDirectory.** Nothing here writes into the
#   repository: not the dataset, not the run record, not a fixture. Fixtures are copied
#   *out* of ``fixtures/`` into a temporary raw tree and never the reverse.
# * **A capability fake stands in only for what the authored modules cannot exhibit.**
#   A vocabulary disagreement and an adapter module missing its entry point are
#   programming faults, so the only way to reach those branches is to patch the
#   authored value for the duration of one assertion. Everything else is driven with
#   real documents and real modules.
# --------------------------------------------------------------------------------------


#: The eight command-line options ``cli.build_parser`` declares, paired with the
#: namespace attribute each one sets and the metavar it advertises. Written out here
#: rather than read from the parser: reading the parser's own declaration and comparing
#: it with itself would assert nothing, and this table is what the module docstring's
#: CLI contract promises a caller.
CLI_OPTIONS = (
    ("--raw-dir", "raw_dir", "DIR"),
    ("--runner-metadata", "runner_metadata", "FILE"),
    ("--allowlist", "allowlist", "FILE"),
    ("--log-dir", "log_dir", "DIR"),
    ("--spark-src", "spark_src", "DIR"),
    ("--findings-json", "findings_json", "FILE"),
    ("--findings-csv", "findings_csv", "FILE"),
    ("--run-record", "run_record", "FILE"),
)

#: The relative locations the documented defaults are built from, restated
#: independently of the module constants that carry them. Each is asserted against the
#: constant as well as used to derive an expected path, so neither can drift alone.
DEFAULT_METADATA_FILENAME = "runner-metadata.json"
DEFAULT_RUN_RECORD_FILENAME = "normalize-run.json"
DEFAULT_FINDINGS_JSON_RELATIVE = "oss-scan-results/findings.json"
DEFAULT_FINDINGS_CSV_RELATIVE = "oss-scan-results/findings.csv"

#: The conditional adapter AAP 0.6.1 creates *"if and only if OSV-Scanner writes an
#: artifact"*. Its source file is expected absent this run, which is what makes the
#: guarded import in ``cli.resolve_adapter`` a live branch rather than dead code.
CONDITIONAL_ADAPTER_SOURCE = "harness/lib/normalize/adapters/osv_scanner.py"


class NativeSignatureTableTests(ShapeTestCase):
    """The per-writer signature table itself, before any document is held to it.

    A signature table is the kind of authored data that fails quietly: a writer missing
    from it is a writer whose malformed artifact still routes, and a signature stricter
    than its writer's real document is a halt on a clean scan. Neither shows up in a test
    that only feeds documents through :func:`shape.route`, so the table's *shape* is
    asserted here and its *effect* is asserted in
    :class:`MalformedKnownArtifactHaltTests`.

    Nothing here duplicates the tables: every expectation is derived from
    ``shape.CANONICAL_TOOLS`` and ``shape.SARIF_PRODUCERS``, which have one authority in
    ``harness/lib/normalize/shape.py``.
    """

    def test_exactly_the_non_sarif_producers_carry_a_signature(self) -> None:
        """The six signature-bearing writers are exactly the six that write natively.

        Derived both ways rather than compared against a literal list, so the assertion
        survives a change to the canonical inventory and fails only when the partition
        itself is wrong.
        """
        expected_with = tuple(
            tool for tool in shape.CANONICAL_TOOLS if tool not in shape.SARIF_PRODUCERS
        )
        self.assertEqual(
            shape.NATIVE_SIGNATURE_TOOLS,
            expected_with,
            msg=(
                "every writer that emits its own native document must carry a "
                "signature: a writer without one is a writer whose malformed artifact "
                "reaches an adapter that reads zero records from it and reports success"
            ),
        )
        self.assertEqual(
            shape.TOOLS_WITHOUT_A_NATIVE_SIGNATURE,
            tuple(tool for tool in shape.CANONICAL_TOOLS if tool in shape.SARIF_PRODUCERS),
            msg=(
                "the three SARIF producers must carry none: SARIF is their only "
                "legitimate shape, so a native signature for them would either "
                "duplicate the conjunction or invent a shape they never write"
            ),
        )

    def test_the_two_partitions_are_disjoint_and_exhaustive(self) -> None:
        """Every canonical tool sits in exactly one of the two partitions."""
        with_signature = set(shape.NATIVE_SIGNATURE_TOOLS)
        without_signature = set(shape.TOOLS_WITHOUT_A_NATIVE_SIGNATURE)
        self.assertEqual(
            with_signature & without_signature,
            set(),
            msg="a tool cannot both carry and not carry a native signature",
        )
        self.assertEqual(
            with_signature | without_signature,
            set(shape.CANONICAL_TOOLS),
            msg=(
                "the two partitions must cover all nine canonical tools; a tool in "
                "neither is a tool no signature decision is ever made about"
            ),
        )
        self.assertEqual(len(shape.NATIVE_SIGNATURES), len(with_signature))

    def test_every_signature_names_its_own_writer_and_accepts_a_container(self) -> None:
        """Each table entry is keyed by the tool it names and accepts real containers.

        The key and the entry's own ``tool`` field are asserted equal because the halt
        report quotes the entry's field: a mis-keyed row would name the wrong writer in
        the one message a reader has to work from.
        """
        for tool, signature in shape.NATIVE_SIGNATURES.items():
            with self.subTest(tool=tool):
                self.assertEqual(signature.tool, tool)
                self.assertIn(tool, shape.CANONICAL_TOOLS)
                self.assertNotIn(tool, shape.SARIF_PRODUCERS)
                self.assertTrue(
                    signature.statement.strip(),
                    msg=(
                        "the statement is quoted verbatim into the halt message and "
                        "into tool-status.md, so it cannot be empty"
                    ),
                )
                self.assertTrue(
                    signature.accepts_object or signature.accepts_array,
                    msg="a signature accepting neither container accepts nothing at all",
                )
                for accepted in signature.top_level:
                    self.assertIn(accepted, (shape.JSON_TYPE_OBJECT, shape.JSON_TYPE_ARRAY))
                self.assertIs(
                    shape.native_signature_for(tool),
                    signature,
                    msg="the lookup must return the authored entry, not a copy of it",
                )

    def test_every_signature_accepts_its_writers_empty_finding_set(self) -> None:
        """A document carrying no finding at all is legitimate for every writer.

        This is the direction a signature gets wrong in the expensive way. A halt on an
        empty finding set would stop a clean scan -- Gitleaks writes ``[]``,
        Dependency-Check writes an empty ``dependencies`` array over a scope with no
        manifest, and a Joern query set that matched nothing writes an empty ``findings``
        array -- so each of those documents is asserted to satisfy its signature and to
        route, with the reason it is legitimate recorded alongside it.
        """
        for tool, document, why in LEGITIMATE_EMPTY_DOCUMENTS:
            with self.subTest(tool=tool, why=why):
                self.assertTrue(
                    shape.matches_native_signature(tool, document),
                    msg=(
                        f"{tool}'s signature must accept {why}: a signature that "
                        "rejected an empty finding set would halt every clean scan"
                    ),
                )
                evidence = shape.native_signature_evidence(tool, document)
                self.assertIs(evidence["signature_required"], True)
                self.assertIs(evidence["matches"], True)
                decision = shape.route(shape.artifact_filename_for(tool), document)
                self.assertEqual(decision.shape, shape.SHAPE_NATIVE)
                self.assertEqual(decision.tool, tool)
                self.assertEqual(
                    decision.adapter,
                    shape.adapter_module_for(tool),
                    msg=(
                        f"an empty but legitimate {tool} document must reach {tool}'s "
                        "own adapter, which is where an empty finding set is a count of "
                        "zero rather than a shape failure"
                    ),
                )

    def test_a_sarif_producer_has_no_signature_and_a_stranger_raises(self) -> None:
        """``None`` is a real answer for a producer; an unknown name is an error.

        The two are deliberately different outcomes. ``None`` means "no native shape of
        this writer's exists to test", which :func:`shape.route` treats as "route on";
        an identifier outside the nine is a programming error in the caller and raises,
        exactly as the other lookups over the authored tables do.
        """
        for tool in shape.TOOLS_WITHOUT_A_NATIVE_SIGNATURE:
            with self.subTest(tool=tool):
                self.assertIsNone(shape.native_signature_for(tool))
                self.assertTrue(
                    shape.matches_native_signature(tool, {"anything": 1}),
                    msg=(
                        "a writer with no native signature cannot fail one; returning "
                        "False here would halt a producer's artifact for failing a test "
                        "that does not exist"
                    ),
                )
                evidence = shape.native_signature_evidence(tool, {"anything": 1})
                self.assertIs(evidence["signature_required"], False)
                self.assertIs(evidence["matches"], True)
                self.assertEqual(evidence["tool"], tool)
        for stranger in ("codeql", "", "checkov.json.bak"):
            with self.subTest(stranger=stranger):
                with self.assertRaises(ValueError):
                    shape.native_signature_for(stranger)

    def test_the_halt_reason_inventory_is_closed_and_distinct(self) -> None:
        """``HALT_REASONS`` is the closed set, and the signature reason is in it.

        The inventory is published as data so a halt record, ``tool-status.md`` and the
        validation criteria consume one authored list. A reason absent from it would be
        a reason those three consumers cannot account for.

        Four reasons, in the order ``route()`` tests them. The fourth,
        ``sarif-producer-artifact-not-sarif``, is the SARIF-only producer rule: a runner
        that writes SARIF and nothing else has no native shape to fall back to, so its
        artifact failing the conjunction is its own condition rather than a native
        signature miss. It sits between the container check and the native signature
        because a producer is decided by its name before any signature is consulted.

        ``REASON_NATIVE_SIGNATURE_MISMATCH`` and ``REASON_NATIVE_SHAPE_UNRECOGNIZED`` are
        deliberately ONE literal here, which is why the tuple has four members rather than
        five: a recognised artifact name whose document does not carry that writer's
        structure is one condition however it is detected -- by the envelope the tool's
        reader requires or by the named signature table -- and a record naming two literals
        for it would let a consumer enumerate a closed set the router can step outside of.
        The identity is asserted below rather than left to the reader.
        """
        self.assertEqual(
            shape.HALT_REASONS,
            (
                shape.REASON_UNRECOGNIZED_ARTIFACT_NAME,
                shape.REASON_UNSUPPORTED_DOCUMENT_TYPE,
                shape.REASON_SARIF_PRODUCER_NOT_SARIF,
                shape.REASON_NATIVE_SIGNATURE_MISMATCH,
            ),
            msg="the four reasons, in the order route() tests them",
        )
        self.assertEqual(
            shape.REASON_NATIVE_SIGNATURE_MISMATCH,
            shape.REASON_NATIVE_SHAPE_UNRECOGNIZED,
            msg="one condition, one literal, whichever layer detected it",
        )
        self.assertEqual(
            len(shape.HALT_REASONS),
            len(set(shape.HALT_REASONS)),
            msg="two reasons sharing a literal would be indistinguishable in a record",
        )
        for reason in shape.HALT_REASONS:
            with self.subTest(reason=reason):
                self.assertIsInstance(reason, str)
                self.assertTrue(reason.strip())

    def test_the_signature_surface_is_exported(self) -> None:
        """Every name the signature surface publishes is in ``__all__``, the reason included.

        ``cli.py`` catches the halt generically and forwards ``details()``, so
        ``REASON_NATIVE_SIGNATURE_MISMATCH`` travels into
        ``harness/artifacts/logs/normalize-run.json`` without the CLI naming it. That is
        precisely why the export list is asserted here: nothing else in the pipeline would
        fail if the constant were unexported, and a reader of the module would have no way
        to name the condition.
        """
        exported = set(shape.__all__)
        self.assertEqual(
            len(shape.__all__),
            len(exported),
            msg="a duplicated export is an authoring slip in the one list consumers read",
        )
        for name in (
            "NATIVE_SIGNATURES",
            "NATIVE_SIGNATURE_TOOLS",
            "TOOLS_WITHOUT_A_NATIVE_SIGNATURE",
            "NativeSignature",
            "native_signature_for",
            "matches_native_signature",
            "native_signature_evidence",
            "REASON_NATIVE_SIGNATURE_MISMATCH",
            "HALT_REASONS",
            "JSON_TYPE_OBJECT",
            "JSON_TYPE_ARRAY",
        ):
            with self.subTest(name=name):
                self.assertIn(name, exported)
                self.assertTrue(hasattr(shape, name))


class MalformedKnownArtifactHaltTests(ShapeTestCase):
    """A recognised artifact name does not license a document of any shape.

    Taking a recognised name for evidence of a recognised structure is the same silent
    failure the rest of this file guards, one layer in. A router keying on the filename
    alone hands ``checkov.json`` carrying ``{"results": "no findings"}`` to the Checkov
    adapter, which finds no ``failed_checks`` to walk, emits zero rows and zero
    rejections and reports success. The independent counting traversal in
    ``reconcile.py`` returns zero over the same document, so the reconciliation identity
    balances at ``0 = 0 + 0`` and parse status reads ``clean`` -- a malformed artifact
    indistinguishable from a scan that found nothing, which is exactly what AAP 0.5.4
    forbids and AAP 0.9.2 lists among the conditions that stop the run.

    Each assertion below is checked against the fixture *and* against that fixture's
    hand-verified expected file, so the two authored documents cannot drift apart
    silently. The counting traversal's zero is asserted rather than described: it is the
    measurement that makes the halt necessary, and a reader should be able to see it
    rather than take the reasoning on trust.
    """

    def entries(self) -> tuple[tuple[str, str, str], ...]:
        """Return the malformed fixtures, failing when none was discovered."""
        if not MALFORMED_KNOWN_FIXTURES:
            self.fail(
                "blocking gap: no malformed-*.json fixture is present in "
                f"{FIXTURES_DIR.relative_to(REPO_ROOT).as_posix()}. Every assertion in "
                "this class would pass over an empty loop, which is the silent green a "
                "negative test must never report."
            )
        return MALFORMED_KNOWN_FIXTURES

    def halt_for_malformed(self, subject: str, document: object) -> shape.UnknownArtifactShape:
        """Route *document* under *subject* and return the halt it must raise."""
        with self.assertRaises(shape.UnknownArtifactShape) as caught:
            shape.route(subject, document)
        return caught.exception

    def test_one_signature_bearing_writer_has_no_malformed_fixture(self) -> None:
        """Five of the six carry one, and the sixth is ``osv-scanner``.

        Asserted so the loop's length is a stated expectation rather than whatever the
        folder happens to hold. OSV-Scanner writes no artifact at all over a scope with
        no resolvable package source (AAP 0.5.4), so there is no document of its to
        malform -- and its signature is still asserted, over the legitimate empty
        document, in :class:`NativeSignatureTableTests`.
        """
        covered = {tool for _, tool, _ in self.entries()}
        missing = [tool for tool in shape.NATIVE_SIGNATURE_TOOLS if tool not in covered]
        self.assertEqual(
            missing,
            [TOOL_WITHOUT_A_FIXTURE],
            msg=(
                "exactly one signature-bearing writer may lack a malformed fixture, and "
                f"it must be {TOOL_WITHOUT_A_FIXTURE!r}: any other absence is a "
                "rejection path nobody has exercised"
            ),
        )
        self.assertEqual(len(self.entries()), len(shape.NATIVE_SIGNATURE_TOOLS) - 1)

    def test_each_malformed_fixture_is_routed_under_the_writers_artifact_name(self) -> None:
        """The fixture's own name resolves to no tool; the artifact name resolves to one.

        This is the assertion that keeps every halt below attributable. Routed under its
        own filename each fixture would halt for the *name* reason -- a different
        condition, owned by ``expected/unknown-shape.rows.json`` -- and the signature
        assertions would pass over the wrong failure. The ``dependency-check`` case is
        the sharp one: its fixture name *ends* in the artifact stem, so a resolver
        matching a suffix rather than the whole stem would resolve it.
        """
        for fixture_name, tool, artifact_name in self.entries():
            with self.subTest(fixture=fixture_name):
                self.assertIsNone(
                    shape.resolve_tool(fixture_name),
                    msg=(
                        f"{fixture_name!r} must resolve to no tool, so the subject this "
                        "class routes under is the artifact filename and not the fixture "
                        "name"
                    ),
                )
                self.assertEqual(shape.resolve_tool(artifact_name), tool)
                expected = self.load_expected(fixture_name)
                self.assertEqual(self.require(expected, "tool"), tool)
                self.assertEqual(self.require(expected, "routed_as", "subject"), artifact_name)
                self.assertIs(
                    self.require(expected, "routed_as", "fixture_name_resolves_to_a_tool"),
                    False,
                )

    def test_every_malformed_document_halts_with_the_signature_reason(self) -> None:
        """Each halts, by exact type, for the signature reason, from both entry points.

        The exact type is asserted with ``assertIs`` rather than ``assertRaises`` alone:
        a ``KeyError`` or ``TypeError`` escaping a parse that was attempted anyway also
        stops the run, but it carries no reason, names no writer and quotes no expected
        shape, so it cannot be told apart from a defect in the normalizer itself.
        """
        for fixture_name, tool, artifact_name in self.entries():
            with self.subTest(fixture=fixture_name):
                document = self.load_fixture(fixture_name)
                expected = self.load_expected(fixture_name)
                error = self.halt_for_malformed(artifact_name, document)

                self.assertIs(
                    type(error),
                    shape.UnknownArtifactShape,
                    msg=(
                        "the halt must be exactly shape.UnknownArtifactShape; asserting "
                        "a supertype would pass on an incidental exception"
                    ),
                )
                for forbidden in (KeyError, TypeError, AttributeError):
                    self.assertNotIsInstance(error, forbidden)
                self.assertEqual(
                    error.reason,
                    shape.REASON_NATIVE_SIGNATURE_MISMATCH,
                    msg=(
                        f"the halt for {artifact_name!r} must be attributed to the "
                        "document not being this writer's native shape"
                    ),
                )
                self.assertEqual(error.reason, self.require(expected, "halt", "reason"))
                self.assertIn(error.reason, shape.HALT_REASONS)
                self.assertNotEqual(error.reason, shape.REASON_UNRECOGNIZED_ARTIFACT_NAME)
                self.assertNotEqual(error.reason, shape.REASON_UNSUPPORTED_DOCUMENT_TYPE)
                self.assertIs(error.sarif_detected, False)
                self.assertIs(shape.is_sarif(document), False)
                self.assertFalse(shape.matches_native_signature(tool, document))

                # Both entry points reach the same decision: a run calls route_artifact
                # over an entry in harness/artifacts/raw/, and only the filename
                # component of that path is read -- nothing is opened here.
                raw_subject = f"harness/artifacts/raw/{artifact_name}"
                with self.assertRaises(shape.UnknownArtifactShape) as by_artifact:
                    shape.route_artifact(raw_subject, document)
                self.assertEqual(by_artifact.exception.reason, error.reason)
                self.assertIs(type(by_artifact.exception), shape.UnknownArtifactShape)
                self.assertEqual(
                    self.require(expected, "routed_as", "second_subject"), raw_subject
                )

    def test_the_halt_quotes_the_writer_the_signature_and_the_observation(self) -> None:
        """The halt is diagnosable without the file it halted on.

        Three things have to travel with it: which writer's shape was expected, what
        that shape is in words, and what was observed where the signature required a
        container. The statement is asserted *equal to the table's own* string rather
        than to a copy written here, so the message cannot describe a requirement the
        implementation does not make.
        """
        for fixture_name, tool, artifact_name in self.entries():
            with self.subTest(fixture=fixture_name):
                document = self.load_fixture(fixture_name)
                expected = self.load_expected(fixture_name)
                signature = shape.NATIVE_SIGNATURES[tool]
                error = self.halt_for_malformed(artifact_name, document)

                self.assertEqual(error.signature_tool, tool)
                self.assertEqual(error.expected_signature, signature.statement)
                self.assertEqual(
                    error.expected_signature,
                    self.require(expected, "native_signature", "expected"),
                    msg=(
                        "the expected file records the signature verbatim; a difference "
                        "here means one of the two authored documents has drifted"
                    ),
                )
                self.assertEqual(error.signature_observation, signature.observe(document))

                observation = error.signature_observation
                self.assertIs(observation["matches"], False)
                self.assertEqual(
                    list(observation["accepted_top_level_types"]),
                    self.require(expected, "native_signature", "accepted_top_level_types"),
                )
                recorded_signature = self.require(expected, "native_signature")
                for field in (
                    "required_key",
                    "required_key_type",
                    "required_key_present",
                    "observed_key_type",
                ):
                    self.assertEqual(
                        observation.get(field),
                        recorded_signature[field],
                        msg=(
                            f"the observation's {field} must equal the value the "
                            "expected file records; gitleaks records null for all "
                            "three key fields, because its signature is the top-level "
                            "type alone"
                        ),
                    )

                message = str(error)
                self.assertIn(tool, message)
                self.assertIn(signature.statement, message)
                self.assertIn("observed", message)
                observed_token = observation.get("observed_key_type") or observation[
                    "observed_top_level_type"
                ]
                self.assertIn(
                    str(observed_token),
                    message,
                    msg=(
                        "the message must name what was observed where the signature "
                        "required a container, or a reader cannot tell an absent key "
                        "from a key of the wrong type"
                    ),
                )

                details = error.details()
                self.assertEqual(details["reason"], shape.REASON_NATIVE_SIGNATURE_MISMATCH)
                self.assertEqual(details["signature_tool"], tool)
                self.assertEqual(details["expected_signature"], signature.statement)
                self.assertEqual(details["signature_observation"], observation)
                self.assertEqual(details["expected_artifacts"], list(shape.ARTIFACT_FILENAMES))
                json.dumps(details)  # the halt record must serialise for the run record

    def test_the_recorded_fixture_facts_match_the_fixtures(self) -> None:
        """Each expected file's account of its fixture is checked against the fixture.

        Digest, byte size, top-level type, length and key order. An expected file that
        described a document no longer on disk would keep every assertion above passing
        while documenting something else entirely.
        """
        import hashlib  # local: the module surface stays the detector alone

        for fixture_name, tool, artifact_name in self.entries():
            with self.subTest(fixture=fixture_name):
                path = self.fixture_path(fixture_name)
                raw = path.read_bytes()
                document = self.load_fixture(fixture_name)
                expected = self.load_expected(fixture_name)
                recorded = self.require(expected, "fixture")

                self.assertEqual(
                    recorded["path"], path.relative_to(REPO_ROOT).as_posix()
                )
                self.assertEqual(recorded["sha256"], hashlib.sha256(raw).hexdigest())
                self.assertEqual(recorded["bytes"], len(raw))
                self.assertIs(recorded["json_well_formed"], True)
                self.assertEqual(recorded["python_type"], type(document).__name__)
                self.assertEqual(recorded["top_level_length"], len(document))
                self.assertEqual(list(recorded["top_level_keys"]), list(document))
                self.assertEqual(self.require(expected, "outcome"), "halt")
                self.assertIs(self.require(expected, "adapter_invoked"), False)
                self.assertIsNone(self.require(expected, "adapter"))
                self.assertIsNone(self.require(expected, "scanner_class"))
                self.assertEqual(
                    self.require(expected, "field_order"),
                    list(self.emit_field_order()),
                    msg=(
                        "the recorded field order is the dataset's twelve fields in "
                        "emit.py's order, carried for parity with the positive expected "
                        "files in this folder"
                    ),
                )
                self.assertEqual(
                    self.require(expected, "halt", "observed_attributes", "stem"),
                    tool,
                    msg=(
                        "for this reason the stem carries the canonical tool "
                        "identifier: the writer is known by the time the signature is "
                        "tested"
                    ),
                )
                self.assertEqual(
                    self.require(expected, "halt", "observed_attributes", "artifact_path"),
                    artifact_name,
                )

    def emit_field_order(self) -> tuple[str, ...]:
        """Return the dataset's field order from its one authority, ``emit.FIELDS``."""
        from normalize import emit  # local by design; see the class docstring

        return tuple(emit.FIELDS)

    def test_the_counting_traversal_would_have_returned_zero(self) -> None:
        """The false clean this halt prevents, measured rather than described.

        ``reconcile.py``'s traversal is deliberately tolerant: a missing or wrong-typed
        container counts as zero and never raises, because its whole value is that it
        shares no code and no judgement with the row builder. That tolerance is correct
        and is left untouched -- and it is exactly why the halt has to live one layer up,
        in the module that reads the document rather than the one that counts it.
        """
        from normalize import reconcile  # local by design; see the class docstring

        for fixture_name, tool, _ in self.entries():
            with self.subTest(fixture=fixture_name):
                document = self.load_fixture(fixture_name)
                expected = self.load_expected(fixture_name)
                counted = reconcile.count_records(tool, document)
                self.assertEqual(
                    counted,
                    0,
                    msg=(
                        f"the independent traversal counts zero records in "
                        f"{fixture_name!r}; with a permissive router the adapter would "
                        "emit zero rows and zero rejections, the identity would balance "
                        "at 0 = 0 + 0 and parse status would read clean"
                    ),
                )
                self.assertEqual(
                    counted,
                    self.require(expected, "counting_traversal", "would_return"),
                )
                self.assertEqual(self.require(expected, "counts", "rows"), 0)
                self.assertEqual(self.require(expected, "rows"), [])

    def test_a_halting_route_over_a_malformed_document_invokes_no_adapter(self) -> None:
        """The halt precedes adapter selection, so no adapter module is imported.

        Nothing inside the document is read beyond its top level and the one container
        the signature names: no field is mapped, no severity is mapped, no path is
        resolved and no scope predicate is evaluated.
        """
        for fixture_name, _, artifact_name in self.entries():
            with self.subTest(fixture=fixture_name):
                document = self.load_fixture(fixture_name)

                def attempt_to_route() -> None:
                    with self.assertRaises(shape.UnknownArtifactShape):
                        shape.route_artifact(
                            f"harness/artifacts/raw/{artifact_name}", document
                        )

                self.assertNoAdapterImportedBy(
                    f"the halting routing call for {fixture_name!r}", attempt_to_route
                )

    def test_the_signature_halt_is_not_a_counted_rejection(self) -> None:
        """A shape halt stops the run; it is not a record counted under a class.

        A rejection is a per-record outcome inside an adapter that already owns its
        artifact, and these documents never reach one. The signature reason is therefore
        asserted a non-member of ``paths.REJECT_CLASSES`` programmatically, over the
        authored tuple, rather than by inspection.
        """
        from normalize import paths  # local by design; see the class docstring

        for candidate in ("halt", shape.REASON_NATIVE_SIGNATURE_MISMATCH):
            with self.subTest(candidate=candidate):
                self.assertNotIn(candidate, paths.REJECT_CLASSES)
                self.assertFalse(paths.is_reject_class(candidate))
        for fixture_name, _, _ in self.entries():
            with self.subTest(fixture=fixture_name):
                expected = self.load_expected(fixture_name)
                self.assertEqual(self.require(expected, "counts", "rejections"), 0)
                self.assertEqual(self.require(expected, "counts", "rejections_by_class"), {})
                self.assertIs(
                    self.require(expected, "halt", "not_a_rejection", "is_reject_class"),
                    False,
                )
                self.assertIsNone(
                    self.require(expected, "halt", "not_a_rejection", "reject_class")
                )

    def test_a_malformed_native_document_under_a_producer_name_halts_as_a_producer(
        self,
    ) -> None:
        """Under a producer's name the halt is the PRODUCER reason, not the signature one.

        The other half of the attribution this class exists for, and the direction where
        two independently correct rules meet. A SARIF-only producer's artifact must satisfy
        the SARIF conjunction exactly, and one of these documents cannot: so the halt is
        real, and it is attributable to the *claimed writer* rather than to a native
        signature the three producers do not have. Both facts are asserted, because a
        report that named ``native-shape-unrecognized`` here would send a reader looking
        for a broken Checkov envelope in a file Opengrep was supposed to have written.

        ``native_signature_for`` returning ``None`` for each producer is asserted in the
        same breath, so the reason is shown to be the producer rule rather than a
        signature that happened to match.
        """
        for fixture_name, _, _ in self.entries():
            document = self.load_fixture(fixture_name)
            for producer_name in SARIF_ARTIFACT_FIXTURES:
                with self.subTest(fixture=fixture_name, claimed=producer_name):
                    producer = shape.resolve_tool(producer_name)
                    self.assertIsNone(shape.native_signature_for(producer))
                    with self.assertRaises(shape.UnknownArtifactShape) as caught:
                        shape.route(producer_name, document)
                    halt = caught.exception
                    self.assertEqual(
                        halt.reason,
                        shape.REASON_SARIF_PRODUCER_NOT_SARIF,
                        msg="attributed to the claimed producer, not to a native signature",
                    )
                    self.assertIn(halt.reason, shape.HALT_REASONS)
                    self.assertNotEqual(
                        halt.reason, shape.REASON_NATIVE_SIGNATURE_MISMATCH
                    )
                    self.assertEqual(halt.stem, producer)
                    self.assertIs(halt.sarif_detected, False)
                    self.assertIsNone(halt.signature_tool)



class RecognisedNameEnvelopeTests(ShapeTestCase):
    """A recognised artifact filename never vouches for the bytes under it.

    AAP 0.5.4 states both halves of the rule in one sentence: a document that is not
    SARIF routes to "the native adapter keyed by the runner that wrote it", and an
    artifact matching neither the SARIF shape nor a **known native shape** is a halt
    rather than a best-effort parse. So the name selects the reader and the bytes must
    still be that reader's shape. Two documents a name alone would have admitted are
    refused here: an artifact under one of the three SARIF producers' names that fails
    the conjunction, and an artifact under any recognised name that carries neither the
    conjunction nor the container the per-shape table names for that tool.

    The check is the envelope and only the envelope -- the container is present and is
    the right JSON type -- which is what keeps the strictness from becoming its own
    defect. An empty report is an ordinary outcome, so
    :meth:`test_an_empty_report_from_every_native_tool_still_routes` is as load-bearing
    as any halt below: a router that refused an empty artifact would stop the run over a
    tool that ran correctly and found nothing, and that is the same class of wrong answer
    in the opposite direction.

    Every judgement about what a container *holds* stays with the adapter that walks it,
    including the Trivy non-empty-unsupported-section halt. This class asserts over
    envelopes; it never asserts over records.
    """

    #: The minimal document that satisfies each native tool's envelope and nothing more.
    #: Authored inline rather than committed as a fixture, deliberately: a fixture is a
    #: captured or authored *artifact*, and these are one-line probes of the predicate
    #: table, each carrying the container AAP 0.5.4 names for its tool with nothing
    #: inside it. Checkov appears twice because its artifact legitimately takes either
    #: top-level form. Trivy appears once, with ``"Results": []``: an empty array is the
    #: complete report of a scan that resolved no target, while a ``Results`` that is
    #: present as ``null`` is not this writer's shape at all and is refused by the
    #: envelope predicate and the declared signature alike -- see
    #: :data:`NEAR_TRIVY_RESULTS_NULL_FIXTURE` and
    #: :meth:`test_a_trivy_report_whose_results_is_null_is_refused_by_one_contract`.
    MINIMAL_ENVELOPES: tuple[tuple[str, object], ...] = (
        ("gitleaks", []),
        ("checkov", {"check_type": "kubernetes", "results": {}}),
        ("checkov", [{"check_type": "kubernetes", "results": {}}]),
        ("trivy", {"Results": []}),
        ("osv-scanner", {"results": []}),
        ("dependency-check", {"dependencies": []}),
        ("joern", {"findings": []}),
    )

    def expected_name_table(self, fixture: str) -> dict[str, dict]:
        """Return the fixture's recorded per-name outcomes, keyed by artifact filename.

        The expected file is the authority, exactly as it is everywhere else in this
        folder: the table it carries is compared against what routing actually does, so
        neither the record nor the router can drift without a named failure. Keying by
        filename rather than by position keeps the comparison independent of the order
        the record happens to list.
        """
        entries = self.require(self.load_expected(fixture), "under_each_recognised_name")
        self.assertIsInstance(entries, list)
        table = {str(entry["artifact_filename"]): entry for entry in entries}
        self.assertEqual(
            sorted(table),
            sorted(shape.ARTIFACT_FILENAMES),
            msg=(
                f"the recorded per-name table for {fixture!r} must cover exactly the "
                "nine artifact filenames, so no name is left unrecorded and none is "
                "recorded that does not exist"
            ),
        )
        return table

    def assertHaltUnderEveryRecognisedName(self, fixture: str) -> None:
        """Assert *fixture* halts under all nine names, with the recorded reason.

        The reason splits by whether the claimed name belongs to a SARIF producer, and
        the split is asserted from ``shape.SARIF_PRODUCERS`` rather than from a list
        repeated here. Both the reason and the recorded reason are checked, so a router
        that halted for the wrong reason -- sending a reader after a producer defect when
        the artifact is simply foreign -- fails.
        """
        document = self.load_fixture(fixture)
        table = self.expected_name_table(fixture)
        for filename in shape.ARTIFACT_FILENAMES:
            tool = shape.resolve_tool(filename)
            recorded = table[filename]
            with self.subTest(fixture=fixture, claimed=filename):
                self.assertEqual(
                    recorded["outcome"],
                    "halt",
                    msg=f"the recorded outcome for {filename!r} must be a halt",
                )
                with self.assertRaises(shape.UnknownArtifactShape) as caught:
                    shape.route(filename, document)
                error = caught.exception
                self.assertIs(type(error), shape.UnknownArtifactShape)
                expected_reason = (
                    shape.REASON_SARIF_PRODUCER_NOT_SARIF
                    if tool in shape.SARIF_PRODUCERS
                    else shape.REASON_NATIVE_SHAPE_UNRECOGNIZED
                )
                self.assertEqual(error.reason, expected_reason)
                self.assertEqual(
                    error.reason,
                    recorded["halt_reason"],
                    msg=(
                        f"the halt reason under {filename!r} must be the one the "
                        "expected file records for that name"
                    ),
                )
                self.assertEqual(
                    error.stem,
                    tool,
                    msg=(
                        "for a resolved name the halt names the canonical tool, so the "
                        "report says whose artifact was refused"
                    ),
                )
                self.assertEqual(
                    error.expectation,
                    shape.native_shape_requirement(tool),
                    msg=(
                        "the halt quotes the requirement from the same table the router "
                        "enforces, so what is stated cannot drift from what is checked"
                    ),
                )
                self.assertEqual(error.expectation, recorded["envelope"])
                self.assertIs(error.sarif_detected, False)
                self.assertIs(
                    shape.matches_native_shape(tool, document),
                    False,
                    msg=f"{fixture!r} must not satisfy {tool!r}'s envelope",
                )
                self.assertIs(recorded["envelope_satisfied"], False)
                json.dumps(error.details())

    def test_the_two_empty_container_fixtures_are_present_and_are_those_containers(self) -> None:
        """Both authored empty containers exist and are the containers they claim.

        The inventory assertion for this class, and it is not ceremony: the object
        fixture halting under nine names and the array fixture routing under one is the
        whole comparison, and a fixture that quietly became the other container would
        make half of it assert the wrong thing while still passing.
        """
        cases = ((EMPTY_OBJECT_FIXTURE, dict), (EMPTY_ARRAY_FIXTURE, list))
        for name, container in cases:
            with self.subTest(fixture=name):
                document = self.load_fixture(name)
                self.assertIsInstance(
                    document,
                    container,
                    msg=(
                        f"fixture {name!r} must carry a top-level "
                        f"{container.__name__}; the pair's whole value is that the two "
                        "differ only in their container"
                    ),
                )
                self.assertEqual(
                    len(document),
                    0,
                    msg=f"fixture {name!r} must be empty, which is the condition it probes",
                )

    def test_an_empty_object_halts_under_every_recognised_name(self) -> None:
        """``{}`` under any of the nine names halts, with the reason recorded for it.

        This is the arbitrary empty container the finding names, and the shape a
        truncated write or a hand-copied placeholder takes. Nine acceptances would each
        be silent: the shared SARIF adapter looks for ``runs`` and finds nothing, the
        Trivy adapter looks for ``Results`` and finds nothing, the Gitleaks adapter
        iterates a document that is not an array at all. All three report zero rows, and
        ``findings.json`` is row-only, so a tool contributing no row is invisible in it.
        """
        self.assertHaltUnderEveryRecognisedName(EMPTY_OBJECT_FIXTURE)

    def test_the_fall_through_document_halts_under_every_recognised_name(self) -> None:
        """``unknown-shape.json`` halts under a recognised name too, not only its own.

        Its own name resolves to no tool, so under that name the halt is attributed to
        the name. Presented under a recognised one it reaches the envelope check and is
        refused there -- including on the two keys it was strengthened with, ``Result``
        for Trivy's ``Results`` and ``findings_summary`` for the Joern collector's
        ``findings``, neither of which is the exact key its envelope requires.
        """
        self.assertHaltUnderEveryRecognisedName(UNKNOWN_SHAPE_FIXTURE)

    def test_both_near_miss_documents_halt_under_every_recognised_name(self) -> None:
        """Neither near-miss document routes under any recognised name.

        Under a producer's name the reason is the producer reason and the halt quotes the
        observed ``version``, which is what distinguishes a SARIF-family document of an
        unaccepted version from an unrelated one. Under a native name the same documents
        carry none of the six native markers, so the reason is the envelope reason: the
        SARIF family is not a fallback shape for a tool that does not write SARIF.
        """
        for name in NEAR_MISS_FIXTURES:
            self.assertHaltUnderEveryRecognisedName(name)

    def test_an_empty_array_routes_only_where_a_bare_array_is_the_envelope(self) -> None:
        """``[]`` routes under ``gitleaks.json`` and halts under the other eight.

        The control that keeps this class honest. A router that halted on every document
        would satisfy every other assertion here and fail this one, and its failure mode
        is not benign: it would stop the run over a tool that ran correctly and found
        nothing, which puts the router in the business of judging results.

        Gitleaks' envelope is the container itself, because its records carry no envelope
        of their own -- the artifact *is* the array (AAP 0.5.4). Checkov's array form is
        refused for the opposite reason: its marker is the elements' shape, so an empty
        array carries no marker and would be indistinguishable from this very document.
        """
        document = self.load_fixture(EMPTY_ARRAY_FIXTURE)
        table = self.expected_name_table(EMPTY_ARRAY_FIXTURE)
        routing_name = shape.artifact_filename_for("gitleaks")

        recorded = table[routing_name]
        self.assertEqual(recorded["outcome"], "route")
        decision = shape.route(routing_name, document)
        self.assertEqual(decision.shape, shape.SHAPE_NATIVE)
        self.assertEqual(decision.shape, recorded["shape"])
        self.assertEqual(decision.tool, "gitleaks")
        self.assertEqual(decision.adapter, shape.adapter_module_for("gitleaks"))
        self.assertEqual(decision.adapter, recorded["adapter"])
        self.assertIs(decision.is_sarif_shape, False)
        self.assertIs(shape.matches_native_shape("gitleaks", document), True)

        for filename in shape.ARTIFACT_FILENAMES:
            if filename == routing_name:
                continue
            tool = shape.resolve_tool(filename)
            with self.subTest(claimed=filename):
                self.assertEqual(table[filename]["outcome"], "halt")
                with self.assertRaises(shape.UnknownArtifactShape) as caught:
                    shape.route(filename, document)
                error = caught.exception
                self.assertEqual(error.reason, table[filename]["halt_reason"])
                self.assertEqual(error.top_level_type, "array")
                self.assertEqual(error.python_type, "list")
                self.assertIsNone(
                    error.version,
                    msg=(
                        "an array has no keys to read a version from, and the halt "
                        "records that absence rather than inventing a value"
                    ),
                )
                self.assertIs(error.sarif_detected, False)
                self.assertIs(shape.matches_native_shape(tool, document), False)

    def test_the_empty_containers_halt_under_their_own_names_by_the_name_reason(self) -> None:
        """Under their own filenames the attribution is the name, not the envelope.

        Recorded and asserted so the two attributions stay distinguishable. Routing
        resolves the writer first, so a name outside the nine stops the decision before
        any envelope is consulted -- and a reader meeting the name reason knows to look
        at where the file came from rather than at what a runner wrote.
        """
        for name in (EMPTY_OBJECT_FIXTURE, EMPTY_ARRAY_FIXTURE):
            with self.subTest(fixture=name):
                expected = self.load_expected(name)
                self.assertIs(
                    self.require(expected, "under_its_own_name", "resolves_to_tool"),
                    False,
                )
                with self.assertRaises(shape.UnknownArtifactShape) as caught:
                    shape.route_artifact(self.subject(name), self.load_fixture(name))
                error = caught.exception
                self.assertEqual(error.reason, shape.REASON_UNRECOGNIZED_ARTIFACT_NAME)
                self.assertEqual(
                    error.reason,
                    self.require(expected, "under_its_own_name", "halt_reason"),
                )
                self.assertEqual(
                    error.stem,
                    name,
                    msg=(
                        "for the name reason there is no tool to name, so the halt "
                        "carries the filename component instead"
                    ),
                )
                self.assertIsNone(
                    error.expectation,
                    msg=(
                        "no envelope was consulted, so no expectation is stated: the "
                        "halt reports what it established and nothing more"
                    ),
                )

    def test_an_empty_report_from_every_native_tool_still_routes(self) -> None:
        """A tool that found nothing is an outcome, not a fault.

        One minimal document per native envelope, each carrying the container AAP 0.5.4
        names for that tool and nothing inside it. Every one routes. This is the
        boundary of the check in the direction that a defect would be invisible: an
        over-strict router produces a halt with a plausible message, and the run stops on
        an artifact that was perfectly valid.
        """
        for tool, document in self.MINIMAL_ENVELOPES:
            filename = shape.artifact_filename_for(tool)
            with self.subTest(tool=tool, top_level=type(document).__name__):
                self.assertIs(
                    shape.matches_native_shape(tool, document),
                    True,
                    msg=(
                        f"the minimal {tool!r} envelope must satisfy the predicate; an "
                        "empty container is a legitimate clean report"
                    ),
                )
                decision = shape.route(filename, document)
                self.assertEqual(decision.tool, tool)
                self.assertEqual(decision.shape, shape.SHAPE_NATIVE)
                self.assertEqual(decision.adapter, shape.adapter_module_for(tool))

    def test_a_trivy_report_whose_results_is_null_is_refused_by_one_contract(self) -> None:
        """``"Results": null`` halts, and both of Trivy's authorities say so.

        The document a recognised name, a present marker and a legal JSON value would each
        have argued for admitting. Trivy is written in Go, which marshals an unset slice
        as ``null`` rather than as ``[]``, so this is a shape a real writer can emit -- and
        it is the one document over which this shape's two authorities could disagree.
        AAP 0.5.4 settles the direction: the count unit is ``Results[]``, an artifact
        matching neither the SARIF shape nor a known native shape halts rather than being
        best-effort parsed, and a record that cannot be attributed with certainty is
        rejected rather than inferred. So the refusal is asserted from **both** entry
        points -- ``matches_native_shape`` over ``shape._matches_trivy_envelope``, and
        ``matches_native_signature`` over ``NATIVE_SIGNATURES["trivy"]`` -- because one
        authority admitting what the other refuses is a defect no output can show: the
        adapter would enumerate nothing from a null member, emit zero rows and zero
        rejections, and the independent traversal would agree at ``0 = 0 + 0`` while the
        recorded signature verdict said the document was never that writer's shape.

        Every value is checked against the fixture *and* against its hand-verified
        expected file, so the two authored documents cannot drift apart silently.
        """
        document = self.load_fixture(NEAR_TRIVY_RESULTS_NULL_FIXTURE)
        expected = self.load_expected(NEAR_TRIVY_RESULTS_NULL_FIXTURE)
        path = self.fixture_path(NEAR_TRIVY_RESULTS_NULL_FIXTURE)
        signature = shape.NATIVE_SIGNATURES["trivy"]
        artifact_name = shape.artifact_filename_for("trivy")

        # The record describes THIS document: digest, size and the member that decides.
        self.assertEqual(
            hashlib.sha256(path.read_bytes()).hexdigest(),
            self.require(expected, "fixture", "sha256"),
        )
        self.assertEqual(path.stat().st_size, self.require(expected, "fixture", "bytes"))
        self.assertIsInstance(document, dict)
        self.assertEqual(list(document), self.require(expected, "fixture", "top_level_keys"))
        self.assertIn(
            shape.TRIVY_RESULTS_KEY,
            document,
            msg=(
                "the fixture must CARRY Results; a document missing it is refused for a "
                "different reason and would leave the present-but-null case untested"
            ),
        )
        self.assertIsNone(
            document[shape.TRIVY_RESULTS_KEY],
            msg="the recorded results_json_type is null, and that is the condition probed",
        )
        self.assertEqual(self.require(expected, "fixture", "results_json_type"), "null")
        self.assertIsNone(
            shape.resolve_tool(NEAR_TRIVY_RESULTS_NULL_FIXTURE),
            msg=(
                "the fixture's own name must resolve to no tool, so the subject routed "
                "under is the artifact filename and not the fixture name"
            ),
        )
        self.assertEqual(self.require(expected, "routed_as", "subject"), artifact_name)
        self.assertIs(shape.is_sarif(document), False)

        # One contract, two entry points, and they answer the same way.
        self.assertIs(
            shape.matches_native_shape("trivy", document),
            False,
            msg=(
                "the envelope requires Results to be present AND a JSON array; a "
                "present-but-null member is not the count unit Results[] and must be "
                "refused here, before any walker is named"
            ),
        )
        self.assertIs(
            shape.matches_native_signature("trivy", document),
            False,
            msg="the declared signature requires the same array and must agree",
        )
        self.assertIs(signature.matches(document), False)
        self.assertEqual(signature.required_key, shape.TRIVY_RESULTS_KEY)
        self.assertEqual(signature.required_key_type, shape.JSON_TYPE_ARRAY)
        self.assertEqual(
            self.require(expected, "one_fail_closed_contract", "required_key_type"),
            shape.JSON_TYPE_ARRAY,
        )
        for recorded_key in ("envelope_verdict", "signature_verdict"):
            self.assertIs(
                self.require(expected, "one_fail_closed_contract", recorded_key),
                False,
                msg=f"the record must state the refusal under {recorded_key!r}",
            )
        self.assertIs(
            self.require(expected, "one_fail_closed_contract", "verdicts_agree"), True
        )

        # No other writer's envelope claims it either.
        recorded_signature = self.require(expected, "native_signature")
        self.assertEqual(recorded_signature["expected"], signature.statement)
        self.assertEqual(recorded_signature["observed_key_type"], "null")
        self.assertIs(recorded_signature["matches"], False)
        for tool in shape.CANONICAL_TOOLS:
            with self.subTest(tool=tool):
                self.assertIs(shape.matches_native_shape(tool, document), False)

        # The halt: named, from both entry points, with the observation that diagnoses it.
        halts: dict[str, shape.UnknownArtifactShape] = {}

        def route_under_the_artifact_name() -> None:
            with self.assertRaises(shape.UnknownArtifactShape) as caught:
                shape.route(artifact_name, document)
            halts["route"] = caught.exception

        self.assertNoAdapterImportedBy(
            f"the envelope halt for {artifact_name!r}", route_under_the_artifact_name
        )
        raw_subject = self.require(expected, "routed_as", "second_subject")
        with self.assertRaises(shape.UnknownArtifactShape) as by_artifact:
            shape.route_artifact(raw_subject, document)
        halts["route_artifact"] = by_artifact.exception

        for entry_point, error in halts.items():
            with self.subTest(entry_point=entry_point):
                self.assertIs(
                    type(error),
                    shape.UnknownArtifactShape,
                    msg=(
                        "a null member is the value most likely to produce a bare "
                        "TypeError from a half-attempted read; the halt must be exactly "
                        "shape.UnknownArtifactShape, which carries a reason a report can "
                        "quote"
                    ),
                )
                for forbidden in (KeyError, TypeError, AttributeError):
                    self.assertNotIsInstance(error, forbidden)
                self.assertEqual(
                    error.reason, shape.REASON_NATIVE_SHAPE_UNRECOGNIZED
                )
                self.assertEqual(error.reason, self.require(expected, "halt", "reason"))
                self.assertIn(error.reason, shape.HALT_REASONS)
                self.assertNotEqual(error.reason, shape.REASON_UNRECOGNIZED_ARTIFACT_NAME)
                self.assertNotEqual(error.reason, shape.REASON_UNSUPPORTED_DOCUMENT_TYPE)
                self.assertNotEqual(error.reason, shape.REASON_SARIF_PRODUCER_NOT_SARIF)
                self.assertEqual(error.stem, "trivy")
                self.assertIs(error.sarif_detected, False)
                self.assertIsNone(error.version)
                self.assertEqual(error.top_level_length, 5)
                self.assertEqual(
                    list(error.top_level_keys),
                    self.require(expected, "halt", "observed_attributes", "top_level_keys"),
                )
                self.assertEqual(error.signature_tool, "trivy")
                self.assertEqual(error.expected_signature, signature.statement)
                self.assertEqual(
                    error.expectation,
                    shape.native_shape_requirement("trivy"),
                    msg=(
                        "the halt quotes the requirement from the same table the router "
                        "enforces, so what is stated cannot drift from what is checked"
                    ),
                )
                observation = dict(error.signature_observation or {})
                self.assertEqual(observation["required_key"], shape.TRIVY_RESULTS_KEY)
                self.assertEqual(observation["required_key_type"], shape.JSON_TYPE_ARRAY)
                self.assertIs(observation["required_key_present"], True)
                self.assertEqual(
                    observation["observed_key_type"],
                    "null",
                    msg=(
                        "required array beside observed null is what tells a reader which "
                        "member decided the refusal and what was wrong with it"
                    ),
                )
                self.assertIs(observation["matches"], False)
                json.dumps(error.details())

        # The false-clean the halt prevents, measured rather than described.
        self.assertEqual(
            reconcile.count_records("trivy", document),
            self.require(expected, "counting_traversal", "would_return"),
            msg=(
                "the independent traversal returns zero over this document, so admitting "
                "it would balance the reconciliation identity at 0 = 0 + 0 and report a "
                "mis-shaped artifact as a clean scan"
            ),
        )
        self.assertEqual(self.require(expected, "rows"), [])
        self.assertEqual(self.require(expected, "counts", "rejections"), 0)
        self.assertIs(self.require(expected, "adapter_invoked"), False)
        for name in (
            shape.REASON_NATIVE_SHAPE_UNRECOGNIZED,
            "halt",
            shape.UnknownArtifactShape.__name__,
        ):
            with self.subTest(not_a_reject_class=name):
                self.assertIs(
                    paths.is_reject_class(name),
                    False,
                    msg=(
                        "a halt is not a counted rejection: this document never reaches "
                        "an adapter, so there is no record to reject and nothing to count "
                        "under any class"
                    ),
                )

    def test_the_trivy_results_contract_answers_once_for_every_observed_type(self) -> None:
        """The envelope predicate and the declared signature agree, state by state.

        The fixture pins one state; this pins the whole member. Each row is a value
        ``Results`` can take, with the verdict the fail-closed contract requires, and both
        authorities are asked separately -- so a relaxation of either alone fails by name
        rather than surviving because the other still refuses. The two array rows are the
        control that keeps the strictness honest: a contract that refused an empty array
        would halt every clean Trivy scan, and that is the same class of wrong answer in
        the opposite direction.

        The recorded matrix in the fixture's expected file carries the same eight states,
        and is compared against these verdicts so neither can drift.
        """
        expected = self.load_expected(NEAR_TRIVY_RESULTS_NULL_FIXTURE)
        recorded = self.require(
            expected, "one_fail_closed_contract", "observed_type_matrix"
        )
        self.assertIsInstance(recorded, list)
        envelope_only = {"SchemaVersion": 2, "ArtifactName": ".", "ArtifactType": "filesystem"}
        target = {"Target": "core/src/main/scala/x.scala", "Class": "secret", "Type": ""}

        # One state of Results per row: its recorded label, the value, and whether the
        # contract admits it. Absence is expressed by the sentinel below rather than by a
        # second document, so every row differs in exactly that one member.
        absent = object()
        states: tuple[tuple[str, object, bool], ...] = (
            ("absent", absent, False),
            ("null", None, False),
            ("empty array", [], True),
            ("array of one target object", [target], True),
            ("object", {"core/src/main/scala/x.scala": target}, False),
            ("string", "[]", False),
            ("number", 3, False),
            ("boolean", True, False),
        )
        self.assertEqual(
            [str(row["results_state"]) for row in recorded],
            [label for label, _, _ in states],
            msg=(
                "the recorded matrix and this one must cover the same states in the same "
                "order, so the record cannot describe a contract the test does not check"
            ),
        )

        for (label, value, admitted), row in zip(states, recorded, strict=True):
            document = dict(envelope_only)
            if value is not absent:
                document[shape.TRIVY_RESULTS_KEY] = value
            with self.subTest(results=label):
                envelope_verdict = shape.matches_native_shape("trivy", document)
                signature_verdict = shape.matches_native_signature("trivy", document)
                self.assertIs(
                    envelope_verdict,
                    admitted,
                    msg=(
                        f"Results as {label} must be "
                        f"{'admitted' if admitted else 'refused'} by the envelope "
                        "predicate: the contract is present AND a JSON array"
                    ),
                )
                self.assertIs(
                    signature_verdict,
                    admitted,
                    msg=(
                        "the declared signature must reach the same verdict as the "
                        "envelope predicate; two authorities over one document is the "
                        "defect this assertion exists to forbid"
                    ),
                )
                self.assertIs(envelope_verdict, signature_verdict)
                self.assertIs(row["envelope_verdict"], admitted)
                self.assertIs(row["signature_verdict"], admitted)
                self.assertEqual(row["outcome"], "route" if admitted else "halt")
                if admitted:
                    decision = shape.route(shape.artifact_filename_for("trivy"), document)
                    self.assertEqual(decision.shape, shape.SHAPE_NATIVE)
                    self.assertEqual(decision.adapter, shape.adapter_module_for("trivy"))
                else:
                    with self.assertRaises(shape.UnknownArtifactShape) as caught:
                        shape.route(shape.artifact_filename_for("trivy"), document)
                    self.assertEqual(
                        caught.exception.reason, shape.REASON_NATIVE_SHAPE_UNRECOGNIZED
                    )

    def test_every_captured_artifact_satisfies_the_envelope_of_the_tool_that_wrote_it(self) -> None:
        """The strictness is asserted against the artifacts this run actually captured.

        The direction a new validator gets wrong in practice: a predicate that refuses
        real output halts the pipeline over nine correct scans. Each captured fixture is
        therefore checked against its own tool's requirement -- the three producers'
        against the SARIF conjunction, the five native artifacts' against their
        container -- so the envelope table is pinned to observed bytes rather than to an
        assumption about them.
        """
        for filename in SARIF_ARTIFACT_FIXTURES:
            with self.subTest(artifact=filename):
                document = self.load_fixture(filename)
                self.assertIs(
                    shape.is_sarif(document),
                    True,
                    msg=(
                        f"{filename!r} is a captured SARIF artifact and must satisfy the "
                        "conjunction; a validator that refused it would halt a correct run"
                    ),
                )
                self.assertEqual(shape.route(filename, document).shape, shape.SHAPE_SARIF)
        for filename in NATIVE_ARTIFACT_FIXTURES:
            tool = shape.resolve_tool(filename)
            with self.subTest(artifact=filename):
                document = self.load_fixture(filename)
                self.assertIs(
                    shape.matches_native_shape(tool, document),
                    True,
                    msg=(
                        f"{filename!r} is {tool!r}'s captured artifact and must satisfy "
                        f"its envelope: {shape.native_shape_requirement(tool)}"
                    ),
                )
                decision = shape.route(filename, document)
                self.assertEqual(decision.shape, shape.SHAPE_NATIVE)
                self.assertEqual(decision.adapter, shape.adapter_module_for(tool))
        with self.subTest(artifact=CHECKOV_ALT_SHAPE_FIXTURE):
            document = self.load_fixture(CHECKOV_ALT_SHAPE_FIXTURE)
            self.assertIs(
                shape.matches_native_shape("checkov", document),
                True,
                msg=(
                    "Checkov's captured multi-framework form must satisfy its envelope; "
                    "AAP 0.5.4 requires both of its top-level forms handled"
                ),
            )

    def test_a_sarif_document_routes_under_every_producer_name(self) -> None:
        """The conjunction, not the specific producer's name, is what admits SARIF.

        Each captured SARIF document is routed under all three producers' names. The
        three share one adapter (AAP 0.5.4), so the shape decision must be identical
        across the three: a document is admitted for satisfying the conjunction, never
        for carrying the filename its own writer uses.
        """
        for source in SARIF_ARTIFACT_FIXTURES:
            document = self.load_fixture(source)
            for claimed_name in SARIF_ARTIFACT_FIXTURES:
                with self.subTest(document=source, claimed=claimed_name):
                    decision = shape.route(claimed_name, document)
                    self.assertEqual(decision.shape, shape.SHAPE_SARIF)
                    self.assertEqual(decision.adapter, shape.SHARED_SARIF_ADAPTER)
                    self.assertEqual(decision.tool, shape.resolve_tool(claimed_name))

    def test_the_envelope_table_covers_the_nine_and_refuses_anything_else(self) -> None:
        """Every canonical tool has a requirement; no other identifier has one.

        A tool missing from the table would raise ``KeyError`` deep inside routing, which
        is the unnamed failure this file's halt assertions exist to forbid. A tool the
        table admitted but the canonical list does not carry would be a tenth tool
        entering the dataset through the router. Both directions are asserted, and the
        unknown-identifier case is a ``ValueError`` -- a caller bug, distinct from every
        artifact condition.
        """
        for tool in shape.CANONICAL_TOOLS:
            with self.subTest(tool=tool):
                requirement = shape.native_shape_requirement(tool)
                self.assertIsInstance(requirement, str)
                self.assertTrue(
                    requirement.strip(),
                    msg=f"{tool!r} must state what its artifact has to be",
                )
                self.assertIsInstance(shape.matches_native_shape(tool, {}), bool)
                if tool in shape.SARIF_PRODUCERS:
                    self.assertIn(
                        shape.SARIF_VERSION,
                        requirement,
                        msg=(
                            f"{tool!r} writes SARIF and nothing else, so its stated "
                            "requirement must name the version the conjunction accepts"
                        ),
                    )
        marker_by_tool = {
            "gitleaks": "array",
            "checkov": shape.CHECKOV_RESULTS_KEY,
            "trivy": shape.TRIVY_RESULTS_KEY,
            "osv-scanner": shape.OSV_SCANNER_RESULTS_KEY,
            "dependency-check": shape.DEPENDENCY_CHECK_DEPENDENCIES_KEY,
            "joern": shape.JOERN_FINDINGS_KEY,
        }
        for tool, marker in marker_by_tool.items():
            with self.subTest(tool=tool, marker=marker):
                self.assertIn(
                    marker,
                    shape.native_shape_requirement(tool),
                    msg=(
                        f"{tool!r}'s stated requirement must name the container it is "
                        "checked for, so the halt message and the predicate describe one "
                        "requirement rather than two"
                    ),
                )
        for absent in ("codeql", "opengrep.sarif.bak", "", "Trivy "):
            with self.subTest(unknown=absent):
                with self.assertRaises(ValueError):
                    shape.matches_native_shape(absent, {})
                with self.assertRaises(ValueError):
                    shape.native_shape_requirement(absent)

    def test_the_four_halt_reasons_are_distinct_and_all_named(self) -> None:
        """``UNKNOWN_SHAPE_REASONS`` is the closed vocabulary, and it is exhaustive.

        A reason string that existed but was not listed could not be classified by a
        reader of ``harness/artifacts/logs/normalize-run.json``, and two reasons sharing
        a string would collapse two different defects into one report line.
        """
        reasons = shape.UNKNOWN_SHAPE_REASONS
        self.assertEqual(len(reasons), 4)
        self.assertEqual(len(set(reasons)), len(reasons))
        for reason in (
            shape.REASON_UNRECOGNIZED_ARTIFACT_NAME,
            shape.REASON_UNSUPPORTED_DOCUMENT_TYPE,
            shape.REASON_SARIF_PRODUCER_NOT_SARIF,
            shape.REASON_NATIVE_SHAPE_UNRECOGNIZED,
        ):
            with self.subTest(reason=reason):
                self.assertIn(reason, reasons)
                self.assertIsInstance(reason, str)
                self.assertTrue(reason.strip())

    def test_an_envelope_halt_and_an_envelope_route_both_import_no_adapter(self) -> None:
        """Neither outcome reaches the adapter layer.

        The halt cannot, since it precedes selection. The routing decision must not
        either: it names an adapter by string key and ``cli.py`` resolves the key to a
        callable, which is what lets the detector be exercised in isolation and what
        stops an adapter from producing rows from a document whose shape was never
        established.
        """
        empty_object = self.load_fixture(EMPTY_OBJECT_FIXTURE)
        empty_array = self.load_fixture(EMPTY_ARRAY_FIXTURE)
        trivy_name = shape.artifact_filename_for("trivy")
        gitleaks_name = shape.artifact_filename_for("gitleaks")

        def attempt_to_route() -> None:
            with self.assertRaises(shape.UnknownArtifactShape):
                shape.route(trivy_name, empty_object)

        self.assertNoAdapterImportedBy(
            f"the envelope halt for {trivy_name!r}", attempt_to_route
        )
        decision = self.assertNoAdapterImportedBy(
            f"the envelope route for {gitleaks_name!r}",
            lambda: shape.route(gitleaks_name, empty_array),
        )
        self.assertEqual(decision.adapter, shape.adapter_module_for("gitleaks"))
        self.assertIsInstance(
            decision.adapter,
            str,
            msg="the adapter is a string key, never a module object",
        )
        self.assertNotIsInstance(decision.adapter, types.ModuleType)


class ArtifactControlledDiagnosticTests(ShapeTestCase):
    """Every artifact-supplied value in routing and halt evidence is bounded and inert.

    Two directions, and the success path is asserted first because it is the one that
    does not look adversarial. A halt is obviously adversarial input, so its rendering
    attracts attention; a successful detection does not, and is: a valid native envelope
    may carry arbitrary extra top-level keys, and ``detection_evidence`` publishes the
    observed keys and the observed ``version`` into
    ``harness/artifacts/logs/normalize-run.json``, a file this pipeline preserves.
    Unbounded and unescaped, a scanner artifact would then choose both the content and
    the size of a durable record, and a credential-bearing URI or an ESC sequence would
    ride into whatever reads it.

    What is required of the rendering is fixed: the value's type and context, its FULL
    character length and sha256 so a bounded excerpt is still identified, a bounded
    excerpt, URI-userinfo redaction, and control escaping. Tab and newline survive,
    because this dataset carries messages with embedded newlines by design (AAP 0.5.4).
    """

    #: A real ESC byte, not the two-character ``\x1b`` spelling. The distinction is the
    #: point: ``repr()`` renders a control as an inert escape, so a test that searched
    #: for the spelling would pass on text that still carried the byte.
    ESC = chr(27)

    #: One value carrying all three hazards at once: a URI userinfo credential, a
    #: terminal control sequence, and a length no record should let an artifact choose.
    HOSTILE = "https://user:secret@evil.example/" + chr(27) + "[2J" + "A" * 900

    def assert_inert(self, rendered: object, where: str) -> None:
        """Require *rendered* to carry no credential and no real control byte."""
        text = str(rendered)
        self.assertNotIn(
            "secret", text, msg=f"{where}: a URI userinfo credential reached the record"
        )
        self.assertNotIn(
            self.ESC, text, msg=f"{where}: a real control byte reached the record"
        )

    def assert_key_provenance(self, entry: object, original: str, where: str) -> None:
        """Require one reported key to carry the FULL evidence contract, not just text.

        Bounding an artifact-supplied key makes it safe to read. It does not make it
        checkable: a reader who needs to know what the artifact actually sent needs the
        original's full length and its full digest, and a 16-hex prefix inside a
        truncation annotation is not the digest. So every reported key carries the same
        contract a scalar value gets -- context, type, full character length, full
        64-character sha256, bounded excerpt, and the counts of what sanitising changed.
        """
        self.assertIsInstance(
            entry, dict, msg=f"{where}: a reported key must be a diagnostic object"
        )
        for field in (
            "context",
            "value_type",
            "character_length",
            "sha256",
            "excerpt",
            "truncated",
            "controls_escaped",
            "userinfo_redactions",
        ):
            self.assertIn(field, entry, msg=f"{where}: missing {field}")
        self.assertEqual(
            entry["character_length"],
            len(original),
            msg=f"{where}: character_length must be the FULL original length",
        )
        self.assertEqual(
            entry["sha256"],
            hashlib.sha256(original.encode("utf-8")).hexdigest(),
            msg=f"{where}: sha256 must be the FULL digest of the original",
        )
        self.assertEqual(
            len(entry["sha256"]), 64, msg=f"{where}: sha256 must be all 64 characters"
        )
        self.assert_inert(entry["excerpt"], f"{where} excerpt")

    def test_success_path_bounds_a_hostile_extra_top_level_key(self) -> None:
        """A VALID native envelope with a hostile extra key still records safely."""
        document = {"Results": [], self.HOSTILE: 1}
        evidence = shape.detection_evidence(document)

        # The decision is unchanged: sanitising evidence must never move routing.
        self.assertFalse(evidence["is_sarif"])
        self.assertFalse(evidence["version_matches"])

        for entry in evidence["top_level_keys"]:
            self.assert_inert(entry, "detection_evidence top_level_keys")
        for excerpt in evidence["top_level_key_excerpts"]:
            self.assert_inert(excerpt, "detection_evidence top_level_key_excerpts")
        self.assertTrue(
            any(
                shape.USERINFO_REDACTION in excerpt
                for excerpt in evidence["top_level_key_excerpts"]
            ),
            msg="the redaction marker should be visible where a credential was removed",
        )
        self.assertEqual(evidence["top_level_keys_total"], 2)

        # The hostile key keeps its FULL provenance despite the bounded excerpt.
        hostile_entries = [
            entry
            for entry in evidence["top_level_keys"]
            if entry["character_length"] == len(self.HOSTILE)
        ]
        self.assertEqual(
            len(hostile_entries),
            1,
            msg="the hostile key must appear exactly once with its full length recorded",
        )
        self.assert_key_provenance(
            hostile_entries[0], self.HOSTILE, "detection_evidence hostile key"
        )
        self.assertEqual(hostile_entries[0]["context"], "top-level key")
        self.assertEqual(hostile_entries[0]["value_type"], "str")
        self.assertTrue(hostile_entries[0]["truncated"])
        self.assertEqual(hostile_entries[0]["userinfo_redactions"], 1)
        self.assertEqual(hostile_entries[0]["controls_escaped"], 1)
        self.assertTrue(evidence["top_level_keys_evidence"])
        self.assertTrue(json.dumps(evidence))

    def test_the_key_count_cap_is_not_used_as_the_excerpt_length_cap(self) -> None:
        """The two bounds are independent; conflating them silently over-truncates.

        ``safe_keys`` caps how many keys are reported AND how long each excerpt may be.
        Passing the key-count cap as the character cap would leave every excerpt bounded
        at 64 characters while the record still claimed the 512-character policy.
        """
        self.assertNotEqual(shape.SHAPE_VALUE_LIMIT, shape.SHAPE_KEYS_REPORTED_LIMIT)
        rendered = shape.safe_keys((self.HOSTILE,))
        excerpt = rendered["keys"][0]["excerpt"]
        # Longer than the key-count cap could ever have allowed.
        self.assertGreater(len(excerpt), shape.SHAPE_KEYS_REPORTED_LIMIT)
        self.assertIn(str(shape.SHAPE_VALUE_LIMIT), excerpt)
        self.assertIn(str(len(self.HOSTILE)), excerpt)

    def test_success_path_publishes_a_safe_version_with_length_and_digest(self) -> None:
        """The observed version is bounded, inert, and still identified by length+digest."""
        evidence = shape.detection_evidence({"version": self.HOSTILE, "runs": []})

        self.assert_inert(evidence["version_observed"], "version_observed")
        self.assertLessEqual(
            len(str(evidence["version_observed"])),
            shape.SHAPE_VALUE_LIMIT + 128,
            msg="the excerpt must be bounded, with room for the truncation annotation",
        )
        detail = evidence["version_observed_evidence"]
        self.assertEqual(detail["value_type"], "str")
        self.assertEqual(detail["character_length"], len(self.HOSTILE))
        self.assertEqual(len(detail["sha256"]), 64)
        self.assertTrue(detail["truncated"])
        self.assertEqual(detail["userinfo_redactions"], 1)
        self.assertEqual(detail["controls_escaped"], 1)
        self.assert_inert(detail["excerpt"], "version_observed_evidence excerpt")

    def test_success_path_caps_how_many_keys_an_artifact_can_publish(self) -> None:
        """A document with thousands of keys cannot choose this record's size."""
        document = {f"k{index}": 1 for index in range(5000)}
        document["version"] = "2.1.0"
        evidence = shape.detection_evidence(document)

        self.assertEqual(evidence["top_level_keys_total"], 5001)
        self.assertEqual(
            evidence["top_level_keys_reported"], shape.SHAPE_KEYS_REPORTED_LIMIT
        )
        self.assertTrue(evidence["top_level_keys_truncated"])
        self.assertEqual(
            len(evidence["top_level_keys"]), shape.SHAPE_KEYS_REPORTED_LIMIT
        )

    def test_wrong_version_halt_renders_its_version_safely(self) -> None:
        """A wrong-version SARIF under a recognised name halts with inert details."""
        with self.assertRaises(shape.UnknownArtifactShape) as caught:
            shape.route_artifact(
                "harness/artifacts/raw/opengrep.sarif",
                {"version": self.HOSTILE, "runs": []},
            )
        error = caught.exception
        details = error.details()

        self.assertEqual(details["reason"], shape.REASON_SARIF_PRODUCER_NOT_SARIF)
        self.assert_inert(details["version"], "halt details version")
        self.assert_inert(str(error), "halt message")
        self.assertEqual(
            details["version_evidence"]["character_length"], len(self.HOSTILE)
        )
        self.assertEqual(len(details["version_evidence"]["sha256"]), 64)
        self.assertTrue(json.dumps(details))

    def test_unknown_shape_halt_bounds_and_sanitises_its_keys(self) -> None:
        """An unknown-shape document's keys are bounded and inert in the halt record."""
        document = {self.HOSTILE: 1}
        document.update({f"k{index}": 1 for index in range(3000)})
        with self.assertRaises(shape.UnknownArtifactShape) as caught:
            shape.route_artifact("harness/artifacts/raw/gitleaks.json", document)
        details = caught.exception.details()

        for entry in details["top_level_keys"]:
            self.assert_inert(entry, "halt details top_level_keys")
        for excerpt in details["top_level_key_excerpts"]:
            self.assert_inert(excerpt, "halt details top_level_key_excerpts")
        self.assertEqual(details["top_level_keys_total"], 3001)
        self.assertEqual(
            details["top_level_keys_reported"], shape.SHAPE_KEYS_REPORTED_LIMIT
        )
        self.assertTrue(details["top_level_keys_truncated"])

        # The halt path carries the same full per-key provenance as the success path:
        # a record that identifies what was rejected is what makes the rejection
        # checkable, and the halt record is the only place that evidence survives.
        hostile_entries = [
            entry
            for entry in details["top_level_keys"]
            if entry["character_length"] == len(self.HOSTILE)
        ]
        self.assertEqual(len(hostile_entries), 1)
        self.assert_key_provenance(
            hostile_entries[0], self.HOSTILE, "halt details hostile key"
        )
        self.assertEqual(hostile_entries[0]["userinfo_redactions"], 1)
        self.assertEqual(hostile_entries[0]["controls_escaped"], 1)
        self.assertTrue(details["top_level_keys_evidence"])
        self.assert_inert(str(caught.exception), "halt message")
        self.assertTrue(json.dumps(details))

    def test_tab_and_newline_survive_because_the_dataset_carries_them(self) -> None:
        """Escaping must not rewrite legitimate evidence (AAP 0.5.4)."""
        rendered = shape.safe_text("first\tsecond\nthird")
        self.assertIn("\t", rendered["text"])
        self.assertIn("\n", rendered["text"])
        self.assertEqual(rendered["controls_escaped"], 0)


class SafeRenderingParityTests(ShapeTestCase):
    """``shape``'s own guard and ``paths``' renderer are one policy, not two.

    ``shape.py`` is a leaf that imports nothing from the package -- AAP 0.6.4 fixes
    that an adapter depends only on ``paths`` and ``severity``, and ``CANONICAL_TOOLS``
    is duplicated here for the same reason. A duplicated *guard* is only acceptable
    while it cannot drift from the one it duplicates, so the two are run over the same
    hostile inputs and required to agree. If someone changes one policy, this fails
    rather than leaving the other quietly weaker.
    """

    CASES = (
        "https://user:secret@evil.example/path",
        "plain text with no hazard at all",
        "control " + chr(27) + "[2J sequence",
        "tab\tand\nnewline are kept",
        "A" * 2000,
        "name@domain.example is not a credential",
        "git@host:path is not a credential either",
    )

    def test_limits_agree(self) -> None:
        """The bound and the redaction marker are the same value in both modules."""
        from normalize import paths

        self.assertEqual(shape.SHAPE_VALUE_LIMIT, paths.DIAGNOSTIC_VALUE_LIMIT)
        self.assertEqual(shape.USERINFO_REDACTION, paths.USERINFO_REDACTION)

    def test_rendering_agrees_on_every_hostile_case(self) -> None:
        """Both implementations produce the same text, digest and change counts."""
        from normalize import paths

        for case in self.CASES:
            with self.subTest(case=case[:40]):
                mine = shape.safe_text(case, limit=shape.SHAPE_VALUE_LIMIT)
                theirs = paths.sanitise_diagnostic(
                    case, limit=shape.SHAPE_VALUE_LIMIT
                )
                self.assertEqual(mine["text"], theirs.text)
                self.assertEqual(mine["sha256"], theirs.sha256)
                self.assertEqual(mine["original_length"], theirs.original_length)
                self.assertEqual(mine["truncated"], theirs.truncated)
                self.assertEqual(mine["controls_escaped"], theirs.controls_escaped)
                self.assertEqual(
                    mine["userinfo_redactions"], theirs.userinfo_redactions
                )

    def test_value_description_agrees_for_a_non_string(self) -> None:
        """A wrong-typed value is described identically by both."""
        from normalize import paths

        for value in ({"a": 1}, [1, 2, 3], 17, None, True):
            with self.subTest(value=repr(value)[:30]):
                mine = shape.safe_value(value, context="version")
                theirs = paths.safe_diagnostic(value, context="version").as_dict()
                self.assertEqual(mine["value_type"], theirs["value_type"])
                self.assertEqual(mine["character_length"], theirs["character_length"])
                self.assertEqual(mine["sha256"], theirs["sha256"])
                self.assertEqual(mine["excerpt"], theirs["excerpt"])

    def test_per_key_provenance_agrees_with_the_other_implementation(self) -> None:
        """Each reported KEY is described identically by both implementations.

        ``safe_keys`` is the newest rendering site and the one an artifact controls most
        directly, so its per-key evidence is pinned to the other module the same way the
        scalar path is. Without this, a future change to one implementation could leave
        the two modules describing the same hostile key differently -- and the parity the
        duplication is justified by would be gone with nothing failing.
        """
        from normalize import paths

        rendered = shape.safe_keys(tuple(self.CASES))
        self.assertEqual(rendered["reported"], len(self.CASES))
        for entry, case in zip(rendered["keys"], self.CASES):
            with self.subTest(case=case[:40]):
                theirs = paths.safe_diagnostic(case, context="top-level key").as_dict()
                self.assertEqual(entry["value_type"], theirs["value_type"])
                self.assertEqual(entry["character_length"], theirs["character_length"])
                self.assertEqual(entry["sha256"], theirs["sha256"])
                self.assertEqual(entry["excerpt"], theirs["excerpt"])
                # And the full digest is the digest of the ORIGINAL, not of the excerpt.
                self.assertEqual(
                    entry["sha256"], hashlib.sha256(case.encode("utf-8")).hexdigest()
                )

class CliTestCase(unittest.TestCase):
    """Temporary-directory and halt-assertion helpers shared by the CLI classes.

    The halt helper exists because every CLI failure carries four facts that must be
    asserted together -- the exception class, the reason drawn from the closed
    ``cli.HALT_REASONS`` set, the exit code, and the details a reader of
    ``normalize-run.json`` acts on. Asserting only the class would pass for any halt at
    all, which is the failure mode a run record exists to prevent.
    """

    def temporary_directory(self) -> Path:
        """Return a fresh temporary directory, released when the test ends."""
        handle = tempfile.TemporaryDirectory(prefix="blitzy-cli-shape-")
        self.addCleanup(handle.cleanup)
        return Path(handle.name)

    def assertHalt(self, halt, *, reason: str, exit_code: int, expected_class=None):
        """Assert one halt's class, reason, exit code and serialisability together."""
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
        as_dict = halt.as_dict()
        self.assertEqual(as_dict["reason"], reason)
        self.assertEqual(as_dict["exit_code"], exit_code)
        self.assertEqual(as_dict["message"], halt.message)
        self.assertIsInstance(as_dict["details"], dict)
        return halt

    def namespace(self, *arguments: str) -> argparse.Namespace:
        """Parse *arguments* with the real parser, so no namespace is hand-built."""
        return cli.build_parser().parse_args(list(arguments))

    def dummy_inputs(self, directory: Path, **overrides) -> cli.Inputs:
        """Build an :class:`cli.Inputs` whose every path sits inside *directory*.

        Used by the stage classes, which each exercise one stage and therefore need the
        other seven fields present but unused. Every default is inside the temporary
        directory, so a stage that unexpectedly wrote something writes it there.
        """
        fields = {
            "raw_dir": directory / "raw",
            "runner_metadata": directory / "logs" / DEFAULT_METADATA_FILENAME,
            "allowlist": directory / "allowlist.txt",
            "log_dir": directory / "logs",
            "spark_src": str(directory / "spark-src"),
            "findings_json": directory / "out" / "findings.json",
            "findings_csv": directory / "out" / "findings.csv",
            "run_record": directory / "out" / DEFAULT_RUN_RECORD_FILENAME,
        }
        fields.update(overrides)
        return cli.Inputs(**fields)


# --------------------------------------------------------------------------------------
# The command-line surface: the parser, and the usage exit
# --------------------------------------------------------------------------------------


class CliParserContractTests(CliTestCase):
    """``cli.build_parser`` declares eight value-taking options and no default.

    Two properties are load-bearing. **No option carries a default here**, because a
    default resolved at parser-construction time would read the environment at import
    time -- the one thing the CLI contract says it never does -- and a test could then
    not supply an environment of its own. And **an unrecognised or incomplete
    invocation exits 2 rather than raising a traceback**, which is argparse's own
    convention and the code ``cli.EXIT_USAGE`` names: a traceback out of a harness entry
    point is indistinguishable, to the shell that ran it, from a crash mid-dataset.
    """

    def test_the_parser_declares_every_documented_option_taking_a_value(self) -> None:
        """Each documented flag parses a value into its own namespace attribute.

        Asserted through ``parse_args`` rather than by reading the parser's internal
        action list, so the assertion is about the behaviour a caller gets.
        """
        parser = cli.build_parser()
        self.assertIsInstance(parser, argparse.ArgumentParser)
        self.assertEqual(parser.prog, "normalize.cli")

        for flag, destination, metavar in CLI_OPTIONS:
            with self.subTest(option=flag):
                namespace = parser.parse_args([flag, f"/tmp/value-for{flag}"])
                self.assertEqual(
                    getattr(namespace, destination),
                    f"/tmp/value-for{flag}",
                    msg=f"{flag} must parse its value into namespace.{destination}",
                )
                self.assertIn(
                    f"{flag} {metavar}",
                    parser.format_usage(),
                    msg=(
                        f"{flag} must advertise the metavar {metavar!r} so its usage "
                        "line says whether it takes a file or a directory"
                    ),
                )

    def test_no_option_carries_a_default_so_importing_reads_nothing(self) -> None:
        """An empty invocation leaves all eight attributes ``None`` and nothing else.

        The defaults live in :func:`cli.resolve_inputs`, at call time, against an
        environment mapping the caller supplies. A default baked into the parser would
        make the module's behaviour depend on the environment that happened to be set
        when it was imported.
        """
        namespace = self.namespace()
        self.assertEqual(
            sorted(vars(namespace)),
            sorted(destination for _, destination, _ in CLI_OPTIONS),
            msg="the parser must declare exactly the eight documented destinations",
        )
        for destination in vars(namespace):
            with self.subTest(destination=destination):
                self.assertIsNone(
                    getattr(namespace, destination),
                    msg=(
                        f"namespace.{destination} must default to None; a default here "
                        "would be an import-time environment read"
                    ),
                )

    def test_an_unrecognised_option_exits_two_and_writes_nothing(self) -> None:
        """The usage exit is argparse's, is ``cli.EXIT_USAGE``, and precedes every write.

        ``main`` parses before it builds the run record, so a usage error cannot leave a
        record, a dataset or a partial file behind. The run-record path is passed
        explicitly and asserted absent afterwards, which is what makes the ordering an
        assertion rather than an inference.
        """
        directory = self.temporary_directory()
        run_record = directory / "out" / DEFAULT_RUN_RECORD_FILENAME
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as raised:
                cli.main(["--run-record", str(run_record), "--not-an-option"])

        self.assertEqual(raised.exception.code, 2)
        self.assertEqual(
            raised.exception.code,
            cli.EXIT_USAGE,
            msg="cli.EXIT_USAGE must be the code argparse actually exits with",
        )
        self.assertIn("unrecognized arguments", stderr.getvalue())
        self.assertIn("--not-an-option", stderr.getvalue())
        self.assertFalse(
            run_record.exists(),
            msg=(
                "a usage error must leave no run record: parsing precedes every write, "
                "and a record for a run that never started would describe nothing"
            ),
        )
        self.assertFalse(
            (directory / "out").exists(),
            msg="nor may it create the directory the record would have gone in",
        )

    def test_an_option_missing_its_value_exits_two(self) -> None:
        """Every option takes a value, so a bare flag is a usage error rather than None."""
        for flag, _, _ in CLI_OPTIONS:
            with self.subTest(option=flag):
                stderr = io.StringIO()
                with contextlib.redirect_stderr(stderr):
                    with self.assertRaises(SystemExit) as raised:
                        cli.main([flag])
                self.assertEqual(raised.exception.code, cli.EXIT_USAGE)
                self.assertIn("expected one argument", stderr.getvalue())

    def test_help_exits_zero_and_states_the_four_exit_codes(self) -> None:
        """``--help`` documents the contract a caller has to script against.

        The exit codes are the interface between this module and the shell that runs it,
        so their absence from the help text is a documentation gap in the interface
        itself rather than in prose about it.
        """
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            with self.assertRaises(SystemExit) as raised:
                cli.main(["--help"])
        self.assertEqual(raised.exception.code, 0)
        # Collapsed to single spaces first: argparse wraps its epilog to the terminal
        # width, so a fragment can legitimately straddle a line break.
        text = " ".join(stdout.getvalue().split())
        for fragment in (
            "0 success",
            "1 halting condition in the data",
            "2 usage error",
            "78 configuration fault",
            "harness/artifacts/raw/",
        ):
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, text)

    def test_the_exit_codes_and_exit_statuses_are_the_documented_distinct_values(self) -> None:
        """Four distinct codes, with 78 tied to the platform's own ``EX_CONFIG``.

        ``cli.EXIT_CONFIG`` is the same code ``harness/lib/scope.sh`` exits with for a
        fault to correct rather than a scanning outcome to classify, so it is asserted
        against :data:`os.EX_CONFIG` rather than against a repeated literal. The three
        exit-status *names* are asserted too: ``timeout`` is the single name AAP 0.8.1
        gives a termination that produced no exit code, and it is a different fact from
        ``unrecorded``, which is the absence of a status file altogether.
        """
        codes = (cli.EXIT_OK, cli.EXIT_HALT, cli.EXIT_USAGE, cli.EXIT_CONFIG)
        self.assertEqual((0, 1, 2, 78), codes)
        self.assertEqual(len(set(codes)), 4, msg="the four exit codes must be distinct")
        self.assertEqual(cli.EXIT_CONFIG, os.EX_CONFIG)

        statuses = (
            cli.EXIT_STATUS_EXITED,
            cli.EXIT_STATUS_TIMEOUT,
            cli.EXIT_STATUS_UNRECORDED,
        )
        self.assertEqual(("exited", "timeout", "unrecorded"), statuses)
        self.assertEqual(len(set(statuses)), 3)


# --------------------------------------------------------------------------------------
# Input resolution: the argument, then the environment, then a named fault
# --------------------------------------------------------------------------------------


class CliInputResolutionTests(CliTestCase):
    """``cli.resolve_inputs`` resolves eight inputs or names what is missing.

    The contract has three parts and each has its own failure mode. An explicit
    argument always wins, so a defaulted value can never silently override one a caller
    named. A default comes from the environment ``harness/env.sh`` exports and from
    nothing else -- no repository-relative path is assumed against whatever directory
    the caller happened to be in. And a required input that can be neither supplied nor
    defaulted is a :class:`cli.ConfigurationFault` naming the flag *and* the variable
    that would have supplied it, because a message that says only "missing input" leaves
    the reader to guess which of the two routes to use.
    """

    def test_the_default_locations_are_the_documented_ones(self) -> None:
        """The four relative default locations, asserted against this file's own copy.

        A default filename that drifted would move ``normalize-run.json`` or the dataset
        without any test noticing, because every other assertion in this class derives
        its expectation from these same constants.
        """
        self.assertEqual(cli.RUNNER_METADATA_FILENAME, DEFAULT_METADATA_FILENAME)
        self.assertEqual(cli.RUN_RECORD_FILENAME, DEFAULT_RUN_RECORD_FILENAME)
        self.assertEqual(cli.FINDINGS_JSON_RELATIVE, DEFAULT_FINDINGS_JSON_RELATIVE)
        self.assertEqual(cli.FINDINGS_CSV_RELATIVE, DEFAULT_FINDINGS_CSV_RELATIVE)

    def test_an_empty_environment_names_every_missing_input_and_its_variable(self) -> None:
        """All eight are missing, each named with the variable that would have supplied it.

        Asserted as the complete list rather than as "at least one": a fault that named
        the first missing input only would send a caller round the loop eight times.
        """
        with self.assertRaises(cli.ConfigurationFault) as raised:
            cli.resolve_inputs(self.namespace(), {})
        fault = self.assertHalt(
            raised.exception,
            reason=cli.HALT_MISSING_INPUT,
            exit_code=cli.EXIT_CONFIG,
            expected_class=cli.ConfigurationFault,
        )
        missing = fault.details["missing"]
        self.assertEqual(
            [entry["input"] for entry in missing],
            [flag for flag, _, _ in CLI_OPTIONS],
            msg="every unresolvable input must be named, in the parser's own order",
        )
        for entry in missing:
            with self.subTest(input=entry["input"]):
                self.assertTrue(
                    entry["defaulted_from"].startswith("$"),
                    msg=(
                        f"{entry['input']} must name the environment variable that "
                        "would have supplied it, so the message says what to do"
                    ),
                )
        self.assertIn("Source harness/env.sh", fault.message)

    def test_an_exported_but_empty_variable_counts_as_unset(self) -> None:
        """``VAR=""`` is an override nobody intended, not a location.

        ``harness/env.sh`` writes every value through ``${VAR:-default}``, so an empty
        string reaching this module means the variable was exported empty. Treating it
        as a path would resolve the dataset to the current directory.
        """
        environ = {
            "HARNESS_RAW_DIR": "",
            "HARNESS_LOG_DIR": "   ",
            "HARNESS_SCOPE_FILE": "",
            "SPARK_SRC": "",
            "HARNESS_REPO_ROOT": "",
        }
        with self.assertRaises(cli.ConfigurationFault) as raised:
            cli.resolve_inputs(self.namespace(), environ)
        self.assertEqual(
            [entry["input"] for entry in raised.exception.details["missing"]],
            [flag for flag, _, _ in CLI_OPTIONS],
        )

    def test_the_environment_supplies_every_default_it_documents(self) -> None:
        """Five variables resolve all eight inputs, three of them derived.

        The derived three are the ones a reader is most likely to get wrong: the runner
        metadata and the run record live in the log tree, and the dataset lives under the
        repository root at its two fixed relative paths.
        """
        directory = self.temporary_directory()
        environ = {
            "HARNESS_RAW_DIR": str(directory / "artifacts" / "raw"),
            "HARNESS_LOG_DIR": str(directory / "artifacts" / "logs"),
            "HARNESS_SCOPE_FILE": str(directory / "scope" / "allowlist.txt"),
            "SPARK_SRC": str(directory / "spark-src"),
            "HARNESS_REPO_ROOT": str(directory / "repo"),
        }
        inputs = cli.resolve_inputs(self.namespace(), environ)

        expected = {
            "raw_dir": directory / "artifacts" / "raw",
            "log_dir": directory / "artifacts" / "logs",
            "allowlist": directory / "scope" / "allowlist.txt",
            "runner_metadata": (
                directory / "artifacts" / "logs" / DEFAULT_METADATA_FILENAME
            ),
            "run_record": (
                directory / "artifacts" / "logs" / DEFAULT_RUN_RECORD_FILENAME
            ),
            "findings_json": directory / "repo" / DEFAULT_FINDINGS_JSON_RELATIVE,
            "findings_csv": directory / "repo" / DEFAULT_FINDINGS_CSV_RELATIVE,
        }
        for field, value in expected.items():
            with self.subTest(field=field):
                self.assertEqual(getattr(inputs, field), value)
        self.assertEqual(inputs.spark_src, str(directory / "spark-src"))

    def test_an_explicit_argument_beats_the_environment_for_every_input(self) -> None:
        """Eight arguments, eight contradicting variables, and the arguments win.

        Asserted for all eight rather than for one: a precedence bug is normally in one
        branch, and the branch that carries it is not predictable from the others.
        """
        directory = self.temporary_directory()
        # Each argument is distinct so a precedence bug cannot hide behind a shared value,
        # and two of them are placed where their owner root requires: the run record
        # inside the log tree the same argument list names, and both dataset members
        # inside the declared repository root. Those two rules are the resolution's own,
        # so an argument list that broke them would be refused before precedence was ever
        # reached, and this test would be asserting nothing about precedence.
        argument_root = directory / "argument"
        stated = {
            "raw_dir": argument_root / "raw_dir",
            "runner_metadata": argument_root / "log_dir" / "runner_metadata",
            "allowlist": argument_root / "allowlist",
            "log_dir": argument_root / "log_dir",
            "spark_src": argument_root / "spark_src",
            "findings_json": argument_root / "findings_json",
            "findings_csv": argument_root / "findings_csv",
            "run_record": argument_root / "log_dir" / "run_record",
        }
        arguments: list[str] = []
        for flag, destination, _ in CLI_OPTIONS:
            arguments.extend([flag, str(stated[destination])])
        environ = {
            "HARNESS_RAW_DIR": str(directory / "environment" / "raw"),
            "HARNESS_LOG_DIR": str(directory / "environment" / "logs"),
            "HARNESS_SCOPE_FILE": str(directory / "environment" / "allowlist.txt"),
            "SPARK_SRC": str(directory / "environment" / "spark-src"),
            "HARNESS_REPO_ROOT": str(argument_root),
        }
        inputs = cli.resolve_inputs(self.namespace(*arguments), environ)
        for flag, destination, _ in CLI_OPTIONS:
            with self.subTest(option=flag):
                value = getattr(inputs, destination)
                self.assertEqual(
                    str(value),
                    str(stated[destination]),
                    msg=f"{flag} must win over the environment default",
                )
                self.assertNotIn("environment", str(value))

    def test_the_log_tree_and_the_metadata_file_derive_from_each_other(self) -> None:
        """Naming either one settles the other, in both directions.

        ``runner-metadata.json`` lives in the log tree, so naming the file settles the
        tree and naming the tree settles the file. Without the first direction a caller
        who passed only ``--runner-metadata`` would be told ``--log-dir`` is missing,
        having already said where it is.
        """
        directory = self.temporary_directory()
        common = [
            "--raw-dir",
            str(directory / "raw"),
            "--allowlist",
            str(directory / "allowlist.txt"),
            "--spark-src",
            str(directory / "spark-src"),
            "--findings-json",
            str(directory / "out" / "findings.json"),
            "--findings-csv",
            str(directory / "out" / "findings.csv"),
        ]

        environ = {"HARNESS_REPO_ROOT": str(directory)}
        metadata_only = cli.resolve_inputs(
            self.namespace(
                *common,
                "--runner-metadata",
                str(directory / "evidence" / DEFAULT_METADATA_FILENAME),
            ),
            environ,
        )
        self.assertEqual(metadata_only.log_dir, directory / "evidence")
        self.assertEqual(
            metadata_only.run_record,
            directory / "evidence" / DEFAULT_RUN_RECORD_FILENAME,
        )

        log_dir_only = cli.resolve_inputs(
            self.namespace(*common, "--log-dir", str(directory / "evidence")), environ
        )
        self.assertEqual(
            log_dir_only.runner_metadata,
            directory / "evidence" / DEFAULT_METADATA_FILENAME,
        )
        self.assertEqual(
            log_dir_only.run_record,
            directory / "evidence" / DEFAULT_RUN_RECORD_FILENAME,
        )

    def test_a_relative_value_is_resolved_to_an_absolute_path(self) -> None:
        """Every resolved input is absolute, and ``~`` is expanded rather than kept.

        The scan root is required absolute by a later stage, and the dataset paths are
        opened for writing: a relative value surviving resolution would put the output
        wherever the process happened to be started from.
        """
        def relative_for(destination: str) -> str:
            """Where each input is stated, relatively, under one prefix.

            ``run_record`` is stated inside the stated log tree because that is the root
            that owns it. Relativeness is what this test is about, and a value the
            resolution would refuse for its location would never reach the absolutising
            step the assertions below are on.
            """
            if destination == "run_record":
                return f"relative/log_dir/{destination}"
            return f"relative/{destination}"

        arguments: list[str] = []
        for flag, destination, _ in CLI_OPTIONS:
            arguments.extend([flag, relative_for(destination)])
        inputs = cli.resolve_inputs(
            self.namespace(*arguments), {"HARNESS_REPO_ROOT": os.getcwd()}
        )

        for _, destination, _ in CLI_OPTIONS:
            with self.subTest(field=destination):
                value = getattr(inputs, destination)
                as_path = Path(value)
                self.assertTrue(
                    as_path.is_absolute(),
                    msg=f"{destination} must be resolved to an absolute path",
                )
                self.assertEqual(
                    as_path, Path(os.path.abspath(relative_for(destination)))
                )

        expanded = cli.resolve_inputs(
            self.namespace(*[part for flag, destination, _ in CLI_OPTIONS
                             for part in (flag, f"~/tilde/{relative_for(destination)}")]),
            {"HARNESS_REPO_ROOT": os.path.expanduser("~")},
        )
        self.assertFalse(
            str(expanded.raw_dir).startswith("~"),
            msg="a leading ~ must be expanded, never carried into a path that is opened",
        )
        self.assertTrue(Path(expanded.raw_dir).is_absolute())

    def test_inputs_is_frozen_and_describes_itself_completely(self) -> None:
        """The resolved inputs are a measurement, so nothing downstream may edit them.

        ``Inputs`` reaches every stage and is serialised into the run record. A mutable
        instance would let one stage change where a later stage writes, and the record
        would then describe the original value rather than the one used.
        """
        directory = self.temporary_directory()
        inputs = self.dummy_inputs(directory)

        self.assertTrue(dataclasses.is_dataclass(inputs))
        option_fields = tuple(destination for _, destination, _ in CLI_OPTIONS)
        field_names = tuple(field.name for field in dataclasses.fields(cli.Inputs))
        self.assertEqual(
            field_names[: len(option_fields)],
            option_fields,
            msg="the eight option-borne fields come first, in the option order",
        )
        self.assertEqual(
            field_names[len(option_fields) :],
            ("output_guards",),
            msg=(
                "the one field that is not an option is the output-guard record: the "
                "owner root each output path was checked against, and the component "
                "checks that established it, measured once during resolution so the run "
                "record states the guard that actually ran"
            ),
        )
        with self.assertRaises(dataclasses.FrozenInstanceError):
            inputs.raw_dir = directory  # type: ignore[misc]

        as_dict = inputs.as_dict()
        self.assertEqual(
            sorted(as_dict),
            sorted(field.name for field in dataclasses.fields(cli.Inputs)),
        )
        for field, value in as_dict.items():
            with self.subTest(field=field):
                if field == "output_guards":
                    self.assertIsInstance(
                        value,
                        Mapping,
                        msg="the guard record serialises as a mapping, not a path",
                    )
                    continue
                self.assertIsInstance(
                    value,
                    str,
                    msg="every path value must serialise as a string for the run record",
                )
                self.assertTrue(Path(value).is_absolute())
        self.assertEqual(json.loads(json.dumps(as_dict)), as_dict)


# --------------------------------------------------------------------------------------
# The interpreter record: measured, compared, and never a verdict
# --------------------------------------------------------------------------------------


class CliInterpreterRecordTests(CliTestCase):
    """``cli.interpreter_record`` describes the interpreter that is actually running.

    Two failure modes, both silent. A record carrying the *expected* version rather than
    the observed one would make every run look compliant, and the comparison AAP 0.4.1
    requires would then be a restatement of the constant. And a comparison that halted
    on a difference would stop the run over a patch release, which AAP 0.4.1 explicitly
    records and continues past.
    """

    def test_the_record_describes_the_running_interpreter_not_a_literal(self) -> None:
        """Executable and version come from this process, established independently.

        The version is compared against ``platform.python_version()`` *and* against
        ``sys.version_info``, two readings that cannot both be satisfied by a literal
        unless the running interpreter genuinely is that version.
        """
        record = cli.interpreter_record()
        self.assertEqual(record["executable"], sys.executable)
        self.assertTrue(Path(record["executable"]).is_absolute())
        self.assertTrue(Path(record["executable"]).exists())

        from_version_info = ".".join(str(part) for part in sys.version_info[:3])
        self.assertEqual(record["observed_version"], platform.python_version())
        self.assertEqual(record["observed_version"], from_version_info)
        self.assertEqual(record["implementation"], platform.python_implementation())
        self.assertNotIn("\n", record["version_string"])
        self.assertEqual(json.loads(json.dumps(record)), record)

    def test_the_comparison_classifies_the_difference_and_never_halts(self) -> None:
        """The label is derived from the two versions, and the run continues either way.

        The expected classification is computed here from the observed and expected
        strings, so this holds on an interpreter that matches and on one that does not:
        an assertion written for one of those two cases would silently stop meaning
        anything on the other.
        """
        record = cli.interpreter_record()
        observed = record["observed_version"]
        expected = record["expected_version"]
        self.assertEqual(expected, cli.EXPECTED_INTERPRETER_VERSION)

        observed_parts = observed.split(".")
        expected_parts = expected.split(".")
        if observed == expected:
            wanted = "matches"
        elif observed_parts[:1] != expected_parts[:1]:
            wanted = "major_difference"
        elif observed_parts[:2] != expected_parts[:2]:
            wanted = "minor_difference"
        else:
            wanted = "patch_difference"

        self.assertEqual(record["comparison"], wanted)
        self.assertIs(record["version_matches_expected"], observed == expected)
        self.assertIs(
            record["halts_on_difference"],
            False,
            msg=(
                "an interpreter difference of any kind is recorded with both values and "
                "the run continues (AAP 0.4.1); it is never a halt"
            ),
        )


# --------------------------------------------------------------------------------------
# Key to module: the half of routing only cli.py can answer
# --------------------------------------------------------------------------------------


class CliAdapterResolutionTests(CliTestCase):
    """``cli.resolve_adapter`` turns a routing key into the module that answers to it.

    The registry here is the inverse of ``shape.py``'s deliberate ignorance of the
    adapter layer, and three things can go wrong in it. A key could map to the wrong
    module, which would adapt an artifact with another tool's reader and produce rows
    nobody could attribute. A key could map to nothing, which for the conditional
    ``osv_scanner`` module is the *expected* state of this run and must fail by naming
    the missing module rather than by falling into the generic unknown-shape halt --
    that would stop the run for a tool doing exactly what it was configured to do. Or a
    module could exist without the entry point every adapter presents.
    """

    def routing_decision(self, name: str):
        """Route one artifact fixture the way ``cli`` routes a raw-tree artifact."""
        path = FIXTURES_DIR / name
        if not path.is_file():
            self.fail(f"blocking gap: required fixture {name!r} is absent")
        document = json.loads(path.read_text(encoding="utf-8"))
        return shape.route_artifact(path.relative_to(REPO_ROOT).as_posix(), document)

    def test_every_artifact_fixture_resolves_to_the_module_its_key_names(self) -> None:
        """Eight artifacts, eight resolutions, each the module the key names.

        The module's own ``__name__`` is compared with the importable name the decision
        advertises, so a registry entry pointing at the wrong module is caught rather
        than merely a missing one.
        """
        exercised = SARIF_ARTIFACT_FIXTURES + NATIVE_ARTIFACT_FIXTURES
        self.assertEqual(
            len(exercised),
            8,
            msg="eight of the nine tools wrote an artifact this run",
        )
        for name in exercised:
            with self.subTest(artifact=name):
                decision = self.routing_decision(name)
                module = cli.resolve_adapter(decision)
                self.assertIsInstance(module, types.ModuleType)
                self.assertEqual(module.__name__, decision.adapter_module_name)
                self.assertIs(module, cli.ADAPTER_REGISTRY[decision.adapter])
                self.assertTrue(
                    callable(getattr(module, "adapt", None)),
                    msg="every adapter presents the same callable entry point",
                )

    def test_the_three_sarif_producers_resolve_to_one_shared_module_object(self) -> None:
        """One key, one module object -- not three copies that could diverge.

        Identity rather than equality: three separately imported modules would compare
        equal by name while carrying independent state, and the point of the shared
        adapter is that all three producers are read by the same code.
        """
        modules = {
            name: cli.resolve_adapter(self.routing_decision(name))
            for name in SARIF_ARTIFACT_FIXTURES
        }
        self.assertEqual(len(modules), 3)
        first = next(iter(modules.values()))
        for name, module in modules.items():
            with self.subTest(artifact=name):
                self.assertIs(module, first)
        self.assertEqual(first.__name__, f"{shape.ADAPTER_PACKAGE}.{shape.SHARED_SARIF_ADAPTER}")

    def test_the_registry_is_immutable_and_holds_no_conditional_module(self) -> None:
        """Six unconditional entries, immutable, with the conditional one deliberately out.

        A mutable registry would let one artifact's processing rebind another's adapter.
        ``osv_scanner`` is absent from it on purpose: its presence there would make the
        import unconditional and the module a hard dependency of every run.
        """
        self.assertIsInstance(cli.ADAPTER_REGISTRY, types.MappingProxyType)
        with self.assertRaises(TypeError):
            cli.ADAPTER_REGISTRY["sarif"] = None  # type: ignore[index]
        self.assertEqual(
            sorted(cli.ADAPTER_REGISTRY),
            ["checkov", "dependency_check", "gitleaks", "joern", "sarif", "trivy"],
        )
        self.assertEqual(cli.CONDITIONAL_ADAPTER_MODULES, ("osv_scanner",))
        for key in cli.CONDITIONAL_ADAPTER_MODULES:
            with self.subTest(conditional=key):
                self.assertNotIn(key, cli.ADAPTER_REGISTRY)

    def test_the_guarded_conditional_import_names_the_absent_module(self) -> None:
        """The OSV adapter is absent this run, so its key must fail by naming it.

        The exception hierarchy is asserted with ``issubclass`` and ``isinstance``
        rather than by name, because the hierarchy is what decides the exit code: a
        :class:`cli.MissingAdapterModule` is a :class:`cli.ConfigurationFault`, hence a
        :class:`cli.NormalizeHalt`, hence exit 78 -- a fault to correct rather than a
        scanning outcome to classify. A sibling class defined outside that chain would
        exit 1 and read as a condition in the data.
        """
        self.assertTrue(issubclass(cli.MissingAdapterModule, cli.ConfigurationFault))
        self.assertTrue(issubclass(cli.ConfigurationFault, cli.NormalizeHalt))
        self.assertTrue(issubclass(cli.NormalizeHalt, Exception))
        self.assertFalse(issubclass(cli.NormalizeHalt, shape.UnknownArtifactShape))

        source = REPO_ROOT / CONDITIONAL_ADAPTER_SOURCE
        self.assertFalse(
            source.exists(),
            msg=(
                f"{CONDITIONAL_ADAPTER_SOURCE} is expected absent this run -- the tool "
                "wrote no artifact -- and its absence is what makes the guarded import "
                "a live branch. Its presence would mean this assertion is exercising "
                "nothing."
            ),
        )

        artifact = f"harness/artifacts/raw/{shape.artifact_filename_for(TOOL_WITHOUT_A_FIXTURE)}"
        decision = shape.RoutingDecision(
            tool=TOOL_WITHOUT_A_FIXTURE,
            shape=shape.SHAPE_NATIVE,
            adapter=shape.adapter_module_for(TOOL_WITHOUT_A_FIXTURE),
            scanner_class=shape.scanner_class_for(TOOL_WITHOUT_A_FIXTURE),
            artifact_path=artifact,
        )
        with self.assertRaises(cli.MissingAdapterModule) as raised:
            cli.resolve_adapter(decision)
        fault = self.assertHalt(
            raised.exception,
            reason=cli.HALT_MISSING_ADAPTER_MODULE,
            exit_code=cli.EXIT_CONFIG,
            expected_class=cli.MissingAdapterModule,
        )
        self.assertIsInstance(fault, cli.ConfigurationFault)
        self.assertIsInstance(fault, cli.NormalizeHalt)
        self.assertNotIsInstance(fault, shape.UnknownArtifactShape)

        self.assertEqual(fault.details["tool"], TOOL_WITHOUT_A_FIXTURE)
        self.assertEqual(fault.details["adapter_module"], decision.adapter_module_name)
        self.assertEqual(fault.details["expected_file"], CONDITIONAL_ADAPTER_SOURCE)
        self.assertEqual(fault.details["artifact_path"], artifact)
        self.assertIn("ImportError", fault.details["import_error"])
        self.assertIn(decision.adapter_module_name, fault.message)
        self.assertIn(CONDITIONAL_ADAPTER_SOURCE, fault.message)
        self.assertIn(
            "not an unknown artifact shape",
            fault.message,
            msg=(
                "the diagnostic must draw the distinction the class exists for: a "
                "missing module, not a document nobody could classify"
            ),
        )

    def test_a_key_in_no_table_at_all_is_a_configuration_fault(self) -> None:
        """A key that is neither registered nor conditional names both tables.

        Driven with a capability fake carrying exactly the four attributes
        ``resolve_adapter`` reads, because ``shape.RoutingDecision`` validates its
        adapter against the routing table and therefore cannot express this fault. The
        fake asserts nothing about its own values: the assertion is on the fault the
        production code raises.
        """
        fake = types.SimpleNamespace(
            adapter="adapter_that_was_never_registered",
            tool="joern",
            artifact_path="harness/artifacts/raw/joern.json",
            adapter_module_name=f"{shape.ADAPTER_PACKAGE}.adapter_that_was_never_registered",
        )
        with self.assertRaises(cli.ConfigurationFault) as raised:
            cli.resolve_adapter(fake)
        fault = self.assertHalt(
            raised.exception,
            reason=cli.HALT_MISSING_ADAPTER_MODULE,
            exit_code=cli.EXIT_CONFIG,
            expected_class=cli.ConfigurationFault,
        )
        self.assertNotIsInstance(
            fault,
            cli.MissingAdapterModule,
            msg=(
                "an unregistered key is a programming fault in the registry, not the "
                "conditional-module case, and the two must stay distinguishable"
            ),
        )
        self.assertEqual(fault.details["adapter"], fake.adapter)
        self.assertEqual(fault.details["registered"], sorted(cli.ADAPTER_REGISTRY))
        self.assertEqual(
            fault.details["conditional"], list(cli.CONDITIONAL_ADAPTER_MODULES)
        )

    def test_a_module_without_a_callable_entry_point_is_refused(self) -> None:
        """A registered module missing ``adapt`` fails at resolution, not mid-artifact.

        Reached by patching the registry for the duration of this assertion, since every
        authored adapter presents the entry point. Without this branch the failure would
        surface as an ``AttributeError`` inside the per-artifact loop, where it would be
        caught as an adapter contract fault and misreported as the artifact's problem.
        """
        stub = types.ModuleType(f"{shape.ADAPTER_PACKAGE}.gitleaks")
        stub.adapt = "not callable"  # type: ignore[attr-defined]
        decision = self.routing_decision("gitleaks.json")
        with unittest.mock.patch.object(
            cli, "ADAPTER_REGISTRY", types.MappingProxyType({decision.adapter: stub})
        ):
            with self.assertRaises(cli.ConfigurationFault) as raised:
                cli.resolve_adapter(decision)
        fault = self.assertHalt(
            raised.exception,
            reason=cli.HALT_MISSING_ADAPTER_MODULE,
            exit_code=cli.EXIT_CONFIG,
            expected_class=cli.ConfigurationFault,
        )
        self.assertIn("exposes no callable 'adapt'", fault.message)
        self.assertEqual(fault.details["adapter_module"], decision.adapter_module_name)
        self.assertIs(
            cli.ADAPTER_REGISTRY[decision.adapter],
            cli.resolve_adapter(decision),
            msg="the patch must be undone: the real registry answers again afterwards",
        )


# --------------------------------------------------------------------------------------
# The vocabulary gate the composition settles before it reads anything
# --------------------------------------------------------------------------------------


class CliVocabularyGateTests(CliTestCase):
    """``cli._verify_vocabularies`` settles the shared vocabularies first.

    Four modules each author the nine canonical identifiers and ``emit`` authors the
    twelve fields. A disagreement between them corrupts counts in a way no later
    assertion can locate: a tool missing from one tuple would be excluded from one
    document while appearing in another, and the reconciliation identity would fail with
    nothing to point at. So it is settled before any artifact is read, and every fault
    it detects is a :class:`cli.ConfigurationFault` -- a programming fault to correct,
    not a condition in a scanner's output.
    """

    def test_the_gate_passes_over_the_authored_modules_and_records_them(self) -> None:
        """It passes, and records each vocabulary from the module that authors it."""
        record: dict = {}
        cli._verify_vocabularies(record)
        vocabularies = record["vocabularies"]

        self.assertEqual(
            vocabularies["canonical_tools"],
            {
                "shape": list(shape.CANONICAL_TOOLS),
                "paths": list(paths.CANONICAL_TOOLS),
                "reconcile": list(reconcile.CANONICAL_TOOLS),
                "severity": list(severity.CANONICAL_TOOLS),
            },
        )
        self.assertEqual(vocabularies["processing_order"], list(shape.CANONICAL_TOOLS))
        self.assertEqual(
            vocabularies["processing_order_source"], "normalize.shape.CANONICAL_TOOLS"
        )
        self.assertEqual(vocabularies["fields"], list(emit.FIELDS))
        self.assertEqual(len(vocabularies["fields"]), 12)
        self.assertEqual(vocabularies["optional_fields"], sorted(emit.OPTIONAL_FIELDS))
        self.assertEqual(
            vocabularies["artifact_filenames"], list(shape.ARTIFACT_FILENAMES)
        )
        self.assertEqual(vocabularies["reject_classes"], list(paths.REJECT_CLASSES))
        self.assertEqual(vocabularies["parse_statuses"], list(cli.PARSE_STATUSES))
        self.assertEqual(json.loads(json.dumps(vocabularies)), vocabularies)

    def test_a_disagreeing_canonical_tool_vocabulary_halts(self) -> None:
        """One module short of a tool stops the run, with all four tuples quoted.

        Patched one module at a time, because a disagreement is a property of the set
        and an implementation comparing only two of the four would pass for the pair it
        happened to compare.
        """
        for module in (shape, paths, reconcile, severity):
            with self.subTest(module=module.__name__):
                shortened = tuple(module.CANONICAL_TOOLS)[:-1]
                with unittest.mock.patch.object(
                    module, "CANONICAL_TOOLS", shortened
                ):
                    with self.assertRaises(cli.ConfigurationFault) as raised:
                        cli._verify_vocabularies({})
                fault = self.assertHalt(
                    raised.exception,
                    reason=cli.HALT_VOCABULARY_MISMATCH,
                    exit_code=cli.EXIT_CONFIG,
                    expected_class=cli.ConfigurationFault,
                )
                self.assertEqual(
                    sorted(fault.details["vocabularies"]),
                    ["paths", "reconcile", "severity", "shape"],
                    msg="the fault must quote every vocabulary, not only the odd one",
                )

    def test_a_field_list_that_is_not_the_twelve_fields_halts(self) -> None:
        """Twelve fields, first ``tool``, last ``in_scope`` -- each half asserted.

        The three mutations are the three ways the schema can break while still looking
        like a field list: one too few, a different first field, and a different last
        field. All three are the same halt reason, and all three are reached.
        """
        authored = tuple(emit.FIELDS)
        mutations = {
            "one_field_short": authored[:-1],
            "reordered_first": (authored[1], authored[0], *authored[2:]),
            "reordered_last": (*authored[:-2], authored[-1], authored[-2]),
        }
        for label, fields in mutations.items():
            with self.subTest(mutation=label):
                with unittest.mock.patch.object(emit, "FIELDS", fields):
                    with self.assertRaises(cli.ConfigurationFault) as raised:
                        cli._verify_vocabularies({})
                fault = self.assertHalt(
                    raised.exception,
                    reason=cli.HALT_VOCABULARY_MISMATCH,
                    exit_code=cli.EXIT_CONFIG,
                    expected_class=cli.ConfigurationFault,
                )
                self.assertEqual(fault.details["fields"], list(fields))

    def test_the_optional_fields_must_be_a_proper_subset_of_the_fields(self) -> None:
        """Absence is permitted for five fields, and never for a field not in the schema.

        Equality rather than proper containment would let ``path`` or ``severity_norm``
        become optional, and the absence convention would then admit a row the schema
        does not carry.
        """
        with unittest.mock.patch.object(
            emit, "OPTIONAL_FIELDS", frozenset(emit.FIELDS)
        ):
            with self.assertRaises(cli.ConfigurationFault) as raised:
                cli._verify_vocabularies({})
        fault = self.assertHalt(
            raised.exception,
            reason=cli.HALT_VOCABULARY_MISMATCH,
            exit_code=cli.EXIT_CONFIG,
            expected_class=cli.ConfigurationFault,
        )
        self.assertEqual(fault.details["optional_fields"], sorted(emit.FIELDS))

    def test_one_artifact_filename_per_canonical_tool_is_required(self) -> None:
        """Nine identifiers need nine filenames; a short table hides a tool entirely.

        A tool whose filename were missing would never be looked for in the raw tree,
        so its artifact would go unread and its absence unrecorded -- which reads
        exactly like a tool that found nothing.
        """
        with unittest.mock.patch.object(
            shape, "ARTIFACT_FILENAMES", tuple(shape.ARTIFACT_FILENAMES)[:-1]
        ):
            with self.assertRaises(cli.ConfigurationFault) as raised:
                cli._verify_vocabularies({})
        fault = self.assertHalt(
            raised.exception,
            reason=cli.HALT_VOCABULARY_MISMATCH,
            exit_code=cli.EXIT_CONFIG,
            expected_class=cli.ConfigurationFault,
        )
        self.assertEqual(
            len(fault.details["artifact_filenames"]), len(shape.CANONICAL_TOOLS) - 1
        )


# --------------------------------------------------------------------------------------
# Which artifacts are routed at all: the raw-tree boundary
# --------------------------------------------------------------------------------------


class CliRawDirectoryBoundaryTests(CliTestCase):
    """``cli._enumerate_raw_directory`` bounds routing to the nine runner artifacts.

    ``harness/artifacts/raw/`` is runner-only, and the two deliberate second appearances
    in the run write outside it: the Opengrep taint A/B arms are valid SARIF under
    ``harness/artifacts/logs/`` and the Joern probe results sit under
    ``queries/joern/results/``. Both would route perfectly, which is exactly why the
    boundary is enforced rather than trusted -- a file reached from outside would add
    rows to a tool's count and to the dataset total, corrupting both.
    """

    def raw_tree(self, *artifacts: str) -> tuple[Path, Path]:
        """Copy the named committed fixtures into a fresh temporary raw tree."""
        directory = self.temporary_directory()
        raw_dir = directory / "raw"
        raw_dir.mkdir(parents=True)
        for name in artifacts:
            source = FIXTURES_DIR / name
            if not source.is_file():
                self.fail(f"blocking gap: required fixture {name!r} is absent")
            shutil.copyfile(source, raw_dir / name)
        return directory, raw_dir

    def test_only_the_nine_fixed_filenames_are_read_and_the_rest_reported(self) -> None:
        """Two artifacts are read; a stray file and a mis-typed entry are reported.

        A reported condition rather than a halt: AAP 0.8.1 has this module report an
        unexpected entry and never fingerprint a document to guess its writer. The
        directory named ``trivy.json`` is the second, distinct condition -- an expected
        artifact *name* that is not a regular file -- and it must not be mistaken for a
        present artifact.
        """
        directory, raw_dir = self.raw_tree("gitleaks.json", "checkov.json")
        (raw_dir / "stray-note.txt").write_text("not a runner artifact\n", encoding="utf-8")
        (raw_dir / "trivy.json").mkdir()

        record: dict = {}
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            present = cli._enumerate_raw_directory(
                self.dummy_inputs(directory, raw_dir=raw_dir), record
            )

        self.assertEqual(sorted(present), ["checkov", "gitleaks"])
        for tool, path in present.items():
            with self.subTest(tool=tool):
                self.assertEqual(path, raw_dir / shape.artifact_filename_for(tool))
                self.assertTrue(path.is_file())

        raw_record = record["raw_directory"]
        self.assertEqual(raw_record["artifacts_present"], ["gitleaks", "checkov"])
        self.assertEqual(
            raw_record["artifacts_absent"],
            [tool for tool in shape.CANONICAL_TOOLS if tool not in present],
        )
        self.assertEqual(raw_record["unexpected_entry_count"], 2)
        conditions = {
            entry["name"]: entry["condition"] for entry in raw_record["unexpected_entries"]
        }
        self.assertEqual(
            conditions["stray-note.txt"],
            "a name that is not one of the nine runner artifacts",
        )
        self.assertEqual(
            conditions["trivy.json"],
            "an expected artifact name that is not a regular file",
        )
        self.assertEqual(
            raw_record["expected_artifact_filenames"], list(shape.ARTIFACT_FILENAMES)
        )
        self.assertIn("reported condition", stderr.getvalue())
        self.assertIn("stray-note.txt", stderr.getvalue())
        self.assertEqual(json.loads(json.dumps(raw_record)), raw_record)

    def test_a_missing_raw_directory_is_a_configuration_fault(self) -> None:
        """The tree is provisioned, not created here, so its absence is a fault to fix."""
        directory = self.temporary_directory()
        with self.assertRaises(cli.ConfigurationFault) as raised:
            cli._enumerate_raw_directory(
                self.dummy_inputs(directory, raw_dir=directory / "absent-raw"), {}
            )
        fault = self.assertHalt(
            raised.exception,
            reason=cli.HALT_RAW_DIRECTORY_MISSING,
            exit_code=cli.EXIT_CONFIG,
            expected_class=cli.ConfigurationFault,
        )
        self.assertEqual(fault.details["raw_dir"], str(directory / "absent-raw"))
        self.assertIn("neither creates nor clears it", fault.message)

    def test_an_artifact_symlinked_out_of_the_raw_tree_halts(self) -> None:
        """A symlink reaching a readable document outside the tree stops the run.

        The target here is valid SARIF, exactly like a taint A/B arm: it would route and
        adapt without complaint. The halt is a condition in the data rather than a
        configuration fault, so it exits 1, and the two must stay distinguishable
        because a reader of an exit code acts differently on each.
        """
        directory, raw_dir = self.raw_tree("gitleaks.json")
        outside = directory / "taint-ab-on.sarif"
        outside.write_text(
            json.dumps({"version": "2.1.0", "runs": [{"results": []}]}), encoding="utf-8"
        )
        os.symlink(outside, raw_dir / "opengrep.sarif")

        with self.assertRaises(cli.NormalizeHalt) as raised:
            cli._enumerate_raw_directory(
                self.dummy_inputs(directory, raw_dir=raw_dir), {}
            )
        halt = self.assertHalt(
            raised.exception,
            reason=cli.HALT_RAW_DIRECTORY_BOUNDARY,
            exit_code=cli.EXIT_HALT,
        )
        self.assertNotIsInstance(
            halt,
            cli.ConfigurationFault,
            msg="a document reached from outside the tree is a data halt, exit 1",
        )
        self.assertEqual(halt.details["tool"], "opengrep")
        self.assertEqual(halt.details["resolved_path"], os.path.realpath(outside))
        self.assertEqual(halt.details["raw_dir"], os.path.realpath(raw_dir))


# --------------------------------------------------------------------------------------
# Two record labels the composer derives rather than copies
# --------------------------------------------------------------------------------------


class CliRecordLabelTests(CliTestCase):
    """``cli._scanner_class_label`` and ``cli._counter_summary`` render, never invent.

    Both turn a measurement into a record field, and both have a failure mode that
    would pass unnoticed. Trivy's class is decided per record from the section a record
    came from, so the run record needs a label for it that no dataset row may carry; and
    a counter an adapter never authors must record ``null`` rather than ``0``, because
    "this adapter does not count that" and "it counted none" are different statements
    and only one of them is a measurement.
    """

    def test_the_per_record_label_is_used_for_trivy_and_for_no_other_tool(self) -> None:
        """Eight fixed classes, and one sentinel rendered as a label for the record."""
        self.assertEqual(cli._scanner_class_label("trivy"), shape.PER_RECORD_LABEL)
        self.assertNotIn(
            shape.PER_RECORD_LABEL,
            shape.SCANNER_CLASSES,
            msg=(
                "the per-record label is a rendering for the run record, never one of "
                "the classes a dataset row carries"
            ),
        )
        for tool in shape.CANONICAL_TOOLS:
            if tool == "trivy":
                continue
            with self.subTest(tool=tool):
                label = cli._scanner_class_label(tool)
                self.assertEqual(label, shape.scanner_class_for(tool))
                self.assertIn(label, shape.SCANNER_CLASSES)
                self.assertNotEqual(label, shape.PER_RECORD_LABEL)

    def test_a_counter_the_adapter_never_authors_records_null_not_zero(self) -> None:
        """One authored counter is carried; the five unauthored ones are ``null`` and named."""
        summary = cli._counter_summary({"multi_location_records": 2, "rows_in_scope": 0})
        self.assertEqual(summary["multi_location_records"], 2)
        self.assertEqual(summary["rows_in_scope"], 0)
        for key in (
            "multi_valued_cwe_records",
            "multi_valued_cve_records",
            "non_filesystem_paths",
            "rows_out_of_scope",
        ):
            with self.subTest(counter=key):
                self.assertIsNone(
                    summary[key],
                    msg=(
                        f"{key} is not authored by this adapter, so the record must say "
                        "so rather than report a measurement of zero"
                    ),
                )
                self.assertIn(key, summary["counters_not_defined_by_adapter"])
        self.assertNotIn(
            "rows_in_scope",
            summary["counters_not_defined_by_adapter"],
            msg="a counter authored with the value 0 is a measurement, not an absence",
        )

    def test_the_path_kind_tally_is_counted_through_the_closed_discriminator(self) -> None:
        """Path kinds are tallied through ``paths.PathKindTally``, so no kind can drift.

        Counted through the discriminator rather than beside it: the tally validates
        every kind against the closed set, so a private counter cannot introduce a kind
        ``paths.py`` does not know about, and the non-filesystem proportion the run
        record publishes is computed from the same one measurement.
        """
        counters = {
            f"{cli._PATH_KIND_COUNTER_PREFIX}{paths.PATH_KIND_TREE_FILE}": 3,
            f"{cli._PATH_KIND_COUNTER_PREFIX}{paths.PATH_KIND_ARCHIVE_MEMBER}": 1,
            "rows_in_scope": 99,
        }
        tally = cli._path_kind_tally(counters)
        self.assertIsInstance(tally, paths.PathKindTally)
        self.assertEqual(tally.total, 4, msg="the non-kind counter must be ignored")
        self.assertEqual(tally.non_filesystem, 1)
        self.assertAlmostEqual(tally.non_filesystem_proportion, 0.25)

        with self.assertRaises(paths.PathPolicyError):
            cli._path_kind_tally(
                {f"{cli._PATH_KIND_COUNTER_PREFIX}not_a_path_kind": 1}
            )
        self.assertEqual(
            tally.as_dict()["by_kind"][paths.PATH_KIND_ARCHIVE_MEMBER],
            1,
            msg="the record carries the tally the discriminator produced, unchanged",
        )


if __name__ == "__main__":  # pragma: no cover - exercised through unittest discovery
    unittest.main(verbosity=2)

