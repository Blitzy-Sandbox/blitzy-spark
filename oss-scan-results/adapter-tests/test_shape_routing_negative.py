"""The mandated negative test of the normalizer's shape detection and routing.

Requirement (AAP 0.5.4, verbatim): the mandated negative test *"asserts the direction
that actually goes wrong -- that a native artifact does **not** route to the SARIF
adapter -- because a permissive detector that accepts a native file as SARIF produces an
empty result set rather than an error, and an empty result set is indistinguishable from
a clean scan."* AAP 0.6.1 gives this file its own row, AAP 0.9.4 lists it in the
definition of done, and AAP 0.9.2 makes a failure here a condition that stops the run.

Why a dedicated test rather than a code review
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

No user-specified rules govern this file: ``review_rules`` reports "No user rules
provided." and that one line is the whole document, corroborated independently by AAP
0.7 and 0.10.2. Enterprise-standard best practice applies in their place and their
absence is expressly not licence to lower the bar -- concretely, every assertion below
names the exact key or the exact exception type rather than settling for something
truthy, and no negative assertion is softened into a smoke test.
"""

from __future__ import annotations

# Standard library only, and only these five (AAP 0.4.1):
#   json     -- parse a fixture without mutating it;
#   sys      -- the sys.path bootstrap, and the sys.modules snapshot proving routing
#               imports no adapter;
#   types    -- ModuleType, so "a string key, not a module" is asserted against the
#               actual module type rather than against a stand-in for it;
#   unittest -- the runner, so the suite needs no third-party plugin;
#   pathlib  -- fixture locations derived from __file__, so nothing depends on the
#               working directory the runner happened to start in.
import json
import sys
import types
import unittest
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

from normalize import shape  # noqa: E402  (import follows the bootstrap by necessity)

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

#: The tool expected to write no artifact at all, so no fixture bears its name.
#: OSV-Scanner exits 128 with "No package sources found" over a scope holding zero
#: resolvable dependency manifests (AAP 0.2.1): the one manifest-shaped file in scope,
#: core/src/main/resources/org/apache/spark/ui/static/package.json, carries a name, a
#: license and a type and no dependencies block. Its adapter key is still asserted
#: present -- see :class:`AdapterTableCompletenessTests` -- because the key's absence,
#: not the artifact's, is what would turn a legitimately written artifact into a halt.
TOOL_WITHOUT_A_FIXTURE = "osv-scanner"


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

    The five native artifacts, Checkov's alternative top-level form, and both near-miss
    documents. ``checkov-alt-shape.json`` is included when present and its presence is
    asserted in :class:`FixtureInventoryTests`, so the inclusion is visible rather than
    conditional in effect.
    """
    names = list(NATIVE_ARTIFACT_FIXTURES)
    if (FIXTURES_DIR / CHECKOV_ALT_SHAPE_FIXTURE).is_file():
        names.append(CHECKOV_ALT_SHAPE_FIXTURE)
    names.extend(NEAR_MISS_FIXTURES)
    names.append(UNKNOWN_SHAPE_FIXTURE)
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
        """Both near-miss documents and the fall-through document exist.

        The near-miss pair is what pins the conjunction and the fall-through is what
        exercises the halt. Losing any one of the three would leave a whole branch of
        the contract unasserted while the suite still reported green.
        """
        for name in (*NEAR_MISS_FIXTURES, UNKNOWN_SHAPE_FIXTURE):
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
                    [str(key) for key in evidence["top_level_keys"]],
                    msg=(
                        f"the expected file for {name!r} records top-level keys that are "
                        "not the fixture's, in order"
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

    One consequence has to be stated rather than glossed, because a reader checking these
    assertions will meet it: the three SARIF producers' entry in the native routing table
    *is* the shared SARIF adapter, so a native document presented under one of their
    names routes to ``sarif`` by the table while its shape is decided ``native`` by the
    content. The shape decision is therefore the invariant these tests pin for that
    direction, and the adapter is pinned in the other direction, where the table and the
    content genuinely disagree.
    """

    def test_a_native_document_under_a_sarif_artifact_name_is_decided_native(self) -> None:
        """A ``.sarif``-named artifact carrying a native document is decided native.

        The filename claims SARIF and the extension agrees with it; the document does
        not. The decision follows the document. Were it to follow the name, the shared
        adapter would walk a document that has no ``runs`` at all and report zero rows.
        """
        for native_name in NATIVE_ARTIFACT_FIXTURES:
            document = self.load_fixture(native_name)
            for claimed_name in SARIF_ARTIFACT_FIXTURES:
                with self.subTest(document=native_name, claimed=claimed_name):
                    decision = shape.route(claimed_name, document)
                    self.assertEqual(
                        decision.shape,
                        shape.SHAPE_NATIVE,
                        msg=(
                            f"the document from {native_name!r} presented under "
                            f"{claimed_name!r} must be decided "
                            f"{shape.SHAPE_NATIVE!r}: the shape follows the document, "
                            "and a .sarif extension is a naming convention rather than "
                            "a shape test"
                        ),
                    )
                    self.assertIs(decision.is_sarif_shape, False)
                    self.assertEqual(
                        decision.tool,
                        shape.resolve_tool(claimed_name),
                        msg=(
                            "the writing runner is identified by the artifact name, "
                            "which is the half of the decision that does follow the name"
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
        """A near-miss document under a real producer name is native, never SARIF.

        The fixtures halt under their own names, so this is the only way to establish
        what the *shape* decision would be for them: presented under a recognised name
        they route rather than halt, and they still fail the conjunction. Without this,
        a detector could relax the conjunction and be caught only by the halt -- which
        the unrecognised name would have produced anyway.
        """
        for name in NEAR_MISS_FIXTURES:
            document = self.load_fixture(name)
            for claimed_name in SARIF_ARTIFACT_FIXTURES:
                with self.subTest(document=name, claimed=claimed_name):
                    decision = shape.route(claimed_name, document)
                    self.assertEqual(
                        decision.shape,
                        shape.SHAPE_NATIVE,
                        msg=(
                            f"the near-miss document from {name!r} must never be decided "
                            f"{shape.SHAPE_SARIF!r}, under any name"
                        ),
                    )
                    self.assertIs(decision.is_sarif_shape, False)

    def test_a_native_document_under_another_native_name_keeps_the_named_adapter(self) -> None:
        """A native document routes by the name that wrote it, not by its content.

        The complement of the two tests above, and the reason routing is keyed by the
        writer at all: native shapes are not distinguishable from one another by a safe
        test, so guessing a tool from content would be a fingerprint rather than a fact.
        The consequence is deliberate -- a mis-named native artifact reaches the adapter
        its *name* claims -- and the raw artifact tree is what makes that safe: it holds
        one artifact per tool that writes one and nothing else ever.
        """
        first, second = NATIVE_ARTIFACT_FIXTURES[0], NATIVE_ARTIFACT_FIXTURES[1]
        document = self.load_fixture(first)
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


if __name__ == "__main__":  # pragma: no cover - exercised through unittest discovery
    unittest.main(verbosity=2)

