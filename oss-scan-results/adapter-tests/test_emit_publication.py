"""How the dataset reaches the disk: publication safety, atomicity and CSV neutralisation.

The other test modules in this directory assert what the rows *say*. This one asserts
what happens to them on the way out, which is a different set of failures and a set that
none of the adapter tests can reach:

* a write redirected through a planted symlink, or raced by a concurrent writer, so the
  deliverable is not at the path the run record names (CWE-59, CWE-367);
* a publication that fails between its two members, leaving this run's ``findings.json``
  beside the previous run's ``findings.csv`` — two files that look like one dataset and
  are not;
* scanner-controlled text reaching ``findings.csv`` as a live spreadsheet formula
  (CWE-1236), and the neutralisation that prevents it silently changing what the dataset
  says a tool reported;
* a run record that claims to carry every named file's size and digest while sixteen of
  its entries carry a size and a null digest.

Each of those is invisible in a passing run and visible only in a test that arranges the
adverse condition on purpose, which is what this file does.

What is asserted, and where
---------------------------
1.  the write protocol is reported as data ........ :class:`StagingProtocolTests`
2.  a symlinked directory component is refused .... :class:`SecureStagingTests`
3.  a symlink at the target is refused ............ :class:`SecureStagingTests`
4.  a pre-planted staging name cannot be written .. :class:`SecureStagingTests`
5.  no staged or partial file survives, ever ...... :class:`SecureStagingTests`
6.  a published member is not group/world writable :class:`SecureStagingTests`
7.  both members share one publication identifier . :class:`AtomicPublicationTests`
8.  the identifier is recomputable and content-derived :class:`AtomicPublicationTests`
9.  a fault mid-publication publishes neither ..... :class:`AtomicPublicationTests`
10. a disagreeing pair publishes neither .......... :class:`AtomicPublicationTests`
11. the neutralisation is exactly reversible ...... :class:`CsvNeutralisationTests`
12. the schema survives it unchanged ............. :class:`CsvNeutralisationTests`
13. the real 9,430-row dataset round-trips ....... :class:`CommittedDatasetTests`
14. the run record is published under the same protocol :class:`RunRecordPublicationTests`
15. every named file the record carries is measured :class:`RunRecordMeasurementTests`
16. every published file is 0o644, verified after promotion :class:`PublishedFileModeTests`

How to run it
-------------
From the repository root::

    python3 -m unittest discover -s oss-scan-results/adapter-tests \\
        -p 'test_emit_publication.py' -v

Standard library only (AAP 0.4.1): no third-party runner, no plugin, no install step,
and AAP 0.4.3 adds no dependency in any direction. ``unittest.mock`` is used in two
places and only there — to make a renderer fail part way through a publication, which is
the one condition that cannot be arranged from outside the module and is exactly the
condition the atomicity contract exists for.

The white-box assertions, and why they are white-box
---------------------------------------------------
:meth:`SecureStagingTests.test_a_pre_planted_staging_path_cannot_be_written_through`
patches ``emit._temporary_name`` so the staging name becomes predictable, which is the
only way to plant a symlink at it: an unpredictable name is itself half the defence, so
the test removes that half in order to prove the other half — exclusive, no-follow
creation — carries the guarantee on its own. The mid-publication failure tests patch
``emit._render_csv`` for the same reason.

:class:`PublishedFileModeTests` adds two more, both for conditions that cannot be
arranged from outside the module. ``os.fchmod`` is replaced with a no-op so that the
``os.open`` mode request is all that remains and a restrictive umask can reduce it,
which is what proves the ``fchmod`` is the assignment rather than decoration; and
``emit._OutputDirectory.replace`` is wrapped to widen the target the instant the rename
completes, which is the post-promotion race the published-mode measurement exists for.

Every one of these is deliberate and all of them are named here so a reader does not
mistake them for reaching into internals out of convenience.

What this file deliberately does not do
---------------------------------------
It never writes to ``oss-scan-results/findings.json``, ``findings.csv`` or anything under
``harness/artifacts/`` — every write goes to a :class:`tempfile.TemporaryDirectory`. It
reads the committed dataset, and only reads it. It judges no finding, ranks no tool,
compares no scanner against another, and deduplicates nothing (AAP 0.3.2). No secret
value appears here: this tree is committed to git, since ``.gitignore:31`` ignores only
``artifacts/``. A defect this file reveals in ``harness/lib/normalize/`` is reported, not
repaired from here.

No user-specified rules govern this file: ``review_rules`` reports "No user rules
provided.", corroborated by AAP 0.7 and 0.10.2. Enterprise-standard best practice applies
in their place, and their absence is not licence to lower the bar — every assertion below
names the exact value, the exact exception type or the exact byte sequence rather than
settling for something truthy.
"""

from __future__ import annotations

# Standard library only, and only these (AAP 0.4.1):
#   csv, json     -- read the written deliverables back the way a consumer would;
#   hashlib       -- recompute a published member's digest independently of emit.py;
#   io            -- the renderer handle's type, and a StringIO to capture the run
#                    record writer's best-effort stderr report without printing it;
#   os, stat      -- the filesystem facts the security assertions are about: symlinks,
#                    inodes and permission bits;
#   pathlib       -- locations derived from __file__, never from the working directory;
#   re            -- counting the module-level bindings of the staged-file mode constant
#                    in emit.py's own source, which is the only way to assert that a
#                    second binding has not been reintroduced (F10);
#   sys           -- the sys.path bootstrap;
#   tempfile      -- every write in this file lands in a temporary directory;
#   unittest      -- the runner, so the suite needs no third-party plugin;
#   unittest.mock -- the two mid-publication failures, as the docstring explains.
import csv
import errno
import hashlib
import io
import json
import os
import re
import stat
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path
from typing import Any
from unittest import mock

# --------------------------------------------------------------------------------------
# The one-time sys.path bootstrap. There is deliberately no __init__.py under
# harness/lib/normalize/: PEP 420 implicit namespace packages make "from normalize import
# emit" work once harness/lib is on sys.path. Every sibling test module carries the same
# two lines; the membership guard keeps repeated discovery imports idempotent.
# --------------------------------------------------------------------------------------
_THIS_FILE = Path(__file__).resolve()
_TESTS_DIR = _THIS_FILE.parent
REPO_ROOT = _THIS_FILE.parents[2]
_LIB_DIR = str(REPO_ROOT / "harness" / "lib")
if _LIB_DIR not in sys.path:
    sys.path.insert(0, _LIB_DIR)

from normalize import cli, emit  # noqa: E402  (import follows the bootstrap by necessity)

#: The committed dataset, read and never written by this file.
FINDINGS_JSON = REPO_ROOT / "oss-scan-results" / "findings.json"
FINDINGS_CSV = REPO_ROOT / "oss-scan-results" / "findings.csv"

#: The row count AAP-side reconciliation established for the committed dataset. Asserted
#: rather than derived, so a dataset that silently changed size is caught here too.
COMMITTED_ROW_COUNT = 9430


def build_row(**overrides: Any) -> dict[str, Any]:
    """Return one valid twelve-field row, with ``overrides`` applied.

    The base row is deliberately dull: every required field carries a plain value and
    every optional field carries one, so a test that wants absence or an adversarial
    string sets exactly that and nothing else varies with it.
    """
    row: dict[str, Any] = {
        "tool": "opengrep",
        "scanner_class": "sast",
        "rule_id": "scala.lang.security.audit.example",
        "message": "an ordinary finding message",
        "severity_native": "warning",
        "severity_norm": "Medium",
        "path": "core/src/main/scala/org/apache/spark/storage/DiskStore.scala",
        "start_line": 72,
        "cwe": "CWE-117",
        "cve": None,
        "package_coordinate": None,
        "in_scope": True,
    }
    row.update(overrides)
    return row


def sha256_of(path: Path) -> str:
    """Return the sha256 of ``path``, computed here rather than taken from ``emit``.

    Deliberately a second implementation: a member's recorded digest is only evidence if
    something other than the code that recorded it can arrive at the same value.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def raw_csv_cells(path: Path) -> list[dict[str, str]]:
    """Parse ``path`` with :mod:`csv` and return the data rows as raw, uncoerced cells.

    This is what a spreadsheet sees: the bytes in the file, before ``emit.py``'s reader
    reverses the neutralisation. Every assertion about what the file *contains* is made
    against this, and every assertion about what the dataset *says* is made against
    ``emit.read_findings_csv``.
    """
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        return [dict(zip(header, record, strict=True)) for record in reader]


class TemporaryOutputMixin(unittest.TestCase):
    """A private temporary directory per test, and the two member paths inside it.

    Nothing in this file writes to a deliverable path: the committed dataset under
    ``oss-scan-results/`` and everything under ``harness/artifacts/`` are read-only here.
    """

    def setUp(self) -> None:
        """Allocate the temporary output directory and derive the member paths."""
        super().setUp()
        self._directory = tempfile.TemporaryDirectory(prefix="blitzy-emit-publication-")
        self.addCleanup(self._directory.cleanup)
        self.output = Path(self._directory.name)
        self.json_path = self.output / "findings.json"
        self.csv_path = self.output / "findings.csv"

    def names_in_output(self) -> list[str]:
        """Return every entry in the output directory, including dot-prefixed ones."""
        return sorted(entry.name for entry in self.output.iterdir())

    def assert_no_staging_residue(self) -> None:
        """Assert the output directory holds no staged, partial or otherwise scratch file.

        The old writer's ``<name>.partial`` sibling is named explicitly as well as the
        current ``.staged`` suffix: a reader finding either would have to decide whether
        it was a deliverable, and that decision is what the atomic contract removes.
        """
        for name in self.names_in_output():
            self.assertFalse(
                name.endswith(".staged") or name.endswith(".partial"),
                f"{name} is a staging residue; a publication leaves none behind",
            )


class StagingProtocolTests(unittest.TestCase):
    """The write protocol is reported as data, so the run record publishes it.

    A security property asserted only in a comment is a claim; asserted as a value in the
    run record it is evidence, and this class is what keeps the value honest.
    """

    def test_the_protocol_names_the_flags_that_carry_the_guarantee(self) -> None:
        """O_CREAT, O_EXCL, O_WRONLY and O_NOFOLLOW, all four, by name."""
        protocol = emit.staging_protocol()
        self.assertEqual(protocol["open_flags"], "O_CREAT|O_EXCL|O_WRONLY|O_NOFOLLOW")
        for flag in ("O_CREAT", "O_EXCL", "O_WRONLY", "O_NOFOLLOW"):
            with self.subTest(flag=flag):
                self.assertTrue(
                    getattr(os, flag) & emit._STAGING_OPEN_FLAGS == getattr(os, flag),
                    f"{flag} must be set on every staging open",
                )

    def test_the_protocol_states_the_directory_and_target_validation(self) -> None:
        """Both refusals are described, because both are refusals rather than repairs."""
        protocol = emit.staging_protocol()
        self.assertIn("realpath", protocol["parent_directory_validation"])
        self.assertIn("symlink", protocol["target_validation"])
        self.assertIn("fsync", protocol["durability"])
        self.assertIn("os.replace", protocol["atomicity"])
        self.assertIn("unlink", protocol["on_failure"])

    def test_the_protocol_is_json_serialisable(self) -> None:
        """It is published inside ``normalize-run.json``, so it must survive json.dumps."""
        json.dumps(emit.staging_protocol())

    def test_the_staging_name_is_unpredictable(self) -> None:
        """Two staging names for one target differ, and neither is the target itself."""
        first = emit._temporary_name("findings.csv")
        second = emit._temporary_name("findings.csv")
        self.assertNotEqual(first, second)
        self.assertNotIn(first, ("findings.csv", "findings.csv.partial"))
        self.assertTrue(first.startswith(".findings.csv."))
        self.assertTrue(first.endswith(".staged"))
        self.assertEqual(
            emit.staging_protocol()["temporary_randomness_bits"],
            emit._TEMP_NAME_RANDOM_BYTES * 8,
        )


class SecureStagingTests(TemporaryOutputMixin):
    """Every way a write could be redirected, raced or left half-finished (F56).

    The findings this class covers are not about malformed data: the rows are valid
    throughout. They are about the filesystem the rows are written into, and about what
    the writer does when that filesystem is not what it appeared to be.
    """

    def test_a_symlinked_directory_component_is_refused(self) -> None:
        """A symlinked parent means the bytes land somewhere the record does not name."""
        real = self.output / "real"
        real.mkdir()
        link = self.output / "link"
        link.symlink_to(real, target_is_directory=True)

        with self.assertRaises(emit.EmitError) as raised:
            emit.publish_findings([build_row()], link / "findings.json", link / "findings.csv")

        self.assertIn("symlink", str(raised.exception))
        self.assertEqual(
            sorted(entry.name for entry in real.iterdir()),
            [],
            "nothing may be written through a symlinked directory component",
        )

    def test_a_symlink_at_the_target_is_refused_and_its_destination_is_untouched(self) -> None:
        """A planted symlink at a member path is reported, not published through."""
        canary = self.output / "canary.json"
        canary.write_text("the previous contents of an unrelated file\n", encoding="utf-8")
        self.json_path.symlink_to(canary)

        with self.assertRaises(emit.EmitError) as raised:
            emit.publish_findings([build_row()], self.json_path, self.csv_path)

        self.assertIn("symlink", str(raised.exception))
        self.assertEqual(
            canary.read_text(encoding="utf-8"),
            "the previous contents of an unrelated file\n",
        )
        self.assertFalse(self.csv_path.exists(), "the second member must not be published")
        self.assert_no_staging_residue()

    def test_a_target_that_is_not_a_regular_file_is_refused(self) -> None:
        """A directory at the target path is a fault to report, never something to replace."""
        self.json_path.mkdir()
        with self.assertRaises(emit.EmitError) as raised:
            emit.publish_findings([build_row()], self.json_path, self.csv_path)
        self.assertIn("not a regular file", str(raised.exception))

    def test_a_pre_planted_staging_path_cannot_be_written_through(self) -> None:
        """Exclusive, no-follow creation carries the guarantee even with the name known.

        The staging name is unpredictable, which is half the defence. This test removes
        that half — patching the name to a fixed value and planting a symlink at it — so
        that what remains is the other half on its own: ``O_CREAT|O_EXCL|O_NOFOLLOW``
        makes the creation fail rather than following the link.
        """
        canary = self.output / "canary.txt"
        canary.write_text("untouched\n", encoding="utf-8")
        planted = self.output / ".findings.json.planted.staged"
        planted.symlink_to(canary)

        with mock.patch.object(emit, "_temporary_name", return_value=planted.name):
            with self.assertRaises(FileExistsError):
                emit.publish_findings([build_row()], self.json_path, self.csv_path)

        self.assertEqual(canary.read_text(encoding="utf-8"), "untouched\n")
        self.assertFalse(self.json_path.exists())
        self.assertFalse(self.csv_path.exists())
        self.assertTrue(planted.is_symlink(), "the planted link is reported, not consumed")

    def test_a_successful_publication_leaves_exactly_the_two_members(self) -> None:
        """No ``.staged`` sibling, no ``.partial`` sibling, nothing else."""
        emit.publish_findings([build_row()], self.json_path, self.csv_path)
        self.assertEqual(self.names_in_output(), ["findings.csv", "findings.json"])
        self.assert_no_staging_residue()

    def test_a_render_that_fails_leaves_no_file_at_the_target_and_no_residue(self) -> None:
        """The failure path removes the staged file and propagates the exception."""

        def render(handle: io.TextIOBase) -> None:
            handle.write("a partial document that must never be published")
            raise ValueError("the renderer failed part way through")

        target = self.output / "document.json"
        with self.assertRaises(ValueError):
            emit.publish_document(target, render, role="a_test_document")

        self.assertFalse(target.exists())
        self.assertEqual(self.names_in_output(), [])

    def test_a_published_member_is_not_group_or_world_writable(self) -> None:
        """The mode is set explicitly, so a permissive umask cannot widen a deliverable."""
        emit.publish_findings([build_row()], self.json_path, self.csv_path)
        for path in (self.json_path, self.csv_path):
            with self.subTest(path=path.name):
                mode = stat.S_IMODE(path.stat().st_mode)
                self.assertEqual(mode & (stat.S_IWGRP | stat.S_IWOTH), 0, f"mode {mode:o}")
                self.assertTrue(mode & stat.S_IRUSR)

    def test_publishing_twice_replaces_the_file_rather_than_truncating_it(self) -> None:
        """``os.replace`` swaps a complete file in; the previous inode is never edited."""
        emit.publish_findings([build_row()], self.json_path, self.csv_path)
        first_inode = self.json_path.stat().st_ino
        first_bytes = self.json_path.read_bytes()

        emit.publish_findings(
            [build_row(message="a different finding message")],
            self.json_path,
            self.csv_path,
        )
        self.assertNotEqual(self.json_path.read_bytes(), first_bytes)
        self.assertNotEqual(
            self.json_path.stat().st_ino,
            first_inode,
            "an atomic publication renames a new file into place rather than rewriting "
            "the one a reader may already have open",
        )

    def test_a_missing_output_directory_is_created(self) -> None:
        """A scratch output directory that does not exist yet is not a fault."""
        nested = self.output / "a" / "b"
        emit.publish_findings(
            [build_row()], nested / "findings.json", nested / "findings.csv"
        )
        self.assertTrue((nested / "findings.json").is_file())
        self.assertTrue((nested / "findings.csv").is_file())


class PublishedFileModeTests(TemporaryOutputMixin):
    """Every published file carries exactly ``0o644``, whatever the umask is (F10).

    The defect this class exists for was not a wrong constant but a **second** one:
    ``emit.py`` carried ``_STAGED_FILE_MODE = 0o644`` with a comment promising an
    explicit mode, and a later module-level binding of the same name set it to
    ``0o666``.  Python resolves a module global when the function runs, so every
    staging open requested ``0o666 & ~umask`` — under a permissive umask a published
    deliverable became group- and world-writable and could be altered after its digest
    was recorded, which is the whole of CWE-732 in one line.

    So four things are asserted, and each catches something the others cannot:

    1. the attribute is ``0o644``;
    2. the **source** contains exactly one module-level binding of the name, which is
       what fails if a second one is ever reintroduced — an attribute assertion alone
       passes as soon as the last binding happens to be right;
    3. a published file is ``0o644`` on disk under a deliberately permissive umask
       (``0o000``) and under a restrictive one (``0o077``), which is the observable
       consequence of the ``fchmod`` assignment rather than of the ``os.open`` request;
    4. the mode is **measured and required** rather than assumed — with the ``fchmod``
       removed, or with the mode widened between the rename and the measurement, the
       publication fails and publishes nothing.

    The umask is process-global, so every test here restores the process's own umask in
    a ``finally`` and no test leaves it changed.
    """

    #: The one mode every file this module publishes must carry.  Written as a literal
    #: rather than read from ``emit`` so that a change to the constant fails this test
    #: instead of being ratified by it.
    PROMISED_MODE = 0o644

    #: A module-level binding of the mode constant: the name at column zero followed by
    #: ``=``.  Matched over ``emit.py``'s own source in MULTILINE mode, so a reference
    #: inside a function body, a comment or an f-string cannot be mistaken for one.
    MODE_BINDING_RE = re.compile(r"^_STAGED_FILE_MODE\s*=", re.MULTILINE)

    def publish_under_umask(self, mask: int) -> emit.PublicationResult:
        """Publish the dataset with the process umask set to ``mask``, then restore it."""
        previous = os.umask(mask)
        try:
            return emit.publish_findings([build_row()], self.json_path, self.csv_path)
        finally:
            os.umask(previous)

    def test_the_module_promises_exactly_one_mode_and_it_is_0o644(self) -> None:
        """The attribute itself: the value every consumer in the module resolves."""
        self.assertEqual(emit._STAGED_FILE_MODE, self.PROMISED_MODE)
        self.assertEqual(
            emit.staging_protocol()["file_mode"], f"0o{self.PROMISED_MODE:o}"
        )

    def test_the_source_carries_exactly_one_binding_of_the_mode_constant(self) -> None:
        """A second binding would win at call time, so its absence is asserted directly.

        Asserted against the source because the defect is invisible in the attribute:
        two bindings leave one value, and the value left is whichever came last.
        """
        source = Path(emit.__file__).read_text(encoding="utf-8")
        bindings = self.MODE_BINDING_RE.findall(source)
        self.assertEqual(
            len(bindings),
            1,
            "emit.py must contain exactly one module-level binding of "
            f"_STAGED_FILE_MODE; found {len(bindings)}. A later binding wins at call "
            "time and every staging open would request its value instead.",
        )

    def test_a_published_member_is_0o644_under_a_permissive_umask(self) -> None:
        """umask 0o000 is the condition under which the request alone yields 0o666."""
        self.publish_under_umask(0o000)
        for path in (self.json_path, self.csv_path):
            with self.subTest(path=path.name):
                mode = stat.S_IMODE(path.stat().st_mode)
                self.assertEqual(
                    mode,
                    self.PROMISED_MODE,
                    f"{path.name} is 0o{mode:o} under umask 0o000; the mode is assigned "
                    "with fchmod precisely so the umask cannot widen it",
                )
                self.assertEqual(mode & (stat.S_IWGRP | stat.S_IWOTH), 0)

    def test_a_published_member_is_0o644_under_a_restrictive_umask(self) -> None:
        """umask 0o077 is the other direction: the mode may not be REDUCED either.

        A deliverable the reconciler cannot read is not a safe deliverable, it is an
        unverifiable one, so ``0o644`` is a requirement in both directions rather than
        a ceiling.
        """
        self.publish_under_umask(0o077)
        for path in (self.json_path, self.csv_path):
            with self.subTest(path=path.name):
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), self.PROMISED_MODE)

    def test_a_document_published_on_its_own_carries_the_same_mode(self) -> None:
        """``publish_document`` is the run record's path, and it is the same protocol."""
        target = self.output / "normalize-run.json"
        previous = os.umask(0o000)
        try:
            member = emit.publish_document(
                target, lambda handle: handle.write("{}\n"), role="a_test_document"
            )
        finally:
            os.umask(previous)
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), self.PROMISED_MODE)
        self.assertEqual(member.mode, self.PROMISED_MODE)

    def test_every_published_member_records_the_mode_it_actually_carries(self) -> None:
        """The record carries a measurement, so a wrong mode is visible not assumed."""
        publication = self.publish_under_umask(0o000)
        for member in publication.members:
            with self.subTest(role=member.role):
                on_disk = stat.S_IMODE(Path(member.path).stat().st_mode)
                self.assertEqual(member.mode, on_disk)
                rendered = member.as_dict()
                self.assertEqual(rendered["mode"], on_disk)
                self.assertEqual(rendered["mode_octal"], f"0o{on_disk:o}")
                self.assertEqual(
                    rendered["mode_promised_octal"], f"0o{self.PROMISED_MODE:o}"
                )
        # And the publication survives json.dumps, because it is published inside
        # normalize-run.json.
        json.dumps(publication.as_dict())

    def test_the_mode_is_asserted_rather_than_hoped_for(self) -> None:
        """Without the ``fchmod`` the umask decides, and the staged check refuses it.

        White-box on purpose, and the only way to arrange the condition: ``fchmod`` is
        replaced with a no-op so that the ``os.open`` request is all that is left, and
        a umask of ``0o077`` reduces it to ``0o600``.  The publication must refuse that
        file rather than publish a deliverable whose permissions the record misstates.
        """
        previous = os.umask(0o077)
        try:
            with mock.patch("os.fchmod", lambda *_args, **_kwargs: None):
                with self.assertRaises(emit.EmitError) as raised:
                    emit.publish_findings([build_row()], self.json_path, self.csv_path)
        finally:
            os.umask(previous)
        self.assertIn("0o600", str(raised.exception))
        self.assertIn(f"0o{self.PROMISED_MODE:o}", str(raised.exception))
        self.assertFalse(self.json_path.exists())
        self.assertFalse(self.csv_path.exists())
        self.assert_no_staging_residue()

    def test_a_mode_widened_after_the_rename_fails_the_publication(self) -> None:
        """The post-promotion measurement, which is the one ``os.replace`` requires.

        ``os.replace`` preserves the mode, so the staged check would be enough if
        nothing else touched the file — the check after promotion is what catches
        something that does.  Arranged by widening the target the instant the rename
        completes, which is exactly the race the measurement exists for (CWE-367).
        """
        real_replace = emit._OutputDirectory.replace

        def replace_then_widen(
            directory: Any, source_name: str, target_name: str
        ) -> None:
            real_replace(directory, source_name, target_name)
            os.chmod(directory.path / target_name, 0o666)

        with mock.patch.object(emit._OutputDirectory, "replace", replace_then_widen):
            with self.assertRaises(emit.EmitError) as raised:
                emit.publish_findings([build_row()], self.json_path, self.csv_path)

        message = str(raised.exception)
        self.assertIn("0o666", message)
        self.assertIn(f"0o{self.PROMISED_MODE:o}", message)
        self.assertIn("was published", message)

    def test_the_protocol_states_the_assignment_and_the_verification(self) -> None:
        """The run record has to carry why the mode is what it is, not only its value."""
        protocol = emit.staging_protocol()
        self.assertIn("fchmod", protocol["file_mode_assignment"])
        self.assertIn("umask", protocol["file_mode_assignment"])
        self.assertIn("AFTER the rename", protocol["file_mode_verification"])
        self.assertIn("0o600", protocol["file_mode_rationale"])
        json.dumps(protocol)


class AtomicPublicationTests(TemporaryOutputMixin):
    """The two members are one generation, or they are not published at all (F57)."""

    PREVIOUS_JSON = '[{"the": "previous generation"}]\n'
    PREVIOUS_CSV = "the,previous,generation\n"

    def seed_previous_generation(self) -> None:
        """Put a recognisable previous generation at both member paths."""
        self.json_path.write_text(self.PREVIOUS_JSON, encoding="utf-8")
        self.csv_path.write_text(self.PREVIOUS_CSV, encoding="utf-8")

    def assert_previous_generation_intact(self) -> None:
        """Assert neither member moved — the whole point of staging before publishing."""
        self.assertEqual(self.json_path.read_text(encoding="utf-8"), self.PREVIOUS_JSON)
        self.assertEqual(self.csv_path.read_text(encoding="utf-8"), self.PREVIOUS_CSV)

    def test_both_members_carry_one_publication_identifier(self) -> None:
        """One identifier, both members, in ``PUBLICATION_ROLES`` order."""
        publication = emit.publish_findings(
            [build_row(), build_row(tool="semgrep")], self.json_path, self.csv_path
        )
        self.assertEqual(
            [member.role for member in publication.members],
            list(emit.PUBLICATION_ROLES),
        )
        identifiers = {member.publication_id for member in publication.members}
        self.assertEqual(len(identifiers), 1)
        self.assertEqual(identifiers.pop(), publication.publication_id)
        self.assertEqual(publication.rows, 2)
        self.assertEqual(publication.scheme, emit.PUBLICATION_SCHEME)

    def test_each_member_records_the_bytes_and_digest_that_are_on_the_disk(self) -> None:
        """The recorded measurement is checkable against the published file itself."""
        publication = emit.publish_findings([build_row()], self.json_path, self.csv_path)
        for member in publication.members:
            with self.subTest(role=member.role):
                path = Path(member.path)
                self.assertEqual(member.size_bytes, path.stat().st_size)
                self.assertEqual(member.sha256, sha256_of(path))

    def test_the_identifier_is_recomputable_from_the_two_published_files(self) -> None:
        """A consumer holding the files can derive it — which is how a mix is detected."""
        publication = emit.publish_findings([build_row()], self.json_path, self.csv_path)
        recomputed = emit.publication_identifier(
            {
                emit.ROLE_FINDINGS_JSON: sha256_of(self.json_path),
                emit.ROLE_FINDINGS_CSV: sha256_of(self.csv_path),
            }
        )
        self.assertEqual(recomputed, publication.publication_id)

    def test_the_identifier_is_content_derived_rather_than_random(self) -> None:
        """Identical rows give the same identifier; a changed row gives a different one."""
        rows = [build_row()]
        first = emit.publish_findings(rows, self.json_path, self.csv_path)
        second = emit.publish_findings(rows, self.json_path, self.csv_path)
        self.assertEqual(first.publication_id, second.publication_id)

        changed = emit.publish_findings(
            [build_row(start_line=73)], self.json_path, self.csv_path
        )
        self.assertNotEqual(changed.publication_id, first.publication_id)

    def test_a_mixed_generation_matches_neither_recorded_identifier(self) -> None:
        """The detection this identifier exists for, arranged and then observed."""
        first = emit.publish_findings([build_row()], self.json_path, self.csv_path)
        first_csv = self.csv_path.read_bytes()
        second = emit.publish_findings(
            [build_row(message="a second generation of the dataset")],
            self.json_path,
            self.csv_path,
        )
        # The second run's JSON beside the first run's CSV: exactly the state a
        # member-by-member publication can leave behind.
        self.csv_path.write_bytes(first_csv)
        mixed = emit.publication_identifier(
            {
                emit.ROLE_FINDINGS_JSON: sha256_of(self.json_path),
                emit.ROLE_FINDINGS_CSV: sha256_of(self.csv_path),
            }
        )
        self.assertNotEqual(mixed, first.publication_id)
        self.assertNotEqual(mixed, second.publication_id)

    def test_a_fault_rendering_the_second_member_publishes_neither(self) -> None:
        """Staging both before publishing either is what makes this hold.

        ``_render_csv`` is patched to raise: the only way to fail between the members
        from outside, and the exact condition a member-by-member writer cannot survive.
        """
        self.seed_previous_generation()
        with mock.patch.object(
            emit, "_render_csv", side_effect=OSError("the device filled up")
        ):
            with self.assertRaises(OSError):
                emit.publish_findings([build_row()], self.json_path, self.csv_path)
        self.assert_previous_generation_intact()
        self.assert_no_staging_residue()

    def test_a_fault_on_the_second_final_rename_leaves_a_detectable_incomplete_set(
        self,
    ) -> None:
        """The window staging cannot close, and the manifest that closes it.

        Every test above this one fails *before* the first rename, which staging alone
        already survives.  This one fails *between* the renames — the case POSIX offers
        no atomic operation for, and the one a member-by-member writer cannot detect
        afterwards.  ``os.replace`` is patched to succeed once and then fail.

        Two things are asserted, and the second is the point.  The first member is on
        disk, because a completed rename cannot be undone and the code does not pretend
        otherwise.  And the completion manifest is **absent**, so
        :func:`emit.require_publication_manifest` refuses the set: a consumer learns the
        set is not one generation without needing to know what the previous one was.
        """
        self.seed_previous_generation()
        manifest_path = self.output / "logs" / "findings-publication.json"
        real_replace = os.replace
        calls: list[int] = []

        def replace_then_fail(*args: Any, **kwargs: Any) -> None:
            calls.append(1)
            if len(calls) == 1:
                real_replace(*args, **kwargs)
                return
            raise OSError("the device filled up between the renames")

        with mock.patch.object(emit.os, "replace", side_effect=replace_then_fail):
            with self.assertRaises(OSError):
                emit.publish_findings(
                    [build_row()],
                    self.json_path,
                    self.csv_path,
                    manifest_path=manifest_path,
                )

        self.assertGreaterEqual(len(calls), 2, "the second rename must have been reached")
        # One member did move. That is not a defect to hide: the point is that it is
        # DETECTABLE, which is what the next two assertions establish.
        self.assertNotEqual(
            self.json_path.read_text(encoding="utf-8"),
            self.PREVIOUS_JSON,
            "the first rename completed, so that member is this generation's",
        )
        self.assertFalse(
            manifest_path.exists(),
            "the manifest is renamed last, so a fault before it leaves it absent — "
            "which is the completion signal being correctly withheld",
        )
        with self.assertRaises(emit.EmitError) as raised:
            emit.require_publication_manifest(manifest_path)
        self.assertIn("absent", str(raised.exception))
        self.assert_no_staging_residue()

    def test_a_stale_manifest_from_the_previous_generation_is_refused(self) -> None:
        """The failure mode a missing manifest does *not* cover.

        A publication that renames its content and then cannot write a commit record
        leaves the PREVIOUS generation's manifest on disk. That record is internally
        consistent — its own member_set_id recomputes — so nothing about the manifest
        alone reveals the problem. What reveals it is re-measuring the members it
        names, which is why the check does that rather than trusting the record.
        """
        manifest_path = self.output / "logs" / "findings-publication.json"
        first = emit.publish_findings(
            [build_row()], self.json_path, self.csv_path, manifest_path=manifest_path
        )
        stale = manifest_path.read_bytes()

        # A second generation of both members, then the FIRST generation's manifest
        # put back: the state a content-renamed, manifest-failed publication leaves.
        second = emit.publish_findings(
            [build_row(message="a second generation")],
            self.json_path,
            self.csv_path,
            manifest_path=manifest_path,
        )
        self.assertNotEqual(first.publication_id, second.publication_id)
        manifest_path.write_bytes(stale)

        with self.assertRaises(emit.EmitError) as raised:
            emit.require_dataset_generation(
                self.json_path, self.csv_path, manifest_path
            )
        message = str(raised.exception)
        self.assertIn("not one generation", message)

        # And the reader refuses to read the pair at all while that record stands,
        # which is the property that matters: the consumer never sees the mixed set.
        with self.assertRaises(emit.EmitError):
            emit.compare_outputs(
                self.json_path, self.csv_path, manifest_path
            )

    def test_a_fault_on_the_manifest_rename_leaves_no_commit_record(self) -> None:
        """The manifest is renamed last, so a fault there withholds the signal.

        The content members are this generation's, and nothing pretends otherwise —
        a completed rename is not undone. What must not happen is a commit record
        appearing for a set whose last step failed, and the ordering is what
        guarantees it: there is nothing left to fail after the manifest rename.
        """
        manifest_path = self.output / "logs" / "findings-publication.json"
        real_replace = os.replace
        calls: list[str] = []

        def replace_failing_on_the_manifest(src, dst, **kwargs):
            name = os.path.basename(str(dst))
            calls.append(name)
            if name == "findings-publication.json":
                raise OSError(errno.EIO, "the manifest rename failed")
            return real_replace(src, dst, **kwargs)

        with unittest.mock.patch("os.replace", replace_failing_on_the_manifest):
            with self.assertRaises(OSError):
                emit.publish_findings(
                    [build_row()],
                    self.json_path,
                    self.csv_path,
                    manifest_path=manifest_path,
                )

        self.assertFalse(
            manifest_path.exists(),
            "no commit record may exist for a publication whose manifest rename failed",
        )
        self.assertIn(
            "findings-publication.json",
            calls,
            "the manifest rename must have been attempted last, after the content",
        )
        with self.assertRaises(emit.EmitError):
            emit.require_dataset_generation(
                self.json_path, self.csv_path, manifest_path
            )

    def test_a_directory_that_cannot_be_made_durable_stops_the_publication(self) -> None:
        """A failed directory fsync is fatal, not a note.

        The manifest's presence is supposed to mean "every member is in place AND
        durable there". A publication that cannot establish durability and writes the
        record anyway has published a promise it cannot keep, so the failure is
        raised — and, because content is synced before the manifest is renamed, no
        commit record is left behind.
        """
        manifest_path = self.output / "logs" / "findings-publication.json"

        def sync_failing(self) -> None:
            raise OSError(errno.EIO, "the directory could not be made durable")

        # Patched on the directory object rather than on os.fsync, so the file
        # fsync each member's own write performs is left working: what is under
        # test is the DIRECTORY sync that establishes the renames, and a global
        # patch would fail the staging write instead and never reach it.
        with unittest.mock.patch.object(
            emit._OutputDirectory, "sync", sync_failing
        ):
            with self.assertRaises(emit.EmitError) as raised:
                emit.publish_findings(
                    [build_row()],
                    self.json_path,
                    self.csv_path,
                    manifest_path=manifest_path,
                )
        self.assertIn("durab", str(raised.exception).lower())
        self.assertFalse(
            manifest_path.exists(),
            "a set whose durability could not be established gets no commit record",
        )

    def test_reading_a_member_is_bound_to_the_identity_that_was_verified(self) -> None:
        """Verification closes its descriptors, so the binding is what carries over.

        Requiring the manifest and then reopening the pathname proves nothing on its
        own: the name can be repointed between the two. The identities the check
        observed are therefore returned and required at open time, so a substituted
        file is refused even though its path is the one the manifest names.
        """
        manifest_path = self.output / "logs" / "findings-publication.json"
        emit.publish_findings(
            [build_row()], self.json_path, self.csv_path, manifest_path=manifest_path
        )
        verified = emit.require_dataset_generation(
            self.json_path, self.csv_path, manifest_path
        )
        identities = verified["verified_member_identities"]
        json_identity = identities[emit.ROLE_FINDINGS_JSON]
        self.assertEqual(len(json_identity), 2)

        # Reading under the verified identity works.
        rows = emit.read_findings_json(self.json_path, json_identity)
        self.assertEqual(len(rows), 1)

        # Substituting the file at that path — same name, different inode — is
        # refused, which a pathname-based reader would not have noticed.
        replacement = self.output / "substituted.json"
        replacement.write_text(self.json_path.read_text(encoding="utf-8"), encoding="utf-8")
        os.replace(replacement, self.json_path)
        with self.assertRaises(emit.EmitError) as raised:
            emit.read_findings_json(self.json_path, json_identity)
        self.assertIn("inode", str(raised.exception))

    def test_the_completion_manifest_is_published_last_and_verifies_every_member(
        self,
    ) -> None:
        """On the success path the manifest exists, agrees, and is required."""
        manifest_path = self.output / "logs" / "findings-publication.json"
        publication = emit.publish_findings(
            [build_row(), build_row(tool="semgrep")],
            self.json_path,
            self.csv_path,
            manifest_path=manifest_path,
        )
        self.assertTrue(manifest_path.is_file())
        recorded = publication.completion_manifest
        self.assertIsNotNone(recorded)
        assert recorded is not None
        self.assertEqual(recorded["path"], str(manifest_path))
        self.assertTrue(recorded["verified_by_producer"])
        self.assertEqual(
            sorted(recorded["verified_members"]),
            sorted([emit.ROLE_FINDINGS_CSV, emit.ROLE_FINDINGS_JSON]),
        )

        document = emit.require_publication_manifest(manifest_path)
        self.assertEqual(document["schema"], emit.MANIFEST_SCHEME)
        self.assertEqual(document["member_set_id"], recorded["member_set_id"])
        self.assertEqual(
            [member["sha256"] for member in document["members"]],
            [member.sha256 for member in publication.members],
        )
        # The manifest does not record its own digest, and says why rather than
        # leaving the omission to be discovered.
        self.assertIn("cannot contain its own digest", document["self_digest"])

    def test_the_member_set_id_is_derived_from_member_bytes(self) -> None:
        """The property ``publication_id`` cannot have, and the reason both exist."""
        manifest_path = self.output / "logs" / "findings-publication.json"
        first = emit.publish_findings(
            [build_row()], self.json_path, self.csv_path, manifest_path=manifest_path
        )
        first_set_id = json.loads(manifest_path.read_text(encoding="utf-8"))[
            "member_set_id"
        ]
        second = emit.publish_findings(
            [build_row(tool="semgrep")],
            self.json_path,
            self.csv_path,
            manifest_path=manifest_path,
        )
        second_set_id = json.loads(manifest_path.read_text(encoding="utf-8"))[
            "member_set_id"
        ]
        self.assertNotEqual(first_set_id, second_set_id)
        self.assertNotEqual(first.publication_id, second.publication_id)
        # Recomputable from nothing but the published digests, which is what a
        # consumer holding only the files can do.
        self.assertEqual(
            second_set_id,
            emit.member_set_identifier(second.members),
        )

    def test_a_subset_manifest_left_by_a_failed_publication_is_refused(self) -> None:
        """The check that matters, and the one a weak manifest check misses.

        A one-member manifest is internally consistent: its ``member_count`` agrees,
        its digest matches, its ``member_set_id`` recomputes.  A consumer that only
        verifies internal consistency would treat it as a complete generation of one
        file, with the omitted member invisible.  Only the expected **role set**
        refuses it, which is why the producer and :func:`emit.require_dataset_generation`
        both pass one.
        """
        manifest_path = self.output / "logs" / "findings-publication.json"
        emit.publish_findings(
            [build_row()], self.json_path, self.csv_path, manifest_path=manifest_path
        )
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
        kept = [m for m in document["members"] if m["role"] == emit.ROLE_FINDINGS_JSON]
        document["members"] = kept
        document["member_count"] = len(kept)
        document["member_set_id"] = emit.member_set_identifier(
            [type("M", (), {"role": m["role"], "sha256": m["sha256"]})() for m in kept]
        )
        manifest_path.write_text(json.dumps(document, indent=2), encoding="utf-8")

        # Internally consistent, so the permissive form accepts it.
        emit.require_publication_manifest(manifest_path)
        # The schema-aware form does not.
        with self.assertRaises(emit.EmitError) as raised:
            emit.require_dataset_generation(
                self.json_path, self.csv_path, manifest_path
            )
        self.assertIn("requires", str(raised.exception))
        self.assertIn(emit.ROLE_FINDINGS_CSV, str(raised.exception))

    def test_a_manifest_declaring_the_wrong_member_count_is_refused(self) -> None:
        """A truncated or padded record cannot be relied on at all."""
        manifest_path = self.output / "logs" / "findings-publication.json"
        emit.publish_findings(
            [build_row()], self.json_path, self.csv_path, manifest_path=manifest_path
        )
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
        document["member_count"] = 3
        manifest_path.write_text(json.dumps(document, indent=2), encoding="utf-8")
        with self.assertRaises(emit.EmitError) as raised:
            emit.require_publication_manifest(manifest_path)
        self.assertIn("member_count", str(raised.exception))

    def test_a_manifest_naming_another_generations_paths_is_refused(self) -> None:
        """A valid record describing other files cannot vouch for these ones."""
        manifest_path = self.output / "logs" / "findings-publication.json"
        other_json = self.output / "elsewhere" / "findings.json"
        other_csv = self.output / "elsewhere" / "findings.csv"
        emit.publish_findings(
            [build_row()], other_json, other_csv, manifest_path=manifest_path
        )
        self.seed_previous_generation()
        with self.assertRaises(emit.EmitError) as raised:
            emit.require_dataset_generation(
                self.json_path, self.csv_path, manifest_path
            )
        self.assertIn("expects", str(raised.exception))

    def test_a_symlink_at_the_manifest_path_is_refused_rather_than_followed(self) -> None:
        """The commit record is a regular file at the path the record names."""
        manifest_path = self.output / "logs" / "findings-publication.json"
        emit.publish_findings(
            [build_row()], self.json_path, self.csv_path, manifest_path=manifest_path
        )
        real = manifest_path.parent / "real-manifest.json"
        manifest_path.rename(real)
        manifest_path.symlink_to(real)
        with self.assertRaises(emit.EmitError) as raised:
            emit.require_publication_manifest(manifest_path)
        self.assertIn("link", str(raised.exception))

    def test_comparing_outputs_requires_the_manifest_when_one_is_named(self) -> None:
        """Field-for-field agreement is the weaker property; the commit record is not."""
        manifest_path = self.output / "logs" / "findings-publication.json"
        publication = emit.publish_findings(
            [build_row(), build_row(tool="semgrep")],
            self.json_path,
            self.csv_path,
            manifest_path=manifest_path,
        )
        comparison = emit.compare_outputs(
            self.json_path,
            self.csv_path,
            manifest_path,
            publication_id=publication.publication_id,
        )
        self.assertTrue(comparison.passed)
        manifest_path.unlink()
        with self.assertRaises(emit.EmitError) as raised:
            emit.compare_outputs(self.json_path, self.csv_path, manifest_path)
        self.assertIn("absent", str(raised.exception))
        # Without a manifest argument the comparison still works, because it also
        # runs on staged bytes that have no published path yet.
        self.assertTrue(emit.compare_outputs(self.json_path, self.csv_path).passed)

    def test_a_manifest_edited_to_match_a_changed_member_is_still_refused(self) -> None:
        """``member_set_id`` is recomputed, so patching the per-member digest is not enough."""
        manifest_path = self.output / "logs" / "findings-publication.json"
        emit.publish_findings(
            [build_row()], self.json_path, self.csv_path, manifest_path=manifest_path
        )
        self.csv_path.write_text("tampered\n", encoding="utf-8")
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
        tampered_digest = hashlib.sha256(b"tampered\n").hexdigest()
        for member in document["members"]:
            if member["role"] == emit.ROLE_FINDINGS_CSV:
                member["bytes"] = len(b"tampered\n")
                member["sha256"] = tampered_digest
        manifest_path.write_text(json.dumps(document, indent=2), encoding="utf-8")
        with self.assertRaises(emit.EmitError) as raised:
            emit.require_publication_manifest(manifest_path)
        self.assertIn("member_set_id", str(raised.exception))

    def test_a_disagreeing_pair_publishes_neither_and_carries_the_comparison(self) -> None:
        """Validation happens on the staged bytes, before either file moves into place."""
        self.seed_previous_generation()
        rows = [build_row(), build_row(tool="semgrep"), build_row(tool="joern")]

        render_csv = emit._render_csv  # captured before the patch replaces the name

        def render_one_row_short(
            all_rows: list[dict[str, Any]],
            handle: io.TextIOBase,
            tally: emit.CsvNeutralisationTally | None = None,
        ) -> None:
            render_csv(all_rows[:-1], handle, tally)

        with mock.patch.object(emit, "_render_csv", new=render_one_row_short):
            with self.assertRaises(emit.ComparisonFailed) as raised:
                emit.publish_findings(rows, self.json_path, self.csv_path)

        comparison = raised.exception.comparison
        self.assertFalse(comparison.passed)
        self.assertIsNotNone(comparison.first_mismatch)
        self.assertEqual(
            comparison.first_mismatch.kind, emit.MISMATCH_ROW_SEQUENCE_LENGTH
        )
        self.assertEqual(comparison.json_path, str(self.json_path))
        self.assert_previous_generation_intact()
        self.assert_no_staging_residue()

    def test_an_invalid_row_publishes_neither_member(self) -> None:
        """Row validation still precedes every write, and it precedes both of them."""
        self.seed_previous_generation()
        with self.assertRaises(emit.EmitError):
            emit.publish_findings(
                [build_row(), build_row(path="/etc/passwd")],
                self.json_path,
                self.csv_path,
            )
        self.assert_previous_generation_intact()
        self.assert_no_staging_residue()

    def test_the_publication_is_json_serialisable_and_names_its_derivation(self) -> None:
        """It is recorded in ``normalize-run.json``, derivation beside value."""
        publication = emit.publish_findings([build_row()], self.json_path, self.csv_path)
        rendered = json.loads(json.dumps(publication.as_dict()))
        self.assertEqual(rendered["publication_id"], publication.publication_id)
        self.assertEqual(rendered["identifier_method"], emit.PUBLICATION_IDENTIFIER_METHOD)
        self.assertEqual(len(rendered["members"]), 2)
        self.assertTrue(rendered["comparison"]["passed"])
        self.assertEqual(rendered["staging"]["scheme"], emit.PUBLICATION_SCHEME)

    def test_a_single_member_publication_records_no_comparison(self) -> None:
        """One file cannot be compared against a second that was not written."""
        member = emit.publish_document(
            self.output / "document.json",
            lambda handle: handle.write("{}\n"),
            role="a_test_document",
        )
        self.assertEqual(member.role, "a_test_document")
        self.assertEqual(member.size_bytes, 3)
        self.assertEqual(member.sha256, sha256_of(self.output / "document.json"))

    def test_the_existing_writers_keep_their_return_contracts(self) -> None:
        """``write_findings`` and friends still return the validated rows they always did."""
        rows = [build_row(), build_row(tool="trivy", scanner_class="vuln")]
        self.assertEqual(len(emit.write_findings(rows, self.json_path, self.csv_path)), 2)
        self.assertEqual(len(emit.write_findings_json(rows, self.json_path)), 2)
        self.assertEqual(len(emit.write_findings_csv(rows, self.csv_path)), 2)
        self.assertTrue(emit.compare_outputs(self.json_path, self.csv_path).passed)
        self.assertTrue(emit.emit_findings(rows, self.json_path, self.csv_path).passed)


class RunRecordPublicationTests(TemporaryOutputMixin):
    """``normalize-run.json`` is published under the same protocol as the dataset (F56).

    The record is the file every number in the result documents is traced back to, so a
    truncated or redirected record is as damaging as a truncated dataset — and the safety
    has to be tested rather than inferred from the absence of a complaint.

    The writer is **fail-closed**: a record that cannot be written and verified is the
    run's outcome rather than a line on stderr beside a success (CWE-703), so each refusal
    below is asserted on the raised ``cli.RunRecordNotPersisted`` — the condition, its
    message and the untouched destination — rather than on a printed one.
    """

    def test_the_record_is_written_and_parses(self) -> None:
        """The ordinary path: a complete, parsable record at the named path."""
        record = cli._new_record([], "2026-01-01T00:00:00Z")
        target = self.output / "normalize-run.json"
        cli._write_run_record(target, record)
        parsed = json.loads(target.read_text(encoding="utf-8"))
        self.assertEqual(parsed["document"], cli.RUN_RECORD_DOCUMENT)
        self.assertEqual(self.names_in_output(), ["normalize-run.json"])

    def test_a_symlink_at_the_record_path_is_refused_and_named(self) -> None:
        """Refused, its destination untouched, and the link named in the refusal.

        Both refusal paths are exercised, because the writer has two and they refuse
        for different reasons. With no owner root declared the descriptor-bound
        publisher refuses the target itself ("the target path is a symlink"); with one
        declared, the containment predicate refuses the component first ("is a symbolic
        link", CWE-73). Either way the canary the link points at is untouched, which is
        the property that matters: a record is never written through a link.
        """
        canary = self.output / "canary.json"
        canary.write_text("untouched\n", encoding="utf-8")
        target = self.output / "normalize-run.json"
        target.symlink_to(canary)

        for owner, expected in ((None, "the target path is a symlink"),
                                (self.output, "is a symbolic link")):
            with self.subTest(owner=str(owner)):
                with self.assertRaises(cli.RunRecordNotPersisted) as caught:
                    cli._write_run_record(
                        target,
                        cli._new_record([], "2026-01-01T00:00:00Z"),
                        owner=owner,
                    )
                message = str(caught.exception)
                self.assertIn("could not be written", message)
                self.assertIn(expected, message)
                self.assertIn(str(target), message)
                self.assertEqual(canary.read_text(encoding="utf-8"), "untouched\n")
                self.assertTrue(target.is_symlink())

    def test_a_symlinked_directory_component_is_refused_and_named(self) -> None:
        """The record never lands outside the directory the run says it wrote it to."""
        real = self.output / "real"
        real.mkdir()
        link = self.output / "link"
        link.symlink_to(real, target_is_directory=True)

        with self.assertRaises(cli.RunRecordNotPersisted) as caught:
            cli._write_run_record(
                link / "normalize-run.json", cli._new_record([], "2026-01-01T00:00:00Z")
            )

        self.assertEqual(sorted(entry.name for entry in real.iterdir()), [])
        self.assertIn("could not be written", str(caught.exception))

    def test_a_record_that_cannot_be_serialised_leaves_no_partial_file(self) -> None:
        """A serialisation fault leaves nothing behind and becomes the run's outcome.

        A circular reference is the realistic version of this fault -- a record assembled
        from live objects can acquire one -- and :func:`json.dump` raises ``ValueError``
        for it part way through the document, which is the case that would otherwise
        leave a truncated record behind.
        """
        record = cli._new_record([], "2026-01-01T00:00:00Z")
        circular: dict[str, Any] = {}
        circular["itself"] = circular
        record["totals"] = circular
        target = self.output / "normalize-run.json"

        with self.assertRaises(cli.RunRecordNotPersisted) as caught:
            cli._write_run_record(target, record)

        self.assertIn("could not be written", str(caught.exception))
        self.assertFalse(
            target.exists(),
            "a record that could not be serialised must not leave a partial file",
        )
        self.assert_no_staging_residue()


#: Cells whose leading character makes a spreadsheet evaluate them, one per trigger, plus
#: the cases that decide whether the scheme is reversible at all: text that already begins
#: with the escape character, text that begins with two of them, a trigger that is the
#: whole cell, and a trigger appearing anywhere but first.
ADVERSARIAL_TEXT = (
    "=cmd|' /c calc'!A0",
    "=1+1",
    "+1+1",
    "-2+3",
    "@SUM(A1)",
    "\tleading tab then text",
    "\rleading carriage return then text",
    "'already begins with the escape character",
    "''begins with two escape characters",
    "'",
    "''",
    "=",
    "+",
    "-",
    "@",
    "\t",
    "\r",
    "an ordinary message with = and + and @ inside it",
    "a message, with a comma and a \"quoted\" fragment",
    "a message\nspanning two lines",
    "an apostrophe in the middle isn't a problem",
    "ünïcödé and an em dash — in a message",
    " leading space then =formula",
    "0",
)


class CsvNeutralisationTests(TemporaryOutputMixin):
    """The spreadsheet-formula rule, and the exactness of its inverse (F55).

    Two properties carry the whole finding. The rule must make every text cell inert —
    otherwise the deliverable still hands a spreadsheet executable content — and it must
    be *exactly* reversible, or the dataset silently stops saying what the tool said.
    Reversibility is the harder of the two, because the awkward input is a cell whose text
    already begins with the escape character; the assertions below put that case, and its
    doubled form, in every position that matters.
    """

    def test_the_rule_is_reported_as_data_and_matches_what_it_does(self) -> None:
        """The recorded rule is the rule: every declared trigger is really escaped."""
        rule = emit.csv_neutralisation_rule()
        self.assertEqual(rule["escape_character"], emit.CSV_FORMULA_ESCAPE)
        self.assertEqual(rule["triggers"], list(emit.CSV_FORMULA_TRIGGERS))
        self.assertTrue(rule["reversible"])
        self.assertEqual(
            rule["applies_to"],
            [field for field in emit.FIELDS if field not in ("start_line", "in_scope")],
        )
        for trigger in rule["triggers"]:
            with self.subTest(trigger=repr(trigger)):
                self.assertEqual(
                    emit.neutralize_csv_text(f"{trigger}x"),
                    f"{emit.CSV_FORMULA_ESCAPE}{trigger}x",
                )
        json.dumps(rule)

    def test_the_inverse_is_exact_for_every_adversarial_input(self) -> None:
        """``restore(neutralize(s)) == s``, character for character, for every case."""
        for text in ADVERSARIAL_TEXT:
            with self.subTest(text=repr(text)):
                written = emit.neutralize_csv_text(text)
                self.assertEqual(emit.restore_csv_text(written), text)

    def test_the_rule_is_injective_over_a_generated_corpus(self) -> None:
        """No two distinct values can be written as the same cell.

        Injectivity is what makes the inverse well defined: if two different messages
        could produce one cell, no reader could recover either. The corpus is generated
        rather than listed — every leading character that matters, crossed with a tail
        that is empty, ordinary, or itself another leading character.
        """
        leaders = list(emit.CSV_FORMULA_TRIGGERS) + [emit.CSV_FORMULA_ESCAPE, "", "x", " "]
        tails = ["", "x", "=y", "'z", "\tw", "'"]
        corpus = {leader + tail for leader in leaders for tail in tails}
        written = {}
        for text in sorted(corpus):
            cell = emit.neutralize_csv_text(text)
            self.assertNotIn(
                cell,
                written,
                f"{text!r} and {written.get(cell)!r} both write as {cell!r}",
            )
            written[cell] = text
            self.assertEqual(emit.restore_csv_text(cell), text)
        self.assertEqual(len(written), len(corpus))

    def test_a_neutralised_cell_no_longer_leads_with_a_trigger(self) -> None:
        """The point of the exercise: no written text cell begins with a trigger."""
        for text in ADVERSARIAL_TEXT:
            if not text:
                continue
            with self.subTest(text=repr(text)):
                cell = emit.neutralize_csv_text(text)
                self.assertNotIn(
                    cell[0],
                    emit.CSV_FORMULA_TRIGGERS,
                    f"{cell!r} would still be evaluated as a formula",
                )

    def test_restore_leaves_a_cell_the_rule_could_not_have_written_alone(self) -> None:
        """An apostrophe leading ordinary text is a value, not an escape."""
        self.assertEqual(emit.restore_csv_text("'ordinary text"), "'ordinary text")
        self.assertEqual(emit.restore_csv_text("ordinary text"), "ordinary text")
        self.assertEqual(emit.restore_csv_text(""), "")
        self.assertEqual(emit.restore_csv_text("'"), "'")

    def adversarial_rows(self) -> list[dict[str, Any]]:
        """One row per adversarial value, in the two text fields most likely to carry it."""
        rows: list[dict[str, Any]] = []
        for index, text in enumerate(ADVERSARIAL_TEXT):
            rows.append(
                build_row(
                    message=text or "a message that cannot be empty",
                    rule_id=text or "a-rule-id-that-cannot-be-empty",
                    severity_native=text or None,
                    # One-based, because a present start_line is at least 1 (emit.py's
                    # final validator). The line number is incidental here -- these rows
                    # exist to carry adversarial TEXT -- but a row this module publishes
                    # still has to be a row the dataset would accept, or the neutralisation
                    # assertions would be made against a document the writer refuses.
                    start_line=index + 1,
                )
            )
        return rows

    def test_the_written_file_is_inert_and_the_dataset_still_says_what_it_said(self) -> None:
        """Both halves at once, over a published pair rather than over the functions."""
        rows = self.adversarial_rows()
        publication = emit.publish_findings(rows, self.json_path, self.csv_path)
        self.assertTrue(publication.comparison.passed)

        # What a spreadsheet sees: no data cell in any text column leads with a trigger.
        for index, cells in enumerate(raw_csv_cells(self.csv_path)):
            for field, cell in cells.items():
                with self.subTest(row=index, field=field):
                    if cell:
                        self.assertNotIn(cell[0], emit.CSV_FORMULA_TRIGGERS)

        # What the dataset says: the tools' own text, in both files, field for field.
        self.assertEqual(emit.read_findings_json(self.json_path), rows)
        self.assertEqual(emit.read_findings_csv(self.csv_path), rows)

    def test_findings_json_is_never_neutralised(self) -> None:
        """The JSON carries every value exactly as the adapter produced it."""
        rows = self.adversarial_rows()
        emit.publish_findings(rows, self.json_path, self.csv_path)
        document = json.loads(self.json_path.read_text(encoding="utf-8"))
        self.assertEqual([row["message"] for row in document], [row["message"] for row in rows])

    def test_the_recorded_count_is_the_number_of_cells_actually_escaped(self) -> None:
        """The tally is recounted from the file, so it cannot drift from the bytes."""
        rows = self.adversarial_rows()
        publication = emit.publish_findings(rows, self.json_path, self.csv_path)
        tally = publication.csv_neutralisation

        observed = 0
        text_fields = [f for f in emit.FIELDS if f not in ("start_line", "in_scope")]
        for cells in raw_csv_cells(self.csv_path):
            for field in text_fields:
                cell = cells[field]
                if emit.restore_csv_text(cell) != cell:
                    observed += 1
        self.assertEqual(tally["cells_escaped"], observed)
        self.assertGreater(observed, 0, "the corpus must exercise the rule at all")
        self.assertEqual(
            tally["cells_examined"],
            sum(1 for cells in raw_csv_cells(self.csv_path) for f in text_fields if cells[f]),
        )
        self.assertEqual(sum(tally["escaped_by_field"].values()), observed)
        self.assertEqual(sum(tally["escaped_by_leading_character"].values()), observed)
        json.dumps(tally)

    def test_the_header_and_the_non_text_columns_are_untouched(self) -> None:
        """The twelve field names, the boolean by name, the integer as an integer."""
        rows = [
            build_row(message="=formula", in_scope=True, start_line=1),
            build_row(message="ordinary", in_scope=False, start_line=None),
        ]
        emit.publish_findings(rows, self.json_path, self.csv_path)
        with self.csv_path.open("r", encoding="utf-8", newline="") as handle:
            records = list(csv.reader(handle))
        self.assertEqual(records[0], list(emit.FIELDS))
        self.assertEqual(records[1][emit.FIELDS.index("in_scope")], emit.CSV_TRUE)
        self.assertEqual(records[2][emit.FIELDS.index("in_scope")], emit.CSV_FALSE)
        self.assertEqual(records[1][emit.FIELDS.index("start_line")], "1")
        self.assertEqual(records[2][emit.FIELDS.index("start_line")], emit.CSV_ABSENT)

    def test_absence_is_still_an_empty_cell_beside_a_json_null(self) -> None:
        """The rule must not turn an absent value into a one-character cell."""
        rows = [build_row(severity_native=None, cwe=None, cve=None, package_coordinate=None)]
        emit.publish_findings(rows, self.json_path, self.csv_path)
        cells = raw_csv_cells(self.csv_path)[0]
        for field in ("severity_native", "cwe", "cve", "package_coordinate"):
            with self.subTest(field=field):
                self.assertEqual(cells[field], emit.CSV_ABSENT)
        self.assertEqual(emit.read_findings_csv(self.csv_path)[0]["severity_native"], None)

    def test_a_row_whose_path_leads_with_a_trigger_is_still_never_absolute(self) -> None:
        """The rule changes a cell's leading character; it never launders a path."""
        with self.assertRaises(emit.EmitError):
            emit.publish_findings(
                [build_row(path="/absolute/path.scala")], self.json_path, self.csv_path
            )
        # A relative path beginning with a trigger is legitimate and is neutralised.
        rows = [build_row(path="-odd-directory/src/main/x.scala")]
        emit.publish_findings(rows, self.json_path, self.csv_path)
        self.assertEqual(
            raw_csv_cells(self.csv_path)[0]["path"],
            "'-odd-directory/src/main/x.scala",
        )
        self.assertEqual(emit.read_findings_csv(self.csv_path), rows)


class CommittedDatasetTests(unittest.TestCase):
    """The real dataset, read and re-rendered — the proof the change costs nothing (F55).

    The adversarial corpus above proves the rule works. This class proves the rule leaves
    the committed deliverable exactly as it is: no cell in the 9,430 committed rows begins
    with a trigger, so re-rendering them produces the same bytes, and the CSV that was
    written before the rule existed still round-trips under the reader that now reverses
    it. Nothing here writes to the deliverables.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Read both committed files once for the whole class."""
        if not FINDINGS_JSON.is_file() or not FINDINGS_CSV.is_file():
            raise unittest.SkipTest(
                f"blocking gap: {FINDINGS_JSON} or {FINDINGS_CSV} is absent, so the "
                "committed dataset cannot be asserted over. Reported, not silently passed."
            )
        cls.json_rows = emit.read_findings_json(FINDINGS_JSON)
        cls.csv_rows = emit.read_findings_csv(FINDINGS_CSV)

    def test_the_committed_files_agree_field_for_field(self) -> None:
        """The typed comparison over the deliverables as they stand."""
        comparison = emit.compare_outputs(FINDINGS_JSON, FINDINGS_CSV)
        self.assertTrue(comparison.passed, msg=str(comparison.first_mismatch))
        self.assertEqual(self.json_rows, self.csv_rows)

    def test_the_committed_dataset_carries_the_recorded_row_count(self) -> None:
        """9,430 rows, counted by parsing rather than by counting lines."""
        self.assertEqual(len(self.json_rows), COMMITTED_ROW_COUNT)
        self.assertEqual(len(self.csv_rows), COMMITTED_ROW_COUNT)
        physical_lines = len(FINDINGS_CSV.read_text(encoding="utf-8").splitlines())
        self.assertNotEqual(
            physical_lines - 1,
            COMMITTED_ROW_COUNT,
            "a line count must be demonstrably wrong here, or the parsed count proves "
            "nothing about how it was taken",
        )

    def test_no_committed_cell_needs_neutralising(self) -> None:
        """So the rule changes no byte of the deliverable, which is the claim to check."""
        offenders = [
            (index, field, value)
            for index, row in enumerate(self.json_rows)
            for field, value in row.items()
            if isinstance(value, str)
            and value
            and value[0] in emit.CSV_FORMULA_TRIGGERS
        ]
        self.assertEqual(offenders, [], "committed cells leading with a formula trigger")

    def test_re_rendering_the_committed_rows_reproduces_both_files_byte_for_byte(self) -> None:
        """The strongest available statement that the writer's change is a no-op here."""
        with tempfile.TemporaryDirectory(prefix="blitzy-emit-rerender-") as directory:
            output = Path(directory)
            publication = emit.publish_findings(
                self.json_rows, output / "findings.json", output / "findings.csv"
            )
            self.assertEqual(publication.csv_neutralisation["cells_escaped"], 0)
            self.assertGreater(publication.csv_neutralisation["cells_examined"], 0)
            for name, committed in (
                ("findings.json", FINDINGS_JSON),
                ("findings.csv", FINDINGS_CSV),
            ):
                with self.subTest(member=name):
                    self.assertEqual(
                        (output / name).read_bytes(),
                        committed.read_bytes(),
                        f"re-rendering the committed rows changed {name}",
                    )

    def test_no_committed_row_carries_an_absolute_path(self) -> None:
        """AAP 0.8.2, measured over the dataset rather than asserted about it."""
        summary = emit.validation_summary(self.json_rows)
        self.assertEqual(summary["absolute_paths"], 0)
        self.assertEqual(summary["rows"], COMMITTED_ROW_COUNT)
        self.assertTrue(summary["passed"], msg=str(summary["violations"]))


class RunRecordMeasurementTests(TemporaryOutputMixin):
    """Every file the run record names is measured, and every null says why (F36).

    The record's own words are *"every file it names ... carries that file's byte size and
    sha256"*, and a record that claims a measurement it did not take is worse than one
    that claims less: a reader who checks one entry and finds it populated has no reason
    to suspect the next. So the assertions below are about the two halves of that claim
    together — the measurement is present wherever it can be taken, and where it cannot
    the entry says so in the same breath.
    """

    def write_stream(self, name: str, text: str) -> Path:
        """Write one runner stream into the temporary directory and return its path."""
        path = self.output / name
        path.write_text(text, encoding="utf-8")
        return path

    def test_a_present_file_always_carries_both_measurements(self) -> None:
        """Size and digest, and the digest equals an independently computed sha256."""
        path = self.write_stream("opengrep.stdout.log", "a runner's own words\n")
        record = cli._file_record(path)
        self.assertTrue(record["present"])
        self.assertEqual(record["bytes"], path.stat().st_size)
        self.assertEqual(record["sha256"], sha256_of(path))
        self.assertNotIn("null_reason", record)

    def test_an_empty_file_is_measured_rather_than_treated_as_absent(self) -> None:
        """Zero bytes is a measurement: the digest of the empty string, not a null."""
        path = self.write_stream("checkov.stderr.log", "")
        record = cli._file_record(path)
        self.assertTrue(record["present"])
        self.assertEqual(record["bytes"], 0)
        self.assertEqual(record["sha256"], hashlib.sha256(b"").hexdigest())

    def test_an_absent_file_says_present_false_and_why_the_nulls_are_correct(self) -> None:
        """The one case where a null measurement is right, stated as such."""
        record = cli._file_record(self.output / "never-written.log")
        self.assertFalse(record["present"])
        self.assertIsNone(record["bytes"])
        self.assertIsNone(record["sha256"])
        self.assertIn("no file exists at this path", record["null_reason"])

    def test_a_stream_is_digested_whether_or_not_its_words_are_embedded(self) -> None:
        """The defect F36 names, asserted in the direction it failed.

        The digest used to be taken only when the tool's own words were needed, which is
        why sixteen present streams carried a byte size beside a null digest. Whether the
        words are embedded is a question about the classification; whether the file can be
        digested is a question about the file.
        """
        path = self.write_stream("semgrep.stdout.log", "a large stream's first line\n")
        for with_text in (False, True):
            with self.subTest(with_text=with_text):
                record = cli._stream_record(path, with_text=with_text)
                self.assertEqual(record["bytes"], path.stat().st_size)
                self.assertEqual(record["sha256"], sha256_of(path))

    def test_an_unembedded_stream_states_why_its_text_is_null(self) -> None:
        """A null ``text`` is explained, so it cannot read as a lost excerpt."""
        path = self.write_stream("trivy.stdout.log", "feed evidence\n")
        record = cli._stream_record(path, with_text=False)
        self.assertIsNone(record["text"])
        self.assertFalse(record["text_truncated"])
        self.assertIn("retained verbatim on disk", record["text_null_reason"])

    def test_an_embedded_stream_carries_its_words_and_is_bounded(self) -> None:
        """The absent-artifact case: the tool's own words, capped and flagged when cut."""
        short = self.write_stream("osv-scanner.stdout.log", "No package sources found.\n")
        record = cli._stream_record(short, with_text=True)
        self.assertEqual(record["text"], "No package sources found.\n")
        self.assertFalse(record["text_truncated"])
        self.assertNotIn("text_null_reason", record)

        oversized = self.write_stream(
            "osv-scanner.stderr.log", "x" * (cli.TOOL_WORDS_EXCERPT_LIMIT + 500)
        )
        cut = cli._stream_record(oversized, with_text=True)
        self.assertTrue(cut["text_truncated"])
        self.assertEqual(len(cut["text"]), cli.TOOL_WORDS_EXCERPT_LIMIT)
        self.assertEqual(cut["text_excerpt_limit"], cli.TOOL_WORDS_EXCERPT_LIMIT)
        # The bound loses nothing silently: the file itself is still measured whole.
        self.assertEqual(cut["bytes"], oversized.stat().st_size)
        self.assertEqual(cut["sha256"], sha256_of(oversized))

    def test_no_digest_is_skippable_by_argument(self) -> None:
        """``_file_record`` takes no ``digest`` switch: that switch was the defect."""
        with self.assertRaises(TypeError):
            cli._file_record(self.output, digest=False)  # type: ignore[call-arg]

    def test_a_tool_with_no_log_directory_names_no_file_and_says_so(self) -> None:
        """Nulls that describe an entry naming nothing, rather than an unmeasured file."""
        words = cli._tool_words(None, "osv-scanner", with_text=True)
        for stream in ("stdout", "stderr"):
            with self.subTest(stream=stream):
                entry = words["streams"][stream]
                self.assertIsNone(entry["path"])
                self.assertFalse(entry["present"])
                self.assertIn("names no file", entry["null_reason"])

    def test_the_records_claim_and_its_data_agree(self) -> None:
        """The skeleton's stated contract, and a walk over every entry it produces.

        The walk is the substance: it visits every mapping in the record that carries a
        ``sha256`` key and asserts the pairing the claim promises — a present file has
        both measurements and no ``null_reason``, an absent one has neither measurement
        and a reason. A future entry added without a measurement is caught here.
        """
        record = cli._new_record([], "2026-01-01T00:00:00Z")
        self.assertIn("byte size and its sha256", record["publication"])
        self.assertIn("null_reason", record["publication"])
        measurement = record["file_measurement"]
        self.assertEqual(measurement["fields"], ["path", "present", "bytes", "sha256"])
        self.assertEqual(measurement["text_excerpt_limit"], cli.TOOL_WORDS_EXCERPT_LIMIT)

        present = self.write_stream("gitleaks.stdout.log", "one line\n")
        record["artifacts"] = [
            cli._file_record(present),
            cli._file_record(self.output / "absent.log"),
            cli._stream_record(present, with_text=False),
            cli._stream_record(self.output / "absent.log", with_text=True),
            cli._tool_words(self.output, "gitleaks", with_text=False),
            cli._tool_words(None, "osv-scanner", with_text=True),
        ]
        entries = list(self.walk_file_entries(record))
        self.assertGreaterEqual(len(entries), 8, "the walk must reach every entry")
        for where, entry in entries:
            with self.subTest(entry=where):
                if entry.get("present"):
                    self.assertIsInstance(entry["bytes"], int)
                    self.assertRegex(entry["sha256"], r"\A[0-9a-f]{64}\Z")
                    self.assertNotIn("null_reason", entry)
                else:
                    self.assertIsNone(entry["bytes"])
                    self.assertIsNone(entry["sha256"])
                    self.assertTrue(entry["null_reason"].strip())

    def walk_file_entries(self, value: Any, where: str = "") -> Any:
        """Yield every ``(location, mapping)`` in ``value`` that describes one file.

        A file entry is a mapping carrying both ``sha256`` and ``present`` — the same
        shape a reader of the record would look for, so this walk finds what they would
        find rather than the keys this test happens to know about.
        """
        if isinstance(value, dict):
            if "sha256" in value and "present" in value:
                yield where or "<root>", value
            for key, nested in value.items():
                yield from self.walk_file_entries(nested, f"{where}/{key}")
        elif isinstance(value, list):
            for index, nested in enumerate(value):
                yield from self.walk_file_entries(nested, f"{where}[{index}]")


if __name__ == "__main__":  # pragma: no cover - the module is normally run by discovery
    unittest.main()
