"""harness/lib/normalize/emit.py — both writers, and the proof they agree.

Purpose, verbatim from AAP 0.6.1: "Both writers, the shared absence convention,
the no-absolute-path assertion, and the typed re-parse comparison."

This module is a leaf of the normalizer. It imports nothing from the ``normalize``
package and nothing outside the CPython standard library, so the adapter tests can
exercise it on its own with no scanner artifact, no graph and no environment file
present.

THE TWELVE FIELDS, IN ORDER (AAP 0.8.2 — "Exactly twelve fields, in the request's
order, in both output files"):

    tool, scanner_class, rule_id, message, severity_native, severity_norm,
    path, start_line, cwe, cve, package_coordinate, in_scope

``FIELDS`` below is the single place that order is written down. Every writer,
reader and comparison in this module iterates it, so no thirteenth field, no
renamed field and no reordering can drift in from anywhere else.

ROW-ONLY, NO METADATA ENVELOPE (AAP 0.5.4). ``findings.json`` is a top-level JSON
array of row objects — never ``{"rows": [...]}``, never a metadata wrapper.
``findings.csv`` is one header row plus one row per finding. A tool that produced
no row is by construction invisible in both files; the nine-tool inventory lives in
``tool-status.md`` and ``severity-map.md`` and is never smuggled in here.

THE ABSENCE CONVENTION (AAP 0.5.4): "Absence is JSON ``null`` and an empty CSV
field, and the two agree row for row." Absence is permitted for exactly five fields
— ``severity_native``, ``start_line``, ``cwe``, ``cve``, ``package_coordinate``
(AAP 0.8.2). ``path`` and ``severity_norm`` may NEVER be absent: a record whose
path could not be resolved was rejected and counted upstream, so a row arriving
here with a null ``path`` or a null ``severity_norm`` is a programming error and
raises rather than being written. That assertion is cheap, and it is the last line
of defence on the dataset's two mandatory fields.

TYPES ON THE WIRE. ``in_scope`` is a JSON boolean, and its CSV literals are exactly
lowercase ``true`` / ``false`` — deliberately written, never Python's ``True`` /
``False`` from stringifying a bool. ``start_line`` is a JSON integer or ``null``,
and an integer or an empty CSV field — never a string, and never ``0`` standing in
for absence. A present ``start_line`` is at least **1**: line numbering is one-based
in every producer this dataset reads, so ``0`` is either an absence written as a
sentinel or a producer's off-by-one, and the final validator refuses both rather than
writing a row that asserts something untrue about the scanned code.

THE CSV IS NEUTRALISED AGAINST SPREADSHEET FORMULAS, REVERSIBLY (CWE-1236). Every
text-valued CSV cell carries text a scanner chose, and a cell beginning with ``=``,
``+``, ``-``, ``@``, a tab or a carriage return is *evaluated* rather than displayed
when the file is opened interactively. So the writer prepends one ``'`` to a text
cell whose first character is one of those — or is ``'`` itself, which is what makes
the inverse unambiguous — and :func:`read_findings_csv` removes exactly one. The rule,
the argument that it is exactly reversible for every input, and the disclosure it
requires are stated in full beside :data:`CSV_FORMULA_ESCAPE`. ``findings.json`` is
never neutralised: it carries every value exactly as the adapter produced it, the
inverse is applied when the CSV is read back, and the two files therefore still agree
field for field under the typed comparison. The rule and the number of cells it
changed are recorded in ``normalize-run.json``, because a deliverable whose bytes can
differ from a tool's literal text has to say so.

NO ABSOLUTE PATH IS EVER EMITTED (AAP 0.8.2), "including for archive members and
other non-filesystem coordinates". Every row's ``path`` is asserted before either
file is opened: no leading slash, no Windows drive prefix, no URI scheme. A
violation raises and names the row; it is never repaired here, because stripping a
prefix would hide a resolver bug in ``paths.py`` and silently change what the row
means. Two shapes are legitimate and must pass: the archive-member form
``<container-relative-to-root>!<member-path>``, and a path carrying preserved
``../`` segments for a container outside the root — the SARIF 2.1.0 errata forbid
normalizing ``..`` out of a path, so ``..`` is correct rather than a defect.

EQUALITY IS ASSERTED BY PARSING BOTH FILES (AAP 0.5.4). Both files are written from
the same validated in-memory rows — neither is derived from the other after writing
— then both are read back from disk, the CSV rows are coerced (``start_line`` to an
int or ``None``, ``in_scope`` to a bool from the literal written, every empty
optional field to ``None``, and the spreadsheet neutralisation below reversed) and
the two are compared in order, row by row and field by field, reporting the FIRST
mismatch with its row index and field name. Nothing here counts lines, in code or in
any message: the historical dataset carried 10,178 parsed rows over 12,762 physical
lines because ``message`` fields carry embedded newlines, so a line count
over-reports by about a quarter. The comparison is returned as data for ``cli.py`` to
serialise into ``harness/artifacts/logs/normalize-run.json``; this module prints
nothing.

PUBLICATION IS SECURE AND ATOMIC, AND THE TWO FILES ARE ONE GENERATION. The dataset
is two files that must describe one row list, so they are published together or not
at all: both members are staged, both are validated by being parsed back, both are
fsynced, and only then is either moved into place. A fault anywhere before the first
move leaves both previous deliverables exactly as they were, rather than this run's
``findings.json`` beside the previous run's ``findings.csv``. Both members of one
publication carry one content-derived publication identifier
(:data:`PUBLICATION_IDENTIFIER_METHOD`), recorded in the run record so a consumer can
recompute it from the two files and detect a mixed generation.

Every byte reaches the disk through the same guarded sequence, and there is no
fallback that writes a target directly (CWE-59, CWE-367): the parent directory is
required to be its own realpath, so a symlinked path component is refused rather than
written through; the staged file is created in that same directory under an
unpredictable name with ``O_CREAT|O_EXCL|O_WRONLY|O_NOFOLLOW``, so a pre-planted path
or a concurrent writer makes the creation fail instead of redirecting it; the bytes
are written through that descriptor, flushed and ``fsync``ed; the file is then
measured by reading it back; and publication is one ``os.replace`` within the
directory followed by an ``fsync`` of the directory itself. On any failure the staged
file is unlinked and the exception propagates — nothing is swallowed and no partial
file is left behind. :func:`staging_protocol` returns that contract as data so the
run record publishes it beside the result rather than asserting it in prose.

DIVISION OF LABOUR (AAP 0.6.4). ``reconcile.py`` owns the row counts and compares
the parsed JSON and parsed CSV counts to the reconciliation identity separately.
This module owns field-level equality, and reports a length difference only as the
reason its ordered walk could not run past the shorter sequence — "a count that
appears in two documents must be one measurement cited twice, never two
measurements".

ROWS ARE WRITTEN IN THE ORDER GIVEN. No sort, no ranking, no summary row, and no
deduplication, ever (AAP 0.3.2): two byte-identical rows from two tools are two
rows, and this module adds nothing to the relationship between them.

Public API
    FIELDS, OPTIONAL_FIELDS, REQUIRED_FIELDS   the schema contract
    CSV_TRUE, CSV_FALSE, CSV_ABSENT            the CSV literals, by name
    CSV_FORMULA_ESCAPE, CSV_FORMULA_TRIGGERS   the neutralisation scheme
    neutralize_csv_text(value)                 the forward rule
    restore_csv_text(cell)                     its exact inverse
    csv_neutralisation_rule()                  the scheme, as data
    CsvNeutralisationTally                     what the rule changed, counted
    EmitError                                  every fault raised here
    ComparisonFailed                           the staged pair disagreed; nothing published
    Mismatch, ComparisonResult                 the comparison, as data
    MISMATCH_FIELD_VALUE,                      the two mismatch kinds, by name
    MISMATCH_ROW_SEQUENCE_LENGTH
    validate_rows(rows)                        -> ordered, checked rows
    write_findings_json(rows, path)            the JSON writer
    write_findings_csv(rows, path)             the CSV writer
    write_findings(rows, json_path, csv_path)  both, from one row list
    publish_findings(rows, json, csv)          -> PublicationResult
    publish_document(path, render, role=...)   one document, same protocol
    PublicationResult, PublicationMember       the publication, as data
    PUBLICATION_SCHEME, PUBLICATION_ROLES,     the publication contract
    PUBLICATION_IDENTIFIER_METHOD,
    ROLE_FINDINGS_JSON, ROLE_FINDINGS_CSV
    publication_identifier(digests)            -> the shared identifier
    staging_protocol()                         the write protocol, as data
    read_findings_json(path)                   -> parsed rows
    read_findings_csv(path)                    -> typed-coerced rows
    compare_outputs(json_path, csv_path)       -> ComparisonResult
    emit_findings(rows, json_path, csv_path)   write both, then compare
"""

import csv
import errno
import hashlib
import json
import os
import re
import secrets
import stat as stat_module
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field as dataclass_field
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import IO, Any, TextIO

__all__ = [
    "CSV_ABSENT",
    "CSV_FALSE",
    "CSV_FORMULA_ESCAPE",
    "CSV_FORMULA_TRIGGERS",
    "CSV_TRUE",
    "ComparisonFailed",
    "ComparisonResult",
    "CsvNeutralisationTally",
    "EmitError",
    "FIELDS",
    "MISMATCH_FIELD_VALUE",
    "MISMATCH_ROW_SEQUENCE_LENGTH",
    "Mismatch",
    "OPTIONAL_FIELDS",
    "PUBLICATION_IDENTIFIER_METHOD",
    "PUBLICATION_ROLES",
    "PUBLICATION_SCHEME",
    "REQUIRED_FIELDS",
    "ROLE_FINDINGS_CSV",
    "ROLE_FINDINGS_JSON",
    "PublicationMember",
    "PublicationResult",
    "compare_outputs",
    "open_verified_member",
    "csv_neutralisation_rule",
    "emit_findings",
    "neutralize_csv_text",
    "publication_identifier",
    "member_set_identifier",
    "require_publication_manifest",
    "require_dataset_generation",
    "MANIFEST_SCHEME",
    "MANIFEST_COMMIT_PROTOCOL",
    "MANIFEST_CONSUMER_REQUIREMENT",
    "MEMBER_SET_IDENTIFIER_METHOD",
    "ROLE_COMPLETION_MANIFEST",
    "publish_document",
    "publish_findings",
    "read_findings_csv",
    "read_findings_json",
    "restore_csv_text",
    "staging_protocol",
    "validate_rows",
    "validation_summary",
    "write_findings",
    "write_findings_csv",
    "write_findings_json",
    "StagedWrite",
    "UnsafeOutputPath",
    "assert_safe_output_path",
    "stage_text",
    "promote_staged",
    "discard_staged",
]

# The twelve fields, in the request's order (AAP 0.8.2). This tuple is the
# contract: it is the only place the order is written down, and it is the
# iteration order of every writer, reader and comparison below.
FIELDS: tuple[str, ...] = (
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
)

# Absence is permitted for exactly these five fields and no others (AAP 0.8.2).
OPTIONAL_FIELDS: frozenset[str] = frozenset(
    {"severity_native", "start_line", "cwe", "cve", "package_coordinate"}
)

# The seven fields that must always carry a value. `path` and `severity_norm` are
# named in AAP 0.8.2 as never absent; the other five are required because the
# absence convention has no room for them — an empty CSV cell means `null`, so an
# empty required field would make the round trip lossy rather than merely odd.
REQUIRED_FIELDS: tuple[str, ...] = tuple(f for f in FIELDS if f not in OPTIONAL_FIELDS)

# The two fields carrying a non-string type on the wire.
_BOOLEAN_FIELD = "in_scope"
_INTEGER_FIELD = "start_line"

_TEXT_FIELDS: tuple[str, ...] = tuple(
    f for f in FIELDS if f not in {_BOOLEAN_FIELD, _INTEGER_FIELD}
)
_REQUIRED_TEXT_FIELDS: frozenset[str] = frozenset(
    f for f in _TEXT_FIELDS if f not in OPTIONAL_FIELDS
)
_OPTIONAL_TEXT_FIELDS: frozenset[str] = frozenset(
    f for f in _TEXT_FIELDS if f in OPTIONAL_FIELDS
)

# The CSV literals, named so no caller has to guess them and no `str(bool)` can
# leak `True`/`False` into the column (AAP 0.5.4, and the historical dataset's
# `in_scope` column holds exactly {'true','false'}).
CSV_TRUE = "true"
CSV_FALSE = "false"
CSV_ABSENT = ""

# A Unix line terminator for the CSV, set explicitly because the csv module's
# default dialect writes CRLF. Embedded newlines inside a quoted `message` are
# preserved verbatim either way; this governs only the row separator.
_CSV_LINE_TERMINATOR = "\n"

# ------------------------------------------------------------------------- #
# The spreadsheet-formula neutralisation (CWE-1236)
#
# Every text field in this dataset carries text a scanner chose: a rule's
# message, its identifier, a path a tool reported. A cell beginning with any of
# the characters below is evaluated as a formula by the common spreadsheet
# applications when the CSV is opened interactively, so a message that begins
# `=cmd|...` or `+HYPERLINK(...)` becomes executable content in a deliverable
# whose whole purpose is to record, verbatim, what a tool said.
#
# THE RULE, in full, and it is one sentence: a text cell whose first character
# is a trigger OR the escape character itself is written with one escape
# character prepended, and every other cell is written unchanged.
#
# THE INVERSE, also one sentence: a cell that begins with the escape character
# AND whose second character is a trigger or the escape character has exactly
# one leading escape character removed, and every other cell is read unchanged.
#
# WHY THAT IS EXACTLY REVERSIBLE, for every input including the awkward ones.
# Write E for the rule, D for the inverse, T for the trigger set and X for the
# escape character. Two cases exhaust every string s:
#   * s is empty, or s[0] is not in T ∪ {X}. Then E(s) = s. D(s) = s too,
#     because s cannot begin with X: X ∈ T ∪ {X}, which this case excludes.
#   * s[0] IS in T ∪ {X}. Then E(s) = X + s, whose first character is X and
#     whose second is s[0] ∈ T ∪ {X}, so D strips exactly one X and returns s.
# So D(E(s)) = s for every s. E is also injective: its output is either s
# (first case) or X + s (second), and the two cannot collide, since a first-case
# s cannot begin with X while every second-case output does. The awkward input —
# a cell whose text genuinely begins with the escape character — is therefore
# handled by escaping it as well, which is precisely what removes the ambiguity:
# without that, `'=x` written by a tool and `=x` neutralised by this module
# would be the same bytes and the inverse would have to guess.
#
# WHY THIS ESCAPE CHARACTER. A leading apostrophe is the format's own
# text-literal marker: spreadsheets consume it and display the remainder as
# text, so a neutralised cell reads correctly for a human as well as being inert.
# It is also plain ASCII, so it cannot interact with the UTF-8 encoding.
#
# WHAT IS NOT TOUCHED. The header row (the twelve field names, none of which
# begins with a trigger), the `in_scope` literals, the `start_line` integer, and
# the empty cell that means absence. Quoting, escaping of embedded quotes and
# newlines, and the row terminator all stay the csv module's business: this rule
# changes a cell's leading character and nothing else about the file.
#
# THE DISCLOSURE. Because the CSV's bytes may then differ from the tool's
# literal text, the rule and the number of cells it affected are recorded in
# harness/artifacts/logs/normalize-run.json (see `csv_neutralisation_rule` and
# `CsvNeutralisationTally`), findings.json is never neutralised, and the inverse
# is applied when the CSV is read back, so the two files still agree field for
# field under the typed comparison.
# ------------------------------------------------------------------------- #

#: The single character prepended to neutralise a cell, and the character the
#: inverse strips. A leading apostrophe is the CSV/spreadsheet convention for
#: "the rest of this cell is text".
CSV_FORMULA_ESCAPE = "'"

#: The leading characters that make a spreadsheet evaluate a cell rather than
#: display it. `=` and `+` open a formula; `-` opens one via a signed
#: expression; `@` introduces a function or a name; a leading tab or carriage
#: return can shift the cell's content into a position where the character after
#: it becomes leading. Fixed as a tuple so the recorded rule and the code that
#: applies it cannot disagree.
CSV_FORMULA_TRIGGERS: tuple[str, ...] = ("=", "+", "-", "@", "\t", "\r")

#: The characters that must be escaped when they lead a cell: every trigger, and
#: the escape character itself — the latter being what makes the inverse
#: unambiguous rather than a guess.
_CSV_ESCAPE_LEADERS: frozenset[str] = frozenset(CSV_FORMULA_TRIGGERS) | {
    CSV_FORMULA_ESCAPE
}

# `int()` accepts more than a faithful round trip should: int("3_0") == 30,
# int(" 4") == 4 and int("+5") == 5 all succeed. A CSV cell that was not written
# by this module's writer must therefore fail rather than be silently coerced into
# agreement, so the integer cell is matched exactly before it is converted.
_START_LINE_CELL_RE = re.compile(r"\A[1-9][0-9]*\Z")

# A URI with an explicit authority: file://, http://, jar:file:// and friends.
_URI_AUTHORITY_RE = re.compile(r"\A[A-Za-z][A-Za-z0-9+.\-]*://")

# Schemes seen in scanner output that carry no authority — `jar:file:/x!/y`,
# `file:/x`, `zip:...`. Matched by name so an ordinary relative path whose first
# segment happens to contain a colon is not mistaken for a URI.
_URI_SCHEME_RE = re.compile(
    r"\A(?:file|jar|zip|tar|gz|jimple|https?|ftps?|sftp|s3|gs|urn|data|classpath):",
    re.IGNORECASE,
)

# `C:\x`, `C:/x` and a bare `C:` are all filesystem-absolute on Windows.
_WINDOWS_DRIVE_RE = re.compile(r"\A[A-Za-z]:(?:[\\/]|\Z)")

# Mismatch kinds, named so cli.py can record which of the two conditions the
# comparison found without matching on prose.
MISMATCH_FIELD_VALUE = "field_value"
MISMATCH_ROW_SEQUENCE_LENGTH = "row_sequence_length"

# How many individual violations `validation_summary` carries in full. The count is
# always exact; the list is bounded so a systematically malformed row set cannot turn
# the run record into a copy of the dataset.
_VALIDATION_VIOLATION_LIMIT = 20

# The two members of one dataset publication, named so the role travels with the
# member in the run record rather than being inferred from a filename a caller
# chose. Both members of one publication carry the same publication identifier.
ROLE_FINDINGS_JSON = "findings_json"
ROLE_FINDINGS_CSV = "findings_csv"
PUBLICATION_ROLES: tuple[str, ...] = (ROLE_FINDINGS_JSON, ROLE_FINDINGS_CSV)

# The versioned name of the publication contract below. It is the first line of the
# identifier's digest material, so an identifier computed under a future scheme can
# never collide with one computed under this scheme.
ROLE_COMPLETION_MANIFEST = "completion_manifest"

#: Schema the completion manifest carries, so a consumer recognises it by content
#: rather than by filename.
MANIFEST_SCHEME = "findings-publication-manifest/1.0.0"

#: Hex characters of the member-set identifier. Long enough that two distinct member
#: sets colliding is not a practical concern, short enough to quote in prose.
MEMBER_SET_IDENTIFIER_LENGTH = 32

MEMBER_SET_IDENTIFIER_METHOD = (
    "sha256 over each published member's role and content sha256 -- NUL-separated "
    "within a member, newline-separated between members, in the order the manifest "
    "lists them -- truncated to 32 lowercase hex characters. Computable by a consumer "
    "from nothing but the files on disk, which is what makes it a completion record: "
    "a set missing a member, or assembled from two generations, yields a value "
    "matching neither generation's manifest."
)

MANIFEST_COMMIT_PROTOCOL = (
    "Every content member is staged in its target's own directory, fsynced, and "
    "measured by reading the staged bytes back through a descriptor bound to the "
    "inode they were written into. This manifest is then staged the same way. Every "
    "content member is renamed onto its target, and this manifest is renamed LAST. "
    "Staging before the first rename closes the window before the renames; N renames "
    "are N atomic operations rather than one, and POSIX offers no N-way atomic "
    "rename, so the window BETWEEN them is closed by this record instead: its "
    "presence means every member it names is in place."
)

MANIFEST_CONSUMER_REQUIREMENT = (
    "REQUIRED. Before treating the members as one generation a consumer must require "
    "this file, re-measure every member it names, and recompute member_set_id from "
    "those digests. normalize.emit.require_publication_manifest is that check, and "
    "the publication runs it itself immediately after publishing, so a manifest "
    "nothing verifies cannot occur on the producing side either."
)

PUBLICATION_SCHEME = "findings-publication/1.0.0"

# How the publication identifier is derived, stated once here so the run record can
# carry the derivation beside the value and a consumer can recompute it from the two
# published files alone. It is deliberately CONTENT-DERIVED rather than random: a
# random identifier would make two runs over identical artifacts disagree in the run
# record, and the AAP's determinism claim ("two runs over identical artifacts produce
# byte-identical files") is asserted against exactly that.
PUBLICATION_IDENTIFIER_METHOD = (
    "sha256 over the UTF-8 text '" + PUBLICATION_SCHEME + "\\n' followed by one "
    "'<role>\\t<sha256>\\n' line per member in ascending role order, truncated to its "
    "first 32 hex characters. Content-derived, so both members of one publication "
    "carry it, two runs over identical rows produce the same value, and a consumer "
    "holding the two files can recompute it: a mixed generation yields an identifier "
    "that matches neither recorded value."
)

# How many hex characters of the publication digest the identifier carries. 32 hex
# characters are 128 bits, which is far beyond what distinguishing two generations of
# one dataset requires, and the full member digests sit beside it either way.
_PUBLICATION_ID_LENGTH = 32

# Bytes read per hashing step when a staged file is digested back off the disk.
# The dataset runs to a few megabytes, so one megabyte per step keeps the peak
# resident size flat without making the loop's overhead measurable.
_DIGEST_CHUNK = 1 << 20

# Bytes of randomness in a staged file's name. The name must be unpredictable so a
# concurrent process cannot pre-create or pre-symlink the path this writer is about
# to use (CWE-59/CWE-367); 96 bits removes any prospect of guessing it, and the
# exclusive no-follow creation below is what makes a guess useless anyway.
_TEMP_NAME_RANDOM_BYTES = 12

# The permissions a staged file is created with. Set explicitly rather than left to
# `open()`'s 0o666-and-umask, so a published deliverable is never group- or
# world-writable however the invoking shell's umask happens to be set.
_STAGED_FILE_MODE = 0o644

# The open flags every staged file is created with (CWE-59/CWE-367):
#   O_CREAT|O_EXCL  the call fails if the path exists at all -- including as a
#                   symlink -- so a planted path cannot be written through, and two
#                   writers cannot both believe they own the same temporary.
#   O_NOFOLLOW      refuses a symlink at the final component outright, stated
#                   explicitly rather than relied upon as a consequence of O_EXCL.
#   O_WRONLY        this descriptor only ever writes.
_STAGING_OPEN_FLAGS = os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW

# Whether this platform's renameat/openat can address a file by (directory fd, name).
# Where it can, every staging syscall is issued against an open descriptor on the
# validated directory, so the directory cannot be swapped between validation and use.
# Where it cannot, the same operations are issued against the validated absolute path
# with the same O_EXCL|O_NOFOLLOW flags -- a narrower guarantee, stated in
# `staging_protocol()` rather than assumed, and never an unguarded write.
# `os.rename` is the member `os.supports_dir_fd` documents for renameat; `os.replace`
# is the same syscall under a different Python name.
_DIR_FD_SUPPORTED = (
    os.open in os.supports_dir_fd
    and os.rename in os.supports_dir_fd
    and os.unlink in os.supports_dir_fd
)


class EmitError(Exception):
    """A fault in a row, an output file, or the agreement between the two files.

    Raised for every condition this module refuses to write or read past: a row
    that is not exactly the twelve fields, an absent value in a field where
    absence is not permitted, an absolute or scheme-bearing path, a CSV header
    that is not the twelve fields in order, and a cell that will not coerce to the
    type its field carries. Every message names the row index and the field so the
    fault is diagnosable from the exception alone.
    """


@dataclass(frozen=True)
class Mismatch:
    """One divergence between the two written files, located precisely.

    Attributes:
        kind: ``MISMATCH_FIELD_VALUE`` where two values disagree, or
            ``MISMATCH_ROW_SEQUENCE_LENGTH`` where the ordered walk ran out of
            rows in one file before the other.
        row_index: The zero-based row position at which the divergence was found.
            For a length difference this is the first position the shorter file
            does not carry.
        field: The field name for a value divergence, and ``None`` for a length
            difference, which belongs to no single field.
        json_value: The value parsed from ``findings.json``, or the number of rows
            it carried where ``kind`` is a length difference.
        csv_value: The value coerced from ``findings.csv``, or the number of rows
            it carried where ``kind`` is a length difference.
        detail: A human-readable statement of what diverged.
    """

    kind: str
    row_index: int
    field: str | None
    json_value: Any
    csv_value: Any
    detail: str

    def as_dict(self) -> dict[str, Any]:
        """Return the mismatch as a JSON-serialisable mapping."""
        return {
            "kind": self.kind,
            "row_index": self.row_index,
            "field": self.field,
            "json_value": self.json_value,
            "csv_value": self.csv_value,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class ComparisonResult:
    """The typed re-parse comparison, returned as data rather than printed.

    ``cli.py`` serialises :meth:`as_dict` into
    ``harness/artifacts/logs/normalize-run.json``, where AAP 0.9.1 requires the
    two files to "match field for field under typed coercion" as a recorded
    outcome.

    Attributes:
        passed: True only where every compared field agreed and both files
            carried the same row positions.
        rows_compared: The number of row positions examined. On a pass this is
            every row in both files; where a mismatch stopped the walk it is the
            number of positions reached, the last of them being the one that
            diverged.
        fields_compared: The number of individual field comparisons performed.
            On a pass this is ``rows_compared * len(FIELDS)``.
        field_order: The field order the comparison iterated, carried so a reader
            of the log can see it was the twelve-field contract.
        json_path: The ``findings.json`` path that was re-read.
        csv_path: The ``findings.csv`` path that was re-read.
        first_mismatch: The first divergence in row order, or ``None`` on a pass.
    """

    passed: bool
    rows_compared: int
    fields_compared: int
    field_order: tuple[str, ...]
    json_path: str
    csv_path: str
    first_mismatch: Mismatch | None

    def as_dict(self) -> dict[str, Any]:
        """Return the result as a JSON-serialisable mapping."""
        return {
            "passed": self.passed,
            "rows_compared": self.rows_compared,
            "fields_compared": self.fields_compared,
            "field_order": list(self.field_order),
            "json_path": self.json_path,
            "csv_path": self.csv_path,
            "first_mismatch": (
                None if self.first_mismatch is None else self.first_mismatch.as_dict()
            ),
        }

    def raise_if_failed(self) -> None:
        """Raise :class:`EmitError` naming the first mismatch where the files disagree.

        Provided so a caller that treats disagreement as a halting condition needs
        one call rather than its own branch. The comparison itself never raises:
        its outcome is data, and whether disagreement halts the run belongs to the
        caller.
        """
        if self.passed:
            return
        mismatch = self.first_mismatch
        if mismatch is None:  # pragma: no cover - defended, not reachable by construction
            raise EmitError(
                f"{self.json_path} and {self.csv_path} disagree, but no mismatch was recorded"
            )
        raise EmitError(
            f"{self.json_path} and {self.csv_path} disagree: {mismatch.detail} "
            f"(kind={mismatch.kind}, row_index={mismatch.row_index}, field={mismatch.field})"
        )


class ComparisonFailed(EmitError):
    """The two staged members disagreed, so neither was published.

    A distinct class rather than a plain :class:`EmitError` because the two conditions
    call for different records: an ``EmitError`` from a writer or a reader says the
    dataset could not be written or read back as this schema at all, while this says
    both files were written and read back and then did not agree — which is the
    outcome ``normalize-run.json`` records as a failed output comparison. The
    comparison itself travels on the exception, so the caller records the same
    measurement it would have recorded on a pass instead of a second one taken after
    the fact.

    Attributes:
        comparison: The :class:`ComparisonResult` that failed, carrying the first
            mismatch with its row index and field name.
    """

    def __init__(self, comparison: ComparisonResult) -> None:
        mismatch = comparison.first_mismatch
        detail = (
            "no mismatch was located, which is itself a fault"
            if mismatch is None
            else mismatch.detail
        )
        super().__init__(
            f"{comparison.json_path} and {comparison.csv_path} disagree under typed "
            f"re-parse, so neither was published: {detail}"
        )
        self.comparison = comparison


@dataclass(frozen=True)
class PublicationMember:
    """One published file, described by the measurement taken of the bytes on disk.

    A member is described once and cited from there: the size and digest below are
    taken from the staged bytes that were validated, and the same values are then
    re-taken from the published file and required to agree — so the record carries
    one measurement of one byte sequence rather than two measurements that happen to
    match (AAP 0.6.4).

    Attributes:
        role: :data:`ROLE_FINDINGS_JSON` or :data:`ROLE_FINDINGS_CSV`. The role
            rather than the filename, so a caller writing to a scratch directory
            still produces a record a reader can interpret.
        path: The final path the member was published to, as the caller gave it.
        size_bytes: The published file's byte size.
        sha256: The published file's sha256, lowercase hex.
        publication_id: The identifier this member shares with every other member of
            the same publication.
    """

    role: str
    path: str
    size_bytes: int
    sha256: str
    publication_id: str

    def as_dict(self) -> dict[str, Any]:
        """Return the member as a JSON-serialisable mapping."""
        return {
            "role": self.role,
            "path": self.path,
            "bytes": self.size_bytes,
            "sha256": self.sha256,
            "publication_id": self.publication_id,
        }


@dataclass(frozen=True)
class PublicationResult:
    """One publication of the dataset, as data for the run record.

    The dataset is two files that must be one generation of one row list. This object
    is what makes that checkable after the fact: both members carry the same
    :attr:`publication_id`, each carries its own byte size and digest, and
    :attr:`comparison` is the typed re-parse comparison that was established **before**
    either file was moved into place.

    Attributes:
        scheme: :data:`PUBLICATION_SCHEME`, the versioned contract this object obeys.
        publication_id: The content-derived identifier both members carry.
        identifier_method: :data:`PUBLICATION_IDENTIFIER_METHOD` — the derivation,
            recorded beside the value so it can be recomputed rather than trusted.
        members: Every published member, in :data:`PUBLICATION_ROLES` order.
        rows: The number of dataset rows the members carry, or ``None`` where the
            member is not a row set at all — the run record, published by this same
            protocol, is one row list's provenance rather than a row list.
        comparison: The typed re-parse comparison over the staged bytes that were
            then published unchanged, or ``None`` for a single-member publication
            where there is no second file to compare against.
        staging: The write protocol the members were published under, as
            :func:`staging_protocol` reports it.
        csv_neutralisation: The spreadsheet-formula rule and the number of cells it
            affected, as :meth:`CsvNeutralisationTally.as_dict` reports them, or
            ``None`` for a publication that wrote no CSV. A dataset whose CSV bytes
            can differ from a tool's literal text has to disclose why.
        completion_manifest: The commit record published last, with its path, byte
            size, digest and byte-derived ``member_set_id``, and the members it was
            verified against — or ``None`` where no manifest target was given, which
            is the single-member case that has no window between renames to close.
    """

    scheme: str
    publication_id: str
    identifier_method: str
    members: tuple[PublicationMember, ...]
    rows: int | None
    comparison: ComparisonResult | None
    staging: Mapping[str, Any]
    csv_neutralisation: Mapping[str, Any] | None = None
    completion_manifest: Mapping[str, Any] | None = None

    def member(self, role: str) -> PublicationMember:
        """Return the member published under ``role``.

        Raises:
            KeyError: Where this publication carries no member for that role, which
                names the role rather than returning ``None`` for a caller to
                mis-handle.
        """
        for member in self.members:
            if member.role == role:
                return member
        raise KeyError(f"this publication carries no member for role {role!r}")

    def as_dict(self) -> dict[str, Any]:
        """Return the publication as a JSON-serialisable mapping."""
        return {
            "scheme": self.scheme,
            "publication_id": self.publication_id,
            "identifier_method": self.identifier_method,
            "members": [member.as_dict() for member in self.members],
            "rows": self.rows,
            "comparison": None if self.comparison is None else self.comparison.as_dict(),
            "staging": dict(self.staging),
            "csv_neutralisation": (
                None
                if self.csv_neutralisation is None
                else dict(self.csv_neutralisation)
            ),
            "completion_manifest": (
                None
                if self.completion_manifest is None
                else dict(self.completion_manifest)
            ),
            "order": (
                "every member was staged in its own target directory, validated by "
                "being parsed back from the staged bytes through a descriptor bound to "
                "the inode they were written into, fsynced, and only then moved into "
                "place; a fault at any point before the first move leaves every "
                "previous deliverable untouched. The completion manifest is staged "
                "after all of them and renamed LAST, which is what closes the window "
                "BETWEEN the renames -- N renames are N atomic operations rather than "
                "one, and POSIX offers no N-way atomic rename, so a record whose "
                "presence means 'every member it names is in place' is the only thing "
                "that can close it"
            ),
        }


# --------------------------------------------------------------------------- #
# The no-absolute-path assertion (AAP 0.8.2, AAP 0.6.1)
# --------------------------------------------------------------------------- #


def _path_violation(value: str) -> str | None:
    """Return why ``value`` is not a root-relative path, or ``None`` where it is.

    The check is deliberately a diagnosis rather than a repair: `paths.py` owns
    resolution, and stripping a prefix here would hide a resolver bug and silently
    change what the row means (AAP 0.6.1).

    Rejected: a leading slash, a leading backslash or UNC prefix, a Windows drive
    prefix, and any URI form — ``file://``, ``jar:file:/...``, ``http://``.

    Accepted, because both are legitimate coordinates rather than defects:
      * the archive-member form ``<container-relative-to-root>!<member-path>``,
        whose single ``!`` separator carries no scheme and no root; and
      * a path carrying preserved ``../`` segments, which the SARIF 2.1.0 errata
        require of a container outside the root (consumers must not normalize
        ``..`` away), so ``..`` is correct rather than absolute.
    """
    if value.startswith("/"):
        return "it begins with '/', which is a filesystem-absolute POSIX path"
    if value.startswith("\\"):
        return "it begins with a backslash, which is a rooted or UNC Windows path"
    if _WINDOWS_DRIVE_RE.match(value):
        return "it carries a Windows drive prefix, which is filesystem-absolute"
    if _URI_AUTHORITY_RE.match(value):
        return "it is a URI with an authority component, not a path relative to the root"
    if _URI_SCHEME_RE.match(value):
        return "it carries a URI scheme, not a path relative to the root"
    # pathlib is consulted last, as a second opinion on absoluteness under both
    # flavours, so a form the explicit checks above did not anticipate is still
    # refused rather than written.
    if PurePosixPath(value).is_absolute() or PureWindowsPath(value).is_absolute():
        return "it is an absolute path"
    return None


# --------------------------------------------------------------------------- #
# Row validation — exactly twelve fields, and the absence convention
# --------------------------------------------------------------------------- #


def _validated_value(field: str, value: Any, row_index: int) -> Any:
    """Return ``value`` unchanged where it is legal for ``field``, else raise.

    Nothing is coerced, trimmed or defaulted here. A value that is not what its
    field carries is a fault in the row rather than something to repair, because
    repairing it would change what the row asserts about the code that was
    scanned.
    """
    where = f"row {row_index}, field '{field}'"

    if field == _BOOLEAN_FIELD:
        # `type(...) is bool` rather than isinstance: bool is a subclass of int,
        # and 0/1 must not pass for a field whose CSV literal is written by name.
        if type(value) is not bool:
            raise EmitError(
                f"{where}: in_scope must be a bool; observed {type(value).__name__} "
                f"({value!r}). in_scope is never absent and is never 0 or 1."
            )
        return value

    if field == _INTEGER_FIELD:
        if value is None:
            return None
        if type(value) is bool or not isinstance(value, int):
            raise EmitError(
                f"{where}: start_line must be an int or None; observed "
                f"{type(value).__name__} ({value!r}). It is never a string, and "
                "absence is None rather than 0."
            )
        if value < 1:
            # Line numbering in every source this dataset expresses is one-based: SARIF
            # 2.1.0 section 3.30.5 fixes region.startLine at 1 for the first line, and
            # Trivy, Gitleaks and Checkov all count from 1 too. So 0 is not a small line
            # number -- it is either an absence written as a sentinel, which this module's
            # own convention forbids (absence is None in JSON and an empty CSV field), or a
            # producer's off-by-one. Neither may be written: a row asserting line 0 asserts
            # something about the scanned code that is not true, and a reader has no way to
            # tell which of the two it was looking at. A record whose start_line cannot be
            # read as a positive integer is rejected upstream under
            # `non_integer_start_line` and counted, which is where it belongs. This is the
            # final boundary, so it refuses independently of the adapters rather than
            # trusting that none of them will ever regress.
            raise EmitError(
                f"{where}: start_line must be at least 1 where it is present; observed "
                f"{value!r}. Line numbering is one-based, and absence is None rather "
                "than 0."
            )
        return value

    if field in _OPTIONAL_TEXT_FIELDS:
        if value is None:
            return None
        if not isinstance(value, str):
            raise EmitError(
                f"{where}: must be a str or None; observed {type(value).__name__} ({value!r})"
            )
        if value == "":
            raise EmitError(
                f"{where}: absence is None, never an empty string. An empty CSV cell "
                "means absent, so an empty string here would not survive the round trip."
            )
        return value

    # The remaining six are required text fields. Two of them — path and
    # severity_norm — are named in AAP 0.8.2 as never absent; the other four are
    # required because the absence convention has no representation for them.
    if not isinstance(value, str):
        raise EmitError(
            f"{where}: must be a non-empty str; observed {type(value).__name__} ({value!r})"
            + (
                ". path and severity_norm are never absent: a record whose path could "
                "not be resolved was rejected and counted upstream."
                if field in ("path", "severity_norm")
                else ""
            )
        )
    if value == "":
        raise EmitError(
            f"{where}: must be a non-empty str; observed an empty string, which the "
            "CSV cannot distinguish from absence"
        )
    if field == "path":
        violation = _path_violation(value)
        if violation is not None:
            raise EmitError(
                f"{where}: no absolute path is ever emitted — {violation}: {value!r}. "
                "Resolution belongs to paths.py; this module refuses the row rather "
                "than repairing it."
            )
    return value


def _validated_row(row: Any, row_index: int) -> dict[str, Any]:
    """Return ``row`` as a new mapping carrying the twelve fields in ``FIELDS`` order.

    The returned dict is built by iterating ``FIELDS``, so its insertion order is
    the contract's order — which is what makes the JSON object's key order right
    by construction rather than by luck.
    """
    if not isinstance(row, Mapping):
        raise EmitError(
            f"row {row_index}: expected a mapping of the twelve fields; observed "
            f"{type(row).__name__}"
        )

    present = set(row.keys())
    missing = [field for field in FIELDS if field not in present]
    unexpected = sorted(str(key) for key in present - set(FIELDS))
    if missing or unexpected:
        parts = []
        if missing:
            parts.append(f"missing {missing}")
        if unexpected:
            parts.append(f"unexpected {unexpected}")
        raise EmitError(
            f"row {row_index}: a row is exactly the twelve fields {list(FIELDS)} — "
            + "; ".join(parts)
        )

    ordered: dict[str, Any] = {}
    for field in FIELDS:
        ordered[field] = _validated_value(field, row[field], row_index)
    return ordered


def validate_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Validate every row and return them as ordered mappings, in the order given.

    Every row is checked before any file is opened, so an invalid row set cannot
    leave a half-written deliverable behind. The order the caller supplied is
    preserved exactly: no sort, no grouping by tool, no ranking and no
    deduplication (AAP 0.3.2).

    Args:
        rows: An iterable of mappings, each carrying exactly the twelve fields.

    Returns:
        A list of new dicts whose keys are ``FIELDS`` in order.

    Raises:
        EmitError: Naming the row index and field of the first fault found.
    """
    return [_validated_row(row, index) for index, row in enumerate(rows)]


def validation_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Measure what :func:`validate_rows` enforced, so the record can state it as a count.

    :func:`validate_rows` *enforces* the schema and raises on the first fault, which
    means a successful write proves the schema held — but it proves it by the absence of
    an exception, and an absence is not a number. The run record has to carry the
    assertion as a measurement: *every emitted row has exactly twelve fields, a resolved
    non-absent* ``path`` *and a non-absent* ``severity_norm``\\ *, absence appears only in
    the five optional fields, and no emitted path is absolute*. This function walks the
    rows that were written and counts each of those, using the same rule definitions
    :func:`validate_rows` uses — :data:`FIELDS`, :data:`OPTIONAL_FIELDS` and
    :func:`_path_violation` — so the record cites the module's own rules rather than a
    second spelling of them.

    It diagnoses and never raises. Called after a successful write, every count below is
    expected to be its passing value and ``passed`` is expected ``True``; called on rows
    that never reached a writer, it names what is wrong with them instead. Either way the
    numbers are real: nothing here is defaulted, and ``violations`` carries the first few
    faults in full rather than only their count.

    Args:
        rows: The rows as written, in the order they were written.

    Returns:
        A JSON-serialisable mapping of the measured assertions.

    >>> row = {f: None for f in FIELDS}
    >>> row.update(
    ...     tool="gitleaks", scanner_class="secret", rule_id="generic-api-key",
    ...     message="a secret", severity_norm="Info", path="core/src/main/x.scala",
    ...     in_scope=True,
    ... )
    >>> summary = validation_summary([row])
    >>> summary["rows"], summary["passed"], summary["absolute_paths"]
    (1, True, 0)
    >>> summary["absence_by_optional_field"]["cwe"]
    1
    """
    field_set = set(FIELDS)
    absence_by_optional: dict[str, int] = {field: 0 for field in sorted(OPTIONAL_FIELDS)}
    exact_twelve = 0
    absent_required: dict[str, int] = {}
    absolute_paths = 0
    violations: list[dict[str, Any]] = []

    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            violations.append(
                {
                    "row_index": index,
                    "condition": "not_a_mapping",
                    "detail": f"expected a mapping of the twelve fields; observed {type(row).__name__}",
                }
            )
            continue
        keys = set(row.keys())
        if keys == field_set:
            exact_twelve += 1
        else:
            violations.append(
                {
                    "row_index": index,
                    "condition": "field_set",
                    "detail": (
                        f"missing {sorted(field_set - keys)}; "
                        f"unexpected {sorted(str(key) for key in keys - field_set)}"
                    ),
                }
            )
        for field in FIELDS:
            value = row.get(field)
            if field in OPTIONAL_FIELDS:
                if value is None:
                    absence_by_optional[field] += 1
                continue
            if value is None:
                absent_required[field] = absent_required.get(field, 0) + 1
                violations.append(
                    {
                        "row_index": index,
                        "condition": "absent_required_field",
                        "detail": f"{field} is never absent, and this row carries None",
                    }
                )
        path = row.get("path")
        if isinstance(path, str):
            violation = _path_violation(path)
            if violation is not None:
                absolute_paths += 1
                violations.append(
                    {
                        "row_index": index,
                        "condition": "absolute_path",
                        "detail": f"{violation}: {path!r}",
                    }
                )

    total = len(rows)
    passed = (
        exact_twelve == total
        and not absent_required
        and absolute_paths == 0
        and not violations
    )
    return {
        "rows": total,
        "field_order": list(FIELDS),
        "fields_per_row_required": len(FIELDS),
        "rows_with_exactly_twelve_fields": exact_twelve,
        "required_fields": list(REQUIRED_FIELDS),
        "optional_fields": sorted(OPTIONAL_FIELDS),
        "rows_with_an_absent_required_field": dict(absent_required),
        "path_absent": absent_required.get("path", 0),
        "severity_norm_absent": absent_required.get("severity_norm", 0),
        "absolute_paths": absolute_paths,
        "absence_by_optional_field": absence_by_optional,
        "passed": passed,
        "violations": violations[:_VALIDATION_VIOLATION_LIMIT],
        "violation_count": len(violations),
        "method": (
            "one pass over the rows as written, applying this module's own FIELDS, "
            "OPTIONAL_FIELDS and _path_violation rules; diagnostic only -- "
            "validate_rows had already refused any row that failed them"
        ),
        "asserts": [
            "every emitted row carries exactly the twelve fields, in FIELDS order",
            "path and severity_norm are never absent",
            "absence appears only in severity_native, start_line, cwe, cve and package_coordinate",
            "no emitted path is absolute, a URI, or otherwise not relative to the SPARK_SRC root",
        ],
    }


# --------------------------------------------------------------------------- #
# The two writers — one row list in, two files out
# --------------------------------------------------------------------------- #


def neutralize_csv_text(value: str) -> str:
    """Return ``value`` made inert as a CSV cell, reversibly (CWE-1236).

    The forward half of the rule stated in full at :data:`CSV_FORMULA_ESCAPE`: a cell
    whose first character is a trigger — or the escape character itself — gets one escape
    character prepended, and nothing else changes. :func:`restore_csv_text` is its exact
    inverse for every input.

    >>> neutralize_csv_text("=cmd|' /c calc'!A0")
    "'=cmd|' /c calc'!A0"
    >>> neutralize_csv_text("-1 is a legitimate message")
    "'-1 is a legitimate message"
    >>> neutralize_csv_text("'already starts with the escape")
    "''already starts with the escape"
    >>> neutralize_csv_text("an ordinary message with = inside it")
    'an ordinary message with = inside it'
    >>> neutralize_csv_text("")
    ''
    """
    if value and value[0] in _CSV_ESCAPE_LEADERS:
        return CSV_FORMULA_ESCAPE + value
    return value


def restore_csv_text(cell: str) -> str:
    """Return the original text of a cell :func:`neutralize_csv_text` may have escaped.

    The inverse half of the rule: one leading escape character is removed where the
    character after it is a trigger or the escape character, and nothing else changes. A
    cell the rule never touched is returned identically, which is why applying this to
    every text cell of a file — including one written before the rule existed — is safe.

    >>> restore_csv_text("'=cmd|' /c calc'!A0")
    "=cmd|' /c calc'!A0"
    >>> restore_csv_text("''already starts with the escape")
    "'already starts with the escape"
    >>> restore_csv_text("'an apostrophe that leads ordinary text")
    "'an apostrophe that leads ordinary text"
    >>> restore_csv_text("an ordinary message")
    'an ordinary message'
    """
    if (
        len(cell) >= 2
        and cell[0] == CSV_FORMULA_ESCAPE
        and cell[1] in _CSV_ESCAPE_LEADERS
    ):
        return cell[1:]
    return cell


def csv_neutralisation_rule() -> dict[str, Any]:
    """Return the neutralisation scheme as data, for the run record to disclose it.

    A dataset whose CSV bytes can differ from a tool's literal text has to say so, and
    say exactly how, or a reader cannot tell a neutralised cell from a tool that really
    reported a leading apostrophe. This is that statement, in the record rather than only
    in this module's docstring.

    >>> rule = csv_neutralisation_rule()
    >>> rule["escape_character"], rule["triggers"]
    ("'", ['=', '+', '-', '@', '\\t', '\\r'])
    >>> rule["reversible"]
    True
    """
    return {
        "name": "leading-formula-character escape",
        "applies_to": [
            field for field in FIELDS if field not in {_BOOLEAN_FIELD, _INTEGER_FIELD}
        ],
        "escape_character": CSV_FORMULA_ESCAPE,
        "triggers": list(CSV_FORMULA_TRIGGERS),
        "also_escaped": (
            "a cell whose text already begins with the escape character, which is what "
            "makes the inverse unambiguous rather than a guess"
        ),
        "rule": (
            "a text cell whose first character is a trigger or the escape character is "
            "written with one escape character prepended; every other cell is written "
            "unchanged"
        ),
        "inverse": (
            "a cell beginning with the escape character whose second character is a "
            "trigger or the escape character has exactly one leading escape character "
            "removed; every other cell is read unchanged"
        ),
        "reversible": True,
        "reversibility": (
            "exact for every input: a cell the rule escaped begins with the escape "
            "character followed by a trigger or the escape character, and a cell it did "
            "not escape cannot begin with the escape character at all, so the two cases "
            "never collide and the inverse never has to guess"
        ),
        "not_applied_to": [
            "the header row, which is the twelve field names",
            f"the in_scope literals '{CSV_TRUE}' and '{CSV_FALSE}'",
            "the start_line integer",
            "the empty cell that means absence",
            "findings.json, which carries every value exactly as the adapter produced it",
        ],
        "why": (
            "a cell beginning with one of the triggers is evaluated as a formula when the "
            "CSV is opened interactively in a spreadsheet, so scanner-controlled text "
            "would become executable content in a deliverable whose purpose is to record "
            "verbatim what a tool reported (CWE-1236)"
        ),
        "round_trip": (
            "the inverse is applied by read_findings_csv, so the typed re-parse "
            "comparison against findings.json is over the tools' own text and the two "
            "files still agree field for field"
        ),
    }


@dataclass
class CsvNeutralisationTally:
    """How many cells the neutralisation actually changed, per field.

    Mutable and filled by the writer as it renders, so the number in the run record is a
    count of what was written rather than a second pass over the rows that could disagree
    with it (AAP 0.6.4 — one measurement, cited).

    Attributes:
        cells_examined: Every text-valued cell the rule was applied to.
        cells_escaped: The cells it changed.
        escaped_by_field: The changed cells, by field name.
        escaped_by_leading_character: The changed cells, by the character that led them,
            so a reader can see which trigger occurred rather than only that one did.
    """

    cells_examined: int = 0
    cells_escaped: int = 0
    escaped_by_field: dict[str, int] = dataclass_field(default_factory=dict)
    escaped_by_leading_character: dict[str, int] = dataclass_field(default_factory=dict)

    def record(self, field_name: str, original: str, written: str) -> None:
        """Count one rendered text cell, and whether the rule changed it."""
        self.cells_examined += 1
        if written != original:
            self.cells_escaped += 1
            self.escaped_by_field[field_name] = (
                self.escaped_by_field.get(field_name, 0) + 1
            )
            leader = original[0]
            self.escaped_by_leading_character[leader] = (
                self.escaped_by_leading_character.get(leader, 0) + 1
            )

    def as_dict(self) -> dict[str, Any]:
        """Return the tally and the rule it applied, as one JSON-serialisable mapping."""
        return {
            "rule": csv_neutralisation_rule(),
            "cells_examined": self.cells_examined,
            "cells_escaped": self.cells_escaped,
            "escaped_by_field": dict(sorted(self.escaped_by_field.items())),
            "escaped_by_leading_character": {
                # The tab and carriage return are rendered as their escape sequences so
                # the record stays readable and a whitespace trigger is still visible.
                key.encode("unicode_escape").decode("ascii"): count
                for key, count in sorted(self.escaped_by_leading_character.items())
            },
            "measured": (
                "counted by the CSV writer as it rendered each cell, so this is the "
                "number of cells actually written escaped rather than a second pass "
                "over the rows"
            ),
        }


def _csv_cell(
    field_name: str, value: Any, tally: CsvNeutralisationTally | None = None
) -> str:
    """Render one validated value as its CSV cell under the shared absence convention.

    Every text-valued cell goes through :func:`neutralize_csv_text` — the rule is applied
    to all of them, and changes only the ones whose leading character requires it. The
    boolean and the integer are rendered by name and by ``str`` respectively and neither
    can produce a leading trigger, so neither is passed through the rule.
    """
    if value is None:
        # Absence is an empty field, matching JSON null (AAP 0.5.4).
        return CSV_ABSENT
    if field_name == _BOOLEAN_FIELD:
        # Written by name so `str(True)` can never put `True` in the column.
        return CSV_TRUE if value else CSV_FALSE
    if field_name == _INTEGER_FIELD:
        return str(value)
    written = neutralize_csv_text(value)
    if tally is not None:
        tally.record(field_name, value, written)
    return written


def _render_json(rows: Sequence[Mapping[str, Any]], handle: IO[str]) -> None:
    """Render the validated rows into ``handle`` as a top-level JSON array.

    A bare array: row-only, no metadata envelope (AAP 0.5.4). ``indent=2`` keeps the
    deliverable auditable by a human; ``ensure_ascii=False`` keeps non-ASCII
    characters as UTF-8 rather than ``\\u`` escapes; ``allow_nan=False`` refuses any
    non-standard JSON literal rather than writing one a strict parser will reject.

    The rows are serialised **in place** where the caller already holds a ``list`` or a
    ``tuple``. By the time they reach here they are the validated in-memory row set both
    writers consume (AAP 0.6.1: *"both writers consume the same validated rows"*), built
    once by :func:`validate_rows`; an unconditional copy would duplicate every one of the
    dataset's row references for the duration of the serialisation, and this function
    only reads them.

    The conversion is made conditional rather than removed. :mod:`json` serialises a
    ``list`` and a ``tuple`` natively and nothing else, so a ``Sequence`` that is neither
    would raise ``TypeError`` part-way through a write and leave a partial staged file
    behind; a foreign sequence type is therefore still materialised, and only the case
    this pipeline actually passes avoids the copy. Nothing is copied for isolation
    either: the same objects reach the CSV writer and the typed comparison, so a copy
    here would make the two writers consume different objects and quietly weaken the
    assertion that they wrote the same rows.
    """
    payload: Any = rows if isinstance(rows, (list, tuple)) else list(rows)
    json.dump(payload, handle, ensure_ascii=False, indent=2, allow_nan=False)
    handle.write("\n")


def _render_csv(
    rows: Sequence[Mapping[str, Any]],
    handle: IO[str],
    tally: CsvNeutralisationTally | None = None,
) -> None:
    """Render the validated rows into ``handle`` as a header row plus one row per finding.

    The header is the twelve field names verbatim -- it is the schema contract, and no
    field name begins with a formula trigger, so the neutralisation has nothing to do
    there.  Every data cell goes through :func:`_csv_cell`, which applies the rule to the
    text-valued ones and counts what it changed into ``tally`` where one is supplied.
    """
    writer = csv.writer(handle, lineterminator=_CSV_LINE_TERMINATOR)
    writer.writerow(FIELDS)
    for row in rows:
        writer.writerow([_csv_cell(name, row[name], tally) for name in FIELDS])


# --------------------------------------------------------------------------- #
# Secure staging and atomic publication (CWE-59, CWE-367)
#
# Every file this module writes is published by the same four steps, and none of
# them can be skipped:
#
#   1. VALIDATE THE DIRECTORY. The target's parent is taken lexically, created if
#      absent, and then required to be its own realpath. A symlink anywhere in the
#      chain — or a parent that resolves somewhere other than where the caller named
#      — is refused rather than written through, because a redirected write publishes
#      the dataset somewhere the run record does not describe.
#   2. STAGE. The bytes go to a randomly named file in that same directory, created
#      with O_CREAT|O_EXCL|O_WRONLY|O_NOFOLLOW so a pre-planted path or a concurrent
#      writer makes the creation fail instead of quietly redirecting it, written
#      through that descriptor, then flushed and fsynced so the bytes are on the
#      device before anything points at them. Same directory, so the move in step 4
#      is a rename within one filesystem and therefore atomic.
#   3. MEASURE AND VALIDATE. The staged file is sized and digested by reading the
#      bytes back off the disk, and — for the two-member dataset publication — parsed
#      back and compared field for field. Nothing is moved into place until every
#      member has passed.
#   4. PUBLISH. os.replace() moves each staged file onto its target, which replaces
#      the path itself and never follows a symlink at it, and the containing
#      directory is fsynced so the rename survives a power loss. The published bytes
#      are then re-digested and required to equal the validated ones.
#
# On any failure the staged file is removed and the exception propagates. There is no
# fallback path that writes the target directly, and no branch that leaves a partial
# file behind for a later reader to mistake for a deliverable.
# --------------------------------------------------------------------------- #


def staging_protocol() -> dict[str, Any]:
    """Return the write protocol as data, for the run record to publish beside the result.

    The protocol is a security property of the deliverable, so it is recorded rather
    than left as a claim in a comment: a reader of
    ``harness/artifacts/logs/normalize-run.json`` can see which syscalls the bytes
    reached the disk through, and whether this platform addressed them by directory
    descriptor.

    >>> protocol = staging_protocol()
    >>> protocol["temporary_name_pattern"]
    '.<target-name>.<24 random hex characters>.staged'
    >>> protocol["open_flags"]
    'O_CREAT|O_EXCL|O_WRONLY|O_NOFOLLOW'
    """
    return {
        "scheme": PUBLICATION_SCHEME,
        "parent_directory_validation": (
            "the target's parent is taken lexically with os.path.abspath and then "
            "walked ONE COMPONENT AT A TIME: each component is opened relative to the "
            "previous component's descriptor with O_RDONLY|O_DIRECTORY|O_NOFOLLOW, and "
            "a missing component is created with os.mkdir(dir_fd=) against that same "
            "descriptor before it is opened. So no component can be a symlink, no "
            "component can be created through one, and no component can be swapped "
            "between being checked and being used. A component that is a link is "
            "distinguished from one that is not a directory with os.lstat(dir_fd=), "
            "because O_NOFOLLOW|O_DIRECTORY reports ELOOP on some platforms and "
            "ENOTDIR on Linux. The whole-path os.path.realpath equality is still "
            "asserted afterwards, now as a cross-check that must agree rather than as "
            "the validation itself; it superseded a sequence that called "
            "mkdir(parents=True) BEFORE that comparison, which created a missing "
            "descendant under a linked ancestor at the redirected destination and only "
            "then refused the write"
        ),
        "identity_binding": (
            "(st_dev, st_ino) is taken through the WRITE descriptor before it is "
            "closed, required again when the staged bytes are measured, and required "
            "once more when the published path is re-measured after the rename -- a "
            "rename preserves the inode, so the file that was validated is the file "
            "that must be at the published path. Without it, 'the published bytes are "
            "the validated bytes' would be a statement about a pathname"
        ),
        "target_validation": (
            "a target that already exists must be a regular file; a symlink or any "
            "other file type at the target path is refused rather than published "
            "through or replaced"
        ),
        "temporary_name_pattern": (
            f".<target-name>.<{_TEMP_NAME_RANDOM_BYTES * 2} random hex characters>.staged"
        ),
        "temporary_randomness_bits": _TEMP_NAME_RANDOM_BYTES * 8,
        "open_flags": "O_CREAT|O_EXCL|O_WRONLY|O_NOFOLLOW",
        "file_mode": f"0o{_STAGED_FILE_MODE:o}",
        "addressing": (
            "openat/renameat/unlinkat against an open descriptor on the validated "
            "directory"
            if _DIR_FD_SUPPORTED
            else "absolute paths under the validated directory; this platform does "
            "not expose dir_fd for os.open/os.rename/os.unlink, so the same "
            "O_EXCL|O_NOFOLLOW flags carry the guarantee without the descriptor"
        ),
        "durability": (
            "each staged file is flushed and os.fsync'd before it is measured, and "
            "the containing directory is os.fsync'd after the rename"
        ),
        "atomicity": (
            "os.replace within one directory, which replaces the target path itself "
            "and follows no symlink at it; every member is staged and validated "
            "before the first member is moved"
        ),
        "on_failure": (
            "the staged file is unlinked and the exception propagates; no partial "
            "file is left behind and there is no unsafe fallback write"
        ),
        "digest_chunk_bytes": _DIGEST_CHUNK,
    }


def _validated_directory(target: Path) -> Path:
    """Return the directory ``target`` will be published into, or refuse to write.

    The check is deliberately strict, and it is a refusal rather than a repair: a
    symlink in the parent chain means the bytes would land somewhere other than the
    path the run record names, and a deliverable whose location is not the location
    it is recorded at is worse than no deliverable (CWE-59).

    Args:
        target: The final path a member is to be published to.

    Returns:
        The lexically absolute parent directory, which is also its own realpath.

    Raises:
        EmitError: Where a component of the parent is a symlink, where the parent
            resolves to a different directory, or where the parent is not a directory.
        OSError: Where the directory cannot be created or inspected.
    """
    intended = Path(os.path.abspath(target.parent))

    # Each component is validated and, where missing, created ONE AT A TIME, and
    # the check on a component happens before the next one is touched.
    #
    # `mkdir(parents=True)` followed by a realpath check was the wrong order: a
    # missing descendant under a symlinked ancestor was CREATED at the redirected
    # destination, and only then did the realpath comparison refuse the write. The
    # bytes never landed, but the directory had already been made somewhere the
    # record does not name, which is a side effect a refusal is supposed to avoid.
    #
    # Where the platform supports descriptor-relative operations, the walk is done
    # against open no-follow directory descriptors, so a component cannot be
    # swapped for a symlink between the moment it is checked and the moment it is
    # used (CWE-367). `_OutputDirectory` then re-walks to obtain its own descriptor
    # and re-applies the same refusals, so the pathname is never the thing trusted.
    if _DIR_FD_SUPPORTED:
        fd = _walk_no_follow(intended, create=True)
        os.close(fd)
    else:  # pragma: no cover - every supported platform has dir_fd
        walked = Path(intended.anchor or os.sep)
        for part in intended.relative_to(walked).parts:
            walked = walked / part
            if not walked.exists() and not walked.is_symlink():
                try:
                    walked.mkdir()
                except FileExistsError:
                    pass
            if walked.is_symlink():
                raise EmitError(
                    f"refusing to write {target}: {walked}, on the path to it, is a "
                    "symlink. The bytes would land somewhere other than the path the "
                    "run record names, so nothing is written."
                )
            if not walked.is_dir():
                raise EmitError(
                    f"refusing to write {target}: {walked}, on the path to it, is not "
                    "a directory"
                )

    resolved = Path(os.path.realpath(intended))
    if resolved != intended:
        raise EmitError(
            f"refusing to write {target}: its parent directory {intended} resolves to "
            f"{resolved}, so a component of the path is a symlink. The bytes would land "
            "somewhere other than the path the run record names, so nothing is written."
        )
    if not intended.is_dir():  # pragma: no cover - the walk above guarantees it
        raise EmitError(
            f"refusing to write {target}: its parent {intended} is not a directory"
        )
    return intended


def _walk_no_follow(directory: Path, *, create: bool) -> int:
    """Open ``directory`` by walking it component by component with ``O_NOFOLLOW``.

    Returns an open descriptor for the directory itself.  Every component is opened
    relative to the previous component's descriptor with ``O_NOFOLLOW|O_DIRECTORY``,
    so no component can be a symlink and no component can be replaced between being
    checked and being used — the two halves of CWE-59 and CWE-367 that a validated
    *pathname* leaves open, because the name is resolved again on every later call.

    Args:
        directory: An absolute directory path.
        create: Whether a missing component may be created.  Creation is relative to
            the parent's descriptor, so a component cannot be created through a link.

    Raises:
        EmitError: Where a component is a symlink, is not a directory, or is missing
            and ``create`` is false.
        OSError: Where a component cannot be created or opened for another reason.
    """
    if not directory.is_absolute():  # pragma: no cover - callers pass abspath
        raise EmitError(f"refusing to open {directory}: it is not an absolute path")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = os.open(directory.anchor or os.sep, flags)
    try:
        for part in directory.relative_to(directory.anchor or os.sep).parts:
            if create:
                try:
                    os.mkdir(part, dir_fd=fd)
                except FileExistsError:
                    pass
            try:
                child = os.open(part, flags, dir_fd=fd)
            except OSError as exc:
                # O_NOFOLLOW|O_DIRECTORY on a symlink reports ELOOP on some
                # platforms and ENOTDIR on Linux, so which errno arrived does not
                # settle what the component is. lstat does: it is the one call that
                # describes the link itself rather than its target, and the two
                # cases need different messages because they need different fixes.
                is_link = False
                if exc.errno in (errno.ELOOP, errno.EMLINK, errno.ENOTDIR):
                    try:
                        is_link = stat_module.S_ISLNK(
                            os.lstat(part, dir_fd=fd).st_mode
                        )
                    except OSError:  # pragma: no cover - raced away between calls
                        is_link = False
                if is_link or exc.errno in (errno.ELOOP, errno.EMLINK):
                    raise EmitError(
                        f"refusing to use {directory}: its component {part!r} is a "
                        "symlink, so a write would land somewhere other than the path "
                        "the run record names"
                    ) from exc
                if exc.errno == errno.ENOTDIR:
                    raise EmitError(
                        f"refusing to use {directory}: its component {part!r} is not a "
                        "directory"
                    ) from exc
                if exc.errno == errno.ENOENT:
                    raise EmitError(
                        f"refusing to use {directory}: its component {part!r} does not "
                        "exist"
                    ) from exc
                raise
            os.close(fd)
            fd = child
    except BaseException:
        os.close(fd)
        raise
    return fd


def _validated_target_name(target: Path, directory: Path) -> str:
    """Return the filename ``target`` publishes to, refusing a non-regular existing path.

    ``os.replace`` never follows a symlink at the destination, so publishing over one
    could not be redirected — but it would silently destroy it, and it would mean the
    previous deliverable at that path was never the file the record described. Either
    way the honest outcome is to refuse and say why.

    Raises:
        EmitError: Where the target has no filename, or exists as something other
            than a regular file.
    """
    name = target.name
    if not name or name in (".", ".."):
        raise EmitError(f"refusing to write {target}: it does not name a file")
    final = directory / name
    if final.is_symlink():
        raise EmitError(
            f"refusing to publish {final}: the target path is a symlink. A deliverable "
            "is a regular file at the path the record names; a symlink there is "
            "reported rather than replaced."
        )
    if final.exists() and not final.is_file():
        raise EmitError(
            f"refusing to publish {final}: the target path exists and is not a regular file"
        )
    return name


def _temporary_name(target_name: str) -> str:
    """Return an unpredictable staging name beside ``target_name`` in the same directory.

    Dot-prefixed so a directory listing shows it as scratch, suffixed ``.staged`` so a
    reader who does see one knows what it is, and carrying
    :data:`_TEMP_NAME_RANDOM_BYTES` bytes of ``os.urandom`` so no concurrent process
    can pre-create or pre-symlink the path this writer is about to use (CWE-367).
    """
    return f".{target_name}.{os.urandom(_TEMP_NAME_RANDOM_BYTES).hex()}.staged"


class _OutputDirectory:
    """One validated output directory, held open for the whole publication.

    Every staging syscall is issued against this descriptor where the platform
    supports it, so the directory cannot be replaced between the moment it was
    validated and the moment it is written into — which is the race a validated path
    alone does not close (CWE-367). The descriptor is also what the post-rename
    ``fsync`` needs, so the rename is durable rather than merely visible.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.fd: int | None = None
        if _DIR_FD_SUPPORTED:
            # Opened by walking every component with O_NOFOLLOW rather than by
            # handing the whole pathname to one os.open. A single open of the
            # validated pathname re-resolves the name, so a component swapped for
            # a symlink between validation and open would have been followed —
            # and the descriptor everything else in this class trusts would then
            # point at the redirected directory. The walk refuses that outright.
            self.fd = _walk_no_follow(path, create=False)

    @property
    def dir_fd(self) -> int | None:
        """The open descriptor, or ``None`` where this platform cannot use one."""
        return self.fd

    def create_exclusive(self, name: str) -> int:
        """Create ``name`` in this directory and return its write-only descriptor."""
        if self.fd is not None:
            return os.open(name, _STAGING_OPEN_FLAGS, _STAGED_FILE_MODE, dir_fd=self.fd)
        return os.open(str(self.path / name), _STAGING_OPEN_FLAGS, _STAGED_FILE_MODE)

    def open_for_read(self, name: str) -> int:
        """Open ``name`` in this directory for reading, refusing a symlink at it."""
        flags = os.O_RDONLY | os.O_NOFOLLOW
        if self.fd is not None:
            return os.open(name, flags, dir_fd=self.fd)
        return os.open(str(self.path / name), flags)

    def replace(self, source_name: str, target_name: str) -> None:
        """Move ``source_name`` onto ``target_name`` within this one directory."""
        if self.fd is not None:
            os.replace(source_name, target_name, src_dir_fd=self.fd, dst_dir_fd=self.fd)
        else:
            os.replace(str(self.path / source_name), str(self.path / target_name))

    def unlink_quietly(self, name: str) -> None:
        """Remove ``name`` if it is still there, swallowing only its absence.

        The one place this module ignores an error, and only this one: it runs on the
        failure path, where the exception being propagated is the one that matters and
        a second fault while cleaning up must not replace it. A cleanup failure other
        than "already gone" is re-raised only if it would leave a file behind, which
        ``FileNotFoundError`` by definition does not.
        """
        try:
            if self.fd is not None:
                os.unlink(name, dir_fd=self.fd)
            else:
                os.unlink(str(self.path / name))
        except FileNotFoundError:
            pass

    def sync(self) -> None:
        """Flush this directory's own entries, so a completed rename survives a crash."""
        if self.fd is not None:
            os.fsync(self.fd)
            return
        fd = os.open(str(self.path), os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def close(self) -> None:
        """Release the descriptor."""
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def _sync_directories(directories: Iterable["_OutputDirectory"]) -> None:
    """Make the renames in each directory durable, treating a failure as fatal.

    A directory ``fsync`` that fails has not established durability, so a caller
    that continues past it publishes a commit record it cannot honour: after a
    crash the manifest would name members whose renames were never persisted.
    The failure is therefore raised rather than logged, which is the difference
    between a protocol and a best effort.
    """
    for directory in directories:
        try:
            directory.sync()
        except OSError as error:  # pragma: no cover - requires a failing fsync
            raise EmitError(
                f"the directory {directory.path} could not be made durable "
                f"({error}), so the renames in it are not established on disk. "
                "Nothing further is published: a completion manifest renamed after "
                "a failed content sync would assert members a crash could lose"
            ) from error


def _digest_and_size(
    directory: _OutputDirectory,
    name: str,
    *,
    expect_inode: tuple[int, int] | None = None,
) -> tuple[int, str, tuple[int, int]]:
    """Return the byte size, sha256 and ``(st_dev, st_ino)`` of ``name``.

    Measured from the file rather than from the string that was written, because the
    bytes on the disk are what gets published and what a reader of the manifest will
    hash. Read in bounded chunks so the peak resident size does not track the
    deliverable's size.

    The identity is returned, and optionally required, because opening by NAME is
    what a re-open does: the staged file is written through one descriptor and then
    measured through another, and the name in between could have been replaced. The
    ``(st_dev, st_ino)`` pair is the only thing that binds the two descriptors to one
    file, so publication threads it from the write, through the measurement, to the
    post-rename re-measurement — a rename preserves the inode, so the pair that was
    validated is the pair that must be at the published path.

    Args:
        directory: The held output directory.
        name: The entry to measure within it.
        expect_inode: The ``(st_dev, st_ino)`` this measurement must describe, or
            ``None`` on the first measurement, which establishes it.

    Raises:
        EmitError: Where ``expect_inode`` is given and the file at ``name`` is a
            different file.
    """
    fd = directory.open_for_read(name)
    try:
        stat = os.fstat(fd)
        identity = (stat.st_dev, stat.st_ino)
        if expect_inode is not None and identity != expect_inode:
            raise EmitError(
                f"refusing to trust {directory.path / name}: it is now device "
                f"{identity[0]} inode {identity[1]}, not the device {expect_inode[0]} "
                f"inode {expect_inode[1]} this publication staged and validated. "
                "Something replaced the file between the write and this measurement."
            )
        size = stat.st_size
        digest = hashlib.sha256()
        while True:
            chunk = os.read(fd, _DIGEST_CHUNK)
            if not chunk:
                break
            digest.update(chunk)
    finally:
        os.close(fd)
    return size, digest.hexdigest(), identity


def publication_identifier(digests: Mapping[str, str]) -> str:
    """Return the identifier both members of one publication carry.

    Derived from the members' content digests exactly as
    :data:`PUBLICATION_IDENTIFIER_METHOD` states, so a consumer holding the published
    files can recompute it: digest each file, and the identifier follows. A generation
    mixed from two runs yields an identifier that matches neither run's recorded
    value, which is the detection this exists for (AAP 0.6.4 — one measurement, cited).

    Args:
        digests: The sha256 of each member, keyed by role.

    Returns:
        The first :data:`_PUBLICATION_ID_LENGTH` hex characters of the digest over the
        canonical material.

    >>> publication_identifier({"findings_csv": "b" * 64, "findings_json": "a" * 64})
    '02bd6e3433c0a606612d9724b5dbefbf'
    >>> publication_identifier({"findings_json": "a" * 64, "findings_csv": "b" * 64})
    '02bd6e3433c0a606612d9724b5dbefbf'
    """
    material = [PUBLICATION_SCHEME]
    for role in sorted(digests):
        material.append(f"{role}\t{digests[role]}")
    text = "\n".join(material) + "\n"
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:_PUBLICATION_ID_LENGTH]


@dataclass(frozen=True)
class _MemberPlan:
    """One member to publish: its role, its target, and how its bytes are rendered.

    ``newline`` differs between the two members and is not incidental: the JSON writer
    pins ``"\\n"`` so the deliverable's line endings do not follow the platform, and
    the CSV writer must pass ``""`` because :mod:`csv` requires it for an embedded
    newline inside a quoted field to be written verbatim rather than translated.
    """

    role: str
    target: Path
    newline: str
    render: Callable[[IO[str]], None]


@dataclass(frozen=True)
class _StagedMember:
    """One member staged, measured, and not yet moved into place."""

    plan: _MemberPlan
    directory: _OutputDirectory
    target_name: str
    temporary_name: str
    size_bytes: int
    sha256: str
    #: ``(st_dev, st_ino)`` of the file these bytes were written into, taken through
    #: the write descriptor. A rename preserves it, so it is what binds the staged
    #: file, the pre-publication measurement and the published path to one file.
    identity: tuple[int, int]

    @property
    def staged_path(self) -> Path:
        """The path the staged bytes currently sit at, for a validator to parse."""
        return self.directory.path / self.temporary_name


def _stage_member(plan: _MemberPlan, directory: _OutputDirectory) -> _StagedMember:
    """Write one member's bytes into a fresh exclusive file and measure them.

    The descriptor is opened before anything is written and closed before anything is
    measured, so the size and digest describe a completed file. A failure at any point
    removes the staged file: a leftover ``.staged`` sibling would be a partial output
    with a plausible name, which is exactly what the atomicity contract forbids.
    """
    target_name = _validated_target_name(plan.target, directory.path)
    temporary_name = _temporary_name(target_name)
    fd = directory.create_exclusive(temporary_name)
    try:
        handle = os.fdopen(fd, "w", encoding="utf-8", newline=plan.newline)
    except BaseException:
        os.close(fd)
        directory.unlink_quietly(temporary_name)
        raise
    try:
        with handle:
            plan.render(handle)
            handle.flush()
            # The bytes are on the device before any name points at them, which is
            # what makes the rename below a publication rather than a promise.
            os.fsync(handle.fileno())
            # Taken through the WRITE descriptor, before it is closed, so it is the
            # identity of the file the bytes went into rather than of whatever the
            # name resolves to afterwards. Everything downstream is bound to it.
            written = os.fstat(handle.fileno())
            written_identity = (written.st_dev, written.st_ino)
            written_size = written.st_size
        size_bytes, digest, identity = _digest_and_size(
            directory, temporary_name, expect_inode=written_identity
        )
        if size_bytes != written_size:
            raise EmitError(
                f"the staged file for {plan.target} was {written_size} bytes when its "
                f"write descriptor was closed and is {size_bytes} bytes now, on the "
                "same inode. Its length changed after it was written, so nothing is "
                "published."
            )
    except BaseException:
        directory.unlink_quietly(temporary_name)
        raise
    return _StagedMember(
        plan=plan,
        directory=directory,
        target_name=target_name,
        temporary_name=temporary_name,
        size_bytes=size_bytes,
        sha256=digest,
        identity=identity,
    )


def member_set_identifier(members: Sequence[Any]) -> str:
    """Return the byte-derived completion identifier for a set of published members.

    ``sha256`` over each member's role and content digest, in published order, joined
    the way :data:`MEMBER_SET_IDENTIFIER_METHOD` states, truncated to 32 hex
    characters.

    This is deliberately NOT :func:`publication_identifier`, and the difference is the
    reason a completion manifest can exist at all.  Both are derived from member
    digests here, but the identifier a *consumer* needs for completion has to be
    computable from nothing but the files on disk, which is what this is: digest each
    published member, in the order the manifest lists them, and the value follows.  A
    set missing a member, or assembled from two generations, yields a value matching
    neither.

    Args:
        members: Staged or published members, each carrying ``sha256`` and a role.

    Returns:
        The 32-character identifier.
    """
    material = "\n".join(
        f"{_member_role(member)}\x00{member.sha256}" for member in members
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[
        :MEMBER_SET_IDENTIFIER_LENGTH
    ]


def _member_role(member: Any) -> str:
    """Return a member's role, whether it is a staged member or a published one."""
    plan = getattr(member, "plan", None)
    return plan.role if plan is not None else member.role


def _manifest_plan(
    target: Path, staged: Sequence[_StagedMember], identifier: str
) -> _MemberPlan:
    """Return the plan that renders the completion manifest for ``staged``.

    The manifest names every content member, its published path, its byte size and
    its content digest, plus the byte-derived ``member_set_id``.  It cannot carry its
    own digest — a document containing its own hash is not constructible — so its
    identity is published in the run record's per-file manifest instead, and the
    manifest says so rather than leaving the omission to be noticed.
    """
    content = [member for member in staged]
    document = {
        "schema": MANIFEST_SCHEME,
        "document": str(target),
        "member_set_id": member_set_identifier(content),
        "member_set_id_method": MEMBER_SET_IDENTIFIER_METHOD,
        "publication_id": identifier,
        "publication_id_method": PUBLICATION_IDENTIFIER_METHOD,
        "member_count": len(content),
        "commit_protocol": MANIFEST_COMMIT_PROTOCOL,
        "required_of_consumers": MANIFEST_CONSUMER_REQUIREMENT,
        "self_digest": (
            "absent by construction: a manifest cannot contain its own digest. Its "
            "byte size and sha256 are published in the run record's per-file manifest"
        ),
        "members": [
            {
                "role": _member_role(member),
                "path": str(member.plan.target),
                "bytes": member.size_bytes,
                "sha256": member.sha256,
            }
            for member in content
        ],
    }

    def render(handle: IO[str]) -> None:
        json.dump(document, handle, indent=2, ensure_ascii=False)
        handle.write("\n")

    return _MemberPlan(
        role=ROLE_COMPLETION_MANIFEST, target=target, newline="\n", render=render
    )


def open_verified_member(
    path: str | Path,
    identity: tuple[int, int] | Sequence[int] | None = None,
) -> IO[str]:
    """Open a published member for reading, bound to the identity that was verified.

    The gap this closes is small and entirely real: a verification that opens each
    member, measures it and closes the descriptor establishes a fact about a file,
    while a reader that afterwards opens the same *pathname* establishes nothing —
    the name can be repointed in between.  Passing the identity
    :func:`require_publication_manifest` observed makes the reader refuse anything
    that is not the file the manifest was checked against.

    ``O_NOFOLLOW`` is used for the open itself, so the final component cannot be a
    symlink, and the mode is required to be a regular file: a FIFO at the path would
    otherwise block a reader indefinitely rather than fail it.

    Args:
        path: The member's published path.
        identity: The ``(st_dev, st_ino)`` pair the verification recorded, or ``None``
            to open without the binding — used only where no manifest was required,
            such as reading STAGED bytes before anything has been published.

    Returns:
        A text-mode handle positioned at the start of the file. ``newline=""`` is
        set, which the ``csv`` module requires and which ``json`` is indifferent to,
        so one opener serves both readers.

    Raises:
        EmitError: Where the path cannot be opened without following a link, is not
            a regular file, or is not the file whose identity was verified.
    """
    target = Path(path)
    try:
        fd = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as error:
        raise EmitError(
            f"{target} could not be opened for reading without following a link "
            f"({error})"
        ) from error
    try:
        info = os.fstat(fd)
        if not stat_module.S_ISREG(info.st_mode):
            raise EmitError(f"{target} is not a regular file")
        if identity is not None:
            wanted = tuple(int(part) for part in identity)
            if (info.st_dev, info.st_ino) != wanted:
                raise EmitError(
                    f"{target} is device {info.st_dev} inode {info.st_ino}, not the "
                    f"{wanted[0]}/{wanted[1]} the completion manifest was verified "
                    "against. The path now names a different file than the one that "
                    "was checked, so the verification does not describe what is about "
                    "to be read"
                )
    except BaseException:
        os.close(fd)
        raise
    return os.fdopen(fd, "r", encoding="utf-8", newline="")


def require_publication_manifest(
    manifest_path: str | Path,
    *,
    expected_roles: Sequence[str] | None = None,
    expected_paths: Mapping[str, str | Path] | None = None,
    expected_publication_id: str | None = None,
) -> dict[str, Any]:
    """Require a completion manifest and every member it names, or raise.

    The check a consumer must run before treating a published set as one generation,
    exposed as a function so that "required in consumers" is something callable rather
    than something asserted in prose.  :func:`_publish_members` runs it itself
    immediately after publishing, so a manifest that nothing verifies cannot occur on
    the producing side either.

    What is established, in order — each step catching something the previous one
    cannot:

    1. The manifest exists, is a **regular file** rather than a link or a device, and
       parses.  Opened ``O_NOFOLLOW`` and read through the descriptor, so a link
       planted at the manifest path is refused rather than followed.
    2. Its declared ``member_count`` equals the number of members it lists, and the
       roles are unique.  A manifest listing two members while declaring three is a
       truncated record, and a repeated role means two entries claim to be the same
       member.
    3. Where ``expected_roles`` is given, the role set matches **exactly** — no
       missing member and no extra one.  This is what refuses a *subset* manifest: a
       one-member record left behind by a failed three-member publication is
       internally consistent and would otherwise pass every other check here.
    4. Where ``expected_paths`` is given, each role's recorded path is the path the
       consumer expects, so a manifest describing some other generation's files
       elsewhere cannot vouch for these.
    5. Every member exists as a regular file and its bytes on disk match the recorded
       size and digest, measured no-follow through a descriptor.
    6. ``member_set_id`` recomputed from those digests equals the recorded one, which
       is what catches a manifest edited to match a changed member.
    7. Where ``expected_publication_id`` is given, the manifest's ``publication_id``
       matches, binding the commit record to the run the consumer thinks it is reading.

    Args:
        manifest_path: The manifest written by the publication.
        expected_roles: The exact role set this publication must carry, or ``None`` to
            accept whatever the manifest declares (weaker, and only appropriate where
            the consumer genuinely does not know the schema).
        expected_paths: Role to path, for the paths the consumer is about to read.
        expected_publication_id: The publication identifier the consumer expects.

    Returns:
        The parsed manifest, so a caller can report what it verified.

    Raises:
        EmitError: Where the manifest is absent, unparseable, incomplete, describes a
            different member set or different paths, or disagrees with any member on
            disk.
    """
    path = Path(manifest_path)
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError as exc:
        raise EmitError(
            f"the completion manifest {path} is absent, so the published set cannot be "
            "established as one generation. A publication that failed between its "
            "renames leaves exactly this state."
        ) from exc
    except OSError as exc:
        raise EmitError(
            f"the completion manifest {path} could not be opened without following a "
            f"link: {exc}. A commit record is a regular file at the path the run "
            "record names."
        ) from exc
    try:
        stat = os.fstat(fd)
        if not stat_module.S_ISREG(stat.st_mode):
            raise EmitError(
                f"{path} is not a regular file, so it is not a commit record this "
                "publication wrote"
            )
        raw = b""
        while True:
            chunk = os.read(fd, _DIGEST_CHUNK)
            if not chunk:
                break
            raw += chunk
    finally:
        os.close(fd)

    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise EmitError(f"the completion manifest {path} could not be parsed: {exc}") from exc
    if not isinstance(document, Mapping) or document.get("schema") != MANIFEST_SCHEME:
        raise EmitError(
            f"{path} does not carry the {MANIFEST_SCHEME} schema, so it is not a "
            "completion manifest for this publication"
        )
    members = document.get("members")
    if not isinstance(members, list) or not members:
        raise EmitError(f"the completion manifest {path} names no members")
    declared = document.get("member_count")
    if declared != len(members):
        raise EmitError(
            f"the completion manifest {path} declares member_count {declared} but "
            f"lists {len(members)} member(s), so the record is truncated or padded and "
            "nothing may rely on it"
        )

    roles = [str(entry.get("role")) for entry in members if isinstance(entry, Mapping)]
    if len(roles) != len(members):
        raise EmitError(f"the completion manifest {path} holds a malformed member")
    if len(set(roles)) != len(roles):
        raise EmitError(
            f"the completion manifest {path} repeats a member role ({', '.join(roles)}). "
            "A role names one member of one publication."
        )
    if expected_roles is not None and set(roles) != set(expected_roles):
        missing = sorted(set(expected_roles) - set(roles))
        extra = sorted(set(roles) - set(expected_roles))
        raise EmitError(
            f"the completion manifest {path} carries roles {sorted(roles)} where this "
            f"publication requires {sorted(expected_roles)}"
            + (f"; missing {missing}" if missing else "")
            + (f"; unexpected {extra}" if extra else "")
            + ". A manifest naming fewer members than the schema requires is exactly "
            "what a publication that failed between its renames leaves behind, and it "
            "is internally consistent, so only the expected role set refuses it."
        )
    if expected_publication_id is not None and (
        document.get("publication_id") != expected_publication_id
    ):
        raise EmitError(
            f"the completion manifest {path} records publication_id "
            f"{document.get('publication_id')}, not the {expected_publication_id} this "
            "consumer expects, so it describes another generation"
        )

    class _Measured:
        def __init__(self, role: str, sha256: str) -> None:
            self.role = role
            self.sha256 = sha256

    measured: list[_Measured] = []
    verified_identities: dict[str, tuple[int, int]] = {}
    for entry in members:
        if not isinstance(entry, Mapping):  # pragma: no cover - caught above
            raise EmitError(f"the completion manifest {path} holds a malformed member")
        role = str(entry.get("role"))
        member_path = Path(str(entry.get("path")))
        if expected_paths is not None:
            wanted = expected_paths.get(role)
            if wanted is None or Path(wanted) != member_path:
                raise EmitError(
                    f"the completion manifest {path} records {member_path} for role "
                    f"{role!r} where this consumer expects {wanted}. The record "
                    "describes files other than the ones about to be read."
                )
        try:
            member_fd = os.open(member_path, os.O_RDONLY | os.O_NOFOLLOW)
        except OSError as exc:
            raise EmitError(
                f"the completion manifest {path} names {member_path}, which could not "
                f"be opened as a regular file without following a link: {exc}. The "
                "published set is incomplete."
            ) from exc
        try:
            member_stat = os.fstat(member_fd)
            if not stat_module.S_ISREG(member_stat.st_mode):
                raise EmitError(
                    f"{member_path} is not a regular file, so it is not the member the "
                    f"completion manifest {path} describes"
                )
            # The identity of the file THIS descriptor refers to, taken while it is
            # still held. It is returned to the caller so consumption can be bound to
            # the file that was verified rather than to the pathname that named it:
            # between a verification that closes its descriptors and a reader that
            # reopens the path, the path can be made to refer to something else.
            member_identity = (member_stat.st_dev, member_stat.st_ino)
            digest = hashlib.sha256()
            size = 0
            while True:
                chunk = os.read(member_fd, _DIGEST_CHUNK)
                if not chunk:
                    break
                size += len(chunk)
                digest.update(chunk)
        finally:
            os.close(member_fd)
        verified_identities[role] = member_identity
        if size != entry.get("bytes") or digest.hexdigest() != entry.get("sha256"):
            raise EmitError(
                f"{member_path} holds {size} bytes / sha256 {digest.hexdigest()}, not "
                f"the {entry.get('bytes')} bytes / sha256 {entry.get('sha256')} the "
                f"completion manifest {path} records. The set is not one generation."
            )
        measured.append(_Measured(role, digest.hexdigest()))

    recomputed = member_set_identifier(measured)
    if recomputed != document.get("member_set_id"):
        raise EmitError(
            f"the completion manifest {path} records member_set_id "
            f"{document.get('member_set_id')} but its own member digests yield "
            f"{recomputed}, so the manifest and the set it describes disagree"
        )
    verified = dict(document)
    # Not part of the manifest's own bytes: this is what the verification observed,
    # handed to the caller so a reader can require the same files rather than the
    # same names. AAP 0.5.4's "verification is always independent of the thing
    # verified" cuts both ways — the check must also be usable by the consumer.
    verified["verified_member_identities"] = {
        role: list(identity) for role, identity in sorted(verified_identities.items())
    }
    return verified


def require_dataset_generation(
    json_path: str | Path,
    csv_path: str | Path,
    manifest_path: str | Path,
    *,
    publication_id: str | None = None,
) -> dict[str, Any]:
    """Require the dataset's completion manifest before either member is read.

    The consumer-side entry point for the dataset schema, so a reader does not have to
    know which roles the publication carries or where they live: it passes the two
    paths it is about to read and this refuses anything that is not one complete
    generation of exactly those two files.

    This exists because the weaker call — the manifest alone, with no expected role
    set — accepts a one-member record, and a one-member record is precisely what a
    two-member publication leaves behind when it fails between its renames.

    Args:
        json_path: The ``findings.json`` about to be read.
        csv_path: The ``findings.csv`` about to be read.
        manifest_path: The completion manifest the publication wrote.
        publication_id: The identifier the consumer expects, where it knows it.

    Returns:
        The verified manifest.

    Raises:
        EmitError: Where the set is not one complete generation of those two files.
    """
    return require_publication_manifest(
        manifest_path,
        expected_roles=(ROLE_FINDINGS_JSON, ROLE_FINDINGS_CSV),
        expected_paths={
            ROLE_FINDINGS_JSON: Path(json_path),
            ROLE_FINDINGS_CSV: Path(csv_path),
        },
        expected_publication_id=publication_id,
    )


def _publish_members(
    plans: Sequence[_MemberPlan],
    rows: int | None,
    validate: Callable[[Mapping[str, _StagedMember]], ComparisonResult | None] | None = None,
    tally: CsvNeutralisationTally | None = None,
    manifest_target: Path | None = None,
) -> PublicationResult:
    """Stage every member, validate them all, then move them all into place.

    This is the whole of AAP 0.6.2's "both files are written from the same validated
    rows ... neither derived from the other", and of the atomicity the dataset needs:
    the two deliverables are one generation of one row list, so a fault between them
    must not leave this run's ``findings.json`` beside the previous run's
    ``findings.csv``. Every member is staged and validated first; the first rename
    happens only once nothing is left that could fail for a reason inside this
    module's control.

    Args:
        plans: The members to publish, in the order they are to be recorded.
        rows: The number of dataset rows the members carry, or ``None`` where the
            member is not a row set.
        validate: Called with the staged members keyed by role once all of them are
            staged, and before any of them is published. It returns the comparison to
            record, or ``None`` where there is no second member to compare against. A
            validator that raises aborts the publication with nothing moved.
        tally: The CSV writer's neutralisation tally where a CSV member was rendered,
            so the count in the result is the writer's own rather than a second pass.

    Returns:
        The :class:`PublicationResult`, both members sharing one identifier.

    Raises:
        EmitError: Where a directory or target is refused, where validation fails, or
            where a published file's digest does not equal the validated one.
        OSError: Where staging, syncing or renaming fails.
    """
    directories: dict[Path, _OutputDirectory] = {}
    staged: list[_StagedMember] = []
    try:
        for plan in plans:
            parent = _validated_directory(plan.target)
            directory = directories.get(parent)
            if directory is None:
                directory = _OutputDirectory(parent)
                directories[parent] = directory
            staged.append(_stage_member(plan, directory))

        comparison = None
        if validate is not None:
            comparison = validate({member.plan.role: member for member in staged})

        digests = {member.plan.role: member.sha256 for member in staged}
        identifier = publication_identifier(digests)

        # THE COMMIT RECORD, staged before the first rename and renamed LAST.
        #
        # Staging every member before the first rename closes the window before the
        # renames. It does not close the window between them: N renames are N atomic
        # operations rather than one, and a fault after the first cannot be undone.
        # POSIX has no N-way atomic rename, so the remaining window is closed with a
        # record instead — one whose presence means "every member named here is in
        # place" and whose absence means the set on disk is not one generation.
        manifest_member = None
        if manifest_target is not None:
            manifest_plan = _manifest_plan(Path(manifest_target), staged, identifier)
            manifest_parent = _validated_directory(manifest_plan.target)
            manifest_directory = directories.get(manifest_parent)
            if manifest_directory is None:
                manifest_directory = _OutputDirectory(manifest_parent)
                directories[manifest_parent] = manifest_directory
            manifest_member = _stage_member(manifest_plan, manifest_directory)
            staged.append(manifest_member)

        content = [m for m in staged if m is not manifest_member]

        # Past this point every member has been written, fsynced, measured and
        # validated. The renames are the only remaining step, and each is atomic.
        for member in content:
            member.directory.replace(member.temporary_name, member.target_name)

        # THE CONTENT RENAMES ARE MADE DURABLE BEFORE THE MANIFEST RENAME.
        #
        # Ordering matters to the one reader that matters, a reader after a crash.
        # Renaming the manifest and syncing everything afterwards can persist the
        # commit record while a content rename is still only in the page cache, so
        # recovery would find a manifest asserting members that are not there —
        # which is precisely the state the manifest exists to make impossible.
        # Syncing content first makes the manifest's presence imply theirs.
        _sync_directories(
            {id(m.directory): m.directory for m in content}.values()
        )

        # LAST, and only once every member it describes is at its published path
        # AND durable there. Its own directory is synced separately, after it.
        if manifest_member is not None:
            manifest_member.directory.replace(
                manifest_member.temporary_name, manifest_member.target_name
            )
            _sync_directories([manifest_member.directory])

        published: list[PublicationMember] = []
        for member in content:
            # Bound to the inode the bytes were written into: a rename preserves it,
            # so requiring it here is what makes "these are the validated bytes" a
            # statement about one file rather than about one pathname.
            size_bytes, digest, _identity = _digest_and_size(
                member.directory, member.target_name, expect_inode=member.identity
            )
            if (size_bytes, digest) != (member.size_bytes, member.sha256):
                raise EmitError(
                    f"{member.plan.target} was published, but its bytes on disk "
                    f"({size_bytes} bytes, sha256 {digest}) are not the bytes that were "
                    f"validated ({member.size_bytes} bytes, sha256 {member.sha256}). "
                    "Something else wrote that path during this publication."
                )
            published.append(
                PublicationMember(
                    role=member.plan.role,
                    path=str(member.plan.target),
                    size_bytes=size_bytes,
                    sha256=digest,
                    publication_id=identifier,
                )
            )
        completion = None
        if manifest_member is not None:
            size_bytes, digest, _identity = _digest_and_size(
                manifest_member.directory,
                manifest_member.target_name,
                expect_inode=manifest_member.identity,
            )
            # THE PRODUCER IS THE FIRST CONSUMER. Everything the manifest names has
            # just been re-measured from its published path above, so reaching here
            # means the record and the disk agree. A manifest nothing verifies is
            # decoration, and `require_publication_manifest` is the same check
            # exposed for any later consumer to run.
            completion = {
                "path": str(manifest_member.plan.target),
                "bytes": size_bytes,
                "sha256": digest,
                "member_set_id": member_set_identifier(content),
                "renamed": "last, after every member it names was in place",
                "verified_by_producer": True,
                "verified_members": [m.plan.role for m in content],
                "required_of_consumers": (
                    "a consumer must require this file and re-measure every member it "
                    "names before treating the set as one generation; "
                    "normalize.emit.require_publication_manifest is that check"
                ),
            }
        staged.clear()
        return PublicationResult(
            scheme=PUBLICATION_SCHEME,
            publication_id=identifier,
            identifier_method=PUBLICATION_IDENTIFIER_METHOD,
            members=tuple(published),
            rows=rows,
            comparison=comparison,
            staging=staging_protocol(),
            csv_neutralisation=None if tally is None else tally.as_dict(),
            completion_manifest=completion,
        )
    except BaseException:
        # Nothing was published, or the publication failed after a rename this
        # module cannot undo. Either way no staged file survives to be mistaken for
        # a deliverable, and the exception carries the reason (never swallowed).
        for member in staged:
            member.directory.unlink_quietly(member.temporary_name)
        raise
    finally:
        for directory in directories.values():
            directory.close()


def _publish_dataset(
    validated: Sequence[Mapping[str, Any]],
    json_path: Path,
    csv_path: Path,
    manifest_path: Path | None = None,
) -> PublicationResult:
    """Publish both dataset members from one validated row list, as one generation.

    The typed re-parse comparison runs on the STAGED bytes, before either file is
    moved into place, so a pair that disagreed would never become the deliverable.
    Those exact bytes are then published, and each published file is re-digested and
    required to equal what was validated — so the comparison recorded against the
    final paths is a statement about the files now at those paths.
    """
    tally = CsvNeutralisationTally()
    plans = (
        _MemberPlan(
            role=ROLE_FINDINGS_JSON,
            target=json_path,
            newline="\n",
            render=lambda handle: _render_json(validated, handle),
        ),
        _MemberPlan(
            role=ROLE_FINDINGS_CSV,
            target=csv_path,
            # newline="" is required by the csv module so a message carrying an
            # embedded newline is written inside its quoted field verbatim rather
            # than being translated on the way out.
            newline="",
            render=lambda handle: _render_csv(validated, handle, tally),
        ),
    )

    def validate(members: Mapping[str, _StagedMember]) -> ComparisonResult:
        json_rows = read_findings_json(members[ROLE_FINDINGS_JSON].staged_path)
        csv_rows = read_findings_csv(members[ROLE_FINDINGS_CSV].staged_path)
        comparison = _compare_rows(json_rows, csv_rows, str(json_path), str(csv_path))
        # A disagreement here cannot come from the rows — both members were rendered
        # from the same validated list — so it is a fault in a writer or a reader, and
        # publishing a knowingly inconsistent pair would bury it. Refuse instead, and
        # carry the comparison out with the exception so the caller records the same
        # measurement it would have recorded on a pass.
        if not comparison.passed:
            raise ComparisonFailed(comparison)
        return comparison

    return _publish_members(
        plans,
        rows=len(validated),
        validate=validate,
        tally=tally,
        manifest_target=manifest_path,
    )


def publish_document(
    path: str | Path,
    render: Callable[[IO[str]], None],
    *,
    role: str,
    newline: str = "\n",
) -> PublicationMember:
    """Publish one text document at ``path`` under this module's write protocol.

    The dataset is not the only file this pipeline writes that must not be truncated,
    redirected through a symlink or raced by a concurrent writer:
    ``harness/artifacts/logs/normalize-run.json`` is the record every number in the
    result documents is traced back to, and a half-written record is worse than none.
    Rather than a second implementation of the same four steps in ``cli.py``, this
    module owns the protocol and exposes it once — one guarded sequence, one place to
    audit (CWE-59, CWE-367).

    Args:
        path: Where the document is published.
        render: Called with an open text handle; everything it writes becomes the
            document. It is called exactly once, on the staged file.
        role: The role recorded for the member, supplied by the caller because this
            module does not know what document it is publishing.
        newline: The handle's newline translation. ``"\\n"`` pins Unix line endings;
            pass ``""`` where the writer manages its own line terminators.

    Returns:
        The published :class:`PublicationMember`, carrying the document's byte size,
        its sha256 and the identifier derived from that digest.

    Raises:
        EmitError: Where the directory or the target path is refused as unsafe.
        OSError: Where the document cannot be written.
    """
    publication = _publish_members(
        (_MemberPlan(role=role, target=Path(path), newline=newline, render=render),),
        rows=None,
    )
    return publication.member(role)


def write_findings_json(
    rows: Iterable[Mapping[str, Any]], path: str | Path
) -> list[dict[str, Any]]:
    """Validate ``rows`` and write ``findings.json`` at ``path``.

    Args:
        rows: The rows to write, in the order they are to appear.
        path: The output path — ``oss-scan-results/findings.json`` in this run,
            passed in rather than hardcoded here.

    Returns:
        The validated rows, so a caller can write the CSV from the same objects.

    Raises:
        EmitError: Where any row is invalid, or where the output directory or target
            path is refused as unsafe. Nothing is written in either case.
        OSError: Where the file cannot be written.
    """
    validated = validate_rows(rows)
    _publish_members(
        (
            _MemberPlan(
                role=ROLE_FINDINGS_JSON,
                target=Path(path),
                newline="\n",
                render=lambda handle: _render_json(validated, handle),
            ),
        ),
        rows=len(validated),
    )
    return validated


def write_findings_csv(
    rows: Iterable[Mapping[str, Any]], path: str | Path
) -> list[dict[str, Any]]:
    """Validate ``rows`` and write ``findings.csv`` at ``path``.

    Args:
        rows: The rows to write, in the order they are to appear.
        path: The output path — ``oss-scan-results/findings.csv`` in this run,
            passed in rather than hardcoded here.

    Returns:
        The validated rows, so a caller can write the JSON from the same objects.

    Raises:
        EmitError: Where any row is invalid, or where the output directory or target
            path is refused as unsafe. Nothing is written in either case.
        OSError: Where the file cannot be written.
    """
    validated = validate_rows(rows)
    tally = CsvNeutralisationTally()
    _publish_members(
        (
            _MemberPlan(
                role=ROLE_FINDINGS_CSV,
                target=Path(path),
                newline="",
                render=lambda handle: _render_csv(validated, handle, tally),
            ),
        ),
        rows=len(validated),
        tally=tally,
    )
    return validated


def write_findings(
    rows: Iterable[Mapping[str, Any]],
    json_path: str | Path,
    csv_path: str | Path,
    manifest_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Write both files from one validated in-memory row list, as one publication.

    AAP 0.6.2: both files are written from the same validated rows, "then
    re-parsed with typed coercion and compared; neither is derived from the other
    after writing". The rows are validated once and rendered twice, so the CSV is
    never generated from the JSON file or the JSON from the CSV.

    Both members are staged, validated and fsynced before either is moved into place,
    so the two deliverables are always one generation of one row list: a fault part
    way through leaves both previous files exactly as they were, rather than this
    run's ``findings.json`` beside the previous run's ``findings.csv``.

    Args:
        rows: The rows to write, in the order they are to appear in both files.
        json_path: Where ``findings.json`` is written.
        csv_path: Where ``findings.csv`` is written.

    Returns:
        The validated rows that were written to both files. Callers that need the
        publication identifier and each member's digest call :func:`publish_findings`,
        which returns them; this function's return value is unchanged.

    Raises:
        ComparisonFailed: Where the two staged files did not agree. Neither is
            published, and the comparison travels on the exception.
        EmitError: Where any row is invalid, or a directory or target path is refused
            as unsafe. Neither file is written in that case.
        OSError: Where either file cannot be written.
    """
    validated = validate_rows(rows)
    _publish_dataset(
        validated,
        Path(json_path),
        Path(csv_path),
        None if manifest_path is None else Path(manifest_path),
    )
    return validated


def publish_findings(
    rows: Iterable[Mapping[str, Any]],
    json_path: str | Path,
    csv_path: str | Path,
    manifest_path: str | Path | None = None,
) -> PublicationResult:
    """Publish both dataset members as one generation and return the publication.

    The same work :func:`write_findings` does, returning the publication rather than
    the rows: the identifier both members carry, each member's byte size and sha256,
    the write protocol they were published under, and the typed re-parse comparison
    established over the staged bytes before either file was moved into place.
    ``cli.py`` serialises it into ``harness/artifacts/logs/normalize-run.json``, where
    it is what lets a reader detect a dataset assembled from two different runs.

    Args:
        rows: The rows to write, in the order they are to appear in both files.
        json_path: Where ``findings.json`` is published.
        csv_path: Where ``findings.csv`` is published.

    Returns:
        The :class:`PublicationResult`, whose ``comparison`` is never ``None``.

    Raises:
        ComparisonFailed: Where the two staged files did not agree. Neither is
            published, and the comparison travels on the exception.
        EmitError: Where any row is invalid, or a directory or target path is refused
            as unsafe. Neither file is written in that case.
        OSError: Where either file cannot be written.
    """
    validated = validate_rows(rows)
    publication = _publish_dataset(
        validated,
        Path(json_path),
        Path(csv_path),
        None if manifest_path is None else Path(manifest_path),
    )
    if manifest_path is not None:
        # The producer is the first consumer, and it runs the STRICT form of the
        # check rather than a form its own success happens to satisfy: the exact
        # role set this schema requires, the exact paths, and the publication
        # identifier. The role-set assertion is the one that refuses a SUBSET
        # manifest — a one-member record left by a failed two-member publication
        # is internally consistent and passes every other check.
        require_publication_manifest(
            manifest_path,
            expected_roles=(ROLE_FINDINGS_JSON, ROLE_FINDINGS_CSV),
            expected_paths={
                ROLE_FINDINGS_JSON: Path(json_path),
                ROLE_FINDINGS_CSV: Path(csv_path),
            },
            expected_publication_id=publication.publication_id,
        )
    return publication


# --------------------------------------------------------------------------- #
# The readers — both files parsed back from disk, the CSV typed on the way in
# --------------------------------------------------------------------------- #


def read_findings_json(
    path: str | Path,
    identity: tuple[int, int] | Sequence[int] | None = None,
) -> list[dict[str, Any]]:
    """Parse ``findings.json`` and return its rows, validating shape and field order.

    The file must be a top-level JSON array of objects, each carrying exactly the
    twelve fields **in ``FIELDS`` order** — ``json`` preserves insertion order, so
    reading ``list(obj.keys())`` back is a genuine check on the written order
    rather than a restatement of the writer's intent.

    Args:
        path: The ``findings.json`` path to parse.

    Returns:
        The rows as ordered mappings, with JSON's own types: str, int, bool, None.

    Raises:
        EmitError: Where the document is not a row-only array, a row is not the
            twelve fields in order, or a value is not what its field carries.
        OSError: Where the file cannot be read.
        json.JSONDecodeError: Where the file is not valid JSON.
    """
    target = Path(path)
    with open_verified_member(target, identity) as handle:
        document = json.loads(handle.read())
    if not isinstance(document, list):
        raise EmitError(
            f"{target}: findings.json is a top-level JSON array of row objects — "
            f"row-only, with no metadata envelope; observed {type(document).__name__}"
        )

    rows: list[dict[str, Any]] = []
    for index, entry in enumerate(document):
        if not isinstance(entry, dict):
            raise EmitError(
                f"{target}: row {index}: expected a JSON object; observed "
                f"{type(entry).__name__}"
            )
        observed_order = list(entry.keys())
        if observed_order != list(FIELDS):
            raise EmitError(
                f"{target}: row {index}: expected the twelve fields in order "
                f"{list(FIELDS)}; observed {observed_order}"
            )
        ordered: dict[str, Any] = {}
        for field in FIELDS:
            ordered[field] = _validated_value(field, entry[field], index)
        rows.append(ordered)
    return rows


def _coerced_csv_cell(field: str, cell: str, row_index: int) -> Any:
    """Coerce one CSV cell to the type its field carries on the wire.

    AAP 0.5.4 fixes the coercion: ``start_line`` to an integer or ``None``,
    ``in_scope`` to a boolean **from the literal written**, and every empty
    optional field to ``None``. A cell that will not coerce is a failure rather
    than a ``None``, because silently reading it as absent would make a corrupted
    file compare equal to a correct one.

    A text cell additionally has the spreadsheet neutralisation reversed by
    :func:`restore_csv_text` — the exact inverse of what the writer applied — so what
    this returns is the tool's own text and the typed comparison against
    ``findings.json`` is over the values the adapters produced rather than over the
    bytes the CSV carries. Absence is decided *after* the inverse, which is safe
    because the rule never turns a non-empty value into an empty cell nor an empty
    cell into a value.
    """
    where = f"row {row_index}, field '{field}'"

    if field == _BOOLEAN_FIELD:
        # Derived from the literal, never `bool(cell)` — which would make the
        # string "false" truthy and turn a real disagreement into a pass.
        if cell == CSV_TRUE:
            return True
        if cell == CSV_FALSE:
            return False
        raise EmitError(
            f"{where}: in_scope must be the literal '{CSV_TRUE}' or '{CSV_FALSE}'; "
            f"observed {cell!r}"
        )

    if field == _INTEGER_FIELD:
        if cell == CSV_ABSENT:
            return None
        if not _START_LINE_CELL_RE.match(cell):
            raise EmitError(
                f"{where}: start_line must be an empty field or a plain integer of "
                f"at least 1; observed {cell!r}. Line numbering is one-based and "
                "absence is an empty field, so neither 0 nor a leading zero can have "
                "been written by this module's writer."
            )
        return int(cell)

    text = restore_csv_text(cell)

    if field in _OPTIONAL_TEXT_FIELDS:
        # An empty field is absence, matching JSON null (AAP 0.5.4).
        return None if text == CSV_ABSENT else text

    # A required text field: an empty cell is a failure, since absence is not
    # permitted for it and the CSV has no other way to spell one.
    if text == CSV_ABSENT:
        raise EmitError(
            f"{where}: this field is never absent, so an empty CSV field is a fault"
            + (
                " — path and severity_norm are the dataset's two mandatory fields"
                if field in ("path", "severity_norm")
                else ""
            )
        )
    return text


def read_findings_csv(
    path: str | Path,
    identity: tuple[int, int] | Sequence[int] | None = None,
) -> list[dict[str, Any]]:
    """Parse ``findings.csv`` and return its rows with every cell typed.

    The header must be exactly the twelve fields in ``FIELDS`` order and every
    record must carry exactly twelve cells; ``csv.reader`` is used rather than
    ``DictReader`` precisely so a missing or extra column is a fault instead of
    being absorbed into a rest key or a default.

    Args:
        path: The ``findings.csv`` path to parse.

    Returns:
        The rows as ordered mappings, coerced to the same types the JSON carries.

    Raises:
        EmitError: Where the header, the field count, or any cell is wrong.
        OSError: Where the file cannot be read.
    """
    target = Path(path)
    rows: list[dict[str, Any]] = []
    # newline="" is required by the csv module so an embedded newline inside a
    # quoted message is returned as part of that field rather than splitting it.
    with open_verified_member(target, identity) as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            raise EmitError(
                f"{target}: findings.csv carries no header row; expected {list(FIELDS)}"
            ) from None
        if header != list(FIELDS):
            raise EmitError(
                f"{target}: header must be the twelve fields in order {list(FIELDS)}; "
                f"observed {header}"
            )
        for index, record in enumerate(reader):
            if len(record) != len(FIELDS):
                raise EmitError(
                    f"{target}: row {index}: expected {len(FIELDS)} fields; observed "
                    f"{len(record)} ({record!r})"
                )
            ordered: dict[str, Any] = {}
            for field, cell in zip(FIELDS, record, strict=True):
                ordered[field] = _validated_value(
                    field, _coerced_csv_cell(field, cell, index), index
                )
            rows.append(ordered)
    return rows


# --------------------------------------------------------------------------- #
# The typed re-parse comparison (AAP 0.5.4)
# --------------------------------------------------------------------------- #


def _values_equal(left: Any, right: Any) -> bool:
    """Compare two field values with their types, so a bool never equals an int.

    ``True == 1`` in Python, and ``None`` compares equal to nothing else, so the
    type is compared first. Without that, an ``in_scope`` of ``True`` would match a
    ``start_line``-style ``1`` and a real divergence would read as agreement.
    """
    if type(left) is not type(right):
        return False
    return bool(left == right)


def _compare_rows(
    json_rows: Sequence[Mapping[str, Any]],
    csv_rows: Sequence[Mapping[str, Any]],
    json_path: str,
    csv_path: str,
) -> ComparisonResult:
    """Compare two parsed row sequences in order and locate the first divergence.

    The walk is ordered and stops at the first mismatch: a boolean pass/fail with
    no locator is not diagnosable, so the row index and field name travel with the
    result.
    """
    common = min(len(json_rows), len(csv_rows))
    rows_examined = 0
    fields_compared = 0
    first_mismatch: Mismatch | None = None

    for index in range(common):
        rows_examined = index + 1
        json_row = json_rows[index]
        csv_row = csv_rows[index]
        for field in FIELDS:
            json_value = json_row[field]
            csv_value = csv_row[field]
            fields_compared += 1
            if not _values_equal(json_value, csv_value):
                first_mismatch = Mismatch(
                    kind=MISMATCH_FIELD_VALUE,
                    row_index=index,
                    field=field,
                    json_value=json_value,
                    csv_value=csv_value,
                    detail=(
                        f"row {index}, field '{field}': findings.json carries "
                        f"{json_value!r} and findings.csv coerces to {csv_value!r}"
                    ),
                )
                break
        if first_mismatch is not None:
            break

    if first_mismatch is None and len(json_rows) != len(csv_rows):
        # The two files do not carry the same row positions, so the ordered walk
        # could not run past the shorter sequence. This is the reason the
        # comparison stopped — not a second measurement of the row count, which is
        # reconcile.py's to make against the reconciliation identity (AAP 0.6.4).
        first_mismatch = Mismatch(
            kind=MISMATCH_ROW_SEQUENCE_LENGTH,
            row_index=common,
            field=None,
            json_value=len(json_rows),
            csv_value=len(csv_rows),
            detail=(
                f"the ordered comparison ran out of rows at position {common}: the two "
                "files do not carry the same row positions, so no field could be "
                "compared there"
            ),
        )

    return ComparisonResult(
        passed=first_mismatch is None,
        rows_compared=rows_examined,
        fields_compared=fields_compared,
        field_order=FIELDS,
        json_path=json_path,
        csv_path=csv_path,
        first_mismatch=first_mismatch,
    )


def compare_outputs(
    json_path: str | Path,
    csv_path: str | Path,
    manifest_path: str | Path | None = None,
    *,
    publication_id: str | None = None,
) -> ComparisonResult:
    """Re-parse both written files and assert they agree field for field.

    Where ``manifest_path`` is given, the dataset's completion manifest is
    **required before either file is opened**, through
    :func:`require_dataset_generation`.  That is the consumer half of the commit
    protocol: field-for-field agreement between two files says nothing about whether
    they are one generation — two files from different runs can agree — so a consumer
    that compares them without requiring the commit record is checking the weaker of
    the two properties.  It is optional only because the comparison is also run on
    STAGED bytes before either file has a published path or a manifest to require.

    AAP 0.5.4: "Before comparing, the re-parsed CSV rows are coerced ... The
    comparison is then ordered row by row and field by field against the parsed
    JSON. Equality is asserted by parsing both files, never by counting lines."
    Both files are read back from disk here — the in-memory rows that produced them
    are deliberately not consulted, because a comparison against the source of
    both files would prove nothing about what reached the disk.

    Args:
        json_path: The ``findings.json`` written by this module.
        csv_path: The ``findings.csv`` written by this module.

    Returns:
        A :class:`ComparisonResult`. The outcome is data: it does not raise on
        disagreement, so ``cli.py`` can record it in normalize-run.json and decide
        what disagreement means for the run. Call
        :meth:`ComparisonResult.raise_if_failed` to turn it into an exception.

    Raises:
        EmitError: Where either file cannot be parsed as this schema at all — a
            wrong header, a wrong field count, an uncoercible cell. Those are
            faults in the file rather than disagreements between the two.
    """
    json_identity = csv_identity = None
    if manifest_path is not None:
        verified = require_dataset_generation(
            json_path, csv_path, manifest_path, publication_id=publication_id
        )
        # Bound, not merely required: the identities the verification observed are
        # handed to the readers, so each reads the file that was checked rather than
        # whatever the pathname resolves to now.
        identities = verified.get("verified_member_identities", {})
        json_identity = identities.get(ROLE_FINDINGS_JSON)
        csv_identity = identities.get(ROLE_FINDINGS_CSV)
    json_rows = read_findings_json(json_path, json_identity)
    csv_rows = read_findings_csv(csv_path, csv_identity)
    return _compare_rows(json_rows, csv_rows, str(json_path), str(csv_path))


def emit_findings(
    rows: Iterable[Mapping[str, Any]],
    json_path: str | Path,
    csv_path: str | Path,
) -> ComparisonResult:
    """Write both output files from one row list, then prove they agree.

    The whole module in one call, in the order AAP 0.5.4 fixes: validate the rows,
    render both files from those same rows, read both back from disk, coerce the
    CSV, and compare in order field by field. The re-parse happens on the staged
    bytes, before either file is moved into place, and those exact bytes are then
    published and re-digested — so a pair that disagreed never becomes the
    deliverable, and the comparison is still a statement about the two published
    files.

    Args:
        rows: The dataset rows, in the order they are to appear in both files.
            The order is preserved exactly; nothing is sorted, grouped or
            deduplicated (AAP 0.3.2).
        json_path: Where ``findings.json`` is written.
        csv_path: Where ``findings.csv`` is written.

    Returns:
        The :class:`ComparisonResult` for ``cli.py`` to serialise into
        ``harness/artifacts/logs/normalize-run.json``. Callers that also need the
        publication identifier and the per-member digests call
        :func:`publish_findings`.

    Raises:
        ComparisonFailed: Where the two files did not agree. Neither is published,
            and the comparison travels on the exception.
        EmitError: Where a row is invalid, a directory or target path is refused as
            unsafe, or a written file cannot be parsed back as this schema.
        OSError: Where either file cannot be written or read.
    """
    publication = publish_findings(rows, json_path, csv_path)
    comparison = publication.comparison
    if comparison is None:  # pragma: no cover - _publish_dataset always sets it
        raise EmitError(
            f"{json_path} and {csv_path} were published without a comparison being "
            "established, which is itself a fault"
        )
    return comparison


# ------------------------------------------------------------------------- #
# Output containment and staged writes (CWE-59, CWE-73, CWE-367)
#
# The guarded write primitives every document this module publishes goes
# through, and the containment predicate `cli.py` binds each output to its one
# owner root with: a target is refused where it escapes that root, where it
# aliases another output, or where any component of its path is a symlink, and a
# staged file is created with O_CREAT|O_EXCL|O_WRONLY|O_NOFOLLOW under a name an
# attacker cannot predict, fsynced, and moved into place atomically.
# ------------------------------------------------------------------------- #

class UnsafeOutputPath(EmitError):
    """A target this module refuses to write, naming the component that is wrong.

    Raised before anything is opened, for every condition under "THE WRITE
    DISCIPLINE" above: a target or an at-or-below-root component that is a
    symbolic link (CWE-59), a target that exists as something other than a
    regular file, a target outside the owner root the caller declared (CWE-73),
    and the two dataset files resolving to one file. A subclass of
    :class:`EmitError` so a caller that already handles this module's faults
    handles these too, and distinct so a caller that wants to report a path fault
    differently from a schema fault can.
    """


#: Bytes of randomness in a staged file's name. Sixteen hex characters: the name
#: cannot be predicted, so it cannot be pre-created as a symlink between two runs
#: the way a deterministic `<name>.partial` sibling could (CWE-59).
_STAGED_TOKEN_BYTES = 8


#: Mode requested when a staged file is created. 0o666 masked by the process
#: umask is exactly what `open(path, "w")` produced before, so the permissions of
#: a published deliverable are unchanged by this discipline.
_STAGED_FILE_MODE = 0o666


#: The flags every write in this package uses. O_EXCL so an existing file at the
#: staged name is an error rather than a target; O_NOFOLLOW so a symlink at that
#: name is an error rather than a redirection to somewhere else (CWE-59);
#: O_CLOEXEC where the platform defines it, so a staged descriptor is not
#: inherited by anything this process starts.
_STAGED_OPEN_FLAGS = (
    os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
)


@dataclass(frozen=True)
class StagedWrite:
    """One file written in full but not yet visible at its destination.

    Attributes:
        target: Where the file will appear once the set it belongs to is
            promoted. Nothing is at this path yet, and the file already published
            there — if any — is still the published one.
        temporary: The staged file, complete on disk beside the target.
        bytes_written: The staged file's byte size as the filesystem reports it,
            so a caller's record carries a measurement rather than the length of
            the string it hoped to write.
    """

    target: Path
    temporary: Path
    bytes_written: int

    def as_dict(self) -> dict[str, Any]:
        """Return the staged write as a JSON-serialisable mapping."""
        return {
            "target": str(self.target),
            "staged_as": str(self.temporary),
            "bytes_written": self.bytes_written,
        }


def assert_safe_output_path(
    path: str | Path, *, boundary: str | Path | None = None
) -> dict[str, Any]:
    """Refuse ``path`` as a write target where any component makes it unsafe.

    The checks, each of which names the component that failed:

    * every component at or below ``boundary`` is examined with ``lstat``, and a
      symbolic link anywhere among them — including the target itself — is
      refused (CWE-59);
    * a target that exists as anything other than a regular file is refused,
      because a directory, a fifo or a device at that path is not a deliverable
      this module can replace;
    * where ``boundary`` is given, a target outside it is refused (CWE-73): the
      caller declared the root that owns this file, and a path that escapes it is
      a configuration fault rather than a location.

    ``boundary`` is the *declared owner root*, and the asymmetry is deliberate.
    Components above it — the clone's own location, whatever mount it sits on —
    are how the run was invoked: they are recorded, and refusing them would make
    a legitimately symlinked checkout unwritable. Components at or below it are
    the ones something could have planted after the run declared its root, and
    those are refused. With no boundary every component up to the filesystem root
    is checked, which is the stricter default and is what a direct caller with no
    owner root to declare gets.

    Args:
        path: The intended target.
        boundary: The declared owner root, or ``None`` to check every component.

    Returns:
        A JSON-serialisable record of what was checked, for the caller's own log.

    Raises:
        UnsafeOutputPath: Where any check above fails.
    """
    target = Path(os.path.abspath(os.path.expanduser(str(path))))
    chain: list[Path] = [*reversed(target.parents), target]
    boundary_path: Path | None = None
    if boundary is not None:
        boundary_path = Path(os.path.abspath(os.path.expanduser(str(boundary))))
        if boundary_path not in chain:
            raise UnsafeOutputPath(
                f"{target} is not inside the owner root {boundary_path} that was "
                "declared for it; an output path that escapes its owner is a "
                "configuration fault, not a location"
            )
        checked = chain[chain.index(boundary_path) + 1 :]
    else:
        # Path('/').parent is Path('/'), so this drops the filesystem root, which
        # is the one component that can never be a link.
        checked = [component for component in chain if component != component.parent]

    for component in checked:
        if os.path.islink(component):
            raise UnsafeOutputPath(
                f"refusing to write {target}: the path component {component} is a "
                "symbolic link. A write that follows a link lands wherever the link "
                "points, which is not the location this run declared"
            )
    if os.path.lexists(target) and not os.path.isfile(target):
        raise UnsafeOutputPath(
            f"refusing to write {target}: it exists and is not a regular file"
        )
    return {
        "target": str(target),
        "owner_root": None if boundary_path is None else str(boundary_path),
        "components_checked": [str(component) for component in checked],
        "symlinked_components": [],
        "target_exists": os.path.lexists(target),
        "checks": [
            "no component at or below the owner root is a symbolic link (lstat)",
            "the target, where it exists, is a regular file",
            (
                "the target is inside the owner root"
                if boundary_path is not None
                else "no owner root was declared, so every component up to the "
                "filesystem root was checked"
            ),
        ],
    }


def _stage(
    target: Path,
    *,
    newline: str,
    boundary: str | Path | None,
    serialise: Callable[[TextIO], None],
) -> StagedWrite:
    """Write one file to an exclusive no-follow temporary beside ``target``.

    The target is checked first, the directory created if it does not exist, and
    the content written, flushed and fsynced before the descriptor is closed — so
    a :class:`StagedWrite` that comes back describes bytes that are on the device,
    not bytes in a buffer. Nothing is visible at ``target``: promotion is
    :func:`promote_staged`, and a staged file nobody promotes is removed by
    :func:`discard_staged`.
    """
    assert_safe_output_path(target, boundary=boundary)
    parent = target.parent
    if str(parent) not in ("", "."):
        parent.mkdir(parents=True, exist_ok=True)
    temporary = parent / f".{target.name}.{secrets.token_hex(_STAGED_TOKEN_BYTES)}.partial"
    descriptor = os.open(temporary, _STAGED_OPEN_FLAGS, _STAGED_FILE_MODE)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline=newline) as handle:
            serialise(handle)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        # The descriptor is closed by the context manager on every path out of
        # it, including this one; only the file itself has to be removed.
        temporary.unlink(missing_ok=True)
        raise
    return StagedWrite(
        target=target,
        temporary=temporary,
        bytes_written=temporary.stat().st_size,
    )


def stage_text(
    path: str | Path,
    text: str,
    *,
    newline: str = "\n",
    boundary: str | Path | None = None,
) -> StagedWrite:
    """Stage ``text`` for ``path`` under the write discipline, without publishing it.

    The primitive ``cli.py`` writes ``harness/artifacts/logs/normalize-run.json``
    with, so the run record and the dataset are written under one set of rules
    rather than two copies of them. The caller verifies the staged file — re-reads
    it and parses it — and then calls :func:`promote_staged`, so a record that
    cannot be read back is never published as this run's evidence.

    Args:
        path: The intended target.
        text: The exact content to write, UTF-8 encoded.
        newline: Passed to the text wrapper. ``"\\n"`` writes the string's
            newlines verbatim.
        boundary: The declared owner root, as :func:`assert_safe_output_path`
            documents.

    Returns:
        The :class:`StagedWrite` describing the staged file.

    Raises:
        UnsafeOutputPath: Where the target is refused.
        OSError: Where the staged file cannot be created or written.
    """
    return _stage(
        Path(os.path.abspath(os.path.expanduser(str(path)))),
        newline=newline,
        boundary=boundary,
        serialise=lambda handle: handle.write(text),
    )


def promote_staged(staged: Sequence[StagedWrite]) -> list[dict[str, Any]]:
    """Move a whole staged set into place, restoring the previous set on failure.

    Each existing target is moved aside to a backup first, then the staged file is
    renamed in; both are ``os.replace``, atomic within a directory. A failure at
    any point in the sequence — a rename that cannot be performed, a target that
    became unwritable — restores every backup and removes every target promoted
    so far, so the set that was already published is the set that remains. That is
    the property a sequential file-by-file publication cannot offer: this run's
    ``findings.json`` beside the previous run's ``findings.csv`` is not a partial
    result but a wrong one (CWE-703).

    Args:
        staged: The staged writes to promote, in the order they are to appear.

    Returns:
        One record per promoted file, for the caller's own log.

    Raises:
        OSError: Where any rename fails. Every backup has been restored and every
            file promoted in this call removed before it propagates.
        EmitError: Where the rollback itself could not complete, naming the exact
            backup files left on disk. That is strictly worse than a failed
            promotion and is reported as its own condition rather than folded into
            the original error, because it is the one case where a reader has to
            act on the filesystem by hand.
    """
    backups: list[tuple[Path, Path]] = []
    promoted: list[Path] = []
    try:
        for entry in staged:
            if os.path.lexists(entry.target):
                backup = entry.target.parent / (
                    f".{entry.target.name}."
                    f"{secrets.token_hex(_STAGED_TOKEN_BYTES)}.previous"
                )
                os.replace(entry.target, backup)
                backups.append((entry.target, backup))
            os.replace(entry.temporary, entry.target)
            promoted.append(entry.target)
    except BaseException as error:
        backed_up = dict(backups)
        unrestored: list[str] = []
        for target in reversed(promoted):
            if target not in backed_up:
                # Nothing was published here before this call, so the honest
                # rollback is for nothing to be published here now.
                try:
                    os.unlink(target)
                except OSError:
                    unrestored.append(
                        f"{target} was promoted by this call and could not be removed"
                    )
        for target, backup in reversed(backups):
            try:
                os.replace(backup, target)
            except OSError:
                unrestored.append(
                    f"{target} is missing and its previous version is at {backup}"
                )
        if unrestored:
            raise EmitError(
                "promotion failed and the previously published set could not be fully "
                f"restored ({type(error).__name__}: {error}); by hand: "
                + "; ".join(unrestored)
            ) from error
        raise
    for _, backup in backups:
        try:
            backup.unlink(missing_ok=True)
        except OSError:  # pragma: no cover - the set is published either way
            pass
    return [entry.as_dict() for entry in staged]


def discard_staged(staged: Iterable[StagedWrite]) -> None:
    """Remove staged files that will not be promoted, leaving the targets alone.

    Called on every path that decides not to publish — a serialisation failure on
    the second file of a pair, a comparison that disagreed — so a staged file
    never survives as litter beside a deliverable, and never as a file a later
    reader could mistake for output.
    """
    for entry in staged:
        try:
            entry.temporary.unlink(missing_ok=True)
        except OSError:  # pragma: no cover - the target is untouched either way
            pass
