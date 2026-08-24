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
for absence.

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
optional field to ``None``) and the two are compared in order, row by row and field
by field, reporting the FIRST mismatch with its row index and field name. Nothing
here counts lines, in code or in any message: the historical dataset carried 10,178
parsed rows over 12,762 physical lines because ``message`` fields carry embedded
newlines, so a line count over-reports by about a quarter. The comparison is
returned as data for ``cli.py`` to serialise into
``harness/artifacts/logs/normalize-run.json``; this module prints nothing.

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
    EmitError                                  every fault raised here
    Mismatch, ComparisonResult                 the comparison, as data
    MISMATCH_FIELD_VALUE,                      the two mismatch kinds, by name
    MISMATCH_ROW_SEQUENCE_LENGTH
    validate_rows(rows)                        -> ordered, checked rows
    write_findings_json(rows, path)            the JSON writer
    write_findings_csv(rows, path)             the CSV writer
    write_findings(rows, json_path, csv_path)  both, from one row list
    read_findings_json(path)                   -> parsed rows
    read_findings_csv(path)                    -> typed-coerced rows
    compare_outputs(json_path, csv_path)       -> ComparisonResult
    emit_findings(rows, json_path, csv_path)   write both, then compare
"""

import csv
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

__all__ = [
    "CSV_ABSENT",
    "CSV_FALSE",
    "CSV_TRUE",
    "ComparisonResult",
    "EmitError",
    "FIELDS",
    "MISMATCH_FIELD_VALUE",
    "MISMATCH_ROW_SEQUENCE_LENGTH",
    "Mismatch",
    "OPTIONAL_FIELDS",
    "REQUIRED_FIELDS",
    "compare_outputs",
    "emit_findings",
    "read_findings_csv",
    "read_findings_json",
    "validate_rows",
    "write_findings",
    "write_findings_csv",
    "write_findings_json",
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

# `int()` accepts more than a faithful round trip should: int("3_0") == 30,
# int(" 4") == 4 and int("+5") == 5 all succeed. A CSV cell that was not written
# by this module's writer must therefore fail rather than be silently coerced into
# agreement, so the integer cell is matched exactly before it is converted.
_START_LINE_CELL_RE = re.compile(r"\A(?:0|[1-9][0-9]*)\Z")

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
        if value < 0:
            raise EmitError(f"{where}: start_line must not be negative; observed {value!r}")
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



# --------------------------------------------------------------------------- #
# The two writers — one row list in, two files out
# --------------------------------------------------------------------------- #


def _csv_cell(field: str, value: Any) -> str:
    """Render one validated value as its CSV cell under the shared absence convention."""
    if value is None:
        # Absence is an empty field, matching JSON null (AAP 0.5.4).
        return CSV_ABSENT
    if field == _BOOLEAN_FIELD:
        # Written by name so `str(True)` can never put `True` in the column.
        return CSV_TRUE if value else CSV_FALSE
    if field == _INTEGER_FIELD:
        return str(value)
    return value


def _prepare_target(path: Path) -> Path:
    """Create the output directory if needed and return the partial-write sibling.

    Each file is written to a sibling and then moved into place with
    :meth:`Path.replace`, which is atomic on the same filesystem. A deliverable is
    therefore either the previous file or this run's complete file, never a
    truncated one — which matters because run-record.md indexes every number to a
    file that exists.
    """
    parent = path.parent
    if str(parent) not in ("", "."):
        parent.mkdir(parents=True, exist_ok=True)
    return path.with_name(path.name + ".partial")


def _write_json_rows(rows: Sequence[Mapping[str, Any]], path: Path) -> None:
    """Write the validated rows as a top-level JSON array."""
    partial = _prepare_target(path)
    try:
        with partial.open("w", encoding="utf-8", newline="\n") as handle:
            # A bare array: row-only, no metadata envelope (AAP 0.5.4). indent=2
            # keeps the deliverable auditable by a human; ensure_ascii=False keeps
            # non-ASCII characters as UTF-8 rather than \u escapes; allow_nan=False
            # refuses any non-standard JSON literal.
            json.dump(list(rows), handle, ensure_ascii=False, indent=2, allow_nan=False)
            handle.write("\n")
        partial.replace(path)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise


def _write_csv_rows(rows: Sequence[Mapping[str, Any]], path: Path) -> None:
    """Write the validated rows as a header row plus one row per finding."""
    partial = _prepare_target(path)
    try:
        # newline="" is required by the csv module so a message carrying an
        # embedded newline is written inside its quoted field verbatim rather than
        # being translated on the way out.
        with partial.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator=_CSV_LINE_TERMINATOR)
            writer.writerow(FIELDS)
            for row in rows:
                writer.writerow([_csv_cell(field, row[field]) for field in FIELDS])
        partial.replace(path)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise


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
        EmitError: Where any row is invalid. Nothing is written in that case.
        OSError: Where the file cannot be written.
    """
    validated = validate_rows(rows)
    _write_json_rows(validated, Path(path))
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
        EmitError: Where any row is invalid. Nothing is written in that case.
        OSError: Where the file cannot be written.
    """
    validated = validate_rows(rows)
    _write_csv_rows(validated, Path(path))
    return validated


def write_findings(
    rows: Iterable[Mapping[str, Any]],
    json_path: str | Path,
    csv_path: str | Path,
) -> list[dict[str, Any]]:
    """Write both files from one validated in-memory row list.

    AAP 0.6.2: both files are written from the same validated rows, "then
    re-parsed with typed coercion and compared; neither is derived from the other
    after writing". This function is that first half — the rows are validated once
    and handed to both writers, so the CSV is never generated from the JSON file
    or the JSON from the CSV.

    Args:
        rows: The rows to write, in the order they are to appear in both files.
        json_path: Where ``findings.json`` is written.
        csv_path: Where ``findings.csv`` is written.

    Returns:
        The validated rows that were written to both files.

    Raises:
        EmitError: Where any row is invalid. Neither file is written in that case.
        OSError: Where either file cannot be written.
    """
    validated = validate_rows(rows)
    _write_json_rows(validated, Path(json_path))
    _write_csv_rows(validated, Path(csv_path))
    return validated



# --------------------------------------------------------------------------- #
# The readers — both files parsed back from disk, the CSV typed on the way in
# --------------------------------------------------------------------------- #


def read_findings_json(path: str | Path) -> list[dict[str, Any]]:
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
    document = json.loads(target.read_text(encoding="utf-8"))
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
                f"{where}: start_line must be an empty field or a plain non-negative "
                f"integer; observed {cell!r}"
            )
        return int(cell)

    if field in _OPTIONAL_TEXT_FIELDS:
        # An empty field is absence, matching JSON null (AAP 0.5.4).
        return None if cell == CSV_ABSENT else cell

    # A required text field: an empty cell is a failure, since absence is not
    # permitted for it and the CSV has no other way to spell one.
    if cell == CSV_ABSENT:
        raise EmitError(
            f"{where}: this field is never absent, so an empty CSV field is a fault"
            + (
                " — path and severity_norm are the dataset's two mandatory fields"
                if field in ("path", "severity_norm")
                else ""
            )
        )
    return cell


def read_findings_csv(path: str | Path) -> list[dict[str, Any]]:
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
    with target.open("r", encoding="utf-8", newline="") as handle:
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


def compare_outputs(json_path: str | Path, csv_path: str | Path) -> ComparisonResult:
    """Re-parse both written files and assert they agree field for field.

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
    json_rows = read_findings_json(json_path)
    csv_rows = read_findings_csv(csv_path)
    return _compare_rows(json_rows, csv_rows, str(json_path), str(csv_path))


def emit_findings(
    rows: Iterable[Mapping[str, Any]],
    json_path: str | Path,
    csv_path: str | Path,
) -> ComparisonResult:
    """Write both output files from one row list, then prove they agree.

    The whole module in one call, in the order AAP 0.5.4 fixes: validate the rows,
    write both files from those same rows, read both back from disk, coerce the
    CSV, and compare in order field by field.

    Args:
        rows: The dataset rows, in the order they are to appear in both files.
            The order is preserved exactly; nothing is sorted, grouped or
            deduplicated (AAP 0.3.2).
        json_path: Where ``findings.json`` is written.
        csv_path: Where ``findings.csv`` is written.

    Returns:
        The :class:`ComparisonResult` for ``cli.py`` to serialise into
        ``harness/artifacts/logs/normalize-run.json``.

    Raises:
        EmitError: Where a row is invalid, or where either written file cannot be
            parsed back as this schema.
        OSError: Where either file cannot be written or read.
    """
    write_findings(rows, json_path, csv_path)
    return compare_outputs(json_path, csv_path)

