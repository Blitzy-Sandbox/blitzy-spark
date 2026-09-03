"""harness/lib/normalize/adapters/gitleaks.py — Gitleaks 8.30.1's native adapter.

Serves exactly one tool, ``gitleaks``, whose runner writes a **bare top-level JSON
array** rather than SARIF.  AAP 0.6.1's row *"One adapter per non-SARIF artifact
written"* is what puts this module here, and AAP 0.5.4's per-shape table is what it
implements: ``rule_id`` from ``RuleID``, ``message`` from ``Description``,
``severity_native`` **absent** because the tool defines no severity vocabulary,
``path`` from ``File`` and ``start_line`` from ``StartLine``.

No user-specified rule governs this file; enterprise-standard best practice applies
in its place (AAP 0.7, AAP 0.10.2), held to the AAP's own bar: verification
independent of the thing verified, reject rather than infer, and a policy fixed
before any output is observed.  Everything cited below is an AAP *requirement*; none
of it is a rule, and none is invented here.

Position in the normalizer
--------------------------
A leaf that depends on exactly two modules.  AAP 0.6.4: *"each adapter depends on
``paths`` and ``severity`` and on nothing else."*  Taken literally --
``normalize.shape``, ``normalize.cli``, ``normalize.emit``, ``normalize.reconcile``
and every sibling adapter are **not** imported, and neither is any third-party
package (AAP 0.4.1: standard library only, so this run introduces no manifest, no
lockfile and no install step, which AAP 0.4.3 forbids).

Two consequences are structural rather than stylistic, and they are the same two the
shared SARIF adapter carries:

* ``reconcile`` is unreachable from here, so the counting traversal forming the
  left-hand side of ``raw finding records = dataset rows + rejected records`` cannot
  reuse a line of row-building code.  That is the point: a count taken from the
  traversal that builds the rows satisfies the identity while testing nothing.
* ``emit.FIELDS`` and ``shape.SCANNER_CLASS_BY_TOOL`` cannot be imported, so
  :data:`FIELDS` and :data:`SCANNER_CLASS` below are authored copies that must agree
  with them **by construction**.  ``shape.py`` keeps the same separation from the
  other direction, naming this adapter by the string key ``"gitleaks"`` rather than
  importing it.

There is no ``__init__.py`` under ``harness/lib/normalize/`` or in this directory, by
design: the package is a PEP 420 implicit namespace package on the pinned CPython
3.13.7, resolved once ``harness/lib`` is on ``sys.path``.  Imports are therefore
absolute and rooted at the package (``from normalize import paths``), never a bare
sibling import.

Nothing here reads a file, an environment variable or a global, and nothing happens
at import time beyond defining constants.  The document, the root, the runner
metadata, the allowlist and the tally all arrive as arguments, which is what makes
:func:`adapt` callable on an already-parsed fixture with no live filesystem.  This
module writes no file: writing belongs to ``emit.py`` and ``cli.py``.  ``os`` is
imported for :func:`os.fspath` alone -- named directly so that no environment access
is even in scope.

The count unit, and the invariant that rests on it
--------------------------------------------------
The count unit is **the top-level array: one element is one record** (AAP 0.5.4),
which is exactly what ``reconcile.count_records`` walks for this tool --
``_count_gitleaks`` is the length of the array and nothing else.  Every element
therefore yields **exactly one outcome -- one row or one rejection, never both and
never neither**.  :func:`_adapt_record` returns a single value of one of those two
types, so the invariant is structural rather than asserted.

An **empty array contributes nothing and is not an error**: Gitleaks legitimately
finds nothing, and eighteen per-directory reports merged into one empty array is the
ordinary shape of a clean scan.  Nor is a runner's exit status ever consulted here.
AAP 0.5.4: *"Non-zero exit with findings is ordinary -- Gitleaks and Checkov both
exit non-zero when they find something."*  This provisioning's Gitleaks exits ``2``
precisely because it found one leak, and that artifact parses.  Array order is
preserved in the returned rows, since both output files use it and ``emit.py``
compares them ordered row by row.

A **non-list document raises rather than rejecting**, which is the one place this
module's treatment of a shape fault differs from a record fault.  The reason is
arithmetic rather than taste: ``reconcile`` counts zero records in a document that is
not an array, so emitting a rejection for one would make ``rows + rejections`` exceed
the record count and break the identity that rejection accounting exists to protect.
AAP 0.5.4 makes an artifact matching no known shape *"a halt rather than a
best-effort parse"*, and ``shape.py`` only refuses a scalar or ``null`` top level
(its ``_is_supported_container`` admits any object or array), so this guard is the
authoritative one for a ``gitleaks.json`` that is an object.

``scanner_class`` is fixed, and it never varies
-----------------------------------------------
AAP 0.5.4's class table fixes ``secret`` for ``gitleaks``.  Trivy is the single tool
whose class is per record; this one is not, so :data:`SCANNER_CLASS` is a constant
and no code path consults record content to decide it.

No severity vocabulary at all -- stated, never assumed
------------------------------------------------------
AAP 0.5.4's gitleaks row reads *"``severity_native`` absent, the tool defining no
severity vocabulary"*, and its native-severity policy table puts this tool under
**No vocabulary at all**: *"``severity_native`` absent, ``severity_norm`` ``Info``,
the absence stated rather than a level assumed."*  The measured 8.30.1 record carries
no severity-like field of any kind -- its eighteen keys are ``RuleID``,
``Description``, ``StartLine``, ``EndLine``, ``StartColumn``, ``EndColumn``,
``Match``, ``Secret``, ``File``, ``SymlinkFile``, ``Commit``, ``Entropy``,
``Author``, ``Email``, ``Date``, ``Message``, ``Tags`` and ``Fingerprint``.

So every row goes through ``severity.SeverityResult.absent()``, whose
``__post_init__`` refuses a ``severity_native`` on that basis and therefore makes the
absence structural.  ``Info`` is **not** hard-coded here, and no native label is
synthesised: inventing one would put a literal into
``oss-scan-results/severity-map.md`` that the tool never emitted.  The tally is fed
once per emitted row all the same, so that document can name this tool explicitly as
one that defines no severity vocabulary (AAP 0.6.2 requires such tools *"named as
such"*).

And if a future artifact ever does carry a severity-like field, **this module does
not start mapping it**.  The policy is fixed before output is observed (AAP 0.1.3);
the observed literal would reach ``severity-map.md`` through the tally, and the
mapping would stay as specified until the policy itself changed.

``Description`` is the rule description -- ``Secret`` and ``Match`` are not
------------------------------------------------------------------------
The single easiest field in this artifact to get wrong.  ``Description`` is the
**rule's** description (``"Identified a Private Key, which may compromise
cryptographic security..."``); ``Message`` is a *git commit message*, not a
description at all; and ``Secret`` and ``Match`` are the captured value.  ``message``
comes from ``Description`` and from nothing else.  An absent or empty ``Description``
is the ``missing_message`` rejection -- there is deliberately no fallback, because
every candidate fallback would either infer content the tool did not state or leak a
captured value into the dataset.

The path base comes from the recorded invocation, and it is genuinely non-obvious
---------------------------------------------------------------------------------
``gitleaks dir`` takes **exactly one path** and reports relative to the **process
working directory** when handed more, so the base is a property of the *invocation*
rather than of the tool (AAP 0.5.4: *"one path per invocation makes paths relative to
that directory; several paths in one makes them relative to the recorded working
directory"*).  Both shapes are real: AAP 0.2.3 records a historical harness whose
runner *"passes all eighteen scope directories to a single invocation"* -- *"exactly
the shape that makes the base non-obvious"* -- while this provisioning's runner cds
to the scan root and hands over one root-relative directory per invocation, eighteen
times, so ``runner-metadata.json`` records ``path_base.kind`` ``scan_root``.

Not one base is therefore resolved here.  The raw ``File`` value, the root and the
per-tool :class:`~normalize.paths.ToolPathBase` are handed to
:func:`normalize.paths.resolve_gitleaks_path`, and whatever it returns is used.  A
reader tempted to "simplify" this into a fixed base would be hard-coding one of the
two invocation shapes and silently mis-pathing every row under the other.

**The runner is never repaired.**  AAP 0.3.2: *"No runner reconfiguration and no
runner edit. ... A runner whose reach or path base differs from expectation is a
condition to record, not a defect to repair"*, and AAP 0.2.3 says the same of this
exact case: *"It is not repaired: no runner is edited."*  The invocation, the working
directory and the resulting base are recorded in ``runner-metadata.json``, which is
what ``tool_base`` exposes; this adapter's whole job is to resolve correctly against
whatever was recorded.

The redaction invariant: no secret value in any field, ever
----------------------------------------------------------
AAP 0.5.4: *"Gitleaks runs with redaction so a matched secret's value never enters an
artifact, and no adapter carries a secret value into any field."*  The provisioned
runner passes ``--redact=100``, so ``Secret`` and ``Match`` arrive as the literal
``REDACTED`` -- and this adapter still never reads them, because the invariant must
not depend on a flag in a file it is forbidden to edit.

The invariant is held **by construction**, not by a filter over a wholesale copy:

* a row is built from exactly the four keys in :data:`SOURCE_FIELDS` --
  ``RuleID``, ``Description``, ``File``, ``StartLine`` -- named one at a time.  The
  record is never copied, spread or iterated into a row;
* every key in :data:`NEVER_READ_FIELDS` is unreachable from this module, ``Secret``,
  ``Match``, ``Entropy`` and ``Fingerprint`` among them.  ``Fingerprint`` is excluded
  even though the measured value is ``<file>:<rule>:<line>``: in commit mode it also
  carries a commit identity, and a field whose safety depends on which mode ran is
  not a field to read;
* a **rejection detail can only ever carry a value whose type makes it safe**.
  :func:`_safe_value_repr` renders ``None``, a ``bool``, an ``int`` and a ``float``
  and otherwise names the type alone, so no string from the record -- from any key,
  expected or not -- can reach a rejection reason.  AAP 0.5.4 requires the parser
  reason retained verbatim; this keeps that channel from becoming a leak channel;
* ``SymlinkFile`` is the one key outside :data:`SOURCE_FIELDS` this module looks at,
  and it is looked at through :func:`_names_more_than_one_location`, which returns a
  **bool and nothing else**.  It is a filesystem path rather than a captured value,
  it is never emitted, and it is never used to fill in an absent ``File``.

``oss-scan-results/adapter-tests/test_gitleaks_adapter.py`` asserts exactly this
(*"that no secret value reaches any field"*) over a fixture whose ``Secret`` and
``Match`` are distinctive sentinels: no emitted row field and no rejection reason may
contain either.

What this module does not do
----------------------------
AAP 0.3.2, in full force.  It performs no cross-tool interpretation of any kind: one
row per finding with the producing tool named, and two tools reporting the same
location produce two rows and no comment.  It judges nothing -- not real, not
important, not a false positive, not a duplicate.  It deduplicates nothing, not even
two identical elements of one array: those are two records and two rows.  It filters
nothing; every element is emitted or rejected, and a row outside the allowlist is
kept with ``in_scope: false`` and counted (AAP 0.9.3).

It also has no path-discovery logic of its own, deliberately: it reads only the
document handed to it, and ``cli.py`` supplies artifacts from
``harness/artifacts/raw/`` only.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from os import fspath
from typing import Any, Final

from normalize import paths
from normalize import severity

__all__ = [
    "ABSENCE_PERMITTED_FIELDS",
    "COUNTER_KEYS",
    "FIELDS",
    "NEVER_READ_FIELDS",
    "REJECT_CLASSES_PRODUCED",
    "SCANNER_CLASS",
    "SOURCE_FIELDS",
    "TOOL",
    "GitleaksAdapterError",
    "adapt",
    "new_counters",
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class GitleaksAdapterError(ValueError):
    """Raised where a *caller* hands this adapter something its contract forbids.

    Deliberately distinct from a rejection.  A rejection describes a defective
    *record* inside an artifact and is counted and carried on from; this exception
    describes a defective *call* -- the wrong tool identifier, a relative root, a
    path base belonging to another tool, a document that is not a JSON array -- and
    stops the caller rather than being absorbed into a rejection count.

    A ``ValueError`` subclass rather than a bare ``assert``: ``python -O`` strips
    ``assert``, and an invariant that disappears under optimisation is not an
    invariant.  AAP 0.5.4's "reject rather than infer" governs record content; a
    caller fault is neither rejected nor inferred, it is raised.
    """


# --------------------------------------------------------------------------- #
# Fixed policy: the tool served, the scanner class, the twelve fields
# --------------------------------------------------------------------------- #

#: The canonical tool identifier every row from this adapter carries.
#:
#: Canonical identifiers are produced mechanically from the stem of the runner and
#: its artifact (AAP 0.5.4) -- ``gitleaks``, never "Gitleaks" and never a product
#: name.  Unlike the shared SARIF adapter, this module serves exactly one tool, so
#: the ``tool`` argument to :func:`adapt` is validated against this constant rather
#: than against a set.
TOOL: Final[str] = "gitleaks"

#: The ``scanner_class`` every row from this adapter carries.
#:
#: AAP 0.5.4's class table fixes ``secret`` for ``gitleaks``, and Trivy is the single
#: tool whose class varies per record.  Authored here rather than imported from
#: ``shape.py`` because AAP 0.6.4 permits an adapter to import ``paths`` and
#: ``severity`` and nothing else; the duplication is required by that constraint, not
#: an oversight, and it is fixed in advance rather than derived from what the
#: artifact turns out to contain.
SCANNER_CLASS: Final[str] = "secret"

#: The twelve fields, in the request's order (AAP 0.8.2).
#:
#: ``emit.py`` owns ``FIELDS`` as the single authored constant everything downstream
#: keys on, and cannot be imported from here, so this copy must agree with it by
#: construction.  Every row carries all twelve keys in this order, present-with-
#: ``None`` rather than omitted, so the CSV column set is uniform.
FIELDS: Final[tuple[str, ...]] = (
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

#: Absence is permitted for exactly these five fields and no others (AAP 0.8.2).
#:
#: ``path`` is not among them: AAP 0.5.4 states *"``path`` is not an optional
#: field"*, so a record whose path cannot be resolved is rejected and counted rather
#: than emitted with a null path.  ``severity_norm`` is likewise never absent, which
#: ``severity.py`` enforces on every construction of its result.
ABSENCE_PERMITTED_FIELDS: Final[frozenset[str]] = frozenset(
    {"severity_native", "start_line", "cwe", "cve", "package_coordinate"}
)

#: ``cwe``, ``cve`` and ``package_coordinate`` are always ``None`` for this shape.
#:
#: A Gitleaks finding reports neither a weakness identifier nor a package: AAP
#: 0.5.4's gitleaks row names ``rule_id``, ``message``, ``path`` and ``start_line``
#: and no other source.  Deriving a CWE from a rule id, or a coordinate from a file
#: path, would be inference.  Note the consequence AAP 0.5.4 states for the
#: package-coordinate precedence: a secret record is **not** dependency-oriented, so
#: an absent coordinate here is not a rejection condition.
_CWE: Final[None] = None
_CVE: Final[None] = None
_PACKAGE_COORDINATE: Final[None] = None


# --------------------------------------------------------------------------- #
# Gitleaks record keys.
#
# Every key this module names, and -- just as load-bearing -- every key it
# refuses to name.  Both sets are exported so the adapter test can assert the
# redaction invariant against the module's own declaration rather than against a
# list a reader retyped.
# --------------------------------------------------------------------------- #

#: An absent, null or blank value earns the ``missing_rule_id`` rejection and
#: nothing is substituted for it -- not the rule ``Description``, and not the rule
#: name that ``Fingerprint`` happens to carry as its middle segment.  A value that
#: is present but not a string is ``malformed_record`` instead
#: (:func:`_text_reject_class`), because something was very much there.
_RULE_ID_KEY: Final[str] = "RuleID"

#: The **rule's** description.  Source of ``message`` -- see the module docstring
#: on why no other key is ever a fallback for it.
_DESCRIPTION_KEY: Final[str] = "Description"

#: The path.  Source of ``path``, resolved by ``paths.py`` against the recorded
#: base.  The metadata may name a different field, in which case
#: :func:`normalize.paths.resolve_gitleaks_path` honours ``record_path_field``;
#: this constant is the default that function itself falls back to, restated here
#: only for the record-identity fields on a rejection.
_FILE_KEY: Final[str] = "File"

#: Gitleaks numbers lines from one, so a value below ``1`` is not a line at all.  A
#: value that is present but is not a positive integer -- a numeric string among
#: them -- is the ``non_integer_start_line`` rejection rather than a coerced value,
#: while an absent or null one is the permitted absence (:func:`_start_line`).
_START_LINE_KEY: Final[str] = "StartLine"

#: The symlink counterpart of ``File``, read **only** through
#: :func:`_names_more_than_one_location` and never emitted.  See that function for
#: why a non-empty value makes the record name more than one location.
_SYMLINK_FILE_KEY: Final[str] = "SymlinkFile"

#: The four keys a row is built from, in row order.  A row is assembled by naming
#: these one at a time; the record is never copied or spread, which is what makes
#: the redaction invariant structural (module docstring, AAP 0.5.4).
SOURCE_FIELDS: Final[tuple[str, ...]] = (
    _RULE_ID_KEY,
    _DESCRIPTION_KEY,
    _FILE_KEY,
    _START_LINE_KEY,
)

#: Keys this module never reads, with the reason each is here.
#:
#: ``Secret`` and ``Match`` are the captured value itself.  ``Entropy`` is a
#: measure derived from it.  ``Fingerprint`` is excluded even though the measured
#: 8.30.1 value is ``<file>:<rule>:<line>``, because in commit mode it also carries
#: a commit identity and a field whose safety depends on which mode ran is not a
#: field to read.  ``Message`` is a git **commit** message and is emphatically not
#: the rule description.  ``Commit``, ``Author``, ``Email`` and ``Date`` are commit
#: provenance the twelve-field schema has no field for, and ``Tags``,
#: ``StartColumn``, ``EndColumn`` and ``EndLine`` are finding detail the schema
#: likewise does not carry -- ``start_line`` is the only positional field, and
#: AAP 0.5.4 gives it exactly one source.
#:
#: Together with :data:`SOURCE_FIELDS` and ``SymlinkFile`` this accounts for every
#: one of the eighteen keys the pinned Gitleaks emits.  The two sets are disjoint,
#: which the adapter test asserts.
NEVER_READ_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "Secret",
        "Match",
        "Entropy",
        "Fingerprint",
        "Message",
        "Commit",
        "Author",
        "Email",
        "Date",
        "Tags",
        "StartColumn",
        "EndColumn",
        "EndLine",
    }
)

#: The rejection classes this adapter can produce, as AAP 0.6.2 requires them to be
#: enumerable: every one needs a negative fixture *"whether or not this run's
#: artifacts contained that case"*, so the set is declared rather than discovered by
#: reading the code.
#:
#: The first four are raised by this module's own checks; ``absent_path`` and
#: ``unresolvable_path`` come back from ``paths.py`` and are returned unchanged, so
#: that the sub-reason it wrote survives.  Every member is a named element of the
#: closed :data:`normalize.paths.REJECT_CLASSES` vocabulary.
REJECT_CLASSES_PRODUCED: Final[tuple[str, ...]] = (
    paths.REJECT_MALFORMED_RECORD,
    paths.REJECT_MISSING_RULE_ID,
    paths.REJECT_MISSING_MESSAGE,
    paths.REJECT_NON_INTEGER_START_LINE,
    paths.REJECT_ABSENT_PATH,
    paths.REJECT_UNRESOLVABLE_PATH,
)


# --------------------------------------------------------------------------- #
# The counter key set.  Fixed and fully pre-initialised, so every call returns
# the same keys and a caller aggregating across artifacts never has to guess
# whether a missing key means zero or means "this adapter forgot".  The key
# names match the shared SARIF adapter's wherever they measure the same thing,
# which is what lets cli.py aggregate the two without a per-adapter special
# case.
# --------------------------------------------------------------------------- #

#: Records naming more than one filesystem location.  The row takes the first --
#: the field the metadata names, ``File`` -- the record still counts once, and this
#: is the number AAP 0.5.4 has reported per tool.  In this shape the second
#: location is ``SymlinkFile``; see :func:`_names_more_than_one_location`.
COUNTER_MULTI_LOCATION: Final[str] = "multi_location_records"

#: Rows whose path names something other than a file in the scanned tree -- an
#: archive member or a location outside the root.  ``run-record.md`` reports the
#: count and the proportion (AAP 0.6.1).  Such a row is kept with ``in_scope:
#: false``, never dropped (AAP 0.9.3).
COUNTER_NON_FILESYSTEM_PATHS: Final[str] = "non_filesystem_paths"

#: The ``in_scope`` decomposition of the emitted rows.  Their sum is the row count,
#: so this is one measurement split rather than a second count of the same thing.
COUNTER_ROWS_IN_SCOPE: Final[str] = "rows_in_scope"
COUNTER_ROWS_OUT_OF_SCOPE: Final[str] = "rows_out_of_scope"

#: Rows carrying no ``start_line``.  Absence is permitted for that field, so this is
#: the only way the number is visible.
COUNTER_START_LINE_ABSENT: Final[str] = "start_line_absent"

#: Rows whose ``severity_native`` is absent.  For this tool that is **every** row,
#: by policy rather than by observation, and the counter is published so
#: ``tool-status.md`` can state the number rather than assert the policy.
COUNTER_SEVERITY_ABSENT: Final[str] = "severity_absent"

#: Prefixes for the two vocabularies that are *derived* rather than authored: one
#: key per :data:`normalize.paths.PATH_KINDS` member and one per
#: :data:`normalize.severity.BASIS_VALUES` member.  Deriving them means this
#: adapter's counter set cannot drift from the vocabularies it reports against.
COUNTER_PATH_KIND_PREFIX: Final[str] = "path_kind_"
COUNTER_SEVERITY_BASIS_PREFIX: Final[str] = "severity_basis_"

_AUTHORED_COUNTER_KEYS: Final[tuple[str, ...]] = (
    COUNTER_MULTI_LOCATION,
    COUNTER_NON_FILESYSTEM_PATHS,
    COUNTER_ROWS_IN_SCOPE,
    COUNTER_ROWS_OUT_OF_SCOPE,
    COUNTER_START_LINE_ABSENT,
    COUNTER_SEVERITY_ABSENT,
)

#: Every key :func:`new_counters` initialises, in a stable order.
#:
#: Note what is deliberately **absent**: there is no adapter-side count of the
#: records walked, and none of the rows or rejections produced.  ``len(rows)`` and
#: ``len(rejections)`` are returned to the caller directly, and a record count taken
#: from *this* traversal would be an attractive nuisance on the left-hand side of
#: ``raw finding records = dataset rows + rejected records`` -- the one place AAP
#: 0.5.4 requires a genuinely independent traversal, which is
#: ``reconcile.count_records``.  Publishing a plausible substitute for it here is how
#: that requirement would quietly be lost.
COUNTER_KEYS: Final[tuple[str, ...]] = (
    *_AUTHORED_COUNTER_KEYS,
    *(f"{COUNTER_PATH_KIND_PREFIX}{kind}" for kind in paths.PATH_KINDS),
    *(f"{COUNTER_SEVERITY_BASIS_PREFIX}{basis}" for basis in severity.BASIS_VALUES),
)


def new_counters() -> dict[str, int]:
    """Return a fresh counter mapping with every key in :data:`COUNTER_KEYS` at zero.

    Exposed so a caller aggregating several artifacts can start from the same key set
    this adapter returns, rather than accumulating into a dict whose missing keys are
    ambiguous between "zero" and "not measured".
    """
    return {key: 0 for key in COUNTER_KEYS}


# --------------------------------------------------------------------------- #
# JSON shape helpers.
#
# These mirror ``reconcile.py``'s reading of the same document: a str, bytes or
# bytearray is never a JSON array, because ``len()`` over a string would count
# characters as findings.
# --------------------------------------------------------------------------- #


def _type_name(value: Any) -> str:
    """Return ``value``'s Python type name, for a message that names a shape.

    Used wherever a diagnostic needs to say *what* was observed without quoting it.
    A type name is safe to print for any value whatsoever, which is exactly why the
    rejection details in this module lean on it (see :func:`_safe_value_repr`).
    """
    return type(value).__name__


def _is_json_array(value: Any) -> bool:
    """Return whether ``value`` is a JSON array (a non-string sequence)."""
    if isinstance(value, (str, bytes, bytearray)):
        return False
    return isinstance(value, Sequence)


def _json_object(value: Any) -> Mapping[str, Any] | None:
    """Return ``value`` where it is a JSON object, else ``None``.

    ``Mapping`` rather than ``dict`` so that a read-only view -- a
    ``MappingProxyType`` from a fixture loader, say -- is as acceptable as a plain
    parsed dict.
    """
    return value if isinstance(value, Mapping) else None


def _safe_value_repr(value: Any) -> str:
    """Render ``value`` for a rejection detail, or name its type where that is unsafe.

    The redaction invariant's last mile (module docstring; AAP 0.5.4).  AAP 0.5.4
    requires a parser reason *retained verbatim*, and a verbatim reason that
    interpolated a record's own text would turn the rejection channel into a leak
    channel -- so only values whose **type** makes them safe are rendered:

    * ``None``, a ``bool``, an ``int`` and a ``float`` are rendered in full.  None of
      them can carry a captured secret, and the actual value is what makes a
      ``non_integer_start_line`` detail actionable;
    * everything else -- a ``str`` above all, but equally a list, a dict or an
      arbitrary object -- is reduced to ``a <type>``.

    A ``str`` is deliberately in the second group.  A Gitleaks record's strings are
    ordinarily a rule id, a description or a path, but a defective artifact can put
    arbitrary text in any key, and "ordinarily safe" is not the standard this
    invariant is held to.
    """
    if value is None or isinstance(value, (bool, int, float)):
        return repr(value)
    return f"a {_type_name(value)}"


def _text_field(
    record: Mapping[str, Any],
    key: str,
) -> tuple[str | None, str | None]:
    """Return one required text field's value, or the reason it cannot be used.

    Returns ``(value, None)`` on success and ``(None, reason)`` on failure, where the
    reason distinguishes the two ways a text field can fail -- a distinction that
    decides which class the record is counted under:

    * **absent or empty** -- the key is missing, ``null``, or a string that is empty
      or entirely whitespace.  The caller maps this to the field's own "missing"
      class (``missing_rule_id`` or ``missing_message``);
    * **structurally wrong** -- present but not a string at all.  The caller maps
      this to ``malformed_record``, because a list or an object where a string
      belongs is a defect in the record's shape rather than an absence, and reading
      "the first element" of one would be inference.

    The returned value is ``strip()``ped.  Trailing whitespace in a rule id or a
    description carries no information, and stripping it here means an
    all-whitespace value is treated as the absence it is rather than as a non-empty
    string that renders as nothing.
    """
    raw = record.get(key)
    if raw is None:
        return None, f"the record carries no {key}"
    if not isinstance(raw, str):
        return None, f"the record's {key} is {_safe_value_repr(raw)}, not a string"
    value = raw.strip()
    if not value:
        return None, f"the record's {key} is empty"
    return value, None


def _text_reject_class(raw: Any, missing_class: str) -> str:
    """Return the rejection class a failed text field earns: missing, or malformed.

    The companion to :func:`_text_field`'s two failure modes, kept as its own function
    so the decision is stated once and read the same way at both call sites.

    An **absent** value (the key missing, or ``null``) and an **empty** one (a string
    that is empty or all whitespace) are the field's own missing class --
    ``missing_rule_id`` or ``missing_message``, the classes AAP 0.5.4 names for
    exactly this.  A value that is present but is **not a string** is
    ``malformed_record`` instead: a list or an object where a string belongs is a
    defect in the record's shape, and it would be misleading to count it as an
    absence when something was very much there.
    """
    if raw is None or isinstance(raw, str):
        return missing_class
    return paths.REJECT_MALFORMED_RECORD


def _start_line(record: Mapping[str, Any]) -> tuple[int | None, str | None]:
    """Return the ``StartLine`` to emit, or the reason it earns a rejection.

    Absence is permitted for ``start_line`` (AAP 0.8.2), so an absent key and an
    explicit ``null`` both yield ``(None, None)`` and no rejection.

    A value that is present but not usable as a line number is the
    ``non_integer_start_line`` rejection condition (AAP 0.5.4).  Three shapes reach
    it, each named in the detail:

    * a non-integer type.  A numeric **string** is included: ``"12"`` is rejected
      rather than coerced, because coercing it would be inference and because
      ``int()`` accepts more than a faithful round trip should (``int("1_2") == 12``);
    * ``True``/``False``, which Python's numeric tower would otherwise admit as ``1``
      and ``0``.  ``isinstance(True, int)`` is ``True``, so the bool test comes
      first;
    * a value below ``1``.  Gitleaks numbers lines from one, so ``0`` is not a line
      and a negative value is not one either -- ``emit.py`` would refuse a negative
      outright, and emitting ``0`` would assert a line that does not exist.

    The class is the same for all three because :data:`normalize.paths.REJECT_CLASSES`
    is a closed set; the detail carries the sub-reason, exactly as AAP 0.5.4 does for
    the ``uriBaseId`` terminal cases.
    """
    raw = record.get(_START_LINE_KEY)
    if raw is None:
        return None, None
    if isinstance(raw, bool) or not isinstance(raw, int):
        return None, (
            f"{_START_LINE_KEY} is {_safe_value_repr(raw)} rather than an integer"
        )
    if raw < 1:
        return None, (
            f"{_START_LINE_KEY} is {raw}, which is not a line number: gitleaks "
            "numbers lines from one"
        )
    return raw, None


def _names_more_than_one_location(record: Mapping[str, Any]) -> bool:
    """Return whether the record names a second filesystem location besides ``File``.

    A Gitleaks finding carries one ``File`` and, where the walk crossed a symbolic
    link, a non-empty ``SymlinkFile`` naming its counterpart -- two paths for one
    finding.  AAP 0.5.4's first representation decision covers exactly that case:
    *"The row takes the first location; the record still counts once; and the number
    of records carrying more than one is reported per tool."*  The row's location is
    the one the runner metadata names (``File``); this function only decides whether
    the record goes into :data:`COUNTER_MULTI_LOCATION`.

    Two properties are deliberate.  It returns a **bool and nothing else**, so the
    value it inspects cannot escape into a row, a rejection or a counter name.  And
    it is **never** consulted as a fallback for an absent or unresolvable ``File``:
    substituting one path for the other would silently change which location the row
    names, which is inference of the kind AAP 0.5.4 forbids -- such a record is
    rejected instead.

    An empty string, whitespace, ``null`` and an absent key all mean "no second
    location", which is the shape every ordinary finding carries.
    """
    counterpart = record.get(_SYMLINK_FILE_KEY)
    return isinstance(counterpart, str) and bool(counterpart.strip())


# --------------------------------------------------------------------------- #
# Argument validation.
#
# Every one of these raises :class:`GitleaksAdapterError` rather than returning a
# rejection: a bad argument is a caller fault, and absorbing it into a rejection
# count would let a wrong root or a foreign path base produce a plausible dataset
# for a whole tool.  Each is validated once per call, before any record is read,
# so a fault surfaces on the call rather than on the first record.
# --------------------------------------------------------------------------- #


def _validated_tool(tool: Any) -> str:
    """Return ``tool`` where it is this adapter's one canonical identifier, else raise.

    Required rather than defaulted even though one module serves one tool: the
    identifier is stamped into every row's ``tool`` field and fed to the tally, so
    accepting a different one would attribute this artifact's rows to another
    scanner.
    """
    if not isinstance(tool, str):
        raise GitleaksAdapterError(
            f"tool must be the canonical tool identifier {TOOL!r}; observed "
            f"{_type_name(tool)}"
        )
    if tool != TOOL:
        raise GitleaksAdapterError(
            f"{tool!r} is not the tool this adapter serves ({TOOL!r}). One adapter "
            "per non-SARIF artifact: the SARIF producers share adapters/sarif.py and "
            "every other native shape has its own module."
        )
    return tool


def _validated_root(root: Any) -> str:
    """Return the scan root as an absolute POSIX-normalised string, else raise.

    A :class:`pathlib.Path` and a string are both accepted -- ``os.fspath`` is the
    one thing ``os`` is imported for -- and the result is normalised through
    ``paths.py`` so that this module and every resolver agree on the root's spelling.

    A relative root is refused here rather than at the first record: it cannot anchor
    anything, and accepting one would produce a plausible-looking wrong answer for
    every row.
    """
    try:
        candidate = fspath(root)
    except TypeError as error:
        raise GitleaksAdapterError(
            f"root must be a str or an os.PathLike naming the SPARK_SRC root; "
            f"observed {_type_name(root)}"
        ) from error
    if isinstance(candidate, bytes):
        raise GitleaksAdapterError(
            "root must be a text path, not bytes: every path in the dataset is text, "
            "and decoding one here would guess an encoding"
        )
    if not candidate:
        raise GitleaksAdapterError("root must not be empty")
    normalised = paths.normalise_reported_path(candidate)
    if not paths.is_absolute_path(normalised):
        raise GitleaksAdapterError(
            f"root must be an absolute path to express a reported path against; "
            f"observed {candidate!r}"
        )
    return normalised


def _validated_tool_base(tool_base: Any) -> paths.ToolPathBase:
    """Return ``tool_base`` where it is this tool's recorded path base, else raise.

    The identifier check is not ceremony.  ``tool_base`` is the per-tool view over
    ``harness/artifacts/logs/runner-metadata.json``, and this adapter resolves nothing
    for itself, so handing it another tool's view would resolve every path against
    the wrong base while every row still looked well-formed -- the exact failure AAP
    0.5.4 requires *"every base taken from the recorded runner metadata"* to prevent.

    It matters more here than anywhere: the Gitleaks base is a property of the
    invocation rather than of the tool, and the two possible invocation shapes anchor
    on different directories.
    """
    if not isinstance(tool_base, paths.ToolPathBase):
        raise GitleaksAdapterError(
            f"tool_base must be a paths.ToolPathBase built from the runner metadata; "
            f"observed {_type_name(tool_base)}"
        )
    if tool_base.tool != TOOL:
        raise GitleaksAdapterError(
            f"tool_base names {tool_base.tool!r} but the artifact is {TOOL!r}; "
            "resolving one tool's paths against another tool's recorded base would "
            "produce a wrong path for every row of it"
        )
    return tool_base


def _validated_allowlist(allowlist: Any) -> tuple[str, ...]:
    """Return the allowlist globs as a tuple, materialised once, else raise.

    Materialising matters: a generator would be exhausted by the first row and every
    subsequent row would silently take ``in_scope: false``.

    The globs' *content* is not checked against the twelve authoritative ones here.
    ``cli.py`` owns that check -- ``paths.allowlist_matches_authoritative_globs`` --
    and duplicating it would put a second, divergable copy of the scope contract in
    an adapter.  What is checked is that each glob is a non-empty string, since a
    non-string pattern would raise from the matcher on the first row rather than on
    the call.
    """
    if isinstance(allowlist, (str, bytes)):
        raise GitleaksAdapterError(
            "allowlist must be an iterable of glob strings, not a single string: a "
            "string would be iterated character by character"
        )
    if not isinstance(allowlist, Iterable):
        raise GitleaksAdapterError(
            f"allowlist must be an iterable of glob strings from "
            f"paths.load_allowlist(); observed {_type_name(allowlist)}"
        )
    globs = tuple(allowlist)
    for index, glob in enumerate(globs):
        if not isinstance(glob, str) or not glob:
            raise GitleaksAdapterError(
                f"allowlist entry {index} must be a non-empty glob string; observed "
                f"{glob!r}"
            )
    return globs


def _validated_tally(tally: Any) -> Any:
    """Return ``tally`` where it can record a severity result, else raise.

    The capability is checked rather than the class, so a test double is as
    acceptable as a :class:`normalize.severity.LiteralTally`.  ``None`` is not: this
    tool defines no severity vocabulary, and the only way
    ``oss-scan-results/severity-map.md`` can *say* so with a row count behind it is
    the tally (AAP 0.6.2 requires such a tool "named as such").  A silently skipped
    tally would leave that document under-reporting with nothing to show it had.
    """
    recorder = getattr(tally, "record", None)
    if not callable(recorder):
        raise GitleaksAdapterError(
            f"tally must expose a callable record(tool, result) -- normally a "
            f"severity.LiteralTally; observed {_type_name(tally)}"
        )
    return tally


def _validated_document(doc: Any) -> Sequence[Any]:
    """Return ``doc`` where it is the top-level array this adapter walks, else raise.

    A ``gitleaks.json`` is a **bare JSON array**; this provisioning's runner builds it
    by concatenating eighteen per-directory arrays into one.  Anything else is a
    document-level shape fault and is **raised rather than rejected**, for the reason
    the module docstring sets out: ``reconcile.count_records`` counts zero records in
    a document that is not an array, so a rejection here would make
    ``rows + rejections`` exceed the record count and break the identity that
    rejection accounting exists to protect.  AAP 0.5.4 makes an artifact matching no
    known shape a halt rather than a best-effort parse.

    Raising rather than returning zero rows is the same point the mandated
    shape-routing negative test makes from the other direction: an empty result set
    is indistinguishable from a clean scan, so a mis-routed or malformed artifact must
    not be able to look like one.

    An **empty** array is not a fault and is returned as-is: Gitleaks finding nothing
    is an ordinary outcome, not a defect.
    """
    if not _is_json_array(doc):
        raise GitleaksAdapterError(
            f"a gitleaks artifact's top level is a JSON array of findings; observed "
            f"{_type_name(doc)}. An artifact matching no known shape is a halt "
            "rather than a best-effort parse, and a document-level fault is raised "
            "rather than counted as a rejection because the independent record count "
            "for a non-array document is zero."
        )
    return doc


# --------------------------------------------------------------------------- #
# One array element -> exactly one outcome
# --------------------------------------------------------------------------- #


def _adapt_record(
    record: Any,
    *,
    tool: str,
    root: str,
    tool_base: paths.ToolPathBase,
    globs: tuple[str, ...],
    tally: Any,
    record_index: int,
    counters: dict[str, int],
) -> dict[str, Any] | paths.Rejection:
    """Return one row **or** one rejection for one element of the top-level array.

    Exactly one of the two, always.  The single return value is what makes the
    one-to-one property structural: there is no path through this function that emits
    both and none that emits neither, so ``dataset rows + rejected records == the
    records walked`` holds by construction rather than by an assertion that could be
    forgotten -- and the records walked is the same unit
    ``reconcile.count_records`` arrives at independently.

    The classification order is fixed and documented rather than incidental, because
    a record can be defective in more than one way at once and the order decides
    which class it is counted under:

    1. the element is not an object -> ``malformed_record``;
    2. no usable ``RuleID`` -> ``missing_rule_id`` (a non-string ``RuleID`` ->
       ``malformed_record``);
    3. no usable ``Description`` -> ``missing_message`` (a non-string ->
       ``malformed_record``);
    4. the path -> ``absent_path``, ``unresolvable_path`` or ``malformed_record``, as
       ``paths.py`` classifies it;
    5. a ``StartLine`` present that is not a usable line number ->
       ``non_integer_start_line``.

    Severity and ``in_scope`` never reject -- each has a defined value for every
    input -- and ``cwe``, ``cve`` and ``package_coordinate`` are constants, so a
    record reaching step 5 becomes a row.

    Nothing is caught broadly here.  Each lookup and conversion is guarded where it
    happens, so a genuine programming error propagates instead of being converted
    into a rejection count that would satisfy reconciliation while hiding a defect.
    """
    # Only the index and the observed type reach the detail: an element that is a
    # bare string could carry anything, and quoting it would put record text into a
    # preserved reason (see _safe_value_repr).
    record_object = _json_object(record)
    if record_object is None:
        return paths.make_rejection(
            paths.REJECT_MALFORMED_RECORD,
            tool,
            f"the array element is {_safe_value_repr(record)}, not an object, so no "
            "finding can be read from it",
            record_index=record_index,
        )

    rule_id, rule_id_failure = _text_field(record_object, _RULE_ID_KEY)
    if rule_id_failure is not None:
        return paths.make_rejection(
            _text_reject_class(
                record_object.get(_RULE_ID_KEY), paths.REJECT_MISSING_RULE_ID
            ),
            tool,
            f"{rule_id_failure}, so the finding names no rule",
            record_index=record_index,
        )

    # `Description` is the RULE description; `Secret` and `Match` are the captured
    # value and `Message` is a git commit message, so none of the three is ever a
    # fallback for it.  A missing description is a counted rejection rather than a row
    # with a substituted message: falling back would both infer content the tool did
    # not state and risk carrying a secret into the field.
    message, message_failure = _text_field(record_object, _DESCRIPTION_KEY)
    if message_failure is not None:
        return paths.make_rejection(
            _text_reject_class(
                record_object.get(_DESCRIPTION_KEY), paths.REJECT_MISSING_MESSAGE
            ),
            tool,
            f"{message_failure}, and no other field substitutes for it: Secret and "
            "Match are the captured value and Message is a git commit message, not "
            "the rule description",
            record_index=record_index,
            rule_id=rule_id,
        )

    # The multi-location count is a property of the record, so it is taken whatever
    # the record's outcome turns out to be (AAP 0.5.4: the row takes the first
    # location, the record still counts once, and the number is reported per tool).
    if _names_more_than_one_location(record_object):
        counters[COUNTER_MULTI_LOCATION] += 1

    # Every base decision is delegated to paths.py; see this module's docstring for
    # why a fixed base would be wrong, and note that the symlink counterpart is never
    # substituted for an absent or unresolvable File.
    resolved = paths.resolve_gitleaks_path(record_object, root, tool_base, tool=tool)
    if isinstance(resolved, paths.Rejection):
        # Returned as-is: paths.py has already named the class and written the
        # sub-reason -- an absent File, or a base the metadata could not establish.
        # Rewording it here would lose that.
        return resolved

    start_line, start_line_failure = _start_line(record_object)
    if start_line_failure is not None:
        return paths.make_rejection(
            paths.REJECT_NON_INTEGER_START_LINE,
            tool,
            start_line_failure,
            record_index=record_index,
            rule_id=rule_id,
        )
    if start_line is None:
        counters[COUNTER_START_LINE_ABSENT] += 1

    # From here nothing can reject: this record is a row.
    #
    # Severity goes through severity.py's no-vocabulary path, which is the only place
    # `Info` is decided.  SeverityResult.absent() carries severity_native None,
    # severity_norm Info and a basis that *states* the absence, and its __post_init__
    # forbids a native literal on that basis -- so the absence is structural rather
    # than a convention this adapter has to remember.
    severity_result = severity.SeverityResult.absent()
    counters[COUNTER_SEVERITY_ABSENT] += 1
    counters[f"{COUNTER_SEVERITY_BASIS_PREFIX}{severity_result.basis}"] += 1
    # The tally is fed once per emitted row, which is what lets severity-map.md name
    # gitleaks as a tool that defines no severity vocabulary and put a row count
    # behind the statement.  A rejected record contributes no row, so counting one
    # here would report an entry against rows the dataset does not contain.
    tally.record(tool, severity_result)

    counters[f"{COUNTER_PATH_KIND_PREFIX}{resolved.kind}"] += 1
    if resolved.is_non_filesystem_coordinate:
        counters[COUNTER_NON_FILESYSTEM_PATHS] += 1

    # in_scope is decided by the allowlist alone, through paths.py's matcher, on the
    # resolved path and carrying its kind -- so an archive member cannot match a glob
    # on its segments and the literal src/test exclusion is applied once, where it
    # lives.  Nothing is ever filtered on it: a row outside the allowlist is kept
    # with in_scope false and counted (AAP 0.9.3), and only evidence about the
    # *runner* establishes a wrong scan root (AAP 0.8.3).
    in_scope = bool(resolved.in_scope(globs))
    counters[COUNTER_ROWS_IN_SCOPE if in_scope else COUNTER_ROWS_OUT_OF_SCOPE] += 1

    # The row is assembled from four named values -- rule_id, message, resolved.path
    # and start_line, taken from RuleID, Description, File and StartLine -- plus
    # constants.  The record is never copied or spread, which is what makes the
    # redaction invariant hold by construction rather than by a filter.
    row: dict[str, Any] = {
        "tool": tool,
        "scanner_class": SCANNER_CLASS,
        "rule_id": rule_id,
        "message": message,
        "severity_native": severity_result.severity_native,
        "severity_norm": severity_result.severity_norm,
        "path": resolved.path,
        "start_line": start_line,
        "cwe": _CWE,
        "cve": _CVE,
        "package_coordinate": _PACKAGE_COORDINATE,
        "in_scope": in_scope,
    }
    return row


# --------------------------------------------------------------------------- #
# The public entry point
# --------------------------------------------------------------------------- #


def adapt(
    doc: Any,
    *,
    tool: str,
    root: Any,
    tool_base: paths.ToolPathBase,
    allowlist: Iterable[str],
    tally: Any,
) -> tuple[list[dict[str, Any]], list[paths.Rejection], dict[str, int]]:
    """Turn one Gitleaks artifact into dataset rows, rejections and counters.

    This is the uniform adapter entry point: every adapter module in this package
    exposes ``adapt`` with this shape, so ``cli.py``'s registry resolves it with
    ``getattr(module, "adapt")`` and every adapter test calls it directly.

    Args:
        doc: The **already-parsed** artifact document -- a ``list`` for this shape,
            not a mapping. Parsing and routing happen upstream, which is what lets a
            test exercise every behaviour on a fixture with no filesystem.
        tool: The canonical tool identifier, which must be :data:`TOOL`. Required
            rather than defaulted: it is stamped into every row and fed to the tally.
        root: The ``SPARK_SRC`` root, as a :class:`pathlib.Path` or a string. Must be
            absolute.
        tool_base: This tool's :class:`normalize.paths.ToolPathBase`, the per-tool
            view over ``harness/artifacts/logs/runner-metadata.json``. Every base
            decision is taken from it and none is assumed -- for this tool above all,
            since ``gitleaks dir`` reports relative to the process working directory
            when handed more than one path, so the base is a property of the recorded
            invocation.
        allowlist: The twelve authoritative globs, as loaded by
            :func:`normalize.paths.load_allowlist`. Consumed once into a tuple.
        tally: A :class:`normalize.severity.LiteralTally` (or anything exposing
            ``record(tool, result)``), fed once per emitted row so
            ``oss-scan-results/severity-map.md`` can name this tool as one that
            defines no severity vocabulary, with the row count behind it.

    Returns:
        A three-tuple ``(rows, rejections, counters)``:

        * ``rows`` -- a list of dicts, each carrying exactly the twelve fields of
          :data:`FIELDS` in that order, in array order;
        * ``rejections`` -- a list of :class:`normalize.paths.Rejection`, each under a
          named member of :data:`normalize.paths.REJECT_CLASSES` -- one of
          :data:`REJECT_CLASSES_PRODUCED` -- with its sub-reason retained;
        * ``counters`` -- a dict of ints over :data:`COUNTER_KEYS`.

        ``len(rows) + len(rejections)`` equals the number of elements in the
        top-level array, which is the same count unit
        :func:`normalize.reconcile.count_records` arrives at independently. An empty
        array yields two empty lists and a zeroed counter set, which is the ordinary
        shape of a clean scan rather than an error.

    Raises:
        GitleaksAdapterError: If an argument is not what the contract requires -- the
            wrong tool, a relative or non-text root, another tool's path base, a
            non-iterable allowlist, a tally that cannot record, or a document that is
            not a JSON array. A caller fault is raised rather than absorbed into a
            rejection count.
        normalize.severity.SeverityPolicyError: If ``tally`` is a ``LiteralTally``
            and rejects the tool identifier -- which cannot happen for ``gitleaks``,
            and is left to surface rather than be caught.

    A tool's exit code is never consulted: a valid artifact is normalized whatever its
    runner returned, since artifact status and exit status are independent (AAP
    0.5.4). This provisioning's Gitleaks exits ``2`` precisely because it found a
    leak, and that is ordinary rather than a reason to doubt the artifact.
    """
    canonical_tool = _validated_tool(tool)
    root_text = _validated_root(root)
    base = _validated_tool_base(tool_base)
    globs = _validated_allowlist(allowlist)
    recorder = _validated_tally(tally)
    records = _validated_document(doc)

    rows: list[dict[str, Any]] = []
    rejections: list[paths.Rejection] = []
    counters = new_counters()

    for record_index, record in enumerate(records):
        outcome = _adapt_record(
            record,
            tool=canonical_tool,
            root=root_text,
            tool_base=base,
            globs=globs,
            tally=recorder,
            record_index=record_index,
            counters=counters,
        )
        if isinstance(outcome, paths.Rejection):
            rejections.append(outcome)
        else:
            rows.append(outcome)

    return rows, rejections, counters
