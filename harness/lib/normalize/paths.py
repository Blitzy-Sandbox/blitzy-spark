"""harness/lib/normalize/paths.py — every path decision in the dataset, in one module.

Purpose, verbatim from AAP 0.6.1: *"Per-tool base resolution from the recorded runner
metadata, the bounded uriBaseId chain walk, Checkov's leading slash, dual
src/main/src/test bytecode resolution, the non-filesystem coordinate serialization,
and the in_scope matcher with true zero-or-more-directories ** semantics."*

No user-specified rule governs this file (AAP 0.7, AAP 0.10.2), so enterprise-standard
best practice applies in its place, held to the AAP's own bar -- verification
independent of the thing verified, reject rather than infer, and a documented
decision for every case rather than a permissive fallback.  Everything cited below
is an AAP *requirement*, never a rule.

Position in the normalizer
--------------------------
A leaf.  Adapters depend on ``paths`` and ``severity`` and on nothing else
(AAP 0.6.4), so this module imports **nothing** from the ``normalize`` package and
nothing outside the CPython standard library.  There is no ``__init__.py`` under
``harness/lib/normalize/`` by design: the package is a PEP 420 implicit namespace
package, resolved once ``harness/lib`` is on ``sys.path``.

**Nothing here reads a global, an environment variable, or a file at import time.**
Every public function takes the root, the allowlist globs and the runner metadata as
arguments (AAP 0.6.1's testability constraint), so an adapter test can exercise it on
a parsed fixture document with no live filesystem beyond the pinned root.  Loaders
are provided and are never called implicitly.

**This module never filters.**  It returns booleans, resolved paths and rejections.
A row that is out of scope, external, an archive member or from a ``src/test`` tree
is *kept* with ``in_scope: false`` (AAP 0.9.3); dropping a row is not its job.

The ``in_scope`` matcher -- the semantics are the whole point
------------------------------------------------------------
``**`` matches **zero or more** path segments.  ``*``, ``?`` and character classes
match **within a single segment and never cross** ``/``.  The match is **anchored at
both ends** against the whole root-relative path.

Every standard-library candidate was measured on the pinned CPython 3.13.7 and each
fails, which is why :func:`match_glob` is written out explicitly:

===============================  =============================================
candidate                        measured failure
===============================  =============================================
``PurePath.match``               ``**`` matches *exactly one* segment, and a
                                 relative pattern is right-anchored.  It drops
                                 essentially every deep file under all twelve
                                 globs.
``fnmatch.fnmatch``              ``**`` becomes ``.*`` and crosses ``/``, so the
                                 **zero-segment** case
                                 ``sql/connect/src/main/z.scala`` is False; and a
                                 single ``*`` crosses ``/`` too, so
                                 ``core/src/main/*`` wrongly matches
                                 ``core/src/main/a/b.py``.
``PurePosixPath.full_match``     Correct semantics, but 3.13+ only.  Used as an
                                 *independent cross-check* during validation,
                                 never as the implementation.
``glob.glob(recursive=True)``    Correct semantics but only against a real
                                 filesystem, so it cannot test a reported archive
                                 member, an external ``../`` coordinate or a
                                 virtual reference.  Used for expansion evidence
                                 (:func:`expand_scope_directories`) only.
===============================  =============================================

AAP 0.5.4 on why this matters: *"Getting this wrong drops whole modules silently,
and a silently dropped module looks exactly like a module with nothing to report."*

The ``src/test`` exclusion lives here
-------------------------------------
``harness/scope/allowlist.txt`` carries no exclusion directive; the exclusion is
implemented in consumer code, and this is that consumer code.  AAP 0.3.1: *"The
exclusion is literal: a path containing ``src/test`` is out of scope."*  It is a
**literal substring test** on the forward-slash-normalised root-relative path and it
**overrides a positive glob match**.

Measured at the pin (59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d): all **20,361** files
whose path contains ``src/test`` carry it as the exact segment pair ``src`` + ``test``,
so the literal reading and the segment-pair reading agree exactly and the literal
introduces **zero** false exclusions.  :func:`src_test_readings_agree` is the
cross-check that keeps that claim checkable rather than asserted.

**Python test sources are IN scope.**  Zero ``python/pyspark`` paths contain
``src/test``; the pin carries 1,203 files under ``python/pyspark/**`` of which 832 are
test modules (any file under a ``tests/`` directory or named ``test_*``; 806 of them
are ``.py``), and every one yields ``in_scope: true``.

Per-tool path base (AAP 0.5.4), every base taken from the recorded metadata
--------------------------------------------------------------------------
=========================  ======================================================
tool family                resolution
=========================  ======================================================
SARIF producers            ``uri`` plus optional ``uriBaseId``: walk the base
                           through ``run.originalUriBaseIds``, following chained
                           bases, then relativize to the root.
``checkov``                root-relative ``file_path`` **with a leading slash**;
                           anchored on ``repo_file_path`` / ``file_abs_path`` and
                           reconciled against the stripped ``file_path``.
``gitleaks``               base depends on the recorded invocation -- one path per
                           invocation is relative to that directory, several in one
                           is relative to the recorded working directory.
``dependency-check``       filesystem-absolute; relativize to the root.
``trivy``                  per-section target semantics; resolved within the
                           section the record came from.
``joern``                  a bytecode class identifier; resolved uniquely against
                           ``src/main`` **and** ``src/test``, rejecting the
                           ambiguous and the unresolvable.
=========================  ======================================================

The base is never assumed to be the tree.  In this provisioning ``opengrep`` and
``semgrep`` emit ``uriBaseId`` ``%SRCROOT%`` with **no** ``run.originalUriBaseIds``,
so the SARIF 2.1.0 section 3.4.4 procedure cannot complete and the *runner-recorded*
base is the only one available; ``datadog-static-analyzer`` emits no ``uriBaseId`` at
all; and ``checkov`` is handed 18 ``-d`` roots in one invocation, so its ``file_path``
is relative to whichever of them the record came from.  Three producers, three
different bases -- hence :class:`ToolPathBase` and the closed ``kind`` vocabulary in
:data:`PATH_BASE_KINDS`, whose ``none`` member carries the metadata's own
instruction: a record with no establishable base is rejected under
``unresolvable_path`` rather than resolved through a guess.

The bounded ``uriBaseId`` walk
------------------------------
Bases **chain**: the specification's own section 3.14.14 example carries a
``uriBaseId`` on a base entry so that ``SRCROOT`` is expressed relative to
``PROJECTROOT``, so the walk follows the chain rather than reading one level.  It is
bounded by a visited-identifier set and by :data:`SARIF_BASE_CHAIN_MAX_DEPTH`
(``8``).  Three terminal cases stay distinct, because collapsing them into one
catch-all is the defect (AAP 0.5.4):

1. a base identifier **absent** from ``originalUriBaseIds``, a chain that **cycles**,
   one that **exceeds the depth**, or a **degenerate** ``file:///`` base -- resolved
   through the runner-recorded base *only where the metadata supplies an explicit
   base for that tool*, and otherwise rejected under ``unresolvable_path`` with the
   sub-reason in the detail;
2. an entry whose **URI is syntactically invalid** -- rejected under ``invalid_uri``;
3. a chain that **terminates on a relative reference with no absolute ancestor** --
   rejected under ``unresolvable_path``, with no metadata fallback.

Two documented producer gaps are handled rather than met with surprise: a
``uriBaseId`` emitted with no matching ``originalUriBaseIds`` entry (semgrep issue
10591), and ``ROOTPATH`` emitted as ``file:///`` inside a git repository (trivy issue
10364).  Two errata constraints are load-bearing: a relative reference **may** begin
with a single slash where required to distinguish items in an archive format (errata
issue 480, amending section 3.4.3), so such a reference is not rejected as absolute;
and consumers **must not normalize** ``..`` **segments out of a path** (the section
3.10.2 amendment).

No absolute path is ever emitted
--------------------------------
AAP 0.5.4 and 0.8.2.  An archive member serializes as
``<container-path-relative-to-root>!<member-path>`` with ``!`` the **single**
separator; a container outside the root takes the same form with its ``../``
segments **preserved rather than normalized**; a bytecode class with no source file
is a rejection rather than a guess.  ``Path.resolve()``, ``os.path.realpath`` and
``os.path.normpath`` are never applied to a reported path -- the first two collapse
``..`` and follow symlinks, and the third collapses ``..`` -- so relativization is
``PurePosixPath`` parts arithmetic that never cancels a ``..`` already present in the
input.  :func:`assert_relative_path` is the one place the invariant is enforced, and
every :class:`ResolvedPath` runs through it on construction.

``path`` is not optional.  Absence is permitted only for ``severity_native``,
``start_line``, ``cwe``, ``cve`` and ``package_coordinate`` (AAP 0.5.4), so a record
whose path cannot be resolved is rejected and counted.

The rejection-class enumeration lives here
------------------------------------------
Adapters may import only ``paths`` and ``severity`` and each must be able to name any
rejection class it produces, so :data:`REJECT_CLASSES` is defined here and passed to
``reconcile`` by ``cli.py``.  The ten classes are ``absent_path``,
``unresolvable_path``, ``invalid_uri``, ``ambiguous_source_resolution``,
``missing_rule_id``, ``missing_message``, ``non_integer_start_line``,
``unformable_package_coordinate``, ``unattributable_section`` and
``malformed_record``.  AAP 0.5.4: *"Reject rather than infer."*  No class is a bucket
for "probably fine", and every one has a negative fixture under
``oss-scan-results/adapter-tests/`` whether or not this run's artifacts contain the
case -- so the names are stable and greppable.

Every persisted diagnostic goes through one renderer, also here
--------------------------------------------------------------
A rejection's ``detail`` and ``record_identity`` are composed from
artifact-supplied strings, and a rejection is *recorded* -- into
``harness/artifacts/logs/normalize-run.json``, and from there quoted into
``tool-status.md``.  Rejecting a record therefore does not stop its content
reaching a durable file, so two hazards have to be closed at the point of
persistence: a terminal control sequence rewrites what a human reading the log
sees, and a URI carrying userinfo (``https://user:token@host/x``) puts whatever
credential the artifact happened to contain into the record.

:func:`sanitise_diagnostic` makes a composed sentence inert while keeping it
readable -- userinfo redacted, control characters rendered as ``<U+XXXX>``, length
bounded at :data:`DIAGNOSTIC_TEXT_LIMIT`.  :func:`safe_diagnostic` *describes* a raw
value instead of showing it -- type, length, sha256, bounded excerpt, structural
context -- and is what a site uses where it would otherwise interpolate
``{value!r}``.  :func:`sanitise_persisted` recurses the same treatment through a
mapping or a list.  ``\\n`` and ``\\t`` are deliberately **not** escaped: this
dataset carries messages with embedded newlines by design (AAP 0.5.4) and escaping
them would rewrite legitimate evidence, while ESC -- the actual injection vector --
is escaped.

The renderer lives in this module because AAP 0.6.4 fixes that an adapter depends
only on ``paths`` and ``severity``, and every adapter already imports this one.  It
is applied at the *persistence boundary* (:meth:`Rejection.as_dict`) rather than at
each of the ~60 sites that compose a detail: per-site sanitising is unenforceable,
and it would rewrite the hand-verified ``detail`` strings in every
``expected/*.rows.json`` for no security gain.  The in-memory attributes are left
exactly as the adapter composed them, so an assertion on ``rejection.detail`` still
reads what the adapter said.

Two recorded divergences from the description this module was specified against
-------------------------------------------------------------------------------
Both are recorded rather than repaired, per the AAP's authority rule.

*The Joern collector.*  The specification describes a provisioned
``harness/lib/joern_collect.py`` whose findings carry ``path``, ``class_file`` and
``path_resolution``.  No such file exists in this provisioning: the collector is
``harness/lib/joern-scan.sc``, and its ``findings[]`` carry ``query_id``,
``severity``, ``message``, ``callee``, ``class``, ``method``, ``file`` and ``line``.
The coordinate is therefore ``class`` -- a **dotted** type full name -- and ``file``
is the frontend's ephemeral ``/tmp/jimple2cpg-<id>/<pkg>/<Class>.class`` extraction
path for 693 of 693 findings, which the runner metadata names explicitly as
``record_path_field_to_ignore``.  :func:`class_key` therefore accepts *either* a
dotted full name or a class-file path, which is the same
"do-not-assume-one-shape" instruction applied to the shape that is actually written.
Where a collector explanation *is* present it is retained in the rejection's detail
and never in a dataset field.

*Union uniqueness, not index precedence.*  Resolving ambiguity with ``setdefault`` would
keep whichever candidate ``os.walk`` reached first and discard the rest, silently.  This
module instead takes the resolution **only where the union of candidates across both key
schemes and both source trees is exactly one distinct path**.  Measured at the pin:
``org/apache/spark/SparkContext`` is unique under ``by_filename`` (core) but has two
distinct candidates once ``by_decl`` is included (core and
``sql/connect/shims/.../shims.scala``, which really does declare stub ``SparkConf``,
``SparkContext``, ``rdd.RDD`` and ``api.java.JavaRDD``).  Only the union reading
rejects it, and rejecting it is what AAP 0.5.4 requires.
"""

from __future__ import annotations

import fnmatch
import glob as _glob
import hashlib
import json
import os
import re
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path, PurePosixPath, PureWindowsPath
from types import MappingProxyType
from typing import Any, Final, NoReturn
from urllib.parse import unquote, urlsplit

# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class PathPolicyError(ValueError):
    """Raised where a caller asks this module for something its contract forbids.

    A ``ValueError`` subclass rather than a bare ``assert``: ``python -O`` strips
    ``assert``, and an invariant that disappears under optimisation is not an
    invariant.  Raised for a malformed argument, an unknown rejection class, an
    unknown path-base kind and any attempt to construct a :class:`ResolvedPath`
    around a value that is not root-relative.
    """


class RunnerMetadataError(PathPolicyError):
    """Raised where ``runner-metadata.json`` cannot supply what a resolver needs.

    AAP 0.6.1: missing metadata for a tool that wrote an artifact is a hard error
    the caller surfaces, never a silent default to the root -- *"Guessing a base is
    exactly how every row for that tool gets a wrong path."*

    Structured detail travels with the message (CWE-703).  The caller records this as a
    configuration fault in ``normalize-run.json``, and a record that says only *"cannot be
    used"* cannot say at which step the document failed or against which measured cap.  So
    a raise site may attach ``details``: a mapping of JSON-serialisable facts --
    ``stage``, the observed value, the cap it crossed -- which ``cli._load_metadata``
    forwards into the fault's own details.  ``details`` defaults to an empty mapping, so
    every existing raise site that passes a message alone is unaffected.
    """

    def __init__(self, message: str, *, details: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details: Mapping[str, Any] = MappingProxyType(dict(details or {}))


# --------------------------------------------------------------------------- #
# The canonical rejection-class enumeration (AAP 0.5.4; this module owns it)
# --------------------------------------------------------------------------- #

#: An absent path: the record carries no path field, or carries an empty one.
REJECT_ABSENT_PATH: Final[str] = "absent_path"

#: An unresolvable path.  Also the destination AAP 0.5.4 routes the ``uriBaseId``
#: base-absent, cycle, over-depth, degenerate-base and no-absolute-ancestor cases
#: to, with the sub-reason carried in the rejection's detail rather than in a
#: separate class -- the AAP names the class, the detail names which case it was.
REJECT_UNRESOLVABLE_PATH: Final[str] = "unresolvable_path"

#: An entry whose URI is syntactically invalid.
REJECT_INVALID_URI: Final[str] = "invalid_uri"

#: An ambiguous bytecode-to-source resolution: two or more distinct source files
#: claim the same class key, so no unique resolution exists to take.
REJECT_AMBIGUOUS_SOURCE_RESOLUTION: Final[str] = "ambiguous_source_resolution"

#: A missing rule identifier.
REJECT_MISSING_RULE_ID: Final[str] = "missing_rule_id"

#: A missing message.
REJECT_MISSING_MESSAGE: Final[str] = "missing_message"

#: A ``start_line`` present but not an integer.
REJECT_NON_INTEGER_START_LINE: Final[str] = "non_integer_start_line"

#: A dependency-oriented record for which no package coordinate can be formed.
REJECT_UNFORMABLE_PACKAGE_COORDINATE: Final[str] = "unformable_package_coordinate"

#: A record that cannot be attributed to a section.
REJECT_UNATTRIBUTABLE_SECTION: Final[str] = "unattributable_section"

#: A malformed record: the container is not the shape its adapter requires.
REJECT_MALFORMED_RECORD: Final[str] = "malformed_record"

#: The closed set of rejection classes, in the order AAP 0.5.4 enumerates them.
#:
#: ``cli.py`` passes this tuple to ``reconcile`` as the vocabulary every rejection
#: class name is validated against, which is what lets ``reconcile`` stay free of
#: any import from this package.  The names are stable and greppable because the
#: adapter tests and ``tool-status.md`` are written against them.
REJECT_CLASSES: Final[tuple[str, ...]] = (
    REJECT_ABSENT_PATH,
    REJECT_UNRESOLVABLE_PATH,
    REJECT_INVALID_URI,
    REJECT_AMBIGUOUS_SOURCE_RESOLUTION,
    REJECT_MISSING_RULE_ID,
    REJECT_MISSING_MESSAGE,
    REJECT_NON_INTEGER_START_LINE,
    REJECT_UNFORMABLE_PACKAGE_COORDINATE,
    REJECT_UNATTRIBUTABLE_SECTION,
    REJECT_MALFORMED_RECORD,
)

_REJECT_CLASS_SET: Final[frozenset[str]] = frozenset(REJECT_CLASSES)

#: One sentence per class, for the rejection tables in ``tool-status.md``.  Kept
#: beside the names so a reader never has to infer what a class covers.
REJECT_CLASS_DESCRIPTIONS: Final[Mapping[str, str]] = MappingProxyType(
    {
        REJECT_ABSENT_PATH: (
            "the record carries no path field, or carries one that is empty"
        ),
        REJECT_UNRESOLVABLE_PATH: (
            "a path that cannot be expressed against the scan root: no establishable "
            "base, a uriBaseId that is absent, cyclic, over-depth or degenerate with "
            "no explicit runner-recorded base, a chain with no absolute ancestor, or "
            "a bytecode class no source file in the tree declares"
        ),
        REJECT_INVALID_URI: (
            "a URI or URI reference that is not syntactically valid"
        ),
        REJECT_AMBIGUOUS_SOURCE_RESOLUTION: (
            "two or more distinct source files claim the same bytecode class key, so "
            "no unique resolution exists to take"
        ),
        REJECT_MISSING_RULE_ID: "the record carries no rule identifier",
        REJECT_MISSING_MESSAGE: "the record carries no message",
        REJECT_NON_INTEGER_START_LINE: (
            "a start_line is present but is not an integer"
        ),
        REJECT_UNFORMABLE_PACKAGE_COORDINATE: (
            "a dependency-oriented record from which no package coordinate can be "
            "formed at any candidate level"
        ),
        REJECT_UNATTRIBUTABLE_SECTION: (
            "a record that cannot be attributed to one of its artifact's finding "
            "sections, so its scanner_class cannot be established"
        ),
        REJECT_MALFORMED_RECORD: (
            "the record is not the shape its adapter requires"
        ),
    }
)


def is_reject_class(value: object) -> bool:
    """Return whether ``value`` is one of the ten canonical rejection classes."""
    return isinstance(value, str) and value in _REJECT_CLASS_SET


# --------------------------------------------------------------------------- #
# Safe diagnostics -- the one renderer every persisted diagnostic goes through
#
# Every string an adapter reports about a record is artifact-supplied: a rule
# identifier, a message, a URI, a class name.  Those strings are composed into
# rejection details and halt messages, and those are *persisted* -- into
# ``harness/artifacts/logs/normalize-run.json``, and from there quoted into
# ``tool-status.md``.  Two hazards follow, and neither is hypothetical for a
# scanner reading a repository that anyone may open a pull request against:
#
#   * a terminal control sequence in a diagnostic rewrites what a human reading
#     the log sees, and a NUL or a C1 byte can truncate or confuse a downstream
#     consumer of the record;
#   * a URI carrying userinfo -- ``https://user:token@host/x`` -- puts a
#     credential the artifact happened to contain into a durable record, and a
#     rejected record is still recorded, so rejecting it is not protection.
#
# So there is exactly one renderer, here, and it has two entry points because
# there are two jobs.  :func:`sanitise_diagnostic` keeps a composed *sentence*
# readable while making it inert; :func:`safe_diagnostic` *describes* a raw value
# -- its type, its length, its digest and a bounded excerpt -- rather than showing
# it, which is what a site that would otherwise interpolate ``{value!r}`` uses.
#
# It lives in this module because AAP 0.6.4 fixes that an adapter depends only on
# ``paths`` and ``severity``, and every adapter already imports this one.  A new
# module would have to be imported by six adapters and by ``cli``; ``shape.py`` is
# a leaf that imports nothing from the package and keeps its own bounded
# rendering, which is a deliberate duplication of a small guard rather than a
# second policy.
# --------------------------------------------------------------------------- #

#: How much of a composed diagnostic *sentence* is carried into a record. Chosen
#: above every detail this pipeline has been observed to produce -- the longest
#: measured was 376 characters, a Joern unresolvable-path explanation -- so the
#: bound catches an artifact-driven blow-up and truncates nothing authored.
DIAGNOSTIC_TEXT_LIMIT: Final[int] = 2_000

#: How much of a single artifact-supplied *value* is excerpted when it is
#: described rather than shown. Deliberately small: the excerpt is there to make a
#: value recognisable, and the digest beside it is what identifies it exactly.
DIAGNOSTIC_VALUE_LIMIT: Final[int] = 512

#: What replaces a URI's userinfo component. A marker rather than a deletion, so
#: the record states that something was removed instead of quietly reading as a
#: URI that never had credentials in it.
USERINFO_REDACTION: Final[str] = "<redacted-userinfo>"

# `scheme://userinfo@` -- the only place a URI may carry a credential (RFC 3986
# section 3.2.1). Anchored on the scheme and the authority marker, so an ordinary
# `name@domain` in prose, a `git@host:path` SSH shorthand and a severity source
# such as `nvd@nist.gov` are all left exactly as the artifact wrote them: those
# are not credentials, and redacting them would remove evidence for nothing.
_URI_USERINFO_RE: Final[re.Pattern[str]] = re.compile(
    r"([A-Za-z][A-Za-z0-9+.\-]*://)([^/?#\s@]+)@"
)

# The characters escaped on their way into a record. Every C0 control except tab
# and newline, DEL, and the whole C1 range. Tab and newline are exempt because
# this dataset carries messages with embedded newlines by design (AAP 0.5.4) and
# escaping them would rewrite legitimate evidence; ESC -- the actual terminal
# injection vector -- is not exempt.
_DIAGNOSTIC_KEEP_CONTROLS: Final[frozenset[str]] = frozenset({"\t", "\n"})


def _is_escapable_control(char: str) -> bool:
    """Whether ``char`` must be escaped before it reaches a log or a record."""
    if char in _DIAGNOSTIC_KEEP_CONTROLS:
        return False
    code = ord(char)
    return code < 0x20 or code == 0x7F or 0x80 <= code <= 0x9F


def _escape_controls(text: str) -> tuple[str, int]:
    """Return ``text`` with every escapable control replaced, and how many there were.

    The replacement is ``<U+XXXX>`` rather than a backslash escape, deliberately.
    A backslash form would be ambiguous with a literal backslash sequence the
    artifact itself carried, and resolving that ambiguity would mean escaping
    backslashes too -- which would rewrite every Windows path in every diagnostic
    for no gain. ``<U+001B>`` cannot be mistaken for anything a terminal acts on,
    and a literal ``<U+001B>`` in artifact text is inert.
    """
    if not any(_is_escapable_control(char) for char in text):
        return text, 0
    escaped: list[str] = []
    count = 0
    for char in text:
        if _is_escapable_control(char):
            escaped.append(f"<U+{ord(char):04X}>")
            count += 1
        else:
            escaped.append(char)
    return "".join(escaped), count


def _redact_userinfo(text: str) -> tuple[str, int]:
    """Return ``text`` with every URI userinfo component replaced, and the count."""
    redactions = 0

    def replace(match: re.Match[str]) -> str:
        nonlocal redactions
        redactions += 1
        return f"{match.group(1)}{USERINFO_REDACTION}@"

    return _URI_USERINFO_RE.sub(replace, text), redactions


def _text_digest(text: str) -> str:
    """Return the sha256 of ``text`` as UTF-8, so a truncated excerpt is still identified."""
    return hashlib.sha256(text.encode("utf-8", errors="surrogatepass")).hexdigest()


@dataclass(frozen=True)
class DiagnosticText:
    """One composed diagnostic sentence, made safe to persist, with what changed.

    Attributes
    ----------
    text:
        The sentence as it may be recorded: userinfo redacted, escapable control
        characters escaped, and bounded where a bound was asked for.
    original_length:
        The character length of the sentence as composed, before anything.
    sha256:
        The digest of the sentence as composed. Present whether or not anything
        changed, so a reader can tie a truncated or redacted rendering back to the
        exact text it came from.
    truncated:
        Whether the bound cut the rendering.
    controls_escaped:
        How many control characters were escaped.
    userinfo_redactions:
        How many URI userinfo components were redacted.

    ``changed`` is false for the overwhelming majority of diagnostics -- ordinary
    prose about an ordinary path -- and that is what keeps the run record stable
    between runs: a sentence that needed nothing is recorded exactly as composed.
    """

    text: str
    original_length: int
    sha256: str
    truncated: bool = False
    controls_escaped: int = 0
    userinfo_redactions: int = 0

    @property
    def changed(self) -> bool:
        """Whether the safe rendering differs from the sentence as composed."""
        return bool(
            self.truncated or self.controls_escaped or self.userinfo_redactions
        )

    def as_dict(self) -> dict[str, Any]:
        """Return what was done, for a record that carries the rendering beside it."""
        return {
            "original_length": self.original_length,
            "sha256": self.sha256,
            "truncated": self.truncated,
            "controls_escaped": self.controls_escaped,
            "userinfo_redactions": self.userinfo_redactions,
        }


def sanitise_diagnostic(
    text: str, *, limit: int | None = DIAGNOSTIC_TEXT_LIMIT
) -> DiagnosticText:
    """Make one composed diagnostic sentence safe to persist, keeping it readable.

    Unlike :func:`safe_diagnostic` this keeps the text: a rejection detail's whole
    job is to be read, and describing it instead of showing it would leave the
    record unusable. What it removes is the part that is dangerous rather than
    informative -- a control sequence, a credential -- and it bounds the length so
    an artifact cannot decide how large this pipeline's record is.

    Args:
        text: The composed sentence.
        limit: The maximum rendered length, or ``None`` for no bound. ``None`` is
            for a value whose size is already governed by a documented limit of
            its own -- a runner's stream excerpt, for instance, which AAP 0.5.4
            requires quoted verbatim and which ``cli.py`` bounds itself.

    Returns:
        A :class:`DiagnosticText` carrying the safe rendering and what changed.

    Raises:
        PathPolicyError: Where ``text`` is not a ``str`` or ``limit`` is not a
            positive ``int`` or ``None``. Both are programming faults in a caller.
    """
    if not isinstance(text, str):
        raise PathPolicyError(
            f"a diagnostic must be a str; observed {type(text).__name__}"
        )
    if limit is not None and (not isinstance(limit, int) or isinstance(limit, bool)):
        raise PathPolicyError(
            f"limit must be an int or None; observed {type(limit).__name__}"
        )
    if limit is not None and limit < 1:
        raise PathPolicyError(f"limit must be positive; observed {limit!r}")

    original_length = len(text)
    digest = _text_digest(text)
    # Redact first, then escape: redaction matches on the URI's own syntax, and an
    # escaped control inside the authority would hide the '@' the pattern anchors
    # on. Truncation is last so it applies to what would actually be written.
    redacted, userinfo_redactions = _redact_userinfo(text)
    escaped, controls_escaped = _escape_controls(redacted)
    truncated = limit is not None and len(escaped) > limit
    if truncated:
        assert limit is not None  # narrowed by `truncated`
        escaped = (
            f"{escaped[:limit]}... [truncated at {limit} of {original_length} "
            f"characters; sha256 {digest[:16]}]"
        )
    return DiagnosticText(
        text=escaped,
        original_length=original_length,
        sha256=digest,
        truncated=truncated,
        controls_escaped=controls_escaped,
        userinfo_redactions=userinfo_redactions,
    )


@dataclass(frozen=True)
class SafeDiagnostic:
    """One artifact-supplied value, described rather than shown.

    Attributes
    ----------
    value_type:
        The value's Python type name -- often the whole diagnosis, since a field
        carrying a ``dict`` where a ``str`` was required is a shape fault.
    context:
        Which field or structure the value came from, supplied by the caller. This
        is the structural context that makes the rendering actionable: a length
        and a digest with no idea where they came from are not.
    character_length:
        The value's length as text, before the excerpt was taken.
    sha256:
        The digest of the whole value as text. Two records carrying the same
        oversized value are recognisable as the same value from this alone.
    excerpt:
        A bounded, redacted, control-escaped prefix -- enough to recognise the
        value, never enough to be a copy of it.
    truncated, controls_escaped, userinfo_redactions:
        What the excerpt did to the value, on the same terms as
        :class:`DiagnosticText`.
    """

    value_type: str
    context: str | None
    character_length: int
    sha256: str
    excerpt: str
    truncated: bool = False
    controls_escaped: int = 0
    userinfo_redactions: int = 0

    def __str__(self) -> str:
        """Render as one line, for a site that would otherwise interpolate ``{value!r}``.

        The order is type, context, length, digest, excerpt -- structure first,
        content last -- so a reader who stops at the first clause still knows what
        kind of fault this is.
        """
        where = f" from {self.context}" if self.context else ""
        return (
            f"{self.value_type}{where} (length {self.character_length}, "
            f"sha256 {self.sha256[:16]}, excerpt {self.excerpt!r}"
            f"{', truncated' if self.truncated else ''})"
        )

    def as_dict(self) -> dict[str, Any]:
        """Return the description as a mapping, for a halt's structured details."""
        return {
            "value_type": self.value_type,
            "context": self.context,
            "character_length": self.character_length,
            "sha256": self.sha256,
            "excerpt": self.excerpt,
            "truncated": self.truncated,
            "controls_escaped": self.controls_escaped,
            "userinfo_redactions": self.userinfo_redactions,
        }


def safe_diagnostic(
    value: Any, *, context: str | None = None, limit: int = DIAGNOSTIC_VALUE_LIMIT
) -> SafeDiagnostic:
    """Describe one artifact-supplied value safely, for a diagnostic that persists.

    This is what a site uses where it would otherwise write ``{value!r}``. A
    ``repr`` is unbounded, carries control characters through untouched, and will
    happily put a credential-bearing URI into a durable record; it also tells a
    reader nothing about a value too large to read.

    A non-string is rendered from its ``repr`` rather than refused, because the
    values that reach here are exactly the ones whose type was wrong. The type name
    is reported separately, so ``dict from locations[0].physicalLocation`` reads as
    the shape fault it is.

    Args:
        value: The value, as it came out of the artifact.
        context: Where it came from -- a field path, a member name -- so the
            description locates the fault as well as characterising it.
        limit: The excerpt bound.

    Returns:
        A :class:`SafeDiagnostic`; ``str()`` of it is the one-line rendering.

    Raises:
        PathPolicyError: Where ``limit`` is not a positive ``int``.
    """
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise PathPolicyError(f"limit must be a positive int; observed {limit!r}")
    if context is not None and not isinstance(context, str):
        raise PathPolicyError(
            f"context must be a str or None; observed {type(context).__name__}"
        )
    text = value if isinstance(value, str) else repr(value)
    rendered = sanitise_diagnostic(text, limit=limit)
    return SafeDiagnostic(
        value_type=type(value).__name__,
        context=context,
        character_length=len(text),
        sha256=rendered.sha256,
        excerpt=rendered.text,
        truncated=rendered.truncated,
        controls_escaped=rendered.controls_escaped,
        userinfo_redactions=rendered.userinfo_redactions,
    )


def sanitise_persisted(value: Any, *, limit: int | None = DIAGNOSTIC_VALUE_LIMIT) -> Any:
    """Return ``value`` safe to serialise, recursing through mappings and sequences.

    The persistence boundary's own helper: :meth:`Rejection.as_dict` and
    ``cli.py``'s halt record pass whole structures through it, so a value added to
    a detail mapping later is covered without anyone having to remember to wrap it.
    A string is sanitised; a mapping and a list are rebuilt with their members
    sanitised; every other JSON scalar is returned unchanged, because an ``int``, a
    ``bool`` and ``None`` carry no control characters and no credential.

    Keys are sanitised too. An artifact-supplied value can reach a key -- a counter
    keyed by an observed section name, for instance -- and a control character in a
    JSON key is exactly as hostile to a reader as one in a value.
    """
    if isinstance(value, str):
        return sanitise_diagnostic(value, limit=limit).text
    if isinstance(value, Mapping):
        return {
            (
                sanitise_diagnostic(key, limit=limit).text
                if isinstance(key, str)
                else key
            ): sanitise_persisted(item, limit=limit)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [sanitise_persisted(item, limit=limit) for item in value]
    return value


@dataclass(frozen=True)
class Rejection:
    """One rejected record, counted under a named class and never inferred into a row.

    AAP 0.5.4: *"Where a record cannot be attributed with certainty, it is rejected
    and the rejection recorded as a class with its count -- never guessed into a
    field."*  A rejection is therefore a first-class result of this module, returned
    alongside :class:`ResolvedPath` rather than raised, so an adapter counts it and
    carries on through the rest of the artifact.

    Attributes
    ----------
    reject_class:
        One of :data:`REJECT_CLASSES`.  Validated on construction: an unknown class
        raises :class:`PathPolicyError` rather than being counted under a name no
        test and no status document knows about.
    tool:
        The canonical tool identifier of the artifact the record came from.
    detail:
        The sub-reason, in enough words to act on -- which of the ``uriBaseId``
        terminal cases it was, which candidates made a resolution ambiguous, or the
        collector explanation retained verbatim for an unresolvable bytecode class.
    record_identity:
        The offending record's identifying fields, as a read-only mapping.  Enough
        to find the record in the artifact again; never the record itself, and never
        a secret value.
    """

    reject_class: str
    tool: str
    detail: str
    record_identity: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not is_reject_class(self.reject_class):
            raise PathPolicyError(
                f"unknown rejection class {self.reject_class!r}; the closed set is "
                f"{', '.join(REJECT_CLASSES)}"
            )
        if not isinstance(self.tool, str) or not self.tool:
            raise PathPolicyError(
                f"a rejection must name the tool whose artifact it came from; "
                f"observed {self.tool!r}"
            )
        if not isinstance(self.detail, str) or not self.detail:
            raise PathPolicyError(
                "a rejection must carry a detail naming the sub-reason; an empty "
                "detail is the catch-all AAP 0.5.4 forbids"
            )
        if not isinstance(self.record_identity, Mapping):
            raise PathPolicyError(
                f"record_identity must be a mapping of identifying fields; observed "
                f"{type(self.record_identity).__name__}"
            )
        # Frozen instances are shared freely, so the mapping is made read-only here
        # rather than trusted to stay unmutated by whoever holds a reference.
        object.__setattr__(
            self, "record_identity", MappingProxyType(dict(self.record_identity))
        )

    def as_dict(self) -> dict[str, Any]:
        """Return a plain, JSON-serialisable dict of this rejection, safe to persist.

        This is the persistence boundary for every rejection: ``cli.py`` calls it
        once per rejection and the result goes into ``normalize-run.json``, which is
        then quoted into ``tool-status.md``.  So the sanitising happens *here*, and
        nowhere else:

        * ``detail`` and every string in ``record_identity`` are put through
          :func:`sanitise_diagnostic` -- URI userinfo redacted, control characters
          escaped, length bounded.  Both are composed from artifact-supplied values
          (a rule identifier, a message, a URI, a class name), and a rejected record
          is still a *recorded* record, so rejecting it is not protection.
        * The in-memory attributes are left exactly as the adapter composed them.
          An assertion on ``rejection.detail`` therefore reads what the adapter said,
          while the durable record reads what is safe to keep.
        * ``diagnostics`` appears **only where something was changed**.  An ordinary
          detail about an ordinary path is recorded byte-for-byte as composed and
          carries no extra key, which is what keeps the run record stable between
          runs over unchanged artifacts; a detail that was redacted or truncated
          says so, with the original length and the digest of the whole text.

        The alternative -- sanitising at each of the ~60 sites that compose a detail
        -- was refused twice over: it is unenforceable (the next site added forgets),
        and it would rewrite the hand-verified ``detail`` strings in every
        ``expected/*.rows.json`` for no security gain, since those details are benign.
        """
        rendered = sanitise_diagnostic(self.detail, limit=DIAGNOSTIC_TEXT_LIMIT)
        record: dict[str, Any] = {
            "reject_class": self.reject_class,
            "tool": self.tool,
            "detail": rendered.text,
            "record_identity": sanitise_persisted(
                dict(self.record_identity), limit=DIAGNOSTIC_VALUE_LIMIT
            ),
        }
        if rendered.changed:
            record["diagnostics"] = rendered.as_dict()
        return record


def make_rejection(
    reject_class: str,
    tool: str,
    detail: str,
    **record_identity: Any,
) -> Rejection:
    """Return a :class:`Rejection`, with the record's identifying fields as kwargs.

    A convenience for the adapters, whose call sites read better as
    ``make_rejection(REJECT_ABSENT_PATH, "checkov", "...", check_id=cid)`` than as a
    dict literal.  It performs no validation of its own: :class:`Rejection` does all
    of it, in one place.
    """
    return Rejection(
        reject_class=reject_class,
        tool=tool,
        detail=detail,
        record_identity=record_identity,
    )



# --------------------------------------------------------------------------- #
# Path kinds -- the discriminator cli.py tallies (AAP 0.6.1)
# --------------------------------------------------------------------------- #

#: A path naming a location inside the scan root.  Whether the file exists on disk is a
#: separate question, and this module deliberately does not answer it: every function here
#: is required to work on a parsed fixture with no live filesystem beyond the pinned root,
#: so a resolver that stat-ed would not be testable that way.  AAP 0.6.1 has
#: ``run-record.md`` report "the rows whose path names something that is not a file on
#: disk", and ``cli._paths_not_on_disk`` takes that count -- once, over the emitted rows,
#: against the same root -- and publishes it in ``normalize-run.json`` under
#: ``totals.paths_not_on_disk``.  It is a different measurement from the path-kind tally
#: below, which classifies a path by its *form*; a ``tree_file`` naming a file the pin does
#: not carry is invisible to the tally and counted there.
PATH_KIND_TREE_FILE: Final[str] = "tree_file"

#: A path that names a location outside the scan root, and therefore carries ``../``
#: segments -- preserved, never normalized away.  The ``..`` need not be at the front:
#: the discriminator is the running segment depth going below zero *anywhere*, which
#: :func:`analyse_containment` computes and :func:`path_kind_for` reads.
PATH_KIND_OUTSIDE_ROOT: Final[str] = "outside_root"

#: An archive member: ``<container-relative-to-root>!<member-path>``.
PATH_KIND_ARCHIVE_MEMBER: Final[str] = "archive_member"

#: A source path resolved from a bytecode class identifier.  A real file in the
#: tree, so *not* a non-filesystem coordinate -- listed separately only because its
#: basis is a class-to-source resolution rather than a reported path.
PATH_KIND_BYTECODE_SOURCE: Final[str] = "bytecode_source"

#: The closed set of path kinds.
PATH_KINDS: Final[tuple[str, ...]] = (
    PATH_KIND_TREE_FILE,
    PATH_KIND_OUTSIDE_ROOT,
    PATH_KIND_ARCHIVE_MEMBER,
    PATH_KIND_BYTECODE_SOURCE,
)

_PATH_KIND_SET: Final[frozenset[str]] = frozenset(PATH_KINDS)

#: The kinds that are *not* a plain file in the scanned tree.  AAP 0.5.4 requires
#: every such row to take ``in_scope: false``, to be kept, and to be counted in the
#: proportion ``run-record.md`` reports.
NON_FILESYSTEM_PATH_KINDS: Final[tuple[str, ...]] = (
    PATH_KIND_OUTSIDE_ROOT,
    PATH_KIND_ARCHIVE_MEMBER,
)

_NON_FILESYSTEM_KIND_SET: Final[frozenset[str]] = frozenset(NON_FILESYSTEM_PATH_KINDS)


def is_non_filesystem_kind(kind: str) -> bool:
    """Return whether ``kind`` is a non-filesystem coordinate.

    ``outside_root`` and ``archive_member`` are; ``tree_file`` and
    ``bytecode_source`` are not.  A caller tallying the reported proportion uses
    this rather than re-deriving the set, so the two can never drift.
    """
    return kind in _NON_FILESYSTEM_KIND_SET


# The basis strings recorded on a ResolvedPath.  These are provenance for the
# record, not policy: a reader of tool-status.md can see *how* each path was
# established, which is what makes a wrong base visible rather than silent.
BASIS_ALREADY_ROOT_RELATIVE: Final[str] = "already-root-relative"
BASIS_ABSOLUTE_RELATIVIZED: Final[str] = "absolute-relativized-to-root"
BASIS_RESOLVED_AGAINST_BASE: Final[str] = "resolved-against-recorded-base"
BASIS_SARIF_BASE_CHAIN: Final[str] = "sarif-uri-base-id-chain"
BASIS_SARIF_METADATA_BASE: Final[str] = "sarif-degenerate-base-metadata-fallback"
BASIS_SARIF_NO_BASE_ID: Final[str] = "sarif-no-uri-base-id-metadata-base"
BASIS_ARCHIVE_LEADING_SLASH: Final[str] = "archive-style-leading-slash-errata-480"
BASIS_ARCHIVE_MEMBER: Final[str] = "archive-member-serialized"
BASIS_CHECKOV_REPO_FILE_PATH: Final[str] = "checkov-repo-file-path"
BASIS_CHECKOV_FILE_ABS_PATH: Final[str] = "checkov-file-abs-path"
BASIS_CHECKOV_FILE_PATH: Final[str] = "checkov-file-path-leading-slash-stripped"
BASIS_TRIVY_SECTION_TARGET: Final[str] = "trivy-section-target"
BASIS_TRIVY_PER_RECORD_PATH: Final[str] = "trivy-per-record-path-refinement"
BASIS_SOURCE_INDEX_FILENAME: Final[str] = "source-index-filename"
BASIS_SOURCE_INDEX_DECLARATION: Final[str] = "source-index-declaration"
BASIS_SOURCE_INDEX_BOTH: Final[str] = "source-index-filename+declaration"

#: The collector's own vocabulary for a bytecode class it could not place.  Kept as
#: a named constant because AAP 0.5.4 has such an explanation retained in the
#: rejection record and never in a dataset field.
COLLECTOR_UNRESOLVED_BYTECODE_ONLY: Final[str] = "unresolved-bytecode-only"


# --------------------------------------------------------------------------- #
# The twelve authoritative globs, and the matcher
# --------------------------------------------------------------------------- #

#: The twelve authoritative scope globs (AAP 0.3.1), byte-exact and in the request's
#: order.  Held here as *documentation and a cross-check* only: the operative copy
#: is ``harness/scope/allowlist.txt``, read with :func:`load_allowlist`, and
#: :func:`allowlist_matches_authoritative_globs` is how a caller confirms the two
#: agree.  AAP 0.8.2: the globs *"are the authoritative scope definition and stay
#: byte-exact"* -- they are never derived, extended or narrowed here.
ALLOWLIST_GLOBS: Final[tuple[str, ...]] = (
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

#: The pattern segment that matches zero or more path segments.
RECURSIVE_SEGMENT: Final[str] = "**"

#: The literal that puts a path out of scope (AAP 0.3.1).
SRC_TEST_MARKER: Final[str] = "src/test"

#: The trailing form :func:`scope_glob_bases` strips, mirroring the arithmetic the
#: provisioned ``harness/lib/scope.sh`` performs with ``base="${glob%/\\*\\*}"``.
GLOB_RECURSIVE_SUFFIX: Final[str] = "/**"

# Measured evidence, recorded so a caller can check a number rather than trust one.
# These are observations at the pinned commit, not policy: the pin's expansion of
# the twelve globs, used by the differential check and cited in run-record.md.
PINNED_EXPANSION_DIRECTORIES: Final[int] = 18
PINNED_EXPANSION_FILES: Final[int] = 4095


def normalise_reported_path(value: str) -> str:
    """Return ``value`` with separators unified and nothing else collapsed.

    Exactly three normalisations are performed, and they are the only ones anywhere
    in this module:

    * backslashes become forward slashes, so a Windows-flavoured report matches;
    * leading ``.`` segments are dropped, so ``./a/b`` and ``.//a/b`` both become
      ``a/b``.  Only *leading* ones: a ``.`` in the middle is left alone, because
      removing it is a normalisation this module has no mandate for;
    * empty interior segments (from ``//``) are dropped, since a POSIX path gives
      them no meaning.

    The **leading slash run is rendered in exactly three forms**, since what the run
    means saturates at two slashes:

    * **no** leading slash -- the path keeps none;
    * **exactly one** -- preserved as one, because a single leading slash is either
      filesystem-absolute or, per SARIF errata issue 480, an archive-distinguishing
      relative reference, and the two readings are the caller's to decide between
      (:func:`resolve_sarif_location`); :func:`strip_single_leading_slash` must
      therefore still find it;
    * **two or more** -- rendered as exactly ``//``.  Two leading slashes are the URI
      authority form, which :func:`parse_uri_reference` must still be able to see, and
      a third or later slash adds an empty segment, which POSIX gives no meaning and
      the interior-empty-segment rule above drops.  So ``///a`` and ``////a//b``
      render as ``//a`` and ``//a/b``.

    Everything else is left exactly as reported.  In particular a ``..`` segment is
    **never** cancelled: the SARIF 2.1.0 errata (the section 3.10.2 amendment) forbid
    normalizing ``..`` out of a path.  ``Path.resolve()``, ``os.path.realpath`` and
    ``os.path.normpath`` are never called on a reported path.

    Raises
    ------
    PathPolicyError
        If ``value`` is not a string.
    """
    if not isinstance(value, str):
        raise PathPolicyError(
            f"a reported path must be a str; observed {type(value).__name__}"
        )
    text = value.replace("\\", "/")
    if text.startswith("//"):
        prefix = "//"
    elif text.startswith("/"):
        prefix = "/"
    else:
        prefix = ""
    segments = [segment for segment in text.split("/") if segment]
    index = 0
    while index < len(segments) and segments[index] == ".":
        index += 1
    return prefix + "/".join(segments[index:])


@lru_cache(maxsize=512)
def _pattern_segments(pattern: str) -> tuple[str, ...]:
    """Return ``pattern`` split into its non-empty segments.

    Cached because the same twelve patterns are matched against thousands of paths
    and the split is pure.  The cache holds only the derived split of an argument
    the caller supplied -- it is not module state that changes behaviour, and no
    file or environment variable is consulted to build it.
    """
    if not isinstance(pattern, str) or not pattern:
        raise PathPolicyError(f"a glob must be a non-empty str; observed {pattern!r}")
    return tuple(segment for segment in pattern.replace("\\", "/").split("/") if segment)


def match_glob(pattern: str, path: str) -> bool:
    """Return whether ``path`` matches ``pattern`` under the AAP's glob semantics.

    ``**`` consumes **zero or more** whole segments.  Any other pattern segment must
    match **exactly one** path segment, using a case-sensitive single-segment glob --
    :func:`fnmatch.fnmatchcase` is safe for that precisely because a segment contains
    no ``/``, so its ``*`` cannot cross a separator.  The match is anchored at both
    ends: the pattern must consume the whole path and the path must consume the whole
    pattern.

    The implementation is an explicit segment-wise walk memoised on
    ``(pattern_index, path_index)``, and the memo is what keeps the zero-or-more branch
    from running away.  Two bounds, distinct and both worth stating with ``P`` for
    ``len(pattern)`` and ``L`` for ``len(path)``: the key space is
    ``(P + 1) * (L + 1)``, so there are ``O(P * L)`` distinct memoised **states** and
    none is ever recomputed; but a ``**`` state probes every split point from its own
    position to the end of the path, up to ``L + 1`` probes, so worst-case
    **operations** are ``O(P * L**2)``.  Recursion depth stays bounded by ``P + L`` --
    a handful of segments each -- because every recursive step advances one index or
    the other.

    Examples
    --------
    ``sql/connect/**/src/main/**`` matches all three of
    ``sql/connect/src/main/z.scala`` (zero intermediate segments -- the case
    ``fnmatch`` gets wrong and for which no directory exists in either tree),
    ``sql/connect/common/src/main/A.scala`` (one) and
    ``sql/connect/client/jvm/src/main/A.scala`` (two -- the case
    ``PurePath.match`` silently drops).  ``core/src/main/*`` does **not** match
    ``core/src/main/a/b.py``.
    """
    pattern_segments = _pattern_segments(pattern)
    path_segments = tuple(
        segment for segment in normalise_reported_path(path).split("/") if segment
    )
    return _match_segments(pattern_segments, path_segments)


def _match_segments(pattern: Sequence[str], path: Sequence[str]) -> bool:
    """Memoised segment-wise match; see :func:`match_glob` for the semantics."""
    pattern_length = len(pattern)
    path_length = len(path)
    memo: dict[tuple[int, int], bool] = {}

    def walk(i: int, j: int) -> bool:
        """Whether ``pattern[i:]`` matches ``path[j:]``; memoised on ``(i, j)``.

        The memo is what stops the zero-or-more branch running away: each ``(i, j)``
        is decided once, however many split points reach it.  Recursion depth is
        bounded separately, by ``len(pattern) + len(path)`` -- a handful of segments
        each -- since every step advances one index or the other.
        """
        key = (i, j)
        cached = memo.get(key)
        if cached is not None:
            return cached
        if i == pattern_length:
            # The pattern is spent: it matches only if the path is spent too, which
            # is what anchors the match at the right-hand end.
            result = j == path_length
        elif pattern[i] == RECURSIVE_SEGMENT:
            # Zero or more segments: try every split point, shortest first, so the
            # zero-segment case is reached even where no deeper split can match.
            result = False
            for k in range(j, path_length + 1):
                if walk(i + 1, k):
                    result = True
                    break
        else:
            result = (
                j < path_length
                and fnmatch.fnmatchcase(path[j], pattern[i])
                and walk(i + 1, j + 1)
            )
        memo[key] = result
        return result

    return walk(0, 0)


def matches_any_glob(path: str, globs: Iterable[str]) -> str | None:
    """Return the first glob in ``globs`` that matches ``path``, or ``None``.

    The glob itself is returned rather than a boolean so a caller can report *which*
    root a row was attributed to.  Order is the allowlist's order, which is the
    request's order, so the answer is deterministic.
    """
    for candidate in globs:
        if match_glob(candidate, path):
            return candidate
    return None


def contains_src_test(path: str) -> bool:
    """Return whether ``path`` contains the literal ``src/test`` (AAP 0.3.1).

    The literal reading, exactly as the AAP states it and exactly as the provisioned
    ``harness/lib/scope.sh`` implements it with ``case "$path" in *src/test*)``.
    """
    return SRC_TEST_MARKER in normalise_reported_path(path)


def has_src_test_segment_pair(path: str) -> bool:
    """Return whether ``path`` carries ``src`` and ``test`` as an exact segment pair.

    The stricter reading of the same exclusion, kept so the literal one can be
    *checked* rather than asserted.  See :func:`src_test_readings_agree`.
    """
    segments = normalise_reported_path(path).split("/")
    return any(
        segments[index] == "src" and segments[index + 1] == "test"
        for index in range(len(segments) - 1)
    )


def src_test_readings_agree(paths: Iterable[str]) -> tuple[str, ...]:
    """Return the paths on which the literal and segment-pair readings disagree.

    An empty result means the literal test introduces zero false exclusions over
    ``paths``.  Measured at the pin over the whole tree: 20,361 files contain
    ``src/test`` literally, 20,361 carry it as the segment pair, and this function
    returns empty -- so the literal reading is safe *and demonstrably so*.  A
    non-empty result is a note for ``run-record.md``, not a silent difference.
    """
    return tuple(
        candidate
        for candidate in paths
        if contains_src_test(candidate) != has_src_test_segment_pair(candidate)
    )


def in_scope(
    path: str,
    globs: Iterable[str],
    *,
    kind: str = PATH_KIND_TREE_FILE,
) -> bool:
    """Return the ``in_scope`` field for ``path`` -- and nothing else.

    Four rules, applied in this order:

    1. a **non-filesystem coordinate** (an archive member, or a location outside the
       root) is never in scope; AAP 0.5.4: *"Every such row takes ``in_scope:
       false``, is kept, and is counted in the reported proportion."*  Applying this
       first matters: an archive member such as
       ``core/src/main/x.jar!org/apache/Foo.class`` would otherwise match
       ``core/src/main/**`` on its segments alone;
    2. a coordinate that **leaves the root** at any segment is never in scope, tested
       here through :func:`analyse_containment` rather than trusted from ``kind``.
       This is not redundant with rule 1: ``kind`` is an argument, so a caller that
       defaulted it, or that classified the path on its first segment alone, would
       otherwise get ``True`` for ``core/src/main/../../../../etc/passwd``, which
       matches ``core/src/main/**`` on its segments while naming a location one level
       above the tree (three concrete segments, four ``..``).  Deciding it here as
       well means the two disagree nowhere;
    3. a path containing the literal ``src/test`` is out of scope, and this
       **overrides** a positive glob match.  Both spellings are tested -- the reported
       one and the canonical shadow -- so ``sql/core/src/main/../test/X.scala``, whose
       reported spelling carries ``src/main`` and whose shadow carries ``src/test``,
       is excluded on the shadow;
    4. otherwise the path is in scope exactly where it matches one of ``globs``.  The
       reported spelling is matched first, and the canonical shadow only where it is a
       different string and the reported spelling did not match.  That order makes the
       rule **monotone**: it can add a match a reported-spelling-only reading misses --
       for ``a/../core/src/main/X.scala``, which lexically names a file under
       ``core/src/main`` -- and can never take one away, so a path carrying no ``..``
       and no interior ``.`` has one spelling and one verdict.

    ``in_scope`` is decided by the allowlist alone (AAP 0.6.4).  A row from a
    directory a runner reached but the allowlist does not cover -- the pin's 47
    out-of-scope ``pom.xml`` files and three lockfiles among them -- takes
    ``in_scope: false`` and is kept.  This function never drops anything, and it never
    changes the string that reaches the dataset: the canonical shadow is a
    classification device, computed here and emitted nowhere, because the SARIF 2.1.0
    errata forbid normalizing ``..`` out of a reported path.

    Raises
    ------
    PathPolicyError
        If ``kind`` is not one of :data:`PATH_KINDS`, or if ``path`` is not a string.
    """
    if kind not in _PATH_KIND_SET:
        raise PathPolicyError(
            f"unknown path kind {kind!r}; the closed set is {', '.join(PATH_KINDS)}"
        )
    if is_non_filesystem_kind(kind):
        return False
    analysis = analyse_containment(path)
    if analysis.escapes_root:
        return False
    if SRC_TEST_MARKER in analysis.reported_path:
        return False
    if SRC_TEST_MARKER in analysis.canonical_path:
        return False
    if matches_any_glob(analysis.reported_path, globs) is not None:
        return True
    if analysis.canonical_differs:
        return matches_any_glob(analysis.canonical_path, globs) is not None
    return False


@dataclass(frozen=True)
class ScopeDecision:
    """Why a path is or is not in scope, for the tables that have to explain it.

    :func:`in_scope` answers the dataset's question with a boolean; this answers a
    reader's question with the reason.  Both consult the same four rules in the same
    order, and both take their containment verdict from the same
    :func:`analyse_containment` walk, so the two can never disagree.

    Attributes
    ----------
    path:
        The normalised reported spelling -- byte-identical to the emitted ``path``.
    in_scope:
        The verdict, identical to what :func:`in_scope` returns for the same
        arguments.
    matched_glob:
        The first allowlist glob that matched, or ``None``.
    excluded_by_src_test:
        Whether the literal ``src/test`` appeared in *either* spelling.
    excluded_as_non_filesystem:
        Whether ``kind`` is an archive member or an outside-root coordinate.
    kind:
        The path kind the caller resolved.
    excluded_as_escaping_root:
        Whether the running-depth walk found the coordinate leaving the root at some
        segment.  Recorded separately from ``excluded_as_non_filesystem`` because the
        two are different findings about one path: the first is what the *string*
        shows, the second is what the caller's resolver *classified*, and a reader
        chasing a misclassification needs to see which of them fired.
    matched_spelling:
        ``"reported"`` where the reported spelling matched, ``"canonical"`` where
        only the canonical shadow did, ``None`` where nothing matched.  This is the
        provenance for a glob match a first-segment reading would have missed.
    canonical_path:
        The canonical shadow, always recorded so a reader can see for themselves that
        it equals ``path`` for every ordinary path.
    """

    path: str
    in_scope: bool
    matched_glob: str | None
    excluded_by_src_test: bool
    excluded_as_non_filesystem: bool
    kind: str
    excluded_as_escaping_root: bool = False
    matched_spelling: str | None = None
    canonical_path: str | None = None

    def reason(self) -> str:
        """Return a one-line explanation of this decision."""
        if self.excluded_as_non_filesystem:
            return (
                f"out of scope: a {self.kind} coordinate is never in scope, "
                "regardless of any glob it happens to match on its segments"
            )
        if self.excluded_as_escaping_root:
            return (
                "out of scope: the coordinate's running segment depth goes below the "
                "root, so it names a location outside the scanned tree however its "
                "leading segments read"
            )
        if self.excluded_by_src_test:
            return (
                "out of scope: the path contains the literal 'src/test', which "
                "overrides any positive glob match"
            )
        if self.matched_glob is not None:
            if self.matched_spelling == "canonical":
                return (
                    f"in scope: matched the allowlist glob {self.matched_glob!r} on "
                    f"the canonical shadow {self.canonical_path!r}, which the reported "
                    "spelling did not match"
                )
            return f"in scope: matched the allowlist glob {self.matched_glob!r}"
        return "out of scope: the path matches none of the allowlist globs"


def scope_decision(
    path: str,
    globs: Iterable[str],
    *,
    kind: str = PATH_KIND_TREE_FILE,
) -> ScopeDecision:
    """Return the :class:`ScopeDecision` for ``path`` under ``globs``.

    The glob search mirrors :func:`in_scope` exactly: the reported spelling first, the
    canonical shadow only where it is a different string and the reported spelling did
    not match, and which of the two matched is recorded rather than left implicit.
    """
    if kind not in _PATH_KIND_SET:
        raise PathPolicyError(
            f"unknown path kind {kind!r}; the closed set is {', '.join(PATH_KINDS)}"
        )
    analysis = analyse_containment(path)
    normalised = analysis.reported_path
    non_filesystem = is_non_filesystem_kind(kind)
    src_test = (
        SRC_TEST_MARKER in normalised or SRC_TEST_MARKER in analysis.canonical_path
    )
    matched: str | None = None
    matched_spelling: str | None = None
    if not non_filesystem and not analysis.escapes_root:
        matched = matches_any_glob(normalised, globs)
        if matched is not None:
            matched_spelling = "reported"
        elif analysis.canonical_differs:
            matched = matches_any_glob(analysis.canonical_path, globs)
            if matched is not None:
                matched_spelling = "canonical"
    return ScopeDecision(
        path=normalised,
        in_scope=(
            (not non_filesystem)
            and (not analysis.escapes_root)
            and (not src_test)
            and matched is not None
        ),
        matched_glob=matched,
        excluded_by_src_test=src_test,
        excluded_as_non_filesystem=non_filesystem,
        kind=kind,
        excluded_as_escaping_root=analysis.escapes_root,
        matched_spelling=matched_spelling,
        canonical_path=analysis.canonical_path,
    )



# --------------------------------------------------------------------------- #
# Loaders -- provided, and never called implicitly
# --------------------------------------------------------------------------- #


def load_allowlist(path: str | os.PathLike[str]) -> tuple[str, ...]:
    """Read ``harness/scope/allowlist.txt`` and return its globs in file order.

    One glob per line.  Blank lines and ``#`` comments are skipped -- the provisioned
    ``harness/lib/scope.sh`` skips ``''|\\#*``, and this loader uses the more
    tolerant strip-then-test reading, which is a superset of that and of the
    ``grep -vE '^[[:space:]]*(#|$)'`` form -- and **no other character is stripped**
    from a glob beyond its line terminator.  Order is preserved, because the
    allowlist's order is the request's order and :func:`matches_any_glob` reports the
    first match.

    The globs are neither rewritten, extended nor narrowed here (AAP 0.8.2).

    Raises
    ------
    PathPolicyError
        If the file yields no globs at all, which would silently put every row out
        of scope -- the failure mode that looks exactly like a clean scan.
    OSError
        Propagated unchanged where the file cannot be read; a caller that cannot
        read the allowlist has a configuration fault, not a scope of zero.
    """
    text = Path(path).read_text(encoding="utf-8")
    globs: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        # splitlines() has already removed the terminator; nothing else is stripped.
        globs.append(line)
    if not globs:
        raise PathPolicyError(
            f"the allowlist at {os.fspath(path)!r} yielded no globs; an empty scope "
            "definition would put every row out of scope, which is indistinguishable "
            "from a clean scan"
        )
    return tuple(globs)


def allowlist_matches_authoritative_globs(globs: Sequence[str]) -> bool:
    """Return whether ``globs`` is exactly :data:`ALLOWLIST_GLOBS`, in order.

    The independent check on the operative copy: the file is the authority, and this
    is how a caller confirms the file still holds the twelve globs the request names,
    byte-exact and in order, without this module ever substituting its own copy for
    the file's.
    """
    return tuple(globs) == ALLOWLIST_GLOBS


def scope_glob_bases(globs: Iterable[str]) -> tuple[str, ...]:
    """Return each glob with exactly one trailing ``/**`` removed.

    Arithmetic on the allowlist, never an extension of it -- the same operation the
    provisioned ``harness/lib/scope.sh`` performs with ``base="${glob%/\\*\\*}"``.  A
    glob not ending in ``/**`` is returned unchanged.  These are *patterns*: nothing
    here touches the filesystem.
    """
    return tuple(
        candidate[: -len(GLOB_RECURSIVE_SUFFIX)]
        if candidate.endswith(GLOB_RECURSIVE_SUFFIX)
        else candidate
        for candidate in globs
    )


def expand_scope_directories(
    root: str | os.PathLike[str],
    globs: Iterable[str],
) -> tuple[str, ...]:
    """Return the existing directories the globs name under ``root``, sorted, unique.

    Evidence only, and the one function here that touches a filesystem -- ``root`` is
    an argument, so nothing is read at import time.  It mirrors ``scope_dirs`` in the
    provisioned ``harness/lib/scope.sh``: strip the trailing ``/**``, expand with
    zero-or-more-directory semantics, keep only directories, and skip any path
    containing the literal ``src/test``.

    ``glob.glob(recursive=True)`` is used *here and only here*, because it has the
    correct ``**`` semantics against a real filesystem while being unable to test a
    reported path string at all.  At the pin this returns exactly
    :data:`PINNED_EXPANSION_DIRECTORIES` (18) directories covering
    :data:`PINNED_EXPANSION_FILES` (4,095) files.
    """
    root_path = os.fspath(root)
    found: set[str] = set()
    for base in scope_glob_bases(globs):
        pattern = os.path.join(root_path, base)
        for candidate in _glob.glob(pattern, recursive=True):
            if not os.path.isdir(candidate):
                continue
            relative = os.path.relpath(candidate, root_path).replace(os.sep, "/")
            if SRC_TEST_MARKER in relative:
                continue
            found.add(relative)
    return tuple(sorted(found))


# --------------------------------------------------------------------------- #
# The pre-parse lexical gate on an untrusted JSON document                    #
# (CWE-400, CWE-674, CWE-770)                                                 #
#                                                                             #
# ONE IMPLEMENTATION, ON EVERY ROUTE.  This gate lives here rather than in    #
# the caller because the two things it must sit between -- the text and       #
# ``json.loads`` -- meet in THIS module for ``runner-metadata.json``:         #
# :func:`load_runner_metadata` owns that read and that parse, and it is       #
# exported, so a caller reaching it directly gets the same protection the     #
# composed pipeline gets.  ``cli`` needs the identical scan for a scanner     #
# artifact, and it consumes these functions rather than restating them: two   #
# implementations of one bound is the defect where the last one silently      #
# wins and no reader can see which is in force.                              #
#                                                                             #
# One regular expression finds the next token; the string scanner below finds  #
# the end of a string. Both are deliberate:                                    #
#                                                                             #
#  * The token pattern matches ONE token and never spans a string, so it       #
#    carries no unbounded quantifier over an alternation and cannot backtrack  #
#    superlinearly (CWE-1333). Driving it with .search(text, position) skips   #
#    whitespace and anything else uninteresting at C speed, so the Python-level#
#    loop runs once per token rather than once per character -- 0.51s over the #
#    73,840,948-byte opengrep.sarif, against 0.27s for json.loads on the same  #
#    file.                                                                    #
#  * The string scanner is index arithmetic over str.find rather than a regex, #
#    because the two obvious regex spellings of a JSON string are both         #
#    QUADRATIC on an unterminated one. Measured on this interpreter: both      #
#    '"(?:[^"\\]|\\.)*"' and the unrolled '"[^"\\]*(?:\\.[^"\\]*)*"' run for   #
#    over 300 seconds on '"' followed by 200,000 '\"' sequences, which is a    #
#    denial of service inside the guard that exists to prevent one. The scan   #
#    below is linear on that input (0.03s) because each str.find starts where  #
#    the previous one stopped, so the scans partition the text.                #
#  * NO TOKEN IS EVER MATERIALISED. The scan works on match SPANS and single   #
#    characters: end() minus start() is a token's length without a copy, so a  #
#    byte-cap-sized numeric or bare-word token is refused on its span before    #
#    anything the size of the document is allocated. A copy taken to measure a  #
#    token is the amplification the narrow bound exists to stop.                #
#                                                                             #
# The scan does not adjudicate well-formedness. It is lenient in exactly the   #
# direction that is safe: where its lexing differs from json's, it counts MORE #
# than json will parse (an unknown character is skipped, a bare word is a      #
# value, a region json rejects as a string is still bounded by its quotes), so #
# an accepted document is never one whose real node count is higher. The       #
# invalid-JSON verdict stays with json.loads, which is the only thing in this  #
# package that has ever produced it.                                          #
# --------------------------------------------------------------------------- #

#: The most digits an untrusted numeric literal may carry before it is refused as a
#: number, and the single source of truth for it: ``cli.STATUS_NUMERIC_DIGIT_LIMIT`` is
#: assigned from this name rather than restating the number, for the reason
#: :data:`METADATA_BYTE_LIMIT` states -- two bindings of one cap is the defect where the
#: last binding wins and no reader can see which is in force (CWE-732 by analogy).
#:
#: CPython raises ``ValueError`` from ``int()`` above 4,300 digits (the
#: ``sys.set_int_max_str_digits`` limit), which ``str.isdigit()`` does not predict, so a
#: guard that tests only for digits and then converts is a ``ValueError`` waiting for a
#: hostile document.  This is the package's ONE digit bound and it has three enforcement
#: points sharing this single number: a ``<tool>.status`` field, through the
#: digit-bounded predicate in ``cli`` that reads it; every numeric literal in an
#: untrusted artifact, through ``cli``'s call to :func:`validate_document_bounds`; and
#: every numeric literal in ``runner-metadata.json``, through this module's own call to
#: it inside :func:`load_runner_metadata`.  The longest literal any committed JSON input
#: carries is 9 digits (``runner-metadata.json``); 7 is the artifact maximum, and 11 is
#: the longest in any committed status file.
STATUS_NUMERIC_DIGIT_LIMIT: Final[int] = 64

#: The bound names :func:`validate_document_bounds` can raise, as published strings rather
#: than as literals restated at each raise site.  ``cli`` maps each to the halt reason its
#: own closed vocabulary carries for that bound, and asserts the mapping is total against
#: :data:`DOCUMENT_BOUNDS`, so a bound added here cannot reach a caller with no halt name.
DOCUMENT_BOUND_DEPTH: Final[str] = "nesting depth"
DOCUMENT_BOUND_NODES: Final[str] = "node count"
DOCUMENT_BOUND_STRING: Final[str] = "longest string"
DOCUMENT_BOUND_MEMBER_NAME: Final[str] = "longest object member name"
DOCUMENT_BOUND_NUMERIC_DIGITS: Final[str] = "numeric literal digit count"
DOCUMENT_BOUND_LITERAL_TOKEN: Final[str] = "bare literal token length"

#: Every bound name above, in the order the scan can reach them.  The tuple exists so a
#: consumer can be checked total against it rather than against a list it restated.
DOCUMENT_BOUNDS: Final[tuple[str, ...]] = (
    DOCUMENT_BOUND_DEPTH,
    DOCUMENT_BOUND_NODES,
    DOCUMENT_BOUND_STRING,
    DOCUMENT_BOUND_MEMBER_NAME,
    DOCUMENT_BOUND_NUMERIC_DIGITS,
    DOCUMENT_BOUND_LITERAL_TOKEN,
)


class DocumentBoundExceeded(Exception):
    """One structural cap crossed by an untrusted JSON document.

    Raised by :func:`validate_document_bounds` over a document's *text*, before it is
    parsed, and by ``cli._measure_document`` over the parsed document afterwards.  Both
    are pure measurements with no tool, no artifact path and no business composing a
    halt: the caller adds that context and converts this into its own named halt -- the
    same division of labour as :class:`RunnerMetadataError`'s own raise sites and as
    ``shape.UnknownArtifactShape``.

    ``bound`` is one of :data:`DOCUMENT_BOUNDS`, so a caller can map it to the halt
    reason its vocabulary carries without parsing the message.  ``limit`` is the cap that
    was in force and ``observed`` the value that crossed it, both of which a record must
    carry for the refusal to be actionable rather than merely categorical.
    """

    def __init__(self, bound: str, limit: int, observed: int) -> None:
        super().__init__(
            f"the artifact's {bound} is {observed}, above the cap of {limit}"
        )
        self.bound = bound
        self.limit = limit
        self.observed = observed

    def details(self) -> dict[str, Any]:
        """Return the bound as halt details: which cap, its value, and what was observed."""
        return {
            "bound": self.bound,
            "bound_cap": self.limit,
            "bound_observed": self.observed,
        }


#: One JSON token, or the opening quote of a string.  A string's *extent* is found by
#: :func:`string_token_end` rather than by this pattern, for the reason above; the ``"``
#: branch matches the delimiter alone.  ``[A-Za-z]+`` covers ``true``, ``false`` and
#: ``null`` -- and any other bare word, which is a value for counting purposes and a syntax
#: error for ``json.loads`` to name.
_JSON_TOKEN: Final[re.Pattern[str]] = re.compile(
    r'"|[{}\[\],:]|-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?|[A-Za-z]+'
)

#: What the innermost open container is, and for an object which half of a member it is
#: expecting.  The distinction is what keeps an object's member NAME out of the node count:
#: a name is length-checked like any other string and is not itself a node, which is the
#: definition ``cli._measure_document`` and ``cli.INGESTION_BOUNDS`` both use.
_CONTAINER_ARRAY: Final[int] = 0
_CONTAINER_OBJECT_KEY: Final[int] = 1
_CONTAINER_OBJECT_VALUE: Final[int] = 2

#: The keys :func:`validate_document_bounds` returns for the three quantities a walk over
#: the *parsed* document can also measure, so a caller comparing the two verdicts does not
#: have to restate them.
STRUCTURAL_MEASUREMENT_KEYS: Final[tuple[str, ...]] = (
    "depth",
    "nodes",
    "longest_string",
)


def string_token_end(text: str, opening: int) -> int:
    """Return the index just past the string opening at ``opening``, or ``-1``.

    ``text[opening]`` is the opening quote.  A quote ends the string unless it is escaped,
    and JSON's escape rule is positional rather than character-class based: a backslash
    escapes whatever follows it, so ``\\\\"`` ends a string and ``\\"`` does not.  The scan
    therefore looks for the next quote and then asks whether a backslash between here and
    there claims it.

    LINEAR BY CONSTRUCTION
    ----------------------
    Every ``str.find`` starts at ``position``, which only ever moves forward, so the
    regions the two searches examine partition the string rather than overlapping it.  A
    hostile string of nothing but escapes costs one iteration per escape and no rescanning
    -- 0.08s for a run of a million backslashes -- where the regex spellings this replaces
    are quadratic on the same input.

    Returns:
        The index one past the closing quote, or ``-1`` where the string is unterminated.
        An unterminated string is not this function's verdict to give: it stops the scan
        and leaves ``json.loads`` to raise the ``JSONDecodeError`` that names it.
    """
    position = opening + 1
    quote = text.find('"', position)
    while quote >= 0:
        backslash = text.find("\\", position, quote)
        if backslash < 0:
            # No escape between here and that quote, so that quote closes the string.
            return quote + 1
        # The backslash consumes the character after it, which may be the quote itself.
        position = backslash + 2
        if position > quote:
            quote = text.find('"', position)
    return -1


def _numeric_literal_digits(text: str, start: int, end: int) -> int:
    """Return how many digits the numeric token at ``text[start:end]`` carries.

    WITHOUT MATERIALISING THE TOKEN.  The token pattern admits a leading ``-``, at most
    one ``.``, at most one ``e``/``E`` and at most one exponent sign, and digits
    everywhere else -- so the digit count is the span minus however many of those four
    characters are present, and each is located with a bounded ``str.find`` or a
    single-character index rather than by copying the token and counting over it.  Asking
    the match object for its matched text here would allocate a second copy of a token
    that can be as long as the whole document, which is exactly the amplification the
    digit cap exists to refuse (CWE-400, CWE-770).

    Every digit in the literal counts, which is stricter than CPython's own limit on the
    integer part alone -- the conservative direction.
    """
    digits = end - start
    if text[start] == "-":
        digits -= 1
    if text.find(".", start, end) >= 0:
        digits -= 1
    exponent = text.find("e", start, end)
    if exponent < 0:
        exponent = text.find("E", start, end)
    if exponent >= 0:
        digits -= 1
        # The pattern requires at least one digit after the exponent, so this index is
        # inside the token whether or not a sign is present.
        if text[exponent + 1] in "+-":
            digits -= 1
    return digits


def validate_document_bounds(
    text: str,
    *,
    depth_limit: int,
    node_limit: int,
    string_limit: int,
    digit_limit: int,
) -> dict[str, int]:
    """Bound an untrusted JSON document's structure BEFORE it is parsed.

    This is the gate a byte cap cannot be: a byte cap bounds the file, and the object
    graph a file of that size encodes is bounded by nothing at all -- a compact document
    well inside the cap can carry tens of millions of values, and ``json.loads``
    allocates every one of them.  A depth, node or string cap applied to the *parsed*
    document therefore names an exhaustion that has already happened; catching
    ``MemoryError`` afterwards is a diagnosis rather than a limit (CWE-400, CWE-770).

    So all four caps are enforced here, over the decoded text, in one left-to-right token
    scan that never materialises a value:

    * **nesting depth** on an explicit integer -- ``len(stack) + 1`` for the value about to
      be counted -- so nothing recurses and no document is deep enough to overflow the
      stack while being measured (CWE-674);
    * **node count** on the same definition ``cli._measure_document`` uses: every JSON
      value is one node, and an object member name is length-checked but is not a node;
    * **string length** on the RAW token, escapes included.  A raw token is never shorter
      than the string it decodes to (``\\uD83D\\uDE00`` is twelve characters of text and one
      of value), so this bound is conservative in the safe direction and the two walks'
      figures relate by ``decoded <= raw`` rather than by equality;
    * **literal token length**, on the match SPAN and before any copy of the token exists:
      a numeric literal is refused on its digit count -- so a 4,300-digit integer, valid
      JSON and an ``int()`` CPython refuses, is refused before the conversion is reached --
      and a bare word is refused on its span, since ``[A-Za-z]+`` can match as far as the
      byte cap allows and the longest legal one is five characters.

    Well-formedness is deliberately out of scope: see the comment block above.  A document
    this function accepts may still be invalid JSON, and ``json.loads`` remains the only
    thing that decides that.

    Args:
        text: The decoded document.  Nothing is read or written; the caller owns the file.
        depth_limit: Deepest nesting permitted.  ``cli.JSON_DEPTH_LIMIT`` for an artifact,
            :data:`METADATA_DEPTH_LIMIT` for the runner metadata.
        node_limit: Most JSON values permitted.
        string_limit: Longest raw string token permitted, member names included.
        digit_limit: Most digits a single numeric literal may carry, and the span a bare
            literal token may reach.  :data:`STATUS_NUMERIC_DIGIT_LIMIT` on every route.

    Returns:
        ``depth``, ``nodes`` and ``longest_string`` -- the same three keys
        ``cli._measure_document`` returns, so the two verdicts can be compared directly --
        plus ``numeric_literal_digits``, which only this walk can measure because the parsed
        document no longer carries the literal.

    Raises:
        DocumentBoundExceeded: A cap was crossed.  Raised at the crossing, so a document
            already refused is not scanned to the end, and naming the bound from
            :data:`DOCUMENT_BOUNDS` so the caller can map it to its own halt reason.
    """
    nodes = 0
    max_depth = 0
    longest = 0
    max_digits = 0
    # One entry per open container, so this walk's own memory is bounded by the depth cap
    # rather than by the document's breadth -- the same property _measure_document has.
    stack: list[int] = []
    position = 0
    end_of_text = len(text)
    search = _JSON_TOKEN.search

    while position < end_of_text:
        match = search(text, position)
        if match is None:
            # No token in the remainder: whitespace, or characters no JSON token begins
            # with. Either way there is nothing left to count.
            break
        # The token is identified by its lead character and measured by its SPAN. Nothing
        # here copies it: asking the match object for its matched text, on a token that can
        # be as long as the document, is the allocation the caps below exist to prevent.
        token_start = match.start()
        token_end = match.end()
        lead = text[token_start]
        position = token_end
        raw_length = 0

        if lead == '"':
            string_end = string_token_end(text, token_start)
            if string_end < 0:
                # Unterminated. json.loads will refuse the document; this walk stops here
                # rather than lexing the remainder of a string as structure.
                break
            position = string_end
            # The delimiters are not part of the string, and the escapes inside it are
            # measured as written: see the string-length note in this docstring.
            raw_length = string_end - token_start - 2
            if stack and stack[-1] == _CONTAINER_OBJECT_KEY:
                # A member name: bounded like any other string, and not a node.
                if raw_length > longest:
                    longest = raw_length
                    if longest > string_limit:
                        raise DocumentBoundExceeded(
                            DOCUMENT_BOUND_MEMBER_NAME, string_limit, longest
                        )
                continue
        elif lead == "}" or lead == "]":
            if stack:
                stack.pop()
            continue
        elif lead == ",":
            if stack and stack[-1] != _CONTAINER_ARRAY:
                stack[-1] = _CONTAINER_OBJECT_KEY
            continue
        elif lead == ":":
            if stack and stack[-1] != _CONTAINER_ARRAY:
                stack[-1] = _CONTAINER_OBJECT_VALUE
            continue

        # Everything reaching here is a value: a string in value position, a container, a
        # number, or a literal word. The enclosing object has now had its value, so the
        # next string it carries is a member name again.
        if stack and stack[-1] == _CONTAINER_OBJECT_VALUE:
            stack[-1] = _CONTAINER_OBJECT_KEY

        nodes += 1
        if nodes > node_limit:
            raise DocumentBoundExceeded(DOCUMENT_BOUND_NODES, node_limit, nodes)
        depth = len(stack) + 1
        if depth > max_depth:
            max_depth = depth
            if max_depth > depth_limit:
                raise DocumentBoundExceeded(
                    DOCUMENT_BOUND_DEPTH, depth_limit, max_depth
                )

        if lead == '"':
            if raw_length > longest:
                longest = raw_length
                if longest > string_limit:
                    raise DocumentBoundExceeded(
                        DOCUMENT_BOUND_STRING, string_limit, longest
                    )
        elif lead == "{":
            stack.append(_CONTAINER_OBJECT_KEY)
        elif lead == "[":
            stack.append(_CONTAINER_ARRAY)
        elif lead == "-" or lead.isdigit():
            # The span bounds the digit count from above and below -- the pattern admits at
            # most four non-digit characters -- so a token whose span alone puts it past the
            # cap is refused here, and the exact count is computed by index arithmetic
            # rather than by copying the token (see _numeric_literal_digits).
            digits = _numeric_literal_digits(text, token_start, token_end)
            if digits > max_digits:
                max_digits = digits
                if max_digits > digit_limit:
                    raise DocumentBoundExceeded(
                        DOCUMENT_BOUND_NUMERIC_DIGITS, digit_limit, max_digits
                    )
        else:
            # A bare word: ``true``, ``false``, ``null`` -- or a run of letters as long as
            # the pattern can reach, which json.loads will refuse as syntax but which this
            # scan must not copy in order to say so. Its SPAN is the whole measurement, and
            # the digit cap is the bound: the longest legal bare word is five characters, so
            # 64 is already three orders of magnitude of headroom.
            span = token_end - token_start
            if span > digit_limit:
                raise DocumentBoundExceeded(
                    DOCUMENT_BOUND_LITERAL_TOKEN, digit_limit, span
                )

    return {
        "depth": max_depth,
        "nodes": nodes,
        "longest_string": longest,
        "numeric_literal_digits": max_digits,
    }


#: The byte cap on the runner-metadata document, and the single source of truth for it:
#: ``cli.RUNNER_METADATA_BYTE_LIMIT`` is assigned from this name rather than restating the
#: number, because two bindings of one cap is exactly the defect that let a 0o666 file mode
#: override a 0o644 one in `emit.py` (CWE-732) -- the last binding wins and no reader can
#: see which one is in force.  The committed metadata is 117,676 bytes, so 64 MiB is a
#: factor of 570; a document that size is not a longer account of nine runners.
METADATA_BYTE_LIMIT: Final[int] = 64 * 1024 * 1024

#: Nesting depth the metadata document may reach.  Measured: the committed document reaches
#: 7, so 32 is a factor of 4.5.  Bounded because a document nested past the interpreter's
#: own limit turns a parse into a ``RecursionError`` (CWE-674), and one nested just inside
#: it turns every later traversal into a stack risk this module cannot recover from.
METADATA_DEPTH_LIMIT: Final[int] = 32

#: JSON values the metadata document may hold.  Measured: the committed document holds
#: 1,315, so 200,000 is a factor of 152.  Bounded so a document inside the byte cap cannot
#: still allocate an unbounded number of small objects (CWE-400, CWE-770).
METADATA_NODE_LIMIT: Final[int] = 200_000

#: Characters any single string -- value or member name -- in the metadata document may
#: carry.  Measured: the longest in the committed document is 1,421, so 1 MiB is a factor
#: of 737.  Bounded for the same reason as the node cap, in the other dimension.
METADATA_STRING_LIMIT: Final[int] = 1024 * 1024


def _metadata_shape_violation(document: Any) -> str | None:
    """Return why ``document`` exceeds a shape cap, or ``None`` where it does not.

    ITERATIVE, NEVER RECURSIVE (CWE-674)
    ------------------------------------
    The walk keeps its own explicit stack, so measuring a deeply nested document cannot
    itself exhaust the interpreter's stack -- a recursive check would raise the very error
    it exists to prevent, at a depth it could not then report.

    The three caps are checked in one pass because they are three dimensions of the same
    hazard: :data:`METADATA_DEPTH_LIMIT` on nesting, :data:`METADATA_NODE_LIMIT` on how
    many values the document holds, and :data:`METADATA_STRING_LIMIT` on any single string,
    a member name included.  The first crossing is returned with the observed value and its
    cap, so the caller's diagnostic names a measurement rather than a category.
    """
    nodes = 0
    stack: list[tuple[Any, int]] = [(document, 1)]
    while stack:
        value, depth = stack.pop()
        nodes += 1
        if depth > METADATA_DEPTH_LIMIT:
            return (
                f"it nests to at least {depth} levels, above the "
                f"{METADATA_DEPTH_LIMIT}-level cap on this input"
            )
        if nodes > METADATA_NODE_LIMIT:
            return (
                f"it holds more than {METADATA_NODE_LIMIT} JSON values, above the cap on "
                "this input"
            )
        if isinstance(value, Mapping):
            for name, member in value.items():
                if isinstance(name, str) and len(name) > METADATA_STRING_LIMIT:
                    return (
                        f"it carries a member name of {len(name)} characters, above the "
                        f"{METADATA_STRING_LIMIT}-character cap on this input"
                    )
                stack.append((member, depth + 1))
        elif isinstance(value, (list, tuple)):
            for member in value:
                stack.append((member, depth + 1))
        elif isinstance(value, str) and len(value) > METADATA_STRING_LIMIT:
            return (
                f"it carries a string of {len(value)} characters, above the "
                f"{METADATA_STRING_LIMIT}-character cap on this input"
            )
    return None


def _metadata_bound_violation(error: DocumentBoundExceeded) -> str:
    """Return the shape sentence for a bound the PRE-parse scan crossed.

    One document must not be adjudicated two ways, so each bound is worded exactly as
    :func:`_metadata_shape_violation` words the same bound over the parsed document: the
    two walks are the same policy at two moments, and a reader comparing a refusal from
    either must not have to decide whether two sentences mean one thing.  The two bounds
    only the text can carry -- a numeric literal's digits and a bare literal token's span
    -- take the same shape of sentence.
    """
    if error.bound == DOCUMENT_BOUND_DEPTH:
        return (
            f"it nests to at least {error.observed} levels, above the "
            f"{error.limit}-level cap on this input"
        )
    if error.bound == DOCUMENT_BOUND_NODES:
        return (
            f"it holds more than {error.limit} JSON values, above the cap on "
            "this input"
        )
    if error.bound == DOCUMENT_BOUND_MEMBER_NAME:
        return (
            f"it carries a member name of {error.observed} characters, above the "
            f"{error.limit}-character cap on this input"
        )
    if error.bound == DOCUMENT_BOUND_STRING:
        return (
            f"it carries a string of {error.observed} characters, above the "
            f"{error.limit}-character cap on this input"
        )
    if error.bound == DOCUMENT_BOUND_NUMERIC_DIGITS:
        return (
            f"it carries a numeric literal of {error.observed} digits, above the "
            f"{error.limit}-digit cap on this input"
        )
    return (
        f"it carries a bare literal token of {error.observed} characters, above the "
        f"{error.limit}-character cap on this input"
    )


def _validate_metadata_text(text: str, location: str, observed_bytes: int) -> None:
    """Enforce the shape caps on metadata TEXT, before anything parses it.

    THE GATE LIVES WHERE THE PARSE LIVES (CWE-400, CWE-674, CWE-770)
    ----------------------------------------------------------------
    :func:`_parse_and_shape_check_metadata` calls this as its first step, and every route
    into this module's metadata parse goes through that function -- the bounded read and
    the caller-supplied ``validated_text`` alike.  So the caps are enforced on the exact
    text ``json.loads`` is about to be handed, on every route, including a direct call
    from a test or a later consumer.  A post-parse check can only diagnose an amplified
    document after the object graph exists; this is the bound.

    ``validated_text`` therefore saves a second READ and never a check: a caller's label
    is not evidence, and a document handed over as validated is scanned here exactly as a
    document read here is.

    Raises:
        RunnerMetadataError: a cap was crossed, under the ``shape`` stage this module has
            always used for a shape refusal and worded as :func:`_metadata_shape_violation`
            words the same bound.  The halt vocabulary does not grow for the moment at
            which the same policy was applied.
    """
    try:
        validate_document_bounds(
            text,
            depth_limit=METADATA_DEPTH_LIMIT,
            node_limit=METADATA_NODE_LIMIT,
            string_limit=METADATA_STRING_LIMIT,
            digit_limit=STATUS_NUMERIC_DIGIT_LIMIT,
        )
    except DocumentBoundExceeded as error:
        violation = _metadata_bound_violation(error)
        raise RunnerMetadataError(
            f"runner metadata at {location!r} was refused on shape rather than on size: "
            f"{violation}. The committed document reaches depth 7, holds 1,315 values and "
            "carries no string longer than 1,421 characters",
            details={
                "stage": "shape",
                "violation": violation,
                "depth_cap": METADATA_DEPTH_LIMIT,
                "node_cap": METADATA_NODE_LIMIT,
                "string_cap": METADATA_STRING_LIMIT,
                "digit_cap": STATUS_NUMERIC_DIGIT_LIMIT,
                "observed_bytes": observed_bytes,
                "enforced_before_the_parse": True,
                **error.details(),
            },
        ) from error
    except MemoryError as error:
        # The scan holds one entry per open container and materialises no token, so this
        # is the host's available memory rather than a walk that grows with the document.
        raise RunnerMetadataError(
            f"runner metadata at {location!r} exhausted the host's memory while its shape "
            f"was being measured, at {observed_bytes} bytes: {type(error).__name__}",
            details={
                "stage": "shape",
                "error": type(error).__name__,
                "observed_bytes": observed_bytes,
            },
        ) from error


def load_runner_metadata(
    path: str | os.PathLike[str],
    *,
    validated_text: str | None = None,
) -> Mapping[str, Any]:
    """Read ``harness/artifacts/logs/runner-metadata.json`` and return it read-only.

    The direction is fixed and must not be inverted (AAP 0.6.4): Stage 1 writes
    ``runner-metadata.json``, the normalizer reads it as **input**, and
    ``oss-scan-results/tool-status.md`` is rendered from it *afterwards*.  This
    module never reads ``tool-status.md`` -- *"The Markdown is an output of the
    pipeline, never an input to it."*

    The document's own ``purpose`` field names this consumer, and its content is
    fixed by AAP 0.6.1: per canonical tool identifier, the script classification, the
    scan-target variable and the value set into it, the resolved scan root, the
    invocation form and working directory, the path base, the JDK major, the
    interpreter path and version, the baked flags and the credential expression.

    BOUNDED ON SIZE *AND* ON SHAPE, BEFORE THE PARSE (CWE-400, CWE-674, CWE-770)
    ----------------------------------------------------------------------------
    This document is a file the module did not write, so it is read under the same
    discipline as a scanner artifact rather than trusted for being Stage 1's own output.
    The read is bounded to the cap plus one byte; :func:`_validate_metadata_text` then
    enforces the depth, node, string and literal caps on the TEXT, so an amplified
    document is refused before ``json.loads`` allocates its object graph rather than
    diagnosed afterwards; the parse's ``RecursionError``, ``MemoryError`` and
    ``ValueError`` become this module's own error rather than escaping as a traceback; and
    :func:`_metadata_shape_violation` measures the parsed document as an independent
    second opinion before it is handed back to a resolver.

    ONE GATE, EVERY ROUTE -- ``validated_text`` SAVES A READ, NEVER A CHECK (CWE-367)
    --------------------------------------------------------------------------------
    The lexical gate lives in :func:`_parse_and_shape_check_metadata`, which is the one
    place this module parses this document, so both routes into the parse pass through it:
    the bounded read above, and the ``validated_text`` a composed caller supplies.  A
    caller's label is not evidence, so handed-over text is scanned here exactly as text
    read here is -- and a direct caller that omits the parameter gets the identical
    protection rather than the unbounded parse this defect used to leave it (F12).

    What ``validated_text`` still buys is the second READ: ``cli._load_metadata`` reads the
    file under the same bounded reader, validates it, and hands those exact bytes over, so
    the bytes parsed are the bytes that side accepted and a replacement between two reads
    has no window to land in.

    Raises
    ------
    RunnerMetadataError
        If the document cannot be measured or read; if it exceeds the byte cap or any of
        the three shape caps; if it is not valid UTF-8 or not valid JSON; if the parse
        exhausts the stack or the host's memory; if it is not a JSON object; or if it
        carries no ``tools`` object.  In every case a resolver would have no base it could
        trust, and AAP 0.6.1 makes that a hard error rather than a default to the root.
    """
    location = os.fspath(path)
    if validated_text is not None:
        # The caller read these exact bytes, so re-reading here would parse bytes nobody
        # validated (CWE-367): the text is taken as given and its size restated for the
        # diagnostics below. Its STRUCTURE is not taken as given -- the gate inside
        # _parse_and_shape_check_metadata runs on it exactly as it runs on a read of our
        # own, because a caller's label is not a measurement.
        text = validated_text
        observed_bytes = len(text.encode("utf-8"))
        if observed_bytes > METADATA_BYTE_LIMIT:
            raise RunnerMetadataError(
                f"runner metadata at {location!r} is {observed_bytes} bytes, above the "
                f"{METADATA_BYTE_LIMIT}-byte cap on this input, so it was refused "
                "before it was parsed",
                details={
                    "stage": "size",
                    "observed_bytes": observed_bytes,
                    "byte_cap": METADATA_BYTE_LIMIT,
                    "source": "validated_text",
                },
            )
        return _parse_and_shape_check_metadata(text, location, observed_bytes)
    try:
        observed_bytes = os.stat(location).st_size
    except OSError as error:
        raise RunnerMetadataError(
            f"runner metadata at {location!r} cannot be measured: "
            f"{type(error).__name__}: {error}",
            details={"stage": "measure", "error": f"{type(error).__name__}: {error}"},
        ) from error
    if observed_bytes > METADATA_BYTE_LIMIT:
        raise RunnerMetadataError(
            f"runner metadata at {location!r} is {observed_bytes} bytes, above the "
            f"{METADATA_BYTE_LIMIT}-byte cap on this input, so it was refused before any "
            "of it was read",
            details={
                "stage": "size",
                "observed_bytes": observed_bytes,
                "byte_cap": METADATA_BYTE_LIMIT,
            },
        )
    try:
        with open(location, "rb") as handle:
            raw = handle.read(METADATA_BYTE_LIMIT + 1)
    except OSError as error:
        raise RunnerMetadataError(
            f"runner metadata at {location!r} cannot be read: "
            f"{type(error).__name__}: {error}",
            details={"stage": "read", "error": f"{type(error).__name__}: {error}"},
        ) from error
    if len(raw) > METADATA_BYTE_LIMIT:
        raise RunnerMetadataError(
            f"runner metadata at {location!r} grew past the {METADATA_BYTE_LIMIT}-byte cap "
            "between being measured and being read, so what was read is not what was "
            "measured and neither can be relied on",
            details={
                "stage": "read",
                "observed_bytes": observed_bytes,
                "byte_cap": METADATA_BYTE_LIMIT,
            },
        )
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise RunnerMetadataError(
            f"runner metadata at {location!r} is not valid UTF-8: {error}",
            details={"stage": "decode", "error": f"{type(error).__name__}: {error}"},
        ) from error
    return _parse_and_shape_check_metadata(text, location, observed_bytes)


def _parse_and_shape_check_metadata(
    text: str, location: str, observed_bytes: int
) -> Mapping[str, Any]:
    """Validate, parse and shape-check metadata text, for both routes above.

    Shared by the ``validated_text`` route and the standalone read so that one
    document cannot be adjudicated two ways.  Every verdict, message and detail key
    is the one :func:`load_runner_metadata` has always raised.

    THE ORDER IS THE PROPERTY.  The lexical gate runs first, over the text, so no route
    into this parse can reach ``json.loads`` with a document whose depth, node count,
    string length or literal length has not been bounded (F12, CWE-400, CWE-674,
    CWE-770).  Because this is the only function here that parses, placing the gate at its
    head is what makes "every route" a structural fact rather than a convention each
    caller has to remember.  The post-parse walk that follows is then the independent
    second opinion it was always meant to be.
    """
    _validate_metadata_text(text, location, observed_bytes)
    try:
        document = json.loads(text)
    except json.JSONDecodeError as error:
        raise RunnerMetadataError(
            f"runner metadata at {location!r} is not valid JSON: {error}",
            details={"stage": "parse", "parser_error": str(error)},
        ) from error
    except RecursionError as error:
        raise RunnerMetadataError(
            f"runner metadata at {location!r} nests too deeply for the parser and "
            f"exhausted the interpreter's stack: {type(error).__name__}",
            details={
                "stage": "parse",
                "error": type(error).__name__,
                "recursion_limit": sys.getrecursionlimit(),
            },
        ) from error
    except MemoryError as error:
        raise RunnerMetadataError(
            f"runner metadata at {location!r} exhausted the host's memory while being "
            f"parsed, at {observed_bytes} bytes: {type(error).__name__}",
            details={
                "stage": "parse",
                "error": type(error).__name__,
                "observed_bytes": observed_bytes,
            },
        ) from error
    except ValueError as error:
        # A numeric literal too long to convert raises ValueError rather than a
        # JSONDecodeError, so it arrives here and must be named rather than escape.
        raise RunnerMetadataError(
            f"runner metadata at {location!r} carries a literal the parser could not "
            f"convert: {type(error).__name__}: {error}",
            details={"stage": "parse", "error": f"{type(error).__name__}: {error}"},
        ) from error
    violation = _metadata_shape_violation(document)
    if violation is not None:
        raise RunnerMetadataError(
            f"runner metadata at {location!r} was refused on shape rather than on size: "
            f"{violation}. The committed document reaches depth 7, holds 1,315 values and "
            "carries no string longer than 1,421 characters",
            details={
                "stage": "shape",
                "violation": violation,
                "depth_cap": METADATA_DEPTH_LIMIT,
                "node_cap": METADATA_NODE_LIMIT,
                "string_cap": METADATA_STRING_LIMIT,
            },
        )
    if not isinstance(document, Mapping):
        raise RunnerMetadataError(
            f"runner metadata at {location!r} must be a JSON object; observed "
            f"{type(document).__name__}"
        )
    tools = document.get(METADATA_TOOLS_KEY)
    if not isinstance(tools, Mapping) or not tools:
        raise RunnerMetadataError(
            f"runner metadata at {location!r} carries no non-empty {METADATA_TOOLS_KEY!r} "
            "object, so no tool's path base can be established; a resolver must not "
            "default to the scan root"
        )
    return MappingProxyType(dict(document))


# --------------------------------------------------------------------------- #
# Relativization -- parts arithmetic that never cancels a '..'
# --------------------------------------------------------------------------- #

#: The single separator between an archive container and a member (AAP 0.5.4).
ARCHIVE_SEPARATOR: Final[str] = "!"

#: Container extensions the archive-member serialization recognises.  Named so the
#: rule is auditable rather than buried in a condition: a per-record path inside a
#: container of one of these types is serialized as a member of it.
ARCHIVE_EXTENSIONS: Final[tuple[str, ...]] = (
    ".jar",
    ".war",
    ".ear",
    ".zip",
    ".tar",
    ".tgz",
    ".tar.gz",
    ".aar",
    ".nupkg",
    ".whl",
    ".egg",
)

#: URI schemes that address a member inside a container, as ``jar:<inner>!<member>``.
ARCHIVE_URI_SCHEMES: Final[tuple[str, ...]] = ("jar", "zip", "tar")

#: A URI with an explicit authority component.
_URI_AUTHORITY_RE: Final[re.Pattern[str]] = re.compile(r"\A[A-Za-z][A-Za-z0-9+.\-]*://")

#: A URI scheme, with or without an authority: ``file:/x``, ``jar:file:/x!/y``.
_URI_SCHEME_RE: Final[re.Pattern[str]] = re.compile(r"\A([A-Za-z][A-Za-z0-9+.\-]*):")

#: ``C:\x``, ``C:/x`` and a bare ``C:`` are all filesystem-absolute on Windows.
_WINDOWS_DRIVE_RE: Final[re.Pattern[str]] = re.compile(r"\A[A-Za-z]:(?:[\\/]|\Z)")

#: Any C0 or C1 control character, which no valid URI reference may carry.
#:
#: Deliberately *not* the same predicate as :func:`_is_escapable_control`, which
#: exempts tab and newline because this dataset's ``message`` field legitimately
#: carries embedded newlines (AAP 0.5.4) and escaping them would rewrite evidence.
#: A **path** is a different field with a different contract: RFC 3986 admits no C0
#: or C1 control in a URI reference at all, and no path in the scanned tree carries
#: one, so every control is refused here including tab and newline.  Conflating the
#: two predicates would let ``%0a`` through into the ``path`` column of a CSV.
_CONTROL_CHARACTER_RE: Final[re.Pattern[str]] = re.compile(r"[\x00-\x1f\x7f-\x9f]")

#: How many control characters a description names individually before summarising.
#: Small on purpose: the description is a diagnostic, and a hostile value carrying
#: hundreds of controls must not turn one rejection detail into a wall of text.
_CONTROL_DESCRIPTION_LIMIT: Final[int] = 3


def describe_control_characters(value: str) -> str | None:
    """Describe the control characters ``value`` carries, or return ``None``.

    The description names each control by its Unicode code point and its index --
    ``U+001B at index 12`` -- and **never reproduces the character itself**.  That is
    the whole point of having a describer rather than interpolating the value: a
    diagnostic composed from a control-bearing value is written to
    ``harness/artifacts/logs/``, to ``normalize-run.json`` and into the Markdown
    records, and an ESC or a CR reaching any of those is log injection (CWE-117) even
    when the record that carried it was rejected.  ``repr()`` would escape a control
    too, but only incidentally: it escapes for readability rather than for safety, so
    relying on it would make the safety property a side effect of a formatting choice.

    Used for two different guards, and it is the same predicate in both because the
    two are the same question asked at different moments (F16):

    * after every percent-decode in :func:`parse_uri_reference`, because the raw
      check that precedes them cannot see what ``%00``, ``%0d`` or ``%1b`` becomes;
    * immediately before a resolved path is emitted, in
      :func:`assert_relative_path` and in :func:`_emitted_path_or_refusal`, because a
      reported path field can carry a literal control with no URI decoding involved
      at all -- a Gitleaks ``File`` or a Dependency-Check ``filePath`` reaches
      :func:`relativize_to_root` directly.

    Returns
    -------
    str | None
        A sentence beginning ``"carries the control character..."``, or ``None`` where
        the value carries none.  ``None`` is the answer for every path in this
        provisioning's dataset: measured over every row of
        ``oss-scan-results/findings.json``, no ``path`` field carries a control
        character or even a ``%``, so both guards are a defence against a future
        artifact rather than a change to this one.  No row count is quoted here on
        purpose -- the dataset is regenerated, so a figure in this docstring would be
        stale by construction, and the measurement is a property of the paths rather
        than of any one generation's size.
    """
    if not isinstance(value, str):
        return None
    found = [
        (index, char)
        for index, char in enumerate(value)
        if _CONTROL_CHARACTER_RE.match(char)
    ]
    if not found:
        return None
    named = ", ".join(
        f"U+{ord(char):04X} at index {index}"
        for index, char in found[:_CONTROL_DESCRIPTION_LIMIT]
    )
    if len(found) > _CONTROL_DESCRIPTION_LIMIT:
        return (
            f"carries {len(found)} control characters, the first "
            f"{_CONTROL_DESCRIPTION_LIMIT} being {named}"
        )
    if len(found) == 1:
        return f"carries the control character {named}"
    return f"carries {len(found)} control characters: {named}"


def split_segments(value: str) -> tuple[str, ...]:
    """Return ``value``'s non-empty segments after :func:`normalise_reported_path`."""
    return tuple(
        segment for segment in normalise_reported_path(value).split("/") if segment
    )


def posix_join(base: str, relative: str) -> str:
    """Join ``relative`` onto ``base`` by concatenating segments, cancelling nothing.

    ``os.path.join`` would be acceptable, but ``os.path.normpath`` -- the thing a
    caller reaches for next -- collapses ``..``, which the SARIF 2.1.0 errata forbid.
    Concatenating segments keeps every ``..`` the input carried, so a coordinate that
    genuinely points outside its base still says so after the join.

    An absolute ``relative`` is returned unchanged: a base cannot prefix an absolute
    reference, and silently prefixing one is how a row acquires a path naming a place
    that does not exist.
    """
    normalised_relative = normalise_reported_path(relative)
    if not normalised_relative:
        return normalise_reported_path(base)
    if normalised_relative.startswith("/") or _WINDOWS_DRIVE_RE.match(normalised_relative):
        return normalised_relative
    normalised_base = normalise_reported_path(base)
    if not normalised_base:
        return normalised_relative
    leading = "/" if normalised_base.startswith("/") else ""
    return leading + "/".join(
        (*split_segments(normalised_base), *split_segments(normalised_relative))
    )


def is_absolute_path(value: str) -> bool:
    """Return whether ``value`` is a filesystem-absolute path under either flavour.

    A leading ``/`` counts, as does a Windows drive or UNC prefix.  A URI is *not* a
    path and is handled by :func:`parse_uri_reference` instead.
    """
    normalised = normalise_reported_path(value)
    if not normalised:
        return False
    if _WINDOWS_DRIVE_RE.match(normalised):
        return True
    return PurePosixPath(normalised).is_absolute()


def relativize_to_root(candidate: str, root: str) -> str:
    """Express ``candidate`` relative to ``root``, preserving every ``..`` it carries.

    A relative ``candidate`` is already root-relative by the caller's contract and is
    returned normalised but otherwise untouched.  An absolute one is expressed by
    parts arithmetic: walk the common leading segments, then emit one ``..`` for each
    remaining root segment followed by the candidate's remainder.  Introducing
    ``../`` prefixes for a location genuinely outside the root is required and
    correct (AAP 0.5.4); cancelling a ``..`` the input already carried is forbidden,
    and this never does, because a remainder segment is copied rather than combined
    with the segment before it.

    ``candidate == root`` yields ``"."``, the only way to name the root itself
    without emitting an absolute path.

    Raises
    ------
    PathPolicyError
        If ``root`` is not an absolute path -- a relative root cannot anchor
        anything, and accepting one would produce a plausible-looking wrong answer
        for every row.
    """
    if not is_absolute_path(root):
        raise PathPolicyError(
            f"the scan root must be an absolute path to relativize against; observed "
            f"{root!r}"
        )
    normalised_candidate = normalise_reported_path(candidate)
    if not normalised_candidate:
        raise PathPolicyError("cannot relativize an empty path")
    if not is_absolute_path(normalised_candidate):
        return normalised_candidate

    root_segments = split_segments(root)
    candidate_segments = split_segments(normalised_candidate)
    common = 0
    limit = min(len(root_segments), len(candidate_segments))
    while common < limit and root_segments[common] == candidate_segments[common]:
        common += 1
    upwards = ("..",) * (len(root_segments) - common)
    remainder = candidate_segments[common:]
    parts = (*upwards, *remainder)
    if not parts:
        return "."
    return "/".join(parts)


# --------------------------------------------------------------------------- #
# Containment -- the one running-depth walk the discriminator and in_scope share
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ContainmentAnalysis:
    """Whether a reported path names a location inside the root, and where it left it.

    The question ``..`` makes hard is *containment*, and it cannot be answered by
    looking at the first segment.  ``core/src/main/../../../../etc/passwd`` carries no
    leading ``..`` at all, yet it names a location one level above the root -- three
    concrete segments against four ``..`` -- and a first-segment test would both
    classify it ``tree_file`` and let it match the allowlist glob ``core/src/main/**``
    on its segments alone, reporting an out-of-tree coordinate as an in-scope file in
    the scanned tree.  This analysis is a segment-wise walk instead, which notices the
    escape wherever it happens.

    Two spellings, and the distinction between them is the whole design:

    ``reported_path``
        The path exactly as it will reach the dataset's ``path`` field -- the
        :func:`normalise_reported_path` spelling, with **every** ``..`` preserved.
        The SARIF 2.1.0 errata (the section 3.10.2 amendment) forbid a consumer
        normalizing ``..`` out of a path, so this string is never rewritten and this
        analysis never changes what is emitted.  AAP 0.5.4 likewise has a container
        outside the root *"expressed with ``../`` segments preserved rather than
        normalized"*.
    ``canonical_path``
        The *canonical shadow*: the same coordinate with each ``..`` cancelled
        against the concrete segment before it, computed for classification only and
        emitted nowhere.  It is what makes ``a/../core/src/main/X.scala`` -- a path
        whose reported spelling matches no glob but which lexically names a file
        under ``core/src/main`` -- answer the scope question truthfully instead of
        falling out of scope on its spelling.

    Boundedness is explicit, because AAP 0.5.4 requires it and because the obvious
    alternative is unbounded: ``Path.resolve()`` and ``os.path.realpath`` touch the
    filesystem and follow symlink chains whose length is not a property of the input
    at all, and ``os.path.normpath`` collapses the ``..`` the errata protect.  This
    walk is one left-to-right pass over the segments, no recursion, no filesystem
    access and no symlink following, so its work is exactly ``segments_walked`` steps
    and its stack never exceeds that many entries.

    Attributes
    ----------
    reported_path:
        The normalised reported spelling, byte-identical to the emitted ``path``.
    segments:
        ``reported_path``'s non-empty segments -- of the container alone for an
        archive reference, since that is the component whose depth decides.
    canonical_path:
        The canonical shadow.  For an archive reference,
        ``<canonical-container>!<member>``.  ``"."`` where the shadow is the root
        itself.
    canonical_segments:
        The shadow's segments.  A leading run of ``..`` survives here: cancelling
        what is not there would claim containment the coordinate does not have.
    escapes_root:
        Whether the running depth went below zero at any segment.  Equivalently
        ``minimum_depth < 0``; both are recorded so a reader can check one against
        the other.
    escaping_segment_index:
        The index of the first segment at which the depth went negative, or ``None``.
        Named rather than merely counted so a rejection detail or a
        ``run-record.md`` note can say *where* the coordinate left the tree.
    minimum_depth:
        The lowest running depth reached.  ``-1`` for the worked example above, whose
        three concrete segments are spent by the third of its four ``..``.
    final_depth:
        The depth after the last segment.  A path can end back inside the root
        having left it on the way (``a/../../b/c``), which is why the minimum
        rather than the final value decides.
    segments_walked:
        The number of segments the walk consumed -- the analysis's whole cost, and
        the bound on it.
    container, member:
        The two halves of an archive reference, or ``None`` for a plain path.  The
        member's own ``..`` cannot move the coordinate relative to the *scan root*
        (it moves within the archive), so only the container is walked.
    """

    reported_path: str
    segments: tuple[str, ...]
    canonical_path: str
    canonical_segments: tuple[str, ...]
    escapes_root: bool
    escaping_segment_index: int | None
    minimum_depth: int
    final_depth: int
    segments_walked: int
    container: str | None
    member: str | None

    @property
    def is_archive_reference(self) -> bool:
        """Whether the analysed value carried the archive separator."""
        return self.member is not None

    @property
    def canonical_differs(self) -> bool:
        """Whether the canonical shadow is a different string from the reported one.

        A caller matching both spellings uses this to skip the second match where
        there is nothing to gain, and a reader uses it to see at a glance that the
        overwhelming majority of paths -- every one carrying no ``..`` and no
        interior ``.`` -- have exactly one spelling.
        """
        return self.canonical_path != self.reported_path

    def as_dict(self) -> dict[str, Any]:
        """Return the analysis as a plain, JSON-serialisable dict in a fixed order."""
        return {
            "reported_path": self.reported_path,
            "canonical_path": self.canonical_path,
            "escapes_root": self.escapes_root,
            "escaping_segment_index": self.escaping_segment_index,
            "minimum_depth": self.minimum_depth,
            "final_depth": self.final_depth,
            "segments_walked": self.segments_walked,
            "container": self.container,
            "member": self.member,
        }


def analyse_containment(value: str) -> ContainmentAnalysis:
    """Walk ``value``'s segments and report whether it stays inside the root.

    The walk keeps two things at once and neither is derivable from the other
    afterwards: a signed running **depth**, which answers containment, and a
    **canonical stack**, which answers "what does this coordinate actually name".

    Per segment:

    * ``.`` moves nothing.  It is skipped for both depth and shadow, and it is
      *not* removed from ``reported_path`` -- :func:`normalise_reported_path` drops
      only a leading ``.`` and this function changes no spelling at all.
    * ``..`` decrements the depth and pops the concrete segment before it.  Where
      the stack holds no concrete segment to pop, the ``..`` is *kept* in the
      shadow, so ``a/../../b`` shadows to ``../b`` rather than to ``b``: cancelling
      a ``..`` against nothing would manufacture containment.
    * anything else increments the depth and is pushed.

    The depth going below zero at **any** index is the escape, recorded with that
    index.  The final depth is reported too, because a coordinate can return inside
    the root after leaving it (``a/../../b/c``), so a test that inspected one position
    rather than every position would call such a coordinate contained.

    An archive reference is split at its first ``!`` by
    :func:`split_archive_reference` and the **container** is walked: a member's
    ``..`` moves within the archive, not relative to the scan root.  The shadow is
    re-serialized as ``<canonical-container>!<member>`` so the two spellings stay
    comparable.

    Examples
    --------
    >>> analyse_containment("core/src/main/scala/A.scala").escapes_root
    False
    >>> analyse_containment("core/src/main/../../../../etc/passwd").escapes_root
    True
    >>> analyse_containment("core/src/main/../../../../etc/passwd").minimum_depth
    -1
    >>> analyse_containment("core/src/main/scala/../java/A.java").escapes_root
    False
    >>> analyse_containment("core/src/main/scala/../java/A.java").canonical_path
    'core/src/main/java/A.java'
    >>> analyse_containment("../x.jar!org/apache/Foo.class").escapes_root
    True

    Raises
    ------
    PathPolicyError
        If ``value`` is not a string, from :func:`normalise_reported_path`.  A
        non-string coordinate is a malformed record, and the caller turns the raised
        error into a counted rejection rather than guessing a classification.
    """
    reported_path = normalise_reported_path(value)
    split = split_archive_reference(reported_path)
    if split is None:
        container: str | None = None
        member: str | None = None
        analysed = reported_path
    else:
        raw_container, raw_member = split
        container = normalise_reported_path(raw_container)
        member = normalise_reported_path(raw_member).lstrip("/")
        analysed = container

    segments = tuple(segment for segment in analysed.split("/") if segment)

    depth = 0
    minimum_depth = 0
    escaping_segment_index: int | None = None
    stack: list[str] = []
    for index, segment in enumerate(segments):
        if segment == ".":
            continue
        if segment == "..":
            depth -= 1
            if depth < minimum_depth:
                minimum_depth = depth
            if depth < 0 and escaping_segment_index is None:
                escaping_segment_index = index
            if stack and stack[-1] != "..":
                stack.pop()
            else:
                stack.append("..")
            continue
        depth += 1
        stack.append(segment)

    canonical_segments = tuple(stack)
    canonical_body = "/".join(canonical_segments) if canonical_segments else "."
    if member is None:
        canonical_path = canonical_body
    else:
        canonical_path = f"{canonical_body}{ARCHIVE_SEPARATOR}{member}"

    return ContainmentAnalysis(
        reported_path=reported_path,
        segments=segments,
        canonical_path=canonical_path,
        canonical_segments=canonical_segments,
        escapes_root=escaping_segment_index is not None,
        escaping_segment_index=escaping_segment_index,
        minimum_depth=minimum_depth,
        final_depth=depth,
        segments_walked=len(segments),
        container=container,
        member=member,
    )


def path_kind_for(relative_path: str) -> str:
    """Return :data:`PATH_KIND_ARCHIVE_MEMBER`, ``OUTSIDE_ROOT`` or ``TREE_FILE``.

    The discriminator is read off the serialized form, so it cannot disagree with
    the string that reaches the dataset: an ``!`` makes it an archive member, a
    coordinate whose running depth goes below zero at any segment makes it outside
    the root, and anything else names a location inside the tree.

    The archive test comes **first** and stays first.  A member inside a container
    that is itself outside the root is classified ``archive_member`` rather than
    ``outside_root`` -- both are non-filesystem coordinates and both take
    ``in_scope: false``, so the choice moves which counter increments and nothing
    else, and ``archive_member`` is the more specific truth.
    :attr:`ContainmentAnalysis.escapes_root` still reports the container's escape for
    any caller that needs it, which is why the two are computed by one walk.

    The escape test is :func:`analyse_containment`, not a test of the first segment.
    A first-segment test inspects one position instead of every position:
    ``core/src/main/../../../../etc/passwd`` carries no leading ``..``, so such a test
    would classify it ``tree_file`` and it would then match ``core/src/main/**`` on
    its segments -- a path one level above the root (three concrete segments, four
    ``..``) reported as an in-scope file.  The walk decides only the kind, and the
    ``in_scope`` verdict that follows from it; the emitted spelling is whatever was
    reported.
    """
    analysis = analyse_containment(relative_path)
    if analysis.is_archive_reference:
        return PATH_KIND_ARCHIVE_MEMBER
    if analysis.escapes_root:
        return PATH_KIND_OUTSIDE_ROOT
    return PATH_KIND_TREE_FILE


def assert_relative_path(value: str) -> str:
    """Return ``value`` where it is a legal emitted path, else raise.

    The one place the invariant lives (AAP 0.8.2: *"No absolute path is ever emitted,
    including for archive members and other non-filesystem coordinates"*).  Every
    :class:`ResolvedPath` runs through it on construction, so no resolver can bypass
    it.

    Refused: a leading ``/``, a leading backslash or UNC prefix, a Windows drive
    prefix, any URI form, a C0 or C1 control character anywhere in the value, and --
    as a second opinion on a shape the explicit checks did not anticipate -- anything
    ``PurePosixPath`` or ``PureWindowsPath`` calls absolute.

    The control check is the second of F16's two guards, placed here because this is
    the one function every emitted path passes through: a resolver could reach a
    control-bearing path by a route ``parse_uri_reference`` never sees, and a reported
    path field is exactly that route -- a Gitleaks ``File`` or a Dependency-Check
    ``filePath`` carrying a literal control byte is relativized and emitted with no
    URI decoding anywhere in its history.  It is refused rather than escaped because
    the ``path`` field is a coordinate a consumer joins onto a root, and a value this
    module rewrote would no longer name what the tool reported (AAP 0.5.4's
    reject-rather-than-infer rule).  The reason names the control by code point and
    never reproduces it, so putting the reason in a rejection detail is safe.

    Accepted, because each is a legitimate coordinate rather than a defect: the
    archive form ``<container>!<member>`` with exactly one ``!``, and a path carrying
    preserved ``../`` segments.

    Raises
    ------
    PathPolicyError
        With the reason named, so a caller can put it in a rejection detail.
    """
    if not isinstance(value, str) or not value:
        raise PathPolicyError(
            f"an emitted path must be a non-empty str; observed {value!r}"
        )
    control = describe_control_characters(value)
    if control is not None:
        # Named first among the shape checks and reported without the value, because
        # every message below interpolates ``value`` -- and while repr() happens to
        # escape a control, a diagnostic's safety must not rest on that.
        raise PathPolicyError(
            f"an emitted path {control}, which no path in the scanned tree does; "
            "emitting it would carry the control into findings.json, findings.csv, "
            "the Markdown records rendered from them and the run logs"
        )
    if value.startswith("/"):
        raise PathPolicyError(
            f"{value!r} begins with '/', which is a filesystem-absolute POSIX path"
        )
    if value.startswith("\\"):
        raise PathPolicyError(
            f"{value!r} begins with a backslash, which is a rooted or UNC Windows path"
        )
    if _WINDOWS_DRIVE_RE.match(value):
        raise PathPolicyError(
            f"{value!r} carries a Windows drive prefix, which is filesystem-absolute"
        )
    if _URI_AUTHORITY_RE.match(value):
        raise PathPolicyError(
            f"{value!r} is a URI with an authority component, not a path relative to "
            "the root"
        )
    scheme = _URI_SCHEME_RE.match(value)
    if scheme is not None and scheme.group(1).lower() in _EMITTED_PATH_FORBIDDEN_SCHEMES:
        raise PathPolicyError(
            f"{value!r} carries the URI scheme {scheme.group(1)!r}, not a path "
            "relative to the root"
        )
    if value.count(ARCHIVE_SEPARATOR) > 1:
        raise PathPolicyError(
            f"{value!r} carries {value.count(ARCHIVE_SEPARATOR)} '!' separators; the "
            "archive-member serialization defines exactly one"
        )
    if PurePosixPath(value).is_absolute() or PureWindowsPath(value).is_absolute():
        raise PathPolicyError(f"{value!r} is an absolute path")
    return value


#: Schemes an emitted path may never carry.  Matched by name so an ordinary relative
#: path whose first segment happens to contain a colon is not mistaken for a URI.
_EMITTED_PATH_FORBIDDEN_SCHEMES: Final[frozenset[str]] = frozenset(
    {
        "file",
        "jar",
        "zip",
        "tar",
        "gz",
        "jimple",
        "http",
        "https",
        "ftp",
        "ftps",
        "sftp",
        "s3",
        "gs",
        "urn",
        "data",
        "classpath",
    }
)



# --------------------------------------------------------------------------- #
# ResolvedPath -- the successful half of every resolver's return type
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ResolvedPath:
    """A root-relative path, with the discriminator and the provenance that go with it.

    Every resolver in this module returns ``ResolvedPath | Rejection``: the path if it
    could be expressed against the root, and a counted :class:`Rejection` if it could
    not.  ``path`` is validated by :func:`assert_relative_path` on construction, so a
    resolver cannot return an absolute path even by mistake.

    Attributes
    ----------
    path:
        The root-relative path exactly as it will reach the dataset's ``path`` field.
    kind:
        One of :data:`PATH_KINDS`.  ``cli.py`` tallies
        :data:`NON_FILESYSTEM_PATH_KINDS` against the total for the count and
        proportion ``run-record.md`` reports.
    basis:
        How the path was established -- one of the ``BASIS_*`` constants.  Provenance
        for ``tool-status.md``, and what makes a wrong base visible instead of silent.
    tool:
        The canonical tool identifier of the artifact the record came from.
    corroboration:
        ``None`` where every field that could corroborate the path agreed, or a
        sentence naming the disagreement.  AAP 0.5.3 on Checkov: *"record a mismatch
        rather than silently preferring one."*  A disagreement is reported, not
        resolved, and never suppresses the row.
    """

    path: str
    kind: str
    basis: str
    tool: str
    corroboration: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in _PATH_KIND_SET:
            raise PathPolicyError(
                f"unknown path kind {self.kind!r}; the closed set is "
                f"{', '.join(PATH_KINDS)}"
            )
        if not isinstance(self.basis, str) or not self.basis:
            raise PathPolicyError(
                "a resolved path must record the basis it was established on"
            )
        if not isinstance(self.tool, str) or not self.tool:
            raise PathPolicyError(
                "a resolved path must name the tool whose record it came from"
            )
        assert_relative_path(self.path)

    @property
    def is_non_filesystem_coordinate(self) -> bool:
        """Whether this path is an archive member or a location outside the root."""
        return is_non_filesystem_kind(self.kind)

    def in_scope(self, globs: Iterable[str]) -> bool:
        """Return this path's ``in_scope`` field under ``globs``.

        A thin pass-through to :func:`in_scope` carrying this path's own ``kind``, so
        an adapter cannot forget that a non-filesystem coordinate is never in scope.
        """
        return in_scope(self.path, globs, kind=self.kind)

    def as_dict(self) -> dict[str, Any]:
        """Return a plain, JSON-serialisable dict of this resolution."""
        return {
            "path": self.path,
            "kind": self.kind,
            "basis": self.basis,
            "tool": self.tool,
            "corroboration": self.corroboration,
        }


def _emitted_path_or_refusal(
    path: str,
    *,
    kind: str,
    basis: str,
    tool: str,
    reject_class: str,
    identity: Mapping[str, Any] | None,
    context: str,
    corroboration: str | None = None,
) -> ResolvedPath | Rejection:
    """Return the :class:`ResolvedPath` for ``path``, or the rejection it earns.

    The reject-rather-than-emit half of F16's second guard.
    :func:`assert_relative_path` already refuses a control-bearing path, and
    :meth:`ResolvedPath.__post_init__` calls it, so nothing can be emitted past it --
    but it *raises*, and a raise is the wrong outcome for artifact content: an escaped
    :class:`PathPolicyError` is a whole-artifact fault in ``cli.py``, which would let
    one hostile record deny the other several thousand.  AAP 0.5.4 is explicit that a
    record which cannot be attributed with certainty is *"rejected and the rejection
    recorded as a class with its count"*, so a resolver calls this and returns the
    counted rejection instead.

    The two are not redundant, they are a pair.  This function is the per-record
    outcome every resolver goes through; the raise inside
    :func:`assert_relative_path` stays as the backstop a resolver added later cannot
    bypass, in exactly the way the absoluteness invariant already works.

    Only the control check is turned into a rejection here.  The other
    :class:`PathPolicyError` conditions -- an unknown ``kind``, an empty ``basis``, an
    empty ``tool`` -- are caller faults rather than artifact content, and absorbing
    those into a rejection count would hide a defect in this pipeline behind a number
    that reads like a defect in a scanner's output.  They still raise.

    Args:
        reject_class: ``invalid_uri`` where the path came from a URI reference, and
            ``malformed_record`` where it came from a native tool's own path field --
            the same split the surrounding resolvers already use for their other
            rejections, so a reader counting classes sees one vocabulary.
        context: What resolved, named for the detail, so a reader can tell a
            control in a container from one in a member.
    """
    control = describe_control_characters(path)
    if control is not None:
        return Rejection(
            reject_class=reject_class,
            tool=tool,
            detail=(
                f"{context} resolves to a path that {control}; the record is rejected "
                "rather than emitted, because a control character in the path field "
                "would reach findings.json, findings.csv, the Markdown records "
                "rendered from them and the run logs"
            ),
            record_identity=dict(identity or {}),
        )
    return ResolvedPath(
        path=path,
        kind=kind,
        basis=basis,
        tool=tool,
        corroboration=corroboration,
    )


@dataclass
class PathKindTally:
    """A running count of resolved path kinds, for the proportion run-record.md reports.

    Deliberately mutable and deliberately trivial: AAP 0.6.1 has ``run-record.md``
    carry *"the non-filesystem path count and proportion"*, and a caller that keeps
    its own counters alongside :data:`NON_FILESYSTEM_PATH_KINDS` would be able to
    drift from the discriminator.  Counting through the discriminator cannot.
    """

    counts: dict[str, int] = field(default_factory=dict)

    def add(self, kind_or_resolved: str | ResolvedPath) -> None:
        """Count one resolution, given either its kind or the resolution itself."""
        kind = (
            kind_or_resolved.kind
            if isinstance(kind_or_resolved, ResolvedPath)
            else kind_or_resolved
        )
        if kind not in _PATH_KIND_SET:
            raise PathPolicyError(
                f"unknown path kind {kind!r}; the closed set is {', '.join(PATH_KINDS)}"
            )
        self.counts[kind] = self.counts.get(kind, 0) + 1

    def add_many(self, kind: str, count: int) -> None:
        """Count ``count`` resolutions of ``kind`` in one validated step.

        The counts an adapter reports are already aggregated: each returns a
        ``path_kind_<kind>`` counter carrying a number, not a stream of observations.
        Replaying that number as ``count`` separate :meth:`add` calls -- which is what
        ``for _ in range(count): tally.add(kind)`` does -- re-enumerates every one of the
        dataset's resolutions, twice over: once to build the per-artifact tally and once
        to fold it into the dataset tally.  That is two method calls and two dict lookups
        per emitted row, however many rows a generation emits, to compute a sum that was
        already known.  No row count is quoted here on purpose: the dataset is
        regenerated, so a figure in this docstring would be stale by construction, and
        the cost this method avoids scales with the rows rather than being a property of
        any one generation.

        The validation is not weakened to buy that: the kind is checked against the same
        closed set :meth:`add` checks it against, and the count must be a non-negative
        ``int``.  A negative count is refused rather than clamped, because a tally that
        can go backwards can be brought back to a plausible-looking total by two
        opposite mistakes, and the reported proportion would then be wrong with nothing
        recording it.  ``bool`` is refused explicitly: it is an ``int`` subclass, and
        ``add_many(kind, True)`` reads as a flag rather than as a count of one.

        A count of ``0`` is accepted and is a no-op, which keeps a caller free to fold a
        complete ``by_kind`` mapping in without filtering it first.
        """
        if kind not in _PATH_KIND_SET:
            raise PathPolicyError(
                f"unknown path kind {kind!r}; the closed set is {', '.join(PATH_KINDS)}"
            )
        if isinstance(count, bool) or not isinstance(count, int):
            raise PathPolicyError(
                f"a path-kind count must be an int; observed {type(count).__name__} "
                f"({count!r})"
            )
        if count < 0:
            raise PathPolicyError(
                f"a path-kind count must not be negative; observed {count!r} for "
                f"{kind!r}. A tally that can go backwards can be balanced by two "
                "opposite mistakes."
            )
        if count == 0:
            return
        self.counts[kind] = self.counts.get(kind, 0) + count

    @property
    def total(self) -> int:
        """Every resolution counted so far."""
        return sum(self.counts.values())

    @property
    def non_filesystem(self) -> int:
        """The resolutions whose kind is a non-filesystem coordinate."""
        return sum(
            count
            for kind, count in self.counts.items()
            if is_non_filesystem_kind(kind)
        )

    @property
    def non_filesystem_proportion(self) -> float:
        """``non_filesystem / total``, or ``0.0`` where nothing has been counted.

        Zero rather than an error for an empty tally: a tool that produced no row has
        no proportion to report, and ``0.0`` beside a total of ``0`` says exactly
        that.
        """
        total = self.total
        if total == 0:
            return 0.0
        return self.non_filesystem / total

    def as_dict(self) -> dict[str, Any]:
        """Return the tally as a plain dict, kinds in :data:`PATH_KINDS` order."""
        return {
            "by_kind": {kind: self.counts.get(kind, 0) for kind in PATH_KINDS},
            "total": self.total,
            "non_filesystem": self.non_filesystem,
            "non_filesystem_proportion": self.non_filesystem_proportion,
        }


# --------------------------------------------------------------------------- #
# Archive members -- one defined serialization, one separator
# --------------------------------------------------------------------------- #


def split_archive_reference(value: str) -> tuple[str, str] | None:
    """Split ``value`` at its first ``!`` into ``(container, member)``, or ``None``.

    ``None`` where the value carries no ``!`` at all, so a caller can tell "not an
    archive reference" from "an archive reference whose container is empty".  The
    split is on the *first* separator, because the container is the outermost thing;
    a member that still carries a ``!`` is a nested reference, which
    :func:`archive_member_path` refuses rather than serializing into a form the
    single-separator invariant cannot describe.

    A ``!/`` sequence -- the shape a ``jar:`` URL uses -- yields a member with its
    leading slash intact; :func:`archive_member_path` strips it when serializing.
    """
    if ARCHIVE_SEPARATOR not in value:
        return None
    container, _, member = value.partition(ARCHIVE_SEPARATOR)
    return container, member


def archive_member_path(container: str, member: str, root: str, *, tool: str) -> ResolvedPath:
    """Serialize an archive member as ``<container-relative-to-root>!<member>``.

    The container is relativized like any other path, so a container outside the root
    keeps its ``../`` segments (AAP 0.5.4, and the SARIF errata's prohibition on
    normalizing ``..`` away) and the result is still relative.  ``!`` is the single
    separator, and the member's leading slashes are removed so the result carries
    exactly one ``!`` and no ambiguous ``!/``.

    Raises
    ------
    PathPolicyError
        If the member is empty, or is itself a nested archive reference carrying
        another ``!``.  Refusing is reject-rather-than-infer: the defined
        serialization describes one container and one member, so a caller turns this
        into a counted ``malformed_record`` rejection rather than inventing a second
        separator the invariant forbids.

        Also where the serialized ``<container>!<member>`` carries a control
        character, which :func:`assert_relative_path` refuses on construction of the
        :class:`ResolvedPath` below -- F16's second guard reaching the non-filesystem
        archive serialization.  No separate check is written here on purpose: all
        three call sites already convert a :class:`PathPolicyError` from this function
        into a counted rejection, so the refusal is already reject-rather-than-emit,
        and a second check would be a second place to keep in step with the first.
    """
    normalised_member = normalise_reported_path(member).lstrip("/")
    if not normalised_member:
        raise PathPolicyError(
            f"an archive reference must name a member inside {container!r}; the member "
            "component is empty"
        )
    if ARCHIVE_SEPARATOR in normalised_member:
        raise PathPolicyError(
            f"nested archive reference {container!r}!{normalised_member!r}: the defined "
            "serialization carries exactly one '!' separator, so a member that is "
            "itself an archive reference is not describable in it"
        )
    relative_container = relativize_to_root(container, root)
    serialized = f"{relative_container}{ARCHIVE_SEPARATOR}{normalised_member}"
    return ResolvedPath(
        path=serialized,
        kind=PATH_KIND_ARCHIVE_MEMBER,
        basis=BASIS_ARCHIVE_MEMBER,
        tool=tool,
    )


def looks_like_archive_container(value: str) -> bool:
    """Return whether ``value``'s extension names a container type.

    Keyed on :data:`ARCHIVE_EXTENSIONS` so the rule is auditable: a per-record path
    beneath a container of one of these types is serialized as a member of it rather
    than being joined onto it as if the container were a directory.
    """
    lowered = normalise_reported_path(value).lower()
    return any(lowered.endswith(extension) for extension in ARCHIVE_EXTENSIONS)


# --------------------------------------------------------------------------- #
# URI references -- parsed, validated, and never normalized
# --------------------------------------------------------------------------- #

#: Outcome of :func:`parse_uri_reference`.
URI_FORM_RELATIVE: Final[str] = "relative"
URI_FORM_ABSOLUTE_PATH: Final[str] = "absolute-path"
URI_FORM_FILE_URI: Final[str] = "file-uri"
URI_FORM_ARCHIVE_URI: Final[str] = "archive-uri"
URI_FORM_FOREIGN_SCHEME: Final[str] = "foreign-scheme"
URI_FORM_INVALID: Final[str] = "invalid"


@dataclass(frozen=True)
class UriReference:
    """A parsed SARIF ``uri`` or any other reported URI reference.

    ``form`` is one of the ``URI_FORM_*`` constants and decides which branch a
    resolver takes.  ``value`` is the percent-decoded payload appropriate to the
    form: a relative reference, an absolute filesystem path, or -- for
    :data:`URI_FORM_ARCHIVE_URI` -- the container, with ``member`` carrying the part
    after the ``!``.  Nothing is normalized: a ``..`` in the input survives into
    ``value``.
    """

    form: str
    value: str
    member: str | None = None
    scheme: str | None = None
    detail: str | None = None


def _decoded_control_refusal(
    raw: str,
    decoded: str,
    *,
    component: str,
    scheme: str | None = None,
) -> UriReference | None:
    """Return the ``invalid`` reference a control-bearing decode earns, or ``None``.

    The post-decode half of the control-character guard (F16).  The check that runs
    on the raw reference cannot see what percent-encoding conceals: ``%00``, ``%0d``
    and ``%1b`` are four perfectly ordinary URI characters each until
    :func:`urllib.parse.unquote` turns them into NUL, CR and ESC, and the decoded
    value is what becomes a row's ``path`` -- reaching ``findings.json``,
    ``findings.csv``, every Markdown record rendered from them and the run logs
    (CWE-176 for the decode, CWE-117 for where it arrives).  So every decode in
    :func:`parse_uri_reference` is followed by this, and there are four of them: an
    archive member behind a scheme, a ``file:`` URI's path, an archive member with no
    scheme, and the general relative-or-absolute decode.

    ``value`` on the returned reference is the **raw** input, exactly as the
    pre-decode branches do: the raw form is still percent-encoded, so it carries no
    control character to propagate, while the decoded form would.  ``detail`` names
    the control by code point through :func:`describe_control_characters` and never
    reproduces it.

    The refusal is the ``invalid`` form rather than a silently stripped or escaped
    value, because AAP 0.5.4 makes ``invalid_uri`` its own rejection condition and
    forbids inference: a reference whose decoding is not a path cannot be repaired
    into one, and the caller counts it under ``invalid_uri`` rather than guessing.
    """
    control = describe_control_characters(decoded)
    if control is None:
        return None
    return UriReference(
        form=URI_FORM_INVALID,
        value=raw,
        scheme=scheme,
        detail=(
            f"the {component} decodes to a value that {control}; a percent-encoded "
            "control character is still a control character once decoded, and no path "
            "in the scanned tree carries one"
        ),
    )


def parse_uri_reference(raw: str) -> UriReference:
    """Parse ``raw`` into a :class:`UriReference`, classifying rather than repairing.

    The forms, and why each is distinguished:

    * ``invalid`` -- empty, carrying a control character, or something
      :func:`urllib.parse.urlsplit` cannot parse.  AAP 0.5.4 makes this its own
      rejection class (``invalid_uri``), separate from an unresolvable path.
      The control-character test is made **twice**: once on ``raw`` before anything
      is decoded, and once on each decoded value through
      :func:`_decoded_control_refusal`.  The raw test alone is not the guard it looks
      like -- ``%1b`` carries no control character and decodes to one -- and this
      function's whole output is decoded, so a single pre-decode check leaves every
      percent-encoded control to arrive in a row's ``path`` (F16, CWE-176).
    * ``file-uri`` -- a ``file:`` URI.  Its path component is percent-decoded into
      ``value`` and returned as it stands: absolute for the ordinary ``file:///p`` and
      ``file:/p`` forms, and **relative** for a rootless reference such as
      ``file:relative/x.txt``, which is accepted rather than repaired or rejected.
      :func:`resolve_sarif_location` relativizes either against the root, which leaves
      a rootless value unchanged and so reads it as already root-relative.  A
      non-empty, non-``localhost`` authority is *invalid* here rather than absolute: it
      names another host, and this pipeline has no way to express that as a path in the
      tree.  An empty path component is *invalid* too, since it names no location.
    * ``archive-uri`` -- a ``jar:``/``zip:``/``tar:`` URI, or any reference carrying
      ``!``.  The container and the member are returned separately for
      :func:`archive_member_path` to serialize.
    * ``foreign-scheme`` -- a well-formed URI in some other scheme (``http:``,
      ``urn:``).  Syntactically valid, so not ``invalid``; not a path in the tree
      either, so a caller rejects it under ``unresolvable_path``.
    * ``absolute-path`` -- no scheme, and filesystem-absolute (a leading ``//``
      authority-like form, or a Windows drive).
    * ``relative`` -- everything else, *including a reference beginning with exactly
      one* ``/``.  Per the SARIF 2.1.0 errata (issue 480, amending section 3.4.3) a
      relative reference may begin with a single slash where required to distinguish
      items in an archive format, so such a reference is **not** rejected as
      absolute here.  The single slash is retained in ``value`` so the caller can
      decide between the two readings against the recorded root -- see
      :func:`resolve_sarif_location`.
    """
    if not isinstance(raw, str) or not raw:
        return UriReference(
            form=URI_FORM_INVALID,
            value="",
            detail="the URI reference is empty",
        )
    if _CONTROL_CHARACTER_RE.search(raw):
        return UriReference(
            form=URI_FORM_INVALID,
            value=raw,
            detail="the URI reference carries a control character",
        )

    scheme_match = _URI_SCHEME_RE.match(raw)
    scheme = scheme_match.group(1).lower() if scheme_match is not None else None

    if scheme in ARCHIVE_URI_SCHEMES:
        inner = raw[scheme_match.end() :]  # type: ignore[union-attr]
        split = split_archive_reference(inner)
        if split is None:
            return UriReference(
                form=URI_FORM_INVALID,
                value=raw,
                scheme=scheme,
                detail=(
                    f"a {scheme!r} URI must separate its container from its member "
                    f"with {ARCHIVE_SEPARATOR!r}"
                ),
            )
        container_raw, member = split
        container = parse_uri_reference(container_raw)
        if container.form == URI_FORM_INVALID:
            return UriReference(
                form=URI_FORM_INVALID,
                value=raw,
                scheme=scheme,
                detail=f"the container of the {scheme!r} URI is invalid: {container.detail}",
            )
        # The container came back from a recursive parse, so its own decode was
        # already checked on the way out of that call and the branch above refused it.
        # It is re-checked here all the same: this function has four decode sites and
        # one of them is behind a recursion, and a guard that holds only because
        # another function happened to run first is a guard nobody can verify locally.
        refusal = _decoded_control_refusal(
            raw, container.value, component=f"container of the {scheme!r} URI", scheme=scheme
        )
        if refusal is not None:
            return refusal
        decoded_member = unquote(member)
        refusal = _decoded_control_refusal(
            raw, decoded_member, component=f"member of the {scheme!r} URI", scheme=scheme
        )
        if refusal is not None:
            return refusal
        return UriReference(
            form=URI_FORM_ARCHIVE_URI,
            value=container.value,
            member=decoded_member,
            scheme=scheme,
        )

    if scheme == "file":
        try:
            parts = urlsplit(raw)
        except ValueError as error:
            return UriReference(
                form=URI_FORM_INVALID,
                value=raw,
                scheme=scheme,
                detail=f"the file URI cannot be parsed: {error}",
            )
        if parts.netloc not in ("", "localhost"):
            return UriReference(
                form=URI_FORM_INVALID,
                value=raw,
                scheme=scheme,
                detail=(
                    f"the file URI names the authority {parts.netloc!r}, which is not "
                    "a location in the scanned tree"
                ),
            )
        decoded = unquote(parts.path)
        if not decoded:
            return UriReference(
                form=URI_FORM_INVALID,
                value=raw,
                scheme=scheme,
                detail="the file URI carries no path component",
            )
        refusal = _decoded_control_refusal(
            raw, decoded, component="path of the file URI", scheme=scheme
        )
        if refusal is not None:
            return refusal
        return UriReference(form=URI_FORM_FILE_URI, value=decoded, scheme=scheme)

    if scheme is not None:
        try:
            urlsplit(raw)
        except ValueError as error:
            return UriReference(
                form=URI_FORM_INVALID,
                value=raw,
                scheme=scheme,
                detail=f"the URI cannot be parsed: {error}",
            )
        return UriReference(form=URI_FORM_FOREIGN_SCHEME, value=raw, scheme=scheme)

    split = split_archive_reference(raw)
    if split is not None:
        container_raw, member = split
        container = parse_uri_reference(container_raw)
        if container.form == URI_FORM_INVALID:
            return UriReference(
                form=URI_FORM_INVALID,
                value=raw,
                detail=f"the container of the archive reference is invalid: {container.detail}",
            )
        refusal = _decoded_control_refusal(
            raw, container.value, component="container of the archive reference"
        )
        if refusal is not None:
            return refusal
        decoded_member = unquote(member)
        refusal = _decoded_control_refusal(
            raw, decoded_member, component="member of the archive reference"
        )
        if refusal is not None:
            return refusal
        return UriReference(
            form=URI_FORM_ARCHIVE_URI,
            value=container.value,
            member=decoded_member,
        )

    decoded = unquote(raw)
    refusal = _decoded_control_refusal(raw, decoded, component="URI reference")
    if refusal is not None:
        return refusal
    if decoded.startswith("//") or _WINDOWS_DRIVE_RE.match(decoded):
        return UriReference(form=URI_FORM_ABSOLUTE_PATH, value=decoded)
    return UriReference(form=URI_FORM_RELATIVE, value=decoded)



# --------------------------------------------------------------------------- #
# The runner-metadata view (AAP 0.6.1, AAP 0.6.4)
# --------------------------------------------------------------------------- #

#: Document keys this module reads.  Named constants because the document is an
#: input contract: a typo here would default a tool to the root, which is the exact
#: failure AAP 0.6.1 makes a hard error.
METADATA_TOOLS_KEY: Final[str] = "tools"
METADATA_SPARK_SRC_KEY: Final[str] = "spark_src"
METADATA_PATH_BASE_KEY: Final[str] = "path_base"

#: Emitted paths are relative to the ``SPARK_SRC`` scan root recorded in the document.
PATH_BASE_KIND_SCAN_ROOT: Final[str] = "scan_root"

#: Emitted paths are relative to whichever of several target paths the record came
#: from, so a root-relative or absolute sibling field named in ``anchor_fields`` is
#: the anchor.
PATH_BASE_KIND_PER_TARGET_DIRECTORY: Final[str] = "per_target_directory"

#: Emitted paths are absolute and must be relativized against the scan root.
PATH_BASE_KIND_FILESYSTEM_ABSOLUTE: Final[str] = "filesystem_absolute"

#: Emitted paths are relative to the process working directory at invocation.
PATH_BASE_KIND_PROCESS_WORKING_DIRECTORY: Final[str] = "process_working_directory"

#: Emitted paths are relative to a per-section target inside the artifact.
PATH_BASE_KIND_PER_SECTION_TARGET: Final[str] = "per_section_target"

#: No filesystem path in the scanned tree is emitted; a class identifier must be
#: resolved to a source path.
PATH_BASE_KIND_BYTECODE_CLASS: Final[str] = "bytecode_class"

#: The runner passes absolute target paths, so emitted paths echo them absolutely.
PATH_BASE_KIND_ABSOLUTE_TARGETS: Final[str] = "absolute_targets"

#: No explicit base could be established from the runner.  The document's own
#: instruction for this member is quoted in :class:`ToolPathBase`: a record with this
#: kind is rejected under ``unresolvable_path`` rather than resolved by a fallback.
PATH_BASE_KIND_NONE: Final[str] = "none"

#: The closed set of path-base kinds, exactly the vocabulary
#: ``runner-metadata.json`` declares in ``path_base_kind_vocabulary``.  A kind
#: outside it is a :class:`RunnerMetadataError`: an unknown base kind means the
#: document and this resolver disagree about what a tool's paths mean, and guessing
#: would give every row for that tool a plausible wrong path.
PATH_BASE_KINDS: Final[tuple[str, ...]] = (
    PATH_BASE_KIND_SCAN_ROOT,
    PATH_BASE_KIND_PER_TARGET_DIRECTORY,
    PATH_BASE_KIND_FILESYSTEM_ABSOLUTE,
    PATH_BASE_KIND_PROCESS_WORKING_DIRECTORY,
    PATH_BASE_KIND_PER_SECTION_TARGET,
    PATH_BASE_KIND_BYTECODE_CLASS,
    PATH_BASE_KIND_ABSOLUTE_TARGETS,
    PATH_BASE_KIND_NONE,
)

_PATH_BASE_KIND_SET: Final[frozenset[str]] = frozenset(PATH_BASE_KINDS)

#: The nine canonical tool identifiers (AAP 0.5.4).  Defined here rather than
#: imported because this module is a leaf: ``shape`` and ``severity`` each hold their
#: own copy for the same reason, and the three are kept identical by the adapter
#: tests rather than by an import that the leaf constraint forbids.
CANONICAL_TOOLS: Final[tuple[str, ...]] = (
    "opengrep",
    "semgrep",
    "datadog-static-analyzer",
    "gitleaks",
    "checkov",
    "trivy",
    "osv-scanner",
    "dependency-check",
    "joern",
)


def _as_string_sequence(value: Any) -> tuple[str, ...]:
    """Return ``value`` as a tuple of strings, or empty where it is not a list of them.

    A ``str`` is deliberately *not* treated as a sequence of one: iterating it would
    turn ``"file_abs_path"`` into fourteen single-character anchor field names, each
    of which would silently match nothing.
    """
    if isinstance(value, str) or not isinstance(value, Sequence):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def _as_mapping(value: Any, *, literal_key: str = "literal") -> Mapping[str, Any]:
    """Return ``value`` as a mapping, wrapping a bare string under ``literal_key``.

    ``invocation_form`` and ``working_directory`` are objects in this provisioning's
    document and could reasonably be plain strings in another.  Tolerating both is
    not inference: a string is carried through verbatim under a named key, and
    nothing is invented for the keys that are then absent.
    """
    if isinstance(value, Mapping):
        return MappingProxyType(dict(value))
    if isinstance(value, str):
        return MappingProxyType({literal_key: value})
    return MappingProxyType({})


@dataclass(frozen=True)
class ToolPathBase:
    """What one tool's reported paths are relative to, as the runner metadata records it.

    Constructed by :func:`tool_path_base` from
    ``harness/artifacts/logs/runner-metadata.json``.  Nothing here is inferred from
    an artifact: the base is a property of how the runner was invoked, and AAP 0.5.4
    requires *"every base taken from the recorded runner metadata"*.

    :attr:`has_explicit_base` is the load-bearing flag.  The documented
    degenerate-base fallback for a broken SARIF ``uriBaseId`` applies **only** where
    it is true; everywhere else the record is rejected and counted rather than
    guessed (AAP 0.5.4).  It is false for ``joern``, whose base value is ``null``
    because no filesystem base exists for a bytecode class -- and false for the
    ``none`` kind, which the document annotates: *"paths.py must reject such a record
    under unresolvable_path rather than fall back."*
    """

    tool: str
    kind: str
    base_value: str | None
    scan_root: str | None
    anchor_fields: tuple[str, ...] = ()
    record_path_field: str | None = None
    record_path_field_to_ignore: str | None = None
    invocation_form: Mapping[str, Any] = field(default_factory=dict)
    working_directory: Mapping[str, Any] = field(default_factory=dict)
    sarif_uri_base_id_emitted: str | None = None
    sarif_original_uri_base_ids_emitted: bool | None = None
    evidence: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in _PATH_BASE_KIND_SET:
            raise RunnerMetadataError(
                f"{self.tool}: path_base.kind {self.kind!r} is outside the document's "
                f"declared vocabulary ({', '.join(PATH_BASE_KINDS)}); a base kind this "
                "resolver does not know cannot be resolved by guessing"
            )
        object.__setattr__(self, "invocation_form", _as_mapping(self.invocation_form))
        object.__setattr__(
            self,
            "working_directory",
            _as_mapping(self.working_directory, literal_key="path"),
        )
        object.__setattr__(self, "anchor_fields", tuple(self.anchor_fields))

    @property
    def has_explicit_base(self) -> bool:
        """Whether the metadata supplies a base value this resolver may anchor on."""
        return self.kind != PATH_BASE_KIND_NONE and bool(self.base_value)

    @property
    def working_directory_path(self) -> str | None:
        """The invocation's working directory, preferring the clone-resolved form.

        ``resolved_path_this_clone`` is preferred over ``path`` because the latter may
        carry an unexpanded ``$HARNESS_SCRATCH_DIR``, and a base containing a literal
        shell variable would resolve nothing.
        """
        for key in ("resolved_path_this_clone", "path", "literal"):
            candidate = self.working_directory.get(key)
            if isinstance(candidate, str) and candidate and "$" not in candidate:
                return candidate
        return None

    @property
    def invocations_per_run(self) -> int | None:
        """How many times the runner invoked its tool, where the document says.

        The distinction the Gitleaks base turns on: one path per invocation makes the
        record's paths relative to that path, while several paths in one invocation
        makes them relative to the process working directory.  Both shapes are
        provisionable -- one invocation per scope directory, or all eighteen
        directories in a single invocation -- so the count is read from the document
        rather than assumed from the tool.
        """
        candidate = self.invocation_form.get("invocations_per_run")
        return candidate if isinstance(candidate, int) else None

    def base_for_relative(self) -> str | None:
        """The directory a relative reported path should be joined onto, or ``None``.

        ``scan_root``, ``filesystem_absolute``, ``per_target_directory`` and
        ``absolute_targets`` all anchor on the recorded base value.
        ``process_working_directory`` anchors on the working directory, falling back
        to the base value only where the document records one.  ``bytecode_class``,
        ``per_section_target`` and ``none`` have no single base and return ``None``,
        which is what makes their callers take the resolver branch that names the
        missing base instead of inventing one.
        """
        if self.kind == PATH_BASE_KIND_PROCESS_WORKING_DIRECTORY:
            return self.working_directory_path or self.base_value
        if self.kind in (
            PATH_BASE_KIND_SCAN_ROOT,
            PATH_BASE_KIND_FILESYSTEM_ABSOLUTE,
            PATH_BASE_KIND_PER_TARGET_DIRECTORY,
            PATH_BASE_KIND_ABSOLUTE_TARGETS,
        ):
            return self.base_value
        return None


def metadata_scan_root(document: Mapping[str, Any]) -> str:
    """Return the pinned scan root the document records.

    Raises
    ------
    RunnerMetadataError
        If ``spark_src`` is absent or is not an absolute path.  Every relativization
        in this module is against this value, so a missing or relative root would
        make every path in the dataset wrong in the same direction -- which is far
        harder to notice than an error here.
    """
    candidate = document.get(METADATA_SPARK_SRC_KEY)
    if not isinstance(candidate, str) or not candidate:
        raise RunnerMetadataError(
            f"runner metadata carries no {METADATA_SPARK_SRC_KEY!r}, so there is no "
            "root to express any path against"
        )
    if not is_absolute_path(candidate):
        raise RunnerMetadataError(
            f"runner metadata records {METADATA_SPARK_SRC_KEY} as {candidate!r}, which "
            "is not an absolute path"
        )
    return normalise_reported_path(candidate)


def metadata_tools(document: Mapping[str, Any]) -> tuple[str, ...]:
    """Return the tool identifiers the document carries, in document order."""
    tools = document.get(METADATA_TOOLS_KEY)
    if not isinstance(tools, Mapping):
        raise RunnerMetadataError(
            f"runner metadata carries no {METADATA_TOOLS_KEY!r} object"
        )
    return tuple(str(name) for name in tools)


def tool_path_base(document: Mapping[str, Any], tool: str) -> ToolPathBase:
    """Return the :class:`ToolPathBase` the document records for ``tool``.

    Raises
    ------
    RunnerMetadataError
        If the document carries no entry for ``tool``, or the entry carries no
        ``path_base`` object.  AAP 0.6.1: *"Missing metadata for a tool that wrote an
        artifact is a hard error the caller surfaces -- not a silent default to the
        root."*
    """
    tools = document.get(METADATA_TOOLS_KEY)
    if not isinstance(tools, Mapping):
        raise RunnerMetadataError(
            f"runner metadata carries no {METADATA_TOOLS_KEY!r} object, so no base for "
            f"{tool!r} can be established"
        )
    entry = tools.get(tool)
    if not isinstance(entry, Mapping):
        raise RunnerMetadataError(
            f"runner metadata carries no entry for {tool!r}; the document lists "
            f"{', '.join(metadata_tools(document))}. A tool that wrote an artifact "
            "with no recorded base cannot be resolved by defaulting to the root"
        )
    base = entry.get(METADATA_PATH_BASE_KEY)
    if not isinstance(base, Mapping):
        raise RunnerMetadataError(
            f"runner metadata for {tool!r} carries no {METADATA_PATH_BASE_KEY!r} object; "
            "the base a tool's paths are relative to is not something to infer"
        )
    kind = base.get("kind")
    if not isinstance(kind, str) or not kind:
        raise RunnerMetadataError(
            f"runner metadata for {tool!r} records no path_base.kind"
        )
    anchor_fields = base.get("anchor_fields")
    return ToolPathBase(
        tool=tool,
        kind=kind,
        base_value=base.get("value") if isinstance(base.get("value"), str) else None,
        scan_root=(
            entry.get("resolved_scan_root")
            if isinstance(entry.get("resolved_scan_root"), str)
            else document.get(METADATA_SPARK_SRC_KEY)
            if isinstance(document.get(METADATA_SPARK_SRC_KEY), str)
            else None
        ),
        anchor_fields=_as_string_sequence(anchor_fields),
        record_path_field=(
            base.get("record_path_field")
            if isinstance(base.get("record_path_field"), str)
            else None
        ),
        record_path_field_to_ignore=(
            base.get("record_path_field_to_ignore")
            if isinstance(base.get("record_path_field_to_ignore"), str)
            else None
        ),
        invocation_form=entry.get("invocation_form", {}),
        working_directory=entry.get("working_directory", {}),
        sarif_uri_base_id_emitted=(
            base.get("sarif_uri_base_id_emitted")
            if isinstance(base.get("sarif_uri_base_id_emitted"), str)
            else None
        ),
        sarif_original_uri_base_ids_emitted=(
            base.get("sarif_original_uri_base_ids_emitted")
            if isinstance(base.get("sarif_original_uri_base_ids_emitted"), bool)
            else None
        ),
        evidence=base.get("evidence") if isinstance(base.get("evidence"), str) else None,
    )


# --------------------------------------------------------------------------- #
# The bounded uriBaseId chain walk (AAP 0.5.4; SARIF 2.1.0 sections 3.4.3/3.4.4/3.14.14)
# --------------------------------------------------------------------------- #

#: The chain depth beyond which the walk stops.  This is **this pipeline's** resource
#: bound, not a SARIF validity rule: the specification imposes no ceiling on how deep
#: an ``originalUriBaseIds`` chain may go, and its own section 3.14.14 example chains
#: two levels (``SRCROOT`` through ``PROJECTROOT``).  Eight is a small constant chosen
#: so that an adversarial or pathological base map cannot make the walk unbounded,
#: over and above the visited-identifier set that already stops a cycle.  The
#: trade-off is stated rather than hidden: a chain that is legitimately deeper than
#: eight is not resolved through its own base map.  It terminates under
#: :data:`BASE_OUTCOME_OVER_DEPTH`, which carries the bound and the traversed chain in
#: its detail, and the record then takes AAP 0.5.4's case 1 -- the runner-recorded base
#: where the metadata supplies an explicit one for that tool, and otherwise a rejection
#: under ``unresolvable_path`` naming the outcome.
SARIF_BASE_CHAIN_MAX_DEPTH: Final[int] = 8

SARIF_URI_KEY: Final[str] = "uri"
SARIF_URI_BASE_ID_KEY: Final[str] = "uriBaseId"
SARIF_ORIGINAL_URI_BASE_IDS_KEY: Final[str] = "originalUriBaseIds"

#: The base resolved to an absolute URI or path.
BASE_OUTCOME_RESOLVED: Final[str] = "resolved"

#: The identifier is not in ``originalUriBaseIds`` -- semgrep issue 10591's shape.
BASE_OUTCOME_ABSENT: Final[str] = "absent"

#: The chain revisited an identifier.
BASE_OUTCOME_CYCLE: Final[str] = "cycle"

#: The chain exceeded :data:`SARIF_BASE_CHAIN_MAX_DEPTH`.
BASE_OUTCOME_OVER_DEPTH: Final[str] = "over-depth"

#: The chain resolved to a base that names nothing usable -- ``file:///`` for a git
#: repository target, trivy issue 10364's shape.
BASE_OUTCOME_DEGENERATE: Final[str] = "degenerate"

#: The chain ended on a relative reference with no absolute ancestor.
BASE_OUTCOME_NO_ABSOLUTE_ANCESTOR: Final[str] = "no-absolute-ancestor"

#: An entry's URI is syntactically invalid, or the entry is not an object.
BASE_OUTCOME_INVALID_URI: Final[str] = "invalid-uri"

#: The closed set of walk outcomes.
BASE_OUTCOMES: Final[tuple[str, ...]] = (
    BASE_OUTCOME_RESOLVED,
    BASE_OUTCOME_ABSENT,
    BASE_OUTCOME_CYCLE,
    BASE_OUTCOME_OVER_DEPTH,
    BASE_OUTCOME_DEGENERATE,
    BASE_OUTCOME_NO_ABSOLUTE_ANCESTOR,
    BASE_OUTCOME_INVALID_URI,
)

#: The outcomes AAP 0.5.4's case 1 covers -- the ones for which the runner-recorded
#: base may stand in, *and only where the metadata supplies an explicit base*.
BASE_OUTCOMES_ELIGIBLE_FOR_METADATA_FALLBACK: Final[tuple[str, ...]] = (
    BASE_OUTCOME_ABSENT,
    BASE_OUTCOME_CYCLE,
    BASE_OUTCOME_OVER_DEPTH,
    BASE_OUTCOME_DEGENERATE,
)

_FALLBACK_ELIGIBLE_SET: Final[frozenset[str]] = frozenset(
    BASE_OUTCOMES_ELIGIBLE_FOR_METADATA_FALLBACK
)


@dataclass(frozen=True)
class BaseResolution:
    """The result of walking a ``uriBaseId`` chain.

    ``outcome`` is one of the ``BASE_OUTCOME_*`` constants and is what keeps the
    three terminal cases distinct -- collapsing them into one catch-all is the defect
    AAP 0.5.4 names.  ``chain`` is the identifiers visited, in order, so a rejection
    detail can show the path the walk took rather than merely that it failed.
    """

    outcome: str
    base: str | None
    chain: tuple[str, ...]
    detail: str

    def __post_init__(self) -> None:
        if self.outcome not in BASE_OUTCOMES:
            raise PathPolicyError(
                f"unknown base-resolution outcome {self.outcome!r}; the closed set is "
                f"{', '.join(BASE_OUTCOMES)}"
            )

    @property
    def eligible_for_metadata_fallback(self) -> bool:
        """Whether AAP 0.5.4's case 1 covers this outcome."""
        return self.outcome in _FALLBACK_ELIGIBLE_SET


def _is_degenerate_base(value: str) -> bool:
    """Return whether ``value`` names the filesystem root rather than a directory.

    ``file:///`` decodes to ``/``, which is trivy issue 10364's documented shape when
    the scanned target is a git repository.  A base of ``/`` would relativize every
    path in the artifact against the filesystem root and quietly produce a long
    ``../`` chain for each, so it is treated as case 1 rather than as a valid root.
    """
    return normalise_reported_path(value).strip("/") == ""


def resolve_uri_base(
    base_id: str,
    original_uri_base_ids: Mapping[str, Any] | None,
    *,
    max_depth: int = SARIF_BASE_CHAIN_MAX_DEPTH,
) -> BaseResolution:
    """Walk ``base_id`` through ``original_uri_base_ids`` and return the base it names.

    Implements the second half of the consumer procedure SARIF 2.1.0 section 3.4.4
    lays down.  That procedure has two branches, and they are different kinds of thing:

    * **normative, first** -- use a value the *end user* configured for that specific
      ``uriBaseId``.  This pipeline configures no per-identifier value, so the branch
      selects nothing here and the procedure falls through to the second.
    * **normative, second** -- otherwise resolve the identifier from
      ``run.originalUriBaseIds``.  That is what this function does, chain and all.

    The runner-recorded base is neither branch: it is *project policy* (AAP 0.5.4),
    applied by :func:`resolve_sarif_location` only after this walk has failed to
    complete, only for the outcomes in
    :data:`BASE_OUTCOMES_ELIGIBLE_FOR_METADATA_FALLBACK`, and only where
    :attr:`ToolPathBase.has_explicit_base` -- with no explicit base for that tool the
    record is rejected under ``unresolvable_path`` instead.  Keeping the two apart is
    what stops a project fallback being read as the specification's own first branch.

    Chains are followed, because they are real: the specification's own section
    3.14.14 example carries a ``uriBaseId`` on a base entry so that ``SRCROOT`` is
    expressed relative to ``PROJECTROOT``.  Each level's relative ``uri`` is
    accumulated and joined beneath the first absolute ancestor found, so
    ``PROJECTROOT`` = ``file:///code/`` with ``SRCROOT`` = ``src/`` yields
    ``/code/src``.

    The walk is bounded twice over: a visited-identifier set stops a cycle on the
    repeat, and ``max_depth`` stops a chain that is merely too long.
    """
    if not isinstance(base_id, str) or not base_id:
        return BaseResolution(
            outcome=BASE_OUTCOME_INVALID_URI,
            base=None,
            chain=(),
            detail="the uriBaseId is empty",
        )
    if not isinstance(original_uri_base_ids, Mapping) or not original_uri_base_ids:
        return BaseResolution(
            outcome=BASE_OUTCOME_ABSENT,
            base=None,
            chain=(base_id,),
            detail=(
                f"the run emits no {SARIF_ORIGINAL_URI_BASE_IDS_KEY!r}, so the section "
                f"3.4.4 procedure cannot resolve {base_id!r} -- the documented producer "
                "gap (semgrep issue 10591)"
            ),
        )

    visited: set[str] = set()
    chain: list[str] = []
    # Relative levels are collected outermost-last and joined beneath the absolute
    # ancestor once one is found, which is the direction the chain is expressed in.
    suffixes: list[str] = []
    current = base_id

    while True:
        if current in visited:
            return BaseResolution(
                outcome=BASE_OUTCOME_CYCLE,
                base=None,
                chain=tuple(chain),
                detail=(
                    f"the {SARIF_URI_BASE_ID_KEY} chain revisits {current!r} after "
                    f"{' -> '.join(chain)}"
                ),
            )
        if len(chain) >= max_depth:
            return BaseResolution(
                outcome=BASE_OUTCOME_OVER_DEPTH,
                base=None,
                chain=tuple(chain),
                detail=(
                    f"the {SARIF_URI_BASE_ID_KEY} chain exceeds the bound of "
                    f"{max_depth} after {' -> '.join(chain)}"
                ),
            )
        visited.add(current)
        chain.append(current)

        entry = original_uri_base_ids.get(current)
        if entry is None:
            return BaseResolution(
                outcome=BASE_OUTCOME_ABSENT,
                base=None,
                chain=tuple(chain),
                detail=(
                    f"{current!r} has no entry in {SARIF_ORIGINAL_URI_BASE_IDS_KEY} "
                    f"(chain {' -> '.join(chain)})"
                ),
            )
        if not isinstance(entry, Mapping):
            return BaseResolution(
                outcome=BASE_OUTCOME_INVALID_URI,
                base=None,
                chain=tuple(chain),
                detail=(
                    f"the {SARIF_ORIGINAL_URI_BASE_IDS_KEY} entry for {current!r} is a "
                    f"{type(entry).__name__}, not an artifactLocation object"
                ),
            )

        raw_uri = entry.get(SARIF_URI_KEY)
        parent = entry.get(SARIF_URI_BASE_ID_KEY)
        parent_id = parent if isinstance(parent, str) and parent else None

        if raw_uri is None:
            # An entry may carry only a uriBaseId, deferring entirely to its parent.
            if parent_id is None:
                return BaseResolution(
                    outcome=BASE_OUTCOME_INVALID_URI,
                    base=None,
                    chain=tuple(chain),
                    detail=(
                        f"the entry for {current!r} carries neither a {SARIF_URI_KEY} "
                        f"nor a {SARIF_URI_BASE_ID_KEY}"
                    ),
                )
            current = parent_id
            continue

        if not isinstance(raw_uri, str):
            return BaseResolution(
                outcome=BASE_OUTCOME_INVALID_URI,
                base=None,
                chain=tuple(chain),
                detail=(
                    f"the {SARIF_URI_KEY} of the entry for {current!r} is a "
                    f"{type(raw_uri).__name__}, not a string"
                ),
            )

        reference = parse_uri_reference(raw_uri)
        if reference.form == URI_FORM_INVALID:
            return BaseResolution(
                outcome=BASE_OUTCOME_INVALID_URI,
                base=None,
                chain=tuple(chain),
                detail=(
                    f"the {SARIF_URI_KEY} {raw_uri!r} of the entry for {current!r} is "
                    f"not a valid URI reference: {reference.detail}"
                ),
            )
        if reference.form == URI_FORM_FOREIGN_SCHEME:
            return BaseResolution(
                outcome=BASE_OUTCOME_DEGENERATE,
                base=None,
                chain=tuple(chain),
                detail=(
                    f"the base {current!r} names {raw_uri!r} in the {reference.scheme!r} "
                    "scheme, which is not a location in the scanned tree"
                ),
            )
        if reference.form == URI_FORM_ARCHIVE_URI:
            return BaseResolution(
                outcome=BASE_OUTCOME_DEGENERATE,
                base=None,
                chain=tuple(chain),
                detail=(
                    f"the base {current!r} names the archive member {raw_uri!r}; a base "
                    "must name a directory, not a member inside a container"
                ),
            )

        if reference.form in (URI_FORM_FILE_URI, URI_FORM_ABSOLUTE_PATH) or (
            reference.form == URI_FORM_RELATIVE and is_absolute_path(reference.value)
        ):
            if _is_degenerate_base(reference.value):
                return BaseResolution(
                    outcome=BASE_OUTCOME_DEGENERATE,
                    base=None,
                    chain=tuple(chain),
                    detail=(
                        f"the base {current!r} resolves to the filesystem root via "
                        f"{raw_uri!r} -- the documented producer gap (trivy issue "
                        "10364), not a usable root"
                    ),
                )
            resolved = reference.value
            for suffix in reversed(suffixes):
                resolved = posix_join(resolved, suffix)
            # A base names a directory, so its trailing separator carries no
            # information and is dropped -- ``file:///code/`` and ``/code`` denote the
            # same directory, and dropping it makes every later join deterministic.
            # This is a *base* normalisation, not a reported-path one: no ``..`` is
            # cancelled and no segment is removed.
            resolved = normalise_reported_path(resolved)
            if len(resolved) > 1:
                resolved = resolved.rstrip("/") or "/"
            return BaseResolution(
                outcome=BASE_OUTCOME_RESOLVED,
                base=resolved,
                chain=tuple(chain),
                detail=(
                    f"resolved through {' -> '.join(chain)} to {resolved!r}"
                    if len(chain) > 1
                    else f"resolved {current!r} to {resolved!r}"
                ),
            )

        # A relative base: it only means something beneath its parent.
        if parent_id is None:
            return BaseResolution(
                outcome=BASE_OUTCOME_NO_ABSOLUTE_ANCESTOR,
                base=None,
                chain=tuple(chain),
                detail=(
                    f"the chain {' -> '.join(chain)} ends on the relative reference "
                    f"{raw_uri!r} with no {SARIF_URI_BASE_ID_KEY} to resolve it against, "
                    "so no absolute ancestor exists"
                ),
            )
        suffixes.append(reference.value)
        current = parent_id



# --------------------------------------------------------------------------- #
# The shared SARIF resolver
# --------------------------------------------------------------------------- #


def _resolve_relative_against_base(
    value: str,
    base: str,
    root: str,
    *,
    tool: str,
    basis: str,
    corroboration: str | None = None,
    reject_class: str,
    identity: Mapping[str, Any] | None,
    context: str,
) -> ResolvedPath | Rejection:
    """Join a relative reference onto ``base``, relativize to ``root``, classify.

    The one place a relative reference becomes a row's ``path``, so the kind is read
    off the serialized result rather than guessed from the input: an ``!`` makes it an
    archive member, and a running segment depth that goes below zero anywhere makes it
    outside the root -- ``path_kind_for`` walks the whole coordinate, so a ``..`` that
    a base joined into the middle of the path is caught exactly like a leading one.

    Because it is that one place, it is also where F16's second guard has to sit for
    the five relative branches that reach it, and the return type therefore widens to
    ``ResolvedPath | Rejection``: the serialized result is checked for a control
    character immediately before it becomes a row's ``path``, and a control-bearing
    one is a counted rejection rather than an emitted value.  Every caller already
    returns whatever this returns straight to an adapter that handles both, so the
    widening changes no call site's control flow -- and ``reject_class``, ``identity``
    and ``context`` are required rather than defaulted so a new caller has to say
    which class its records are counted under instead of inheriting one silently.
    """
    joined = posix_join(base, value)
    relative = relativize_to_root(joined, root)
    return _emitted_path_or_refusal(
        relative,
        kind=path_kind_for(relative),
        basis=basis,
        tool=tool,
        corroboration=corroboration,
        reject_class=reject_class,
        identity=identity,
        context=context,
    )


def resolve_sarif_location(
    uri: Any,
    uri_base_id: Any,
    original_uri_base_ids: Mapping[str, Any] | None,
    root: str,
    tool_base: ToolPathBase,
    *,
    tool: str | None = None,
    record_identity: Mapping[str, Any] | None = None,
) -> ResolvedPath | Rejection:
    """Resolve one SARIF ``physicalLocation.artifactLocation`` to a root-relative path.

    The shared resolver for every SARIF producer, since AAP 0.5.4 gives them one
    adapter.  ``uri`` and ``uri_base_id`` come from the ``artifactLocation``;
    ``original_uri_base_ids`` from the enclosing ``run``.

    The branches, each classified rather than collapsed:

    * an absent or empty ``uri`` -- ``absent_path``;
    * a syntactically invalid URI reference -- ``invalid_uri``;
    * a ``file:`` URI, or a reference that is unambiguously filesystem-absolute --
      relativized straight to the root, carrying ``../`` where it lands outside;
    * an archive URI (``jar:``/``zip:``/``tar:``) or any reference carrying ``!`` --
      serialized as ``<container>!<member>``;
    * a foreign scheme (``http:``, ``urn:``) -- ``unresolvable_path``, since it is
      valid but names nothing in the tree;
    * a reference beginning with exactly one ``/`` -- **not** rejected as absolute,
      per errata issue 480.  It is read as filesystem-absolute only where it actually
      names a location under the recorded root, and otherwise as the
      archive-distinguishing relative reference the errata describe.  Both readings
      are recorded in ``basis``, so neither is silent;
    * anything else -- relative, resolved against the base from
      :func:`resolve_uri_base`, or, where that walk hit one of
      :data:`BASE_OUTCOMES_ELIGIBLE_FOR_METADATA_FALLBACK`, against the
      runner-recorded base **only where** :attr:`ToolPathBase.has_explicit_base`.
      With no explicit base it is ``unresolvable_path``, with the walk's outcome in
      the detail.

    AAP 0.5.4: *"The documented degenerate-base fallback is not a catch-all -- it
    applies where the metadata makes the base known, and everywhere else the record
    is rejected and counted rather than guessed."*
    """
    tool_name = tool or tool_base.tool
    identity = dict(record_identity or {})
    identity.setdefault(SARIF_URI_KEY, uri)
    if isinstance(uri_base_id, str) and uri_base_id:
        identity.setdefault(SARIF_URI_BASE_ID_KEY, uri_base_id)

    if uri is None or (isinstance(uri, str) and not uri):
        return Rejection(
            reject_class=REJECT_ABSENT_PATH,
            tool=tool_name,
            detail=(
                "the SARIF artifactLocation carries no uri, so the record names no "
                "location; path is not an optional field"
            ),
            record_identity=identity,
        )
    if not isinstance(uri, str):
        return Rejection(
            reject_class=REJECT_MALFORMED_RECORD,
            tool=tool_name,
            detail=(
                f"the SARIF artifactLocation uri is a {type(uri).__name__}, not a string"
            ),
            record_identity=identity,
        )

    reference = parse_uri_reference(uri)

    if reference.form == URI_FORM_INVALID:
        return Rejection(
            reject_class=REJECT_INVALID_URI,
            tool=tool_name,
            detail=f"the uri {uri!r} is not a valid URI reference: {reference.detail}",
            record_identity=identity,
        )

    if reference.form == URI_FORM_FOREIGN_SCHEME:
        return Rejection(
            reject_class=REJECT_UNRESOLVABLE_PATH,
            tool=tool_name,
            detail=(
                f"the uri {uri!r} is a valid {reference.scheme!r} URI but names no "
                "location in the scanned tree, so it cannot be expressed against the "
                "scan root"
            ),
            record_identity=identity,
        )

    if reference.form == URI_FORM_ARCHIVE_URI:
        container = reference.value
        member = reference.member or ""
        if not is_absolute_path(container):
            base = tool_base.base_for_relative()
            if base is None:
                return Rejection(
                    reject_class=REJECT_UNRESOLVABLE_PATH,
                    tool=tool_name,
                    detail=(
                        f"the archive reference {uri!r} names the relative container "
                        f"{container!r}, and the runner metadata records path_base.kind "
                        f"{tool_base.kind!r} with no base to resolve it against"
                    ),
                    record_identity=identity,
                )
            container = posix_join(base, container)
        try:
            return archive_member_path(container, member, root, tool=tool_name)
        except PathPolicyError as error:
            return Rejection(
                reject_class=REJECT_MALFORMED_RECORD,
                tool=tool_name,
                detail=f"the archive reference {uri!r} cannot be serialized: {error}",
                record_identity=identity,
            )

    if reference.form in (URI_FORM_FILE_URI, URI_FORM_ABSOLUTE_PATH):
        relative = relativize_to_root(reference.value, root)
        return _emitted_path_or_refusal(
            relative,
            kind=path_kind_for(relative),
            basis=BASIS_ABSOLUTE_RELATIVIZED,
            tool=tool_name,
            reject_class=REJECT_INVALID_URI,
            identity=identity,
            context=f"the uri {uri!r} read as filesystem-absolute",
        )

    # From here the reference is relative in SARIF's sense.  A single leading slash
    # is the errata-480 shape and is explicitly not treated as absolute; it is read
    # as absolute only where it demonstrably names a location under the root.
    value = reference.value
    if value.startswith("/"):
        normalised_root = normalise_reported_path(root).rstrip("/")
        if value == normalised_root or value.startswith(normalised_root + "/"):
            relative = relativize_to_root(value, root)
            return _emitted_path_or_refusal(
                relative,
                kind=path_kind_for(relative),
                basis=BASIS_ABSOLUTE_RELATIVIZED,
                tool=tool_name,
                reject_class=REJECT_INVALID_URI,
                identity=identity,
                context=(
                    f"the uri {uri!r}, whose single leading slash names a location "
                    "under the recorded scan root"
                ),
                corroboration=(
                    "read as filesystem-absolute because the single leading slash "
                    "names a location under the recorded scan root; the errata-480 "
                    "archive reading would have produced a path under it twice"
                ),
            )
        value = value.lstrip("/")
        if not value:
            return Rejection(
                reject_class=REJECT_ABSENT_PATH,
                tool=tool_name,
                detail=(
                    f"the uri {uri!r} names no location once its leading slash is read "
                    "as the archive-distinguishing form of SARIF errata issue 480"
                ),
                record_identity=identity,
            )
        leading_slash_basis: str | None = BASIS_ARCHIVE_LEADING_SLASH
    else:
        leading_slash_basis = None

    if isinstance(uri_base_id, str) and uri_base_id:
        walk = resolve_uri_base(uri_base_id, original_uri_base_ids)
        if walk.outcome == BASE_OUTCOME_RESOLVED and walk.base is not None:
            return _resolve_relative_against_base(
                value,
                walk.base,
                root,
                tool=tool_name,
                reject_class=REJECT_INVALID_URI,
                identity=identity,
                context=f"the uri {uri!r} resolved through its {SARIF_URI_BASE_ID_KEY} chain",
                basis=leading_slash_basis or BASIS_SARIF_BASE_CHAIN,
                corroboration=(
                    None
                    if leading_slash_basis is None
                    else f"{BASIS_ARCHIVE_LEADING_SLASH}; {walk.detail}"
                ),
            )
        if walk.outcome == BASE_OUTCOME_INVALID_URI:
            return Rejection(
                reject_class=REJECT_INVALID_URI,
                tool=tool_name,
                detail=walk.detail,
                record_identity=identity,
            )
        if not walk.eligible_for_metadata_fallback:
            # BASE_OUTCOME_NO_ABSOLUTE_ANCESTOR: case 3, which AAP 0.5.4 rejects
            # outright rather than routing through the metadata fallback.
            return Rejection(
                reject_class=REJECT_UNRESOLVABLE_PATH,
                tool=tool_name,
                detail=walk.detail,
                record_identity=identity,
            )
        if not tool_base.has_explicit_base:
            return Rejection(
                reject_class=REJECT_UNRESOLVABLE_PATH,
                tool=tool_name,
                detail=(
                    f"{walk.detail}; and the runner metadata records path_base.kind "
                    f"{tool_base.kind!r} with no explicit base for {tool_name!r}, so the "
                    "documented degenerate-base fallback does not apply and this record "
                    "is rejected rather than resolved through a guess"
                ),
                record_identity=identity,
            )
        base = tool_base.base_for_relative()
        if base is None:
            return Rejection(
                reject_class=REJECT_UNRESOLVABLE_PATH,
                tool=tool_name,
                detail=(
                    f"{walk.detail}; and path_base.kind {tool_base.kind!r} supplies no "
                    "single directory a relative reference can be joined onto"
                ),
                record_identity=identity,
            )
        return _resolve_relative_against_base(
            value,
            base,
            root,
            tool=tool_name,
            reject_class=REJECT_INVALID_URI,
            identity=identity,
            context=f"the uri {uri!r} resolved against the runner-recorded base",
            basis=leading_slash_basis or BASIS_SARIF_METADATA_BASE,
            corroboration=(
                f"{walk.outcome}: {walk.detail}; resolved through the runner-recorded "
                f"base {base!r}"
            ),
        )

    # No uriBaseId at all -- datadog-static-analyzer's shape.  The metadata's base is
    # the only anchor there is, and its absence is a rejection rather than a default.
    base = tool_base.base_for_relative()
    if base is None:
        return Rejection(
            reject_class=REJECT_UNRESOLVABLE_PATH,
            tool=tool_name,
            detail=(
                f"the uri {uri!r} is relative and carries no {SARIF_URI_BASE_ID_KEY}, and "
                f"the runner metadata records path_base.kind {tool_base.kind!r} with no "
                "base to resolve it against"
            ),
            record_identity=identity,
        )
    return _resolve_relative_against_base(
        value,
        base,
        root,
        tool=tool_name,
        reject_class=REJECT_INVALID_URI,
        identity=identity,
        context=(
            f"the uri {uri!r}, which carries no {SARIF_URI_BASE_ID_KEY}, resolved "
            "against the runner-recorded base"
        ),
        basis=leading_slash_basis or BASIS_SARIF_NO_BASE_ID,
    )


# --------------------------------------------------------------------------- #
# The per-tool native resolvers (AAP 0.5.4's base table)
# --------------------------------------------------------------------------- #


def resolve_recorded_path(
    value: Any,
    root: str,
    tool_base: ToolPathBase,
    *,
    tool: str | None = None,
    basis: str | None = None,
    section_base: str | None = None,
    record_identity: Mapping[str, Any] | None = None,
) -> ResolvedPath | Rejection:
    """Resolve one reported filesystem coordinate under ``tool_base``'s kind.

    The shared core the native resolvers delegate to, so each tool's convention is
    stated once in its own wrapper and the mechanics are not written five times.  It
    handles the three shapes any tool can report -- an absolute path, a relative one,
    and an archive reference carrying ``!`` -- and takes its base from the recorded
    ``kind`` rather than from an assumption about the tool.

    ``section_base`` is required for :data:`PATH_BASE_KIND_PER_SECTION_TARGET`, whose
    base lives inside the artifact rather than in the metadata; its absence is a
    rejection, not a fall back to the root.
    """
    tool_name = tool or tool_base.tool
    identity = dict(record_identity or {})
    identity.setdefault("reported_path", value)

    if value is None or (isinstance(value, str) and not value.strip()):
        return Rejection(
            reject_class=REJECT_ABSENT_PATH,
            tool=tool_name,
            detail=(
                f"the record carries no {tool_base.record_path_field or 'path'} value, "
                "so it names no location; path is not an optional field"
            ),
            record_identity=identity,
        )
    if not isinstance(value, str):
        return Rejection(
            reject_class=REJECT_MALFORMED_RECORD,
            tool=tool_name,
            detail=(
                f"the record's {tool_base.record_path_field or 'path'} is a "
                f"{type(value).__name__}, not a string"
            ),
            record_identity=identity,
        )

    if tool_base.kind == PATH_BASE_KIND_BYTECODE_CLASS:
        # Not a rejection: a caller reaching here has sent a bytecode coordinate to
        # the filesystem resolver, which would relativize an ephemeral extraction
        # path into a plausible-looking wrong answer for every row.  A contract
        # violation is raised loudly rather than counted quietly.
        raise PathPolicyError(
            f"{tool_name}: path_base.kind is 'bytecode_class', so the record's "
            f"{tool_base.record_path_field or 'class'} field must be resolved with "
            "resolve_bytecode_class(); the metadata names "
            f"{tool_base.record_path_field_to_ignore or 'file'} as the field to ignore "
            "precisely because it is not a path in the scanned tree"
        )
    if tool_base.kind == PATH_BASE_KIND_NONE:
        return Rejection(
            reject_class=REJECT_UNRESOLVABLE_PATH,
            tool=tool_name,
            detail=(
                f"the runner metadata records path_base.kind 'none' for {tool_name!r}: "
                "no explicit base could be established from the runner, and the "
                "document's own instruction is to reject such a record rather than fall "
                "back"
            ),
            record_identity=identity,
        )

    archive = split_archive_reference(value)
    if archive is not None:
        container_raw, member = archive
        container = container_raw
        if not is_absolute_path(container):
            base = (
                section_base
                if tool_base.kind == PATH_BASE_KIND_PER_SECTION_TARGET
                else tool_base.base_for_relative()
            )
            if base is None:
                return Rejection(
                    reject_class=REJECT_UNRESOLVABLE_PATH,
                    tool=tool_name,
                    detail=(
                        f"the archive reference {value!r} names the relative container "
                        f"{container!r} and path_base.kind {tool_base.kind!r} supplies no "
                        "base to resolve it against"
                    ),
                    record_identity=identity,
                )
            container = posix_join(base, container)
        try:
            return archive_member_path(container, member, root, tool=tool_name)
        except PathPolicyError as error:
            return Rejection(
                reject_class=REJECT_MALFORMED_RECORD,
                tool=tool_name,
                detail=f"the archive reference {value!r} cannot be serialized: {error}",
                record_identity=identity,
            )

    if is_absolute_path(value):
        relative = relativize_to_root(value, root)
        return _emitted_path_or_refusal(
            relative,
            kind=path_kind_for(relative),
            basis=basis or BASIS_ABSOLUTE_RELATIVIZED,
            tool=tool_name,
            reject_class=REJECT_MALFORMED_RECORD,
            identity=identity,
            context=f"the record's absolute path {value!r} relativized to the root",
        )

    if tool_base.kind == PATH_BASE_KIND_PER_SECTION_TARGET:
        if section_base is None:
            return Rejection(
                reject_class=REJECT_UNRESOLVABLE_PATH,
                tool=tool_name,
                detail=(
                    f"path_base.kind 'per_section_target' means {value!r} is relative to "
                    "the section it came from, and no section base was supplied; the "
                    "scan root is not a substitute for it"
                ),
                record_identity=identity,
            )
        return _resolve_relative_against_base(
            value,
            section_base,
            root,
            tool=tool_name,
            reject_class=REJECT_MALFORMED_RECORD,
            identity=identity,
            context=f"the record's path {value!r} resolved against its section base",
            basis=basis or BASIS_RESOLVED_AGAINST_BASE,
        )

    base = tool_base.base_for_relative()
    if base is None:
        return Rejection(
            reject_class=REJECT_UNRESOLVABLE_PATH,
            tool=tool_name,
            detail=(
                f"the record's path {value!r} is relative and path_base.kind "
                f"{tool_base.kind!r} supplies no base to resolve it against"
            ),
            record_identity=identity,
        )
    # Where the recorded base *is* the scan root, the reported path was already
    # root-relative and the basis says so rather than implying a join that changed
    # nothing.  Gitleaks' File field in this provisioning is exactly this case.
    default_basis = (
        BASIS_ALREADY_ROOT_RELATIVE
        if normalise_reported_path(base).rstrip("/")
        == normalise_reported_path(root).rstrip("/")
        else BASIS_RESOLVED_AGAINST_BASE
    )
    return _resolve_relative_against_base(
        value,
        base,
        root,
        tool=tool_name,
        reject_class=REJECT_MALFORMED_RECORD,
        identity=identity,
        context=f"the record's path {value!r} resolved against the recorded base",
        basis=basis or default_basis,
    )



#: Checkov's three path fields, and what each is relative to.  The anchor order is
#: the metadata's ``anchor_fields`` where it supplies one; this is the fallback order
#: and the reason for it: ``repo_file_path`` is root-relative, ``file_abs_path`` is
#: absolute, and both are anchors, while ``file_path`` is relative to whichever
#: ``-d`` target the record came from and is therefore corroboration only.
CHECKOV_FILE_PATH_FIELD: Final[str] = "file_path"
CHECKOV_FILE_ABS_PATH_FIELD: Final[str] = "file_abs_path"
CHECKOV_REPO_FILE_PATH_FIELD: Final[str] = "repo_file_path"
CHECKOV_DEFAULT_ANCHOR_FIELDS: Final[tuple[str, ...]] = (
    CHECKOV_REPO_FILE_PATH_FIELD,
    CHECKOV_FILE_ABS_PATH_FIELD,
)

_CHECKOV_ANCHOR_BASIS: Final[Mapping[str, str]] = MappingProxyType(
    {
        CHECKOV_REPO_FILE_PATH_FIELD: BASIS_CHECKOV_REPO_FILE_PATH,
        CHECKOV_FILE_ABS_PATH_FIELD: BASIS_CHECKOV_FILE_ABS_PATH,
        CHECKOV_FILE_PATH_FIELD: BASIS_CHECKOV_FILE_PATH,
    }
)


def strip_single_leading_slash(value: str) -> str:
    """Remove exactly one leading ``/`` from ``value``, leaving everything else alone.

    Checkov's convention, and the user's first worked example: ``file_path`` is
    scan-target-relative *and* carries a leading slash, as in ``/folder1/A.tf``.
    Exactly one slash is removed, so a ``//`` prefix -- which would be an authority,
    not a path -- is still visible to :func:`assert_relative_path` rather than being
    quietly flattened into a relative-looking path.
    """
    normalised = normalise_reported_path(value)
    if normalised.startswith("/") and not normalised.startswith("//"):
        return normalised[1:]
    return normalised


def resolve_checkov_path(
    record: Mapping[str, Any],
    root: str,
    tool_base: ToolPathBase,
    *,
    tool: str = "checkov",
) -> ResolvedPath | Rejection:
    """Resolve a Checkov failed check to a root-relative path, and reconcile it.

    The user's first worked example, carried in unchanged (AAP 0.5.3).  ``file_path``
    is relative to the scan target and carries a leading slash: *"Read as
    filesystem-absolute it relativizes to a long ``../`` chain and the row silently
    takes ``in_scope: false``"*, which is the failure the example warns about.

    The subtlety that makes reconciliation the reliable route rather than a mere
    cross-check: this provisioning's runner passes **one ``-d`` per expanded scope
    directory** -- eighteen of them in a single invocation, which is why the metadata
    records ``path_base.kind`` ``per_target_directory`` -- so the slash-stripped
    ``file_path`` is relative to *that* scope directory and not to the scan root.  A
    measured record from the pinned tree makes it concrete:
    ``file_path`` ``/dockerfiles/spark/bindings/R/Dockerfile`` beside
    ``repo_file_path``
    ``/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings/R/Dockerfile``.
    Stripping the slash off ``file_path`` alone would name a directory that does not
    exist at the root.

    So an **anchor** field is resolved first -- ``repo_file_path`` (root-relative,
    leading slash) then ``file_abs_path`` (absolute), in the order the metadata's
    ``anchor_fields`` records -- and the stripped ``file_path`` is used to
    **corroborate**: it should be a suffix of the anchored result.  A disagreement is
    *recorded* in :attr:`ResolvedPath.corroboration` rather than silently resolved in
    favour of one field, and never suppresses the row.

    Where no anchor field is present and the base kind is ``per_target_directory``,
    the record is rejected under ``unresolvable_path``: there is no way to know which
    of eighteen targets a bare ``file_path`` came from, and the scan root is not a
    substitute for the answer.
    """
    if not isinstance(record, Mapping):
        return Rejection(
            reject_class=REJECT_MALFORMED_RECORD,
            tool=tool,
            detail=(
                f"a checkov failed check must be an object; observed "
                f"{type(record).__name__}"
            ),
            record_identity={},
        )
    identity = {
        key: record.get(key)
        for key in ("check_id", CHECKOV_FILE_PATH_FIELD, CHECKOV_REPO_FILE_PATH_FIELD)
        if record.get(key) is not None
    }
    raw_file_path = record.get(CHECKOV_FILE_PATH_FIELD)
    corroborator = (
        strip_single_leading_slash(raw_file_path)
        if isinstance(raw_file_path, str) and raw_file_path.strip()
        else None
    )

    anchors = tool_base.anchor_fields or CHECKOV_DEFAULT_ANCHOR_FIELDS
    for anchor in anchors:
        raw = record.get(anchor)
        if not isinstance(raw, str) or not raw.strip():
            continue
        relative = _checkov_anchor_to_relative(anchor, raw, root)
        notes: list[str] = []
        # Where both anchors are present they are reconciled against each other, not
        # merely used in order.  This is the reconciliation the worked example calls
        # for: `repo_file_path` read as filesystem-absolute would produce a long `../`
        # chain, and comparing it with the relativized `file_abs_path` is what catches
        # that rather than shipping it.
        for other in anchors:
            if other == anchor:
                continue
            other_raw = record.get(other)
            if not isinstance(other_raw, str) or not other_raw.strip():
                continue
            other_relative = _checkov_anchor_to_relative(other, other_raw, root)
            if other_relative != relative:
                notes.append(
                    f"{other} resolves to {other_relative!r} while {anchor} resolves to "
                    f"{relative!r}; the first anchor in the recorded order is used and "
                    "the disagreement recorded rather than resolved"
                )
        file_path_note = _checkov_corroboration(
            anchor=anchor,
            anchored=relative,
            corroborator=corroborator,
            tool_base=tool_base,
        )
        if file_path_note is not None:
            notes.append(file_path_note)
        return _emitted_path_or_refusal(
            relative,
            kind=path_kind_for(relative),
            basis=_CHECKOV_ANCHOR_BASIS.get(anchor, BASIS_RESOLVED_AGAINST_BASE),
            tool=tool,
            reject_class=REJECT_MALFORMED_RECORD,
            identity=identity,
            context=f"the failed check's {anchor}",
            corroboration="; ".join(notes) if notes else None,
        )

    if corroborator is None:
        return Rejection(
            reject_class=REJECT_ABSENT_PATH,
            tool=tool,
            detail=(
                "the failed check carries none of "
                f"{', '.join((*anchors, CHECKOV_FILE_PATH_FIELD))}, so it names no "
                "location; path is not an optional field"
            ),
            record_identity=identity,
        )

    if tool_base.kind == PATH_BASE_KIND_SCAN_ROOT:
        # A single -d target equal to the scan root: the stripped file_path really is
        # root-relative, and saying so is not a guess.
        return _emitted_path_or_refusal(
            corroborator,
            kind=path_kind_for(corroborator),
            basis=BASIS_CHECKOV_FILE_PATH,
            tool=tool,
            reject_class=REJECT_MALFORMED_RECORD,
            identity=identity,
            context=f"the failed check's {CHECKOV_FILE_PATH_FIELD}",
            corroboration=(
                "resolved from file_path alone: no anchor field was present, and the "
                "recorded path_base.kind is 'scan_root', so the stripped file_path is "
                "root-relative"
            ),
        )

    return Rejection(
        reject_class=REJECT_UNRESOLVABLE_PATH,
        tool=tool,
        detail=(
            f"the failed check carries only {CHECKOV_FILE_PATH_FIELD} "
            f"({raw_file_path!r}), which the runner metadata records as relative to "
            f"whichever of the runner's targets the record came from (path_base.kind "
            f"{tool_base.kind!r}, anchor fields {', '.join(anchors)}). Without an "
            "anchor field there is no way to know which target that was, and the scan "
            "root is not a substitute for the answer"
        ),
        record_identity=identity,
    )


def _checkov_anchor_to_relative(anchor: str, raw: str, root: str) -> str:
    """Express one Checkov anchor field as a root-relative path, **by field contract**.

    The branch is chosen by *which field* it is, never by whether the value looks
    absolute -- and that distinction is the whole of the user's first worked example.
    ``repo_file_path`` is root-relative *and* carries a leading slash, so it looks
    filesystem-absolute while not being it: relativizing it against the root yields a
    long ``../`` chain and the row silently takes ``in_scope: false``.  Reading the
    field's recorded contract instead cannot make that mistake.

    * :data:`CHECKOV_FILE_ABS_PATH_FIELD` -- filesystem-absolute; relativize.
    * :data:`CHECKOV_REPO_FILE_PATH_FIELD` -- root-relative with a leading slash;
      strip exactly one slash.
    * anything else another provisioning might name as an anchor -- relativize where
      the value is absolute, strip a leading slash where it is not, so an unforeseen
      anchor still resolves rather than raising.
    """
    if anchor == CHECKOV_FILE_ABS_PATH_FIELD:
        return relativize_to_root(raw, root)
    if anchor == CHECKOV_REPO_FILE_PATH_FIELD:
        return strip_single_leading_slash(raw)
    if is_absolute_path(raw) and not normalise_reported_path(raw).startswith("//"):
        normalised_root = normalise_reported_path(root).rstrip("/")
        candidate = normalise_reported_path(raw)
        if candidate == normalised_root or candidate.startswith(normalised_root + "/"):
            return relativize_to_root(raw, root)
    return strip_single_leading_slash(raw)


def _checkov_corroboration(
    *,
    anchor: str,
    anchored: str,
    corroborator: str | None,
    tool_base: ToolPathBase,
) -> str | None:
    """Return ``None`` where ``file_path`` corroborates the anchor, else the mismatch.

    Under ``per_target_directory`` the stripped ``file_path`` is relative to one of
    the runner's targets, so it corroborates by being a **suffix** of the anchored
    path.  Under ``scan_root`` it should be the anchored path exactly.  Either way a
    disagreement is reported and the anchored path is still used: AAP 0.5.3 --
    *"record a mismatch rather than silently preferring one."*
    """
    if corroborator is None:
        return f"no {CHECKOV_FILE_PATH_FIELD} present to corroborate {anchor}"
    if tool_base.kind == PATH_BASE_KIND_SCAN_ROOT:
        if corroborator == anchored:
            return None
        return (
            f"{CHECKOV_FILE_PATH_FIELD} {corroborator!r} does not equal the path "
            f"anchored on {anchor} ({anchored!r}), although path_base.kind is "
            "'scan_root' and the two should agree exactly; the anchored path is used "
            "and the disagreement recorded rather than resolved"
        )
    if anchored == corroborator or anchored.endswith("/" + corroborator):
        return None
    return (
        f"{CHECKOV_FILE_PATH_FIELD} {corroborator!r} is not a suffix of the path "
        f"anchored on {anchor} ({anchored!r}); the anchored path is used and the "
        "disagreement recorded rather than resolved"
    )


def resolve_gitleaks_path(
    record: Mapping[str, Any],
    root: str,
    tool_base: ToolPathBase,
    *,
    tool: str = "gitleaks",
) -> ResolvedPath | Rejection:
    """Resolve a Gitleaks finding's ``File`` to a root-relative path.

    Why the base is genuinely non-obvious: ``gitleaks dir`` takes exactly one path and
    reports relative to the process working directory when handed more.  The base
    therefore depends on the recorded invocation, not on the tool -- one path per
    invocation makes the record relative to that path, several in one makes it
    relative to the working directory -- so it is read from
    :class:`ToolPathBase` rather than assumed.  In this provisioning the runner cds to
    the scan root and hands over one directory per invocation, so the recorded kind is
    ``scan_root``; a runner passing all eighteen scope directories in a single
    invocation is the other shape, and its base is the working directory.  A differing
    base is a condition to record, never a runner to repair (AAP 0.3.2 forbids any
    runner edit or reconfiguration).

    No secret value is read from the record, and none can reach a path field: only the
    ``File`` field (or whatever ``record_path_field`` the metadata names) is consulted.
    """
    if not isinstance(record, Mapping):
        return Rejection(
            reject_class=REJECT_MALFORMED_RECORD,
            tool=tool,
            detail=f"a gitleaks finding must be an object; observed {type(record).__name__}",
            record_identity={},
        )
    field_name = tool_base.record_path_field or "File"
    return resolve_recorded_path(
        record.get(field_name),
        root,
        tool_base,
        tool=tool,
        record_identity={
            "RuleID": record.get("RuleID"),
            "StartLine": record.get("StartLine"),
            field_name: record.get(field_name),
        },
    )


def resolve_dependency_check_path(
    file_path: Any,
    root: str,
    tool_base: ToolPathBase,
    *,
    tool: str = "dependency-check",
    record_identity: Mapping[str, Any] | None = None,
) -> ResolvedPath | Rejection:
    """Resolve a Dependency-Check dependency's ``filePath`` to a root-relative path.

    The runner passes ``--scan`` an absolute path per scope directory, so
    ``dependencies[].filePath`` is filesystem-absolute under the scan root and the
    resolution is a straight relativization -- the recorded kind is
    ``filesystem_absolute``.  A nested dependency whose ``filePath`` carries a ``!``
    is serialized as an archive member by the shared core, so a JAR member never
    reaches the dataset as an absolute path.
    """
    return resolve_recorded_path(
        file_path,
        root,
        tool_base,
        tool=tool,
        basis=BASIS_ABSOLUTE_RELATIVIZED,
        record_identity=record_identity,
    )


def resolve_trivy_path(
    target: Any,
    root: str,
    tool_base: ToolPathBase,
    *,
    per_record_path: Any = None,
    section: str | None = None,
    section_base: str | None = None,
    tool: str = "trivy",
    record_identity: Mapping[str, Any] | None = None,
) -> ResolvedPath | Rejection:
    """Resolve a Trivy record within the section it came from.

    ``target`` is the enclosing ``Results[].Target``.  ``per_record_path`` is the
    optional path a section supplies for the individual record -- the metadata's own
    words: *"A section may supply its own per-record path or StartLine; those refine
    the enclosing Target rather than replacing the base."*  Three refinements, each
    stated rather than inferred:

    * an **absolute** per-record path is relativized to the root on its own;
    * a **relative** per-record path beneath a ``Target`` that names a container --
      keyed on :data:`ARCHIVE_EXTENSIONS`, which is how Trivy reports a member of a
      JAR -- is serialized as ``<container>!<member>``;
    * a **relative** per-record path otherwise names the same artifact base the
      ``Target`` does, so it is resolved against that base; where it merely restates
      the ``Target``, the ``Target``'s own resolution stands.

    For the merged artifact the recorded kind is ``scan_root``, because the runner's
    merge step prefixes each part's ``Target`` with that part's ``ArtifactName``.  The
    eighteen retained per-directory reports under ``logs/trivy.parts/`` are **not**
    root-anchored -- the metadata says so explicitly -- so a caller reading those
    passes a ``per_section_target`` base and a ``section_base``; passing neither is a
    rejection rather than a silent reading against the root.
    """
    identity = dict(record_identity or {})
    identity.setdefault("Target", target)
    if section is not None:
        identity.setdefault("section", section)

    resolved_target = resolve_recorded_path(
        target,
        root,
        tool_base,
        tool=tool,
        basis=BASIS_TRIVY_SECTION_TARGET,
        section_base=section_base,
        record_identity=identity,
    )
    if isinstance(resolved_target, Rejection) or per_record_path is None:
        return resolved_target
    if not isinstance(per_record_path, str) or not per_record_path.strip():
        # An empty refinement refines nothing; the Target's resolution stands, and the
        # emptiness is not read as a path.
        return resolved_target

    refinement = normalise_reported_path(per_record_path)
    if is_absolute_path(refinement):
        relative = relativize_to_root(refinement, root)
        return _emitted_path_or_refusal(
            relative,
            kind=path_kind_for(relative),
            basis=BASIS_TRIVY_PER_RECORD_PATH,
            tool=tool,
            reject_class=REJECT_MALFORMED_RECORD,
            identity=identity,
            context=f"the absolute per-record path {per_record_path!r}",
        )
    if refinement == resolved_target.path or resolved_target.path.endswith("/" + refinement):
        return resolved_target
    if looks_like_archive_container(resolved_target.path):
        try:
            member = archive_member_path(
                resolved_target.path, refinement, root, tool=tool
            )
        except PathPolicyError as error:
            return Rejection(
                reject_class=REJECT_MALFORMED_RECORD,
                tool=tool,
                detail=(
                    f"the per-record path {per_record_path!r} inside the container "
                    f"{resolved_target.path!r} cannot be serialized: {error}"
                ),
                record_identity=identity,
            )
        return _emitted_path_or_refusal(
            member.path,
            kind=member.kind,
            basis=BASIS_TRIVY_PER_RECORD_PATH,
            tool=tool,
            reject_class=REJECT_MALFORMED_RECORD,
            identity=identity,
            context=(
                f"the per-record path {per_record_path!r} serialized as a member of "
                f"the container {resolved_target.path!r}"
            ),
            corroboration=(
                f"the enclosing Target {resolved_target.path!r} names a container, so "
                "the per-record path is serialized as a member of it"
            ),
        )
    base = tool_base.base_for_relative()
    if base is None:
        return Rejection(
            reject_class=REJECT_UNRESOLVABLE_PATH,
            tool=tool,
            detail=(
                f"the per-record path {per_record_path!r} is relative and path_base.kind "
                f"{tool_base.kind!r} supplies no base to resolve it against"
            ),
            record_identity=identity,
        )
    return _resolve_relative_against_base(
        refinement,
        base,
        root,
        tool=tool,
        reject_class=REJECT_MALFORMED_RECORD,
        identity=identity,
        context=f"the per-record path {per_record_path!r} resolved against its section target",
        basis=BASIS_TRIVY_PER_RECORD_PATH,
    )



# --------------------------------------------------------------------------- #
# Joern: bytecode class -> source path, against src/main AND src/test
# --------------------------------------------------------------------------- #

#: The extensions a JVM class can have been compiled from, and therefore the only
#: ones the source index carries.  A ``.py`` file has no bytecode in this graph, so
#: indexing it would add keys nothing can ever match.
SOURCE_EXTENSIONS: Final[tuple[str, ...]] = (".scala", ".java")

#: Both source trees.  AAP 0.5.4 requires resolution against ``src/main`` **and**
#: ``src/test``, because every ``-tests`` artifact the build emitted is in the graph
#: input, so a Joern finding can legitimately name bytecode compiled from a test
#: tree.  The provisioned collector indexes ``src/main`` alone, so its coordinate
#: space is narrower than the one a finding can name.
SOURCE_TREES: Final[tuple[str, ...]] = ("main", "test")

#: Directory names never walked when building the index: build output, VCS metadata
#: and virtualenvs hold no source of record and would add duplicate keys.
SOURCE_INDEX_SKIP_DIRECTORIES: Final[frozenset[str]] = frozenset(
    {".git", ".idea", "target", "build", "node_modules", "__pycache__", ".venv", "venv"}
)

#: The bytecode class-file suffix.
CLASS_FILE_SUFFIX: Final[str] = ".class"

#: The frontend's own extraction directory, as the collector's regex expects it.
_JIMPLE_PREFIX_RE: Final[re.Pattern[str]] = re.compile(r"\A.*?(?:\A|/)jimple2cpg-\d+/")

#: This run stages the frontend's input under ``harness/artifacts/cpg-input/``, so a
#: class-file path may carry that prefix instead of the ``jimple2cpg-<digits>/`` one.
CPG_INPUT_STAGING_SEGMENT: Final[str] = "cpg-input"

#: A Java identifier: what every segment of a package path is, and what no staging
#: directory name (``jimple2cpg-13348921788793719165``, ``core__spark-core.jar``) is.
_JAVA_IDENTIFIER_RE: Final[re.Pattern[str]] = re.compile(r"\A[A-Za-z_$][A-Za-z0-9_$]*\Z")

#: Top-level type declarations, per source language.  Applied per line, so a type
#: declared inside a nested ``package`` block is found as readily as one at the top --
#: which is what makes ``sql/connect/shims``'s stub declarations visible.
_DECLARATION_PATTERNS: Final[Mapping[str, re.Pattern[str]]] = MappingProxyType(
    {
        ".scala": re.compile(
            r"^[ \t]*(?:(?:final|sealed|abstract|implicit|case|override|private"
            r"(?:\[[^\]]*\])?|protected(?:\[[^\]]*\])?|@[\w.]+(?:\([^)]*\))?)[ \t]+)*"
            r"(?:class|object|trait|enum)[ \t]+([A-Za-z_][A-Za-z0-9_$]*)",
            re.MULTILINE,
        ),
        ".java": re.compile(
            r"^[ \t]*(?:(?:public|protected|private|final|abstract|static|strictfp"
            r"|sealed|non-sealed|@[\w.]+(?:\([^)]*\))?)[ \t]+)*"
            r"(?:class|interface|enum|record|@interface)[ \t]+([A-Za-z_][A-Za-z0-9_$]*)",
            re.MULTILINE,
        ),
    }
)


def class_key(identifier: str) -> str:
    """Reduce a bytecode class identifier to ``<package path>/<outer type name>``.

    Two input shapes are accepted, because a collector may write either.  This
    provisioning's collector (``harness/lib/joern-scan.sc``) emits ``class`` as a
    **dotted type full name** -- ``org.apache.spark.sql.connect.SparkSession$$anon$2``
    -- and a collector that emits a class-**file path** is the other shape.  The
    metadata names the ephemeral ``file`` field as the one to ignore precisely because
    it is the frontend's extraction path rather than a location in the tree, so no
    assumption is made about which shape arrives:

    * a value containing ``/`` or ending in ``.class`` is read as a path.  A
      ``jimple2cpg-<digits>/`` prefix is stripped; failing that, everything up to and
      including a ``cpg-input/`` staging segment is stripped; failing both, the
      **longest suffix of segments that are all valid Java identifiers** is taken,
      which is exactly what a package path is and is what no staging directory name
      can be (``jimple2cpg-13348921788793719165`` and ``core__spark-core.jar`` both
      carry characters an identifier cannot);
    * anything else is read as a dotted full name and split on ``.``.

    The basename is then truncated at its **first** ``$``, so a companion object
    (``Foo$``), an anonymous class (``Foo$$anon$3``) and a nested class (``Foo$Bar``)
    all collapse to the outer type ``Foo`` -- the same reduction the collector
    performs, so a key built here matches one built there.

    Raises
    ------
    PathPolicyError
        If the identifier is empty, or reduces to no type name at all -- ``"$"`` and
        ``"a/b/"`` among them.  A caller turns this into a counted
        ``malformed_record`` rejection.
    """
    if not isinstance(identifier, str) or not identifier.strip():
        raise PathPolicyError(
            f"a bytecode class identifier must be a non-empty str; observed "
            f"{identifier!r}"
        )
    text = identifier.strip().replace("\\", "/")

    if "/" in text or text.endswith(CLASS_FILE_SUFFIX):
        text = _strip_staging_prefix(text)
        if text.endswith(CLASS_FILE_SUFFIX):
            text = text[: -len(CLASS_FILE_SUFFIX)]
        parts = [segment for segment in text.split("/") if segment]
    else:
        if text.endswith("."):
            raise PathPolicyError(
                f"the dotted class name {identifier!r} ends on a separator and names no "
                "type"
            )
        parts = [segment for segment in text.split(".") if segment]

    if not parts:
        raise PathPolicyError(
            f"the class identifier {identifier!r} reduces to no segments"
        )
    outer = parts[-1].split("$", 1)[0]
    if not outer:
        raise PathPolicyError(
            f"the class identifier {identifier!r} reduces to no type name once its "
            "companion, anonymous and nested suffixes are removed"
        )
    return "/".join((*parts[:-1], outer))


def _strip_staging_prefix(path: str) -> str:
    """Strip a frontend or staging prefix from a class-file path.

    Three rules, tried in order and each documented at :func:`class_key`: the
    frontend's ``jimple2cpg-<digits>/`` extraction directory, this run's
    ``cpg-input/`` staging directory, and -- where neither applies -- the
    longest all-Java-identifier suffix, which is the package path by definition.
    """
    match = _JIMPLE_PREFIX_RE.match(path)
    if match is not None:
        return path[match.end() :]

    segments = [segment for segment in path.split("/") if segment]
    if CPG_INPUT_STAGING_SEGMENT in segments:
        index = len(segments) - 1 - segments[::-1].index(CPG_INPUT_STAGING_SEGMENT)
        remainder = segments[index + 1 :]
        # The staged file itself sits directly under the staging directory, so its
        # own name is dropped along with the prefix where it is still present.
        if remainder and not _JAVA_IDENTIFIER_RE.match(remainder[0]):
            remainder = remainder[1:]
        if remainder:
            return "/".join(remainder)

    # Longest suffix of segments that are all valid Java identifiers.  The class-file
    # basename is tested without its extension, since '.' is not an identifier
    # character.
    probe = list(segments)
    if probe and probe[-1].endswith(CLASS_FILE_SUFFIX):
        probe[-1] = probe[-1][: -len(CLASS_FILE_SUFFIX)]
    start = len(probe)
    while start > 0 and _JAVA_IDENTIFIER_RE.match(probe[start - 1]):
        start -= 1
    if start < len(probe):
        return "/".join(segments[start:])
    return "/".join(segments)


def source_index_key(relative_path: str) -> tuple[str, str] | None:
    """Return ``(package_dir, file_base_name)`` for a source path, or ``None``.

    The package directory is derived from the path: everything after
    ``<module>/src/{main,test}/<language>/``.  ``None`` where the path carries no
    ``src/{main,test}/<language>/`` prefix at all, since such a file is not on a
    compiled source root and its classes are not in the graph.
    """
    segments = split_segments(relative_path)
    for index in range(len(segments) - 3):
        if segments[index] == "src" and segments[index + 1] in SOURCE_TREES:
            # index+2 is the language directory (scala, java); the package path is
            # everything between it and the file name.
            package = "/".join(segments[index + 3 : -1])
            base = segments[-1]
            stem = base.rsplit(".", 1)[0] if "." in base else base
            return package, stem
    return None


@dataclass(frozen=True)
class SourceIndex:
    """A bytecode-class-key to source-path index over ``src/main`` **and** ``src/test``.

    Two key schemes, both keyed on the package directory the path implies:

    * ``by_filename`` -- ``<package dir>/<file base name>``;
    * ``by_decl`` -- ``<package dir>/<declared type name>``, needed because Scala
      permits several top-level types in one file, so ``RangePartitioner`` resolves
      only through ``Partitioner.scala``.

    Resolution takes the **union** of both schemes and succeeds only where that union
    is exactly one distinct path (AAP 0.5.4: *"takes the resolution only where it is
    unique, and rejects the ambiguous and the unresolvable"*).  Index precedence is
    deliberately **not** used: measured at the pin, ``org/apache/spark/SparkContext``
    is unique under ``by_filename`` yet has two distinct candidates once ``by_decl``
    is included -- ``core``'s ``SparkContext.scala`` and
    ``sql/connect/shims``'s ``shims.scala``, which really does declare stub
    ``SparkConf``, ``SparkContext``, ``rdd.RDD`` and ``api.java.JavaRDD``.  Under
    precedence that record would resolve silently to ``core``; under union it is
    rejected, which is what the AAP requires.  The provisioned collector resolves such
    collisions with ``setdefault`` -- first wins in walk order -- and this deliberately
    does not copy that.

    A documented limitation, stated rather than papered over: ``by_decl`` keys on the
    **path-derived** package directory, so a type declared inside a nested ``package``
    block is keyed under its file's directory rather than its logical full name --
    ``shims.scala``'s ``rdd.RDD`` is keyed ``org/apache/spark/RDD``, not
    ``org/apache/spark/rdd/RDD``.  The effect is a possible *unresolvable*, never a
    wrong resolution, so reject-rather-than-infer is preserved.
    """

    by_filename: Mapping[str, tuple[str, ...]]
    by_decl: Mapping[str, tuple[str, ...]]
    files_indexed: int = 0
    trees_indexed: tuple[str, ...] = SOURCE_TREES
    declarations_read: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "by_filename",
            MappingProxyType({key: tuple(value) for key, value in self.by_filename.items()}),
        )
        object.__setattr__(
            self,
            "by_decl",
            MappingProxyType({key: tuple(value) for key, value in self.by_decl.items()}),
        )
        object.__setattr__(self, "trees_indexed", tuple(self.trees_indexed))

    @classmethod
    def from_mapping(
        cls,
        by_filename: Mapping[str, Iterable[str]] | None = None,
        by_decl: Mapping[str, Iterable[str]] | None = None,
        *,
        files_indexed: int = 0,
        trees_indexed: Iterable[str] = SOURCE_TREES,
        declarations_read: bool = True,
    ) -> SourceIndex:
        """Build an index from explicit mappings, with no filesystem access at all.

        The constructor the adapter tests use: AAP 0.6.1 has every adapter test run
        *"on a parsed fixture document without a live filesystem beyond the pinned
        root"*, and a synthetic index makes the ambiguous, the declaration-only and the
        ``src/test`` cases assertable without depending on the tree still containing
        the file that once demonstrated them.
        """
        return cls(
            by_filename={key: tuple(value) for key, value in (by_filename or {}).items()},
            by_decl={key: tuple(value) for key, value in (by_decl or {}).items()},
            files_indexed=files_indexed,
            trees_indexed=tuple(trees_indexed),
            declarations_read=declarations_read,
        )

    def candidates(self, key: str) -> tuple[str, ...]:
        """Return the distinct source paths claiming ``key``, sorted, across both schemes."""
        found = set(self.by_filename.get(key, ())) | set(self.by_decl.get(key, ()))
        return tuple(sorted(found))

    def schemes_for(self, key: str) -> tuple[str, ...]:
        """Return which key schemes contributed a candidate for ``key``."""
        schemes: list[str] = []
        if self.by_filename.get(key):
            schemes.append(BASIS_SOURCE_INDEX_FILENAME)
        if self.by_decl.get(key):
            schemes.append(BASIS_SOURCE_INDEX_DECLARATION)
        return tuple(schemes)

    def basis_for(self, key: str) -> str:
        """Return the ``BASIS_*`` string naming which scheme(s) resolved ``key``."""
        schemes = self.schemes_for(key)
        if len(schemes) == 2:
            return BASIS_SOURCE_INDEX_BOTH
        if schemes:
            return schemes[0]
        return BASIS_SOURCE_INDEX_FILENAME

    @property
    def ambiguous_filename_keys(self) -> tuple[str, ...]:
        """Keys under which two or more distinct files share a base name."""
        return tuple(
            sorted(key for key, value in self.by_filename.items() if len(set(value)) > 1)
        )

    @property
    def ambiguous_declaration_keys(self) -> tuple[str, ...]:
        """Keys under which two or more distinct files declare the same type."""
        return tuple(
            sorted(key for key, value in self.by_decl.items() if len(set(value)) > 1)
        )

    def statistics(self) -> dict[str, Any]:
        """Return the index's shape, for the record that has to explain a rejection count."""
        return {
            "files_indexed": self.files_indexed,
            "trees_indexed": list(self.trees_indexed),
            "declarations_read": self.declarations_read,
            "by_filename_keys": len(self.by_filename),
            "by_decl_keys": len(self.by_decl),
            "ambiguous_by_filename": len(self.ambiguous_filename_keys),
            "ambiguous_by_decl": len(self.ambiguous_declaration_keys),
        }


def build_source_index(
    root: str | os.PathLike[str],
    *,
    extensions: Iterable[str] = SOURCE_EXTENSIONS,
    trees: Iterable[str] = SOURCE_TREES,
    read_declarations: bool = True,
) -> SourceIndex:
    """Walk ``root`` and build the dual index over ``src/main`` and ``src/test``.

    ``root`` is an argument, so nothing is read at import time and a caller builds
    the index once and passes it to every resolution -- there is no module-level
    cache, because a cache keyed on a path is hidden state a test cannot reset.

    Measured at the pin by this function itself, via :meth:`SourceIndex.statistics`:
    6,759 files indexed, 6,755 ``by_filename`` keys of which 4 are ambiguous, and
    15,230 ``by_decl`` keys of which 107 are ambiguous.  Including
    ``src/test`` roughly doubles the ambiguity, and that is the point: the graph input
    contains every ``-tests`` artifact the build produced, so a class from a test tree
    is a resolution this index must be able to make -- and a collision it must be able
    to refuse.

    ``read_declarations=False`` builds ``by_filename`` only, for a caller that wants
    the cheap index; the resulting :class:`SourceIndex` records that it did, so a
    reader of a rejection count knows which index produced it.

    **A traversal that could not read part of the tree raises rather than returning a
    smaller index.**  ``os.walk`` swallows every error by default: an unreadable
    directory is skipped and the walk continues, so an index missing a whole module
    looks exactly like an index over a tree that never contained it.  Every Joern
    record whose class lives under the skipped directory then becomes an
    ``unresolvable_path`` rejection, and the resulting count is both wrong and
    indistinguishable from a correct one -- the shaded-third-party outcome this
    resolver produces legitimately for five findings in six.  A count nobody can
    reproduce is a condition this pipeline halts on (AAP 0.9.2), so ``onerror`` is
    wired to re-raise: the :class:`OSError` reaches ``cli._build_source_index``, which
    turns it into a named configuration fault carrying the failing path.  There is no
    tolerance mode, deliberately -- a tolerated skip would have to be recorded
    somewhere a reader looks, and the only place that reliably is is the halt.
    """
    root_path = Path(os.fspath(root))
    wanted_extensions = tuple(extensions)
    wanted_trees = tuple(trees)
    by_filename: dict[str, list[str]] = {}
    by_decl: dict[str, list[str]] = {}
    files_indexed = 0

    for directory, subdirectories, filenames in os.walk(
        root_path, onerror=_raise_traversal_error
    ):
        subdirectories[:] = [
            name for name in subdirectories if name not in SOURCE_INDEX_SKIP_DIRECTORIES
        ]
        for filename in filenames:
            extension = os.path.splitext(filename)[1]
            if extension not in wanted_extensions:
                continue
            absolute = os.path.join(directory, filename)
            relative = os.path.relpath(absolute, root_path).replace(os.sep, "/")
            keyed = source_index_key(relative)
            if keyed is None:
                continue
            package, stem = keyed
            segments = split_segments(relative)
            tree = next(
                (
                    segments[index + 1]
                    for index in range(len(segments) - 1)
                    if segments[index] == "src" and segments[index + 1] in SOURCE_TREES
                ),
                None,
            )
            if tree not in wanted_trees:
                continue
            files_indexed += 1
            filename_key = f"{package}/{stem}" if package else stem
            _append_unique(by_filename, filename_key, relative)
            if not read_declarations:
                continue
            pattern = _DECLARATION_PATTERNS.get(extension)
            if pattern is None:
                continue
            for declared in _declared_type_names(absolute, pattern):
                declaration_key = f"{package}/{declared}" if package else declared
                _append_unique(by_decl, declaration_key, relative)

    return SourceIndex(
        by_filename=by_filename,
        by_decl=by_decl,
        files_indexed=files_indexed,
        trees_indexed=wanted_trees,
        declarations_read=read_declarations,
    )


def _raise_traversal_error(error: OSError) -> NoReturn:
    """Re-raise a directory-traversal failure instead of letting ``os.walk`` drop it.

    ``os.walk``'s default ``onerror`` is ``None``, which means *ignore*: the entry that
    could not be listed is omitted and the walk reports success.  This function is
    passed as ``onerror`` so the failure propagates with the ``filename`` the operating
    system attached to it, which is the one piece of information a reader needs to act
    -- ``cli._build_source_index`` catches :class:`OSError` and raises a named
    configuration fault whose message carries it.

    The error is re-raised unchanged rather than wrapped.  Its type (``PermissionError``
    against ``FileNotFoundError``, say) is what tells a reader whether the tree is
    misconfigured or the process is under-privileged, and a wrapper would have to
    reproduce both that type and the ``errno`` to say as much.
    """
    raise error


def _append_unique(index: dict[str, list[str]], key: str, value: str) -> None:
    """Append ``value`` under ``key`` unless already present.

    Every candidate is kept, deliberately: ``setdefault`` -- keeping the first and
    discarding the rest -- is what makes an ambiguity invisible, and an invisible
    ambiguity is a wrong path nobody can see.
    """
    bucket = index.setdefault(key, [])
    if value not in bucket:
        bucket.append(value)


def _declared_type_names(path: str, pattern: re.Pattern[str]) -> tuple[str, ...]:
    """Return the top-level type names declared in the file at ``path``.

    **A read failure propagates.**  Swallowing it would report *no declarations*, which
    is the same silence as a file that genuinely declares nothing: the declaration
    scheme is the only route to eighteen of the pinned run's hundred and seven Joern
    resolutions -- ``ProcessBuilderLike`` in ``DriverRunner.scala``,
    ``RangePartitioner`` in ``Partitioner.scala`` -- so a file dropped that way removes
    resolutions the index is supposed to make and converts each affected record into an
    ``unresolvable_path`` rejection that reads as an ordinary shaded-class outcome.  The
    count is then unreproducible, and AAP 0.9.2 halts on a condition that makes a count
    unreproducible rather than recording a number nobody can check.  The
    :class:`OSError` carries the offending filename and reaches
    ``cli._build_source_index``, which names it in a configuration fault.

    Decoding errors *are* replaced rather than raised, and that is not the same
    decision: a declaration line is ASCII even where a comment elsewhere in the file is
    not, so replacement loses nothing the pattern could have matched, whereas an
    unreadable file loses every declaration it holds.
    """
    text = Path(path).read_text(encoding="utf-8", errors="replace")
    return tuple(dict.fromkeys(pattern.findall(text)))


def resolve_bytecode_class(
    identifier: Any,
    index: SourceIndex,
    root: str,
    *,
    tool: str = "joern",
    collector_explanation: str | None = None,
    record_identity: Mapping[str, Any] | None = None,
) -> ResolvedPath | Rejection:
    """Resolve a bytecode class identifier to a unique source path in the tree.

    Two properties of :class:`SourceIndex` carry the resolution: the index spans
    ``src/main`` **and** ``src/test``, and ambiguity is rejected rather than
    first-won.

    Outcomes:

    * exactly one distinct candidate -- a :class:`ResolvedPath` of kind
      ``bytecode_source``, with :meth:`SourceIndex.basis_for` recording whether the
      filename scheme, the declaration scheme or both supplied it.  A candidate in a
      ``src/test`` tree resolves like any other and is **retained**; the literal
      ``src/test`` exclusion then gives the row ``in_scope: false`` (AAP 0.5.4 and
      0.9.3), which is a kept row rather than a dropped one;
    * two or more -- ``ambiguous_source_resolution``, with every candidate named in
      the detail so a reader can see the collision rather than the count alone;
    * none -- ``unresolvable_path``.  This is the ordinary outcome for a third-party
      class shaded into Spark's JARs, and it is a counted rejection rather than a row
      with an invented path.  Measured over the 693 findings the provisioning's own
      run produced: 585 unresolvable -- exactly the count of shaded third-party
      classes it recorded -- 0 ambiguous, and **107** resolved, against the 89 its
      ``src/main``-only, filename-only resolver managed.  The extra 18 are precisely
      the ``org.apache.spark`` classes it recorded as having a source filename that
      differs from the class name, which only the declaration scheme can place;
    * an identifier that reduces to no type name -- ``malformed_record``.

    ``collector_explanation`` is the collector's own account of a resolution it could
    not make -- ``unresolved-bytecode-only`` and its siblings.  AAP 0.5.4 has such an
    explanation *"retained in the rejection record, never in a dataset field"*, so it
    is appended to the rejection's detail and reaches no row.  **Every** rejection route
    composes it -- the absent, blank and non-string identifier routes included: a route
    that dropped it would lose the collector's own account of the failure exactly where a
    reader of ``tool-status.md`` looks for it, while the rejection still counted.  The
    composition runs through :func:`_with_collector_explanation`, which returns the detail
    unchanged where no explanation was supplied, so a record carrying none reads as the
    adapter composed it.  This provisioning's collector emits no such field, so the
    parameter is usually ``None``; it is honoured because a collector that emits one
    supplies the only account of a resolution it could not make.
    """
    identity = dict(record_identity or {})
    identity.setdefault("class", identifier)

    if identifier is None or (isinstance(identifier, str) and not identifier.strip()):
        return Rejection(
            reject_class=REJECT_ABSENT_PATH,
            tool=tool,
            detail=_with_collector_explanation(
                "the finding carries no class identifier, and the runner metadata "
                "records the file field as the one to ignore, so no coordinate remains "
                "to resolve",
                collector_explanation,
            ),
            record_identity=identity,
        )
    if not isinstance(identifier, str):
        return Rejection(
            reject_class=REJECT_MALFORMED_RECORD,
            tool=tool,
            detail=_with_collector_explanation(
                f"the finding's class identifier is a {type(identifier).__name__}, not a "
                "string",
                collector_explanation,
            ),
            record_identity=identity,
        )

    try:
        key = class_key(identifier)
    except PathPolicyError as error:
        return Rejection(
            reject_class=REJECT_MALFORMED_RECORD,
            tool=tool,
            detail=_with_collector_explanation(str(error), collector_explanation),
            record_identity=identity,
        )

    identity.setdefault("class_key", key)
    candidates = index.candidates(key)

    if len(candidates) == 1:
        relative = candidates[0]
        try:
            assert_relative_path(relative)
        except PathPolicyError as error:
            return Rejection(
                reject_class=REJECT_UNRESOLVABLE_PATH,
                tool=tool,
                detail=_with_collector_explanation(
                    f"the source index holds {relative!r} for {key!r}, which is not a "
                    f"root-relative path: {error}",
                    collector_explanation,
                ),
                record_identity=identity,
            )
        return ResolvedPath(
            path=relative,
            kind=PATH_KIND_BYTECODE_SOURCE,
            basis=index.basis_for(key),
            tool=tool,
            corroboration=(
                "resolved from a src/test tree, which is retained with in_scope false "
                "rather than dropped"
                if SRC_TEST_MARKER in relative
                else None
            ),
        )

    if len(candidates) > 1:
        return Rejection(
            reject_class=REJECT_AMBIGUOUS_SOURCE_RESOLUTION,
            tool=tool,
            detail=_with_collector_explanation(
                f"the class key {key!r} is claimed by {len(candidates)} distinct source "
                f"files ({', '.join(candidates)}), so no unique resolution exists; the "
                "record is rejected rather than resolved to whichever the index "
                "happened to see first",
                collector_explanation,
            ),
            record_identity=identity,
        )

    return Rejection(
        reject_class=REJECT_UNRESOLVABLE_PATH,
        tool=tool,
        detail=_with_collector_explanation(
            f"no source file under src/{' or src/'.join(index.trees_indexed)} in the "
            f"pinned tree is named {key.rsplit('/', 1)[-1]!r} in the package "
            f"{key.rsplit('/', 1)[0]!r} or declares that type, so the bytecode class "
            f"{identifier!r} has no source coordinate in the tree",
            collector_explanation,
        ),
        record_identity=identity,
    )


def _with_collector_explanation(detail: str, explanation: str | None) -> str:
    """Append a collector's own resolution explanation to a rejection detail.

    The explanation is retained here and nowhere else: AAP 0.5.4 keeps it in the
    rejection record and out of every dataset field.
    """
    if not explanation:
        return detail
    return f"{detail}; collector path_resolution: {explanation}"


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #

__all__ = [
    # Errors
    "PathPolicyError",
    "RunnerMetadataError",
    # Rejection vocabulary
    "REJECT_ABSENT_PATH",
    "REJECT_UNRESOLVABLE_PATH",
    "REJECT_INVALID_URI",
    "REJECT_AMBIGUOUS_SOURCE_RESOLUTION",
    "REJECT_MISSING_RULE_ID",
    "REJECT_MISSING_MESSAGE",
    "REJECT_NON_INTEGER_START_LINE",
    "REJECT_UNFORMABLE_PACKAGE_COORDINATE",
    "REJECT_UNATTRIBUTABLE_SECTION",
    "REJECT_MALFORMED_RECORD",
    "REJECT_CLASSES",
    "REJECT_CLASS_DESCRIPTIONS",
    "is_reject_class",
    "Rejection",
    "make_rejection",
    # Safe diagnostics -- the one renderer for every persisted diagnostic
    "DIAGNOSTIC_TEXT_LIMIT",
    "DIAGNOSTIC_VALUE_LIMIT",
    "USERINFO_REDACTION",
    "DiagnosticText",
    "SafeDiagnostic",
    "sanitise_diagnostic",
    "safe_diagnostic",
    "sanitise_persisted",
    # Path kinds and provenance
    "PATH_KIND_TREE_FILE",
    "PATH_KIND_OUTSIDE_ROOT",
    "PATH_KIND_ARCHIVE_MEMBER",
    "PATH_KIND_BYTECODE_SOURCE",
    "PATH_KINDS",
    "NON_FILESYSTEM_PATH_KINDS",
    "is_non_filesystem_kind",
    "BASIS_ALREADY_ROOT_RELATIVE",
    "BASIS_ABSOLUTE_RELATIVIZED",
    "BASIS_RESOLVED_AGAINST_BASE",
    "BASIS_SARIF_BASE_CHAIN",
    "BASIS_SARIF_METADATA_BASE",
    "BASIS_SARIF_NO_BASE_ID",
    "BASIS_ARCHIVE_LEADING_SLASH",
    "BASIS_ARCHIVE_MEMBER",
    "BASIS_CHECKOV_REPO_FILE_PATH",
    "BASIS_CHECKOV_FILE_ABS_PATH",
    "BASIS_CHECKOV_FILE_PATH",
    "BASIS_TRIVY_SECTION_TARGET",
    "BASIS_TRIVY_PER_RECORD_PATH",
    "BASIS_SOURCE_INDEX_FILENAME",
    "BASIS_SOURCE_INDEX_DECLARATION",
    "BASIS_SOURCE_INDEX_BOTH",
    "COLLECTOR_UNRESOLVED_BYTECODE_ONLY",
    "ResolvedPath",
    "PathKindTally",
    # Scope
    "ALLOWLIST_GLOBS",
    "RECURSIVE_SEGMENT",
    "SRC_TEST_MARKER",
    "GLOB_RECURSIVE_SUFFIX",
    "PINNED_EXPANSION_DIRECTORIES",
    "PINNED_EXPANSION_FILES",
    "normalise_reported_path",
    "match_glob",
    "matches_any_glob",
    "contains_src_test",
    "has_src_test_segment_pair",
    "src_test_readings_agree",
    "in_scope",
    "ScopeDecision",
    "scope_decision",
    # Loaders
    "load_allowlist",
    "allowlist_matches_authoritative_globs",
    "scope_glob_bases",
    "expand_scope_directories",
    "load_runner_metadata",
    # The bounds the runner-metadata read enforces (CWE-400, CWE-674, CWE-770)
    "METADATA_BYTE_LIMIT",
    "METADATA_DEPTH_LIMIT",
    "METADATA_NODE_LIMIT",
    "METADATA_STRING_LIMIT",
    # The one pre-parse lexical gate, shared with cli's artifact route
    "STATUS_NUMERIC_DIGIT_LIMIT",
    "DOCUMENT_BOUND_DEPTH",
    "DOCUMENT_BOUND_NODES",
    "DOCUMENT_BOUND_STRING",
    "DOCUMENT_BOUND_MEMBER_NAME",
    "DOCUMENT_BOUND_NUMERIC_DIGITS",
    "DOCUMENT_BOUND_LITERAL_TOKEN",
    "DOCUMENT_BOUNDS",
    "STRUCTURAL_MEASUREMENT_KEYS",
    "DocumentBoundExceeded",
    "string_token_end",
    "validate_document_bounds",
    # Relativization and serialization
    "ARCHIVE_SEPARATOR",
    "ARCHIVE_EXTENSIONS",
    "ARCHIVE_URI_SCHEMES",
    "split_segments",
    "posix_join",
    "is_absolute_path",
    "relativize_to_root",
    "ContainmentAnalysis",
    "analyse_containment",
    "path_kind_for",
    "assert_relative_path",
    "split_archive_reference",
    "archive_member_path",
    "looks_like_archive_container",
    # URI references
    "URI_FORM_RELATIVE",
    "URI_FORM_ABSOLUTE_PATH",
    "URI_FORM_FILE_URI",
    "URI_FORM_ARCHIVE_URI",
    "URI_FORM_FOREIGN_SCHEME",
    "URI_FORM_INVALID",
    "UriReference",
    "parse_uri_reference",
    # Runner metadata
    "METADATA_TOOLS_KEY",
    "METADATA_SPARK_SRC_KEY",
    "METADATA_PATH_BASE_KEY",
    "PATH_BASE_KIND_SCAN_ROOT",
    "PATH_BASE_KIND_PER_TARGET_DIRECTORY",
    "PATH_BASE_KIND_FILESYSTEM_ABSOLUTE",
    "PATH_BASE_KIND_PROCESS_WORKING_DIRECTORY",
    "PATH_BASE_KIND_PER_SECTION_TARGET",
    "PATH_BASE_KIND_BYTECODE_CLASS",
    "PATH_BASE_KIND_ABSOLUTE_TARGETS",
    "PATH_BASE_KIND_NONE",
    "PATH_BASE_KINDS",
    "CANONICAL_TOOLS",
    "ToolPathBase",
    "metadata_scan_root",
    "metadata_tools",
    "tool_path_base",
    # SARIF
    "SARIF_BASE_CHAIN_MAX_DEPTH",
    "SARIF_URI_KEY",
    "SARIF_URI_BASE_ID_KEY",
    "SARIF_ORIGINAL_URI_BASE_IDS_KEY",
    "BASE_OUTCOME_RESOLVED",
    "BASE_OUTCOME_ABSENT",
    "BASE_OUTCOME_CYCLE",
    "BASE_OUTCOME_OVER_DEPTH",
    "BASE_OUTCOME_DEGENERATE",
    "BASE_OUTCOME_NO_ABSOLUTE_ANCESTOR",
    "BASE_OUTCOME_INVALID_URI",
    "BASE_OUTCOMES",
    "BASE_OUTCOMES_ELIGIBLE_FOR_METADATA_FALLBACK",
    "BaseResolution",
    "resolve_uri_base",
    "resolve_sarif_location",
    # Native resolvers
    "resolve_recorded_path",
    "CHECKOV_FILE_PATH_FIELD",
    "CHECKOV_FILE_ABS_PATH_FIELD",
    "CHECKOV_REPO_FILE_PATH_FIELD",
    "CHECKOV_DEFAULT_ANCHOR_FIELDS",
    "strip_single_leading_slash",
    "resolve_checkov_path",
    "resolve_gitleaks_path",
    "resolve_dependency_check_path",
    "resolve_trivy_path",
    # Joern
    "SOURCE_EXTENSIONS",
    "SOURCE_TREES",
    "SOURCE_INDEX_SKIP_DIRECTORIES",
    "CLASS_FILE_SUFFIX",
    "CPG_INPUT_STAGING_SEGMENT",
    "class_key",
    "source_index_key",
    "SourceIndex",
    "build_source_index",
    "resolve_bytecode_class",
]
