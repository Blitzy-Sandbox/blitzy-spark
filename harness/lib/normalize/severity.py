"""Severity policy for the OSS scanner normalizer: fixed before any output is observed.

This module is the single place where a scanner's own severity vocabulary becomes
this dataset's ``severity_norm`` band, and where the basis for that decision is
recorded.  The policy below was decided in advance from the Agent Action Plan and
is deliberately *not* fitted to the artifacts this run happens to produce.

Position in the normalizer
--------------------------
A leaf.  Every adapter depends on ``paths`` and ``severity`` and on nothing else
(AAP §0.6.4), so this module imports nothing from the ``normalize`` package and
never will.  Only the standard library is used (AAP §0.4.1): no third-party
import, hence no manifest, no lockfile and no install step.  There is no
``__init__.py`` under ``harness/lib/normalize/`` by design -- the package is a
PEP 420 implicit namespace package, resolved once ``harness/lib`` is on
``sys.path``.

The closed output vocabulary
----------------------------
``severity_norm`` takes one of exactly five literals, spelled as in
:data:`SEVERITY_NORM`: ``Critical``, ``High``, ``Medium``, ``Low``, ``Info``.
``severity_norm`` is **never absent** (AAP §0.8.2); ``severity_native`` may be.
The invariant is enforced on every construction of :class:`SeverityResult` and
raises :class:`SeverityPolicyError` rather than relying on ``assert``, which
``python -O`` would strip.

Table 1 -- SARIF ``level``
--------------------------
``error`` -> ``High``, ``warning`` -> ``Medium``, ``note`` -> ``Low``,
``none`` -> ``Info``.  The comparison is case-insensitive: SARIF spells these
lower-case, and a producer that differs only in case should map rather than fall
through to unmapped.  ``severity_native`` records the literal as observed.

Table 2 -- label vocabularies (Trivy, Checkov, Dependency-Check, OSV ecosystem labels)
--------------------------------------------------------------------------------------
``CRITICAL`` -> ``Critical``; ``HIGH`` -> ``High``; ``MODERATE`` and ``MEDIUM``
-> ``Medium``; ``LOW`` -> ``Low``; ``NEGLIGIBLE``, ``INFO``, ``INFORMATIONAL``,
``UNKNOWN`` and ``NONE`` -> ``Info``.

The lookup is case-insensitive and surrounding whitespace is stripped; nothing
else is done to the literal, and the observed spelling is what reaches
``severity_native``.  Case folding is load-bearing rather than cosmetic: real
artifacts carry both ``MODERATE`` and lower-case ``moderate``, so a
case-sensitive table would map one and miss the other.

Table 3 -- CVSS numeric score (CVSS v3.1 §5 qualitative scale)
--------------------------------------------------------------
``>= 9.0`` Critical, ``>= 7.0`` High, ``>= 4.0`` Medium, ``> 0.0`` Low,
``== 0.0`` Info.  Comparisons are numeric on a ``float`` and never lexical:
scores arrive carrying float32-to-float64 representation artifacts such as
``3.200000047683716``, which a string test bands wrongly.

A value that will not coerce to ``float``, or that coerces to a number outside
``0.0``-``10.0``, is **not** banded and **not** clamped -- it is disclosed as an
unmapped literal instead.

CVSS ``None`` band, emitted under this dataset's own label
----------------------------------------------------------
The 0.0 band is the CVSS standard's ``None``.  This dataset's vocabulary has no
``None`` label, so the standard's ``None`` band is emitted under this dataset's
own label ``Info``.  That single relabelling is a mapping this dataset defines,
not a CVSS label; the other four labels and all four boundaries are the
standard's.  :data:`POLICY_CVSS_NONE_AS_INFO` carries that statement as one
authored string for ``severity-map.md`` to quote.

Precedence: label over score
----------------------------
The native label governs whenever it is in the mapped vocabulary, and a CVSS
score is consulted only where no mapped label exists.  Either way the entry used
is recorded in ``selected_entry``.  :data:`POLICY_LABEL_OVER_SCORE` carries that
statement verbatim for the same reason.

Evaluation order in :func:`resolve`, in full:

1. ``sarif_level`` present and in table 1 -> that band, basis
   :data:`BASIS_SARIF_LEVEL`.
2. ``label`` present and in table 2 -> that band, basis :data:`BASIS_LABEL`.
   A score, if one was supplied, is not consulted; that is the point of the
   precedence rule.
3. No mapped label, and at least one *bandable* score candidate -> table 3,
   basis :data:`BASIS_CVSS_SCORE`.
4. No mapped label and no bandable candidate, but the label is itself a numeric
   CVSS base score in range -> table 3, basis :data:`BASIS_CVSS_SCORE`, with
   ``selected_entry["source"]`` recorded as ``"label"``.  Real artifacts put
   bare scores in the severity field, so this keeps the band correct however an
   adapter routes the value.  A CVSS *vector* string is excluded from this path
   by an explicit shape test (see below) and can never be read as a score.
5. A literal that survives all of the above -> ``Info`` disclosed as an unmapped
   literal, basis :data:`BASIS_UNMAPPED_LITERAL`.  Where more than one
   unmapped literal was supplied, the disclosed one is the ``sarif_level`` if
   there was one, else the ``label``, else the first score candidate as
   observed.
6. Nothing at all -> :meth:`SeverityResult.absent`: ``severity_native`` ``None``,
   ``severity_norm`` ``Info``, basis :data:`BASIS_NO_VOCABULARY` -- the absence
   stated rather than a level assumed.  This is the path for a tool that defines
   no severity vocabulary, and for any record whose severity field is null.

Unmapped literals are disclosed, never silently banded
------------------------------------------------------
A literal outside every mapped vocabulary maps to ``Info`` and is listed with
the rows it affected.  ``Info`` alone would be indistinguishable from a
deliberate ``Info``, so the basis and the literal are both recorded and
:class:`LiteralTally` is the tally ``severity-map.md`` is rendered from.

A CVSS *vector* string (``CVSS:3.1/AV:N/AC:L/...``) is an unmapped literal, not
a score: it is neither a mapped label nor a number.  ``Info`` is the right band
for it, but only as a disclosed unmapped literal carrying its row count.  The
shape is detected explicitly by :data:`CVSS_VECTOR_PATTERN` before any numeric
coercion is attempted.

Score candidate entries and the selection order
-----------------------------------------------
``resolve(scores=...)`` accepts a sequence of candidates, each either

* a mapping carrying the score under ``"score"`` (falling back to ``"value"``,
  ``"baseScore"``, ``"base_score"``), optionally a ``"source"`` (falling back to
  ``"type"``) and optionally a ``"version"`` (falling back to
  ``"cvss_version"``); unknown keys are ignored; or
* a bare number or numeric string.

An advisory commonly carries several scores from different sources, so the
selection is by a documented total order and the chosen entry is recorded:

1. highest CVSS version, compared as a numeric tuple parsed from the version
   string, with an absent or unparseable version sorting lowest;
2. then the highest score;
3. then the lexicographically smallest source, an absent source sorting first;
4. then the earliest index in the supplied sequence.

Step 4 makes the order total, so two calls on the same input cannot select
differently.  Only bandable candidates are eligible; a candidate that will not
coerce or falls outside range is ineligible rather than clamped.

Score rendering convention
--------------------------
Where a score is what ``severity_native`` records, it is rendered with
**exactly one decimal place** -- ``f"{value:.1f}"``.  So ``3.200000047683716``
becomes ``"3.2"``, ``7.5`` becomes ``"7.5"``, ``10.0`` becomes ``"10.0"`` and
``0.0`` becomes ``"0.0"``.  The full-precision value is kept in
``selected_entry["score"]``, where nothing is lost.
:data:`POLICY_SCORE_RENDERING` states the convention for ``severity-map.md``.

Scope of this module
--------------------
It maps literals to bands per tool and tallies them per tool.  It does not rank
tools, contrast their vocabularies, or characterise what a tool's severity
distribution shows: AAP §0.3.2 forbids cross-tool interpretation of any kind.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, fields
from typing import Any

__all__ = [
    "BASIS_CVSS_SCORE",
    "BASIS_LABEL",
    "BASIS_NO_VOCABULARY",
    "BASIS_SARIF_LEVEL",
    "BASIS_UNMAPPED_LITERAL",
    "BASIS_VALUES",
    "CANONICAL_TOOLS",
    "CVSS_BAND_TABLE",
    "CVSS_VECTOR_PATTERN",
    "LITERAL_KEY_FIELDS",
    "LiteralCount",
    "LiteralTally",
    "POLICY_CVSS_NONE_AS_INFO",
    "POLICY_LABEL_OVER_SCORE",
    "POLICY_SCORE_RENDERING",
    "POLICY_SELECTED_ENTRY_TALLY",
    "POLICY_STATEMENTS",
    "POLICY_UNMAPPED_DISCLOSURE",
    "SEVERITY_NORM",
    "SeverityPolicyError",
    "SeverityResult",
    "band_for_score",
    "cvss_band_table",
    "is_cvss_vector",
    "label_table",
    "render_score",
    "resolve",
    "sarif_level_table",
]


# --------------------------------------------------------------------------- #
# The closed output vocabulary                                                #
# --------------------------------------------------------------------------- #

#: The only values ``severity_norm`` may take, most severe first.  The order is
#: significant: it is the report order used by :class:`LiteralTally`.
SEVERITY_NORM: tuple[str, ...] = ("Critical", "High", "Medium", "Low", "Info")

#: The band every fallback path resolves to.  Named rather than inlined so the
#: three places that need it cannot drift apart.
INFO: str = "Info"


# --------------------------------------------------------------------------- #
# The basis constants: how a result's band was arrived at                     #
# --------------------------------------------------------------------------- #

#: The band came from the SARIF ``level`` table.
BASIS_SARIF_LEVEL: str = "sarif_level"

#: The band came from the native label vocabulary.
BASIS_LABEL: str = "label"

#: The band came from a CVSS numeric score; ``selected_entry`` names which one.
BASIS_CVSS_SCORE: str = "cvss_score"

#: The record carried no severity vocabulary at all; the absence is stated.
BASIS_NO_VOCABULARY: str = "no_vocabulary"

#: A literal outside every mapped vocabulary, banded ``Info`` and disclosed.
BASIS_UNMAPPED_LITERAL: str = "unmapped_literal"

#: Every legal value of :attr:`SeverityResult.basis`, in reporting order.
BASIS_VALUES: tuple[str, ...] = (
    BASIS_SARIF_LEVEL,
    BASIS_LABEL,
    BASIS_CVSS_SCORE,
    BASIS_NO_VOCABULARY,
    BASIS_UNMAPPED_LITERAL,
)


# --------------------------------------------------------------------------- #
# The nine canonical tool identifiers                                         #
# --------------------------------------------------------------------------- #

#: The nine canonical tool identifiers, hyphenated, in the order the scanner
#: class table lists them.  ``severity-map.md`` must carry an entry for every
#: one of these -- including a tool that contributed zero rows, which the
#: row-only dataset files cannot show.
CANONICAL_TOOLS: tuple[str, ...] = (
    "opengrep",
    "semgrep",
    "datadog-static-analyzer",
    "joern",
    "gitleaks",
    "checkov",
    "osv-scanner",
    "dependency-check",
    "trivy",
)


# --------------------------------------------------------------------------- #
# Authored policy statements, for severity-map.md to quote rather than         #
# paraphrase.  One authored string per statement, so the document and the code  #
# cannot disagree.                                                             #
# --------------------------------------------------------------------------- #

POLICY_CVSS_NONE_AS_INFO: str = (
    "The CVSS v3.1 qualitative scale (specification document, section 5) names "
    "five bands: None at 0.0, Low 0.1-3.9, Medium 4.0-6.9, High 7.0-8.9 and "
    "Critical 9.0-10.0. This dataset's severity_norm vocabulary has no None "
    "label, so the standard's None band is emitted under this dataset's own "
    "label Info. That relabelling is a mapping this dataset defines, not a "
    "CVSS label. The other four labels and all four boundaries are the "
    "standard's."
)

POLICY_LABEL_OVER_SCORE: str = (
    "Precedence when a label and one or more scores coexist: the native label "
    "governs whenever it is in the mapped vocabulary, and a CVSS score is "
    "consulted only where no mapped label exists. Either way the entry used is "
    "recorded -- the label, or the score with its source and version."
)

POLICY_SCORE_RENDERING: str = (
    "Where a CVSS score is what severity_native records, it is rendered with "
    "exactly one decimal place: 3.200000047683716 is recorded as 3.2, 7.5 as "
    "7.5, 10.0 as 10.0 and 0.0 as 0.0. Band selection is performed on the "
    "full-precision float, never on the rendered text, and the full-precision "
    "value is retained in the selected score entry."
)

POLICY_UNMAPPED_DISCLOSURE: str = (
    "A literal outside every mapped vocabulary maps to Info and is listed here "
    "with the rows it affected. A CVSS vector string is such a literal rather "
    "than a score: it is neither a mapped label nor a number, so it is "
    "disclosed as an unmapped literal instead of being banded numerically. A "
    "numeric value that will not coerce to a float, or that falls outside "
    "0.0-10.0, is likewise disclosed rather than clamped."
)

#: The four policy statements keyed by a stable identifier, so a document
#: generator can emit them all without hard-coding constant names.
POLICY_STATEMENTS: tuple[tuple[str, str], ...] = (
    ("label_over_score", POLICY_LABEL_OVER_SCORE),
    ("cvss_none_as_info", POLICY_CVSS_NONE_AS_INFO),
    ("score_rendering", POLICY_SCORE_RENDERING),
    ("unmapped_disclosure", POLICY_UNMAPPED_DISCLOSURE),
)


class SeverityPolicyError(ValueError):
    """A severity result or a tally key violated this module's invariants.

    Raised rather than asserted so the invariant survives ``python -O``, which
    strips ``assert`` statements.  Every occurrence is a programming fault in a
    caller -- a band outside :data:`SEVERITY_NORM`, a basis outside
    :data:`BASIS_VALUES`, or a tool identifier outside :data:`CANONICAL_TOOLS`
    -- and never a property of the scanned artifact.
    """


# --------------------------------------------------------------------------- #
# The return type                                                             #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class SeverityResult:
    """One record's severity, together with the basis on which it was decided.

    Attributes
    ----------
    severity_native:
        The literal as observed, neither upper-cased nor otherwise normalized.
        The single exception is that surrounding whitespace is stripped, which
        the policy sanctions explicitly, so that ``" Moderate "`` and
        ``"Moderate"`` are one literal in the tally rather than two.  ``None``
        where the record carried no severity vocabulary; absence is permitted
        for this field.  Where a numeric score is what was recorded, this is the
        one-decimal rendering described by :data:`POLICY_SCORE_RENDERING`.
    severity_norm:
        One of :data:`SEVERITY_NORM`.  Never ``None`` and never absent.
    basis:
        One of :data:`BASIS_VALUES` -- how the band was arrived at.
    selected_entry:
        The record of what was *used*.  Exactly two shapes occur, and the key
        set of each is **closed** -- validation rejects any other key:
        ``{"label": "<observed literal>"}`` for a label or a SARIF level, and
        ``{"score": <float>, "source": <str | None>, "version": <str | None>}``
        for a score.  ``None`` on the unmapped and no-vocabulary paths, where
        nothing was used to derive the band -- the band came from policy.  The
        key set is closed so that the four fields :class:`LiteralTally` keys on
        are a *complete* representation of the entry: were an unlisted key
        permitted, two entries differing only in it would collapse into one
        tally bucket and the provenance AAP §0.5.4 requires recorded would be
        lost at aggregation.
    unmapped_literal:
        The disclosed literal, set if and only if :attr:`basis` is
        :data:`BASIS_UNMAPPED_LITERAL`.

    The instance is frozen and its ``selected_entry`` is copied on construction,
    so a caller cannot mutate a result after the fact.  Instances are not
    hashable in practice because ``selected_entry`` is a ``dict``; nothing in
    this module hashes them, and :class:`LiteralTally` keys on the seven fields
    named by :data:`LITERAL_KEY_FIELDS` instead -- the literal, the band, the
    basis *and* the selected entry decomposed into its four scalar parts.
    """

    severity_native: str | None
    severity_norm: str
    basis: str
    selected_entry: dict[str, Any] | None = None
    unmapped_literal: str | None = None

    def __post_init__(self) -> None:
        """Enforce every invariant at construction, on every path.

        This is what makes ``severity_norm is never absent`` a hard invariant
        rather than a convention: there is no way to obtain a
        :class:`SeverityResult` that violates it, whichever code path built it.
        """
        if self.severity_norm not in SEVERITY_NORM:
            raise SeverityPolicyError(
                "severity_norm must be one of "
                f"{SEVERITY_NORM!r}, got {self.severity_norm!r}"
            )
        if self.basis not in BASIS_VALUES:
            raise SeverityPolicyError(
                f"basis must be one of {BASIS_VALUES!r}, got {self.basis!r}"
            )
        if self.severity_native is not None and not isinstance(
            self.severity_native, str
        ):
            raise SeverityPolicyError(
                "severity_native must be a str or None, got "
                f"{type(self.severity_native).__name__}"
            )
        if self.basis == BASIS_UNMAPPED_LITERAL:
            if self.unmapped_literal is None:
                raise SeverityPolicyError(
                    "unmapped_literal must be set when basis is "
                    f"{BASIS_UNMAPPED_LITERAL!r}"
                )
        elif self.unmapped_literal is not None:
            raise SeverityPolicyError(
                "unmapped_literal must be None unless basis is "
                f"{BASIS_UNMAPPED_LITERAL!r}, got basis {self.basis!r}"
            )
        if self.basis == BASIS_NO_VOCABULARY and self.severity_native is not None:
            raise SeverityPolicyError(
                "severity_native must be None when basis is "
                f"{BASIS_NO_VOCABULARY!r}, got {self.severity_native!r}"
            )
        if self.selected_entry is None:
            if self.basis in (BASIS_SARIF_LEVEL, BASIS_LABEL, BASIS_CVSS_SCORE):
                raise SeverityPolicyError(
                    f"selected_entry is required when basis is {self.basis!r}"
                )
        else:
            if not isinstance(self.selected_entry, Mapping):
                raise SeverityPolicyError(
                    "selected_entry must be a mapping or None, got "
                    f"{type(self.selected_entry).__name__}"
                )
            _validate_selected_entry(self.basis, self.selected_entry)
            # Copy so a caller's dict cannot be mutated into this frozen result
            # after construction.  object.__setattr__ is the sanctioned way to
            # assign to a frozen dataclass from inside __post_init__.
            object.__setattr__(self, "selected_entry", dict(self.selected_entry))

    @classmethod
    def absent(cls) -> SeverityResult:
        """The result for a record carrying no severity vocabulary at all.

        ``severity_native`` ``None``, ``severity_norm`` ``Info``, basis
        :data:`BASIS_NO_VOCABULARY` -- the absence stated rather than a level
        assumed.  This is the path for a tool that defines no severity
        vocabulary and for any record whose severity field is null.
        """
        return cls(
            severity_native=None,
            severity_norm=INFO,
            basis=BASIS_NO_VOCABULARY,
            selected_entry=None,
            unmapped_literal=None,
        )

    @classmethod
    def unmapped(cls, literal: str) -> SeverityResult:
        """The disclosed result for a literal outside every mapped vocabulary.

        ``Info``, with the literal recorded in both ``severity_native`` and
        ``unmapped_literal`` so :class:`LiteralTally` can list it with the rows
        it affected.
        """
        if not isinstance(literal, str):
            raise SeverityPolicyError(
                f"unmapped literal must be a str, got {type(literal).__name__}"
            )
        return cls(
            severity_native=literal,
            severity_norm=INFO,
            basis=BASIS_UNMAPPED_LITERAL,
            selected_entry=None,
            unmapped_literal=literal,
        )


#: The only keys a label or SARIF-level ``selected_entry`` may carry.
_LABEL_ENTRY_KEYS: frozenset[str] = frozenset({"label"})

#: The only keys a CVSS-score ``selected_entry`` may carry.
_SCORE_ENTRY_KEYS: frozenset[str] = frozenset({"score", "source", "version"})


def _validate_selected_entry(basis: str, entry: Mapping[str, Any]) -> None:
    """Check ``selected_entry`` carries one of the two documented shapes, exactly.

    Both the required keys and the *permitted* keys are enforced.  Closing the
    key set is what makes :data:`LITERAL_KEY_FIELDS` a complete representation
    of the entry: :class:`LiteralTally` decomposes an entry into ``label``,
    ``score``, ``source`` and ``version`` and keys on those four, so a key
    outside the closed set would be dropped at aggregation and two selections
    differing only in it would be counted as one.  Rejecting the key here,
    where it is a programming fault in a caller, is stronger than hashing an
    opaque remainder into the key: it keeps the tally's identity readable and
    every column of it named.

    The scalar types are enforced for the same reason -- a key whose value type
    varies cannot be ordered deterministically in :meth:`LiteralTally.entries`.
    """
    if basis in (BASIS_SARIF_LEVEL, BASIS_LABEL):
        if "label" not in entry:
            raise SeverityPolicyError(
                f"selected_entry for basis {basis!r} must carry a 'label' key, "
                f"got keys {sorted(entry)!r}"
            )
        if not isinstance(entry["label"], str):
            raise SeverityPolicyError(
                "selected_entry['label'] must be a str, got "
                f"{type(entry['label']).__name__}"
            )
        unexpected = set(entry) - _LABEL_ENTRY_KEYS
        if unexpected:
            raise SeverityPolicyError(
                f"selected_entry for basis {basis!r} may carry only "
                f"{sorted(_LABEL_ENTRY_KEYS)!r}, got unexpected "
                f"{sorted(unexpected)!r}"
            )
        return
    if basis == BASIS_CVSS_SCORE:
        if "score" not in entry:
            raise SeverityPolicyError(
                f"selected_entry for basis {basis!r} must carry a 'score' key, "
                f"got keys {sorted(entry)!r}"
            )
        if not isinstance(entry["score"], float):
            raise SeverityPolicyError(
                "selected_entry['score'] must be a float, got "
                f"{type(entry['score']).__name__}"
            )
        for optional in ("source", "version"):
            value = entry.get(optional)
            if value is not None and not isinstance(value, str):
                raise SeverityPolicyError(
                    f"selected_entry[{optional!r}] must be a str or None, got "
                    f"{type(value).__name__}"
                )
        unexpected = set(entry) - _SCORE_ENTRY_KEYS
        if unexpected:
            raise SeverityPolicyError(
                f"selected_entry for basis {basis!r} may carry only "
                f"{sorted(_SCORE_ENTRY_KEYS)!r}, got unexpected "
                f"{sorted(unexpected)!r}"
            )
        return
    raise SeverityPolicyError(
        f"basis {basis!r} does not take a selected_entry, got {sorted(entry)!r}"
    )


# --------------------------------------------------------------------------- #
# Table 1: SARIF level                                                        #
# --------------------------------------------------------------------------- #

# Keys are lower-case; the lookup lower-cases the observed literal so a producer
# differing only in case maps rather than falling through to unmapped.
_SARIF_LEVEL_MAP: dict[str, str] = {
    "error": "High",
    "warning": "Medium",
    "note": "Low",
    "none": INFO,
}


def sarif_level_table() -> dict[str, str]:
    """Return table 1 as a fresh dict, for a document generator to render."""
    return dict(_SARIF_LEVEL_MAP)


# --------------------------------------------------------------------------- #
# Table 2: label vocabularies                                                 #
# --------------------------------------------------------------------------- #

# Keys are upper-case; the lookup strips and upper-cases the observed literal.
# Case folding is a measured requirement, not a nicety: artifacts carry both
# ``MODERATE`` and lower-case ``moderate`` for the same band.
_LABEL_MAP: dict[str, str] = {
    "CRITICAL": "Critical",
    "HIGH": "High",
    "MODERATE": "Medium",
    "MEDIUM": "Medium",
    "LOW": "Low",
    "NEGLIGIBLE": INFO,
    "INFO": INFO,
    "INFORMATIONAL": INFO,
    "UNKNOWN": INFO,
    "NONE": INFO,
}


def label_table() -> dict[str, str]:
    """Return table 2 as a fresh dict, for a document generator to render."""
    return dict(_LABEL_MAP)


# --------------------------------------------------------------------------- #
# Table 3: CVSS numeric score                                                 #
# --------------------------------------------------------------------------- #

#: The CVSS v3.1 §5 qualitative scale as data, most severe first: band label,
#: inclusive lower bound, inclusive upper bound.  This is the display form
#: ``severity-map.md`` renders; :func:`band_for_score` implements the same
#: boundaries as ordered numeric comparisons, and the two are cross-checked
#: against each other by this module's self-check.
CVSS_BAND_TABLE: tuple[tuple[str, float, float], ...] = (
    ("Critical", 9.0, 10.0),
    ("High", 7.0, 8.9),
    ("Medium", 4.0, 6.9),
    ("Low", 0.1, 3.9),
    (INFO, 0.0, 0.0),
)

#: The lowest and highest score this scale defines.  A value outside the closed
#: interval is not banded and not clamped -- it is disclosed as unmapped.
CVSS_SCORE_MIN: float = 0.0
CVSS_SCORE_MAX: float = 10.0


def cvss_band_table() -> tuple[tuple[str, float, float], ...]:
    """Return table 3 as data, for a document generator to render."""
    return CVSS_BAND_TABLE


def band_for_score(score: float) -> str:
    """Band a CVSS numeric score, by ordered numeric comparison.

    ``>= 9.0`` Critical, ``>= 7.0`` High, ``>= 4.0`` Medium, ``> 0.0`` Low,
    ``== 0.0`` Info.  The comparison is numeric and never lexical, so scores
    carrying float32-to-float64 representation artifacts such as
    ``3.200000047683716`` band correctly.

    Raises
    ------
    SeverityPolicyError
        If ``score`` is not a finite number within ``0.0``-``10.0``.  Callers
        that must tolerate an out-of-range value test it with
        :func:`_bandable_score` first and disclose it as an unmapped literal;
        this function never clamps.
    """
    value = _finite_float(score)
    if value is None or not (CVSS_SCORE_MIN <= value <= CVSS_SCORE_MAX):
        raise SeverityPolicyError(
            f"score must be a finite number within "
            f"{CVSS_SCORE_MIN}-{CVSS_SCORE_MAX}, got {score!r}"
        )
    if value >= 9.0:
        return "Critical"
    if value >= 7.0:
        return "High"
    if value >= 4.0:
        return "Medium"
    if value > 0.0:
        return "Low"
    return INFO


def render_score(score: float) -> str:
    """Render a score for ``severity_native``: exactly one decimal place.

    ``3.200000047683716`` -> ``"3.2"``, ``7.5`` -> ``"7.5"``, ``10.0`` ->
    ``"10.0"``, ``0.0`` -> ``"0.0"``.  Banding is always performed on the
    full-precision float, never on this text, and the full-precision value is
    kept in ``selected_entry["score"]``.
    """
    value = _finite_float(score)
    if value is None:
        raise SeverityPolicyError(f"score must be a finite number, got {score!r}")
    return f"{value:.1f}"



# --------------------------------------------------------------------------- #
# Literal shapes: CVSS vectors, numeric coercion, observed text               #
# --------------------------------------------------------------------------- #

#: A CVSS vector string, recognised by its ``CVSS:`` prefix.  Matched against
#: the whitespace-stripped literal, case-insensitively.  A vector is neither a
#: mapped label nor a number, so it is disclosed as an unmapped literal; this
#: explicit test exists so a vector can never be mistaken for a score, even
#: though it would also fail numeric coercion.
CVSS_VECTOR_PATTERN: re.Pattern[str] = re.compile(r"^CVSS:", re.IGNORECASE)

#: Keys a score candidate mapping may carry the score under, in precedence
#: order.  Unknown keys are ignored.
_SCORE_KEYS: tuple[str, ...] = ("score", "value", "baseScore", "base_score")

#: Keys a score candidate mapping may carry its source under.
_SOURCE_KEYS: tuple[str, ...] = ("source", "type")

#: Keys a score candidate mapping may carry its CVSS version under.
_VERSION_KEYS: tuple[str, ...] = ("version", "cvss_version")

#: How many numeric components of a CVSS version string participate in the
#: selection order.  Padding to a fixed width keeps the comparison total, so
#: ``3.1`` sorts above ``3`` rather than below it.
_VERSION_KEY_WIDTH: int = 4


def is_cvss_vector(literal: Any) -> bool:
    """Return whether ``literal`` is a CVSS vector string rather than a score."""
    if not isinstance(literal, str):
        return False
    return CVSS_VECTOR_PATTERN.match(literal.strip()) is not None


def _finite_float(value: Any) -> float | None:
    """Coerce to a finite ``float``, or return ``None`` if that is impossible.

    ``bool`` is rejected even though it is an ``int`` subclass: a boolean in a
    severity field is not a score, and silently reading ``True`` as ``1.0``
    would be an inference this policy does not make.  ``NaN`` and the
    infinities are rejected without importing ``math``, which is outside this
    normalizer's permitted import set.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        candidate = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text or is_cvss_vector(text):
            return None
        try:
            candidate = float(text)
        except ValueError:
            return None
    else:
        return None
    if candidate != candidate:  # NaN is the only value unequal to itself.
        return None
    if candidate in (float("inf"), float("-inf")):
        return None
    return candidate


def _bandable_score(value: Any) -> float | None:
    """Coerce to a bandable score, or ``None`` if it is not one.

    Bandable means: coerces to a finite ``float`` and lies within the closed
    interval ``0.0``-``10.0``.  A value outside that interval is *not* clamped;
    returning ``None`` is what routes it to disclosure as an unmapped literal.
    """
    candidate = _finite_float(value)
    if candidate is None:
        return None
    if not (CVSS_SCORE_MIN <= candidate <= CVSS_SCORE_MAX):
        return None
    return candidate


def _clean_literal(value: Any) -> str | None:
    """Normalize an incoming literal to a stripped string, or ``None``.

    Surrounding whitespace is stripped -- the one transformation the policy
    sanctions -- and nothing else is done to the literal.  An empty or
    whitespace-only literal is treated as an absence rather than as a literal to
    disclose, because there is no literal there to list.  A non-string value is
    rendered with ``str()`` so it can still be disclosed rather than discarded.
    """
    if value is None:
        return None
    text = value.strip() if isinstance(value, str) else str(value).strip()
    return text or None


def _version_key(version: Any) -> tuple[int, ...]:
    """Return a total sort key for a CVSS version string.

    The numeric components are parsed in order and padded to a fixed width, so
    ``"3.1"`` sorts above ``"3"`` and both sort above an absent or unparseable
    version, which takes an all-negative key.
    """
    if version is None:
        return (-1,) * _VERSION_KEY_WIDTH
    text = version if isinstance(version, str) else str(version)
    numbers = [int(part) for part in re.findall(r"\d+", text)]
    if not numbers:
        return (-1,) * _VERSION_KEY_WIDTH
    padded = (numbers + [0] * _VERSION_KEY_WIDTH)[:_VERSION_KEY_WIDTH]
    return tuple(padded)


@dataclass(frozen=True)
class _ScoreCandidate:
    """One parsed score candidate, with everything the selection order needs."""

    index: int
    score: float | None
    source: str | None
    version: str | None
    observed: str


def _first_present(entry: Mapping[str, Any], keys: Iterable[str]) -> Any:
    """Return the value of the first key present in ``entry``, else ``None``."""
    for key in keys:
        if key in entry:
            return entry[key]
    return None


def _parse_candidate(index: int, raw: Any) -> _ScoreCandidate:
    """Parse one score candidate: a mapping, or a bare number or numeric string.

    A candidate that will not coerce, that falls outside ``0.0``-``10.0``, or
    that is a CVSS vector carries ``score=None`` and is therefore ineligible for
    selection.  Its observed text is retained so it can be disclosed if no
    candidate is bandable.

    A mapping carrying none of the recognised score keys yields an empty
    observed text: it holds no severity vocabulary to disclose, so it
    contributes nothing rather than putting a mapping's ``repr`` into a dataset
    field.  Whether such a record is rejected outright is the adapter's
    decision, not this policy's.
    """
    if isinstance(raw, Mapping):
        raw_score = _first_present(raw, _SCORE_KEYS)
        raw_source = _first_present(raw, _SOURCE_KEYS)
        raw_version = _first_present(raw, _VERSION_KEYS)
    else:
        raw_score = raw
        raw_source = None
        raw_version = None
    source = _clean_literal(raw_source)
    version = _clean_literal(raw_version)
    observed = _clean_literal(raw_score) or ""
    return _ScoreCandidate(
        index=index,
        score=_bandable_score(raw_score),
        source=source,
        version=version,
        observed=observed,
    )


def _parse_candidates(scores: Any) -> list[_ScoreCandidate]:
    """Parse every supplied score candidate, preserving the supplied order.

    A single mapping, or a lone number or string, is accepted as a one-element
    sequence, so an adapter holding exactly one score need not wrap it.
    """
    if scores is None:
        return []
    if isinstance(scores, Mapping) or isinstance(scores, (int, float, str)):
        raw_items: Sequence[Any] = [scores]
    elif isinstance(scores, Sequence):
        raw_items = scores
    elif isinstance(scores, Iterable):
        raw_items = list(scores)
    else:
        raw_items = [scores]
    return [_parse_candidate(index, raw) for index, raw in enumerate(raw_items)]


def _selection_key(candidate: _ScoreCandidate) -> tuple[Any, ...]:
    """The total order documented in the module docstring, as a sort key.

    Ascending: highest CVSS version first (negated component-wise), then highest
    score, then the lexicographically smallest source, then the earliest index.
    The final component makes the order total, so two calls on the same input
    cannot select differently.
    """
    version_key = tuple(-part for part in _version_key(candidate.version))
    score = candidate.score if candidate.score is not None else CVSS_SCORE_MIN
    return (version_key, -score, candidate.source or "", candidate.index)


def _select_candidate(candidates: Sequence[_ScoreCandidate]) -> _ScoreCandidate:
    """Select one candidate by the documented total order."""
    return sorted(candidates, key=_selection_key)[0]


# --------------------------------------------------------------------------- #
# The single public resolver                                                  #
# --------------------------------------------------------------------------- #


def resolve(
    *,
    label: Any = None,
    scores: Any = None,
    sarif_level: Any = None,
) -> SeverityResult:
    """Resolve one record's severity into a band, recording the basis.

    Every argument is keyword-only, so a caller cannot transpose a label and a
    level.  The evaluation order is the one set out in the module docstring:

    1. a ``sarif_level`` in table 1;
    2. a ``label`` in table 2 -- a score, if any, is not consulted;
    3. otherwise the highest-precedence bandable score candidate;
    4. otherwise a ``label`` that is itself a numeric score in range;
    5. otherwise the surviving literal, disclosed as unmapped and banded
       ``Info``;
    6. otherwise :meth:`SeverityResult.absent`.

    Parameters
    ----------
    label:
        The record's native severity literal, as observed.  Not necessarily a
        string: a numeric severity field is accepted and, at step 4, banded.
    scores:
        Zero or more CVSS score candidates.  Each is a mapping carrying the
        score under ``score``/``value``/``baseScore``/``base_score`` with an
        optional ``source``/``type`` and ``version``/``cvss_version``, or a bare
        number or numeric string.  A single candidate need not be wrapped in a
        sequence.
    sarif_level:
        A SARIF ``level`` value.  Where a SARIF result carries no ``level`` and
        a rule property supplies a label instead, the caller passes that as
        ``label`` rather than as ``sarif_level``.

    Returns
    -------
    SeverityResult
        Always.  ``severity_norm`` is always one of :data:`SEVERITY_NORM`; this
        function has no code path that returns ``None`` for it and no code path
        that raises for artifact content.  :class:`SeverityPolicyError` can
        still surface a programming fault in a caller.
    """
    level_text = _clean_literal(sarif_level)
    label_text = _clean_literal(label)

    # 1. SARIF level.
    if level_text is not None:
        band = _SARIF_LEVEL_MAP.get(level_text.lower())
        if band is not None:
            return SeverityResult(
                severity_native=level_text,
                severity_norm=band,
                basis=BASIS_SARIF_LEVEL,
                selected_entry={"label": level_text},
            )

    # 2. A label in the mapped vocabulary governs, whatever the scores say.
    if label_text is not None:
        band = _LABEL_MAP.get(label_text.upper())
        if band is not None:
            return SeverityResult(
                severity_native=label_text,
                severity_norm=band,
                basis=BASIS_LABEL,
                selected_entry={"label": label_text},
            )

    # 3. No mapped label: consult the score candidates.
    candidates = _parse_candidates(scores)
    bandable = [candidate for candidate in candidates if candidate.score is not None]
    if bandable:
        chosen = _select_candidate(bandable)
        # mypy/readers: chosen.score is not None by construction of `bandable`.
        score = chosen.score if chosen.score is not None else CVSS_SCORE_MIN
        return SeverityResult(
            severity_native=render_score(score),
            severity_norm=band_for_score(score),
            basis=BASIS_CVSS_SCORE,
            selected_entry={
                "score": score,
                "source": chosen.source,
                "version": chosen.version,
            },
        )

    # 4. A severity literal that is itself a numeric score in range.  A CVSS
    #    vector is excluded explicitly so it can never be read as a score.
    if label_text is not None and not is_cvss_vector(label_text):
        numeric = _bandable_score(label_text)
        if numeric is not None:
            return SeverityResult(
                severity_native=render_score(numeric),
                severity_norm=band_for_score(numeric),
                basis=BASIS_CVSS_SCORE,
                selected_entry={
                    "score": numeric,
                    "source": "label",
                    "version": None,
                },
            )

    # 5. Disclose the surviving literal.  Where more than one was supplied the
    #    SARIF level is disclosed first, then the label, then the first score
    #    candidate as observed -- a fixed order, so the disclosure is
    #    reproducible.
    for literal in (level_text, label_text):
        if literal is not None:
            return SeverityResult.unmapped(literal)
    for candidate in candidates:
        if candidate.observed:
            return SeverityResult.unmapped(candidate.observed)

    # 6. Nothing at all: state the absence rather than assume a level.
    return SeverityResult.absent()



# --------------------------------------------------------------------------- #
# The tally severity-map.md is rendered from                                  #
# --------------------------------------------------------------------------- #


POLICY_SELECTED_ENTRY_TALLY: str = (
    "The entry that governed a band is part of a tallied literal's identity, "
    "not a detail discarded when rows are aggregated. A bucket is keyed on the "
    "observed literal, the band, the basis and the selected entry decomposed "
    "into its four scalar parts -- the label, and a score's value, source and "
    "version -- each carried as its own field rather than as one rendered "
    "string. So two advisories scored 7.5 by different sources, or under "
    "different CVSS versions, are reported as two entries naming their sources "
    "rather than as one entry that names neither."
)
# Deliberately not a member of POLICY_STATEMENTS: those four are statements
# about the *mapping* -- how a literal becomes a band -- and this one is about
# the tally's identity, which is downstream of the mapping.  It reaches
# severity-map.md through its own key in the normalizer's run record.


def _optional_text_key(value: str | None) -> tuple[bool, str]:
    """A total sort key for an optional string: present values first, then text.

    ``None`` cannot be compared with ``str``, so an optional column needs a
    two-part key rather than the bare value.  Absence sorts *after* every
    present value, matching the existing convention that the absent literal is
    reported below the literals.
    """
    return (value is None, value or "")


def _optional_score_key(value: float | None) -> tuple[bool, float]:
    """A total sort key for an optional score: present values first, then value.

    Ascending by score, so the smallest score sorts first.  The band already
    orders the report by severity, so ordering within one band by ascending
    score merely has to be *stable and total*, which this is.
    """
    return (value is None, value if value is not None else CVSS_SCORE_MIN)


@dataclass(frozen=True)
class _LiteralKey:
    """One tally bucket's identity: the literal, the band, and what was used.

    Frozen, so it hashes, and every field is a scalar, so the hash and the sort
    order are pure functions of the content.

    AAP §0.5.4 requires that *"the entry used is recorded -- the label, or the
    score with its source and version"*.  Keying on the literal alone -- or on
    the ``(literal, band, basis)`` triple this class replaced -- satisfied that
    requirement per record and then destroyed it per report: a score is
    recorded in ``severity_native`` as its one-decimal rendering, so two
    advisories scored 7.5 by different sources, or under CVSS 3.1 and 4.0,
    produced an identical triple and collapsed into a single bucket whose
    provenance was whichever entry happened to arrive first -- and nothing
    downstream could tell that a collapse had happened.

    The selected entry is decomposed into its four scalar parts rather than
    stored whole for two reasons.  A ``dict`` is unhashable, so it cannot be a
    key at all; and a *rendered* string -- ``"7.5 (NVD:cvssv3, 3.1)"`` -- would
    make the report's columns unparseable by anything but a human, where the
    four fields can be read, sorted and compared directly.  The decomposition
    is complete because :func:`_validate_selected_entry` closes the key set of
    both entry shapes.
    """

    severity_native: str | None
    severity_norm: str
    basis: str
    selected_label: str | None
    selected_score: float | None
    selected_source: str | None
    selected_version: str | None

    @classmethod
    def from_result(cls, result: SeverityResult) -> _LiteralKey:
        """Decompose one :class:`SeverityResult` into its bucket identity.

        A result with no selected entry -- the unmapped and no-vocabulary paths,
        where the band came from policy rather than from anything observed --
        yields ``None`` in all four provenance fields.  That is a distinct
        identity from a selection whose fields happen to be absent, because the
        basis is part of the key.
        """
        entry: Mapping[str, Any] = (
            result.selected_entry if result.selected_entry is not None else {}
        )
        return cls(
            severity_native=result.severity_native,
            severity_norm=result.severity_norm,
            basis=result.basis,
            selected_label=entry.get("label"),
            selected_score=entry.get("score"),
            selected_source=entry.get("source"),
            selected_version=entry.get("version"),
        )

    def sort_key(self) -> tuple[Any, ...]:
        """The report order for this bucket, as a total sort key.

        Band first (most severe first, following :data:`SEVERITY_NORM`), then
        the literal with the absent literal last, then the basis, then the four
        provenance fields in the order :data:`LITERAL_KEY_FIELDS` names them.
        Every component is a comparable scalar or a fixed-shape tuple, so the
        order is total: two runs over the same rows cannot order two buckets
        differently, whatever order they were recorded in.

        The first three components are exactly the order this class replaced, so
        extending the key added entries where a collapse used to occur without
        moving any entry that did not collapse.
        """
        return (
            SEVERITY_NORM.index(self.severity_norm),
            _optional_text_key(self.severity_native),
            self.basis,
            _optional_text_key(self.selected_label),
            _optional_score_key(self.selected_score),
            _optional_text_key(self.selected_source),
            _optional_text_key(self.selected_version),
        )


#: The field names one tally bucket is keyed on, in key order.  Derived from
#: :class:`_LiteralKey` rather than authored beside it, so the two cannot drift;
#: :class:`LiteralCount` carries every one of them under the same name, which
#: the self-check asserts.  A record consumer reads this to know which columns
#: constitute a literal's identity rather than inferring it from the data.
LITERAL_KEY_FIELDS: tuple[str, ...] = tuple(
    field.name for field in fields(_LiteralKey)
)


@dataclass(frozen=True)
class LiteralCount:
    """One observed native literal for one tool, with the rows it affected.

    Attributes
    ----------
    tool:
        A canonical tool identifier from :data:`CANONICAL_TOOLS`.
    severity_native:
        The observed literal, or ``None`` where the rows carried no severity
        vocabulary.
    severity_norm:
        The band those rows took.
    basis:
        The basis on which they took it, from :data:`BASIS_VALUES`.
    selected_label:
        The label or SARIF level that was *used*, where one was; ``None`` on
        every other path.  Equal to ``severity_native`` on the label and
        SARIF-level paths by construction, and carried separately so that the
        provenance column can be read without knowing which basis implies it.
    selected_score:
        The full-precision score that was used, where a score was; ``None``
        otherwise.  ``severity_native`` carries this score's one-decimal
        rendering, so this field -- not that one -- is what a reader compares
        when two entries differ below the first decimal place.
    selected_source:
        The score entry's source as the artifact spelled it, where the entry
        named one; ``None`` where it did not, and on every non-score path.
    selected_version:
        The score entry's CVSS version as the artifact spelled it, on the same
        terms.
    rows:
        How many rows carried this literal *and this selection* for this tool.
    unmapped:
        Whether ``basis`` is :data:`BASIS_UNMAPPED_LITERAL`.  Redundant with
        ``basis`` by construction, and present because it is the column
        ``severity-map.md`` needs to list the unmapped literals with their row
        counts.

    The four ``selected_*`` fields are the decomposition described by
    :data:`POLICY_SELECTED_ENTRY_TALLY`, carried here under the same names
    :data:`LITERAL_KEY_FIELDS` uses so that a serialised entry states its own
    identity.  ``cli._severity_record`` serialises these instances with
    ``dataclasses.asdict``, so they reach the run record -- and therefore
    ``severity-map.md`` -- without a second mapping layer that could drop a
    column.
    """

    tool: str
    severity_native: str | None
    severity_norm: str
    basis: str
    selected_label: str | None
    selected_score: float | None
    selected_source: str | None
    selected_version: str | None
    rows: int
    unmapped: bool


class LiteralTally:
    """Counts observed severity literals per tool, for ``severity-map.md``.

    Entries are keyed on the seven fields :data:`LITERAL_KEY_FIELDS` names --
    the literal, the band, the basis, and the selected entry decomposed into its
    label, score, source and version -- rather than on the literal alone.  For a
    fixed policy the same literal always resolves to the same band, so the first
    three fields split nothing in practice; they mean that if a literal ever did
    resolve two ways, the readout would show two entries rather than one
    corrupted count.  The last four are load-bearing rather than defensive: a
    score reaches ``severity_native`` as a one-decimal rendering, so without
    them two advisories scored 7.5 by different sources -- or under different
    CVSS versions -- were one bucket, and the provenance AAP §0.5.4 requires
    recorded could not reach ``severity-map.md`` at all.
    :data:`POLICY_SELECTED_ENTRY_TALLY` states that contract for the document to
    quote.

    A tool that contributed zero rows still needs an entry, because
    ``findings.json`` and ``findings.csv`` are row-only and cannot show it.
    Seeding is what provides that: :meth:`with_all_tools` (or :meth:`seed`)
    registers an identifier so it appears in :meth:`by_tool` with an empty
    entry tuple and a row count of zero.

    An unknown tool identifier is rejected rather than silently creating a tenth
    bucket, so a typo cannot quietly split one tool's counts in two.

    This class counts per tool and nothing more.  It does not rank tools or
    compare their vocabularies; AAP §0.3.2 forbids cross-tool interpretation.
    """

    __slots__ = ("_counts",)

    def __init__(self, tools: Iterable[str] | None = None) -> None:
        """Create a tally, optionally seeding it with tool identifiers."""
        self._counts: dict[str, dict[_LiteralKey, int]] = {}
        if tools is not None:
            self.seed(*tools)

    @classmethod
    def with_all_tools(cls) -> LiteralTally:
        """Create a tally seeded with all nine canonical tool identifiers.

        This is the constructor the normalizer uses, so that every one of the
        nine appears in ``severity-map.md`` whether or not it produced a row.
        """
        return cls(CANONICAL_TOOLS)

    @staticmethod
    def _validate_tool(tool: Any) -> str:
        """Return ``tool`` if it is canonical, else raise loudly."""
        if not isinstance(tool, str):
            raise SeverityPolicyError(
                f"tool identifier must be a str, got {type(tool).__name__}"
            )
        if tool not in CANONICAL_TOOLS:
            raise SeverityPolicyError(
                f"unknown tool identifier {tool!r}; the canonical identifiers "
                f"are {CANONICAL_TOOLS!r}"
            )
        return tool

    def seed(self, *tools: str) -> None:
        """Register tool identifiers so they report even with zero rows."""
        for tool in tools:
            self._counts.setdefault(self._validate_tool(tool), {})

    def record(self, tool: str, result: SeverityResult) -> None:
        """Count one row's severity result against one tool.

        The whole result contributes to the bucket identity, including the entry
        that was selected: :meth:`_LiteralKey.from_result` decomposes it, so
        nothing about the result is discarded on its way into the tally.

        Raises
        ------
        SeverityPolicyError
            If ``tool`` is not a canonical identifier, or ``result`` is not a
            :class:`SeverityResult`.
        """
        key_tool = self._validate_tool(tool)
        if not isinstance(result, SeverityResult):
            raise SeverityPolicyError(
                "result must be a SeverityResult, got "
                f"{type(result).__name__}"
            )
        bucket = self._counts.setdefault(key_tool, {})
        key = _LiteralKey.from_result(result)
        bucket[key] = bucket.get(key, 0) + 1

    def tools(self) -> tuple[str, ...]:
        """Every tracked tool identifier, in canonical order."""
        return tuple(tool for tool in CANONICAL_TOOLS if tool in self._counts)

    def entries(self, tool: str) -> tuple[LiteralCount, ...]:
        """Every literal observed for ``tool``, in a stable report order.

        Ordered by :meth:`_LiteralKey.sort_key`: band (most severe first,
        following :data:`SEVERITY_NORM`), then literals before the absent
        literal, then the literal text, then the basis, then the four provenance
        fields.  The order is a pure function of the content -- it reads nothing
        but the key -- so two runs over the same rows render an identical
        document whatever order the rows arrived in.

        Each returned :class:`LiteralCount` carries its bucket's whole identity,
        so an entry states which selection it counted rather than leaving a
        reader to assume there was only one.
        """
        key_tool = self._validate_tool(tool)
        bucket = self._counts.get(key_tool, {})
        ordered = sorted(bucket.items(), key=lambda item: item[0].sort_key())
        return tuple(
            LiteralCount(
                tool=key_tool,
                severity_native=key.severity_native,
                severity_norm=key.severity_norm,
                basis=key.basis,
                selected_label=key.selected_label,
                selected_score=key.selected_score,
                selected_source=key.selected_source,
                selected_version=key.selected_version,
                rows=rows,
                unmapped=key.basis == BASIS_UNMAPPED_LITERAL,
            )
            for key, rows in ordered
        )

    def by_tool(self) -> dict[str, tuple[LiteralCount, ...]]:
        """Every tracked tool's entries, keyed by identifier in canonical order.

        A seeded tool that recorded nothing maps to an empty tuple, which is how
        a zero-row tool still reaches ``severity-map.md``.
        """
        return {tool: self.entries(tool) for tool in self.tools()}

    def unmapped_by_tool(self) -> dict[str, tuple[LiteralCount, ...]]:
        """Only the unmapped literals, per tool, with the rows they affected.

        This is the disclosure AAP §0.5.4 requires: a literal outside every
        mapped vocabulary maps to ``Info`` and is listed with its rows.
        """
        return {
            tool: tuple(entry for entry in self.entries(tool) if entry.unmapped)
            for tool in self.tools()
        }

    def row_count(self, tool: str) -> int:
        """How many rows were recorded for ``tool``."""
        key_tool = self._validate_tool(tool)
        return sum(self._counts.get(key_tool, {}).values())

    def total_rows(self) -> int:
        """How many rows were recorded across every tracked tool."""
        return sum(sum(bucket.values()) for bucket in self._counts.values())

    def band_counts(self, tool: str) -> dict[str, int]:
        """Rows per band for ``tool``, covering all five bands.

        Every band appears, including one with zero rows, so a rendered table
        has no ragged columns.
        """
        key_tool = self._validate_tool(tool)
        totals = {band: 0 for band in SEVERITY_NORM}
        for entry in self.entries(key_tool):
            totals[entry.severity_norm] += entry.rows
        return totals

    def __len__(self) -> int:
        """The number of tracked tools, seeded or recorded."""
        return len(self._counts)

    def __repr__(self) -> str:
        """A representation naming the tracked tools and the total rows."""
        return (
            f"{type(self).__name__}(tools={len(self._counts)}, "
            f"rows={self.total_rows()})"
        )



# --------------------------------------------------------------------------- #
# Self-check: the policy asserted against itself                              #
#                                                                             #
# Run as ``python3 harness/lib/normalize/severity.py``.  Exits 0 when every    #
# check passes and 1 otherwise, naming each failure.  The checks are part of   #
# the module rather than an external script because this policy is what every  #
# adapter's severity field depends on, and because the display form of table 3 #
# and the comparisons that implement it must be shown to agree rather than     #
# assumed to.                                                                 #
# --------------------------------------------------------------------------- #


def _self_check() -> tuple[int, list[str]]:
    """Assert the policy against itself; return (checks run, failures)."""
    failures: list[str] = []
    observed: list[SeverityResult] = []
    checks = 0

    def check(condition: bool, message: str) -> None:
        nonlocal checks
        checks += 1
        if not condition:
            failures.append(message)

    def resolved(**kwargs: Any) -> SeverityResult:
        result = resolve(**kwargs)
        observed.append(result)
        return result

    def raises(call: Any, *args: Any, **kwargs: Any) -> bool:
        try:
            call(*args, **kwargs)
        except SeverityPolicyError:
            return True
        return False

    # -- Table 1: SARIF levels, and an unknown level ------------------------ #
    for level, expected in (
        ("error", "High"),
        ("warning", "Medium"),
        ("note", "Low"),
        ("none", INFO),
        ("ERROR", "High"),
        ("Warning", "Medium"),
    ):
        result = resolved(sarif_level=level)
        check(
            result.severity_norm == expected
            and result.basis == BASIS_SARIF_LEVEL
            and result.severity_native == level
            and result.selected_entry == {"label": level},
            f"sarif level {level!r} expected {expected}, got {result!r}",
        )
    unknown_level = resolved(sarif_level="fatal")
    check(
        unknown_level.severity_norm == INFO
        and unknown_level.basis == BASIS_UNMAPPED_LITERAL
        and unknown_level.unmapped_literal == "fatal",
        f"unknown sarif level must be disclosed, got {unknown_level!r}",
    )

    # -- Table 2: labels, case-insensitively, whitespace stripped ----------- #
    label_cases = {
        "CRITICAL": "Critical",
        "HIGH": "High",
        "MODERATE": "Medium",
        "MEDIUM": "Medium",
        "LOW": "Low",
        "NEGLIGIBLE": INFO,
        "INFO": INFO,
        "INFORMATIONAL": INFO,
        "UNKNOWN": INFO,
        "NONE": INFO,
    }
    for literal, expected in label_cases.items():
        for spelling in (literal, literal.lower(), literal.capitalize()):
            result = resolved(label=spelling)
            check(
                result.severity_norm == expected
                and result.basis == BASIS_LABEL
                and result.severity_native == spelling
                and result.selected_entry == {"label": spelling},
                f"label {spelling!r} expected {expected}, got {result!r}",
            )
    padded = resolved(label=" Moderate ")
    check(
        padded.severity_norm == "Medium" and padded.severity_native == "Moderate",
        f"' Moderate ' must strip to Moderate/Medium, got {padded!r}",
    )

    # -- Table 3: the four boundaries, on the correct side ------------------ #
    for score, expected in (
        (10.0, "Critical"),
        (9.0, "Critical"),
        (8.9, "High"),
        (7.0, "High"),
        (6.9, "Medium"),
        (4.0, "Medium"),
        (3.9, "Low"),
        (0.1, "Low"),
        (0.0, INFO),
    ):
        result = resolved(scores=[score])
        check(
            result.severity_norm == expected and result.basis == BASIS_CVSS_SCORE,
            f"score {score} expected {expected}, got {result!r}",
        )
        check(
            band_for_score(score) == expected,
            f"band_for_score({score}) expected {expected}, "
            f"got {band_for_score(score)}",
        )

    # The display table and the comparisons that implement it must agree at
    # every boundary: one is what severity-map.md renders, the other is what
    # bands a row.
    for band, low, high in CVSS_BAND_TABLE:
        for edge in (low, high):
            check(
                band_for_score(edge) == band,
                f"CVSS_BAND_TABLE says {edge} is {band}, "
                f"band_for_score says {band_for_score(edge)}",
            )

    # -- Float representation artifacts: numeric banding, clean rendering --- #
    for raw, expected_band, expected_text in (
        (3.200000047683716, "Low", "3.2"),
        (3.299999952316284, "Low", "3.3"),
        (5.300000190734863, "Medium", "5.3"),
        (7.5, "High", "7.5"),
        (9.1, "Critical", "9.1"),
        (10.0, "Critical", "10.0"),
        (0.0, INFO, "0.0"),
    ):
        result = resolved(scores=[raw])
        check(
            result.severity_norm == expected_band
            and result.severity_native == expected_text
            and result.selected_entry is not None
            and result.selected_entry["score"] == raw,
            f"score {raw!r} expected {expected_band}/{expected_text!r} with the "
            f"full-precision value retained, got {result!r}",
        )
    # The same literal arriving in the severity field rather than as a score
    # bands identically, so the outcome does not depend on how an adapter routes
    # it.
    as_label = resolved(label="3.200000047683716")
    check(
        as_label.severity_norm == "Low"
        and as_label.severity_native == "3.2"
        and as_label.basis == BASIS_CVSS_SCORE
        and as_label.selected_entry is not None
        and as_label.selected_entry["source"] == "label",
        f"a numeric severity literal must band numerically, got {as_label!r}",
    )

    # -- CVSS vectors are disclosed literals, never scores ------------------ #
    for vector in (
        "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
        "CVSS:4.0/AV:L/AC:H/AT:N/PR:L/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N",
    ):
        result = resolved(label=vector)
        check(
            result.severity_norm == INFO
            and result.basis == BASIS_UNMAPPED_LITERAL
            and result.severity_native == vector
            and result.unmapped_literal == vector,
            f"vector {vector!r} must be a disclosed unmapped literal, "
            f"got {result!r}",
        )
        check(
            is_cvss_vector(vector) and _bandable_score(vector) is None,
            f"vector {vector!r} must never coerce to a score",
        )
    vector_as_score = resolved(scores=["CVSS:3.1/AV:N/AC:L/PR:N/UI:N"])
    check(
        vector_as_score.basis == BASIS_UNMAPPED_LITERAL,
        f"a vector supplied as a score must still be disclosed, "
        f"got {vector_as_score!r}",
    )

    # -- Precedence: a mapped label governs over any score ------------------ #
    precedence = resolved(label="LOW", scores=[{"score": 9.8, "source": "NVD"}])
    check(
        precedence.severity_norm == "Low"
        and precedence.basis == BASIS_LABEL
        and precedence.selected_entry == {"label": "LOW"},
        f"a mapped label must govern over a contradicting score, "
        f"got {precedence!r}",
    )

    # -- Deterministic selection among several score candidates ------------- #
    many = [
        {"score": 5.3, "source": "NVD", "version": "3.1"},
        {"score": 9.1, "source": "GHSA", "version": "3.1"},
        {"score": 7.5, "source": "OSV", "version": "4.0"},
    ]
    first_pass = resolved(scores=many)
    second_pass = resolved(scores=list(many))
    check(
        first_pass.selected_entry == second_pass.selected_entry
        and first_pass.severity_norm == second_pass.severity_norm,
        "score selection must be deterministic across calls, got "
        f"{first_pass!r} then {second_pass!r}",
    )
    check(
        first_pass.selected_entry is not None
        and first_pass.selected_entry["version"] == "4.0"
        and first_pass.selected_entry["score"] == 7.5
        and first_pass.severity_norm == "High",
        f"highest CVSS version must win the selection, got {first_pass!r}",
    )
    version_tie = resolved(
        scores=[
            {"score": 7.5, "source": "NVD", "version": "3.1"},
            {"score": 7.5, "source": "GHSA", "version": "3.1"},
        ]
    )
    check(
        version_tie.selected_entry is not None
        and version_tie.selected_entry["source"] == "GHSA",
        f"a version and score tie must break on the smallest source, "
        f"got {version_tie!r}",
    )
    score_tie = resolved(
        scores=[
            {"score": 4.0, "source": "NVD", "version": "3.1"},
            {"score": 8.8, "source": "NVD", "version": "3.1"},
        ]
    )
    check(
        score_tie.selected_entry is not None
        and score_tie.selected_entry["score"] == 8.8,
        f"a version tie must break on the highest score, got {score_tie!r}",
    )
    unversioned = resolved(scores=[{"score": 6.0}, {"score": 2.0, "version": "3.1"}])
    check(
        unversioned.selected_entry is not None
        and unversioned.selected_entry["score"] == 2.0,
        f"a versioned candidate must outrank an unversioned one, "
        f"got {unversioned!r}",
    )
    bare = resolved(scores=7.5)
    check(
        bare.severity_norm == "High"
        and bare.selected_entry == {"score": 7.5, "source": None, "version": None},
        f"a lone bare score need not be wrapped, got {bare!r}",
    )
    alt_keys = resolved(scores=[{"baseScore": 9.4, "type": "Primary"}])
    check(
        alt_keys.severity_norm == "Critical"
        and alt_keys.selected_entry is not None
        and alt_keys.selected_entry["source"] == "Primary",
        f"the documented fallback keys must be honoured, got {alt_keys!r}",
    )

    # -- Out of range, non-numeric and boolean values are disclosed --------- #
    for raw, expected_literal in (
        (-1.0, "-1.0"),
        (11.0, "11.0"),
        ("n/a", "n/a"),
        (True, "True"),
        (float("nan"), "nan"),
    ):
        result = resolved(scores=[raw])
        check(
            result.severity_norm == INFO
            and result.basis == BASIS_UNMAPPED_LITERAL
            and result.unmapped_literal == expected_literal,
            f"score {raw!r} must be disclosed as {expected_literal!r}, "
            f"got {result!r}",
        )
    check(
        raises(band_for_score, 11.0) and raises(band_for_score, -0.1),
        "band_for_score must refuse an out-of-range score rather than clamp it",
    )

    # -- Absence: no vocabulary at all -------------------------------------- #
    for kwargs in ({}, {"label": None}, {"label": "   "}, {"scores": []}):
        result = resolved(**kwargs)
        check(
            result.severity_native is None
            and result.severity_norm == INFO
            and result.basis == BASIS_NO_VOCABULARY
            and result.selected_entry is None
            and result.unmapped_literal is None,
            f"resolve({kwargs!r}) must state the absence, got {result!r}",
        )
    absent = SeverityResult.absent()
    check(
        absent.severity_native is None
        and absent.severity_norm == INFO
        and absent.basis == BASIS_NO_VOCABULARY,
        f"SeverityResult.absent() is malformed: {absent!r}",
    )

    # -- The closed vocabulary, over every result produced above ------------ #
    for result in observed:
        check(
            result.severity_norm in SEVERITY_NORM,
            f"severity_norm {result.severity_norm!r} is outside SEVERITY_NORM",
        )
        check(
            result.basis in BASIS_VALUES,
            f"basis {result.basis!r} is outside BASIS_VALUES",
        )

    # -- The invariant is unbypassable -------------------------------------- #
    check(
        raises(
            SeverityResult,
            severity_native="x",
            severity_norm="Sev1",
            basis=BASIS_LABEL,
        ),
        "a band outside SEVERITY_NORM must be refused",
    )
    check(
        raises(
            SeverityResult,
            severity_native="x",
            severity_norm=INFO,
            basis="guessed",
        ),
        "a basis outside BASIS_VALUES must be refused",
    )
    check(
        raises(
            SeverityResult,
            severity_native="x",
            severity_norm=INFO,
            basis=BASIS_UNMAPPED_LITERAL,
        ),
        "an unmapped result without its literal must be refused",
    )
    check(
        raises(
            SeverityResult,
            severity_native="x",
            severity_norm=INFO,
            basis=BASIS_LABEL,
            selected_entry={"label": "x"},
            unmapped_literal="x",
        ),
        "an unmapped_literal on a mapped basis must be refused",
    )
    check(
        raises(
            SeverityResult,
            severity_native="x",
            severity_norm="High",
            basis=BASIS_LABEL,
        ),
        "a mapped basis without a selected_entry must be refused",
    )
    check(
        raises(
            SeverityResult,
            severity_native="7.5",
            severity_norm="High",
            basis=BASIS_CVSS_SCORE,
            selected_entry={"label": "7.5"},
        ),
        "a score basis whose selected_entry carries no score must be refused",
    )
    caller_dict = {"label": "HIGH"}
    copied = SeverityResult(
        severity_native="HIGH",
        severity_norm="High",
        basis=BASIS_LABEL,
        selected_entry=caller_dict,
    )
    caller_dict["label"] = "mutated"
    check(
        copied.selected_entry == {"label": "HIGH"},
        f"selected_entry must be copied on construction, got {copied!r}",
    )

    # -- The tally ---------------------------------------------------------- #
    tally = LiteralTally()
    tally.record("dependency-check", resolve(label="moderate"))
    tally.record("dependency-check", resolve(label="moderate"))
    tally.record("osv-scanner", resolve(label="MODERATE"))
    tally.record("gitleaks", SeverityResult.absent())
    tally.record("dependency-check", resolve(label="CVSS:3.1/AV:N/AC:L"))
    dc_entries = tally.entries("dependency-check")
    check(
        len(dc_entries) == 2
        and any(
            entry.severity_native == "moderate"
            and entry.rows == 2
            and entry.severity_norm == "Medium"
            and entry.basis == BASIS_LABEL
            and not entry.unmapped
            for entry in dc_entries
        ),
        f"one literal recorded twice must count 2, got {dc_entries!r}",
    )
    check(
        tally.row_count("dependency-check") == 3 and tally.total_rows() == 5,
        f"row counts are wrong: {tally!r}",
    )
    check(
        len(tally.unmapped_by_tool()["dependency-check"]) == 1
        and tally.unmapped_by_tool()["osv-scanner"] == (),
        f"unmapped disclosure is wrong: {tally.unmapped_by_tool()!r}",
    )
    check(
        tally.entries("osv-scanner")[0].severity_native == "MODERATE"
        and tally.entries("dependency-check")[0].severity_norm == "Medium",
        "the same literal for two tools must give two separate entries",
    )
    check(
        tally.entries("gitleaks")[0].severity_native is None
        and tally.entries("gitleaks")[0].basis == BASIS_NO_VOCABULARY,
        f"an absent literal must be reported as such, "
        f"got {tally.entries('gitleaks')!r}",
    )
    check(
        tally.band_counts("dependency-check") == {
            "Critical": 0,
            "High": 0,
            "Medium": 2,
            "Low": 0,
            INFO: 1,
        },
        f"band counts are wrong: {tally.band_counts('dependency-check')!r}",
    )
    seeded = LiteralTally.with_all_tools()
    check(
        len(seeded.by_tool()) == 9
        and set(seeded.by_tool()) == set(CANONICAL_TOOLS)
        and all(entries == () for entries in seeded.by_tool().values())
        and seeded.total_rows() == 0,
        f"seeding all nine must yield nine zero-row entries, got {seeded!r}",
    )
    check(
        seeded.tools() == CANONICAL_TOOLS,
        f"seeded tools must report in canonical order, got {seeded.tools()!r}",
    )
    check(
        raises(tally.record, "codeql", SeverityResult.absent())
        and raises(tally.seed, "semgrep-pro")
        and raises(tally.entries, "unknown-tool")
        and raises(tally.record, "trivy", "not-a-result"),
        "an unknown tool identifier, or a non-result, must be refused",
    )

    # -- The selected entry is part of a bucket's identity ------------------ #
    #
    # A score reaches severity_native as its one-decimal rendering, so equal
    # renderings from different score entries are exactly the case that used to
    # collapse.  These checks assert the collapse cannot recur.
    check(
        LITERAL_KEY_FIELDS
        == (
            "severity_native",
            "severity_norm",
            "basis",
            "selected_label",
            "selected_score",
            "selected_source",
            "selected_version",
        ),
        f"LITERAL_KEY_FIELDS has drifted: {LITERAL_KEY_FIELDS!r}",
    )
    count_fields = {field.name for field in fields(LiteralCount)}
    check(
        set(LITERAL_KEY_FIELDS) <= count_fields,
        f"LiteralCount must carry every key field, missing "
        f"{sorted(set(LITERAL_KEY_FIELDS) - count_fields)!r}",
    )

    provenance = LiteralTally()
    provenance.record(
        "dependency-check",
        resolve(scores=[{"score": 7.5, "source": "NVD:cvssv3", "version": "3.1"}]),
    )
    provenance.record(
        "dependency-check",
        resolve(scores=[{"score": 7.5, "source": "redhat", "version": "3.1"}]),
    )
    provenance.record(
        "dependency-check",
        resolve(scores=[{"score": 7.5, "source": "redhat", "version": "3.1"}]),
    )
    provenance.record(
        "dependency-check",
        resolve(scores=[{"score": 7.5, "source": "redhat", "version": "4.0"}]),
    )
    score_entries = provenance.entries("dependency-check")
    check(
        len({entry.severity_native for entry in score_entries}) == 1
        and len(score_entries) == 3,
        f"three score entries rendering '7.5' must stay three entries, "
        f"got {score_entries!r}",
    )
    check(
        {
            (entry.selected_source, entry.selected_version, entry.rows)
            for entry in score_entries
        }
        == {("NVD:cvssv3", "3.1", 1), ("redhat", "3.1", 2), ("redhat", "4.0", 1)},
        f"each entry must name its own source and version and count its own "
        f"rows, got {score_entries!r}",
    )
    check(
        all(
            entry.selected_score == 7.5 and entry.selected_label is None
            for entry in score_entries
        )
        and provenance.row_count("dependency-check") == 4,
        f"the score path must record the score and no label, and the row total "
        f"must be unaffected by the split, got {score_entries!r}",
    )
    reversed_order = LiteralTally()
    for one_result in reversed(
        [
            resolve(scores=[{"score": 7.5, "source": "NVD:cvssv3", "version": "3.1"}]),
            resolve(scores=[{"score": 7.5, "source": "redhat", "version": "3.1"}]),
            resolve(scores=[{"score": 7.5, "source": "redhat", "version": "3.1"}]),
            resolve(scores=[{"score": 7.5, "source": "redhat", "version": "4.0"}]),
        ]
    ):
        reversed_order.record("dependency-check", one_result)
    check(
        reversed_order.entries("dependency-check") == score_entries,
        "the report order must be a pure function of the content, not of the "
        "order the rows were recorded in",
    )

    label_entry = LiteralTally()
    label_entry.record("dependency-check", resolve(label="HIGH"))
    label_entry.record("trivy", resolve(sarif_level="error"))
    label_entry.record("gitleaks", SeverityResult.absent())
    label_entry.record("checkov", SeverityResult.unmapped("VERY-BAD"))
    check(
        label_entry.entries("dependency-check")[0].selected_label == "HIGH"
        and label_entry.entries("trivy")[0].selected_label == "error"
        and all(
            getattr(label_entry.entries(tool)[0], name) is None
            for tool in ("dependency-check", "trivy")
            for name in ("selected_score", "selected_source", "selected_version")
        ),
        "a label or level entry must record the label it used and no score",
    )
    check(
        all(
            getattr(label_entry.entries(tool)[0], name) is None
            for tool in ("gitleaks", "checkov")
            for name in LITERAL_KEY_FIELDS[3:]
        ),
        "a band that came from policy rather than from an observed entry must "
        "record no selection at all",
    )
    check(
        raises(
            SeverityResult,
            severity_native="HIGH",
            severity_norm="High",
            basis=BASIS_LABEL,
            selected_entry={"label": "HIGH", "vector": "CVSS:3.1/AV:N"},
        )
        and raises(
            SeverityResult,
            severity_native="7.5",
            severity_norm="High",
            basis=BASIS_CVSS_SCORE,
            selected_entry={"score": 7.5, "vector": "CVSS:3.1/AV:N"},
        )
        and raises(
            SeverityResult,
            severity_native="7.5",
            severity_norm="High",
            basis=BASIS_CVSS_SCORE,
            selected_entry={"score": 7.5, "source": 3},
        ),
        "an unlisted key, or a wrongly typed one, must be refused: the four "
        "provenance fields are a complete decomposition only while the entry's "
        "key set is closed",
    )

    # -- The authored policy statements match the code they describe -------- #
    check(
        render_score(3.200000047683716) in POLICY_SCORE_RENDERING
        and render_score(10.0) in POLICY_SCORE_RENDERING,
        "POLICY_SCORE_RENDERING has drifted from render_score",
    )
    check(
        all(band in POLICY_CVSS_NONE_AS_INFO for band in SEVERITY_NORM if band != INFO)
        and INFO in POLICY_CVSS_NONE_AS_INFO,
        "POLICY_CVSS_NONE_AS_INFO does not name the bands it describes",
    )
    check(
        len(POLICY_STATEMENTS) == 4
        and all(isinstance(text, str) and text for _, text in POLICY_STATEMENTS),
        "POLICY_STATEMENTS must carry four non-empty authored strings",
    )
    check(
        isinstance(POLICY_SELECTED_ENTRY_TALLY, str)
        and POLICY_SELECTED_ENTRY_TALLY
        and POLICY_SELECTED_ENTRY_TALLY
        not in [text for _, text in POLICY_STATEMENTS],
        "POLICY_SELECTED_ENTRY_TALLY must be authored, and must stay outside "
        "POLICY_STATEMENTS: those four state the mapping, this one states the "
        "tally's identity and is carried under its own record key",
    )
    check(
        sarif_level_table() == _SARIF_LEVEL_MAP
        and sarif_level_table() is not _SARIF_LEVEL_MAP
        and label_table() == _LABEL_MAP
        and label_table() is not _LABEL_MAP,
        "the table accessors must return fresh copies of the policy tables",
    )
    check(
        len(SEVERITY_NORM) == 5 and len(set(SEVERITY_NORM)) == 5,
        f"SEVERITY_NORM must hold five distinct bands, got {SEVERITY_NORM!r}",
    )
    check(
        len(CANONICAL_TOOLS) == 9 and len(set(CANONICAL_TOOLS)) == 9,
        f"CANONICAL_TOOLS must hold nine distinct identifiers, "
        f"got {CANONICAL_TOOLS!r}",
    )

    return checks, failures


def main() -> int:
    """Run the self-check, report it, and return a process exit code."""
    checks, failures = _self_check()
    for failure in failures:
        print(f"FAIL: {failure}")
    print(f"severity policy self-check: {checks - len(failures)}/{checks} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
