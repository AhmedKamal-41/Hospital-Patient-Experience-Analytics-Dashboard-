"""HCAHPS measure-id taxonomy — the single source of truth for the pivot.

The CMS HCAHPS dataset is published in long format. Each (facility_id,
hcahps_measure_id) row carries one *answer slice* for one *dimension*.

A measure_id looks like:    H_COMP_1_A_P
                            <root-----><suffix>

`parse_measure_id("H_COMP_1_A_P")` → ("H_COMP_1", "top_box_pct", "Always")

The semantic_column is the column in core.patient_experience that this
slice populates after the pivot. The answer_label is human-readable.

Verified against the 2026-01-26 Care Compare release (see
scripts/profile_hcahps.sql for the data confirming this taxonomy).
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Suffix → (semantic_column, answer_label)
# ---------------------------------------------------------------------------
ANSWER_SUFFIXES: dict[str, tuple[str, str]] = {
    "A_P":          ("top_box_pct",    "Always"),
    "U_P":          ("middle_box_pct", "Usually"),
    "SN_P":         ("bottom_box_pct", "Sometimes/Never"),
    "Y_P":          ("top_box_pct",    "Yes"),
    "N_P":          ("bottom_box_pct", "No"),
    "DY":           ("top_box_pct",    "Definitely Yes"),
    "PY":           ("middle_box_pct", "Probably Yes"),
    "DN":           ("bottom_box_pct", "Definitely No"),
    "9_10":         ("top_box_pct",    "Rating 9-10"),
    "7_8":          ("middle_box_pct", "Rating 7-8"),
    "0_6":          ("bottom_box_pct", "Rating 0-6"),
    "LINEAR_SCORE": ("linear_score",   "Linear mean"),
    "STAR_RATING":  ("star_rating",    "Star rating"),
}

# Order matters: longer suffixes must be checked first so that "9_10" is
# tried before "_10" would be (none of the others share a tail, but be
# defensive).
_SUFFIX_ORDER: tuple[str, ...] = tuple(
    sorted(ANSWER_SUFFIXES.keys(), key=len, reverse=True)
)

# ---------------------------------------------------------------------------
# Composite dimensions used in marts.patient_experience_index
# ---------------------------------------------------------------------------
# Confirmed via profile_hcahps.sql query 1.2: every dimension below has a
# `_LINEAR_SCORE` answer-type row in raw.hcahps. The Phase 4 spec sketched
# 9 composites; CMS publishes 8 in the 2026-01-26 release. H_CLEAN and
# H_QUIET (NOT H_CLEAN_HSP / H_QUIET_HSP) are the linear-score-bearing
# roots; their _HSP siblings only carry the pct_ternary breakdowns.
COMPOSITE_DIMENSIONS: list[tuple[str, str]] = [
    ("H_COMP_1",     "Communication with Nurses"),
    ("H_COMP_2",     "Communication with Doctors"),
    ("H_COMP_5",     "Communication About Medicines"),
    ("H_COMP_6",     "Discharge Information"),
    ("H_CLEAN",      "Cleanliness of Hospital Environment"),
    ("H_QUIET",      "Quietness of Hospital Environment"),
    ("H_HSP_RATING", "Overall Hospital Rating"),
    ("H_RECMND",     "Would Recommend the Hospital"),
]

# Convenience lookups
COMPOSITE_LABELS: dict[str, str] = dict(COMPOSITE_DIMENSIONS)
COMPOSITE_ROOTS: frozenset[str] = frozenset(COMPOSITE_LABELS)


def parse_measure_id(raw_id: str) -> tuple[str, str, str]:
    """Split a HCAHPS measure_id into (dimension_root, semantic_col, label).

    >>> parse_measure_id("H_COMP_1_A_P")
    ('H_COMP_1', 'top_box_pct', 'Always')
    >>> parse_measure_id("H_HSP_RATING_LINEAR_SCORE")
    ('H_HSP_RATING', 'linear_score', 'Linear mean')
    >>> parse_measure_id("H_STAR_RATING")
    ('H', 'star_rating', 'Star rating')
    """
    if not raw_id:
        raise ValueError("empty measure_id")
    for suffix in _SUFFIX_ORDER:
        tail = "_" + suffix
        if raw_id.endswith(tail) and len(raw_id) > len(tail):
            root = raw_id[: -len(tail)]
            semantic_col, label = ANSWER_SUFFIXES[suffix]
            return root, semantic_col, label
    raise ValueError(f"measure_id {raw_id!r} matches no known suffix")
