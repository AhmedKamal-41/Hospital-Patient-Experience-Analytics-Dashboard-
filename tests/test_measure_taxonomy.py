"""Unit tests for src.measure_taxonomy. No DB, no network."""

from __future__ import annotations

import pytest

from src.measure_taxonomy import (
    ANSWER_SUFFIXES,
    COMPOSITE_DIMENSIONS,
    COMPOSITE_ROOTS,
    parse_measure_id,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("H_COMP_1_A_P",              ("H_COMP_1",     "top_box_pct",    "Always")),
        ("H_COMP_1_U_P",              ("H_COMP_1",     "middle_box_pct", "Usually")),
        ("H_COMP_1_SN_P",             ("H_COMP_1",     "bottom_box_pct", "Sometimes/Never")),
        ("H_COMP_6_Y_P",              ("H_COMP_6",     "top_box_pct",    "Yes")),
        ("H_COMP_6_N_P",              ("H_COMP_6",     "bottom_box_pct", "No")),
        ("H_RECMND_DY",               ("H_RECMND",     "top_box_pct",    "Definitely Yes")),
        ("H_RECMND_PY",               ("H_RECMND",     "middle_box_pct", "Probably Yes")),
        ("H_RECMND_DN",               ("H_RECMND",     "bottom_box_pct", "Definitely No")),
        ("H_HSP_RATING_9_10",         ("H_HSP_RATING", "top_box_pct",    "Rating 9-10")),
        ("H_HSP_RATING_7_8",          ("H_HSP_RATING", "middle_box_pct", "Rating 7-8")),
        ("H_HSP_RATING_0_6",          ("H_HSP_RATING", "bottom_box_pct", "Rating 0-6")),
        ("H_HSP_RATING_LINEAR_SCORE", ("H_HSP_RATING", "linear_score",   "Linear mean")),
        ("H_HSP_RATING_STAR_RATING",  ("H_HSP_RATING", "star_rating",    "Star rating")),
        ("H_STAR_RATING",             ("H",            "star_rating",    "Star rating")),
        ("H_CLEAN_LINEAR_SCORE",      ("H_CLEAN",      "linear_score",   "Linear mean")),
    ],
)
def test_parse_measure_id_known_suffixes(
    raw: str, expected: tuple[str, str, str]
) -> None:
    assert parse_measure_id(raw) == expected


def test_every_documented_suffix_is_reachable() -> None:
    """Every suffix in ANSWER_SUFFIXES must be parseable when appended."""
    for suffix, (col, label) in ANSWER_SUFFIXES.items():
        root, parsed_col, parsed_label = parse_measure_id(f"H_TEST_{suffix}")
        assert root == "H_TEST"
        assert parsed_col == col
        assert parsed_label == label


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "H_COMP_1_GARBAGE",
        "H_COMP_1_LINEAR",          # close but not LINEAR_SCORE
        "H_COMP_1_A",               # close but not A_P
        "BARE_NO_SUFFIX",
    ],
)
def test_unknown_suffix_raises(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_measure_id(bad)


def test_composite_dimensions_match_phase1_findings() -> None:
    """The 8 linear-score-bearing composites confirmed by profile_hcahps.sql."""
    assert COMPOSITE_ROOTS == {
        "H_COMP_1", "H_COMP_2", "H_COMP_5", "H_COMP_6",
        "H_CLEAN", "H_QUIET", "H_HSP_RATING", "H_RECMND",
    }
    assert len(COMPOSITE_DIMENSIONS) == 8


def test_h_clean_hsp_is_not_a_composite() -> None:
    """Guard against regressing the spec mismatch caught in profiling."""
    assert "H_CLEAN_HSP" not in COMPOSITE_ROOTS
    assert "H_QUIET_HSP" not in COMPOSITE_ROOTS
