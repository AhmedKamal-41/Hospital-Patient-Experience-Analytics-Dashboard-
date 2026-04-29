"""Explicit CSV-header → DB-column maps for each ingested dataset.

The CMS CSV headers are TitleCase With Spaces (e.g. "Facility ID", "City/Town")
and are NOT the same as the JSON datastore field names. Auto-snake-casing them
would silently break the day CMS renames a column. An explicit map fails loud:
a missing CSV header is a hard error, an unexpected extra header is a logged
warning.

Headers verified against the 2026-01-26 Care Compare release.
"""

from __future__ import annotations

from typing import TypedDict


class DatasetConfig(TypedDict):
    dataset_id: str
    table: str
    pk_columns: tuple[str, ...]
    column_map: dict[str, str]


HOSPITAL_GENERAL_INFO_MAP: dict[str, str] = {
    "Facility ID": "facility_id",
    "Facility Name": "facility_name",
    "Address": "address",
    "City/Town": "citytown",
    "State": "state",
    "ZIP Code": "zip_code",
    "County/Parish": "countyparish",
    "Telephone Number": "telephone_number",
    "Hospital Type": "hospital_type",
    "Hospital Ownership": "hospital_ownership",
    "Emergency Services": "emergency_services",
    "Meets criteria for birthing friendly designation":
        "meets_criteria_for_birthing_friendly_designation",
    "Hospital overall rating": "hospital_overall_rating",
    "Hospital overall rating footnote": "hospital_overall_rating_footnote",
    "MORT Group Measure Count": "mort_group_measure_count",
    "Count of Facility MORT Measures": "count_of_facility_mort_measures",
    "Count of MORT Measures Better": "count_of_mort_measures_better",
    "Count of MORT Measures No Different": "count_of_mort_measures_no_different",
    "Count of MORT Measures Worse": "count_of_mort_measures_worse",
    "MORT Group Footnote": "mort_group_footnote",
    "Safety Group Measure Count": "safety_group_measure_count",
    "Count of Facility Safety Measures": "count_of_facility_safety_measures",
    "Count of Safety Measures Better": "count_of_safety_measures_better",
    "Count of Safety Measures No Different": "count_of_safety_measures_no_different",
    "Count of Safety Measures Worse": "count_of_safety_measures_worse",
    "Safety Group Footnote": "safety_group_footnote",
    "READM Group Measure Count": "readm_group_measure_count",
    "Count of Facility READM Measures": "count_of_facility_readm_measures",
    "Count of READM Measures Better": "count_of_readm_measures_better",
    "Count of READM Measures No Different": "count_of_readm_measures_no_different",
    "Count of READM Measures Worse": "count_of_readm_measures_worse",
    "READM Group Footnote": "readm_group_footnote",
    "Pt Exp Group Measure Count": "pt_exp_group_measure_count",
    "Count of Facility Pt Exp Measures": "count_of_facility_pt_exp_measures",
    "Pt Exp Group Footnote": "pt_exp_group_footnote",
    "TE Group Measure Count": "te_group_measure_count",
    "Count of Facility TE Measures": "count_of_facility_te_measures",
    "TE Group Footnote": "te_group_footnote",
}

HCAHPS_MAP: dict[str, str] = {
    "Facility ID": "facility_id",
    "Facility Name": "facility_name",
    "Address": "address",
    "City/Town": "citytown",
    "State": "state",
    "ZIP Code": "zip_code",
    "County/Parish": "countyparish",
    "Telephone Number": "telephone_number",
    "HCAHPS Measure ID": "hcahps_measure_id",
    "HCAHPS Question": "hcahps_question",
    "HCAHPS Answer Description": "hcahps_answer_description",
    "Patient Survey Star Rating": "patient_survey_star_rating",
    "Patient Survey Star Rating Footnote": "patient_survey_star_rating_footnote",
    "HCAHPS Answer Percent": "hcahps_answer_percent",
    "HCAHPS Answer Percent Footnote": "hcahps_answer_percent_footnote",
    "HCAHPS Linear Mean Value": "hcahps_linear_mean_value",
    "Number of Completed Surveys": "number_of_completed_surveys",
    "Number of Completed Surveys Footnote": "number_of_completed_surveys_footnote",
    "Survey Response Rate Percent": "survey_response_rate_percent",
    "Survey Response Rate Percent Footnote": "survey_response_rate_percent_footnote",
    "Start Date": "start_date",
    "End Date": "end_date",
}

UNPLANNED_VISITS_MAP: dict[str, str] = {
    "Facility ID": "facility_id",
    "Facility Name": "facility_name",
    "Address": "address",
    "City/Town": "citytown",
    "State": "state",
    "ZIP Code": "zip_code",
    "County/Parish": "countyparish",
    "Telephone Number": "telephone_number",
    "Measure ID": "measure_id",
    "Measure Name": "measure_name",
    "Compared to National": "compared_to_national",
    "Denominator": "denominator",
    "Score": "score",
    "Lower Estimate": "lower_estimate",
    "Higher Estimate": "higher_estimate",
    "Number of Patients": "number_of_patients",
    "Number of Patients Returned": "number_of_patients_returned",
    "Footnote": "footnote",
    "Start Date": "start_date",
    "End Date": "end_date",
}


DATASETS: dict[str, DatasetConfig] = {
    "hospital_general_info": {
        "dataset_id": "xubh-q36u",
        "table": "raw.hospital_general_info",
        "pk_columns": ("facility_id",),
        "column_map": HOSPITAL_GENERAL_INFO_MAP,
    },
    "hcahps": {
        "dataset_id": "dgck-syfz",
        "table": "raw.hcahps",
        "pk_columns": ("facility_id", "hcahps_measure_id"),
        "column_map": HCAHPS_MAP,
    },
    "unplanned_visits": {
        "dataset_id": "632h-zaca",
        "table": "raw.unplanned_visits",
        "pk_columns": ("facility_id", "measure_id"),
        "column_map": UNPLANNED_VISITS_MAP,
    },
}
