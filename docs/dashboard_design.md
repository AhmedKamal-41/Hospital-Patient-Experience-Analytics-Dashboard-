# Hospital Patient Experience Dashboard — design spec

This is the click-through I follow while building the Looker Studio
dashboard. It assumes the 5 sheets created by `scripts/export_to_sheets.py`
exist as data sources named:

- `hospitals` — 5,426 rows; one per hospital, with `composite_score` joined in
- `patient_experience_dim_long` — 38,312 rows; 8 composites × ~4,789 hospitals
- `state_rankings` — 53 rows
- `top_bottom` — 20 rows
- `meta` — 1 row, used for headline numbers and last-refresh stamp

Dashboard size: 1440×900 (desktop default), 12-column grid.

---

## Style guide (apply to all pages)

**Colors.** Single-hue blue scale for all data viz. `#0B3D91` (dark navy)
as the primary, `#4A7BC8` mid, `#A6C2E8` light, `#F0F5FB` bg accent. Use
`#5A6470` (slate) for axis labels and `#1F2937` for body text. **Never**
red/yellow/green — sends a stoplight signal that doesn't fit a metric where
"low" doesn't mean "the hospital is bad."

**Typography.** Looker Studio default font is Roboto. Set:
- Page title: 22pt bold, `#1F2937`
- Section headers: 18pt bold
- Subheaders: 14pt regular
- Body / chart labels: 11pt regular
- Footnotes: 10pt regular, `#5A6470`

**Numbers.** All composite scores: 1 decimal, `/100` suffix. Percentages:
0 decimals, `%` suffix. Counts: comma thousands separator, no decimals.

**Empty states.** Every chart's properties → STYLE → "Missing data" → set
to the literal text **"No data — see Methodology"**. Never blank, never
zero (zero is a real value here).

**Page header.** Each page top-left: dashboard title in 22pt + a 1-line
subtitle. Top-right: text component reading
`Last refreshed: <last_refresh_utc from meta>`.

---

## Calculated fields (define once per data source)

Looker Studio calculated fields live on the data source, not the chart.
For each: open the data source, **Add a field**, paste the formula.

### On `meta` data source

| Field | Formula | Used by |
|---|---|---|
| `pct_scored_label` | `CONCAT(FORMAT_NUMBER("#,##0", total_scored), " of ", FORMAT_NUMBER("#,##0", total_hospitals), " (", ROUND(total_scored / total_hospitals * 100, 0), "%)")` | Page 1 KPI #1 |
| `insufficient_pct` | `ROUND(total_suppressed / total_hospitals * 100, 0)` | Page 1 KPI #4 |

These can't live in the source sheet because the literal `"3,183 of 4,789 (66%)"` mixes two
columns into one display string. Keeping the underlying numbers in the
sheet (`total_scored`, `total_hospitals`) lets you change the format later
without re-exporting.

### On `hospitals` data source

| Field | Formula | Used by |
|---|---|---|
| `composite_score_or_badge` | `CASE WHEN composite_score IS NULL THEN "Insufficient data" ELSE CONCAT(CAST(composite_score AS TEXT), " /100") END` | Page 3 header |
| `composite_score_bin` | `ROUND(composite_score, 0)` | Page 1 histogram (Looker bins on the rounded integer for a 30-bin distribution) |
| `is_scored` | `CASE WHEN composite_score IS NULL THEN "Insufficient data" ELSE "Scored" END` | Page 1 histogram, Page 3 filter |

These can't live in the export because they format display strings or
contain CASE branches that depend on filter context.

### On `patient_experience_dim_long` data source

| Field | Formula | Used by |
|---|---|---|
| `linear_or_blank` | `IFNULL(linear_score, 0)` | Page 3 bar chart (Looker won't draw a bar at NULL) |

### Page 3 only — peer comparison
The Page 3 deep-dive bar chart needs a "state median for this dimension
and hospital_type." This requires **blending** two data sources in Looker:
the per-hospital `patient_experience_dim_long` and a derived
state-and-type median calculated via a second blend in Looker (no
sheet-side change needed). See Page 3 below for the click-through.

---

## Page 1 — National Overview

> Goal: in 5 seconds, the viewer knows N hospitals scored, the median, and
> that 1/3 of hospitals lack enough data.

**Page-level filters:** none.

### Layout (12-col grid)

```
┌─────────┬─────────┬─────────┬─────────┐
│ KPI #1  │ KPI #2  │ KPI #3  │ KPI #4  │   row 1   (each: 3 cols × 2 rows)
├─────────┴─────────┴─────────┴─────────┤
│                                       │
│         Histogram (full width)        │   rows 3–7 (12 × 5)
│                                       │
├───────────────────────────────────────┤
│      Insight callout text block       │   rows 8–10 (12 × 3)
└───────────────────────────────────────┘
```

### KPI #1 — Hospitals analyzed
- Type: **Scorecard**
- Data source: `meta`
- Metric: `pct_scored_label` (set its aggregation to `MAX` since it's a string)
- Title: "Hospitals analyzed"
- Style: 36pt bold number, 11pt label
- Why scorecard: single headline number, no comparison needed

### KPI #2 — National median composite
- Type: **Scorecard**
- Data source: `state_rankings`
- Metric: `MEDIAN(median)` — taking median of state medians weights states equally; alternatively use `hospitals` data source with `MEDIAN(composite_score)` for a hospital-weighted figure (pick one and document the choice in the methodology page)
- Number format: 1 decimal, suffix `/100`
- Title: "National median (state-weighted)"

### KPI #3 — Top score
- Type: **Scorecard**
- Data source: `hospitals`
- Metric: `MAX(composite_score)`
- Number format: 1 decimal, suffix `/100`
- Title: "Top score"

### KPI #4 — Insufficient-data rate
- Type: **Scorecard**
- Data source: `meta`
- Metric: `insufficient_pct`
- Number format: 0 decimals, suffix `%`
- Color: `#5A6470` (neutral slate). **Don't use a warning color.**
- Title: "Insufficient data"
- Subtitle (via the description field): "fewer than 6 of 8 dimensions reporting"

### Histogram — composite_score distribution
- Type: **Bar chart** (column orientation)
- Data source: `hospitals`
- Dimension: `composite_score_bin` (the calculated field rounding to integer)
- Sort: dimension ascending
- Metric: `RECORD COUNT`
- Filter: `is_scored = "Scored"` (drop the 1,606 unsuppressed)
- Bars: solid `#0B3D91`, 1px gap
- X-axis: range 60–100, label "Composite score"
- Y-axis: label "Hospitals"
- Reference line: type **constant**, value `84.5` (the national median), color `#1F2937` 1px dashed, label "National median"
- Why column-bar binned: Looker doesn't have a native histogram widget; pre-binning on a calculated field gives full visual control without client-side aggregation

### Insight callout
- Type: **Text** component (not a chart)
- Background: `#F0F5FB`, rounded corners (3px), padding 16px
- Title (18pt bold): "Why are top performers not famous academic centers?"
- Body (11pt regular):

> Top-scoring hospitals on this composite are small specialty surgical
> and community facilities — Unity Medical Center (TN), Citizens Medical
> Center (LA), Advanced Surgical Hospital (PA). This is **not a bug**.
> HCAHPS asks patients about their experience: cleanliness, communication,
> quietness. Smaller hospitals with shorter stays and elective procedures
> tend to score higher. Big academic centers handle harder cases and
> longer admissions and routinely score lower on patient-experience
> alone — but better on outcomes (mortality, readmission). See the
> Methodology page for what HCAHPS doesn't measure.

---

## Page 2 — State Comparison

> Goal: which states have higher / lower median patient experience, and
> how spread is the distribution within each state.

**Page-level filter:** "Minimum hospital count" slider. Filter control on
`state_rankings.hospital_count`, slider type, default value **10**, min 1,
max 300. Title: "Min hospitals per state".

### Layout

```
┌───────────────────────────────────────┐
│        Choropleth (full width)        │   rows 1–6 (12 × 6)
├──────────────────┬────────────────────┤
│ Ranking table    │   Bar chart        │   rows 7–12 (each 6 × 6)
└──────────────────┴────────────────────┘
```

### Choropleth
- Type: **Geo chart** → subtype **Filled map**
- Data source: `state_rankings`
- Geo dimension: `state` (set type to **Region** → **United States** → **State (USPS)**)
- Color metric: `median`
- Color palette: **Sequential single-hue** → custom gradient `#A6C2E8` (low) → `#0B3D91` (high). **Don't pick a diverging palette.**
- Tooltip: `state`, `hospital_count`, `median`, `p25`, `p75`
- Why filled-map: the visceral "where" question is geographic; a sortable bar chart can't tell you that the Mountain West clusters high in 2 seconds the way a map does

### Ranking table
- Type: **Table**
- Data source: `state_rankings`
- Dimensions: `state`, `hospital_count`
- Metrics: `p25`, `median`, `p75`, `p90`
- Sort: `median` DESC
- Conditional formatting: on `median` — same `#A6C2E8 → #0B3D91` gradient, applied via **Style → Heatmap** on the column
- Pagination: 25 rows
- Why table: the dashboard needs *exact* numbers per state; the choropleth is for shape, the table is for value

### Bar chart — median by state
- Type: **Bar chart** (horizontal)
- Data source: `state_rankings`
- Dimension: `state`
- Metric: `median`
- Sort: metric DESC
- Bar color: `#0B3D91`
- X-axis (metric axis): range 75–95 to amplify visible spread
- Show data labels: on, 1 decimal
- Why horizontal bar: 50+ states with text labels read better stacked vertically than rotated 90°

---

## Page 3 — Hospital Deep-Dive

> Goal: pick any hospital, see how it compares against same-state same-
> type peers across the 8 composite dimensions.

**Page-level filters:**

1. **State** — dropdown filter on `hospitals.state`, default **All**
2. **Hospital type** — dropdown filter on `hospitals.hospital_type`, default **All**
3. **Hospital** — dropdown filter on `hospitals.name` with **Search box** enabled (this is the type-ahead). Default value: a specific facility from the bottom-5 list — e.g. **Coast Plaza Hospital** — so the page loads with a meaningful demo. Set this in the filter control's **Default values** field.

The state and hospital-type filters apply **only to the hospital picker**.
They must NOT apply to the per-hospital metrics. Achieve this by setting
the State and Type filters to **only affect the hospital picker** chart
(in Looker: select the chart, **Resource → Manage filters**, set the
State/Type filter scope explicitly).

### Layout

```
┌───────────────────────────────────────┐
│ Hospital header band (full width)     │   row 1 (12 × 1)
├───────────────────────────────────────┤
│ State / Type / Hospital pickers       │   row 2 (12 × 1)
├──────────────────┬────────────────────┤
│ 8-dim bar chart  │   Peer table       │   rows 3–8 (each 6 × 6)
├──────────────────┴────────────────────┤
│   Readmission rate scorecard          │   row 9 (12 × 2)
└───────────────────────────────────────┘
```

### Header band
- Type: **Table** (single row, dressed as a header)
- Data source: `hospitals`
- Dimensions: `name`, `state`, `hospital_type`, `ownership`
- Metric: `composite_score_or_badge` (the CASE-NULL calc)
- Show header row: off
- Style: large font (18pt) on the metric, 14pt on dimensions, all left-aligned
- Why table: scorecard cards can only show one metric; we need name + state + type + score in one band

### 8-dimension bar chart
- Type: **Bar chart** (horizontal grouped)
- Data source: `patient_experience_dim_long`
- Filter: `kind = "composite"` AND `facility_id = <selected>` (the page filter on hospital handles the second clause automatically — Looker propagates it because both this chart and the picker share `hospitals.facility_id` via the **Blend** I'll define below)
- Dimension: `dimension_label` (sort by `dimension_root`'s sort_order — set sort to "Sort by" → custom field if needed)
- Metric: `linear_or_blank` (the calc that NULL → 0; otherwise NULL bars don't render)
- Reference line: state-and-type median for the same dimension. Add this via **Add → Blend data**:
    - Left: `patient_experience_dim_long`
    - Right: a second copy joined on `dimension_root` and `state` to compute median per (dim, state, hospital_type)
    - This is where the second blend earns its keep. Easier alternative for v1: skip the reference line, surface peer scores via the Peer table instead.
- Bar color: `#0B3D91`
- X-axis: 0–100
- Show data labels: on, 1 decimal
- Why horizontal grouped bar: 8 dimensions, 1 facility — vertical labels would cramp; grouping vs. stacking matters because the metrics are independent (cleanliness ≠ part of recommend), not parts of a whole

### Peer table — top 3 / bottom 3 in the same state + type
- Type: **Table**
- Data source: `hospitals`
- Filters:
    - `state = <selected hospital's state>` — driven by another blend with the picker
    - `hospital_type = <selected hospital's type>`
    - `composite_score IS NOT NULL`
- Dimensions: `name`, `state`
- Metric: `composite_score`
- Sort: composite_score DESC
- Row limit: 6 (top 3 + bottom 3 — split into two stacked tables in Looker since one table can't natively show both ends)
- Conditional formatting: row containing the selected hospital → background `#F0F5FB`

### Readmission rate scorecard
- Type: **Scorecard with comparison**
- Data source: `unplanned_visits` — _not currently exported._ For v1, either add a 6th sheet `unplanned_visits` (one row per facility × measure) or skip this widget. **Decision: skip for v1**, link to the methodology page footnote that says "Readmission deep-dive in v2." Keeping scope tight.

---

## Page 4 — Methodology & Limitations

> Goal: when someone asks "is this dashboard credible?", this is the page
> that answers them.

All Text components, no charts. Single column, full-width.

### Sections (in order)

```
1. What this dashboard measures
2. Data source
3. Refresh cadence
4. How the composite is computed
5. Why suppression matters
6. Why top performers are small specialty hospitals
7. What HCAHPS does NOT measure
8. Limitations
9. Repository link
```

Use 18pt bold for section headers, 11pt regular for body. Wrap the whole
page in a 12-col container so reading-line stays narrow.

### Suggested copy

**1. What this dashboard measures.** The Hospital Consumer Assessment of
Healthcare Providers and Systems (HCAHPS) survey, administered by CMS to
random samples of recently-discharged inpatients at every Medicare-certified
hospital in the US. This dashboard summarizes 8 standard HCAHPS composites
into a single 0–100 score per hospital.

**2. Data source.** [CMS Care Compare Provider Data Catalog](https://data.cms.gov/provider-data/).
Datasets: Hospital General Information (`xubh-q36u`), Patient Survey HCAHPS
(`dgck-syfz`), Unplanned Hospital Visits (`632h-zaca`).

**3. Refresh cadence.** CMS publishes Care Compare quarterly. This dashboard
was last refreshed: **{last_refresh_utc from meta}**.

**4. How the composite is computed.** Equal-weight average of the linear
(0–100) score for these 8 dimensions: Communication with Nurses (`H_COMP_1`),
Communication with Doctors (`H_COMP_2`), Communication About Medicines
(`H_COMP_5`), Discharge Information (`H_COMP_6`), Cleanliness
(`H_CLEAN`), Quietness (`H_QUIET`), Overall Hospital Rating
(`H_HSP_RATING`), Would Recommend (`H_RECMND`). If fewer than 6 of 8
report, the score is suppressed.

**5. Why suppression matters.** 1,606 of 4,789 hospitals (33%) lack
enough data — most are small rural or specialty facilities below the
HCAHPS 100-survey threshold. Showing them with partial scores would
penalize them for being small.

**6. Why top performers are small specialty hospitals.** HCAHPS measures
patient experience: cleanliness, quietness, communication, recommendation.
It does NOT measure clinical outcomes. Smaller hospitals with shorter
stays and elective procedures structurally score higher because their
care environment is less chaotic. This isn't a flaw in the data — it's a
real signal about a real (but narrow) construct.

**7. What HCAHPS does NOT measure.** Mortality, complications, readmission,
infection rates, surgical outcomes, equity gaps, wait times.

**8. Limitations.** Self-selection bias (sicker patients respond less);
English-and-Spanish-only surveys (other-language patients excluded);
mode adjustment (mail vs. phone vs. web responses are statistically
adjusted, which adds noise); response rate variance (national average
≈21%; some hospitals 8%, others 50%).

**9. Repo:** <https://github.com/{owner}/Hospital-Patient-Experience-Analytics-Dashboard>

---

## Build order (suggested)

1. Connect all 5 data sources first; check field types are right
2. Add calculated fields on each data source
3. Build Page 4 (Methodology) first — it's just text, fastest win
4. Page 1 KPIs, then histogram, then callout
5. Page 2 choropleth, then table, then bar
6. Page 3 picker scaffolding first, header band, then bar chart, then peer table
7. Apply style guide globally last (Theme & layout → custom theme)
