# Tasks

## Goal
Assess whether NYC 311 service requests systematically under- or over-represent
neighborhoods, and evaluate the equity implications for city service allocation.

The core question: **do communities with certain demographic characteristics
(race, income, age) file fewer 311 complaints per capita — and if so, does that
translate into worse service outcomes?**

The primary unit of analysis is the **community board** (59 districts citywide),
matched across the 311 dataset (`erm2-nwe9`) and the ACS 5-year 2023 estimates.
The study window is **March 1, 2023 – March 1, 2026**.

---

## Completed

### Extract
- [x] `extract/get_311.py` — pulls monthly and total SR counts per community board from Socrata
- [x] `extract/get_acs.py` — reads ACS demographic data (population, race/ethnicity, median age) from local xlsx
- [x] `extract/get_geo.py` — fetches community district boundary polygons from Socrata

### Transform
- [x] `transform/merge_311_acs.py` — left-joins monthly 311 data with ACS demographics on `community_board`

### Visualize
- [x] `visualize/choropleth.py` — choropleth maps of total SR count and SR per 1,000 residents (QA/sanity check)

---

## Up Next

### Analysis
- [ ] Compute SR rate per 1,000 residents for each community board (done in choropleth — formalize in `transform/`)
- [ ] Correlation analysis: SR rate vs. race/ethnicity composition, income, other demographic variables
- [ ] Identify outlier community boards (high/low SR rate relative to demographics)
- [ ] Add economic ACS variables (median household income, poverty rate) — see `get_acs.py` design notes

### Output (format TBD)
- [ ] Decide on final output format: GitHub Pages, PDF report, or both
- [ ] If GitHub Pages: set up site structure and figure export pipeline
- [ ] If report: determine key tables and figures needed
- [ ] Write narrative interpretation of findings

### Stretch
- [ ] Extend analysis to complaint type (level=2 in `get_sr_counts`) — are certain complaint categories more skewed?
- [ ] Time trend analysis: has representation improved or worsened over the study window?
