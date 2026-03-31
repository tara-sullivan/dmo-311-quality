# 311 Data Quality Analysis

## Purpose
Assess whether NYC 311 complaints systematically under/over-represent neighborhoods; evaluate equity implications for service allocation.

## Stack
- Python via `uv` — add packages with `uv add`, never `pip install`
- Socrata API via `sodapy`
- ACS data: local xlsx files in `data/raw/`

## Structure
src/dmo_311_quality/
  extract/      # API pulls and local file reads only
  transform/    # merges and analytical datasets
  visualize/    # figure generation → output/figures/
  utils/        # shared helpers (socrata.py, config_paths.py)
data/raw/       # never modified
data/processed/ # transformed outputs
output/figures/
output/reports/

## Conventions
- Python data pipeline with ETL stage separation (extract → transform → visualize)
- All paths via `config_paths.py` — never hardcode
- Write reusable functions; use `# %%` cell blocks throughout, and add a `if __name__ == '__main__'` test block immediately after each function definition so it can be run interactively in VS Code
- In general, datasets shouldn't be saved to git. Please add appropriate filepaths to the `.gitignore`
- Prefer fewer files; don't create a new module unless it serves a clearly distinct.
- Prefer fewer functions; don't create redundant functions. 
- Single orchestration file that runs all extract steps in order
- All file saves (paths + actual I/O) live in `run_extract.py`, not in individual extract modules. Wrap everything in a `main()` function. Break `main()` into sub-functions per dataset, each with its own `if __name__ == '__main__'` test block

## Tasks
Maintain TASKS.md. After every session, update it with what was completed and what's next.

## Workflow
- Propose an approach and wait for approval before writing code
- After each function, summarize what it does and what should be verified
- Flag any assumptions made about data structure or column names
