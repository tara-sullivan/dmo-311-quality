# %%
import datetime

import pandas as pd

from dmo_311_quality.utils.config_paths import PROCESSED_DATA_DIR
from dmo_311_quality.utils.socrata import socrata_api_query

# %%
_TIME_VAR = 'created_date'

# NYC Open Data dataset ID for 311 Service Requests
DATASET_ID = 'erm2-nwe9'


# %%
def get_sr_counts(
    start_date: str,
    end_date: str,
    geo: str = 'community_board',
    time: str | None = 'month',
    level: int = 0,
) -> pd.DataFrame:
    """Query aggregated 311 service request counts from NYC Open Data.

    Args:
        start_date: Inclusive start date, ISO format (e.g. '2023-03-01').
        end_date: Inclusive end date, ISO format.
        geo: Column to group by geographically (default: 'community_board').
        time: Time granularity. 'month' adds month and year columns.
            Pass None to omit the time dimension entirely (one row per geo unit).
        level: Problem-type breakdown depth.
            0 = no breakdown (all complaint types aggregated).
            2 = complaint_type + descriptor.

    Returns:
        DataFrame with sr_count and grouping columns.
    """
    if time == 'month':
        time_select = (
            f', date_extract_m({_TIME_VAR}) as month'
            f', date_extract_y({_TIME_VAR}) as year'
        )
        time_group = (
            f', date_extract_m({_TIME_VAR})'
            f', date_extract_y({_TIME_VAR})'
        )
    else:
        time_select = ''
        time_group = ''

    if level == 0:
        problem_cols: list[str] = []
    elif level == 2:
        problem_cols = ['complaint_type', 'descriptor']
    else:
        raise ValueError(f'Unsupported level: {level!r}. Use 0 or 2.')

    problem_select = (', ' + ', '.join(problem_cols)) if problem_cols else ''
    problem_group = problem_select  # same columns

    select_str = f'{geo}{time_select}, count(*) as sr_count{problem_select}'
    group_str = f'{geo}{time_group}{problem_group}'

    return socrata_api_query(
        dataset_id=DATASET_ID,
        select=select_str,
        where=(
            f"(date_trunc_ymd({_TIME_VAR}) >= '{start_date}')"
            f" AND (date_trunc_ymd({_TIME_VAR}) <= '{end_date}')"
        ),
        group=group_str,
        timeout=480,
        limit=500000,
    )


# %%
if __name__ == '__main__':
    # Quick smoke test: one day, no time dimension
    df_test = get_sr_counts(
        start_date='2024-07-04',
        end_date='2024-07-04',
        time=None,
        level=0,
    )
    print(df_test.head())


# %%
def get_311_counts(
    start_date: str = '2023-01-01',
    end_date: str | None = None,
) -> pd.DataFrame:
    """Pull monthly 311 SR counts per community board.

    Args:
        start_date: Inclusive start date (default: '2023-01-01').
        end_date: Inclusive end date (default: today).

    Returns:
        DataFrame with columns: community_board, month, year, sr_count.
    """
    if end_date is None:
        end_date = datetime.date.today().isoformat()

    return get_sr_counts(start_date=start_date, end_date=end_date, time='month', level=0)


# %%
if __name__ == '__main__':
    # Test: one month of data
    df_monthly = get_311_counts(start_date='2024-01-01', end_date='2024-01-31')
    print(f'{len(df_monthly):,} rows | columns: {df_monthly.columns.tolist()}')
    print(df_monthly.head())


# %%
def get_311_total_counts(
    start_date: str = '2023-03-01',
    end_date: str = '2026-03-01',
) -> pd.DataFrame:
    """Pull total 311 SR counts per community board over a date range.

    Aggregates all months into a single count per community board —
    no time dimension is included.

    Args:
        start_date: Inclusive start date (default: '2023-03-01').
        end_date: Inclusive end date (default: '2026-03-01').

    Returns:
        DataFrame with columns: community_board, sr_count.
    """
    return get_sr_counts(start_date=start_date, end_date=end_date, time=None, level=0)


# %%
if __name__ == '__main__':
    df_totals = get_311_total_counts()
    print(f'{len(df_totals):,} rows | columns: {df_totals.columns.tolist()}')
    print(df_totals.sort_values('sr_count', ascending=False).head())

    out_path = PROCESSED_DATA_DIR / '311_totals.parquet'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_totals.to_parquet(out_path, index=False, engine='fastparquet')
    print(f'Saved to {out_path}')
