# %%
import datetime

import pandas as pd

from dmo_311_quality.utils.socrata import socrata_api_query

# %%
_TIME_VAR = 'created_date'

# NYC Open Data dataset ID for 311 Service Requests
DATASET_ID = 'erm2-nwe9'


# %%
def get_sr_counts(
    start_date: str = '2023-01-01',
    end_date: str | None = None,
    geo: str = 'community_board',
    time: str | None = 'month',
    level: int = 0,
) -> pd.DataFrame:
    """Query aggregated 311 service request counts from NYC Open Data.

    Args:
        start_date: Inclusive start date, ISO format (default: '2023-01-01').
        end_date: Inclusive end date, ISO format (default: today).
        geo: Column to group by geographically (default: 'community_board').
        time: Time granularity. 'month' adds month and year columns.
            Pass None to omit the time dimension entirely (one row per geo unit).
        level: Problem-type breakdown depth.
            0 = no breakdown (all complaint types aggregated).
            2 = complaint_type + descriptor.

    Returns:
        DataFrame with sr_count and grouping columns.
    """
    if end_date is None:
        end_date = datetime.date.today().isoformat()

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

    df = socrata_api_query(
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

    numeric_cols = ['sr_count'] + (['month', 'year'] if time == 'month' else [])
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
    return df


# %%
if __name__ == '__main__':
    # Monthly counts per community board, 2023 to today
    df = get_sr_counts()
    print(f'{len(df):,} rows | columns: {df.columns.tolist()}')
    print(df.head())

    # # Total counts per community board (no time dimension)
    # df_totals = get_sr_counts(time=None)
    # print(f'\nTotals: {len(df_totals):,} rows')
    # print(df_totals.sort_values('sr_count', ascending=False).head())

# %%
