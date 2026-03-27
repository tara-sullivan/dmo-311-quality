# %%
from dmo_311_quality.utils.socrata import socrata_api_query

import textwrap
# %%

## Note: I'd like to use weeks, but things get wonky around new years
# Need to sort out ISO weeks

# 'descriptor_2'
time_var = 'created_date'

def get_sr_counts(start_date, geo='community_board', time='month', level=2):

    if time == 'month':
        time_str_select = (
            f', date_extract_m({time_var}) as month'
            + f', date_extract_y({time_var}) as year'
        )
        time_str_group = (
            f', date_extract_m({time_var})'
            + f', date_extract_y({time_var})'
        )

    if level == 2:
        problem_var = ['complaint_type', 'descriptor'] 

    select_str = (
        f'{geo}, ' + ', '.join(problem_var)
        # + f', date_extract_woy({time_var}) as week'
        + time_str_select
        + ', count(*) as sr_count'
    )
    print(textwrap.fill(select_str, 80))
    group_str = (
        f'{geo}, ' + ', '.join(problem_var)
        # + f', date_extract_woy({time_var})'
        + time_str_group
    )
    print(textwrap.fill(group_str, 80))

    df = socrata_api_query(
        dataset_id='erm2-nwe9',
        select=select_str,
        where=f"(date_trunc_ymd({time_var}) = '{start_date}')",
        group=group_str,
        timeout=480,
    )

    return df


if __name__ == '__main__':
    df = get_sr_counts(start_date='2026-01-01')
# %%
