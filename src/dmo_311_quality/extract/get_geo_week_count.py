# %%
from dmo_311_quality.utils.socrata import socrata_api_query

import textwrap
# %%

geo_var = 'community_board'
problem_var = ['complaint_type', 'descriptor'] # 'descriptor_2'
time_var = 'created_date'

select_str = (
    f'{geo_var},'
    # ' complaint_type, descriptor,'  #' descriptor_2,'
    f' date_extract_woy({time_var}) as week,'
    f' date_extract_y({time_var}) as year,'
    ' count(*) as sr_count',
)
print(textwrap.fill(select_str, 80))
group_str = (
        f"{geo_var}"
        # ", complaint_type, descriptor" # ", descriptor_2"
        ", year, week"
    )
print(textwrap.fill(group_str, 80))

df = socrata_api_query(
    dataset_id='erm2-nwe9',
    select=select_str,
    where=f"(date_trunc_ymd({time_var}) = '2025-12-31')",
    group=group_str,
    timeout=480,
)
# %%
