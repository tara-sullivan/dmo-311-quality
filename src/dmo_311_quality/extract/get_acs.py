# %%
import pandas as pd

from dmo_311_quality.utils.config_paths import ACS_DIR


# %%

acs_cd_file = ACS_DIR / 'CommunityDistrict-PUMA' / 'Demographic' / 'Dem_1923_CDTA.xlsx'

df = pd.read_excel(acs_cd_file)
# %%
