# %%
import pandas as pd

from dmo_311_quality.utils.config_paths import PROCESSED_DATA_DIR

if __name__ == '__main__':
    df_311 = pd.read_parquet(PROCESSED_DATA_DIR / '311_raw.parquet')
    df_acs = pd.read_parquet(PROCESSED_DATA_DIR / 'acs_cd_demographic.parquet')

# %%

keep_row = (
    df['']
)

# %%

def merge_311_acs(
    df_311: pd.DataFrame,
    df_acs: pd.DataFrame,
) -> pd.DataFrame:
    """Left-join 311 service request data with ACS demographic data.

    Joins on community_board so every 311 row is retained. Rows with no
    ACS match (e.g. '0 Unspecified', JIA entries) will have null ACS columns.

    Args:
        df_311: 311 service request counts with a community_board column.
        df_acs: ACS demographic data with a community_board column.

    Returns:
        Merged DataFrame with all 311 rows and ACS columns appended.
    """
    merged = pd.merge(
        left=df_311,
        right=df_acs, 
        on='community_board', 
        how='left',
        validate='m:1',
        indicator=True
    )

    # Report on unmatched 311 community boards
    unmatched = merged[merged['Pop_1E'].isna()]['community_board'].unique()
    if len(unmatched):
        print(
            f'{len(unmatched)} community_board value(s) had no ACS match '
            f'(rows retained with null ACS columns):\n  ' + '\n  '.join(sorted(unmatched))
        )

    return merged


if __name__ == '__main__':

    merged = merge_311_acs(df_311, df_acs)

    out_path = PROCESSED_DATA_DIR / '311_acs_merged.parquet'
    merged.to_parquet(out_path, index=False, engine='fastparquet')
    print(f'Saved {len(merged):,} rows to {out_path}')

# %%
