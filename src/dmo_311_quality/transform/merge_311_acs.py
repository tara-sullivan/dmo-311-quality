# %%
import pandas as pd

from dmo_311_quality.utils.config_paths import PROCESSED_DATA_DIR


if __name__ == '__main__':
    df_311 = pd.read_parquet(PROCESSED_DATA_DIR / '311_sr_counts.parquet')
    df_acs = pd.read_parquet(PROCESSED_DATA_DIR / 'acs_cd_demographic.parquet')


    keep_row = (
        (df_311['year'] * 100 + df_311['month'] >= 202503)
        & (df_311['year'] * 100 + df_311['month'] <= 202603)
    )
    df_311 = df_311.loc[keep_row].groupby('community_board')['sr_count'].sum()

# %%

def merge_311_acs(
    df_311: pd.DataFrame,
    df_acs: pd.DataFrame,
    acs_cols = ['Pop_1E', 'Borough'],
    validate = '1:1',
    print_unmatched: bool = False,
) -> pd.DataFrame:
    """Left-join 311 service request data with ACS demographic data.

    Joins on community_board so every 311 row is retained. Rows with no
    ACS match (e.g. '0 Unspecified', JIA entries) will have null ACS columns.

    Args:
        df_311: 311 service request counts with a community_board column.
        df_acs: ACS demographic data with a community_board column.
        acs_cols: List of ACS columns to include in the merge.
        validate: String to pass to pd.merge's validate parameter.
        print_unmatched: Whether to print unmatched 311 community boards.

    Returns:
        Merged DataFrame with all 311 rows and ACS columns appended.
    """
    merged = pd.merge(
        left=df_311,
        right=df_acs[acs_cols + ['community_board']], 
        on='community_board', 
        how='left',
        validate=validate,
        # indicator=True
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

    merged = merge_311_acs(df_311, df_acs, print_unmatched=True)

    # out_path = PROCESSED_DATA_DIR / '311_acs_merged.parquet'
    # merged.to_parquet(out_path, index=False, engine='fastparquet')
    # print(f'Saved {len(merged):,} rows to {out_path}')

# %%
