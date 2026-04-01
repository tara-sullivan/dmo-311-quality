# %%
import pandas as pd

from dmo_311_quality.utils.config_paths import PROCESSED_DATA_DIR, SR_WINDOW_START, SR_WINDOW_END


# %%
def merge_311_acs(
    df_311: pd.DataFrame,
    df_acs: pd.DataFrame,
    acs_cols: list[str] = ['Pop_1E', 'Borough'],
    validate: str = '1:1',
    print_unmatched: bool = False,
) -> pd.DataFrame:
    """Left-join 311 service request data with ACS demographic data.

    Joins on community_board so every 311 row is retained. Rows with no
    ACS match (e.g. '0 Unspecified', JIA entries) will have null ACS columns.

    Args:
        df_311: 311 service request counts with a community_board column.
        df_acs: ACS demographic data with a community_board column.
        acs_cols: List of ACS columns to include in the merge.
        validate: Passed to pd.merge's validate parameter.
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
    )

    if print_unmatched:
        unmatched = merged[merged['Pop_1E'].isna()]['community_board'].unique()
        if len(unmatched):
            print(
                f'{len(unmatched)} community_board value(s) had no ACS match '
                f'(rows retained with null ACS columns):\n  ' + '\n  '.join(sorted(unmatched))
            )

    return merged


# %%
if __name__ == '__main__':
    df_311_raw = pd.read_parquet(PROCESSED_DATA_DIR / '311_sr_counts_ym.parquet')
    df_acs = pd.read_parquet(PROCESSED_DATA_DIR / 'acs_cd_demographic.parquet')
    merged = merge_311_acs(df_311_raw, df_acs, print_unmatched=True, validate='m:1')
    print(merged.head())


# %%
def complaints_per_capita(
    df_311: pd.DataFrame,
    df_acs: pd.DataFrame,
    window_start: int = SR_WINDOW_START,
    window_end: int = SR_WINDOW_END,
) -> pd.DataFrame:
    """Compute 311 complaints per 1,000 residents by community board.

    Filters 311 data to the given window (YYYYMM integers, inclusive),
    aggregates to one row per community board, joins ACS population, and
    computes a per-capita rate. Rows with no ACS match (e.g. '0 Unspecified')
    are dropped.

    Args:
        df_311: Monthly 311 SR counts with columns: community_board, month,
            year, sr_count.
        df_acs: ACS demographic data with columns: community_board, Pop_1E,
            Borough (as returned by get_acs_demographics()).
        window_start: Start of aggregation window as YYYYMM integer (inclusive).
            Defaults to SR_WINDOW_START from config_paths.
        window_end: End of aggregation window as YYYYMM integer (inclusive).
            Defaults to SR_WINDOW_END from config_paths.

    Returns:
        DataFrame with one row per community board (up to 59 rows). Columns:
        community_board, sr_count, Pop_1E, Borough, sr_per_1k.
    """
    yyyymm = df_311['year'] * 100 + df_311['month']
    df_window = df_311.loc[(yyyymm >= window_start) & (yyyymm <= window_end)]

    sr_totals = (
        df_window.groupby('community_board', as_index=False)['sr_count']
        .sum()
    )

    merged = merge_311_acs(sr_totals, df_acs, acs_cols=['Pop_1E', 'Borough'])

    n_unmatched = merged['Pop_1E'].isna().sum()
    if n_unmatched:
        unmatched = merged.loc[merged['Pop_1E'].isna(), 'community_board'].tolist()
        print(f'Dropping {n_unmatched} unmatched community board(s): {unmatched}')
    merged = merged.dropna(subset=['Pop_1E']).copy()

    merged['sr_per_1k'] = merged['sr_count'] / merged['Pop_1E'] * 1000

    return merged.reset_index(drop=True)


# %%
if __name__ == '__main__':
    df_311 = pd.read_parquet(PROCESSED_DATA_DIR / '311_sr_counts_ym.parquet')
    df_acs = pd.read_parquet(PROCESSED_DATA_DIR / 'acs_cd_demographic.parquet')

    df = complaints_per_capita(df_311, df_acs)
    print(f'{len(df):,} rows | columns: {df.columns.tolist()}')
    print(df.sort_values('sr_per_1k', ascending=False).head(10))

# %%
