# %%
import pandas as pd

from dmo_311_quality.transform.merge_311_acs import complaints_per_capita
from dmo_311_quality.utils.config_paths import PROCESSED_DATA_DIR


# %%
def transform_complaints_per_capita() -> pd.DataFrame:
    """Compute 311 complaints per 1,000 residents by community board and save.

    Loads 311 monthly counts and ACS demographics from processed parquets,
    filters to March 2025–March 2026, joins on community_board, and computes
    sr_per_1k. Saves result to data/processed/311_acs_per_capita.parquet.

    Returns:
        DataFrame with one row per community board: community_board, sr_count,
        Pop_1E, Borough, sr_per_1k.
    """
    df_311 = pd.read_parquet(PROCESSED_DATA_DIR / '311_sr_counts.parquet')
    df_acs = pd.read_parquet(PROCESSED_DATA_DIR / 'acs_cd_demographic.parquet')

    df = complaints_per_capita(df_311, df_acs)

    out_path = PROCESSED_DATA_DIR / '311_acs_per_capita.parquet'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, engine='fastparquet')
    print(f'Per-capita complaints: {len(df):,} rows → {out_path}')
    return df


# %%
if __name__ == '__main__':
    df = transform_complaints_per_capita()
    print(df.sort_values('sr_per_1k', ascending=False).head(10))


# %%
def main() -> None:
    """Run all transform steps in order and save outputs to data/processed/."""
    print('--- Transform: complaints per capita ---')
    transform_complaints_per_capita()

    print('\nTransform complete.')


# %%
if __name__ == '__main__':
    main()

# %%
