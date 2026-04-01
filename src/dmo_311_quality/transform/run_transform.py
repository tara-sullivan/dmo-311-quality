# %%
import pandas as pd

# from dmo_311_quality.transform.merge_311_acs import complaints_per_capita, enrich_with_economic
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
<<<<<<< HEAD
def transform_economic_join(
    df_311=None,
    econ_vars=None,
    out_filename='311_acs_economic.parquet',
=======
_SR_PARQUET: dict[str, str] = {
    'all': '311_sr_counts',
    'potholes': '311_sr_potholes',
    'rodents': '311_sr_rodents',
}


# %%
def transform_economic_join(
    complaint_type: str = 'all',
    econ_vars: list[str] = ['MdHHIncE', 'PerCapIncE', 'PBwPvE'],
>>>>>>> a1d5f3706f59018a8c791437bce4af3727e996ed
) -> pd.DataFrame:
    """Join ACS economic variables into per-capita complaints and save.

    Loads 311 SR counts for the given complaint_type, ACS demographics, and
    ACS economic data, then enriches with median household income, per-capita
    income, and poverty rate.

    Saves to data/processed/311_acs_economic.parquet (complaint_type='all') or
    data/processed/311_acs_economic_{complaint_type}.parquet otherwise.

    Args:
        complaint_type: One of 'all', 'potholes', 'rodents'. Controls which
            311 SR parquet is loaded and where the output is saved.
        econ_vars: ACS economic columns to join.

    Returns:
        DataFrame with one row per community board: community_board, sr_count,
        Pop_1E, Borough, sr_per_1k, MdHHIncE, PerCapIncE, PBwPvE, poverty_rate.
    """
<<<<<<< HEAD
    if econ_vars is None:
        econ_vars = ['MdHHIncE', 'PerCapIncE', 'PBwPvE']
    if df_311 is None:
        df_311 = pd.read_parquet(PROCESSED_DATA_DIR / '311_sr_counts.parquet')
=======
    sr_file = _SR_PARQUET[complaint_type]
    df_311 = pd.read_parquet(PROCESSED_DATA_DIR / f'{sr_file}.parquet')
>>>>>>> a1d5f3706f59018a8c791437bce4af3727e996ed
    df_acs = pd.read_parquet(PROCESSED_DATA_DIR / 'acs_cd_demographic.parquet')
    df_econ = pd.read_parquet(PROCESSED_DATA_DIR / 'acs_cd_economic.parquet')
    df_econ = df_econ[['community_board'] + econ_vars]

    df = complaints_per_capita(df_311, df_acs)
    # add in economic varaibles
    df = pd.merge(df, df_econ, on='community_board', how='left', validate='1:1')
    # calculate poverty rate if PBwPvE is included
    if 'PBwPvE' in econ_vars:
        df['poverty_rate'] = df['PBwPvE'] / df['Pop_1E']  # Convert percentage to proportion

    suffix = '' if complaint_type == 'all' else f'_{complaint_type}'
    out_path = PROCESSED_DATA_DIR / f'311_acs_economic{suffix}.parquet'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, engine='fastparquet')
    print(f'Economic join ({complaint_type}): {len(df):,} rows → {out_path}')
    return df


# %%
if __name__ == '__main__':
    df = transform_economic_join('potholes')
    print(df[['community_board', 'sr_per_1k', 'MdHHIncE', 'poverty_rate']].sort_values('sr_per_1k', ascending=False).head(10))


# %%
def main() -> None:
    """Run all transform steps in order and save outputs to data/processed/."""
    print('--- Transform: complaints per capita ---')
    transform_complaints_per_capita()

    print('\n--- Transform: economic join ---')
    for ct in _SR_PARQUET:
        transform_economic_join(ct)

    print('\nTransform complete.')


# %%
if __name__ == '__main__':
    main()

# %%
