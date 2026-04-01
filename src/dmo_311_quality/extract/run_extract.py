# %%
import pandas as pd

from dmo_311_quality.extract.get_311 import get_sr_counts
from dmo_311_quality.extract.get_acs import get_acs_demographics, get_acs_economic, get_acs_languages
from dmo_311_quality.extract.get_geo import get_cd_boundaries
from dmo_311_quality.utils.config_paths import PROCESSED_DATA_DIR

RUN_ALL = False

# SoQL filter for pothole-related complaints across all agency types
POTHOLE_WHERE = (
    "(complaint_type = 'Street Condition' AND descriptor = 'Pothole')"
    " OR (complaint_type = 'Highway Condition' AND descriptor = 'Pothole - Highway')"
    " OR (complaint_type = 'Bridge Condition' AND descriptor = 'Pothole')"
    " OR (complaint_type = 'Tunnel Condition' AND descriptor = 'Pothole - Tunnel')"
)

RODENT_WHERE = (
    "(LOWER(complaint_type) LIKE '%rodent%')"
    " OR (LOWER(complaint_type) LIKE '%rat%')"
    " OR (LOWER(descriptor) LIKE '%rodent%')"
    " OR (LOWER(descriptor) LIKE '%rat%')"
    " OR (LOWER(descriptor_2) LIKE '%rodent%')"
    " OR (LOWER(descriptor_2) LIKE '%rat%')"
)


# %%
def extract_311() -> pd.DataFrame:
    """Pull monthly 311 SR counts per community board and save to parquet.

    Returns:
        DataFrame with columns: community_board, month, year, sr_count.
    """
    df = get_sr_counts()
    out_path = PROCESSED_DATA_DIR / '311_sr_counts_ym.parquet'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, engine='fastparquet')
    print(f'311 counts: {len(df):,} rows → {out_path}')
    return df


# %%
if __name__ == '__main__':
    if RUN_ALL:
        df_311 = extract_311()
        print(df_311.head())


# %%
def extract_acs() -> pd.DataFrame:
    """Load ACS demographic data for NYC community districts and save to parquet.

    Returns:
        DataFrame with one row per community district (59 rows).
    """
    df = get_acs_demographics()
    out_path = PROCESSED_DATA_DIR / 'acs_cd_demographic.parquet'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, engine='fastparquet')
    print(f'ACS demographics: {len(df):,} rows → {out_path}')
    return df


# %%
if __name__ == '__main__':
    if RUN_ALL:
        df_acs = extract_acs()
        print(df_acs[['community_board', 'Pop_1E', 'Borough']].head())


# %%
def extract_311_potholes() -> pd.DataFrame:
    """Pull monthly pothole SR counts per community board and save to parquet.

    Covers four pothole complaint types across Street, Highway, Bridge, and
    Tunnel agencies. Returns the same schema as extract_311() so choropleth.py
    functions work without modification.

    Returns:
        DataFrame with columns: community_board, month, year, sr_count.
    """
    df = get_sr_counts(where_extra=POTHOLE_WHERE)
    out_path = PROCESSED_DATA_DIR / '311_sr_potholes_ym.parquet'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, engine='fastparquet')
    print(f'311 pothole counts: {len(df):,} rows → {out_path}')
    return df


# %%
if __name__ == '__main__':
    if RUN_ALL:
        df_potholes = extract_311_potholes()
        print(df_potholes.head())


# %%
def extract_311_rodents() -> pd.DataFrame:
    """Pull monthly rodent-related SR counts per community board and save to parquet.

    Covers complaint types and descriptors containing 'rodent' or 'rat'.
    Returns the same schema as extract_311() so choropleth.py functions work
    without modification.

    Returns:
        DataFrame with columns: community_board, month, year, sr_count.
    """
    df = get_sr_counts(where_extra=RODENT_WHERE)
    out_path = PROCESSED_DATA_DIR / '311_sr_rodents_ym.parquet'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, engine='fastparquet')
    print(f'311 rodent counts: {len(df):,} rows → {out_path}')
    return df


# %%
if __name__ == '__main__':
    if RUN_ALL:
        df_rodents = extract_311_rodents()
        print(df_rodents.head())


# %%
def extract_acs_economic() -> pd.DataFrame:
    """Load ACS economic data for NYC community districts and save to parquet.

    Returns:
        DataFrame with one row per community district (59 rows). Includes
        median household income, per capita income, and population below poverty.
    """
    df = get_acs_economic()
    out_path = PROCESSED_DATA_DIR / 'acs_cd_economic.parquet'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, engine='fastparquet')
    print(f'ACS economic: {len(df):,} rows → {out_path}')
    return df


# %%
if __name__ == '__main__':
    if RUN_ALL:
        df_econ = extract_acs_economic()
        print(df_econ[['community_board', 'MdHHIncE', 'PerCapIncE', 'PBwPvE']].head())


# %%
def extract_acs_languages() -> pd.DataFrame:
    """Load ACS language/LEP data for NYC community districts and save to parquet.

    Returns:
        DataFrame with one row per community district (59 rows). Includes
        community_board and lep_rate (Limited English Proficiency share of pop 5+).
    """
    df = get_acs_languages()
    out_path = PROCESSED_DATA_DIR / 'acs_cd_language.parquet'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, engine='fastparquet')
    print(f'ACS language/LEP: {len(df):,} rows → {out_path}')
    return df


# %%
if __name__ == '__main__':
    if RUN_ALL:
        df_lang = extract_acs_languages()
        print(df_lang[['community_board', 'lep_rate']].sort_values('lep_rate', ascending=False).head())


# %%
def extract_geo() -> pd.DataFrame:
    """Fetch CD boundary polygons and save to parquet with WKT geometry.

    Geometry is stored as WKT strings so fastparquet can write the file.

    Returns:
        DataFrame with columns: community_board, boro_cd, geometry (WKT).
    """
    gdf = get_cd_boundaries()
    df_out = pd.DataFrame(gdf)
    df_out['geometry'] = gdf['geometry'].apply(lambda g: g.wkt)

    out_path = PROCESSED_DATA_DIR / 'cd_boundaries.parquet'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_parquet(out_path, index=False, engine='fastparquet')
    print(f'CD boundaries: {len(df_out):,} rows → {out_path}')
    return df_out


# %%
if __name__ == '__main__':
    if RUN_ALL:
        df_geo = extract_geo()
        print(df_geo[['community_board', 'boro_cd']].head())


# %%
def main(
    run_311: bool = True,
    run_311_potholes: bool = True,
    run_311_rodents: bool = True,
    run_acs: bool = True,
    run_acs_economic: bool = True,
    run_acs_languages: bool = True,
    run_geo: bool = True,
) -> None:
    """Run extract steps in order and save outputs to data/processed/.

    All steps run by default. Pass False for any step to skip it, e.g.:
        main(run_311=False, run_311_potholes=False, run_311_rodents=False)

    Args:
        run_311: Pull all 311 SR counts from Socrata.
        run_311_potholes: Pull pothole SR counts from Socrata.
        run_311_rodents: Pull rodent SR counts from Socrata.
        run_acs: Load ACS demographic data from local xlsx.
        run_acs_economic: Load ACS economic data from local xlsx.
        run_acs_languages: Load ACS language/LEP data from local xlsx.
        run_geo: Fetch community district boundary polygons.
    """
    if run_311:
        print('--- Extract: 311 service requests ---')
        extract_311()

    if run_311_potholes:
        print('\n--- Extract: 311 pothole service requests ---')
        extract_311_potholes()

    if run_311_rodents:
        print('\n--- Extract: 311 rodent service requests ---')
        extract_311_rodents()

    if run_acs:
        print('\n--- Extract: ACS demographics ---')
        extract_acs()

    if run_acs_economic:
        print('\n--- Extract: ACS economic ---')
        extract_acs_economic()

    if run_acs_languages:
        print('\n--- Extract: ACS language/LEP ---')
        extract_acs_languages()

    if run_geo:
        print('\n--- Extract: community district boundaries ---')
        extract_geo()

    print('\nExtract complete.')


# %%
if __name__ == '__main__':
    main()
    # main(run_311=False, run_311_potholes=False, run_311_rodents=False, run_geo=False) 

# %%
