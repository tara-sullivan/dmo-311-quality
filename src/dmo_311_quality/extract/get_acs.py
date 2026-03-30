# %%
import pandas as pd

from dmo_311_quality.utils.config_paths import ACS_DIR

# %%
# Maps the 2-letter borough prefix used in ACS GeoIDs to the borough name
# used in the 311 community_board field (e.g. "BK01" → "01 BROOKLYN")
BOROUGH_CODE_MAP: dict[str, str] = {
    'BK': 'BROOKLYN',
    'BX': 'BRONX',
    'MN': 'MANHATTAN',
    'QN': 'QUEENS',
    'SI': 'STATEN ISLAND',
}

# Columns to retain from the Demographic ACS file, plus the join key.
# Suffix key: E = estimate, M = margin of error, C = CV, P = percent, Z = z-score
DEMOGRAPHIC_COLS: list[str] = [
    'GeoID',
    'GeogName',
    'Borough',
    'Pop_1E',       # Total population
    'Hsp1E',        # Hispanic/Latino
    'WtNHE',        # White non-Hispanic
    'BlNHE',        # Black non-Hispanic
    'AsnNHE',       # Asian non-Hispanic
    'AIANNHE',      # American Indian/Alaska Native non-Hispanic
    'NHPINHE',      # Native Hawaiian/Pacific Islander non-Hispanic
    'OthNHE',       # Other race non-Hispanic
    'Rc2plNHE',     # Two or more races non-Hispanic
    'MdAgeE',       # Median age
]


# %%
def load_acs_sheet(
    category: str,
    geo_level: str = 'CommunityDistrict-PUMA',
) -> pd.DataFrame:
    """Load the data sheet from an ACS xlsx file.

    Reads the first sheet (index 0), which contains the data for all ACS
    category files (e.g. Demographic, Economic, Social, Housing).

    Args:
        category: Subdirectory name matching an ACS category folder,
            e.g. 'Demographic', 'Economic', 'Social', 'Housing'.
        geo_level: Top-level ACS geography folder under ACS_DIR.
            Defaults to 'CommunityDistrict-PUMA'.

    Returns:
        Raw DataFrame for the requested category — no filtering applied.
    """
    category_dir = ACS_DIR / geo_level / category
    xlsx_files = list(category_dir.glob('*.xlsx'))

    if not xlsx_files:
        raise FileNotFoundError(
            f'No xlsx files found in {category_dir}'
        )
    if len(xlsx_files) > 1:
        raise ValueError(
            f'Expected one xlsx file in {category_dir}, found {len(xlsx_files)}: '
            + ', '.join(f.name for f in xlsx_files)
        )

    return pd.read_excel(xlsx_files[0], sheet_name=0)


# %%
if __name__ == '__main__':
    df_raw = load_acs_sheet('Demographic')
    print(f'{len(df_raw):,} rows | {len(df_raw.columns)} columns')
    print('CDTAType counts:', df_raw['CDTAType'].value_counts().to_dict())
    print(df_raw[['GeoID', 'GeogName', 'CDTAType']].head())


# %%
def make_community_board_key(geo_id: str) -> str:
    """Convert an ACS CDTA GeoID to the 311 community_board format.

    Args:
        geo_id: Two-letter borough code followed by a two-digit CD number,
            e.g. 'BK01'.

    Returns:
        String matching the 311 community_board field, e.g. '01 BROOKLYN'.

    Raises:
        ValueError: If the borough code prefix is not in BOROUGH_CODE_MAP.
    """
    borough_code = geo_id[:2]
    cd_number = geo_id[2:]

    if borough_code not in BOROUGH_CODE_MAP:
        raise ValueError(
            f"Unrecognized borough code '{borough_code}' in GeoID '{geo_id}'. "
            f'Expected one of: {list(BOROUGH_CODE_MAP.keys())}'
        )

    return f'{cd_number} {BOROUGH_CODE_MAP[borough_code]}'


# %%
if __name__ == '__main__':
    examples = ['BK01', 'BX12', 'MN05', 'QN09', 'SI03']
    for geo_id in examples:
        print(f'{geo_id!r} → {make_community_board_key(geo_id)!r}')


# %%
def get_acs_demographics() -> pd.DataFrame:
    """Load and clean ACS demographic data for NYC community districts.

    Reads the CommunityDistrict-PUMA Demographic xlsx, filters to community
    district rows (CDTAType == 'CD'), builds a community_board join key that
    matches the 311 dataset, and returns a selection of estimate columns.

    Returns:
        DataFrame with one row per community district (59 rows). Includes
        the community_board join key and estimate columns for population,
        race/ethnicity, and median age.
    """
    df = load_acs_sheet('Demographic')

    # Exclude JIA rows (parks, airports — e.g. Central Park, JFK)
    df = df[df['CDTAType'] == 'CD'].copy()

    # Build join key matching 311 community_board values (e.g. "01 BROOKLYN")
    df['community_board'] = df['GeoID'].apply(make_community_board_key)

    return df[['community_board'] + DEMOGRAPHIC_COLS].reset_index(drop=True)


# %%
if __name__ == '__main__':
    df = get_acs_demographics()
    print(f'{len(df):,} rows | columns: {df.columns.tolist()}')
    print(df[['community_board', 'Pop_1E', 'Borough']].head())

# %%
