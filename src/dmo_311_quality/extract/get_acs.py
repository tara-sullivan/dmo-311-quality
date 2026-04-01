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


# Columns to retain from the Economic ACS file, plus the join key.
ECONOMIC_COLS: list[str] = [
    'GeoID',
    'GeogName',
    'Borough',
    'MdHHIncE',     # Median household income
    'PerCapIncE',   # Per capita income
    'PBwPvE',       # Population below poverty level (poverty status determined)
]


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
def get_acs_economic() -> pd.DataFrame:
    """Load and clean ACS economic data for NYC community districts.

    Reads the CommunityDistrict-PUMA Economic xlsx, filters to community
    district rows (CDTAType == 'CD'), builds a community_board join key that
    matches the 311 dataset, and returns income and poverty estimate columns.

    Returns:
        DataFrame with one row per community district (59 rows). Includes
        community_board, GeoID, GeogName, Borough, MdHHIncE, PerCapIncE,
        and PBwPvE (population below poverty level).
    """
    df = load_acs_sheet('Economic')

    df = df[df['CDTAType'] == 'CD'].copy()

    df['community_board'] = df['GeoID'].apply(make_community_board_key)

    return df[['community_board'] + ECONOMIC_COLS].reset_index(drop=True)


# %%
if __name__ == '__main__':
    df_econ = get_acs_economic()
    print(f'{len(df_econ):,} rows | columns: {df_econ.columns.tolist()}')
    print(df_econ[['community_board', 'MdHHIncE', 'PerCapIncE', 'PBwPvE']].head(10))


# %%
def parse_language_district(cd_str: str) -> list[str]:
    """Convert a language-file Community District string to community_board keys.

    Handles single-CD rows (e.g. 'BX CD 4' → ['04 BRONX']) and compound rows
    where one PUMA covers two CDs (e.g. 'BX CDs 1 & 2' → ['01 BRONX', '02 BRONX']).

    Args:
        cd_str: Community District string from the DCP language xlsx, e.g.
            'BX CD 4', 'MN CDs 5 & 6'.

    Returns:
        List of community_board strings in '## BOROUGH' format.
    """
    parts = cd_str.strip().split()
    borough = BOROUGH_CODE_MAP[parts[0]]

    if parts[1] == 'CDs':
        # Compound: e.g. ['BX', 'CDs', '1', '&', '2']
        return [f'{int(parts[2]):02d} {borough}', f'{int(parts[4]):02d} {borough}']
    else:
        # Single: e.g. ['BX', 'CD', '4']
        return [f'{int(parts[2]):02d} {borough}']


# %%
if __name__ == '__main__':
    examples = ['BX CD 4', 'BX CDs 1 & 2', 'MN CDs 5 & 6', 'SI CD 1']
    for ex in examples:
        print(f'{ex!r} → {parse_language_district(ex)}')


# %%
def get_acs_languages() -> pd.DataFrame:
    """Load and clean ACS language/LEP data for NYC community districts.

    Reads the DCP CommunityDistrict-PUMA language xlsx ('English Lang Use &
    Proficiency' sheet), extracts estimates for total population 5+, English-
    only speakers, non-English speakers, and Limited English Proficiency (LEP)
    count, then expands compound-PUMA rows into one row per community district.

    Note: Four PUMAs cover two community districts each. Those pairs share the
    same language profile — sub-PUMA disaggregation is not available in this
    source.

    Returns:
        DataFrame with one row per community district (59 rows). Columns:
        community_board, pop_5plus, eng_only, non_eng, lep_count, lep_rate.
    """
    lang_dir = ACS_DIR / 'CommunityDistrict-PUMA' / 'Languages'
    xlsx_files = list(lang_dir.glob('dcp-lang-spk-at-home-puma_*.xlsx'))

    if not xlsx_files:
        raise FileNotFoundError(f'No dcp-lang-spk-at-home-puma_*.xlsx found in {lang_dir}')
    if len(xlsx_files) > 1:
        raise ValueError(
            f'Expected one matching xlsx in {lang_dir}, found {len(xlsx_files)}: '
            + ', '.join(f.name for f in xlsx_files)
        )

    # Rows 0-3: title, subtitle, two blank rows; rows 4-5: two-level header; row 6+: data
    df_raw = pd.read_excel(
        xlsx_files[0],
        sheet_name='English Lang Use & Proficiency',
        header=None,
        skiprows=4,
    )

    # Skip the two header rows, keep data only
    df = df_raw.iloc[2:].reset_index(drop=True)

    # Note: excel file has multiple column lines; not sure there is a better way to do this
    # Select estimate columns by verified position:
    # 2: Community District, 3: Total pop 5+, 7: Eng only, 11: Non-Eng, 19: LEP
    df = df[[2, 3, 7, 11, 19]].copy()
    df.columns = ['cd_str', 'pop_5plus', 'eng_only', 'non_eng', 'lep_count']

    # Drop footer rows (source notes, blank rows)
    valid_prefixes = tuple(BOROUGH_CODE_MAP.keys())
    df = df[df['cd_str'].apply(
        lambda x: isinstance(x, str) and x.startswith(valid_prefixes)
    )].copy()

    # Expand compound-PUMA rows into one row per community board
    rows = []
    for _, row in df.iterrows():
        for cb in parse_language_district(row['cd_str']):
            rows.append({
                'community_board': cb,
                'pop_5plus': row['pop_5plus'],
                'eng_only': row['eng_only'],
                'non_eng': row['non_eng'],
                'lep_count': row['lep_count'],
            })

    result = pd.DataFrame(rows)
    # rate should be derived from the spreadsheet, not the actualy popuation counts, since some of the community districts are 
    # combined in data (i.e. MN CDs 1 & 2)
    result['lep_rate'] = result['lep_count'] / result['pop_5plus']

    result = result[['community_board', 'lep_rate']]

    return result.reset_index(drop=True)


# %%
if __name__ == '__main__':
    df_lang = get_acs_languages()
    print(f'{len(df_lang):,} rows | columns: {df_lang.columns.tolist()}')
    print(df_lang[['community_board', 'lep_rate']].sort_values('lep_rate', ascending=False).head(10))
    # Verify compound-CD expansion: paired CDs should share identical stats
    compound_cbs = ['01 BRONX', '02 BRONX', '03 BRONX', '06 BRONX',
                    '01 MANHATTAN', '02 MANHATTAN', '05 MANHATTAN', '06 MANHATTAN']
    print('\nCompound-CD pairs (lep_rate should match within each pair):')
    print(df_lang[df_lang['community_board'].isin(compound_cbs)][['community_board', 'lep_rate']].to_string(index=False))

# %%
