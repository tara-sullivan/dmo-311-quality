# %%
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape

from dmo_311_quality.utils.config_paths import PROCESSED_DATA_DIR
from dmo_311_quality.utils.socrata import socrata_api_query

# %%
# NYC Open Data dataset ID for community district boundaries
CD_BOUNDARIES_DATASET = '6ak9-vek3'

# Maps the leading digit of boro_cd to the borough name used in community_board
BORO_CD_MAP: dict[str, str] = {
    '1': 'MANHATTAN',
    '2': 'BRONX',
    '3': 'BROOKLYN',
    '4': 'QUEENS',
    '5': 'STATEN ISLAND',
}


# %%
def _boro_cd_to_community_board(boro_cd: str) -> str:
    """Convert a Socrata boro_cd value to the 311 community_board format.

    Args:
        boro_cd: String like '314' (borough digit + 2-digit CD number).

    Returns:
        String matching the 311 community_board field, e.g. '14 BROOKLYN'.

    Raises:
        ValueError: If the borough digit is not in BORO_CD_MAP.
    """
    boro_digit = boro_cd[0]
    cd_number = boro_cd[1:].lstrip('0') or '0'
    cd_padded = cd_number.zfill(2)

    if boro_digit not in BORO_CD_MAP:
        raise ValueError(
            f"Unrecognized borough digit '{boro_digit}' in boro_cd '{boro_cd}'. "
            f'Expected one of: {list(BORO_CD_MAP.keys())}'
        )

    return f'{cd_padded} {BORO_CD_MAP[boro_digit]}'


# %%
if __name__ == '__main__':
    examples = ['101', '214', '314', '409', '501']
    for boro_cd in examples:
        print(f'{boro_cd!r} → {_boro_cd_to_community_board(boro_cd)!r}')


# %%
def get_cd_boundaries() -> gpd.GeoDataFrame:
    """Fetch NYC community district boundary polygons from NYC Open Data.

    Queries dataset 6ak9-vek3, parses GeoJSON geometry, and builds a
    community_board join key matching the 311 dataset format.

    Returns:
        GeoDataFrame with columns: community_board, boro_cd, geometry (EPSG:4326).
    """
    df = socrata_api_query(
        dataset_id=CD_BOUNDARIES_DATASET,
        select='boro_cd, the_geom',
        timeout=30,
        limit=100,  # ~59 community districts + JIA
    )

    df['geometry'] = [shape(geo) for geo in df['the_geom']]
    df['community_board'] = df['boro_cd'].apply(_boro_cd_to_community_board)

    gdf = gpd.GeoDataFrame(
        df[['community_board', 'boro_cd', 'geometry']],
        geometry='geometry',
        crs='EPSG:4326',
    )

    return gdf


# %%
if __name__ == '__main__':
    gdf = get_cd_boundaries()
    print(f'{len(gdf):,} rows | CRS: {gdf.crs}')
    print(gdf[['community_board', 'boro_cd']].head())

    out_path = PROCESSED_DATA_DIR / 'cd_boundaries.parquet'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Convert to plain DataFrame with WKT geometry so fastparquet can write it
    df_out = pd.DataFrame(gdf)
    df_out['geometry'] = gdf['geometry'].apply(lambda g: g.wkt)
    df_out.to_parquet(out_path, index=False, engine='fastparquet')
    print(f'Saved to {out_path}')
