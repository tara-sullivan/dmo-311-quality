# %%
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.axes import Axes

from dmo_311_quality.extract.get_geo import get_cd_boundaries
from dmo_311_quality.utils.config_paths import PROCESSED_DATA_DIR, ROOT_DIR

from dmo_311_quality.transform.merge_311_acs import merge_311_acs
# %%
FIGURES_DIR = ROOT_DIR / 'output' / 'figures'

## %%
if __name__ == '__main__':
    df_311 = pd.read_parquet(PROCESSED_DATA_DIR / '311_sr_counts.parquet')
    df_acs = pd.read_parquet(PROCESSED_DATA_DIR / 'acs_cd_demographic.parquet')

    cd_gdf = get_cd_boundaries()

# %%

def sr_per_1k(df_311, df_acs, start_date: str = '2025-03', end_date: str = '2026-03') -> pd.DataFrame:
    """Calculate 311 service requests per 1,000 residents."""
    start = int(start_date.replace('-', ''))
    end = int(end_date.replace('-', ''))
    keep_row = df_311['year'] * 100 + df_311['month']
    df = (
        df_311.loc[keep_row.between(start, end)]
        .groupby('community_board')['sr_count']
        .sum()
    )

    df = merge_311_acs(df, df_acs, acs_cols=['Pop_1E'], validate='1:1')
    df['sr_per_1k'] = df['sr_count'] / df['Pop_1E'] * 1000
    return df

if __name__ == '__main__':
    df = sr_per_1k(df_311, df_acs)
    
# %%

def gdf_per_1k(gdf, sr_df):
    """Merge sr_per_1k values into the GeoDataFrame."""
    gdf = gdf.merge(sr_df[['community_board', 'sr_per_1k', 'sr_count']], on='community_board', how='left')
    return gdf

if __name__ == '__main__':
    gdf = gdf_per_1k(cd_gdf, df)

# %%
def make_choropleth(
    gdf,
    column: str,
    title: str,
    ax: Axes,
    cmap: str = 'YlOrRd',
) -> None:
    """Plot a single choropleth panel onto the provided axes.

    Args:
        gdf: GeoDataFrame with geometry and the column to plot.
        column: Column name to use for the fill color.
        title: Axes title.
        ax: Matplotlib Axes to draw on.
        cmap: Colormap name (default: 'YlOrRd').
    """

    # Base layer for missing data
    gdf.plot(color='gray', edgecolor='white', linewidth=0.3, alpha=0.3, ax=ax) 
    gdf.plot(
        column=column,
        ax=ax,
        cmap=cmap,
        legend=True,
        legend_kwds={'shrink': 0.7},
        missing_kwds={'color': 'lightgrey', 'label': 'No data'},
        edgecolor='white',
        linewidth=0.3,
    )
    ax.set_title(title, fontsize=11)
    ax.axis('off')


# %%
if __name__ == '__main__':

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle(
        '311 Service Requests by Community District\n(Mar 2023 – Mar 2026)',
        fontsize=13,
        y=1.01,
    )

    make_choropleth(gdf, 'sr_count', 'Total SR Count', axes[0])
    make_choropleth(gdf, 'sr_per_1k', 'SR per 1,000 Residents', axes[1])

    plt.tight_layout()

    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    out_path = FIGURES_DIR / 'choropleth_311_acs.png'
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    print(f'Saved figure to {out_path}')

# %%
