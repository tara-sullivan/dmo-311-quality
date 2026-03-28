# %%
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.axes import Axes

from dmo_311_quality.extract.get_geo import get_cd_boundaries
from dmo_311_quality.utils.config_paths import PROCESSED_DATA_DIR, ROOT_DIR

# %%
FIGURES_DIR = ROOT_DIR / 'output' / 'figures'


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
    # Test make_choropleth with a simple column on the boundary GeoDataFrame
    import numpy as np

    gdf_test = get_cd_boundaries()
    gdf_test['dummy'] = np.random.default_rng(seed=0).integers(0, 100, len(gdf_test))

    fig, ax = plt.subplots(figsize=(5, 5))
    make_choropleth(gdf_test, 'dummy', 'Dummy values (smoke test)', ax)
    plt.tight_layout()
    plt.show()


# %%
if __name__ == '__main__':
    # Full pipeline: load data, merge, compute per-capita, plot and save
    df_311 = pd.read_parquet(PROCESSED_DATA_DIR / '311_totals.parquet')
    df_acs = pd.read_parquet(PROCESSED_DATA_DIR / 'acs_cd_demographic.parquet')

    df = df_311.merge(df_acs[['community_board', 'Pop_1E']], on='community_board', how='left')
    df['sr_count'] = pd.to_numeric(df['sr_count'])
    df['Pop_1E'] = pd.to_numeric(df['Pop_1E'])
    df['sr_per_1k'] = df['sr_count'] / df['Pop_1E'] * 1000

    gdf = get_cd_boundaries()
    gdf = gdf.merge(df, on='community_board', how='left')
    gdf = gdf[gdf['sr_count'].notna()].copy()

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
