# source /Users/tarasullivan/Documents/oda/dmo-311-quality/.venv/bin/activate
# %%
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.axes import Axes

from dmo_311_quality.utils.config_paths import PROCESSED_DATA_DIR, ROOT_DIR

# %%
FIGURES_DIR = ROOT_DIR / 'output' / 'figures'

BOROUGH_COLORS: dict[str, str] = {
    'Bronx': '#e41a1c',
    'Brooklyn': '#377eb8',
    'Manhattan': '#4daf4a',
    'Queens': '#ff7f00',
    'Staten Island': '#984ea3',
}


# %%
def make_scatter(
    ax: Axes,
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    xlabel: str,
    ylabel: str,
    title: str,
) -> None:
    """Plot a scatter panel with borough-colored points and a regression line.

    Args:
        ax: Matplotlib Axes to draw on.
        df: DataFrame with columns for x_col, y_col, and Borough.
        x_col: Column name for the x-axis variable.
        y_col: Column name for the y-axis variable.
        xlabel: x-axis label.
        ylabel: y-axis label.
        title: Axes title.
    """
    for borough, group in df.groupby('Borough'):
        ax.scatter(
            group[x_col],
            group[y_col],
            color=BOROUGH_COLORS.get(borough, 'gray'),
            label=borough,
            alpha=0.8,
            edgecolors='white',
            linewidths=0.4,
            s=55,
            zorder=3,
        )

    x = df[x_col].to_numpy()
    y = df[y_col].to_numpy()
    m, b = np.polyfit(x, y, 1)
    x_line = np.linspace(x.min(), x.max(), 200)
    ax.plot(x_line, m * x_line + b, color='gray', linewidth=1.2, linestyle='--', zorder=2)

    ax.set_xlabel(xlabel, fontsize=10)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.set_title(title, fontsize=11)
    ax.legend(title='Borough', fontsize=8, title_fontsize=8)


# %%
if __name__ == '__main__':
    df = pd.read_parquet(PROCESSED_DATA_DIR / '311_acs_economic.parquet')

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle(
        '311 SR Rate vs. Economic Variables by Community Board\n(Mar 2025 – Mar 2026)',
        fontsize=13,
        y=1.01,
    )

    make_scatter(
        axes[0], df,
        x_col='MdHHIncE', y_col='sr_per_1k',
        xlabel='Median Household Income ($)',
        ylabel='SR per 1,000 Residents',
        title='SR Rate vs. Median Household Income',
    )

    make_scatter(
        axes[1], df,
        x_col='poverty_rate', y_col='sr_per_1k',
        xlabel='Poverty Rate',
        ylabel='SR per 1,000 Residents',
        title='SR Rate vs. Poverty Rate',
    )

    plt.tight_layout()

    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    out_path = FIGURES_DIR / 'scatter_sr_income.png'
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    print(f'Saved figure to {out_path}')

# %%
