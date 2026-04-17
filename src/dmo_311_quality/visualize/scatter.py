# source /Users/tarasullivan/Documents/oda/dmo-311-quality/.venv/bin/activate
# %%
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.axes import Axes

from dmo_311_quality.utils.config_paths import (
    PROCESSED_DATA_DIR, ROOT_DIR, SR_WINDOW_START, SR_WINDOW_END,
)

# %%
FIGURES_DIR = ROOT_DIR / 'output' / 'figures'

BOROUGH_COLORS: dict[str, str] = {
    'Bronx': '#e41a1c',
    'Brooklyn': '#377eb8',
    'Manhattan': '#4daf4a',
    'Queens': '#ff7f00',
    'Staten Island': '#984ea3',
}

_COMPLAINT_LABELS: dict[str, str] = {
    'all': 'All SRs',
    'potholes': 'Potholes',
    'rodents': 'Rodents',
}

_DIMENSION_CONFIG: dict[str, dict] = {
    'economic': {
        'parquet_prefix': '311_acs_economic',
        'output_stem': 'scatter_sr_income',
        'panels': [
            {'x_col': 'MdHHIncE',    'xlabel': 'Median Household Income ($)', 'title': 'SR Rate vs. Median Household Income'},
            {'x_col': 'poverty_rate', 'xlabel': 'Poverty Rate',                'title': 'SR Rate vs. Poverty Rate'},
        ],
        'suptitle_prefix': 'SR Rate vs. Economic Variables',
    },
    'language': {
        'parquet_prefix': '311_acs_language',
        'output_stem': 'scatter_sr_language',
        'panels': [
            {'x_col': 'lep_rate', 'xlabel': 'LEP Rate', 'title': 'SR Rate vs. LEP Rate'},
        ],
        'suptitle_prefix': 'SR Rate vs. LEP Rate',
    },
}


def _fmt_yyyymm(yyyymm: int) -> str:
    """Convert YYYYMM integer to 'Mon YYYY' string (e.g. 202503 → 'Mar 2025')."""
    import calendar
    year, month = divmod(yyyymm, 100)
    return f'{calendar.month_abbr[month]} {year}'


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
def plot_scatter(complaint_type: str = 'all', dimension: str = 'economic') -> None:
    """Build and save a scatter figure for the given complaint type and dimension.

    Loads the appropriate enriched parquet, draws one panel per dimension
    variable, and saves to output/figures/.

    Args:
        complaint_type: One of 'all', 'potholes', 'rodents'.
        dimension: One of 'economic' or 'language'.
    """
    cfg = _DIMENSION_CONFIG[dimension]
    suffix = '' if complaint_type == 'all' else f'_{complaint_type}'
    df = pd.read_parquet(PROCESSED_DATA_DIR / f'{cfg["parquet_prefix"]}{suffix}.parquet')

    label = _COMPLAINT_LABELS[complaint_type]
    date_range = f'{_fmt_yyyymm(SR_WINDOW_START)} – {_fmt_yyyymm(SR_WINDOW_END)}'
    n_panels = len(cfg['panels'])

    fig, axes = plt.subplots(1, n_panels, figsize=(7 * n_panels, 6))
    if n_panels == 1:
        axes = [axes]

    fig.suptitle(
        f'311 {cfg["suptitle_prefix"]} by Community Board — {label}\n({date_range})',
        fontsize=13,
        y=1.01,
    )

    for ax, panel in zip(axes, cfg['panels']):
        make_scatter(
            ax, df,
            x_col=panel['x_col'],
            y_col='sr_per_1k',
            xlabel=panel['xlabel'],
            ylabel='SR per 1,000 Residents',
            title=panel['title'],
        )

    plt.tight_layout()

    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    out_path = FIGURES_DIR / f'{cfg["output_stem"]}_{complaint_type}.png'
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f'Saved figure → {out_path}')


# %%
if __name__ == '__main__':
    for dim in ['economic', 'language']:
        for ct in _COMPLAINT_LABELS:
            plot_scatter(ct, dim)

# %%
