# %%
import pandas as pd
from scipy import stats

from dmo_311_quality.utils.config_paths import PROCESSED_DATA_DIR, ROOT_DIR

REPORTS_DIR = ROOT_DIR / 'output' / 'reports'


# %%
def compute_correlation() -> pd.DataFrame:
    """Compute Pearson correlations between sr_per_1k and economic variables.

    Loads the enriched economic parquet and computes Pearson r and p-value
    for sr_per_1k vs MdHHIncE, PerCapIncE, and poverty_rate. Saves results
    to output/reports/correlation_sr_income.csv and prints to stdout.

    Returns:
        DataFrame with columns: variable, pearson_r, p_value.
    """
    df = pd.read_parquet(PROCESSED_DATA_DIR / '311_acs_economic.parquet')

    variables = {
        'MdHHIncE': 'Median Household Income',
        'PerCapIncE': 'Per Capita Income',
        'poverty_rate': 'Poverty Rate',
    }

    rows = []
    for col, label in variables.items():
        r, p = stats.pearsonr(df['sr_per_1k'], df[col])
        rows.append({'variable': label, 'pearson_r': round(r, 3), 'p_value': round(p, 4)})

    results = pd.DataFrame(rows)
    print(results.to_string(index=False))

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS_DIR / 'correlation_sr_income.csv'
    results.to_csv(out_path, index=False)
    print(f'\nCorrelation table saved → {out_path}')
    return results


# %%
if __name__ == '__main__':
    df_corr = compute_correlation()

# %%
