# %%
import pandas as pd
from scipy import stats

from dmo_311_quality.utils.config_paths import PROCESSED_DATA_DIR, ROOT_DIR

REPORTS_DIR = ROOT_DIR / 'output' / 'reports'

_DIMENSION_CONFIG: dict[str, dict] = {
    'economic': {
        'parquet_prefix': '311_acs_economic',
        'variables': {
            'MdHHIncE': 'Median Household Income',
            'PerCapIncE': 'Per Capita Income',
            'poverty_rate': 'Poverty Rate',
        },
        'output_stem': 'correlation_sr_income',
    },
    'language': {
        'parquet_prefix': '311_acs_language',
        'variables': {
            'lep_rate': 'LEP Rate',
        },
        'output_stem': 'correlation_sr_language',
    },
}


# %%
def compute_correlation(complaint_type: str = 'all', dimension: str = 'economic') -> pd.DataFrame:
    """Compute Pearson correlations between sr_per_1k and dimension variables.

    Args:
        complaint_type: One of 'all', 'potholes', 'rodents'.
        dimension: One of 'economic' or 'language'. Controls which parquet is
            loaded, which variables are correlated, and the output filename.

    Returns:
        DataFrame with columns: variable, pearson_r, p_value.
    """
    cfg = _DIMENSION_CONFIG[dimension]
    suffix = '' if complaint_type == 'all' else f'_{complaint_type}'
    df = pd.read_parquet(PROCESSED_DATA_DIR / f'{cfg["parquet_prefix"]}{suffix}.parquet')

    rows = []
    for col, label in cfg['variables'].items():
        r, p = stats.pearsonr(df['sr_per_1k'], df[col])
        rows.append({'variable': label, 'pearson_r': round(r, 3), 'p_value': round(p, 4)})

    results = pd.DataFrame(rows)
    print(f'\n[{dimension} | {complaint_type}]')
    print(results.to_string(index=False))

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS_DIR / f'{cfg["output_stem"]}_{complaint_type}.csv'
    results.to_csv(out_path, index=False)
    print(f'\nCorrelation table saved → {out_path}')
    return results


# %%
if __name__ == '__main__':
    for dim in ['economic', 'language']:
        for ct in ['all', 'potholes', 'rodents']:
            df_corr = compute_correlation(ct, dim)

# %%
