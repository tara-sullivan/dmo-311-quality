# %%
from dmo_311_quality.analyze.correlations import compute_correlation



# %%
def main() -> None:
    """Run all analysis steps in order and save outputs to output/reports/."""
    print('--- Analyze: SR rate vs. economic variables (Pearson correlation) ---')
    for ct in ['all', 'potholes', 'rodents']:
        compute_correlation(ct)

    print('\nAnalysis complete.')


# %%
if __name__ == '__main__':
    main()

# %%
