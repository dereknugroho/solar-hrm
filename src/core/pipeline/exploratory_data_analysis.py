from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from core.utils import pd_config
from core.utils.config import FILEPATHS
from core.utils.logger import info
from core.utils.paths import from_root
from core.utils.utils import ensure_dataframe
from core.utils.validation import validate_installations_feature_engineered, validate_readings_feature_engineered

def plot_panels_reporting_over_time(cleaned_unpartitioned: pd.DataFrame, save: bool = False) -> pd.DataFrame:
    """Plot number of panels reporting over time."""
    df = cleaned_unpartitioned.copy()

    # Create a date column
    df['date'] = cleaned_unpartitioned['timestamp'].dt.date

    # Obtain the maximum number of panels reporting for each installation on each date
    daily_max = (
        df
        .groupby(['installation_id', 'date'])['panels_reporting']
        .max()
        .reset_index()
    )

    # Obtain the maximum total number of panels reporting for all installations on each date
    daily_sum = (
        daily_max
        .groupby('date')['panels_reporting']
        .sum()
        .reset_index()
    )

    # Generate scatterplot
    plt.figure(figsize=(10,5))
    plt.scatter(daily_sum['date'], daily_sum['panels_reporting'], s=10)
    plt.title('Number of active panels over time')
    plt.xlabel('Year')
    plt.ylabel('Number of active panels')
    plt.grid(True)
    plt.tight_layout()

    if save:
        plt.savefig(from_root(FILEPATHS['panels_reporting']))

    info(f"\U00002705 Successfully generated and saved plot [Number of active panels over time] to {from_root(FILEPATHS['panels_reporting'])}")

    return daily_sum

def plot_total_energy_production_over_time(readings_feature_engineered: pd.DataFrame, save: bool = False):
    """Plot total energy production (in megawatt-hours) over time."""
    df = readings_feature_engineered.copy()

    # Create a date column
    df['date'] = readings_feature_engineered['timestamp'].dt.date

    # Obtain the total amount of energy produced on each date
    daily_sum = (
        df
        .groupby(['date'])['energy_prod_wh_5min']
        .sum()
        .reset_index()
    )

    daily_sum['energy_prod_mwh'] = daily_sum['energy_prod_wh_5min'] / 1_000_000

    # Generate scatterplot
    plt.figure(figsize=(10,5))
    plt.scatter(daily_sum['date'], daily_sum['energy_prod_mwh'], s=10)
    plt.title('Daily energy production over time')
    plt.xlabel('Year')
    plt.ylabel('Daily energy production\n(megawatt-hours)')
    plt.grid(True)
    plt.tight_layout()

    if save:
        plt.savefig(from_root(FILEPATHS['energy_production']))

    info(f"\U00002705 Successfully generated and saved plot [Daily energy production over time] to {from_root(FILEPATHS['energy_production'])}")

    return daily_sum

def create_figures_directory():
    """Create empty figures directory."""
    Path(FILEPATHS['dir_figures']).mkdir(parents=True, exist_ok=True)
    info(f"\U00002705 Successfully created empty directory {from_root(FILEPATHS['dir_figures'])}")

if __name__ == '__main__':
    cleaned_unpartitioned = pd.read_parquet(from_root(FILEPATHS['cleaned_unpartitioned']))
    installations_feature_engineered = pd.read_parquet(from_root(FILEPATHS['installations_feature_engineered']))
    readings_feature_engineered = pd.read_parquet(from_root(FILEPATHS['readings_feature_engineered']))

    create_figures_directory()

    print(f'------------------------------------------------')
    print(f'| cleaned_unpartitioned (count: {len(cleaned_unpartitioned)}) |')
    print(f'------------------------------------------------')
    for col in cleaned_unpartitioned.columns:
        print(f'Column: {col} [{cleaned_unpartitioned[col].dtype}] [num_unique: {cleaned_unpartitioned[col].nunique()}] [num_NA: {cleaned_unpartitioned[col].isna().sum()}]')
    
    print(f'-------------------------------------------------')
    print(f'| installations_feature_engineered (count: {len(installations_feature_engineered)}) |')
    print(f'-------------------------------------------------')
    for col in installations_feature_engineered.columns:
        print(f'Column: {col} [{installations_feature_engineered[col].dtype}] [num_unique: {installations_feature_engineered[col].nunique()}] [num_NA: {installations_feature_engineered[col].isna().sum()}]')

    print(f'-------------------------------------------------')
    print(f'| readings_feature_engineered (count: {len(readings_feature_engineered)}) |')
    print(f'-------------------------------------------------')
    for col in readings_feature_engineered.columns:
        print(f'Column: {col} [{readings_feature_engineered[col].dtype}] [num_unique: {readings_feature_engineered[col].nunique()}] [num_NA: {readings_feature_engineered[col].isna().sum()}]')

    plot_panels_reporting_over_time(cleaned_unpartitioned, save=True)
    plot_total_energy_production_over_time(readings_feature_engineered, save=True)
