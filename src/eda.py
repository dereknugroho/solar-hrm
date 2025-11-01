import sys
from pathlib import Path

# --- Safety for running this script directly from src/ ---
if __package__ is None:
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

import matplotlib.pyplot as plt

from src.utils.config import FILEPATHS
from src.utils.utils import fetch_parquet

def _plot_watts_vs_datetime(group_by: str | None, daily_average: bool):
    """Internal function that handles shared plotting logic."""
    pass

def watts_vs_datetime(daily_average: bool = True):
    """Plot solar production in watts versus datetime."""
    return _plot_watts_vs_datetime(group_by=None, daily_average=daily_average)

def watts_vs_datetime_by_community(daily_average: bool = True):
    """Plot solar production in watts versus datetime grouped by community."""
    return _plot_watts_vs_datetime(group_by="community", daily_average=daily_average)

def watts_vs_datetime_by_sortation_area(daily_average: bool = True):
    """Plot solar production in watts versus datetime grouped by sortation area."""
    return _plot_watts_vs_datetime(group_by="sortation_area", daily_average=daily_average)

if __name__ == '__main__':
    solar_df = fetch_parquet(FILEPATHS['parquet_processed'])

    for col in solar_df.columns:
        print(f'Column:\t{col} [{solar_df[col].dtype}]')
        print(f'Number of unique values in {col}: {solar_df[col].nunique()}')
        print(f'Number of NA values in {col}: {solar_df[col].isna().sum()}')
        print('===========================')
