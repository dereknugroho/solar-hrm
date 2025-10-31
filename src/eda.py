import sys
from pathlib import Path

# --- Safety for running this script directly from src/ ---
if __package__ is None:
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

import os
import pandas as pd

from src.utils.config import PREPROCESSING
from src.utils.paths import from_root

def fetch_parquet(filepath: str) -> pd.DataFrame:
    """Load processed parquet into master dataframe."""
    return pd.read_parquet(from_root(filepath))

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
    pass
