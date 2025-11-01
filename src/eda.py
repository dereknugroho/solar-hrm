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

"""
- WATTS over DATETIME
    * GROUP_BY (all, COMMUNITY_NAME, FORWARD_SORTATION_AREA)
    * AGGREGATION (sum, min, avg, median, max)
    - Hourly (24 hours)
    - Daily (one month)
    - Monthly (one year)
    - Yearly (2016-2024)
"""

if __name__ == '__main__':
    solar_df = fetch_parquet(FILEPATHS['parquet_processed'])

    for col in solar_df.columns:
        print(f'Column:\t{col} [{solar_df[col].dtype}]')
        print(f'Number of unique values in {col}: {solar_df[col].nunique()}')
        print(f'Number of NA values in {col}: {solar_df[col].isna().sum()}')
        print('===========================')
