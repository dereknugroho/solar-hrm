import sys
from pathlib import Path

# --- Safety for running this script directly from src/ ---
if __package__ is None:
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

import matplotlib.pyplot as plt
import pandas as pd

from src.utils.config import AGG_METHODS, FILEPATHS, TIME_SEG, TIME_SEG_CODES
from src.utils.utils import fetch_parquet

# Show all rows
pd.set_option('display.max_rows', None)

# Show all columns
pd.set_option('display.max_columns', None)

"""
- Number of installed solar panels over time
    * Approach #1: Sum of solar panels for each SYSTEM_ID each day/year
    * Approach #2: Average number of panels reporting per day/year

- WATTS over DATETIME
    * GROUP_BY (all, COMMUNITY_NAME, FORWARD_SORTATION_AREA)
        [Need to implement: grouping by COMMUNITY_NAME and FORWARD_SORTATION_AREA]
    * AGGREGATION (sum, min, avg, median, max)
    - Hourly (24 hours)
    - Daily (one month)
    - Monthly (one year)
    - Yearly (2016-2024)
"""

if __name__ == '__main__':
    pass
