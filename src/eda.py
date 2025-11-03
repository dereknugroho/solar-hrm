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

def sum_watts_over_time(solar_df):
    pass
    # # Typical hour, day, month, year
    # solar_df['hour'] = solar_df['DATETIME'].dt.hour
    # solar_df['day'] = solar_df['DATETIME'].dt.day
    # solar_df['month'] = solar_df['DATETIME'].dt.month
    # solar_df['year'] = solar_df['DATETIME'].dt.year
    # sum_typical_hour = solar_df.groupby('hour', as_index=False)['WATTS'].sum()
    # sum_typical_day = solar_df.groupby('day', as_index=False)['WATTS'].sum()
    # sum_typical_month = solar_df.groupby('month', as_index=False)['WATTS'].sum()
    # sum_typical_year = solar_df.groupby('year', as_index=False)['WATTS'].sum()
    # return sum_typical_hour, sum_typical_day, sum_typical_month, sum_typical_year

    # # Sequential hour, day, month, year
    # sum_sequential_hour = solar_df.resample('H', on='DATETIME')['WATTS'].sum()
    # sum_sequential_day = solar_df.resample('D', on='DATETIME')['WATTS'].sum()
    # sum_sequential_week = solar_df.resample('W', on='DATETIME')['WATTS'].sum()
    # sum_sequential_month = solar_df.resample('M', on='DATETIME')['WATTS'].sum()
    # sum_sequential_year = solar_df.resample('Y', on='DATETIME')['WATTS'].sum()
    # return sum_sequential_hour, sum_sequential_day, sum_sequential_month, sum_sequential_year

if __name__ == '__main__':
    solar_df = fetch_parquet(FILEPATHS['parquet_processed'])

    print(solar_df.head())

    for col in solar_df.columns:
        print(f'Column:\t{col} [{solar_df[col].dtype}]')
        print(f'Number of unique values in {col}: {solar_df[col].nunique()}')
        print(f'Number of NA values in {col}: {solar_df[col].isna().sum()}')
        print('===========================')

    sum_watts_over_time(solar_df)