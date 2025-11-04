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

- WATTS over DATETIME
    * GROUP_BY (all, COMMUNITY_NAME, FORWARD_SORTATION_AREA)
    * AGGREGATION (sum, min, avg, median, max)
    - Hourly (24 hours)
    - Daily (one month)
    - Monthly (one year)
    - Yearly (2016-2024)
"""

def watts_over_time_sequential(solar_df: pd.DataFrame, seg: str, agg: str):
    """Segment dataframe solar_df into sequential chunks of time."""
    if agg not in AGG_METHODS:
        raise ValueError(f'Invalid agg {agg} must be a valid aggregation method: {AGG_METHODS}')
    if seg not in TIME_SEG_CODES:
        raise ValueError(f'Invalid seg {seg} must be a valid time segmentation code: {TIME_SEG_CODES}')

    return getattr(solar_df.sort_values('DATETIME').resample(seg, on='DATETIME')['WATTS'], agg)()

def watts_over_time_non_sequential(solar_df, seg: str, agg: str):
    """Segment dataframe solar_df into non-sequential chunks of time."""
    if agg not in AGG_METHODS:
        raise ValueError(f'Invalid agg {agg} must be a valid aggregation method: {AGG_METHODS}')
    if seg not in TIME_SEG:
        raise ValueError(f'Invalid seg {seg} must be a valid time segmentation interval: {TIME_SEG}')

    solar_df = solar_df.copy()
    solar_df[seg] = getattr(solar_df['DATETIME'].dt, seg)

    return getattr(solar_df.groupby(seg, as_index=False)['WATTS'], agg)()

if __name__ == '__main__':
    solar_df = fetch_parquet(FILEPATHS['parquet_processed'])

    # print(solar_df.head())

    # for col in solar_df.columns:
    #     print(f'Column:\t{col} [{solar_df[col].dtype}]')
    #     print(f'Number of unique values in {col}: {solar_df[col].nunique()}')
    #     print(f'Number of NA values in {col}: {solar_df[col].isna().sum()}')
    #     print('===========================')

    # month_seg_seq_sum = watts_over_time_sequential(solar_df, seg = 'M', agg = 'sum')
    # print(f'month_seg_seq_sum: {type(month_seg_seq_sum)}\n{month_seg_seq_sum}')

    # month_seg_seq_mean = watts_over_time_sequential(solar_df, seg = 'M', agg = 'mean')
    # print(f'month_seg_seq_mean: {type(month_seg_seq_mean)}\n{month_seg_seq_mean}')

    # month_seg_non_seq_sum = watts_over_time_non_sequential(solar_df, seg = 'month', agg = 'sum')
    # print(f'month_seg_non_seq_sum: {type(month_seg_non_seq_sum)}\n{month_seg_non_seq_sum}')

    # month_seg_non_seq_mean = watts_over_time_non_sequential(solar_df, seg = 'month', agg = 'mean')
    # print(f'month_seg_non_seq_mean: {type(month_seg_non_seq_mean)}\n{month_seg_non_seq_mean}')

