import matplotlib.pyplot as plt
import pandas as pd

from core.utils import pd_config
from core.utils.config import FILEPATHS
from core.utils.paths import from_root
from core.utils.utils import ensure_dataframe
from core.utils.validation import validate_installations_feature_engineered, validate_readings_feature_engineered

@ensure_dataframe
def plot_panels_reporting_over_time(clean_unpartitioned_parquet: pd.DataFrame):
    """
    Plot the daily number of panels reporting over time.

    X = date
    Y = daily sum of total maximum panels reporting across all installations

    Note: must use clean, unpartitioned parquet because partitioned data
          does not contain information about change in panels reporting over time
    """
    # Create a date column
    clean_unpartitioned_parquet['date'] = clean_unpartitioned_parquet['timestamp'].dt.date

    # Obtain the max panels reporting for each combination of installation and date
    daily_max = (
        clean_unpartitioned_parquet
        .groupby(['installation_id', 'date'])['panels_reporting']
        .max()
        .reset_index()
    )

    # Obtain the daily sum of the total maximum panels reporting for each date
    daily_sum = (
        daily_max
        .groupby('date')['panels_reporting']
        .sum()
        .reset_index()
    )

    # Generate scatterplot
    plt.figure(figsize=(10,5))
    plt.scatter(daily_sum['date'], daily_sum['panels_reporting'], s=10)
    plt.title('Daily Sum of Total Maximum Panels Reporting')
    plt.xlabel('Date')
    plt.ylabel('Daily Sum of Total Maximum Panels Reporting')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    clean_unpartitioned_parquet = pd.read_parquet(from_root(FILEPATHS['clean_unpartitioned_parquet']))
    print(f'------------------------------------------------')
    print(f'| clean_unpartitioned_parquet (count: {len(clean_unpartitioned_parquet)}) |')
    print(f'------------------------------------------------')
    for col in clean_unpartitioned_parquet.columns:
        print(f'Column: {col} [{clean_unpartitioned_parquet[col].dtype}] [num_unique: {clean_unpartitioned_parquet[col].nunique()}] [num_NA: {clean_unpartitioned_parquet[col].isna().sum()}]')
    plot_panels_reporting_over_time(clean_unpartitioned_parquet)

    # installations_feature_engineered = pd.read_parquet(from_root(FILEPATHS['installations_feature_engineered']))
    # readings_feature_engineered = pd.read_parquet(from_root(FILEPATHS['readings_feature_engineered']))
    # validate_installations_feature_engineered(installations_feature_engineered)
    # validate_readings_feature_engineered(readings_feature_engineered)
    
    # print(f'-------------------------------------------------')
    # print(f'| installations_feature_engineered (count: {len(installations_feature_engineered)}) |')
    # print(f'-------------------------------------------------')
    # for col in installations_feature_engineered.columns:
    #     print(f'Column: {col} [{installations_feature_engineered[col].dtype}] [num_unique: {installations_feature_engineered[col].nunique()}] [num_NA: {installations_feature_engineered[col].isna().sum()}]')

    # print(f'-------------------------------------------------')
    # print(f'| readings_feature_engineered (count: {len(readings_feature_engineered)}) |')
    # print(f'-------------------------------------------------')
    # for col in readings_feature_engineered.columns:
    #     print(f'Column: {col} [{readings_feature_engineered[col].dtype}] [num_unique: {readings_feature_engineered[col].nunique()}] [num_NA: {readings_feature_engineered[col].isna().sum()}]')
