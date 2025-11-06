import pandas as pd

from src.utils.utils import ensure_dataframe

@ensure_dataframe
def validate_initial_installations(installations: pd.DataFrame) -> None:
    expected_columns = {
        'installation_id',
        'num_panels',
        'community'
    }
    missing_columns = expected_columns - set(installations.columns)
    extra_columns = set(installations.columns) - expected_columns

    # Check column integrity
    if missing_columns:
        raise KeyError(f'Missing expected columns: {missing_columns}')
    if extra_columns:
        raise ValueError(f'Unexpected extra columns: {extra_columns}')

    # Check installation_id integrity
    if installations['installation_id'].dtype != 'int64':
        raise TypeError(f'installation_id must be int64 dtype')
    if installations['installation_id'].isnull().any():
        raise ValueError(f'installation_id contains null values')

    # Check num_panels integrity
    if installations['num_panels'].dtype != 'int64':
        raise TypeError(f'num_panels must be int64 dtype')
    if installations['num_panels'].isnull().any():
        raise ValueError(f'num_panels contains null values')
    if installations['num_panels'].lt(0).any():
        raise ValueError(f'num_panels must be greater than or equal to 0')

    # Check community integrity
    if installations['community'].dtype != 'category':
        raise TypeError('community must be category dtype')
    if installations['community'].isnull().any():
        raise ValueError('category contains null values')

@ensure_dataframe
def validate_initial_readings(readings: pd.DataFrame) -> None:
    expected_columns = {
        'installation_id',
        'timestamp',
        'power_watts_5min_avg'
    }
    missing_columns = expected_columns - set(readings.columns)
    extra_columns = set(readings.columns) - expected_columns

    # Check column integrity
    if missing_columns:
        raise KeyError(f'Missing expected columns: {missing_columns}')
    if extra_columns:
        raise ValueError(f'Unexpected extra columns: {extra_columns}')

    # Check installation_id integrity
    if readings['installation_id'].dtype != 'int64':
        raise TypeError(f'installation_id must be int64 dtype')
    if readings['installation_id'].isnull().any():
        raise ValueError(f'installation_id contains null values')

    # Check timestamp integrity
    if readings['timestamp'].dtype != 'datetime64[ns]':
        raise TypeError(f'timestamp must be datetime64[ns] dtype')
    if readings['timestamp'].isnull().any():
        raise ValueError(f'timestamp contains null values')

    # Check power_watts_5min_avg integrity
    if readings['power_watts_5min_avg'].dtype != 'float64':
        raise TypeError(f'power_watts_5min_avg must be float64 dtype')
    if readings['power_watts_5min_avg'].isnull().any():
        raise ValueError(f'power_watts_5min_avg contains null values')
    if readings['power_watts_5min_avg'].lt(0).any():
        raise ValueError(f'power_watts_5min_avg must be greater than or equal to 0.0')
