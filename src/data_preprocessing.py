import sys
from pathlib import Path

# --- Safety for running this script directly from src/ ---
if __package__ is None:
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

import os
import pandas as pd

from src.utils.config import FILEPATHS, PREPROCESSING
from src.utils.paths import from_root
from src.utils.utils import ensure_dataframe
from src.utils.validation import validate_initial_installations, validate_initial_readings

# Show all rows and columns
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

@ensure_dataframe
def drop_unused_columns(solar_data: pd.DataFrame) -> pd.DataFrame:
    """Drop unused columns specified in config.json."""
    return solar_data.drop(columns=PREPROCESSING['drop_columns'])

@ensure_dataframe
def rename_columns(solar_data: pd.DataFrame) -> pd.DataFrame:
    """Rename columns specified in config.json."""
    return solar_data.rename(columns=PREPROCESSING['rename_columns'])

@ensure_dataframe
def drop_null_PK_combinations(solar_data: pd.DataFrame) -> pd.DataFrame:
    """Drop rows containing a null value in installation_id or timestamp."""
    return solar_data.dropna(subset=['installation_id', 'timestamp'])

@ensure_dataframe
def standardize_column_text(solar_data: pd.DataFrame) -> pd.DataFrame:
    """Standardize column text strings."""
    solar_data['community'] = solar_data['community'].replace(PREPROCESSING['community_name_corrections'])
    return solar_data

@ensure_dataframe
def modify_column_dtypes(solar_data: pd.DataFrame) -> pd.DataFrame:
    """Convert dtype object columns to proper dtype."""
    solar_data['timestamp'] = pd.to_datetime(
        solar_data['timestamp'],
        format=PREPROCESSING['date_format'],
        errors='coerce',
    )

    solar_data['power_watts_5min_avg'] = solar_data['power_watts_5min_avg'].astype('float')
    solar_data['community'] = solar_data['community'].astype('category')

    return solar_data

@ensure_dataframe
def partition_solar_data(solar_data: pd.DataFrame):
    """Decompose dataframe solar_data into two dataframes with a one-to-many relationship."""
    # Construct dataframe representing solar panel installations
    installations = (
        solar_data[[
            'installation_id',
            'num_panels',
            'community',
        ]]
        .copy()
    )

    # Construct dataframe representing meter readings
    readings = (
        solar_data[[
            'installation_id',
            'timestamp',
            'power_watts_5min_avg',
        ]]
        .copy()
    )

    return installations, readings

def preprocess(preprocessed_exists: bool = False) -> pd.DataFrame:
    """
    Perform the following preprocessing tasks:
    - Save a parquet of the unprocessed data
    - Drop unused columns
    - Rename columns
    - Drop rows with null PK values
    - Standardize column text values
    - Convert column types
    - Partition the data into two separate dataframes
    """
    if preprocessed_exists:
        print(f'All parquets detected in data/01_preprocessed, reading parquets now...')

        # Load parquets into dataframes
        installations = pd.read_parquet(from_root(FILEPATHS['installations_v1']))
        readings = pd.read_parquet(from_root(FILEPATHS['readings_v1']))

        # Validate partitioned dataframes
        validate_initial_installations(installations)
        validate_initial_readings(readings)
    else:
        print(f'No parquets detected in data/01_preprocessed, generating parquets now...')
        # Load csv into dataframe
        solar_data = pd.read_csv(
            from_root(FILEPATHS['raw_csv']),
        )

        # Save unprocessed data into parquet
        solar_data.to_parquet(
            from_root(FILEPATHS['raw_parquet']),
            index=False,
        )

        # Run preprocessing pipeline with parameters loaded from config.json
        solar_data = drop_unused_columns(solar_data)
        solar_data = rename_columns(solar_data)
        solar_data = drop_null_PK_combinations(solar_data)
        solar_data = standardize_column_text(solar_data)
        solar_data = modify_column_dtypes(solar_data)

        # Partition solar_data into two separate dataframes
        installations, readings = partition_solar_data(solar_data)

        # Validate partitioned dataframes
        validate_initial_installations(installations)
        validate_initial_readings(readings)

        # Save partitioned data into parquets
        installations.to_parquet(
            from_root(FILEPATHS['installations_v1']),
            index=False,
        )
        readings.to_parquet(
            from_root(FILEPATHS['readings_v1']),
            index=False,
        )

    return installations, readings

def preprocessed_parquets_exist() -> bool:
    """Return True if all required parquet files exist."""
    required_keys = ['raw_parquet', 'installations_v1', 'readings_v1']
    return all(os.path.exists(from_root(FILEPATHS[k])) for k in required_keys)

if __name__ == '__main__':
    installations, readings = preprocess(
        preprocessed_exists=preprocessed_parquets_exist()
    )

    print(f'installations (count: {len(installations)}) *****')
    for col in installations.columns:
        print(f'Column:\t{col} [{installations[col].dtype}]')
        print(f'Number of unique values in {col}: {installations[col].nunique()}')
        print(f'Number of NA values in {col}: {installations[col].isna().sum()}')
        print('===========================')

    print(f'readings (count: {len(readings)}) *****')
    for col in readings.columns:
        print(f'Column:\t{col} [{readings[col].dtype}]')
        print(f'Number of unique values in {col}: {readings[col].nunique()}')
        print(f'Number of NA values in {col}: {readings[col].isna().sum()}')
        print('===========================')
