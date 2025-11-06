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

@ensure_dataframe
def drop_unused_columns(solar_df: pd.DataFrame) -> pd.DataFrame:
    """Drop unused columns specified in config.json."""
    return solar_df.drop(columns=PREPROCESSING['drop_columns'])

@ensure_dataframe
def rename_columns(solar_df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns specified in config.json."""
    return solar_df.rename(columns=PREPROCESSING['rename_columns'])

@ensure_dataframe
def drop_null_PK_combinations(solar_df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows containing a null value in installation_id or timestamp."""
    return solar_df.dropna(subset=['installation_id', 'timestamp'])

@ensure_dataframe
def standardize_column_text(solar_df: pd.DataFrame) -> pd.DataFrame:
    """Perform ."""
    solar_df['community'] = solar_df['community'].replace(PREPROCESSING['community_name_corrections'])
    return solar_df

@ensure_dataframe
def convert_object_dtypes(solar_df: pd.DataFrame) -> pd.DataFrame:
    """Convert dtype object columns to proper dtype."""
    solar_df['timestamp'] = pd.to_datetime(
        solar_df['timestamp'],
        format=PREPROCESSING['date_format'],
        errors='coerce',
    )

    solar_df['power_watts_5min_avg'] = solar_df['power_watts_5min_avg'].astype('float')
    solar_df['community'] = solar_df['community'].astype('category')

    return solar_df

@ensure_dataframe
def partition_solar_data(solar_df: pd.DataFrame):
    """
    Decompose dataframe solar_df into two dataframes with a one-to-many relationship:

    panel_installation -< meter_reading.
    """
    # # Construct dataframe representing solar panel installations
    # installations = (
    #     solar_df[[
    #         'installation_id',
    #         'num_panels',
    #         'community',
    #     ]]
    #     .drop_duplicates(subset=['installation_id'])
    #     .reset_index(drop=True)
    # )

    # Construct dataframe representing solar panel installations
    installations = (
        solar_df.loc[
            solar_df.groupby('installation_id')['num_panels'].transform('max') == solar_df['num_panels']
        ]
        .drop_duplicates(subset=['installation_id'])
        .reset_index(drop=True)
    )

    # Construct dataframe representing meter readings
    readings = (
        solar_df[[
            'installation_id',
            'timestamp',
            'power_watts_5min_avg',
            'energy_watt_hour'
        ]]
        .copy()
    )

    return installations, readings

@ensure_dataframe
def validate_schema(installations: pd.DataFrame, readings: pd.DataFrame) -> None:
    """Ensure newly-partitioned dataframes are well-formed."""

    # Ensure referential integrity
    invalid_installations = (
        readings
        .loc[~readings['installation_id']
        .isin(installations['installation_id'])]
    )
    if not invalid_installations.empty:
        raise ValueError(f"Invalid installation_id found in readings:\n{invalid_installations['installation_id'].unique()}")

    # Ensure one-to-many structure
    duplicated_panel_installations = (
        installations[
            installations.duplicated(subset=['installation_id'], keep=False)
        ]
    )
    if not duplicated_panel_installations.empty:
        raise ValueError(f'Duplicate installation_id entries in installations:\n{duplicated_panel_installations}')

    # Ensure unique timestamps per panel installation
    duplicated_readings = (
        readings[
            readings.duplicated(subset=['installation_id', 'timestamp'], keep=False)
        ]
    )
    if not duplicated_readings.empty:
        raise ValueError(f'Duplicate readings found:\n{duplicated_readings}')

    # Ensure a valid number of panels
    if not installations['num_panels'].ge(1).all():
        raise ValueError('num_panels must be an integer greater than or equal to 1')

    # # Ensure a valid value for power_watts_5min_avg
    # if not readings['power_watts_5min_avg'].ge(0).all():
    #     raise ValueError('power_watts_5min_avg must be greater than or equal to 0.0')

    # # Ensure a valid value for energy_watt_hour
    # if not readings['energy_watt_hour'].ge(0).all():
    #     raise ValueError('energy_watt_hour must be greater than or equal to 0.0')

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
    if not preprocessed_exists:
        # Load csv into dataframe
        solar_df = pd.read_csv(
            from_root(FILEPATHS['input_csv']),
        )

        # Save unprocessed data into parquet
        solar_df.to_parquet(
            from_root(FILEPATHS['parquet_raw']),
            index=False,
        )

        # Run preprocessing pipeline with parameters loaded from config.json
        solar_df = drop_unused_columns(solar_df)
        solar_df = rename_columns(solar_df)
        solar_df = drop_null_PK_combinations(solar_df)
        solar_df = standardize_column_text(solar_df)
        solar_df = convert_object_dtypes(solar_df)

        # Partition solar_df in order to enforce entity integrity
        installations, readings = partition_solar_data(solar_df)
        validate_schema(installations, readings)

        # Save partitioned data into parquets
        installations.to_parquet(
            from_root(FILEPATHS['parquet_panel_installation']),
            index=False,
        )
        readings.to_parquet(
            from_root(FILEPATHS['parquet_meter_reading']),
            index=False,
        )
    else:
        # Load panel_installation parquet into dataframe
        installations = pd.read_parquet(from_root(FILEPATHS['parquet_panel_installation']))
        readings = pd.read_parquet(from_root(FILEPATHS['parquet_meter_reading']))

    return installations, readings

def partitioned_parquets_exist() -> bool:
    """Return True if all required parquet files exist."""
    required_keys = ['parquet_panel_installation', 'parquet_meter_reading']
    return all(os.path.exists(from_root(FILEPATHS[k])) for k in required_keys)

if __name__ == '__main__':
    installations, readings = preprocess(
        preprocessed_exists=partitioned_parquets_exist()
    )

    print(f'installations *****')
    for col in installations.columns:
        print(f'Column:\t{col} [{installations[col].dtype}]')
        print(f'Number of unique values in {col}: {installations[col].nunique()}')
        print(f'Number of NA values in {col}: {installations[col].isna().sum()}')
        print('===========================')

    print(f'readings *****')
    for col in readings.columns:
        print(f'Column:\t{col} [{readings[col].dtype}]')
        print(f'Number of unique values in {col}: {readings[col].nunique()}')
        print(f'Number of NA values in {col}: {readings[col].isna().sum()}')
        print('===========================')