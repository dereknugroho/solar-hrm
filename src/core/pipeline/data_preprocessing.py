import os
import pandas as pd

from core.utils import pd_config
from core.utils.config import FILEPATHS, PREPROCESSING
from core.utils.paths import from_root
from core.utils.utils import create_clean_directory, ensure_dataframe
from core.utils.validation import validate_installations_preprocessed, validate_readings_preprocessed

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
def partition_solar_data(solar_data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Decompose dataframe solar_data into two dataframes with a one-to-many relationship."""
    # Construct dataframe representing solar panel installations
    installations = (
        solar_data[[
            'installation_id',
            'panels_reporting',
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

def preprocess(preprocessed_exists: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    If preprocessing has not occurred:
        - Save a parquet of the unprocessed data
        - Drop unused columns
        - Rename columns
        - Drop rows with invalid primary key combinations
        - Standardize values in str columns
        - Modify column data types
        - Partition data
        - Perform preliminary validation on partitioned data
        - Save parquets of partitioned data
    Otherwise: read the preprocessed data
    """
    if preprocessed_exists:
        # Load parquets into dataframes
        installations_preprocessed = pd.read_parquet(from_root(FILEPATHS['installations_preprocessed']))
        readings_preprocessed = pd.read_parquet(from_root(FILEPATHS['readings_preprocessed']))

        # Validate partitioned preprocessed data
        validate_installations_preprocessed(installations_preprocessed)
        validate_readings_preprocessed(readings_preprocessed)

        print(f"\U00002705 Successfully read valid preprocessed parquets in {FILEPATHS['dir_preprocessing']}")
    else:
        print(f"Invalid preprocessed parquets in {FILEPATHS['dir_preprocessing']}; generating new parquets now...")
        # Clean up target directory for parquets
        create_clean_directory(FILEPATHS['dir_preprocessing'])

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
        installations_preprocessed, readings_preprocessed = partition_solar_data(solar_data)

        # Validate partitioned preprocessed data
        validate_installations_preprocessed(installations_preprocessed)
        validate_readings_preprocessed(readings_preprocessed)

        # Save partitioned data into parquets
        installations_preprocessed.to_parquet(
            from_root(FILEPATHS['installations_preprocessed']),
            index=False,
        )
        readings_preprocessed.to_parquet(
            from_root(FILEPATHS['readings_preprocessed']),
            index=False,
        )
        print(f"\U00002705 Valid preprocessed parquets generated and saved in {FILEPATHS['dir_preprocessing']}")

    return installations_preprocessed, readings_preprocessed

def check_preprocessed_parquets_exist() -> bool:
    """Return True if all required preprocessed parquet files exist."""
    filepath_keys = ['raw_parquet', 'installations_preprocessed', 'readings_preprocessed']
    return all(os.path.exists(from_root(FILEPATHS[k])) for k in filepath_keys)

if __name__ == '__main__':
    installations_preprocessed, readings_preprocessed = preprocess(
        preprocessed_exists=check_preprocessed_parquets_exist(),
    )

    print(f'***************\ninstallations_preprocessed summary (count: {len(installations_preprocessed)})\n***************')
    for col in installations_preprocessed.columns:
        print(f'Column:\t{col} [{installations_preprocessed[col].dtype}]')
        print(f'Number of unique values in {col}: {installations_preprocessed[col].nunique()}')
        print(f'Number of NA values in {col}: {installations_preprocessed[col].isna().sum()}')
        print('===========================')

    print(f'***************\nreadings_preprocessed summary (count: {len(readings_preprocessed)})\n***************')
    for col in readings_preprocessed.columns:
        print(f'Column:\t{col} [{readings_preprocessed[col].dtype}]')
        print(f'Number of unique values in {col}: {readings_preprocessed[col].nunique()}')
        print(f'Number of NA values in {col}: {readings_preprocessed[col].isna().sum()}')
        print('===========================')
