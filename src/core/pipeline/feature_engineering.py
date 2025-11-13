import os

import pandas as pd

from core.utils import pd_config
from core.utils.config import FILEPATHS, FEATURE_ENGINEERING
from core.utils.logger import info
from core.utils.paths import from_root
from core.utils.utils import create_clean_directory, ensure_dataframe
from core.utils.validation import validate_installations_feature_engineered, validate_readings_feature_engineered

@ensure_dataframe
def build_installations_features(installations_preprocessed: pd.DataFrame) -> pd.DataFrame:
    """Build additional features onto the installations table after grouping by installation_id."""
    cat_dtype = installations_preprocessed['community'].dtype

    installations_feature_engineered = (
        installations_preprocessed
        .groupby('installation_id', as_index=False)
        .agg(
            community=('community', lambda x: x.mode()[0] if not x.mode().empty else None),
            panels_reporting_max=('panels_reporting', 'max'),
            panels_reporting_efficiency=('panels_reporting',
                lambda x: (x == x.max()).sum() / len(x)
            ),
            panels_reporting_avg=('panels_reporting', 'mean'),
        )
    )

    installations_feature_engineered['community'] = installations_feature_engineered['community'].astype(cat_dtype)

    return installations_feature_engineered

@ensure_dataframe
def build_readings_features(readings_preprocessed: pd.DataFrame) -> pd.DataFrame:
    """Build additional features onto the readings table."""
    readings_feature_engineered = readings_preprocessed.copy()

    # Compute watt-hours per 5-minute interval
    readings_feature_engineered['energy_prod_wh'] = readings_preprocessed['power_watts_5min_avg'] * 300 / 3600

    return readings_feature_engineered

def build_feature_dataset(
    installations_preprocessed: pd.DataFrame,
    readings_preprocessed: pd.DataFrame,
    feature_engineered_exists: bool = False
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build features into installations and readings tables."""
    if feature_engineered_exists:
        installations_feature_engineered = pd.read_parquet(from_root(FILEPATHS['installations_feature_engineered']))
        readings_feature_engineered = pd.read_parquet(from_root(FILEPATHS['readings_feature_engineered']))

        # Validate feature-engineered data
        validate_installations_feature_engineered(installations_feature_engineered)
        validate_readings_feature_engineered(readings_feature_engineered)

        info(f"\U00002705 Successfully read and validated feature-engineered parquets in directory {FILEPATHS['dir_feature_engineered']}")
    else:
        info(f"Missing or invalid feature-engineered parquets in directory {FILEPATHS['dir_feature_engineered']} \U00002014 generating parquets now...")
        # Clean up target directory for feature-engineered parquets
        create_clean_directory(FILEPATHS['dir_feature_engineered'])

        # Build engineered features on preprocessed installations table and preprocessed readings table
        installations_feature_engineered = build_installations_features(installations_preprocessed)
        readings_feature_engineered = build_readings_features(readings_preprocessed)

        # Validate feature-engineered data
        validate_installations_feature_engineered(installations_feature_engineered)
        validate_readings_feature_engineered(readings_feature_engineered)

        # Save feature-engineered data into parquets
        installations_feature_engineered.to_parquet(
            from_root(FILEPATHS['installations_feature_engineered']),
            index=False,
        )
        readings_feature_engineered.to_parquet(
            from_root(FILEPATHS['readings_feature_engineered']),
            index=False,
        )
        info(f"\U00002705 Successfully generated, saved, read, and validated feature-engineered parquets in directory {FILEPATHS['dir_feature_engineered']}")

    return installations_feature_engineered, readings_feature_engineered

def check_feature_engineered_parquets_exist() -> bool:
    """Return True if all required preprocessed parquet files exist."""
    filepath_keys = ['installations_feature_engineered', 'readings_feature_engineered']
    return all(os.path.exists(from_root(FILEPATHS[k])) for k in filepath_keys)

if __name__ == '__main__':
    installations_preprocessed = pd.read_parquet(from_root(FILEPATHS['installations_preprocessed']))
    readings_preprocessed = pd.read_parquet(from_root(FILEPATHS['readings_preprocessed']))

    installations_feature_engineered, readings_feature_engineered = build_feature_dataset(
        installations_preprocessed=installations_preprocessed,
        readings_preprocessed=readings_preprocessed,
        feature_engineered_exists=check_feature_engineered_parquets_exist(),
    )

    print(f'-------------------------------------------------')
    print(f'| installations_feature_engineered (count: {len(installations_feature_engineered)}) |')
    print(f'-------------------------------------------------')
    for col in installations_feature_engineered.columns:
        print(f'Column: {col} [{installations_feature_engineered[col].dtype}] [num_unique: {installations_feature_engineered[col].nunique()}] [num_NA: {installations_feature_engineered[col].isna().sum()}]')

    print(f'-------------------------------------------------')
    print(f'| readings_feature_engineered (count: {len(readings_feature_engineered)}) |')
    print(f'-------------------------------------------------')
    for col in readings_feature_engineered.columns:
        print(f'Column: {col} [{readings_feature_engineered[col].dtype}] [num_unique: {readings_feature_engineered[col].nunique()}] [num_NA: {readings_feature_engineered[col].isna().sum()}]')
