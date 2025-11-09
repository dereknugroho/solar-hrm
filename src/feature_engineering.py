import sys
from pathlib import Path

# --- Safety for running this script directly from src/ ---
if __package__ is None:
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

import os

import pandas as pd

from src.utils import pd_config
from src.utils.config import FILEPATHS, FEATURE_ENGINEERING
from src.utils.paths import from_root
from src.utils.utils import create_clean_directory, ensure_dataframe

@ensure_dataframe
def build_installations_features(installations_preprocessed: pd.DataFrame) -> pd.DataFrame:
    """Build additional features onto the installations table after grouping by installation_id."""
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
    """"""
    if feature_engineered_exists:
        installations_feature_engineered = pd.read_parquet(from_root(FILEPATHS['installations_feature_engineered']))
        readings_feature_engineered = pd.read_parquet(from_root(FILEPATHS['readings_feature_engineered']))

        # To do: validate installations_feature_engineered and readings_feature_engineered

        print(f"\U00002705 Successfully read valid feature-engineered parquets in {FILEPATHS['dir_feature_engineered']}")
    else:
        print(f"Invalid feature-engineered parquets in {FILEPATHS['dir_feature_engineered']}; generating new parquets now...")
        # Clean up target directory for feature-engineered parquets
        create_clean_directory(FILEPATHS['dir_feature_engineered'])

        # Build engineered features on preprocessed installations table and preprocessed readings table
        installations_feature_engineered = build_installations_features(installations_preprocessed)
        readings_feature_engineered = build_readings_features(readings_preprocessed)

        # To do: validate installations_feature_engineered and readings_feature_engineered

        # Save feature-engineered data into parquets
        installations_feature_engineered.to_parquet(
            from_root(FILEPATHS['installations_feature_engineered']),
            index=False,
        )
        readings_feature_engineered.to_parquet(
            from_root(FILEPATHS['readings_feature_engineered']),
            index=False,
        )
        print(f"\U00002705 Feature-engineered parquets generated and saved in {FILEPATHS['dir_feature_engineered']}")

    return installations_feature_engineered, readings_feature_engineered

def check_feature_engineered_parquets_exist() -> bool:
    """Return True if all required preprocessed parquet files exist."""
    filepath_keys = ['installations_feature_engineered', 'readings_feature_engineered']
    return all(os.path.exists(from_root(FILEPATHS[k])) for k in filepath_keys)

if __name__ == '__main__':
    installations_preprocessed = pd.read_parquet(from_root(FILEPATHS['installations_preprocessed']))
    readings_preprocessed = pd.read_parquet(from_root(FILEPATHS['readings_preprocessed']))

    installations_feature_engineered, readings_feature_engineered = build_feature_dataset(
        feature_engineered_exists=check_feature_engineered_parquets_exist(),
        installations_preprocessed=installations_preprocessed,
        readings_preprocessed=readings_preprocessed,
    )

    print(f'***************\ninstallations_feature_engineered summary (count: {len(installations_feature_engineered)})\n***************')
    for col in installations_feature_engineered.columns:
        print(f'Column:\t{col} [{installations_feature_engineered[col].dtype}]')
        print(f'Number of unique values in {col}: {installations_feature_engineered[col].nunique()}')
        print(f'Number of NA values in {col}: {installations_feature_engineered[col].isna().sum()}')
        print('===========================')

    print(f'***************\nreadings_feature_engineered summary (count: {len(readings_feature_engineered)})\n***************')
    for col in readings_feature_engineered.columns:
        print(f'Column:\t{col} [{readings_feature_engineered[col].dtype}]')
        print(f'Number of unique values in {col}: {readings_feature_engineered[col].nunique()}')
        print(f'Number of NA values in {col}: {readings_feature_engineered[col].isna().sum()}')
        print('===========================')
