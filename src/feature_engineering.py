import sys
from pathlib import Path

# --- Safety for running this script directly from src/ ---
if __package__ is None:
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

import pandas as pd

from src.utils.config import FILEPATHS, FEATURE_ENGINEERING
from src.utils.paths import from_root
from src.utils.utils import ensure_dataframe

# Show all rows and columns
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

@ensure_dataframe

if __name__ == '__main__':
    installations = pd.read_parquet(from_root(FILEPATHS['installations_v1']))
    readings = pd.read_parquet(from_root(FILEPATHS['readings_v1']))

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

    installations = (
        installations
        .groupby('installation_id', as_index=False)
        .agg({
            'panels_reporting': ['mean', 'max'],
            'community': lambda x: x.mode()[0] if not x.mode().empty else None,
        })
    )

    installations.columns = [
        'installation_id',
        'panels_reporting_avg',
        'panels_reporting_max',
        'community',
    ]

    installations = installations[[
        'installation_id',
        'community',
        'panels_reporting_avg',
        'panels_reporting_max',
    ]]

    print(f'installations (count: {len(installations)}):\n{installations}')
