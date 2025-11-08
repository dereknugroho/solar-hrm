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
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

@ensure_dataframe
def aggregate_installations(installations: pd.DataFrame) -> pd.DataFrame:
    """Generate statistics of panels_reporting for each installation."""
    installations_agg = (
        installations
        .groupby('installation_id', as_index=False)
        .agg(
            community=('community', lambda x: x.mode()[0] if not x.mode().empty else None),
            avg_panels_reporting=('panels_reporting', 'mean'),
            max_panels_reporting=('panels_reporting', 'max'),
            freq_max_panels_reporting=('panels_reporting',
                lambda x: (x == x.max()).sum() / len(x)
            ),
        )
    )

    return installations_agg

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

    installations = aggregate_installations(installations)

    print(f'installations (count: {len(installations)}):\n{installations}')
