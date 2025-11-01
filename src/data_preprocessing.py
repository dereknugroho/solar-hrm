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
    """Drop unused columns WATT_HOUR and KILOWATT_HOUR."""
    return solar_df.drop(columns=PREPROCESSING['drop_columns'])

@ensure_dataframe
def rename_DATE_column(solar_df: pd.DataFrame) -> pd.DataFrame:
    """Rename DATE column to DATETIME."""
    return solar_df.rename(columns=PREPROCESSING['rename_columns'])

@ensure_dataframe
def clean_community_names(solar_df: pd.DataFrame) -> pd.DataFrame:
    """Apply corrections to COMMUNITY_NAME column."""
    solar_df['COMMUNITY_NAME'] = solar_df['COMMUNITY_NAME'].replace(PREPROCESSING['community_name_corrections'])
    return solar_df

@ensure_dataframe
def convert_object_dtypes(solar_df: pd.DataFrame) -> pd.DataFrame:
    """Convert dtype object columns to proper dtype."""
    solar_df['DATETIME'] = pd.to_datetime(
        solar_df['DATETIME'],
        format=PREPROCESSING['date_format'],
        errors='coerce',
    )

    solar_df['COMMUNITY_NAME'] = solar_df['COMMUNITY_NAME'].astype('category')
    solar_df['FORWARD_SORTATION_AREA'] = solar_df['FORWARD_SORTATION_AREA'].astype('category')

    return solar_df

def preprocess(use_preprocessed) -> pd.DataFrame:
    """
    Perform the following preprocessing tasks:
    - Save a parquet of the raw data
    - Drop unused columns
    - Rename columns
    - Clean values
    - Convert column types
    - Save a parquet of the processed data
    """
    if not use_preprocessed:
        # Load csv into dataframe
        solar_df = pd.read_csv(
            from_root(FILEPATHS['input_csv']),
        )

        # Save unprocessed data into parquet
        solar_df.to_parquet(
            from_root(FILEPATHS['parquet_raw']),
            index=False,
        )

        # Drop unused columns
        solar_df = drop_unused_columns(solar_df)

        # Rename DATE column to DATETIME
        solar_df = rename_DATE_column(solar_df)

        # Clean values in COMMUNITY_NAME column
        solar_df = clean_community_names(solar_df)

        # Convert object dtype columns to proper dtype
        solar_df = convert_object_dtypes(solar_df)

        # Save preprocessed data into parquet
        solar_df.to_parquet(
            from_root(FILEPATHS['parquet_processed']),
            index=False,
        )

    # Load parquet into dataframe
    solar_df = pd.read_parquet(
        from_root(FILEPATHS['parquet_processed']),
    )

    return solar_df

if __name__ == '__main__':
    solar_df = preprocess(
        use_preprocessed=os.path.exists(
            from_root(FILEPATHS['parquet_processed'])
        )
    )

    for col in solar_df.columns:
        print(f'Column:\t{col} [{solar_df[col].dtype}]')
        print(f'Number of unique values in {col}: {solar_df[col].nunique()}')
        print(f'Number of NA values in {col}: {solar_df[col].isna().sum()}')
        print('===========================')
