"""
Running this module will perform all of the following tasks:

- Save a parquet of the raw data
- Drop unused columns
- Clean values
- Convert column types
- Save a parquet of the processed data
"""

import os

import pandas as pd

from utils.config_loader import config
from utils.preprocessing_utils import ensure_dataframe

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

cfg = config['preprocessing']

@ensure_dataframe
def drop_unused_columns(solar_df: pd.DataFrame) -> pd.DataFrame:
    """Drop unused columns WATT_HOUR and KILOWATT_HOUR."""
    return solar_df.drop(columns=cfg['drop_columns'])

@ensure_dataframe
def clean_community_names(solar_df: pd.DataFrame) -> pd.DataFrame:
    """Apply corrections to COMMUNITY_NAME column."""
    solar_df['COMMUNITY_NAME'] = solar_df['COMMUNITY_NAME'].replace(cfg['community_name_corrections'])
    return solar_df

@ensure_dataframe
def convert_object_dtypes(solar_df: pd.DataFrame) -> pd.DataFrame:
    """Convert dtype object columns to proper dtype."""
    solar_df['DATE'] = pd.to_datetime(
        solar_df['DATE'],
        format=cfg['date_format'],
        errors='coerce',
    )

    solar_df['COMMUNITY_NAME'] = solar_df['COMMUNITY_NAME'].astype('category')
    solar_df['FORWARD_SORTATION_AREA'] = solar_df['FORWARD_SORTATION_AREA'].astype('category')

    return solar_df

def preprocess(use_preprocessed):
    """
    Perform the following preprocessing tasks:
    - Save a parquet of the raw data
    - Drop unused columns
    - Clean values
    - Convert column types
    - Save a parquet of the processed data
    """
    if not use_preprocessed:
        # Load csv into dataframe
        solar_df = pd.read_csv(cfg['filepaths']['input_csv'])

        # Save unprocessed data into parquet
        solar_df.to_parquet(
            cfg['filepaths']['parquet_raw'],
            index=False,
        )

        # Clean values in COMMUNITY_NAME column
        solar_df = clean_community_names(solar_df)

        # Convert object dtype columns to proper dtype
        solar_df = convert_object_dtypes(solar_df)

        # Save preprocessed data into parquet
        solar_df.to_parquet(
            cfg['filepaths']['parquet_processed'],
            index=False,
        )

    # Load parquet into dataframe
    solar_df = pd.read_parquet(cfg['filepaths']['parquet_processed'])

    # Show column information for solar_df
    for col in solar_df.columns:
        print(f'Column:\t{col} [{solar_df[col].dtype}]')
        print(f'Number of unique values in {col}: {solar_df[col].nunique()}')
        print(f'Number of NA values in {col}: {solar_df[col].isna().sum()}')
        print('===========================')

    return solar_df

if __name__ == '__main__':
    solar_df = preprocess(
        use_preprocessed=os.path.exists(config['preprocessing']['filepaths']['parquet_processed'])
    )
