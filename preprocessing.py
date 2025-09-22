import os
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def drop_unused_columns(solar_df):
    """Drop unused columns WATT_HOUR and KILOWATT_HOUR."""
    return solar_df.drop(columns=['WATT_HOUR', 'KILOWATT_HOUR'])

def clean_community_names(solar_df):
    """Apply corrections to COMMUNITY_NAME column."""
    solar_df['COMMUNITY_NAME'] = solar_df['COMMUNITY_NAME'].replace({'dartmouth': 'Dartmouth'})
    return solar_df

# 2016.11.02 07:55:00

def convert_object_dtypes(solar_df):
    """Convert dtype object columns to proper dtype."""
    solar_df['DATE'] = pd.to_datetime(
        solar_df['DATE'],
        format='%Y.%m.%d %H:%M:%S',
        errors='coerce',
    )

    solar_df['COMMUNITY_NAME'] = solar_df['COMMUNITY_NAME'].astype('category')
    solar_df['FORWARD_SORTATION_AREA'] = solar_df['FORWARD_SORTATION_AREA'].astype('category')

    return solar_df

def main(use_preprocessed):
    if not use_preprocessed:
        # Load csv into dataframe
        solar_df = pd.read_csv('data/Solar_City_Micro_Inverters.csv')

        # Save unprocessed data into parquet
        solar_df.to_parquet('parquets/solar_df_unprocessed.parquet', index=False)

        # Remove WATT_HOUR and KILOWATT_HOUR columns
        solar_df = drop_unused_columns(solar_df)

        # Apply corrections to values in COMMUNITY_NAME column
        solar_df = clean_community_names(solar_df)

        # Convert object dtype columns to proper dtype
        solar_df = convert_object_dtypes(solar_df)

        # Save preprocessed data into parquet
        solar_df.to_parquet('parquets/solar_df.parquet', index=False)

    # Load parquet into dataframe
    solar_df = pd.read_parquet('parquets/solar_df.parquet')

    # Show column information for solar_df
    for col in solar_df.columns:
        print(f'Column:\t{col} [{solar_df[col].dtype}]')
        print(f'Number of unique values in {col}: {solar_df[col].nunique()}')
        print(f'Number of NA values in {col}: {solar_df[col].isna().sum()}')
        print('===========================')

    return solar_df

if __name__ == '__main__':
    main(os.path.exists('parquets/solar_df.parquet'))
