import os

import pandas as pd

from src.data_preprocessing import preprocess
from src.utils.config import FILEPATHS
from src.utils.paths import from_root

# Show all rows
pd.set_option('display.max_rows', None)

# Show all columns
pd.set_option('display.max_columns', None)

def main():
    #################
    # Preprocessing #
    #################
    solar_df = preprocess(
        use_preprocessed=os.path.exists(
            from_root(FILEPATHS['parquet_processed'])
        )
    )

    # Show column information for solar_df
    for col in solar_df.columns:
        print(f'Column:\t{col} [{solar_df[col].dtype}]')
        print(f'Number of unique values in {col}: {solar_df[col].nunique()}')
        print(f'Number of NA values in {col}: {solar_df[col].isna().sum()}')
        print('===========================')

    print(f'{solar_df.head()}')
    print(f'===================')
    print(f'{solar_df.tail()}')

    #############################
    # Exploratory Data Analysis #
    #############################


if __name__ == '__main__':
    main()
