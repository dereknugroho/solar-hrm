import os

import pandas as pd

from src.data_preprocessing import preprocess, preprocessed_parquets_exist
from src.feature_engineering import aggregate_installations
from src.utils import pd_config
from src.utils.config import FILEPATHS
from src.utils.paths import from_root

def main():
    print(f'Initiating pipeline...')

    # Data preprocessing
    installations, readings = preprocess(
        preprocessed_exists=preprocessed_parquets_exist()
    )

    # Feature engineering
    installations_agg = aggregate_installations(installations)

    print(f'***************\nreadings summary (count: {len(readings)})\n***************')
    for col in readings.columns:
        print(f'Column:\t{col} [{readings[col].dtype}]')
        print(f'Number of unique values in {col}: {readings[col].nunique()}')
        print(f'Number of NA values in {col}: {readings[col].isna().sum()}')
        print('===========================')
    
    print(f'***************\ninstallations_agg summary (count: {len(installations_agg)})\n***************')
    for col in installations_agg.columns:
        print(f'Column:\t{col} [{installations_agg[col].dtype}]')
        print(f'Number of unique values in {col}: {installations_agg[col].nunique()}')
        print(f'Number of NA values in {col}: {installations_agg[col].isna().sum()}')
        print('===========================')

    print(f'***************\ninstallations_agg table (count: {len(installations_agg)})***************\n{installations_agg}')

if __name__ == '__main__':
    main()
