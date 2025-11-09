import os

import pandas as pd

from src.data_preprocessing import preprocess, check_preprocessed_parquets_exist
from src.feature_engineering import build_feature_dataset, check_feature_engineered_parquets_exist
from src.utils import pd_config
from src.utils.config import FILEPATHS
from src.utils.paths import from_root

def main():
    print(f'Initiating pipeline...')

    # Data preprocessing
    installations_preprocessed, readings_preprocessed = preprocess(
        preprocessed_exists=check_preprocessed_parquets_exist()
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

    # Feature engineering
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

if __name__ == '__main__':
    main()
