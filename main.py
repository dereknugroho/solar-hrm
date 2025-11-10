import os

import pandas as pd

from core.pipeline.data_preprocessing import preprocess, check_preprocessed_parquets_exist
from core.pipeline.feature_engineering import build_feature_dataset, check_feature_engineered_parquets_exist
from core.utils import pd_config
from core.utils.config import FILEPATHS
from core.utils.paths import from_root

def main():
    print(f'Initiating pipeline...')

    # Data preprocessing
    installations_preprocessed, readings_preprocessed = preprocess(
        preprocessed_exists=check_preprocessed_parquets_exist()
    )

    print(f'------------------------------------------------')
    print(f'| installations_preprocessed (count: {len(installations_preprocessed)}) |')
    print(f'------------------------------------------------')
    for col in installations_preprocessed.columns:
        print(f'Column: {col} [{installations_preprocessed[col].dtype}] [num_unique: {installations_preprocessed[col].nunique()}] [num_NA: {installations_preprocessed[col].isna().sum()}]')

    print(f'-------------------------------------------')
    print(f'| readings_preprocessed (count: {len(readings_preprocessed)}) |')
    print(f'-------------------------------------------')
    for col in readings_preprocessed.columns:
        print(f'Column: {col} [{readings_preprocessed[col].dtype}] [num_unique: {readings_preprocessed[col].nunique()}] [num_NA: {readings_preprocessed[col].isna().sum()}]')

    # Feature engineering
    installations_feature_engineered, readings_feature_engineered = build_feature_dataset(
        feature_engineered_exists=check_feature_engineered_parquets_exist(),
        installations_preprocessed=installations_preprocessed,
        readings_preprocessed=readings_preprocessed,
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

if __name__ == '__main__':
    main()
