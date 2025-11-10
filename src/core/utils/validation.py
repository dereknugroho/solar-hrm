import pandas as pd

from core.utils.utils import ensure_dataframe

def validate_dataframe(df: pd.DataFrame, expected_spec: dict) -> None:
    expected_columns = set(expected_spec.keys())
    missing = expected_columns - set(df.columns)
    extra = set(df.columns) - expected_columns
    if missing:
        raise KeyError(f'Missing columns: {missing}')
    if extra:
        raise ValueError(f'Unexpected columns: {extra}')

    for col, spec in expected_spec.items():
        dtype, allow_null, min_val = spec.get('dtype'), spec.get('allow_null', False), spec.get('min', None)
        if df[col].dtype != dtype:
            raise TypeError(f'{col} must be {dtype} dtype')
        if not allow_null and df[col].isnull().any():
            raise ValueError(f'{col} contains null values')
        if min_val is not None and (df[col] < min_val).any():
            raise ValueError(f'{col} must be >= {min_val}')

@ensure_dataframe
def validate_installations_preprocessed(installations_preprocessed: pd.DataFrame) -> None:
    spec = {
        'installation_id': {'dtype': 'int64'},
        'panels_reporting': {'dtype': 'int64', 'min': 0},
        'community': {'dtype': 'category'},
    }
    validate_dataframe(installations_preprocessed, spec)

@ensure_dataframe
def validate_readings_preprocessed(readings_preprocessed: pd.DataFrame) -> None:
    spec = {
        'installation_id': {'dtype': 'int64'},
        'timestamp': {'dtype': 'datetime64[ns]'},
        'power_watts_5min_avg': {'dtype': 'float64', 'min': 0},
    }
    validate_dataframe(readings_preprocessed, spec)

@ensure_dataframe
def validate_installations_feature_engineered(installations_feature_engineered: pd.DataFrame) -> None:
    spec = {
        'installation_id': {'dtype': 'int64'},
        'community': {'dtype': 'category'},
        'panels_reporting_max': {'dtype': 'int64', 'min': 0},
        'panels_reporting_efficiency': {'dtype': 'float64', 'min': 0},
        'panels_reporting_avg': {'dtype': 'float64', 'min': 0},
    }
    validate_dataframe(installations_feature_engineered, spec)

@ensure_dataframe
def validate_readings_feature_engineered(readings_feature_engineered: pd.DataFrame) -> None:
    spec = {
        'installation_id': {'dtype': 'int64'},
        'timestamp': {'dtype': 'datetime64[ns]'},
        'power_watts_5min_avg': {'dtype': 'float64', 'min': 0},
        'energy_prod_wh': {'dtype': 'float64', 'min': 0},
    }
    validate_dataframe(readings_feature_engineered, spec)
