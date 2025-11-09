import os
import shutil
from functools import wraps

import pandas as pd

from src.utils.config import from_root, FILEPATHS

def check_parquets_exist(filepath_keys: list[str]) -> bool:
    """Return True if all required parquet files exist."""
    return all(os.path.exists(from_root(FILEPATHS[k])) for k in filepath_keys)

def create_clean_directory(directory):
    """Create clean target directory."""
    dir_from_root = from_root(directory)

    if os.path.exists(dir_from_root):
        shutil.rmtree(dir_from_root)

    os.makedirs(dir_from_root, exist_ok=True)

def ensure_dataframe(func):
    """Decorator to check that all arguments are a pandas DataFrame."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        for arg in args:
            if not isinstance(arg, pd.DataFrame):
                raise TypeError(f"{func.__name__}: all positional arguments must be pandas DataFrames")
        for kw, val in kwargs.items():
            if not isinstance(val, pd.DataFrame):
                raise TypeError(f"{func.__name__}: all keyword arguments must be pandas DataFrames")
        return func(*args, **kwargs)
    return wrapper
