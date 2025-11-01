import pandas as pd

from functools import wraps

from src.utils.config import from_root

def ensure_dataframe(func):
    """Decorator to check that the first argument is a pandas DataFrame."""
    @wraps(func)
    def wrapper(df, *args, **kwargs):
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"{func.__name__}: argument must be a pandas DataFrame")
        return func(df, *args, **kwargs)

    return wrapper

def fetch_parquet(filepath: str) -> pd.DataFrame:
    """Load processed parquet into master dataframe."""
    return pd.read_parquet(from_root(filepath))
