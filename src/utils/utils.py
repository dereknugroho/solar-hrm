import pandas as pd

from functools import wraps

from src.utils.config import from_root

# def ensure_dataframe(func):
#     """Decorator to check that the first argument is a pandas DataFrame."""
#     @wraps(func)
#     def wrapper(df, *args, **kwargs):
#         if not isinstance(df, pd.DataFrame):
#             raise TypeError(f"{func.__name__}: argument must be a pandas DataFrame")
#         return func(df, *args, **kwargs)

#     return wrapper

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


def fetch_parquet(filepath: str) -> pd.DataFrame:
    """Load processed parquet into master dataframe."""
    return pd.read_parquet(from_root(filepath))
