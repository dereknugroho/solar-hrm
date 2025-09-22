import pandas as pd

from functools import wraps

def ensure_dataframe(func):
    """Decorator to check that the first argument is a pandas DataFrame."""
    @wraps(func)
    def wrapper(df, *args, **kwargs):
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"{func.__name__}: argument must be a pandas DataFrame")
        return func(df, *args, **kwargs)

    return wrapper
