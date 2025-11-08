import pandas as pd

def set_pandas_display_options():
    """Configure pandas CLI display options."""

    # Do not wrap columns onto new row
    pd.set_option('display.expand_frame_repr', False)

    # Always display all rows
    pd.set_option('display.max_rows', None)

    # Always display all columns
    pd.set_option('display.max_columns', None)

    print('Pandas display options configured')

set_pandas_display_options()
