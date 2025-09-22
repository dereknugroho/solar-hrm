import os

from src.preprocessing import preprocess
from src.utils.config_loader import config

def main(use_preprocessed):
    solar_df = preprocess(use_preprocessed)

if __name__ == '__main__':
    main(
        use_preprocessed=os.path.exists(config['preprocessing']['filepaths']['parquet_processed']),
    )

'''
Production-grade enhancements:
1. Use a JSON config file to load paths
2. Error handling if reading CSV or Parquet fails
3. Use logging module instead of print() statements and redirect log to file
4. Log timing for preprocessing tasks
5. Implement chunked reading
6. Do not mutate dataframes in place
'''