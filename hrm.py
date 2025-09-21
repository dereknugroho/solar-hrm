import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

if __name__ == '__main__':
    # # Load csv into parquet
    # solar_df = pd.read_csv('Solar_City_Micro_Inverters.csv')
    # solar_df.to_parquet('solar.parquet')

    # Load parquet file into dataframes
    solar_df = pd.read_parquet('solar.parquet')

    print(f'Number of rows in solar_df: {len(solar_df)}')

    # # Show incorrect calculation of WATT_HOUR from WATTS
    # print(solar_df[(solar_df['SYSTEM_ID'] == 1433118) & (solar_df['WATT_HOUR'].notna())])

    # # Show column information for solar_df
    # for col in solar_df.columns:
    #     print(f'Column:\t{col} [{solar_df[col].dtype}]')
    #     print(f'Number of unique values in {col}: {solar_df[col].nunique()}')
    #     print(f'Number of NA values in {col}: {solar_df[col].isna().sum()}')
    #     print('===========================')

    # Show information about solar energy
    '''
    - Count number of unique values in each column
    - Count number of NA values in each column
    - Count number of unique values of COMMUNITY_NAME for each FORWARD_SORTATION_AREA

    WATTS
    WATT_HOUR
    KILOWATT_HOUR
    '''

    # num_unique_solar_df_postal_code = solar_df[['FORWARD_SORTATION_AREA']].nunique()
    # print(num_unique_solar_df_postal_code)

    # unique_solar_df_postal_code = solar_df['FORWARD_SORTATION_AREA'].unique()
    # print(unique_solar_df_postal_code)

    # num_unique_solar_df_community_name = solar_df[['COMMUNITY_NAME']].nunique()
    # print(num_unique_solar_df_community_name)

    # unique_solar_df_community_name = solar_df['COMMUNITY_NAME'].unique()
    # print(unique_solar_df_community_name)
