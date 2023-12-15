# ================================== STEP 3 ==================================
from pyspark.sql import functions as F
from merge_by_market import merge_by_market

def merge_stocks_covid(spark, stock_column, stock_markets, covid_column, covid_area, sector, read_path, write_path):
    '''Merges Covid and stock market data in chosen markets individually and together.'''
    # Group covid data and data for each market by Year and Week 
    merged_dfs = merge_by_market(spark, stock_column, stock_markets, covid_column, covid_area, sector, read_path, write_path)

    # If only 1 market in list, necessary CSVs are already generated
    if len(merged_dfs) == 1:
        return

    print('============ Merging all Covid and stock market data... ============')

    # Merge all the stock markets
    result_df = None
    for df in merged_dfs:
        result_df = df if result_df is None else result_df.unionAll(df)
    
    # Group by Year, Week and covid_column
    if stock_column == 'Volume':
        result_df = result_df.groupBy('Year', 'Week', covid_column).agg(F.sum(stock_column).alias(f"Total_{stock_column}"))
    else:
        result_df = result_df.groupBy('Year', 'Week', covid_column).agg(F.avg(stock_column).alias(f"Average_{stock_column}"))

    # Write to CSV file
    csv_path = f"{write_path}/CSVs/all_{stock_column}_{covid_area[1]}.csv"
    print(f"Writing to {csv_path} ...")
    result_df.write.csv(csv_path, header=True, mode="overwrite")    

    print('====================================================================')
    return result_df
