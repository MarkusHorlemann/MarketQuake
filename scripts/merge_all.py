# ================================== STEP 3 ==================================
from pyspark.sql import functions as F
from merge_by_market import merge_by_market

def merge_stocks_covid(spark, stock_column, stock_markets, covid_column, covid_area, sector, read_path, write_path):
    '''Merges Covid and stock market data in chosen markets individually and together.'''
    # Group covid data and data for each market by Year and Week 
    market_dfs, covid_df = merge_by_market(spark, stock_column, stock_markets, covid_column, covid_area, sector, read_path, write_path)

    # If only 1 market in list, necessary CSVs are already generated
    if len(market_dfs) == 1:
        return

    print('============ Merging all Covid and stock market data... ============')

    # Merge all the stock markets
    stock_df = None
    for df in market_dfs:
        stock_df = df if stock_df is None else stock_df.unionAll(df)
   
    # Group by Year and Week
    if stock_column == 'Volume':
        stock_df = stock_df.groupBy('Year', 'Week').agg(F.sum(stock_column).alias(f"Total_{stock_column}"))
    else:
        stock_df = stock_df.groupBy('Year', 'Week').agg(F.avg(stock_column).alias(f"Average_{stock_column}"))
    
    # Write market data alone to CSV
    path_market = f"{write_path}/stock_market_data/CSVs/all_{stock_column}.csv"
    print(f"Writing to {path_market} ...")
    stock_df.write.csv(path_market, header=True, mode="overwrite")

    # Join Covid and stock market data
    result_df = stock_df.join(covid_df, ["Year", "Week"])

    # Write to CSV file
    path_merged = f"{write_path}/stocks_covid_merged/CSVs/all_{stock_column}_{covid_area[1]}.csv"
    print(f"Writing to {path_merged} ...")
    result_df.write.csv(path_merged, header=True, mode="overwrite")    

    print('====================================================================')
    return result_df
