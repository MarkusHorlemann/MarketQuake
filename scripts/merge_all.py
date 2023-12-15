# ================================== STEP 3 ==================================
from pyspark.sql import functions as F
from merge_by_market import merge_by_market

def merge_stocks_covid(spark, stock_column, stock_markets, covid_column, covid_area, sector, _, bucket_path):
    '''Reads and merges Covid and stock market data in chosen markets altogether.'''
    print('============ Merging all Covid and stock market data... ============')

    # Define read path
    read_path = f"{bucket_path}/CSVs/{market}_{stock_column}_{covid_area[1]}.csv"

    # Read and merge all intermediate CSV files
    result_df = None
    for market in stock_markets:
        df = spark.read.csv(read_path, header=True, inferSchema=True)
        result_df = df if result_df is None else result_df.unionAll(df)
    
    # Group by Year, Week and covid_column
    if stock_column == 'Volume':
        result_df = result_df.groupBy('Year', 'Week', covid_column).agg(F.sum(stock_column).alias(f"Total_{stock_column}"))
    else:
        result_df = result_df.groupBy('Year', 'Week', covid_column).agg(F.avg(stock_column).alias(f"Average_{stock_column}"))

    # Write to CSV file
    write_path = f"{bucket_path}/CSVs/all_{stock_column}_{covid_area[1]}.csv"
    print(f"Writing to {write_path} ...")
    result_df.write.csv(write_path, header=True, mode="overwrite")    

    print('====================================================================')
