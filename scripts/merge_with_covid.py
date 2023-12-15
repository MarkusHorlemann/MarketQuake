# ================================== STEP 3 ==================================
from pyspark.sql import functions as F
from merge_by_market import merge_by_market
from plot import plot_stocks_corona

def merge_stocks_and_covid(spark, stock_column, stock_markets, covid_column, covid_area, sector, read_path, write_path):
    # Group covid data and data for each market by Year and Week 
    market_dfs, covid_df = merge_by_market(spark, stock_column, stock_markets, read_path, write_path, sector)

    # Merge all the stock markets
    stock_df = None
    for df in market_dfs:
        stock_df = df if stock_df is None else stock_df.unionAll(df)
   
    # Group by Year and Week
    if stock_column == 'Volume':
        df = df.groupBy('Year', 'Week').agg(F.sum(stock_column).alias(f"Total_{stock_column}"))
    else:
        df = df.groupBy('Year', 'Week').agg(F.avg(stock_column).alias(f"Average_{stock_column}"))
    
    # Join and plot covid and stock market data
    result_df = stock_df.join(covid_df, ["Year", "Week"])
    plot_path = f"{write_path}/stocks_covid_merged/plots/all_{stock_column}_{covid_area[1]}.png"
    plot_stocks_corona(result_df, stock_column, covid_column, None, plot_path)

    return result_df
