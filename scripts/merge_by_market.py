# ================================== STEP 2 ==================================
from pyspark.sql import functions as F
from collect_stock_data import data_by_stock

def merge_by_market(spark, stock_column, stock_markets, covid_column, covid_area, sector, read_path, write_path):
    '''Groups DataFrames for each stock market by Year and Week calculating the average value for stock_column.
    Then merges with Covid data and returns the list of merged DataFrames.'''
    # Initialize an empty list for grouped DataFrames
    merged_dfs = []

    # Get Covid data
    covid_df = filter_corona_by_location(spark, covid_column, covid_area, read_path)

    for market in stock_markets:
        # Read and cleanse CSVs for market
        df = data_by_stock(spark, market, stock_column, read_path)

        print(f'============ Filtering and merging stock market data for {market}... ============')

        # Filter by sector
        if sector != "None":
            sector_df = spark.read.csv(f"{read_path}/stock_market_data/CategorisedStocks.csv", header=True, inferSchema=True)
            sector_df = sector_df.filter(sector_df.Sector == sector).select("Company")
            df = df.join(sector_df, df['Name'] == sector_df['Company'])

        # Group price/volume by 'Market', 'Year' and 'Week'
        if stock_column == 'Volume':
            df = df.groupBy('Market', 'Year', 'Week').agg(F.sum(stock_column).alias(f"Total_{stock_column}"))
        else:
            df = df.groupBy('Market', 'Year', 'Week').agg(F.avg(stock_column).alias(f"Average_{stock_column}"))

        # Merge with Covid data
        df = df.join(covid_df, ["Year", "Week"])
        merged_dfs.append(df)

        # Write to CSV file
        csv_path = f"{write_path}/CSVs/{market}_{stock_column}_{covid_area[1]}.csv"
        print(f"Writing to {csv_path} ...")
        df.write.csv(csv_path, header=True, mode="overwrite")        

        print('================================================================================')
    
    return merged_dfs


def filter_corona_by_location(spark, column, area, read_path):
    '''Filters Covid data by chosen area and groups by Year and Week.'''
    print(f'============ Filtering and grouping Covid data... ============')
    
    # Read the CSV file into a DataFrame with header and schema inferred
    division = area[0]
    location = area[1]
    df = spark.read.csv(f"{read_path}/covid_death_data/export_{division}.csv", header=True, inferSchema=True)

    # Filter by world / region / country
    df = df.filter(F.col(df.columns[0]) == location)

    # Select only the necessary columns
    df = df.select(df.columns[0], 'date', column)

    # Convert the date string to a date format and extract the week and year
    df = df.withColumn("date", F.to_date(F.col("date"), 'yyyy-MM-dd'))
    df = df.withColumn("Week", F.weekofyear(F.col("date")))
    df = df.withColumn("Year", F.year(F.col("date")))

    # Group by 'year' and 'week_of_year' and then aggregate
    print('==============================================================')
    return df.groupBy(df.columns[0], "Year", "Week").agg(F.avg(F.col(column)).alias("average_" + column))
