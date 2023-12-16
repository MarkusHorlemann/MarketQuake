from pyspark.sql import functions as F


def process_stocks(spark, column, market_name, read_path):
    '''Reads all CSV files for particular market, filters by relevant dates and groups by Year and Week.'''
    print(f'Reading and cleansing data for stocks in {market_name}...')

    # Read all stock files in market into one DataFrame
    df = spark.read.csv(f"{read_path}/stock_market_data/{market_name}/", header=True, inferSchema=True)

    # Only select necessary columns
    df = df.select('Date', column)

    # Convert Date string to PySpark date type
    df = df.withColumn("Date", F.to_date(F.col("Date"), "dd-MM-yyy"))

    # Filter data for relevant period (January 2020 to December 2022)
    df = df.filter((F.col("Date") >= "2020-01-01") & (F.col("Date") <= "2022-12-31"))

    # Add 'Market' column to DataFrame
    df = df.withColumn("Market", F.lit(market_name))

    # Calculate weekly average prices / weekly total volume
    df = df.withColumn("Year", F.year("Date"))
    df = df.withColumn("Week", F.weekofyear("Date"))
    if column == 'Volume':
        df = df.groupBy("Market", "Year", "Week").agg(F.sum(column).alias(column))
    else:
        df = df.groupBy("Market", "Year", "Week").agg(F.avg(column).alias(column))
    
    # Remove columns with null values in any row
    return df.na.drop()


def merge_by_market(spark, stock_column, stock_market, covid_df, read_path, write_path_final):
    '''Groups DataFrames for given stock market by Year and Week calculating the average value for stock_column.
    Then merges with Covid data and returns the merged DataFrame.'''

    # Get stock market data
    stock_df = process_stocks(spark, stock_column, stock_market, read_path)

    # Merge with Covid data
    df = stock_df.join(covid_df, ["Year", "Week"])

    # Write to CSV file
    print(f'Writing to {write_path_final} ...')
    df.write.csv(write_path_final, header=True, mode="overwrite")     

    return df


# # Filter by sector
# if sector != "None":
#     sector_df = spark.read.csv(f"{read_path}/stock_market_data/CategorisedStocks.csv", header=True, inferSchema=True)
#     sector_df = sector_df.filter(sector_df.Sector == sector).select("Company")
#     df = df.join(sector_df, df['Name'] == sector_df['Company'])
