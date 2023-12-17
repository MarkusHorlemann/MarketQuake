from pyspark.sql import functions as F


def cleanse_stocks(df, column, group):
    '''Filters DataFrame for particular group by relevant dates, adds columns by Year and Week.'''
    print(f'\nCleansing data for stocks in {group}...')

    # Only select necessary columns
    df = df.select('Date', column)

    # Convert Date string to PySpark date type
    df = df.withColumn("Date", F.to_date(F.col("Date"), "dd-MM-yyy"))

    # Filter data for relevant period (January 2018 to December 2022)
    df = df.filter((F.col("Date") >= "2018-01-01") & (F.col("Date") <= "2022-12-31"))

    # Add 'Group' column to DataFrame
    df = df.withColumn("Group", F.lit(group))

    # Calculate weekly average prices / weekly total volume
    df = df.withColumn("Year", F.year("Date"))
    df = df.withColumn("Week", F.weekofyear("Date"))
    if column == 'Volume':
        df = df.groupBy("Group", "Year", "Week").agg(F.sum(column).alias(column))
    else:
        df = df.groupBy("Group", "Year", "Week").agg(F.avg(column).alias(column))
   
    # Remove columns with null values in any row
    return df.na.drop()


def merge_by_group(stock_df, stock_column, stock_group, covid_df, write_path_final):
    '''Groups DataFrames for given stock market / sector by Year and Week calculating the average   
    value for stock_column. Then merges with Covid data and returns the merged DataFrame.'''

    # Cleanse stock market data
    stock_df = cleanse_stocks(stock_df, stock_column, stock_group)

    # Merge with Covid data
    df = stock_df.join(covid_df, on=["Year", "Week"], how='leftouter')

    # Write to CSV file
    print(f'Writing to {write_path_final} ...')
    df.write.csv(write_path_final, header=True, mode="overwrite")     

    return df
