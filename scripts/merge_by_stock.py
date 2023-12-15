# ================================== STEP 1 ==================================
import os
from pyspark.sql.functions import col, weekofyear, year, avg, sum, to_date, lit

class MissingDataException(Exception):
    pass


def clean_and_group_by_week(spark, column, market, file_path):
    '''Groups stock prices and volume by week from January 2020 to December 2022.'''
    # Read the CSV file
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    file_name = file_path[file_path.rfind('/')+1 : -4] 

    # Only select necessary columns
    df = df.select('Date', column)

    # Convert Date string to PySpark date type
    df = df.withColumn("Date", to_date(col("Date"), "dd-MM-yyy"))

    # Filter data for relevant period (January 2020 to December 2022)
    df = df.filter((col("Date") >= "2020-01-01") & (col("Date") <= "2022-12-31"))

    # Add 'Market' column to DataFrame
    df = df.withColumn("Market", lit(market))

    # Add 'Name' column to DataFrame
    df = df.withColumn("Name", lit(file_name))

    # Calculate weekly average prices / weekly total volume
    df = df.withColumn("Year", year("Date"))
    df = df.withColumn("Week", weekofyear("Date"))
    if column == 'Volume':
        df = df.groupBy("Market", "Name", "Year", "Week").agg(sum(column).alias(column))
    else:
        df = df.groupBy("Market", "Name", "Year", "Week").agg(avg(column).alias(column))
    
    # Check if any column has a null value in any row
    if df.na.drop().count() == df.count():
        return df
    else:
        raise MissingDataException("DataFrame missing information for at least one week.")


def merge_by_stock(spark, market_name: str, column: str, read_path: str):
    '''Merges the CSV files for a given stock market into a Dataframe and cleanses it by 
    calculating average prices and total volume for each week between January 2020 and December 2022.
    It disregards files with missing information for at least one week.'''

    print(f'============ Reading and cleansing data for stocks in {market_name}... ============')

    # Load file paths from GCS bucket | gsutil ls
    files = [line.strip() for line in os.popen(f'ls {read_path}/stock_market_data/{market_name}/*.csv')]

    # Cleanse and merge dataframes
    final_df = None
    for file_path in files:
        try:
            df = clean_and_group_by_week(spark, column, market_name, file_path)
            final_df = df if final_df is None else final_df.unionAll(df)
            print(f"File {file_path} processed.")
        except MissingDataException:
            print(f"File {file_path} disregarded due to missing data.")
    
    print('===================================================================================')
    return final_df
