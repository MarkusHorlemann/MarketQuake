'''This script merges the Stock Market dataset into 4 Dataframes and cleanses them by calculating average
prices and total volume for each week, as well as removing files with missing information for at least one week.'''

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, weekofyear, year, avg, sum, to_date

class MissingDataException(Exception):
    pass

def clean_and_group_by_week(df, file_name):
    '''Groups stock prices and volume by week from January 2020 to December 2022.'''
    # Add 'Name' column to DataFrame
    df = df.withColumn("Name", file_name.replace('.csv', ''))

    # Convert Date string to PySpark date type
    df = df.withColumn("Date", to_date(col("Date"), "dd-MM-yyy"))

    # Filter data for relevant period (January 2020 to December 2022)
    df = df.filter((col("Date") >= "2020-01-01") & (col("Date") <= "2022-12-31"))

    # Calculate weekly average prices
    df = df.withColumn("Week", weekofyear("Date"))
    df = df.withColumn("Year", year("Date"))
    df = df.groupBy("Name", "Year", "Week").agg(
        sum("Volume").alias("Volume"),
        avg("Low").alias("Low"),
        avg("High").alias("High"),
        avg("Open").alias("Open"),
        avg("Close").alias("Close"),
        avg("Adjusted Close").alias("Adjusted Close"))
    
    # Check if any column has a null value in any row
    if df.na.drop().count() == df.count():
        return df
    else:
        raise MissingDataException("DataFrame missing information for at least one week.")

# Initialize Spark session
spark = SparkSession.builder.appName("StockDataCleansing").getOrCreate()
dataset_path = "gs://marketquake_data/stock_market_data"

for folder in ['forbes2000', 'nasdaq', 'nyse', 'sp500']:
    # Define DataFrame schema
    final_df = spark.createDataFrame([], ["Name", "Year", "Week", "Volume", "Low", "High", "Open", "Close", "Adjusted Close"])
    
    # Load file names from GCS bucket
    files = [line.strip() for line in os.popen(f'gsutil ls {dataset_path}/{folder}/*.csv')]

    # Cleanse and merge dataframes
    for file_path in files:
        df = spark.read.csv(file_path, header=True)
        file_name = file_path.replace(f"{dataset_path}/{folder}/", '')
        try:
            df = clean_and_group_by_week(df, file_name)
            final_df = final_df.union(df)
        except MissingDataException:
            print(f"File {file_path} disregarded due to missing data.")
    
    # Write merged file to GCS
    df.write.option("header","true").csv(f"{dataset_path}_clean/{folder}.csv")

# Stop Spark session
spark.stop()
