'''This script merges the Stock Market dataset into 4 Dataframes for each market and cleanses them by 
calculating average prices and total volume for each week between January 2020 and December 2022.
It ignores files with missing information for at least one week.'''

import os, sys
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, weekofyear, year, avg, sum, to_date, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

class MissingDataException(Exception):
    pass

def clean_and_group_by_week(file_path):
    '''Groups stock prices and volume by week from January 2020 to December 2022.'''
    # Read the CSV file
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    file_name = file_path[file_path.rfind('/')+1 : -4] 

    # Add 'Name' column to DataFrame
    df = df.withColumn("Name", lit(file_name))

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

# Read folder name from argument
if len(sys.argv) != 2:
    raise Exception("No folder name passed!")
elif len(sys.argv[1]) == 0:
    raise Exception("Empty string passed as folder name!")
folder = sys.argv[1]

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StockDataCleansing") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
dataset_path = "gs://marketquake_data/stock_market_data"

# Load file names from GCS bucket
files = [line.strip() for line in os.popen(f'gsutil ls {dataset_path}/{folder}/*.csv')]
dfs = []

# Cleanse and merge dataframes
for file_path in files:
    try:
        dfs.append(clean_and_group_by_week(file_path))
        print(f"File {file_path} processed.")
    except MissingDataException:
        print(f"File {file_path} disregarded due to missing data.")

# Merge and write to GCS
final_df = reduce(DataFrame.union, dfs)
final_df.write.option("header","true").csv(f"{dataset_path}_clean/{folder}.csv")

# Stop Spark session
spark.stop()
