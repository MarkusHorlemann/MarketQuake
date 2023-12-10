'''This script cleanses the Stock Market dataset in place by calculating average stock prices and 
total volume for each week, and removing files with missing information for at least one week.'''

import os
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, weekofyear, avg, sum, to_date, expr

class MissingDataException(Exception):
    pass

def clean_and_group_by_week(df):
    '''Groups stock prices and volume by week from January 2020 to December 2022.'''
    # Convert Date string to PySpark date type
    df = df.withColumn("Date", to_date(col("Date"), "dd-MM-yyy"))

    # Filter data for relevant period (January 2020 to December 2022)
    df = df.filter((col("Date") >= "2020-01-01") & (col("Date") <= "2022-12-31"))

    # Calculate weekly average prices
    df = df.withColumn("Week", weekofyear("Date"))
    df = df.groupBy("Week").agg(
        sum("Volume").alias("Volume"),
        avg("Low").alias("Low"),
        avg("Open").alias("Open"),
        avg("High").alias("High"),
        avg("Close").alias("Close"),
        avg("Adjusted Close").alias("Adjusted Close"))
    
    # Check if any column has a null value in any row
    if df.na.drop().count() == df.count():
        return df
    else:
        raise MissingDataException("DataFrame missing information for at least one week.")

# Initialize Spark session
spark = SparkSession.builder.appName("StockPriceAnalysis").getOrCreate()

# Load file names from GCS bucket
dataset_path = "gs://marketquake_data/stock_market_data"
files = [line.strip() for line in os.popen(f'gsutil ls {dataset_path}/*/*.csv')]

# Overwrite files with cleansed version or delete them if data is missing
for file in files:
    df = spark.read.csv(file, header=True)
    subprocess.run(["gsutil", "rm", file])
    try:
        df = clean_and_group_by_week(df)
        df.write.csv(file)
    except MissingDataException:
        print(f"File {file} removed due to missing data.")

# Stop Spark session
spark.stop()
