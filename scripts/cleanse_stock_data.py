'''This script cleanses the Stock Market dataset in place by calculating average stock prices and 
total volume for each week, and removing files with missing information for at least one week.'''

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
    rows_with_nulls = df.filter(expr("OR(" + ", ".join(f"isNull({col})" for col in df.columns) + ")"))
    if rows_with_nulls.count() == 0:
        return df
    else:
        raise MissingDataException("DataFrame missing information for at least one week.")

# Initialize Spark session
spark = SparkSession.builder.appName("StockPriceAnalysis").getOrCreate()

# Load data from GCS bucket
dataset_path = "gs://marketquake_data/stock_market_data"
folders = spark.read.text(f"gsutil ls {dataset_path}/*").select("value").collect()
print(f"Folders in {dataset_path}: {folders}")

# Overwrite files in each folder with cleansed version or delete them if data is missing
for folder in folders:
    files = spark.read.text(f"gsutil ls {dataset_path}/{folder}/*.csv").select("value").collect()

    for file in files:
        file_path = f"{dataset_path}/{folder}/{file}"
        df = spark.read.csv(file_path, header=True)

        try:
            df = clean_and_group_by_week(df)
            df.write.mode("overwrite").csv(file_path)
        except MissingDataException:
            subprocess.run(["gsutil", "rm", file_path])
            print(f"File {file_path} removed due to missing data.")

# Stop Spark session
spark.stop()
