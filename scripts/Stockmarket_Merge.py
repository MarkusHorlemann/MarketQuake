from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_add, date_sub, mean


def merge_stock_markets_by_week(files):

    # Initialize Spark session

    spark = SparkSession.builder \
        .config("spark.driver.memory", "4g") \
        .appName("StockMarketAnalysis") \
        .getOrCreate()

    # Define start and end dates
    start_date = "2020-01-06"
    end_date = "2022-12-18"  # Update the end date accordingly

    # Generate a list of weeks
    weeks = ['2020-01-01 - 2020-01-05']
    given_date = start_date

    while given_date <= end_date:
        # Calculate the start and end of the current week
        week_start = given_date
        week_end = str(spark.createDataFrame([(given_date,)], ["Date"])
                       .withColumn("Date", to_date("Date"))
                       .select(date_sub(date_add("Date", 7), 1).alias("Week_End"))
                       .first().Week_End)

        # Create the week range string and add it to the list
        week_range = f"{week_start} - {week_end}"
        weeks.append(week_range)

        # Move to the next week
        given_date = str(spark.createDataFrame([(given_date,)], ["Date"])
                         .withColumn("Date", to_date("Date"))
                         .select(date_add("Date", 7).alias("Next_Week_Start"))
                         .first().Next_Week_Start)

    # Create a DataFrame for the weeks
    weeks_df = spark.createDataFrame([(week,) for week in weeks], ["Week"])

    # Define the schema for the DataFrame
    schema = "Date STRING, Close DOUBLE"

    dfs = [spark.read.schema(schema).csv(file, header=True) for file in files]

    # Combine DataFrames into a single DataFrame
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = merged_df.union(df)

    # Convert 'Date' column to PySpark DateType
    merged_df = merged_df.withColumn("Date", to_date("Date", "dd-MM-yyyy"))

    # Filter rows starting from November 25, 2019
    merged_df = merged_df.filter(col('Date') >= '2020-01-01')

    # Define the UDF to check if a date is within a week
    def check_date_within_week(date, week):
        start = week.substr(1, 10)
        end = week.substr(14, 10)

        # Return boolean condition directly
        return col("date").between(start, end)

    # Join the DataFrames and filter rows
    result_df = merged_df.join(weeks_df, check_date_within_week(col("Date"), col("Week")), how="inner")

    mean_close_by_week = result_df.groupBy("Week").agg(mean("Close").alias("Mean_Close"))

    mean_close_by_week_sorted = mean_close_by_week.orderBy("Week")

    spark.stop()

    return mean_close_by_week_sorted



