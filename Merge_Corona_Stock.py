from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_add, concat, lit


def merge_corona_stock_markets_by_week(file_path, file_path2):
    # Initialize Spark session
    spark = SparkSession.builder.appName("StockMarketAnalysis").getOrCreate()

    export_world = spark.read.csv(file_path, header=True)
    file_stockmarket_close = spark.read.csv(file_path2, header=True)

    # Show the original data frames
    export_world.show(export_world.count())
    file_stockmarket_close.show(file_stockmarket_close.count())

    # Select relevant columns from export_world
    selected_columns = ['date', 'daily_covid_deaths']
    export_world = export_world.select(selected_columns)

    # Create a new column with date plus 7 days
    date_plus_7_days = export_world.withColumn(
        "date_plus_7_days",
        concat(col("date"), lit(" - "), date_add(col("date"), 6).cast("string"))
    )

    # Drop the original date column
    date_plus_7_days = date_plus_7_days.drop("date")

    # Reorder columns and rename the new column to "Date"
    date_plus_7_days_reordered = date_plus_7_days.select("date_plus_7_days", "daily_covid_deaths")
    export_world = date_plus_7_days_reordered.withColumnRenamed("date_plus_7_days", "Date")

    # Show the modified export_world DataFrame
    total_rows = export_world.count()
    print(total_rows)
    export_world.show(export_world.count())

    # Delete the last 10 rows
    export_world = export_world.limit(total_rows - 48)

    # Merge the two data frames using the correct column for join
    # Assuming the common column for join is "Week" in file_stockmarket_close
    merged_df = file_stockmarket_close.join(export_world, file_stockmarket_close["Week"] == export_world["Date"])

    # Select specific columns
    selected_columns_merged = merged_df.select(
        col("Date"),
        col("Mean_Close"),
        col("daily_covid_deaths")
    )

    # Stop Spark session
    spark.stop()

    return selected_columns_merged
