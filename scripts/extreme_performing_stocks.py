from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w
import matplotlib.pyplot as plt
import pandas as pd


def find_extremes(csv_path):
    spark = SparkSession.builder.appName("StockMarketExtremes").getOrCreate()

    # Load the CSV data
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    # Filter data for the specified date range (2020, week 1 to 2022, week 50)
    filtered_data = df.filter((f.col("year") == 2020) & (f.col("week") >= 1) |
                              (f.col("year") > 2020) & (f.col("year") < 2022) |
                              (f.col("year") == 2022) & (f.col("week") <= 50))

    # Calculate the average closing price for each stock
    average_prices = filtered_data.groupBy("name").agg(f.avg("close").alias("avg_closing_price"))

    # Rank stocks based on average closing price
    windowSpec = w.orderBy(f.desc("avg_closing_price"))
    ranked_stocks = average_prices.withColumn("rank", f.rank().over(windowSpec))

    # Identify top-performing and bottom-performing stocks
    top_performing_stocks = ranked_stocks.filter(f.col("rank") == 1)
    bottom_performing_stocks = ranked_stocks.filter(f.col("rank") == f.max("rank").over(w.partitionBy()))

    """
    # Calculate correlation between individual stock performance and overall market trends
    correlation_data = filtered_data.join(average_prices, "name", "inner")
    correlation = correlation_data.stat.corr("closing_price", "avg_closing_price")

    # Display top-performing and bottom-performing stocks
    print("Top-performing stocks:")
    top_performing_stocks.show()

    print("Bottom-performing stocks:")
    bottom_performing_stocks.show()

    print(f"Correlation between individual stock performance and overall market trends: {correlation}")

    """

    # Extract the names of top and bottom performing stocks
    top_stock_names = top_performing_stocks.select("name").rdd.flatMap(lambda x: x).collect()
    bottom_stock_names = bottom_performing_stocks.select("name").rdd.flatMap(lambda x: x).collect()

    # Filter data for top and bottom performing stocks
    top_stock_data = df.filter(f.col("name").isin(top_stock_names))
    bottom_stock_data = df.filter(f.col("name").isin(bottom_stock_names))

    # Convert Spark DataFrames to Pandas DataFrames for plotting
    top_stock_pd = top_stock_data.toPandas()
    bottom_stock_pd = bottom_stock_data.toPandas()

    # Plotting
    plt.figure(figsize=(12, 6))

    for stock_name in top_stock_names:
        stock_data = top_stock_pd[top_stock_pd["name"] == stock_name]
        plt.plot(stock_data["week"], stock_data["close"], label=f"{stock_name} (Top Performing)")

    for stock_name in bottom_stock_names:
        stock_data = bottom_stock_pd[bottom_stock_pd["name"] == stock_name]
        plt.plot(stock_data["week"], stock_data["close"], label=f"{stock_name} (Bottom Performing)")

    plt.xlabel("Week")
    plt.ylabel("Closing Price")
    plt.title("Top and Bottom Performing Stocks Over Time")
    plt.legend()
    plt.grid(True)
    plt.show()

    # Stop the Spark session
    spark.stop()
