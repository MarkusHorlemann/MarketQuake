from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w

def find_extremes(csv_path):
    spark = SparkSession.builder.appName("StockMarketExtremes").getOrCreate()

    # Load the CSV data
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    # Define a window specification to rank stocks based on closing price
    window_spec = w.partitionBy("stock_symbol").orderBy("date")

    # Calculate rank of each stock based on closing price
    df_ranked = df.withColumn("rank", f.rank().over(window_spec))

    # Find the top-performing and bottom-performing stocks
    top_performing_stocks = df_ranked.filter(f.col("rank") == 1)
    bottom_performing_stocks = df_ranked.filter(f.col("rank") == f.max("rank").over(w.partitionBy()))

    # Calculate the correlation between individual stock performance and overall market trends
    correlation_df = df.select("stock_symbol", "closing_price").groupBy("stock_symbol").agg(
        f.corr("closing_price", "daily_deaths").alias("correlation")
    )

    # Stop the Spark session
    spark.stop()

    return top_performing_stocks, bottom_performing_stocks, correlation_df
