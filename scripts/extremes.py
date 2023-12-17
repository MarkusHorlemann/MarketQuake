from merge_all import process_corona
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def calculate_extremes(df, column, group):
    '''Computes the weekly return by taking the ratio of the weekly day's price to the previous week's 
    price and subtracting 1. The highest performing stock is the one having the largest total return.'''
    # Step 1: Calculate weekly returns
    windowSpec = Window().partitionBy("Name").orderBy("Year", "Week")
    df_returns = df.withColumn("WeeklyReturn", F.col(column) / F.lag(column).over(windowSpec) - 1)

    # Step 2: Aggregate returns to get overall weekly performance
    df_performance = df_returns.groupBy("Name").agg(F.sum("WeeklyReturn").alias("Performance"))

    # Step 3: Identify the worst and best performing stock
    worst = df_performance.orderBy("Performance").first()
    best = df_performance.orderBy(F.desc("Performance")).first()

    # Display the results
    print(f"\nThe worst performing stock in {group} is:", worst["Name"])
    print(f"The best performing stock in {group} is:", best["Name"])


def find_for_market(spark, stock_column, stock_markets, covid_column, covid_area, read_path, write_path):
    '''Reads and finds highest/lowest performing stock in chosen markets. Then merges with Covid data.'''
    print("========================================================================================")

    pass

    print("========================================================================================")



def find_for_sector(spark, stock_column, sectors, covid_column, covid_area, read_path, write_path):
    pass

