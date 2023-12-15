import sys
from pyspark.sql import SparkSession
from merge_with_covid import merge_stocks_and_covid

READ = "../marketquake_data" # "gs://marketquake_data"
WRITE = "../marketquake_results" # "gs://marketquake_results"

# Assign argumetns
stock_column = sys.argv[1]
if sys.argv[2] == 'all':
    stock_markets = ['forbes2000', 'nasdaq', 'nyse', 'sp500']
else:
    stock_markets = [sys.argv[2]]
covid_column = sys.argv[3]
covid_area = (sys.argv[4], sys.argv[5])
sector = sys.argv[6]

# Print arguments
print("=================================================")
print(f"Received arguments: stock_column={stock_column}, stock_markets={stock_markets}, covid_column={covid_column}, covid_area={covid_area}, sector={sector}")
print("=================================================")

# Initialize Spark session
spark = SparkSession.builder\
    .appName("MarketQuakeAnalysis")\
    .config("spark.driver.memory", "8g")\
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Start the analysis
result_df = merge_stocks_and_covid(spark,
                                   stock_column,
                                   stock_markets,
                                   covid_column,
                                   covid_area,
                                   sector,
                                   READ,
                                   WRITE)
result_df.show(result_df.count())
spark.stop()
