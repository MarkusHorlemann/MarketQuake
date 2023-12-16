import sys
from pyspark.sql import SparkSession
from merge_all import merge_stocks_covid

DATA = "gs://marketquake_data"
RESULTS = "gs://marketquake_results"

# Assign argumetns
stock_column = sys.argv[1]
if sys.argv[2] == 'all':
    stock_markets = ['sp500', 'forbes2000', 'nyse', 'nasdaq']
else:
    stock_markets = [sys.argv[2]]
covid_column = sys.argv[3]
covid_area = (sys.argv[4], sys.argv[5])

# Print arguments
print("========================================================================================")
print(f"Received arguments:\n\tstock_column={stock_column},\n\tstock_markets={stock_markets},\n\tcovid_column={covid_column},\n\tcovid_area={covid_area}")
print("========================================================================================")

# Initialize Spark session
spark = SparkSession.builder.appName("MarketQuakeAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Start the analysis
merge_stocks_covid(spark, stock_column, stock_markets, covid_column, covid_area, DATA, RESULTS)

spark.stop()
