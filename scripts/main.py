import sys
from pyspark.sql import SparkSession
from merge_all import merge_markets_covid, merge_sectors_covid

DATA = "gs://marketquake_data"
RESULTS = "gs://marketquake_results"

markets = ['sp500', 'forbes2000', 'nyse', 'nasdaq']
sectors = ['Healthcare', 'Technology', 'Industrials']

# Assign arguments
stock_column = sys.argv[1]

if sys.argv[2] == 'all_markets':
    stock_groups = markets
    analyze = merge_markets_covid
elif sys.argv[2] == 'all_sectors':
    stock_groups = sectors
    analyze = merge_sectors_covid
else:
    stock_groups = [sys.argv[2]]
    if sys.argv[2] in markets:
        analyze = merge_markets_covid
    elif sys.argv[2] in sectors:
        analyze = merge_sectors_covid
    else:
        raise Exception(f"Invalid stock group argument: {sys.argv[2]}")
    

covid_column = sys.argv[3]
covid_area = (sys.argv[4], sys.argv[5])

# Print arguments
print("========================================================================================")
print(f"Received arguments:\n\tstock_column={stock_column},\n\tstock_groups={stock_groups},\n\tcovid_column={covid_column},\n\tcovid_area={covid_area}")
print("========================================================================================")

# Initialize Spark session
spark = SparkSession.builder.appName("MarketQuakeAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Start the analysis
analyze(spark, stock_column, stock_groups, covid_column, covid_area, DATA, RESULTS)

spark.stop()
