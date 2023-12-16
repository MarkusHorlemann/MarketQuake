from pyspark.sql import functions as F
from merge_by_market import merge_by_market


def process_corona(spark, column, area, read_path):
    '''Filters Covid data by chosen area and groups by Year and Week.'''
    print(f'Filtering and grouping Covid data...')
    
    # Read the CSV file into a DataFrame with header and schema inferred
    division = area[0]
    location = area[1]
    df = spark.read.csv(f"{read_path}/covid_death_data/export_{division}.csv", header=True, inferSchema=True)

    # Filter by world / region / country
    df = df.filter(F.col(df.columns[0]) == location)

    # Select only the necessary columns
    df = df.select(df.columns[0], 'date', column)

    # Convert the date string to a date format and extract the week and year
    df = df.withColumn("date", F.to_date(F.col("date"), 'yyyy-MM-dd'))
    df = df.withColumn("Week", F.weekofyear(F.col("date")))
    df = df.withColumn("Year", F.year(F.col("date")))

    # Group by 'year' and 'week_of_year' and then aggregate
    return df.groupBy(df.columns[0], "Year", "Week").agg(F.avg(F.col(column)).alias(column))


def merge_stocks_covid(spark, stock_column, stock_markets, covid_column, covid_area, read_path, write_path):
    '''Reads and merges Covid and stock market data in chosen markets altogether.'''
    print("========================================================================================")

    # Define path to write CSVs
    csv_path = f"{write_path}/CSVs/all_{stock_column}_{covid_area[1]}_{covid_column}.csv"

    # Get Covid data
    covid_df = process_corona(spark, covid_column, covid_area, read_path)

    # Merge data for all stock markets
    result_df = None
    for market in stock_markets:
        df = merge_by_market(spark, stock_column, market, covid_df,
                             read_path, csv_path.replace('CSVs/all_', f'CSVs/{market}_'))
        result_df = result_df.unionAll(df) if result_df else df
    
    # If just one stock_market, necessary CSV are already generated
    if len(stock_markets) == 1:
        return

    print('Merging all Covid and stock market data...')

    # Group by Year, Week and covid_column
    if stock_column == 'Volume':
        result_df = result_df.groupBy('Year', 'Week', covid_column).agg(F.sum(stock_column).alias(stock_column))
    else:
        result_df = result_df.groupBy('Year', 'Week', covid_column).agg(F.avg(stock_column).alias(stock_column))

    # Write to CSV file
    print(f'Writing to {csv_path} ...')
    result_df.write.csv(csv_path, header=True, mode="overwrite")

    print("========================================================================================")
