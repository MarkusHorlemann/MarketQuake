from pyspark.sql import functions as F
from merge_by_group import merge_by_group


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


def merge_markets_covid(spark, stock_column, stock_markets, covid_column, covid_area, read_path, write_path):
    '''Reads and merges Covid and stock market data in chosen markets altogether.'''
    print("========================================================================================")

    # Define path to write CSVs
    csv_path = f"{write_path}/CSVs/general/all_{stock_column}_{covid_area[1]}_{covid_column}.csv"

    # Get Covid data
    covid_df = process_corona(spark, covid_column, covid_area, read_path)

    result_df = None
    for market in stock_markets:
        # Read all stock files in market into one DataFrame
        df = spark.read.csv(f"{read_path}/stock_market_data/{market}/", header=True, inferSchema=True)

        # Merge with Corona data
        df = merge_by_group(df, stock_column, market, covid_df, csv_path.replace('general/all_', f'general/{market}_'))

        # Merge with other stock markets
        result_df = result_df.unionAll(df) if result_df else df

    # If just one stock_market, necessary CSV are already generated
    if len(stock_markets) == 1:
        return

    print('\nMerging all Covid and stock market data...')

    # Group by Year, Week and covid_column
    if stock_column == 'Volume':
        result_df = result_df.groupBy('Year', 'Week', covid_column).agg(F.sum(stock_column).alias(stock_column))
    else:
        result_df = result_df.groupBy('Year', 'Week', covid_column).agg(F.avg(stock_column).alias(stock_column))

    # Write to CSV file
    print(f'Writing to {csv_path} ...')
    result_df.write.csv(csv_path, header=True, mode="overwrite")

    print("========================================================================================")


def merge_sectors_covid(spark, stock_column, sectors, covid_column, covid_area, read_path, write_path):
    '''Reads and merges Covid and stock data in individual sectors.'''
    print("========================================================================================")

    # Get Covid data
    covid_df = process_corona(spark, covid_column, covid_area, read_path)

    # Read categorized stocks file
    print("Reading and filtering stock data for each market...")
    sectors_df = spark.read.csv(f"{read_path}/categorized_stocks.csv", header=True, inferSchema=True)

    # Read stock files in each market
    stock_dfs = [spark.read.csv(f"{read_path}/stock_market_data/{market}/", header=True, inferSchema=True)
                 for market in ['sp500', 'forbes2000', 'nyse', 'nasdaq']]

    for sector in sectors:
        # Filter sectors DataFrame by sector
        filter_df = sectors_df.filter(sectors_df['Category'] == sector).select("Name")

        result_df = None
        for df in stock_dfs:
            # Filter stock DataFrame by sector
            df = df.join(filter_df, on=['Name'])

            # Merge with other stock markets
            result_df = result_df.unionAll(df) if result_df else df

            # Remove used stock names to avoid duplicates
            filter_df = filter_df.subtract(df.select("Name"))

        # Merge with Corona data
        csv_path = f"{write_path}/CSVs/general/{sector}_{stock_column}_{covid_area[1]}_{covid_column}.csv"
        result_df = merge_by_group(result_df, stock_column, sector, covid_df, csv_path)

    print("========================================================================================")