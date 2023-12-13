from pyspark.sql.functions import to_date, weekofyear, year, col
from pyspark.sql import functions as F, SparkSession
import os


# Function to aggregate a specific column in a CSV file
def aggregate_column(spark, column, file_path, sector):
    # Read the CSV file into a DataFrame with header and schema inferred
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    if sector != "No specific sector":
        df = select_sector(spark, df, sector)

    # Select the columns to keep
    columns_to_keep = ['Year', 'Week', column]
    df = df.select(columns_to_keep)

    # Group by 'Year' and 'Week' and then aggregate
    df_grouped = df.groupBy('Year', 'Week').agg(F.avg(column).alias("average_" + column))

    df_ordered = df_grouped.orderBy('Year', 'Week')

    return df_ordered


# Function to merge multiple stock DataFrames
def merge_multiple_stocks(spark, column, file_paths, location, sector):
    if not file_paths:
        print("No file paths provided.")
        return None

    # Read and aggregate each file into a DataFrame
    # Initialize an empty list for DataFrames
    data_frames = []
    counter = 0

    # Create two new directories
    os.makedirs(location + "StockPlots/")
    os.makedirs(location + "StockDataMerged/")

    # Loop through each file path and process the file
    for file_path in file_paths:

        aggregated_df = aggregate_column(spark, column, file_path, sector)
        data_frames.append(aggregated_df)

        # Save each DataFrame as a CSV
        output_csv_path = location + "StockDataMerged/StockTable" + str(counter) + ".csv"
        aggregated_df.write.csv(output_csv_path, mode="overwrite", header=True)

        # Plot each DataFrame
        col_names = aggregated_df.columns
        plot_file_path = location + "StockPlots/StockPlot" + str(counter) + ".png"
        plot_title = f"Plot for {os.path.basename(file_path)}"
        plot_stocks(aggregated_df, col_names, plot_title, plot_file_path)
        counter += 1

    # Initialize the merged DataFrame with the first DataFrame in the list
    merged_df = data_frames[0]

    # Iterate through the remaining DataFrames and merge them one by one
    for i, df in enumerate(data_frames[1:], start=2):  # Start with 2 for suffix
        suffix = f'_df{i}'
        for col in df.columns:
            if col not in ['Year', 'Week']:
                df = df.withColumnRenamed(col, col + suffix)
        merged_df = merged_df.join(df, ['Year', 'Week'], 'outer')

    # Coalesce the average_Close columns into one
    avg_close_cols = [col for col in merged_df.columns if 'average_' in col]
    merged_df = merged_df.withColumn('Close', F.coalesce(*avg_close_cols))

    # Optionally, drop the original average_Close columns
    for col in avg_close_cols:
        merged_df = merged_df.drop(col)

    # Order data frame by column and then by date
    sorted_df = merged_df.orderBy('Year', 'Week')

    # Debug: Show the final merged DataFrame
    # print("Final merged DataFrame:")
    # merged_df.show(sorted_df.count())

    return merged_df


def merge_corona_by_week(spark, column_name, file_path):
    # Read the CSV file into a DataFrame with header and schema inferred
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Assuming the date column is named 'Date', adjust if it's different
    date_column_name = 'Date'

    # Convert the date string to a date format and extract the week and year
    df = df.withColumn("date", to_date(col(date_column_name), 'yyyy-MM-dd'))
    df = df.withColumn("Week", weekofyear(col("date")))
    df = df.withColumn("Year", year(col("date")))

    # Select only the necessary columns: 'year', 'week_of_year', and the column to aggregate
    df = df.select("Year", "Week", column_name)

    # Group by 'year' and 'week_of_year' and then aggregate
    aggregated_df = df.groupBy("Year", "Week").agg(F.avg(col(column_name)).alias("average_" + column_name)).orderBy(
        "Year", "Week")

    return aggregated_df


def merge_stocks_and_corona_by_date(spark, column_stock, column_corona, file_path_stock, filepath_corona, location, sector):
    merged_stock_df = merge_multiple_stocks(spark, column_stock, file_path_stock, location, sector)
    merged_covid_death_df = merge_corona_by_week(spark, column_corona, filepath_corona)

    result_df = merged_stock_df.join(merged_covid_death_df, ["Year", "Week"], "inner")

    # result_df.show(result_df.count())

    plot_graph_stock_corona(result_df, location)

    output_path = location + "merged_stocks_corona.csv"
    # print(output_path)
    result_df.write.csv(output_path, mode="overwrite", header=True)

    return result_df


def plot_graph_stock_corona(df, location):
    import pandas as pd
    import matplotlib.pyplot as plt

    # Convert PySpark DataFrame to Pandas DataFrame
    pandas_df = df.toPandas()

    # Create a 'Date' column in Pandas DataFrame
    pandas_df['Date'] = pandas_df['Year'].astype(str) + '-' + pandas_df['Week'].astype(str) + '-1'
    pandas_df['Date'] = pd.to_datetime(pandas_df['Date'], format='%Y-%W-%w')

    # Sort the DataFrame by the 'Date' column
    pandas_df.sort_values('Date', inplace=True)

    # Plotting
    fig, ax1 = plt.subplots(figsize=(14, 7))

    # Plot 'Close' column
    color = 'tab:blue'
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Close', color=color)
    ax1.plot(pandas_df['Date'], pandas_df['Close'], color=color, marker='o')
    ax1.tick_params(axis='y', labelcolor=color)

    # Create a second y-axis for 'average_daily_covid_deaths'
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:red'
    ax2.set_ylabel('Average Daily Covid Deaths', color=color)  # we already handled the x-label with ax1
    ax2.plot(pandas_df['Date'], pandas_df['average_daily_covid_deaths'], color=color, marker='o')
    ax2.tick_params(axis='y', labelcolor=color)

    # Other plot settings
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.title('Stock Prices and Covid Deaths Over Time')
    plt.xticks(rotation=45)
    plt.grid(True)
    # plt.show()

    # Save the plot as an image file
    output = location + "plot_stock_corona.png"
    # print(output)
    fig.savefig(output)


def plot_stocks(df, column_names, plot_title, plot_file_path):
    import pandas as pd
    import matplotlib.pyplot as plt

    # Convert PySpark DataFrame to Pandas DataFrame
    pandas_df = df.toPandas()

    # Create a 'Date' column in Pandas DataFrame
    pandas_df['Date'] = pandas_df[column_names[0]].astype(str) + '-' + pandas_df[column_names[1]].astype(str) + '-1'
    pandas_df['Date'] = pd.to_datetime(pandas_df['Date'], format='%Y-%W-%w')

    # Sort the DataFrame by the 'Date' column
    pandas_df.sort_values('Date', inplace=True)

    # Drop the 'Year' and 'Week' columns if they are not needed
    pandas_df.drop(columns=['Year', 'Week'], inplace=True)

    # Plotting
    plt.figure(figsize=(14, 7))
    plt.plot(pandas_df['Date'], pandas_df[column_names[2]], marker='o')  # Ensure this is the correct column to plot

    plt.title(plot_title)
    plt.xlabel('Date')
    plt.ylabel('Data Value')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save the plot as an image file and show it
    plt.savefig(plot_file_path)
    # print(plot_file_path)
    # plt.show()


def select_sector(spark, df, sector):
    sectors = {'Healthcare': ["ABBV", "ABC", "ABT", "AMGN", "BAX", "BDX", "BIIB", "BMY", "CAH", "DGX",
                              "DVA", "EW", "GILD", "HSIC", "HOLX", "ILMN", "ISRG", "JNJ", "LH", "LLY", "MDT", "REGN",
                              "RHHBY", "RNA", "THC", "UHS", "UHT", "UNH", "VEEV", "ZBH", "ZTS"],

               "Industry": ["AAPL", "ADI", "ADP", "ADSK", "AKAM", "ALSN", "AMAT", "AMD", "AAPL", "ADI", "ADP", "ADSK",
                            "AKAM", "ALSN", "AMAT", "AMD",
                            "ANET", "ASML", "ATEN", "AVGO", "BIDU", "BIIB", "CSCO", "CSIQ", "CRM", "DAL", "EA", "EBAY",
                            "EPAM", "EQIX", "FIS", "FISV",
                            "GIB", "GILD", "GOOG", "GRMN", "HPE", "IBM", "ILMN", "INFY", "INTU", "ISRG", "JD", "JNPR",
                            "KLAC", "KEYS", "LDOS", "LRCX", "LNVGY", "LPL", "MCHP", "MEI", "MSFT", "MSI", "MU", "MXIM",
                            "NCMI", "NCR", "NEU", "NFLX", "NIU", "NKLA", "NLOK", "NNDM", "NOVA", "NOW", "NTAP", "NTCT",
                            "NTDOY", "NTES", "NXPI", "OMC", "PANW", "PAYC", "PAYX", "PHG", "PKI", "PLD", "PRGO", "PRU",
                            "PSA-PQ", "PSA-PR", "PTBRY", "PWR", "PX", "QSR", "QTWO", "RACE", "RADA", "RBA", "RCI",
                            "REGN",
                            "RHHBY", "RIO", "RIOCF", "RMD", "RNECY", "RNLSY", "ROK", "ROST", "RSG",
                            "SAP", "SE", "STM", "TXN", "UMC", "VEEV", "VMW", "VRSK", "VRSN", "WDAY",
                            "WDC", "WIT"],

               "Industrials": ["AAL", "ABM", "ADP", "AER", "AGM", "AIN", "AIRI", "AIT", "ALSN", "AME", "ARW", "ASGN",
                               "ASHTF", "ATI", "AWK", "BAESY", "BHE", "BLD",
                               "BMO", "DAL", "EMR", "ETN", "GD", "GE", "GLW", "GM", "GPC", "GVA",
                               "GWW", "HAL", "HII", "HON", "HUBB", "IBM", "IR", "ITW", "JBT", "JCI",
                               "KS", "LDOS", "LII", "LMT", "LUV", "MAN", "MCD", "MGA", "MMM", "MPX",
                               "NOC", "NSC", "AS", "TDG", "TDY", "TEL", "TEN", "TGH", "TGI", "THC", "THO", "THR",
                               "TNC", "TNK", "TNP", "TOL", "TPC", "TRN", "TTM", "TXT", "UAL", "UNP",
                               "UPS", "URI", "VMC", "VMI", "WAB", "WCN", "WM", "WNC", "WOR", "XPO",
                               "ZBH"]}

    # Check if the sector is present in the dictionary
    if sector not in sectors:
        print(f"Sector '{sector}' not found in sectors dictionary.")
        return spark.createDataFrame([], df.schema)  # Returns an empty DataFrame with the same schema

    # Get the list of stocks for the given sector
    sector_stocks = sectors[sector]

    # Filter the DataFrame to keep rows where the stock symbol is in the sector's list
    filtered_df = df.filter(col("Name").isin(sector_stocks))

    return filtered_df

'''
# Example usage
spark = SparkSession.builder.appName("StockMarketAnalysis").getOrCreate()
file_paths = [
    '/Users/christiansawadogo/PycharmProjects/CloudAndBigDta/Project1/StockFiles/stock_market_data_clean_test1.csv',
    '/Users/christiansawadogo/PycharmProjects/CloudAndBigDta/Project1/StockFiles/stock_market_data_clean_test2.csv',
    '/Users/christiansawadogo/PycharmProjects/CloudAndBigDta/Project1/StockFiles/stock_market_data_clean_test3.csv',
    '/Users/christiansawadogo/PycharmProjects/CloudAndBigDta/Project1/StockFiles/stock_market_data_clean_test4.csv'
]

file_path = '/Users/christiansawadogo/PycharmProjects/CloudAndBigDta/Project1/coronadeath/export_world.csv'
column_name1 = 'Close'
column_name2 = 'daily_covid_deaths'

storage_location = '/Users/christiansawadogo/Desktop/Studium/Fünftes Semester/Kurse_Auslandssemester_Madrid/Cloud and Big Data/CloudAndBigData_Project/Script_Results/'
sector = "Healthcare"

merged_df = merge_stocks_and_corona_by_date(spark, column_name1, column_name2, file_paths, file_path, storage_location, sector)
merged_df.show(merged_df.count())

spark.stop()

'''''