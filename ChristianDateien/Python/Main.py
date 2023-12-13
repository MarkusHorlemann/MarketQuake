from pyspark.sql import SparkSession
from StockmarketCovidAnalysis import merge_stocks_and_corona_by_date


def main(stock_files, covid_file, stock_column, covid_column, storage_location, sector):
    spark = SparkSession.builder.appName("StockMarketAnalysis").getOrCreate()

    merged_df = merge_stocks_and_corona_by_date(spark, stock_column, covid_column, stock_files, covid_file,
                                                storage_location, sector)
    merged_df.show(merged_df.count())

    spark.stop()


def get_user_choice(options, prompt):
    while True:
        for idx, option in enumerate(options, start=1):
            print(f"{idx}. {option}")

        choice = input(prompt)

        if choice.isdigit() and 1 <= int(choice) <= len(options):
            return options[int(choice) - 1]
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    print("Please enter the required information for merging stock and COVID data.")

    # Prompting user for file paths
    n = int(input("Enter number of stock files: "))
    stock_files = [input(f"Enter path for stock file {i + 1}: ") for i in range(n)]

    covid_file = input("Enter the path for the COVID data file: ")

    # Providing options for column names
    stock_column_options = ['Close', 'Open', 'High', 'Low']  # Example options
    covid_column_options = ['daily_covid_deaths', 'daily_covid_cases']  # Example options

    stock_column = get_user_choice(stock_column_options, "Choose the column for stock data: ")
    covid_column = get_user_choice(covid_column_options, "Choose the column for COVID data: ")

    # Prompting user for storage location
    storage_location = input("Enter the location to store results: ")

    # Prompting user for sector
    sector_options = ['Healthcare', 'Industry', 'Industrials', 'No specific sector']
    sector = get_user_choice(sector_options, "Choose the sector (or select 'No specific sector' if not applicable): ")

    main(stock_files, covid_file, stock_column, covid_column, storage_location, sector)
