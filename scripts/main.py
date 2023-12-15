from pyspark.sql import SparkSession
from merge_with_covid import merge_stocks_and_covid

def get_user_choice(options, prompt):
    '''Presents the choice options and ask user to select one.'''
    while True:
        for idx, option in enumerate(options, start=1):
            print(f"{idx}. {option}")

        choice = input(prompt)
        if choice.isdigit() and 1 <= int(choice) <= len(options):
            return options[int(choice)-1]
        else:
            print("Invalid choice. Please try again.")


def select_stock_data():
    '''Lets the user choose the stock markets to analyze and the metric (column) to analyze them with.'''
    # Provide options for user choice
    stock_market_options = ['forbes2000', 'nasdaq', 'nyse', 'sp500']
    stock_column_options = ['Volume', 'Low', 'High', 'Open', 'Close', 'Adjusted Close']

    # Choose number of stock markets
    stock_market_num = 0
    while stock_market_num < 1 and stock_market_num > 4:
        stock_market_num = int(input("Enter the number of stock markets for the analysis: "))

    # Choose stock markets and metric
    if stock_market_num == 4:
        stock_markets = stock_market_options
    else:
        stock_markets = {get_user_choice(stock_market_options, f"Enter the name of stock market {i+1}: ") for i in range(stock_market_num)}

    # Choose metric (column)
    stock_column = get_user_choice(stock_column_options, "Choose the column in stock data you want to analyze: ")
    return stock_markets, stock_column


def select_covid_data(covid_area_options: list, covid_column_options: list):
    '''Lets the user choose the area and metric (column) to analyze Covid data with.'''
    # Provide options for user choice
    covid_area_options = ['world', 'regions', 'country']
    covid_region_options = ['Americas', 'Europe', 'Asia', 'Africa', 'Oceania']
    covid_column_options = ['daily_covid_deaths'] #, 'daily_covid_cases'

    # Choose area division
    location = get_user_choice(covid_area_options, "Choose the area division for COVID data: ")

    # Choose region / country
    if location == 'world':
        covid_area = (location, 'World')
    elif location == 'regions':
        region = get_user_choice(covid_region_options, "Enter the region you want to analyze: ")
        covid_area = (location, region)
    else:
        country = input("Enter the country iso3c code: ")
        covid_area = ('country', country)

    # Choose metric (column)
    covid_column = get_user_choice(covid_column_options, "Choose the column for COVID data: ")

    return covid_area, covid_column


# Provide sector options for user choice
sector_options = ['Healthcare', 'Industry', 'Industrials', 'No specific sector']

print("Welcome to MarketQuake! Please enter the required information for analyzing stock market and COVID-19 data.")
print("Please, input NUMBERS and not words.")

# Choose stock markets and stock column
stock_markets, stock_column = select_stock_data()

# Choose economy sector
sector = get_user_choice(sector_options, "Choose the economy sector (or 'No specific sector' if not applicable): ")

# Choose Covid area and metric
covid_area, covid_column = select_covid_data()

# Start the analysis
spark = SparkSession.builder.appName("MarketQuakeAnalysis").getOrCreate()
result_df = merge_stocks_and_covid(spark,
                                   stock_column,
                                   stock_markets,
                                   covid_column,
                                   covid_area,
                                   sector,
                                   "gs://marketquake_data",
                                   "gs://marketquake_results")
result_df.show(result_df.count())
spark.stop()
