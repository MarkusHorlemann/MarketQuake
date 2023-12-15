'''Generates PySpark command to execute in Cloud based on user input.'''

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
    stock_market_options = ['sp500', 'forbes2000', 'nyse', 'nasdaq']
    stock_column_options = ['Volume', 'Low', 'High', 'Open', 'Close', 'Adjusted Close']

    # Choose number of stock markets
    stock_market_num = 0
    while stock_market_num != 1 and stock_market_num != 4:
        stock_market_num = int(input("Enter the number of stock markets for the analysis (1 or 4): "))

    # Choose stock markets and metric
    if stock_market_num == 4:
        stock_market = 'all'
    else:
        stock_market = get_user_choice(stock_market_options, f"Enter the name of the stock market: ")

    # Choose metric (column)
    stock_column = get_user_choice(stock_column_options, "Choose the column in stock data you want to analyze: ")
    return stock_market, stock_column


def select_covid_data():
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


print(f'============ Welcome to MarketQuake! ============')
print("Please enter the required information for analyzing stock market and COVID-19 data.")
print("Please, input NUMBERS and not words.")

# Choose stock markets and stock column
stock_market, stock_column = select_stock_data()

# Choose economy sector
sector_options = ['Healthcare', 'Industry', 'Industrials', 'None']
sector = get_user_choice(sector_options, "Choose the economy sector (or 'None' if not applicable): ")

# Choose Covid area and metric
covid_area, covid_column = select_covid_data()

command = f"spark-submit main.py {stock_column} {stock_market} {covid_column} {covid_area[0]} {covid_area[1]} {sector} --py-files collect_stock_data.py merge_by_market.py merge_all.py"
print(f"Your PySpark command is:\n{command}")
