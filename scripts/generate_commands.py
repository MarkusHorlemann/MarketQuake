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


def select_covid_data():
    '''Lets the user choose the area and metric (column) to analyze Covid data with.'''
    # Provide options for user choice
    covid_area_options = ['world', 'regions', 'country']
    covid_region_options = ['Americas', 'Europe', 'Asia', 'Africa', 'Oceania']
    covid_column_options = ['daily_covid_deaths', 'daily_covid_cases']

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


def select_market():
    '''Lets the user choose the stock markets to analyze.'''
    # Provide options for user choice
    stock_market_options = ['sp500', 'forbes2000', 'nyse', 'nasdaq']
    
    # Choose number of stock markets
    stock_market_num = get_user_choice(['one individual', 'all'], "Enter how many stock markets you want to analyze: ")

    # Choose stock markets and metric
    if stock_market_num == 'all':
        stock_market = 'all_markets'
    else:
        stock_market = get_user_choice(stock_market_options, f"Enter the name of the stock market: ")

    return stock_market


def select_sector():
    '''Lets the user choose the economy sectors to analyze.'''
    # Provide options for user choice
    sector_options = ['Healthcare', 'Technology', 'Industrials']

    # Choose number of economy sectors
    sectors_num = get_user_choice(['one individual', 'all'], "Enter how many economy sectors you want to analyze: ")

    # Choose sectors and metric
    if sectors_num == 'all':
        sector = 'all_sectors'
    else:
        sector = get_user_choice(sector_options, f"Enter the name of the sector: ")
    
    return sector


print(f'=================== Welcome to MarketQuake! ===================')
print("Please enter the required information for analyzing stock market and COVID-19 data.")
print("Please, input NUMBERS and not words.")

# Choose stock metric (column)
stock_column_options = ['Volume', 'Low', 'High', 'Open', 'Close', 'AdjustedClose']
stock_column = get_user_choice(stock_column_options, "Choose the column in stock data you want to analyze: ")

# Choose Covid area and metric
covid_area, covid_column = select_covid_data()

# Choose to analyze the extremes or group in general
how = get_user_choice(['extremes', 'general'], f"Choose to analyze the extremes or markets / sectors in general: ")

# Choose either markets or sectors for analysis
if how == 'extremes':
    group = select_market()
else:
    group = get_user_choice(['markets', 'sectors'], "Choose to analyze stocks by markets or by sectors: ")
    if group == 'markets':
        group = select_market()
    else:
        group = select_sector()

# Generate PySpark command
command = f"spark-submit main.py {how} {stock_column} {group} {covid_column} {covid_area[0]} {covid_area[1]} --py-files merge_by_group.py merge_all.py"
print(f"\nYour PySpark command is:\n{command}")

# Generate plotting command
if how == 'general':
    command = f"python plot.py {stock_column} {group} {covid_column} {covid_area[1]}"
    print(f"\nYour plotting command is:\n{command}")

print()
