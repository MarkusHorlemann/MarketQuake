import pandas as pd
import matplotlib.pyplot as plt

def plot_market(df, column, market, plot_file_path):
    '''Plots the development of chosen column for particular stock market.'''
    print(f"Plotting {column} column in {market} ...")

    # Convert PySpark DataFrame to Pandas DataFrame
    pandas_df = df.toPandas()

    # Create a 'Date' column in Pandas DataFrame
    pandas_df['Date'] = pandas_df[df.Year].astype(str) + '-' + pandas_df[df.Week].astype(str) + '-1'
    pandas_df['Date'] = pd.to_datetime(pandas_df['Date'], format='%Y-%W-%w')

    # Sort the DataFrame by the 'Date' column
    pandas_df.sort_values('Date', inplace=True)

    # Drop the 'Year' and 'Week' columns if they are not needed
    pandas_df.drop(columns=['Year', 'Week'], inplace=True)

    # Plotting
    plt.figure(figsize=(14, 7))
    plt.plot(pandas_df['Date'], pandas_df[column], marker='o')

    plt.title(f"Plot for {market}")
    plt.xlabel('Date')
    plt.ylabel('Data Value')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save the plot as an image
    print(f"Writing to {plot_file_path} ...")
    plt.savefig(plot_file_path)


def plot_stocks_corona(df, stock_column, market, covid_column, location, plot_file_path):
    '''Plots the stock market development with COVID death tolls.'''
    print(f"Plotting {stock_column} column in {market} and {covid_column} in {location}...")

    # Convert PySpark DataFrame to Pandas DataFrame
    pandas_df = df.toPandas()

    # Create a 'Date' column in Pandas DataFrame
    pandas_df['Date'] = pandas_df['Year'].astype(str) + '-' + pandas_df['Week'].astype(str) + '-1'
    pandas_df['Date'] = pd.to_datetime(pandas_df['Date'], format='%Y-%W-%w')

    # Sort the DataFrame by the 'Date' column
    pandas_df.sort_values('Date', inplace=True)

    # Plotting
    fig, ax1 = plt.subplots(figsize=(14, 7))

    # Plot column
    color = 'tab:blue'
    ax1.set_xlabel('Date')
    ax1.set_ylabel(stock_column, color=color)
    ax1.plot(pandas_df['Date'], pandas_df[stock_column], color=color, marker='o')
    ax1.tick_params(axis='y', labelcolor=color)

    # Create a second y-axis for 'average_daily_covid_deaths'
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:red'
    ax2.set_ylabel('Average Daily Covid Deaths', color=color)  # we already handled the x-label with ax1
    ax2.plot(pandas_df['Date'], pandas_df[covid_column], color=color, marker='o')
    ax2.tick_params(axis='y', labelcolor=color)

    # Other plot settings
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    if market is None:
        plt.title('Stock Prices and Covid Deaths Over Time')
    else:
        plt.title(f'{market} Stock Prices and Covid Deaths Over Time')
    plt.xticks(rotation=45)
    plt.grid(True)

    # Save the plot as an image file
    print(f"Writing to {plot_file_path} ...")
    fig.savefig(plot_file_path)
