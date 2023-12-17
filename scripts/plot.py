import sys, os
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import storage

RESULTS = "gs://marketquake_results"

def plot_market(df, column, write_path):
    '''Plots the development of chosen column for particular stock market.'''

    # Create a 'Date' column in Pandas DataFrame
    df['Date'] = df['Year'].astype(str) + '-' + df['Week'].astype(str) + '-1'
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%W-%w')

    # Sort the DataFrame by the 'Date' column
    df.sort_values('Date', inplace=True)

    # Plotting
    plt.figure(figsize=(14, 7))
    plt.plot(df['Date'], df[column], marker='o')
    plt.title("Stock Prices Over Time")
    
    plt.xlabel('Date')
    plt.ylabel('Data Value')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save the plot locally
    print(f"Writing to {RESULTS}/{write_path} ...")
    plt.savefig('plot.png')

    # Upload the local file to GCS
    client = storage.Client()
    blob = client.get_bucket(RESULTS.replace('gs://', '')).blob(write_path)
    blob.upload_from_filename('plot.png')


def plot_stocks_corona(df, stock_column, covid_column, write_path):
    '''Plots the stock market development with COVID tolls.'''

    # Create a 'Date' column in Pandas DataFrame
    df['Date'] = df['Year'].astype(str) + '-' + df['Week'].astype(str) + '-1'
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%W-%w')

    # Sort the DataFrame by the 'Date' column
    df.sort_values('Date', inplace=True)

    # Plotting
    fig, ax1 = plt.subplots(figsize=(14, 7))

    # Plot stock market column
    color = 'tab:blue'
    ax1.set_xlabel('Date')
    ax1.set_ylabel(stock_column, color=color)
    ax1.plot(df['Date'], df[stock_column], color=color, marker='o')
    ax1.tick_params(axis='y', labelcolor=color)

    # Create a second y-axis for Covid column
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:red'
    ax2.set_ylabel(covid_column, color=color)  # we already handled the x-label with ax1
    ax2.plot(df['Date'], df[covid_column], color=color, marker='o')
    ax2.tick_params(axis='y', labelcolor=color)

    # Other plot settings
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.title('Stock Prices and Covid Cases Over Time')
    plt.xticks(rotation=45)
    plt.grid(True)

    # Save the plot locally
    print(f"Writing to {RESULTS}/{write_path} ...")
    plt.savefig('plot.png')

    # Upload the local file to GCS
    client = storage.Client()
    blob = client.get_bucket(RESULTS.replace('gs://', '')).blob(write_path)
    blob.upload_from_filename('plot.png')


# Assign argumetns
stock_column = sys.argv[1]
if sys.argv[2] == 'all_markets':
    stock_groups = ['sp500', 'forbes2000', 'nyse', 'nasdaq', 'all']
elif sys.argv[2] == 'all_sectors':
    stock_groups = ['Healthcare', 'Technology', 'Industrials']
else:
    stock_groups = [sys.argv[2]]
covid_column = sys.argv[3]
location = sys.argv[4]

# Print arguments
print("========================================================================================")
print(f"Received arguments:\n\tstock_column={stock_column},\n\tstock_groups={stock_groups},\n\tcovid_column={covid_column},\n\tlocation={location}")
print("========================================================================================")


for group in stock_groups:
    # Define common file name
    name = f"{group}_{stock_column}_{location}_{covid_column}"

    # Define read path and read CSV
    read_path = f"{RESULTS}/CSVs/{name}.csv"
    print(f"Reading from {read_path} ...")
    df = pd.read_csv(list(os.popen(f'gsutil ls {read_path}/*.csv'))[0])

    # Define write path
    plot_path = f"Plots/{name}.png"

    # Plot DataFrame
    plot_stocks_corona(df, stock_column, covid_column, plot_path)
    print()
