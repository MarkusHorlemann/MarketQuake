import os
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


# Load CSV folder names
csv_folders = [line.strip() for line in os.popen(f'gsutil ls {RESULTS}/CSVs/')]
for folder in csv_folders:
    # Parse user input in folder names
    folder = folder.replace(f'{RESULTS}/CSVs/', '')
    if folder == '':
        continue
    parse = folder.replace('.csv/', '')

    market = parse[:parse.find('_')]
    parse = parse[parse.find('_')+1:]

    stock_column = parse[:parse.find('_')]
    parse = parse[parse.find('_')+1:]

    location = parse[:parse.find('_')]
    covid_column = parse[parse.find('_')+1:]

    # Define write paths
    market_path = f"Plots/{market}_{stock_column}.png"
    covid_path = f"{market_path.replace('.png', '')}_{location}_{covid_column}.png"

    # Define read path and read CSV
    read_path = list(os.popen(f'gsutil ls {RESULTS}/CSVs/{folder}*.csv'))[0]
    print(f"Reading from {RESULTS}/CSVs/{folder} ...")
    df = pd.read_csv(read_path)

    # Plot DataFrame
    plot_market(df, stock_column, market_path)
    plot_stocks_corona(df, stock_column, covid_column, covid_path)
    print()
