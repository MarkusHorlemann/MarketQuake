# MarketQuake: Decoding the Pandemic Punch on Global Economy

---
**NOTE:** Visit our [website](https://sites.google.com/ucm.es/marketquake) for more information and better experience!

## Description of the Problem

The unprecedented challenges posed by COVID-19 in recent years highlighted the need for strategic crisis management. For investors and businesses, understanding how financial markets respond to the progression of a pandemic is crucial to assess and manage risk, make predictions for investment decisions and develop long-term plans for the future.

Our project aims to investigate the impact of the COVID-19 crisis on the world's economy through the analysis of two comprehensive datasets containing stock market information and Covid-related death toll. Focusing on stocks in NASDAQ, S&P500, and NYSE listed companies, the study will explore market trends, sector-specific performance, and the behaviour of various technical indicators during different phases of the crisis. 

### Key Questions to Address:
1. How did stock markets perform during different phases of the Coronavirus crisis?
2. When did significant market fluctuations occur, and how were they related to major Coronavirus-related events?  What trends can be observed?
3. Did the stock market exhibit heightened volatility during significant spikes in the COVID-19 death toll?
4. Is there a strong correlation between NASDAQ, S&P500, and NYSE indices, or do they exhibit unique patterns? Were there any notable differences in resilience between the indices?
5. What were the top-performing and bottom-performing stocks during the crisis, and how did individual stock performance correlate with overall market trends?
6. Which sectors were most and least affected by the Coronavirus crisis? Did any sectors experience growth during the pandemic?
7. What conclusions and forecasts can be made about crisis management in different companies?

The [results](https://sites.google.com/ucm.es/marketquake/results/evaluation) of the analysis will provide insights into stock performance, volatility, and potential investment strategies, contributing to the understanding of the economic implications of the global health crisis.

## Description of the Data

### Stock Market Dataset
- **Size:** 10.23 GB
- **Source:** [Kaggle](https://www.kaggle.com/datasets/paultimothymooney/stock-market-data)
- **Content:** Daily stock market prices for NASDAQ, S&P500, and NYSE listed companies from March 1980 to December 2022 in CSV format. Additionally includes companies from the Forbes Global 2000 ranking. Columns: Date, Volume, High, Low, Closing Price.

### COVID-19 Global Excess Deaths Dataset
- **Size:** 10.54 GB
- **Source:** [Kaggle](https://www.kaggle.com/datasets/joebeachcapital/covid19-global-excess-deaths-daily-updates)
- **Content:** Daily Covid-related death numbers in CSV format classified by area (world, region, continent, country), income group, and calculation method. Also provides daily infection numbers by country. Data spans from January 2020 to November 2023.

## Try it yourself
To replicate our analysis, refer to the following sections for setting up the environment, accessing the datasets, and executing the code.

### Prerequisites
As a professor, ensure that you are using your UCM Google account to access our [Marketquake](https://console.cloud.google.com/welcome?project=marketquake) project on Google Cloud Platform. External users cannot access any resources in the project. If you think you should be granted the permission, please, contant one of the authors of this repository.

### Data Acccess
To access the data used for our analysis, proceed to our project's [Google Cloud Storage](https://console.cloud.google.com/storage/browser?project=marketquake).
The revelant directories are:
- **marketquake_data:** contains the source data after cleansing.
  - **stock_market_data:** contains the Stock Market dataset. Here, we also included a small CSV file with stock categorization.
  - **covid_death_data:** contains the COVID-19 Global Excess Deaths dataset.
- **marketquake_results:** contains all the result files needed to answer the questions of our study.
  - **Plots:** contains the results of our study in the form of graphs in PNG.
  - **CSVs:** contains the results of our study in the CSV table format.
    - **extremes:** contains the results of our study concerning extreme-performing stocks.
    - **general:** contains the results of our study concerning the analysis of markets and sectors in general.

Refer to our project [website](https://sites.google.com/ucm.es/marketquake) for more details.

### Running the Code
To get a step-by-step demonstration of how to generate test cases and run the program, watch our [YouTube tutorial](https://youtu.be/X4iAlnSDSxo?feature=shared)!

1. Make sure you have checked out our GitHub repository on your local device.
2. Go to the Google Cloud Platform website and sign in using your UCM credentials.
3. From the Navigation Menu, go to "Dataproc" and then select "Clusters."
4. Locate the marketquake-cluster. Make sure it is running, otherwise start it by clicking the "Start" button on the same page. Navigate to Cluster Details by clicking on the cluster name.
5. Navigate to "VM instances" and open an SSH session of the master node (marketquake-cluster-m) in your browser. You might have to wait a couple of seconds till you can do it.
6. Wait for the terminal to open and authorize. Then, click "Upload File" in the top-right corner to upload all files from the scripts/ (but not preprocessing/) directory. Wait for the files to load.
7. (optional) After the upload, verify the files presence by typing ls in the command line.
8. Generate the needed PySpark and/or plotting command by running: python generate_commands.py. Answer the appearing prompts based on your specific needs.
9. Once the PySpark command is displayed as output in the terminal, copy and paste it into the command line and run it. Wait for the computation to complete and display the output, in particular the paths where resulting CSVs files are now stored.
10. (optional) To check the presence of the CSV files for plotting, After completion, go to the Navigation Menu, select "Cloud Storage", and then "Buckets." Then go to the marketquake_results bucket, navigate to the CSVs/general directory and choose a CSV folder corresponding the displayed path in the terminal output. Click on the part and "Authenticated URL" to download it to your local device.
11. Return to the SSH browser session, copy the plotting command generated earlier (if applicable), paste and run in from the terminal. Wait for the plot generation to complete and display the path in the GCS where the plot has been saved.
12. To view the plot, go back to Google Storage, but this time navigate to marketquake_results/Plots/general.
13. Choose the plot you're interested in, click on its link, and then on "Authenticated URL." The plot should now be displayed in your browser window, and you can save it to your local device for further interpretation.

Here are some example commands you can generate:
- spark-submit main.py general Close all_markets daily_covid_deaths world World --py-files merge_by_group.py merge_all.py
- spark-submit main.py general AdjustedClose sp500 daily_covid_deaths country USA --py-files merge_by_group.py merge_all.py
- spark-submit main.py general Close all_sectors daily_covid_deaths regions Europe --py-files merge_by_group.py merge_all.py
- spark-submit main.py extremes Close all_markets daily_covid_deaths world World --py-files merge_by_group.py merge_all.py

etc...
