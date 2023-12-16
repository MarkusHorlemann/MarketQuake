# MarketQuake: Decoding the Pandemic Punch on Global Economy

---
**NOTE:** Visit our website for more information and better experience!

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

The results of the analysis will provide insights into stock performance, volatility, and potential investment strategies, contributing to the understanding of the economic implications of the global health crisis.

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

Refer to our project website for more details.

### Running the Code
TBA

## Results
The results of the analysis will be documented in the /results directory. For detailed insights, visualizations, and interpretations, visit our project webpage.
