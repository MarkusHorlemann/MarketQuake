# MarketQuake: Decoding the Pandemic Punch on Global Economy

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

## Description of the Need for Big Data Processing

1. **Data Volume:** Processing over 20 GB of data distributed over extended periods requires Big Data processing due to its scale.
2. **Data Variety:** Combining, cleansing, and unifying two differently structured datasets with numerous correlated tables poses a challenge to conventional data processing methods.
3. **Limited Local Storage:** Utilizing cost-effective Cloud-based solutions is essential for storing the large dataset.

## Description of Needed Tools and Infrastructures

- **Github Repository:** Code base, version control, collaboration, backup management.
- **Python (Numpy / Pandas / PySpark / Matplotlib):** Programming logic and statistical analysis.
- **Google Cloud Platform (GCS, Dataproc, BigQuery):** Large data storage, parallel processing, efficient data querying, resource management, computation outsourcing.
- **Google Tables, Excel & Microsoft Power BI:** Visualization.
- **Google Sites:** Project webpage.

## Try it yourself
To replicate our analysis, refer to the following sections for setting up the environment, accessing the datasets, and executing the code.

### Prerequisites
TBA (Ensure you have the necessary software and accounts set up, including access to Google Cloud Platform.)

### Data Acccess
TBA

### Running the Code
TBA

## Results
The results of the analysis will be documented in the /results directory. For detailed insights, visualizations, and interpretations, visit our project webpage.
