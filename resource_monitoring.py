import matplotlib.pyplot as plt
from pyspark.sql import Row
import psutil
import time
from google.cloud import storage

import threading


def monitor_during_execution(spark, func, args, interval_seconds=1, duration_seconds=10):
    resource_usage_data = []

    # Define a function that will run the analysis
    def run_analysis():
        func(*args)

    # Start the analysis function in a separate thread
    analysis_thread = threading.Thread(target=run_analysis)
    analysis_thread.start()

    start_time = time.time()
    while analysis_thread.is_alive() and time.time() - start_time < duration_seconds:
        cpu_usage = psutil.cpu_percent(interval=interval_seconds)
        memory_usage = psutil.virtual_memory().percent

        # Storing the data in a list of Row objects
        resource_usage_data.append(Row(time=time.time() - start_time, cpu=cpu_usage, memory=memory_usage))

    # Wait for the analysis thread to finish if it's still running
    analysis_thread.join()

    # Creating a DataFrame from the list of Row objects
    return spark.createDataFrame(resource_usage_data)


RESULTS = "gs://marketquake_results"


def plot_resource_usage(df, RESULTS, write_path):
    """
    Plot the CPU and Memory usage over time using a DataFrame and upload the plot to Google Cloud Storage.

    :param df: DataFrame containing resource usage data
    :param RESULTS: The GCS bucket path where the plot will be stored
    :param write_path: The path within the GCS bucket where the plot will be saved
    """
    # Convert Spark DataFrame to Pandas DataFrame for plotting
    pandas_df = df.toPandas()

    # Plotting
    plt.figure(figsize=(10, 6))

    # CPU Usage
    plt.subplot(2, 1, 1)
    plt.plot(pandas_df['time'], pandas_df['cpu'], label='CPU Usage (%)', color='blue')
    plt.xlabel('Time (seconds)')
    plt.ylabel('CPU Usage (%)')
    plt.title('CPU Usage Over Time')
    plt.grid(True)

    # Memory Usage
    plt.subplot(2, 1, 2)
    plt.plot(pandas_df['time'], pandas_df['memory'], label='Memory Usage (%)', color='red')
    plt.xlabel('Time (seconds)')
    plt.ylabel('Memory Usage (%)')
    plt.title('Memory Usage Over Time')
    plt.grid(True)

    plt.tight_layout()

    # Save the plot locally
    local_file = 'plot.png'
    plt.savefig(local_file)
    print(f"Plot saved locally as {local_file}")

    # Upload the local file to GCS
    client = storage.Client()
    bucket_name = RESULTS.replace('gs://', '')
    blob = client.get_bucket(bucket_name).blob(write_path)
    blob.upload_from_filename(local_file)
    print(f"Plot uploaded to {RESULTS}/{write_path}")
