import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def load_dataframes(data_dir="data", tickers=None):
    """
    Load stock data into a single pandas DataFrame from CSV files in data_dir.
    If tickers is provided, only load CSV files matching those tickers.

    Parameters:
    - data_dir: str, directory containing CSV files
    - tickers: list of str, stock tickers to load (default None means load all)

    Returns:
    - master_df: pandas DataFrame containing all data combined.
    """
    csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
    if tickers is not None:
        ticker_set = set(tickers)
        csv_files = [f for f in csv_files if f.replace(".csv", "") in ticker_set]
    if not csv_files:
        print(f"No CSV files found in {data_dir} for selected tickers.")
        return None

    dfs = []
    for file in csv_files:
        file_path = os.path.join(data_dir, file)
        df = pd.read_csv(file_path)
        # Add Ticker column if missing
        if 'Ticker' not in df.columns:
            ticker = file.replace(".csv", "").replace("_", ".")
            df['Ticker'] = ticker
        dfs.append(df)

    if dfs:
        master_df = pd.concat(dfs, ignore_index=True)
        return master_df
    else:
        return None

def load_dataframes_spark(data_dir="data", tickers=None):
    """
    Load stock data into a single Spark DataFrame from CSV files in data_dir.
    If tickers is provided, only load CSV files matching those tickers.

    Returns:
    - master_df: Spark DataFrame containing all data combined.
    - spark: SparkSession object
    """
    spark = SparkSession.builder \
        .appName("StockDataLoad") \
        .master("local[*]") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .getOrCreate()

    csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
    if tickers is not None:
        ticker_set = set(tickers)
        csv_files = [f for f in csv_files if f.replace(".csv", "") in ticker_set]
    if not csv_files:
        print(f"No CSV files found in {data_dir} for selected tickers.")
        spark.stop()
        return None, None

    dfs = []
    for file in csv_files:
        file_path = os.path.join(data_dir, file)
        df = spark.read.option("header", "true").csv(file_path)
        # Add Ticker column if missing
        if 'Ticker' not in df.columns:
            ticker = file.replace(".csv", "").replace("_", ".")
            df = df.withColumn("Ticker", lit(ticker))
        dfs.append(df)

    if dfs:
        master_df = dfs[0]
        for df in dfs[1:]:
            master_df = master_df.unionByName(df)
        return master_df, spark
    else:
        spark.stop()
        return None, None
