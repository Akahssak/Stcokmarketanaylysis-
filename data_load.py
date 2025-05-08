import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def load_dataframes(data_dir="data", existing_df=None, cache_path="data/cached_master_df.parquet", sampling_fraction=0.5, source="parquet", tickers=None):
    """
    Load stock data into PySpark DataFrame from specified source ('parquet' or 'csv').
    If existing_df is provided, return it immediately with the Spark session.
    If source is 'parquet' and cache exists, load from cache_path.
    If source is 'csv' or parquet load fails, load from CSV files.

    Parameters:
    - sampling_fraction: float, fraction of data to sample (default 0.5)
    - source: str, data source type: 'parquet' or 'csv' (default 'parquet')
    - tickers: list of str, stock tickers to load (default None means load all)

    Returns:
    - master_df: PySpark DataFrame containing all data with unified schema.
    """
    spark = SparkSession.builder \
        .appName("StockDataLoad") \
        .master("local[*]") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .getOrCreate()

    if existing_df is not None:
        print("Existing DataFrame provided, skipping data loading.")
        return existing_df, spark

    if source == "parquet" and os.path.exists(cache_path):
        print(f"Loading cached DataFrame from {cache_path}")
        try:
            master_df = spark.read.parquet(cache_path)
            return master_df, spark
        except Exception as e:
            print(f"Failed to load parquet cache due to: {e}")
            print("Falling back to loading CSV files.")
            source = "csv"

    if source == "csv":
        csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
        if tickers is not None:
            # Filter csv_files to only those matching tickers
            ticker_set = set(tickers)
            csv_files = [f for f in csv_files if f.replace(".csv", "") in ticker_set]
        if not csv_files:
            print(f"No CSV files found in {data_dir} for selected tickers.")
            spark.stop()
            return None, spark

        master_df = None
        expected_cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker']
        for file in csv_files:
            file_path = os.path.join(data_dir, file)
            df = spark.read.option("header", True).csv(file_path)
            print(f"Loaded file {file} into PySpark DataFrame with {df.count()} rows.")
            # Ensure columns are as expected and add missing columns if needed
            for col in expected_cols:
                if col not in df.columns:
                    if col == 'Ticker':
                        ticker = file.replace(".csv", "").replace("_", ".")
                        df = df.withColumn("Ticker", lit(ticker))
                    else:
                        df = df.withColumn(col, lit(None))
            df = df.select(expected_cols)
            if master_df is None:
                master_df = df
            else:
                master_df = master_df.unionByName(df)

        # Reduce data size using sample with given fraction
        master_df = master_df.sample(fraction=sampling_fraction, seed=42)

        # Delete cache directory before writing to avoid Windows file locking issues
        if os.path.exists(cache_path):
            try:
                if os.path.isdir(cache_path):
                    shutil.rmtree(cache_path)
                else:
                    os.remove(cache_path)
                print(f"Deleted existing cache at {cache_path} before writing new cache.")
            except Exception as e:
                print(f"Failed to delete existing cache due to: {e}")

        # Cache the sampled DataFrame to Parquet for future runs with error handling
        try:
            master_df.write.mode("overwrite").parquet(cache_path)
            print(f"Cached sampled DataFrame to {cache_path}")
        except Exception as e:
            print(f"Failed to cache DataFrame to parquet due to: {e}")
            print("Skipping caching to avoid errors.")

        print(f"Loaded and combined selected CSV files into master DataFrame with {sampling_fraction*100}% sampling.")
        return master_df, spark

    print(f"Unsupported source '{source}'. Returning None.")
    spark.stop()
    return None, spark
