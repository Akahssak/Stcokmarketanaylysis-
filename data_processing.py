"""
data_processing.py

Script to clean missing data, calculate per-company metrics,
and classify stock trend as 'Bullish', 'Bearish', or 'Neutral' based on past N days.

Usage:
    python data_processing.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, stddev, when, lag
from pyspark.sql.window import Window

def process_data(master_df, spark, trend_days=7):
    """
    Process the master DataFrame:
    - Clean missing data
    - Calculate metrics per company
    - Classify stock trend based on past N days

    Returns:
    - processed_df: DataFrame with trend classification
    - metrics_df: DataFrame with per-company metrics
    """
    # Convert columns to appropriate types
    df = master_df.withColumn("Open", col("Open").cast("double")) \
                  .withColumn("High", col("High").cast("double")) \
                  .withColumn("Low", col("Low").cast("double")) \
                  .withColumn("Close", col("Close").cast("double")) \
                  .withColumn("Volume", col("Volume").cast("double")) \
                  .withColumn("Date", col("Date").cast("date"))

    # Drop rows with missing critical data
    df_clean = df.dropna(subset=["Date", "Close", "Volume"])

    # Calculate per-company metrics
    metrics_df = df_clean.groupBy("Ticker").agg(
        avg("Close").alias("avg_close"),
        max("High").alias("max_high"),
        min("Low").alias("min_low"),
        stddev("Volume").alias("stddev_volume")
    )

    # Define window for trend calculation
    window_spec = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(-trend_days, -1)

    # Calculate past N days average close price
    df_with_lag = df_clean.withColumn("past_avg_close", avg("Close").over(window_spec))

    # Classify trend
    df_trend = df_with_lag.withColumn(
        "Trend",
        when(col("Close") > col("past_avg_close"), "Bullish")
        .when(col("Close") < col("past_avg_close"), "Bearish")
        .otherwise("Neutral")
    )

    return df_trend, metrics_df

if __name__ == "__main__":
    from data_load import load_dataframes_from_csv

    master_df, spark = load_dataframes_from_csv()
    if master_df:
        processed_df, metrics_df = process_data(master_df, spark)
        print("Sample processed data with trend classification:")
        processed_df.select("Date", "Ticker", "Close", "past_avg_close", "Trend").show(10)
        print("Per-company metrics:")
        metrics_df.show()
    spark.stop()
