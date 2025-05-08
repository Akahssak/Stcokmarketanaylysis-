"""
analysis.py

Script to perform analysis and generate insights on stock market data.

Functions:
- analyze_data: Analyze processed stock data and generate summary statistics,
  top gainers, top losers, and daily percentage change data.

Usage:
    Import analyze_data and call with processed DataFrame and Spark session.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def analyze_data(processed_df, spark):
    """
    Analyze processed stock data.

    Parameters:
    - processed_df: PySpark DataFrame with processed stock data. Expected columns:
        ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume', ...]
    - spark: SparkSession object

    Returns:
    - summary_df: DataFrame with summary statistics per ticker
    - top_gainers_df: DataFrame with top gainers based on daily percentage change
    - top_losers_df: DataFrame with top losers based on daily percentage change
    - daily_pct_change_df: DataFrame with daily percentage change per ticker
    - buy_sell_signals_df: DataFrame with buy/sell signals for top 5 stocks
    """

    # Calculate daily percentage change per ticker
    window_spec = Window.partitionBy("Ticker").orderBy("Date")
    daily_pct_change_df = processed_df.withColumn(
        "prev_close",
        F.lag("Close").over(window_spec)
    ).withColumn(
        "daily_pct_change",
        (F.col("Close") - F.col("prev_close")) / F.col("prev_close") * 100
    ).na.drop(subset=["daily_pct_change"])

    # Summary statistics per ticker
    summary_df = processed_df.groupBy("Ticker").agg(
        F.min("Date").alias("start_date"),
        F.max("Date").alias("end_date"),
        F.count("Date").alias("num_days"),
        F.avg("Close").alias("avg_close"),
        F.min("Close").alias("min_close"),
        F.max("Close").alias("max_close"),
        F.avg("Volume").alias("avg_volume")
    )

    # Get latest date's daily_pct_change for each ticker
    latest_date_df = daily_pct_change_df.groupBy("Ticker").agg(
        F.max("Date").alias("latest_date")
    ).alias("latest_date_df")

    daily_pct_change_df_alias = daily_pct_change_df.alias("daily_pct_change_df")

    latest_pct_change_df = daily_pct_change_df_alias.join(
        latest_date_df,
        (daily_pct_change_df_alias.Ticker == latest_date_df.Ticker) &
        (daily_pct_change_df_alias.Date == latest_date_df.latest_date)
    ).select(
        daily_pct_change_df_alias.Ticker,
        daily_pct_change_df_alias.daily_pct_change
    )

    # Top gainers: sort descending by daily_pct_change
    top_gainers_df = latest_pct_change_df.orderBy(F.desc("daily_pct_change")).limit(10)

    # Top losers: sort ascending by daily_pct_change
    top_losers_df = latest_pct_change_df.orderBy("daily_pct_change").limit(10)

    # Generate buy/sell signals for top 5 stocks
    buy_sell_signals_df = generate_buy_sell_signals(processed_df, top_gainers_df.limit(5), spark)

    return summary_df, top_gainers_df, top_losers_df, daily_pct_change_df, buy_sell_signals_df

def generate_buy_sell_signals(processed_df, top_stocks_df, spark):
    """
    Generate buy/sell signals for given top stocks using a simple heuristic:
    - If the latest daily_pct_change > 0, signal "Buy"
    - If the latest daily_pct_change < 0, signal "Sell"
    - Otherwise, signal "Hold"

    Parameters:
    - processed_df: PySpark DataFrame with processed stock data
    - top_stocks_df: PySpark DataFrame with top stocks (Ticker column)
    - spark: SparkSession object

    Returns:
    - DataFrame with columns: Ticker, latest_date, daily_pct_change, signal
    """
    from pyspark.sql.functions import col, lit, when, max as spark_max

    # Get latest date per ticker
    latest_date_df = processed_df.groupBy("Ticker").agg(spark_max("Date").alias("latest_date"))

    # Join to get latest close price and previous close price
    window_spec = Window.partitionBy("Ticker").orderBy("Date")
    daily_pct_change_df = processed_df.withColumn(
        "prev_close",
        F.lag("Close").over(window_spec)
    ).withColumn(
        "daily_pct_change",
        (F.col("Close") - F.col("prev_close")) / F.col("prev_close") * 100
    ).na.drop(subset=["daily_pct_change"])

    # Filter for top stocks only
    top_tickers = [row.Ticker for row in top_stocks_df.select("Ticker").collect()]
    filtered_df = daily_pct_change_df.filter(col("Ticker").isin(top_tickers))

    # Get latest date's daily_pct_change for each ticker
    latest_pct_change_df = filtered_df.join(
        latest_date_df,
        (filtered_df.Ticker == latest_date_df.Ticker) &
        (filtered_df.Date == latest_date_df.latest_date)
    ).select(
        filtered_df.Ticker,
        filtered_df.Date.alias("latest_date"),
        filtered_df.daily_pct_change
    )

    # Generate signals
    signals_df = latest_pct_change_df.withColumn(
        "signal",
        when(col("daily_pct_change") > 0, lit("Buy"))
        .when(col("daily_pct_change") < 0, lit("Sell"))
        .otherwise(lit("Hold"))
    )

    return signals_df
