"""
run_pipeline.py

Script to run the full ETL pipeline from data fetch to report generation.

Usage:
    python run_pipeline.py
"""

import time
import os
import pandas as pd
from data_fetch import fetch_and_save_stock_data
from data_load import load_dataframes
from data_processing import process_data
from analysis import analyze_data
from visualization import plot_stock_price, plot_top_gainers_losers
from reporting import save_metrics_csv, export_plots_pdf, log_job_duration
from nse_tickers_nsetools import fetch_nse_tickers_nsetools

master_df = None
spark = None

from data_fetch import fetch_stock_data_online

from data_fetch import generate_mock_stock_data

def main(sampling_fraction=0.5, source="parquet", use_online_data=False, use_mock_data=False):
    global master_df, spark
    start_time = time.time()

    # Step 0: Fetch NSE tickers CSV
    print("Fetching NSE tickers list...")
    fetch_nse_tickers_nsetools()

    # Step 1: Fetch data - Removed data fetching as per new plan
    tickers_csv = "nse_tickers_nsetools.csv"
    if os.path.exists(tickers_csv):
        df_tickers = pd.read_csv(tickers_csv)
        tickers = [symbol + ".NS" for symbol in df_tickers['SYMBOL'].tolist()]
        print(f"Tickers CSV file found with {len(tickers)} tickers. Skipping data fetch step.")
    else:
        print(f"Tickers CSV file '{tickers_csv}' not found. Exiting.")
        return

    # Step 2: Load data
    print("Loading data into PySpark DataFrame...")
    if use_mock_data:
        print("Generating mock stock data for quick testing...")
        combined_df = generate_mock_stock_data(tickers=tickers, num_days=30)
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("StockDataLoadMock") \
            .master("local[*]") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.hadoop.io.native.lib.available", "false") \
            .getOrCreate()
        master_df = spark.createDataFrame(combined_df)
    elif use_online_data:
        print("Fetching stock data online from internet...")
        combined_df = fetch_stock_data_online(tickers)
        if combined_df.empty:
            print("No data fetched online. Exiting.")
            return
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("StockDataLoadOnline") \
            .master("local[*]") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.hadoop.io.native.lib.available", "false") \
            .getOrCreate()
        master_df = spark.createDataFrame(combined_df)
    else:
        if master_df is None:
            master_df, spark = load_dataframes(sampling_fraction=sampling_fraction, source=source)
        else:
            print("Using existing loaded DataFrame, skipping reload.")
            master_df, spark = load_dataframes(existing_df=master_df, sampling_fraction=sampling_fraction, source=source)
    if master_df is None:
        print("No data loaded. Exiting.")
        return

    # Step 3: Process data
    print("Processing data...")
    processed_df, metrics_df = process_data(master_df, spark)

    # Step 4: Analyze data
    print("Analyzing data...")
    summary_df, top_gainers_df, top_losers_df, daily_pct_change_df, buy_sell_signals_df = analyze_data(processed_df, spark)

    # Step 5: Visualization
    print("Generating visualizations...")
    sample_ticker = "INFY.NS"
    pdf = processed_df.filter(processed_df.Ticker == sample_ticker).select("Date", "Ticker", "Close").toPandas()
    plot_stock_price(pdf, sample_ticker)
    print(f"Line chart for {sample_ticker} generated and saved.")

    plot_top_gainers_losers(top_gainers_df, top_losers_df)
    print("Top gainers and losers bar charts generated and saved.")

    # New Step: AI Stock Suggestions
    print("Fetching AI stock suggestions...")
    from ai_integration import get_ai_stock_suggestions
    from visualization import plot_ai_stock_suggestions

    ai_suggestions = get_ai_stock_suggestions()
    plot_ai_stock_suggestions(ai_suggestions)
    print("AI stock suggestions chart generated and saved.")

    # Step 6: Reporting
    print("Generating reports...")
    csv_path = save_metrics_csv(metrics_df)
    plot_dir = "plots"
    plot_paths = [
        os.path.join(plot_dir, f"{sample_ticker}_price_line_chart.png"),
        os.path.join(plot_dir, "top_gainers_bar_chart.png"),
        os.path.join(plot_dir, "top_losers_bar_chart.png")
    ]
    pdf_path = export_plots_pdf(plot_paths)

    # Save top 5 stocks with buy/sell signals
    from reporting import save_top_stocks_with_signals
    save_top_stocks_with_signals(buy_sell_signals_df)

    end_time = time.time()
    log_job_duration(start_time, end_time)

    spark.stop()
    print("Pipeline completed successfully.")

if __name__ == "__main__":
    main()
