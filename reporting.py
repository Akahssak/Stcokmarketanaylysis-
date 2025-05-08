"""
reporting.py

Script to save key metrics as CSV, export plots and summaries as PDF,
and include PySpark job duration using time module.

Usage:
    python reporting.py
"""

import os
import time
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

def save_metrics_csv(metrics_df, output_dir="reports"):
    """
    Save metrics DataFrame as CSV.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    csv_path = os.path.join(output_dir, "key_metrics.csv")
    metrics_df.toPandas().to_csv(csv_path, index=False)
    print(f"Saved key metrics CSV at {csv_path}")
    return csv_path

def export_plots_pdf(plot_paths, output_dir="reports"):
    """
    Export given plot image files into a single PDF.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    pdf_path = os.path.join(output_dir, "stock_report.pdf")
    with PdfPages(pdf_path) as pdf:
        for plot_path in plot_paths:
            fig = plt.figure()
            img = plt.imread(plot_path)
            plt.imshow(img)
            plt.axis('off')
            pdf.savefig(fig)
            plt.close()
    print(f"Exported plots to PDF at {pdf_path}")
    return pdf_path

def log_job_duration(start_time, end_time, output_dir="reports"):
    """
    Log the PySpark job duration in seconds to a text file.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    duration = end_time - start_time
    log_path = os.path.join(output_dir, "job_duration.txt")
    with open(log_path, "w") as f:
        f.write(f"PySpark job duration: {duration:.2f} seconds\n")
    print(f"Logged job duration at {log_path}")
    return log_path

def save_top_stocks_with_signals(signals_df, output_dir="reports"):
    """
    Save the top 5 stocks with buy/sell signals to a CSV file.

    Parameters:
    - signals_df: PySpark DataFrame with columns: Ticker, latest_date, daily_pct_change, signal
    - output_dir: Directory to save the CSV file

    Returns:
    - Path to the saved CSV file
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    csv_path = os.path.join(output_dir, "top_5_stocks_buy_sell_signals.csv")
    pdf = signals_df.toPandas()
    pdf.to_csv(csv_path, index=False)
    print(f"Saved top 5 stocks buy/sell signals CSV at {csv_path}")
    return csv_path

if __name__ == "__main__":
    import sys
    from data_load import load_dataframes_from_csv
    from data_processing import process_data
    from analysis import analyze_data
    from visualization import plot_stock_price, plot_top_gainers_losers

    start_time = time.time()

    master_df, spark = load_dataframes_from_csv()
    if master_df:
        processed_df, metrics_df = process_data(master_df, spark)
        summary_df, top_gainers_df, top_losers_df, daily_pct_change_df = analyze_data(processed_df, spark)

        # Save metrics CSV
        csv_path = save_metrics_csv(metrics_df)

        # Generate plots
        sample_ticker = "INFY.NS"
        pdf = processed_df.select("Date", "Ticker", "Close").toPandas()
        plot_stock_price(pdf, sample_ticker)
        plot_top_gainers_losers(top_gainers_df, top_losers_df)

        # Collect plot paths
        plot_dir = "plots"
        plot_paths = [
            os.path.join(plot_dir, f"{sample_ticker}_price_line_chart.png"),
            os.path.join(plot_dir, "top_gainers_bar_chart.png"),
            os.path.join(plot_dir, "top_losers_bar_chart.png")
        ]

        # Export plots to PDF
        pdf_path = export_plots_pdf(plot_paths)

    end_time = time.time()
    log_job_duration(start_time, end_time)

    spark.stop()
