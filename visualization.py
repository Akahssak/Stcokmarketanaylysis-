"""
visualization.py

Script to create visualizations using matplotlib and seaborn:
- Line chart for stock price over time
- Bar chart for top gainers/losers
- Sector-wise heatmap (assuming sector data available)

Usage:
    python visualization.py
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot_stock_price(df, ticker, output_dir="plots"):
    """
    Plot line chart of stock closing price over time for a given ticker.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    df_ticker = df[df['Ticker'] == ticker].sort_values('Date')
    plt.figure(figsize=(10, 6))
    plt.plot(df_ticker['Date'], df_ticker['Close'], label=f"{ticker} Close Price")
    plt.xlabel("Date")
    plt.ylabel("Close Price")
    plt.title(f"Stock Price Over Time - {ticker}")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    file_path = os.path.join(output_dir, f"{ticker}_price_line_chart.png")
    plt.savefig(file_path)
    plt.close()
    print(f"Saved line chart for {ticker} at {file_path}")

def plot_top_gainers_losers(top_gainers_df, top_losers_df, output_dir="plots"):
    """
    Plot bar charts for top gainers and losers.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Convert to pandas
    gainers = top_gainers_df.select("Ticker", "pct_change").toPandas()
    losers = top_losers_df.select("Ticker", "pct_change").toPandas()

    plt.figure(figsize=(12, 6))
    sns.barplot(x="Ticker", y="pct_change", data=gainers, palette="Greens_r")
    plt.title("Top Gainers - Last 7 Days")
    plt.ylabel("Percentage Change (%)")
    plt.xlabel("Ticker")
    plt.xticks(rotation=45)
    plt.tight_layout()
    gainers_path = os.path.join(output_dir, "top_gainers_bar_chart.png")
    plt.savefig(gainers_path)
    plt.close()
    print(f"Saved top gainers bar chart at {gainers_path}")

    plt.figure(figsize=(12, 6))
    sns.barplot(x="Ticker", y="pct_change", data=losers, palette="Reds_r")
    plt.title("Top Losers - Last 7 Days")
    plt.ylabel("Percentage Change (%)")
    plt.xlabel("Ticker")
    plt.xticks(rotation=45)
    plt.tight_layout()
    losers_path = os.path.join(output_dir, "top_losers_bar_chart.png")
    plt.savefig(losers_path)
    plt.close()
    print(f"Saved top losers bar chart at {losers_path}")

def plot_sector_heatmap(sector_df, output_dir="plots"):
    """
    Plot sector-wise heatmap of average percentage change.
    sector_df should have columns: ['Sector', 'avg_pct_change']
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    df = sector_df.toPandas()
    df_pivot = df.pivot("Sector", "Sector", "avg_pct_change")  # Pivot for heatmap

    plt.figure(figsize=(10, 8))
    sns.heatmap(df_pivot, annot=True, cmap="coolwarm", center=0)
    plt.title("Sector-wise Average Percentage Change Heatmap")
    plt.tight_layout()
    heatmap_path = os.path.join(output_dir, "sector_heatmap.png")
    plt.savefig(heatmap_path)
    plt.close()
    print(f"Saved sector heatmap at {heatmap_path}")

def plot_ai_stock_suggestions(suggestions, output_dir="plots"):
    """
    Plot AI stock suggestions for top 5 stocks to buy/sell for long and short term.
    suggestions: dict with keys 'long_term_buy', 'long_term_sell', 'short_term_buy', 'short_term_sell'
                 each value is a list of stock tickers.
    """
    import numpy as np

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    categories = ['Long Term Buy', 'Long Term Sell', 'Short Term Buy', 'Short Term Sell']
    data = [
        len(suggestions.get('long_term_buy', [])),
        len(suggestions.get('long_term_sell', [])),
        len(suggestions.get('short_term_buy', [])),
        len(suggestions.get('short_term_sell', []))
    ]

    # For better visualization, we will plot counts and list tickers as labels
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(categories, data, color=['green', 'red', 'green', 'red'])

    # Add ticker labels on top of bars
    for i, cat in enumerate(categories):
        tickers = suggestions.get(cat.lower().replace(' ', '_'), [])
        label = "\n".join(tickers)
        ax.text(i, data[i] + 0.1, label, ha='center', va='bottom', fontsize=8)

    ax.set_ylabel('Number of Stocks')
    ax.set_title('AI Suggested Stocks to Buy/Sell (Long and Short Term)')
    plt.tight_layout()
    file_path = os.path.join(output_dir, "ai_stock_suggestions.png")
    plt.savefig(file_path)
    plt.close()
    print(f"Saved AI stock suggestions chart at {file_path}")

if __name__ == "__main__":
    import sys
    from analysis import analyze_data
    from data_load import load_dataframes_from_csv
    from data_processing import process_data

    master_df, spark = load_dataframes_from_csv()
    if master_df:
        processed_df, _ = process_data(master_df, spark)
        summary_df, top_gainers_df, top_losers_df, daily_pct_change_df = analyze_data(processed_df, spark)

        # Convert processed_df to pandas for plotting stock price of a sample ticker
        sample_ticker = "INFY.NS"
        pdf = processed_df.select("Date", "Ticker", "Close").toPandas()
        plot_stock_price(pdf, sample_ticker)

        plot_top_gainers_losers(top_gainers_df, top_losers_df)

        # Sector data is not available in current dataset, so skipping sector heatmap

    spark.stop()
