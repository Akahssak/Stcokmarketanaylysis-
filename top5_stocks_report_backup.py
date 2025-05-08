import time
from data_load import load_dataframes
from data_processing import process_data
from analysis import analyze_data

def main():
    start_time = time.time()

    # Load data
    master_df, spark = load_dataframes_from_csv()
    if master_df is None:
        print("No data loaded. Exiting.")
        return

    # Process data
    processed_df, metrics_df = process_data(master_df, spark)

    # Analyze data
    summary_df, top_gainers_df, top_losers_df, daily_pct_change_df, buy_sell_signals_df = analyze_data(processed_df, spark)

    # Collect top 5 stocks with buy/sell signals
    top5_signals = buy_sell_signals_df.toPandas()

    print("Top 5 Stocks with Buy/Sell Signals:")
    print("-----------------------------------")
    for idx, row in top5_signals.iterrows():
        print(f"{idx+1}. Ticker: {row['Ticker']}")
        print(f"   Date: {row['latest_date']}")
        print(f"   Daily % Change: {row['daily_pct_change']:.2f}%")
        print(f"   Signal: {row['signal']}")
        print()

    end_time = time.time()
    print(f"Analysis completed in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
