"""
data_fetch.py

Script to fetch and download historical stock data for multiple companies using yfinance.
Saves each company's data as a CSV in the data/ directory.

Also includes a mock data generator for quick testing.

Usage:
    python data_fetch.py
"""

import os
import yfinance as yf
import pandas as pd
import numpy as np
from nse_tickers import fetch_nse_tickers

def fetch_and_save_stock_data(tickers, start_date="2010-01-01", end_date=None, data_dir="data"):
    """
    Fetch historical stock data for given tickers and save as CSV files.

    Parameters:
    - tickers: list of stock ticker symbols (e.g., ['INFY.NS', 'TCS.NS'])
    - start_date: start date for historical data (YYYY-MM-DD)
    - end_date: end date for historical data (YYYY-MM-DD), default None means today
    - data_dir: directory to save CSV files
    """
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date)
        if hist.empty:
            print(f"No data found for {ticker}. Skipping.")
            continue
        # Reset index to have Date as a column
        hist.reset_index(inplace=True)
        # Add Ticker column
        hist['Ticker'] = ticker
        # Save to CSV
        file_path = os.path.join(data_dir, f"{ticker.replace('.', '_')}.csv")
        hist.to_csv(file_path, index=False)
        print(f"Saved data to {file_path}")

def fetch_stock_data_online(tickers, start_date="2010-01-01", end_date=None):
    """
    Fetch historical stock data for given tickers from the internet and return combined DataFrame.

    Parameters:
    - tickers: list of stock ticker symbols (e.g., ['INFY.NS', 'TCS.NS'])
    - start_date: start date for historical data (YYYY-MM-DD)
    - end_date: end date for historical data (YYYY-MM-DD), default None means today

    Returns:
    - combined_df: pandas DataFrame containing concatenated data for all tickers
    """
    all_data = []
    for ticker in tickers:
        print(f"Fetching data for {ticker} from internet...")
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date)
        if hist.empty:
            print(f"No data found for {ticker}. Skipping.")
            continue
        hist.reset_index(inplace=True)
        hist['Ticker'] = ticker
        all_data.append(hist)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"Fetched data for {len(all_data)} tickers from internet.")
        return combined_df
    else:
        print("No data fetched from internet.")
        return pd.DataFrame()

def generate_mock_stock_data(tickers=None, num_days=30):
    """
    Generate mock stock data for given tickers for quick testing.

    Parameters:
    - tickers: list of stock ticker symbols (e.g., ['INFY.NS', 'TCS.NS'])
               If None, defaults to ['MOCK1', 'MOCK2', 'MOCK3']
    - num_days: number of days of data to generate

    Returns:
    - combined_df: pandas DataFrame with mock stock data
    """
    if tickers is None:
        tickers = ['MOCK1', 'MOCK2', 'MOCK3']

    date_range = pd.date_range(end=pd.Timestamp.today(), periods=num_days)
    all_data = []

    for ticker in tickers:
        np.random.seed(hash(ticker) % 2**32)
        prices = np.cumprod(1 + np.random.normal(0, 0.01, size=num_days)) * 100
        volumes = np.random.randint(1000, 10000, size=num_days)
        df = pd.DataFrame({
            'Date': date_range,
            'Open': prices * np.random.uniform(0.95, 1.05, size=num_days),
            'High': prices * np.random.uniform(1.00, 1.10, size=num_days),
            'Low': prices * np.random.uniform(0.90, 1.00, size=num_days),
            'Close': prices,
            'Volume': volumes,
            'Ticker': ticker
        })
        all_data.append(df)

    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"Generated mock stock data for {len(tickers)} tickers and {num_days} days.")
    return combined_df

if __name__ == "__main__":
    # Fetch NSE tickers list
    df_tickers = fetch_nse_tickers()
    if df_tickers is not None:
        # Extract Symbol column and append .NS suffix for Yahoo Finance
        tickers = [symbol + ".NS" for symbol in df_tickers['SYMBOL'].tolist()]
        print(f"Total tickers to fetch: {len(tickers)}")
        fetch_and_save_stock_data(tickers)
    else:
        print("Failed to fetch NSE tickers list. Exiting.")
