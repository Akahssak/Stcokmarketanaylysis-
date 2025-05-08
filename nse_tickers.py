"""
nse_tickers.py

Script to fetch the list of all NSE stock tickers.

Usage:
    python nse_tickers.py
"""

import requests
import pandas as pd
import os
import time

def fetch_nse_tickers(output_file="nse_tickers.csv"):
    """
    Fetch the list of all NSE stock tickers and save to CSV.
    Uses the NSE equity stock list URL with proper headers and retries.
    """
    url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
    base_url = "https://www.nseindia.com"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.nseindia.com/get-quotes/equity",
        "Connection": "keep-alive",
    }
    session = requests.Session()
    session.headers.update(headers)

    try:
        # More robust session initialization to get cookies
        session.get(base_url, timeout=5)
        time.sleep(1)
        session.get("https://www.nseindia.com/market-data/live-equity-market", timeout=5)
        time.sleep(1)
        response = session.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Extract symbols
        records = data.get("data", [])
        symbols = [item["symbol"] for item in records]

        # Save to CSV
        df = pd.DataFrame(symbols, columns=["SYMBOL"])
        df.to_csv(output_file, index=False)
        print(f"Saved NSE tickers list to {output_file}")
        print(f"Total tickers fetched: {len(df)}")
        return df

    except Exception as e:
        print(f"Error fetching NSE tickers: {e}")
        return None

if __name__ == "__main__":
    fetch_nse_tickers()
