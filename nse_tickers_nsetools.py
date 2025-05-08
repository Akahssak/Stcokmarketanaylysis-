from nsetools import Nse
import pandas as pd

def fetch_nse_tickers_nsetools(output_file="nse_tickers_nsetools.csv"):
    """
    Fetch NSE tickers using nsetools and save to CSV.
    """
    nse = Nse()
    all_stock_codes = nse.get_stock_codes()
    
    # Check if all_stock_codes is a list instead of dict
    if isinstance(all_stock_codes, list):
        # Convert list to dict assuming list of dicts with 'symbol' and 'name' keys or similar structure
        # This is a generic approach; adjust if structure is different
        try:
            all_stock_codes_dict = {}
            for item in all_stock_codes:
                # If item is dict with keys 'symbol' and 'name'
                if isinstance(item, dict) and 'symbol' in item and 'name' in item:
                    all_stock_codes_dict[item['symbol']] = item['name']
                else:
                    # If item is just a symbol string, map symbol to symbol as name
                    all_stock_codes_dict[item] = item
            all_stock_codes = all_stock_codes_dict
        except Exception:
            # Fallback: create dict with symbol as key and symbol as value
            all_stock_codes = {str(sym): str(sym) for sym in all_stock_codes}
    
    # Remove the first entry which is 'SYMBOL' header if present
    if 'SYMBOL' in all_stock_codes:
        del all_stock_codes['SYMBOL']

    symbols = list(all_stock_codes.keys())
    df = pd.DataFrame(symbols, columns=["SYMBOL"])
    df.to_csv(output_file, index=False)
    print(f"Saved NSE tickers list to {output_file}")
    print(f"Total tickers fetched: {len(df)}")
    return df

if __name__ == "__main__":
    fetch_nse_tickers_nsetools()
