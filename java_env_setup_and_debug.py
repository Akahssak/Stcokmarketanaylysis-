import os

def check_java_home():
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        print(f"JAVA_HOME is set to: {java_home}")
    else:
        print("JAVA_HOME is not set. Please set it to your JDK installation path.")

def print_loaded_csv_files(data_dir="data", tickers=None):
    import os
    csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
    print(f"All CSV files in {data_dir}: {csv_files}")
    if tickers is not None:
        ticker_set = set(tickers)
        filtered_files = [f for f in csv_files if f.replace(".csv", "") in ticker_set]
        print(f"Filtered CSV files for tickers {tickers}: {filtered_files}")
    else:
        print("No tickers specified, loading all CSV files.")

if __name__ == "__main__":
    check_java_home()
    # Example tickers to test filtering
    example_tickers = ["INFY_NS", "TCS_NS"]
    print_loaded_csv_files(tickers=example_tickers)
