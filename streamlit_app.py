import streamlit as st
import os
from PIL import Image
import pandas as pd
from analysis import analyze_data
from data_load import load_dataframes_from_csv
from data_processing import process_data

# Set page config for Streamlit app
st.set_page_config(
    page_title="Stock Market Analytics Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for Groww-like colorful theme
st.markdown(
    """
    <style>
    /* Background gradient */
    .reportview-container {
        background: linear-gradient(135deg, #6a11cb 0%, #2575fc 100%);
        color: white;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    /* Sidebar style */
    .css-1d391kg {
        background: #1e1e2f;
        color: white;
    }
    /* Header style */
    .css-1v3fvcr h1 {
        color: #ffb400;
        font-weight: 700;
    }
    /* Widget labels */
    label, .css-1aumxhk {
        color: white;
        font-weight: 600;
    }
    /* Buttons */
    .stButton>button {
        background-color: #ffb400;
        color: #1e1e2f;
        font-weight: 700;
        border-radius: 8px;
        padding: 8px 16px;
        border: none;
        transition: background-color 0.3s ease;
    }
    .stButton>button:hover {
        background-color: #ffcc33;
        color: #1e1e2f;
    }
    /* Images */
    .element-container img {
        border-radius: 12px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Title and description
st.title("📈 Stock Market Analytics Dashboard")
st.markdown(
    """
    Welcome to the Stock Market Analytics Dashboard inspired by Groww's colorful UI.
    Explore stock price trends, top gainers and losers, and AI stock suggestions.
    """
)

# Sidebar for user inputs
st.sidebar.header("Filters")

# Load available tickers from plots directory by scanning plot filenames
def get_available_tickers(plots_dir="plots"):
    tickers = []
    if os.path.exists(plots_dir):
        for file in os.listdir(plots_dir):
            if file.endswith("_price_line_chart.png"):
                ticker = file.split("_price_line_chart.png")[0]
                tickers.append(ticker)
    return sorted(tickers)

available_tickers = get_available_tickers()

selected_ticker = st.sidebar.selectbox("Select Stock Ticker", available_tickers, index=available_tickers.index("INFY.NS") if "INFY.NS" in available_tickers else 0)

# Load and analyze data
master_df, spark = load_dataframes_from_csv()
if master_df is not None:
    processed_df, metrics_df = process_data(master_df, spark)
    summary_df, top_gainers_df, top_losers_df, daily_pct_change_df, buy_sell_signals_df = analyze_data(processed_df, spark)

    # Convert PySpark DataFrames to pandas for display
    top_gainers_pdf = top_gainers_df.toPandas()
    top_losers_pdf = top_losers_df.toPandas()
    buy_sell_signals_pdf = buy_sell_signals_df.toPandas()
else:
    top_gainers_pdf = pd.DataFrame()
    top_losers_pdf = pd.DataFrame()
    buy_sell_signals_pdf = pd.DataFrame()

# Display stock price line chart for selected ticker
st.subheader(f"Stock Price Over Time - {selected_ticker}")
price_chart_path = f"plots/{selected_ticker}_price_line_chart.png"
if os.path.exists(price_chart_path):
    image = Image.open(price_chart_path)
    st.image(image, use_container_width=True)
else:
    st.warning("Price chart not available for the selected ticker.")

# Display top gainers and losers tables side by side
st.subheader("Top Gainers and Losers - Last 7 Days")
col1, col2 = st.columns(2)

with col1:
    st.markdown("### Top Gainers")
    if not top_gainers_pdf.empty:
        st.dataframe(top_gainers_pdf.style.background_gradient(cmap='Greens'))
    else:
        st.warning("Top gainers data not available.")

with col2:
    st.markdown("### Top Losers")
    if not top_losers_pdf.empty:
        st.dataframe(top_losers_pdf.style.background_gradient(cmap='Reds'))
    else:
        st.warning("Top losers data not available.")

# Display buy/sell signals table
st.subheader("Top 5 Stocks Buy/Sell Signals")
if not buy_sell_signals_pdf.empty:
    st.dataframe(buy_sell_signals_pdf.style.applymap(lambda x: 'color: green;' if x == 'Buy' else ('color: red;' if x == 'Sell' else 'color: black;'), subset=['signal']))
else:
    st.info("Buy/Sell signals data not available.")

# Display AI stock suggestions if available
st.subheader("AI Suggested Stocks to Buy/Sell")
ai_suggestions_path = "plots/ai_stock_suggestions.png"
if os.path.exists(ai_suggestions_path):
    image = Image.open(ai_suggestions_path)
    st.image(image, use_container_width=True)
else:
    st.info("AI stock suggestions chart not available.")

# Footer
st.markdown(
    """
    <hr>
    <p style="text-align:center; color:#ffb400;">&copy; 2024 Stock Market Analytics Dashboard</p>
    """,
    unsafe_allow_html=True,
)
