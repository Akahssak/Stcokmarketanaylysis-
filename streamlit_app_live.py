import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time

# Import project-specific modules for data loading and analysis
from data_load import load_dataframes_from_csv
from data_processing import process_data
from analysis import analyze_data

# Set page config for Streamlit app
st.set_page_config(
    page_title="Live Stock Suggestions with Candlestick Charts",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for Groww-like colorful theme and fonts
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
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    /* Widget labels */
    label, .css-1aumxhk {
        color: white;
        font-weight: 600;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
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
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    .stButton>button:hover {
        background-color: #ffcc33;
        color: #1e1e2f;
    }
    /* Tables */
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    .dataframe tbody tr th {
        vertical-align: top;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    .dataframe thead th {
        text-align: left;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    /* Stock name styling */
    .stock-name {
        font-size: 1.2em;
        font-weight: 700;
        color: #ffb400;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("📈 Live Stock Suggestions with Candlestick Charts")
st.markdown(
    """
    This dashboard provides stock suggestions with buy/sell signals and candlestick charts based on local CSV data.
    """
)

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time

# Import project-specific modules for data loading and analysis
from data_load import load_dataframes_from_csv
from data_processing import process_data
from analysis import analyze_data

# Set page config for Streamlit app
st.set_page_config(
    page_title="Live Stock Suggestions with Candlestick Charts",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for Groww-like colorful theme and fonts
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
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    /* Widget labels */
    label, .css-1aumxhk {
        color: white;
        font-weight: 600;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
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
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    .stButton>button:hover {
        background-color: #ffcc33;
        color: #1e1e2f;
    }
    /* Tables */
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    .dataframe tbody tr th {
        vertical-align: top;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    .dataframe thead th {
        text-align: left;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    /* Stock name styling */
    .stock-name {
        font-size: 1.2em;
        font-weight: 700;
        color: #ffb400;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("📈 Live Stock Suggestions with Candlestick Charts")
st.markdown(
    """
    This dashboard provides stock suggestions with buy/sell signals and candlestick charts based on local CSV data.
    """
)

@st.cache_data(show_spinner=True)
def load_and_process_data(selected_tickers):
    master_df, spark = load_dataframes_from_csv(selected_tickers)
    if master_df is None:
        return None, None, None
    processed_df, metrics_df = process_data(master_df, spark)
    summary_df, top_gainers_df, top_losers_df, daily_pct_change_df, buy_sell_signals_df = analyze_data(processed_df, spark)
    return processed_df, buy_sell_signals_df, top_gainers_df

# Get list of all available tickers from data files
import os
data_dir = "data"
all_files = os.listdir(data_dir)
all_tickers = [f.replace(".csv", "") for f in all_files if f.endswith(".csv")]

# Sidebar: select tickers to analyze (limit to 20 for performance)
selected_tickers = st.sidebar.multiselect(
    "Select up to 20 stocks to analyze",
    options=all_tickers,
    default=all_tickers[:20],
    max_selections=20,
)

if st.button("Load and Analyze Stock Data"):
    with st.spinner("Loading and analyzing data..."):
        processed_df, buy_sell_signals_df, top_gainers_df = load_and_process_data(selected_tickers)
        if processed_df is None:
            st.error("No data loaded. Please check your data files.")
        else:
            top5_signals = buy_sell_signals_df.toPandas()

            st.subheader("Top 5 Stocks with Buy/Sell Signals")
            for idx, row in top5_signals.iterrows():
                st.markdown(f"**{idx+1}. Ticker:** {row['Ticker']}")
                st.markdown(f"   - Date: {row['latest_date']}")
                st.markdown(f"   - Daily % Change: {row['daily_pct_change']:.2f}%")
                st.markdown(f"   - Signal: {row['signal']}")
                st.markdown("---")

# Sidebar to select stocks to display candlestick charts and signals
selected_stocks = st.sidebar.multiselect(
    "Select stocks to view candlestick charts and signals",
    options=[],
    default=[],
)

# Display candlestick charts with buy/sell signals from CSV data
if selected_stocks and 'processed_df' in locals() and 'buy_sell_signals_df' in locals():
    for ticker in selected_stocks:
        st.subheader(f"{ticker}")
        # Filter processed data for the selected ticker
        filtered_df = processed_df.filter(processed_df.Ticker == ticker).toPandas()
        if filtered_df.empty:
            st.warning(f"No data available for {ticker}")
            continue

        # Create candlestick chart with plotly
        fig = go.Figure(data=[go.Candlestick(
            x=filtered_df['Date'],
            open=filtered_df['Open'],
            high=filtered_df['High'],
            low=filtered_df['Low'],
            close=filtered_df['Close'],
            name='Candlestick'
        )])

        # Add buy/sell signals markers if available
        signals = buy_sell_signals_df.filter(buy_sell_signals_df.Ticker == ticker).toPandas()
        buy_signals = signals[signals['signal'] == 'Buy']
        sell_signals = signals[signals['signal'] == 'Sell']

        fig.add_trace(go.Scatter(
            x=buy_signals['latest_date'],
            y=filtered_df.loc[filtered_df['Date'].isin(buy_signals['latest_date']), 'Close'],
            mode='markers',
            marker=dict(symbol='triangle-up', color='green', size=10),
            name='Buy Signal'
        ))

        fig.add_trace(go.Scatter(
            x=sell_signals['latest_date'],
            y=filtered_df.loc[filtered_df['Date'].isin(sell_signals['latest_date']), 'Close'],
            mode='markers',
            marker=dict(symbol='triangle-down', color='red', size=10),
            name='Sell Signal'
        ))

        fig.update_layout(
            xaxis_title='Date',
            yaxis_title='Price (INR)',
            template='plotly_dark',
            height=500,
            margin=dict(l=40, r=40, t=40, b=40)
        )

        st.plotly_chart(fig, use_container_width=True)

# Sidebar to select stocks to display candlestick charts and signals
selected_stocks = st.sidebar.multiselect(
    "Select stocks to view candlestick charts and signals",
    options=[],
    default=[],
)

# Display candlestick charts with buy/sell signals from CSV data
if selected_stocks and 'processed_df' in locals() and 'buy_sell_signals_df' in locals():
    for ticker in selected_stocks:
        st.subheader(f"{ticker}")
        # Filter processed data for the selected ticker
        filtered_df = processed_df.filter(processed_df.Ticker == ticker).toPandas()
        if filtered_df.empty:
            st.warning(f"No data available for {ticker}")
            continue

        # Create candlestick chart with plotly
        fig = go.Figure(data=[go.Candlestick(
            x=filtered_df['Date'],
            open=filtered_df['Open'],
            high=filtered_df['High'],
            low=filtered_df['Low'],
            close=filtered_df['Close'],
            name='Candlestick'
        )])

        # Add buy/sell signals markers if available
        signals = buy_sell_signals_df.filter(buy_sell_signals_df.Ticker == ticker).toPandas()
        buy_signals = signals[signals['signal'] == 'Buy']
        sell_signals = signals[signals['signal'] == 'Sell']

        fig.add_trace(go.Scatter(
            x=buy_signals['latest_date'],
            y=filtered_df.loc[filtered_df['Date'].isin(buy_signals['latest_date']), 'Close'],
            mode='markers',
            marker=dict(symbol='triangle-up', color='green', size=10),
            name='Buy Signal'
        ))

        fig.add_trace(go.Scatter(
            x=sell_signals['latest_date'],
            y=filtered_df.loc[filtered_df['Date'].isin(sell_signals['latest_date']), 'Close'],
            mode='markers',
            marker=dict(symbol='triangle-down', color='red', size=10),
            name='Sell Signal'
        ))

        fig.update_layout(
            xaxis_title='Date',
            yaxis_title='Price (INR)',
            template='plotly_dark',
            height=500,
            margin=dict(l=40, r=40, t=40, b=40)
        )

        st.plotly_chart(fig, use_container_width=True)
