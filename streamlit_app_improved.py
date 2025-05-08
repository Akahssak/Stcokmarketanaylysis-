import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Import project-specific modules for data loading and analysis
from data_load import load_dataframes
from data_processing import process_data
from analysis import analyze_data

# Set page config for Streamlit app
st.set_page_config(
    page_title="Improved Stock Market Analytics Dashboard",
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

st.title("📈 StellarStock Insights Dashboard")
st.markdown(
    """
    This dashboard provides enhanced stock market analytics with interactive charts, buy/sell signals, and AI suggestions.
    """
)

@st.cache_data(show_spinner=True)
def load_and_process_data(selected_tickers):
    master_df, spark = load_dataframes(data_dir="data", tickers=selected_tickers)
    if master_df is None:
        return None, None, None, None, None
    processed_df, metrics_df = process_data(master_df, spark)
    summary_df, top_gainers_df, top_losers_df, daily_pct_change_df, buy_sell_signals_df = analyze_data(processed_df, spark)

    # Convert Spark DataFrames to Pandas DataFrames for caching compatibility
    processed_pdf = processed_df.toPandas() if processed_df is not None else None
    buy_sell_signals_pdf = buy_sell_signals_df.toPandas() if buy_sell_signals_df is not None else None
    top_gainers_pdf = top_gainers_df.toPandas() if top_gainers_df is not None else None
    top_losers_pdf = top_losers_df.toPandas() if top_losers_df is not None else None
    summary_pdf = summary_df.toPandas() if summary_df is not None else None

    return processed_pdf, buy_sell_signals_pdf, top_gainers_pdf, top_losers_pdf, summary_pdf

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

# Sidebar: date range filter (assuming data has Date column)
start_date = st.sidebar.date_input("Start Date", datetime.now() - timedelta(days=365))
end_date = st.sidebar.date_input("End Date", datetime.now())

if start_date > end_date:
    st.sidebar.error("Start date must be before end date")

if st.sidebar.button("Load and Analyze Stock Data"):
    with st.spinner("Loading and analyzing data..."):
        processed_pdf, buy_sell_signals_pdf, top_gainers_pdf, top_losers_pdf, summary_pdf = load_and_process_data(selected_tickers)
        if processed_pdf is None:
            st.error("No data loaded. Please check your data files.")
        else:
            # Filter processed_df by date range
            processed_pdf['Date'] = pd.to_datetime(processed_pdf['Date'])
            # Adjust date filtering to include all dates if no data in range
            mask = (processed_pdf['Date'] >= pd.to_datetime(start_date)) & (processed_pdf['Date'] <= pd.to_datetime(end_date))
            filtered_pdf = processed_pdf.loc[mask]
            if filtered_pdf.empty:
                st.warning("No data available for the selected stocks in the selected date range. Showing all available data instead.")
                filtered_pdf = processed_pdf

            # Display summary statistics
            st.subheader("Summary Statistics")
            if summary_pdf is not None:
                st.dataframe(summary_pdf)
            else:
                st.info("Summary data not available.")

            # Tabs for different views
            tabs = st.tabs(["Top Gainers", "Top Losers", "Buy/Sell Signals", "Candlestick Charts", "AI Suggestions"])

            with tabs[0]:
                st.subheader("Top Gainers")
                if top_gainers_pdf is not None:
                    st.dataframe(top_gainers_pdf.style.background_gradient(cmap='Greens'))
                else:
                    st.info("Top gainers data not available.")

            with tabs[1]:
                st.subheader("Top Losers")
                if top_losers_pdf is not None:
                    st.dataframe(top_losers_pdf.style.background_gradient(cmap='Reds'))
                else:
                    st.info("Top losers data not available.")

            with tabs[2]:
                st.subheader("Buy/Sell Signals")
                if buy_sell_signals_pdf is not None:
                    st.dataframe(buy_sell_signals_pdf.style.applymap(
                        lambda x: 'color: green;' if x == 'Buy' else ('color: red;' if x == 'Sell' else 'color: black;'),
                        subset=['signal']
                    ))
                else:
                    st.info("Buy/Sell signals data not available.")

            with tabs[3]:
                st.subheader("Candlestick Charts")
                # Multi-select stocks to view charts
                selected_stocks = st.multiselect(
                    "Select stocks to view candlestick charts",
                    options=selected_tickers,
                    default=selected_tickers[:5]
                )
                if selected_stocks:
                    for ticker in selected_stocks:
                        st.markdown(f"### {ticker}")

                        # Fetch full historical data for the ticker from CSV ignoring date filter for full range
                        ticker_file = f"data/{ticker}.csv"
                        if not os.path.exists(ticker_file):
                            st.warning(f"No data file found for {ticker}.")
                            continue
                        ticker_df = pd.read_csv(ticker_file)
                        ticker_df['Date'] = pd.to_datetime(ticker_df['Date'])
                        ticker_df = ticker_df.sort_values('Date')

                        # Provide options for time range selection
                        time_range = st.selectbox(
                            f"Select time range for {ticker}",
                            options=['1 Month', '3 Months', '6 Months', '1 Year', 'All'],
                            index=3,
                            key=f"time_range_{ticker}"
                        )

                        end_date_chart = ticker_df['Date'].max()
                        if time_range == '1 Month':
                            start_date_chart = end_date_chart - pd.DateOffset(months=1)
                        elif time_range == '3 Months':
                            start_date_chart = end_date_chart - pd.DateOffset(months=3)
                        elif time_range == '6 Months':
                            start_date_chart = end_date_chart - pd.DateOffset(months=6)
                        elif time_range == '1 Year':
                            start_date_chart = end_date_chart - pd.DateOffset(years=1)
                        else:
                            start_date_chart = ticker_df['Date'].min()

                        chart_df = ticker_df[(ticker_df['Date'] >= start_date_chart) & (ticker_df['Date'] <= end_date_chart)]

                        if chart_df.empty:
                            st.warning(f"No data available for {ticker} in the selected time range.")
                            continue

                        fig = go.Figure(data=[go.Candlestick(
                            x=chart_df['Date'],
                            open=chart_df['Open'],
                            high=chart_df['High'],
                            low=chart_df['Low'],
                            close=chart_df['Close'],
                            name='Candlestick'
                        )])

                        # Add buy/sell signals markers if available
                        signals = buy_sell_signals_pdf[buy_sell_signals_pdf['Ticker'] == ticker]
                        buy_signals = signals[signals['signal'] == 'Buy']
                        sell_signals = signals[signals['signal'] == 'Sell']

                        fig.add_trace(go.Scatter(
                            x=buy_signals['latest_date'],
                            y=chart_df.loc[chart_df['Date'].isin(buy_signals['latest_date']), 'Close'],
                            mode='markers',
                            marker=dict(symbol='triangle-up', color='green', size=10),
                            name='Buy Signal'
                        ))

                        fig.add_trace(go.Scatter(
                            x=sell_signals['latest_date'],
                            y=chart_df.loc[chart_df['Date'].isin(sell_signals['latest_date']), 'Close'],
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

            with tabs[4]:
                st.subheader("AI Suggested Stocks to Buy/Sell")
                ai_suggestions_path = "plots/ai_stock_suggestions.png"
                if os.path.exists(ai_suggestions_path):
                    from PIL import Image
                    image = Image.open(ai_suggestions_path)
                    st.image(image, use_container_width=True)
                else:
                    st.info("AI stock suggestions chart not available.")

            # New section: Recommended 5 stocks among selected 20
            st.subheader("Recommended 5 Stocks Among Selected")
            if top_gainers_pdf is not None:
                recommended_stocks = top_gainers_pdf.head(5)
                st.dataframe(recommended_stocks.style.background_gradient(cmap='Blues'))
            else:
                st.info("Recommended stocks data not available.")

# Footer
st.markdown(
    """
    <hr>
    <p style="text-align:center; color:#ffb400;">&copy; 2024 Improved Stock Market Analytics Dashboard</p>
    """,
    unsafe_allow_html=True,
)
