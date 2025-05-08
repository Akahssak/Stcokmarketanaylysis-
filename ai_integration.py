"""
ai_integration.py

Module to integrate with Google AI API to get stock suggestions.
"""

def get_ai_stock_suggestions():
    """
    Placeholder function to call Google AI API and get top 5 stocks to buy/sell
    for long and short term.

    Returns:
        dict: A dictionary with keys 'long_term_buy', 'long_term_sell',
              'short_term_buy', 'short_term_sell', each containing a list of
              stock tickers or dicts with stock info.
    """
    # TODO: Implement actual API call using user-provided Google AI API details.
    # For now, return dummy data for testing visualization.

    suggestions = {
        "long_term_buy": ["INFY.NS", "TCS.NS", "HDFC.NS", "RELIANCE.NS", "ICICI.NS"],
        "long_term_sell": ["YESBANK.NS", "PNB.NS", "IDFC.NS", "SBI.NS", "BANKBARODA.NS"],
        "short_term_buy": ["BAJAJ-AUTO.NS", "MARUTI.NS", "HCLTECH.NS", "LT.NS", "ASIANPAINT.NS"],
        "short_term_sell": ["TATASTEEL.NS", "JSWSTEEL.NS", "COALINDIA.NS", "ONGC.NS", "BPCL.NS"]
    }
    return suggestions
