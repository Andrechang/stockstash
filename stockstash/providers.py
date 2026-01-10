import yfinance as yf
import pandas as pd

class YFinanceProvider:
    def fetch(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp):
        stock = yf.Ticker(symbol)
        df = stock.history(start=start, end=end)
        return df
