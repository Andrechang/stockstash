import yfinance as yf
import pandas as pd

class YFinanceProvider:
    def fetch(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp):
        stock = yf.Ticker(symbol)
        df = stock.history(start=start, end=end)
        df.reset_index(inplace=True)
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date']).dt.date
        return df
