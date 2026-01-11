from pathlib import Path
import pandas as pd

class ParquetStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def path(self, key: str) -> Path:
        return self.root / f"{key}.parquet"

    def exists(self, key: str) -> bool:
        return self.path(key).exists()

    def read(self, key: str) -> pd.DataFrame:
        df = pd.read_parquet(self.path(key))
        df.reset_index(drop=True, inplace=True)    

        # Convert Date to date-only (remove timezone and time)
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date']).dt.date
        
        return df

    def write(self, key: str, df: pd.DataFrame):
        df = df.copy()  # Don't modify original dataframe
        df.reset_index(drop=True, inplace=True)
        
        # Convert Date to date-only (remove timezone and time)
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date']).dt.date
        
        df.to_parquet(self.path(key))