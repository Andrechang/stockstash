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
        return pd.read_parquet(self.path(key))

    def write(self, key: str, df: pd.DataFrame):
        df.to_parquet(self.path(key))