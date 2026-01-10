# stockstash

**Intelligent local caching for time-series APIs using Parquet**

`stockstash` provides incremental, gap-aware caching for time-series data.
It avoids re-downloading overlapping date ranges and stores data using Parquet.

This library is especially useful for financial data APIs such as **yfinance**,
so that you dont need to download same data again.

---

## ✨ Features

- 📦 Local persistent cache using Parquet
- 🧠 Intelligent gap detection (downloads only missing dates)
- 🔌 Provider abstraction (yfinance included)
- 🗂 One file per symbol (simple & scalable)
- ♻ Reusable across sessions
---

## 📦 Installation

```bash
pip install stockstash
```

## 🚀 Quick Start

Take a look at the example: 
```
python examples/yfinance_example.py
```

```
from stockstash import TimeSeriesCache, ParquetStore, YFinanceProvider

cache = TimeSeriesCache(
    store=ParquetStore("./data"),
    provider=YFinanceProvider(),
)

df = cache.load(
    key="BTC-USD",
    start="2023-01-01",
    end="2023-12-31",
)

print(df.tail())
```

On subsequent runs, only missing dates are downloaded.

## 🧠 How It Works

Cached data is loaded from a local Parquet file

Missing date ranges are automatically detected

Only missing ranges are fetched from the API

New data is merged and deduplicated

Cache is updated on disk

## 📁 Cache Layout
```
data/
└── AAPL.parquet
└── ETH-USD.parquet
```

Each file contains a Pandas DataFrame indexed by DatetimeIndex.

## 🔌 Supported Providers
yfinance (built-in) 
from stockstash import YFinanceProvider


You can add your own provider by implementing:
```
class Provider:
    def fetch(self, key: str, start, end):
        ...
```


