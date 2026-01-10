import logging
from stockstash import TimeSeriesCache, ParquetStore, YFinanceProvider

# Enable debug logging to see cache activity
# logging.basicConfig(
#     level=logging.DEBUG,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )

cache = TimeSeriesCache(
    store=ParquetStore("./data"),
    provider=YFinanceProvider(),
)

df = cache.load(
    key="AAPL",
    start="2025-06-01",
    end="2025-06-29",
)

print(df.tail())

df2 = cache.load( # will only fetch missing data from July and August
    key="AAPL",
    start="2025-06-01",
    end="2025-08-29",
)

print(df2.tail())