import logging
import pandas as pd
from .ranges import find_missing_ranges

logger = logging.getLogger(__name__)

class TimeSeriesCache:
    def __init__(self, store, provider):
        self.store = store
        self.provider = provider

    def load(
        self,
        key: str,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
    ) -> pd.DataFrame:
        start = pd.Timestamp(start)
        end = pd.Timestamp(end)

        if self.store.exists(key):
            df = self.store.read(key)
            logger.debug(f"Cache hit for {key}: loaded {len(df)} existing records")
        else:
            df = pd.DataFrame()
            logger.debug(f"Cache miss for {key}: no existing data found")

        if df.empty:
            logger.debug(f"Downloading full range for {key}: {start} to {end}")
            new = self.provider.fetch(key, start, end)
            logger.debug(f"Downloaded {len(new)} records for {key}")
            self.store.write(key, new)
            return new

        # Ensure Date column is datetime for range operations
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Convert start/end to date-only for comparison
        start_date = start.date()
        end_date = end.date()
        
        missing = find_missing_ranges(df['Date'].dt.date, start_date, end_date)
        logger.debug(f"Found {len(missing)} missing ranges for {key}: {missing}")

        downloaded_count = 0
        for s, e in missing:
            logger.debug(f"Downloading missing range for {key}: {s} to {e}")
            new = self.provider.fetch(key, s, e)
            if not new.empty:
                downloaded_count += len(new)
                df = pd.concat([df, new])
                logger.debug(f"Downloaded {len(new)} records for range {s} to {e}")
            else:
                logger.debug(f"No data available for range {s} to {e}")

        # Sort and remove duplicates from the complete dataset
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date')
        df = df.drop_duplicates(subset=['Date'], keep='last')
        
        # Save the complete merged dataset (includes data outside requested range)
        if downloaded_count > 0:
            self.store.write(key, df)
        
        # Filter by date range for return value only
        result_df = df[(df['Date'] >= start) & (df['Date'] <= end)]

        reused_count = len(result_df) - downloaded_count
        logger.debug(f"Saved {len(df)} total records to cache for {key}")
        logger.debug(f"Returning {len(result_df)} records for requested range (reused: {reused_count}, downloaded: {downloaded_count})")
        
        return result_df
