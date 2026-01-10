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
            
            # Ensure timezone consistency
            if not df.empty and df.index.tz is not None:
                # If dataframe has timezone info, localize start/end to the same timezone
                if start.tz is None:
                    start = start.tz_localize(df.index.tz)
                if end.tz is None:
                    end = end.tz_localize(df.index.tz)
            elif not df.empty and df.index.tz is None:
                # If dataframe is timezone-naive, ensure start/end are also naive
                if start.tz is not None:
                    start = start.tz_localize(None)
                if end.tz is not None:
                    end = end.tz_localize(None)
        else:
            df = pd.DataFrame()
            logger.debug(f"Cache miss for {key}: no existing data found")

        if df.empty:
            logger.debug(f"Downloading full range for {key}: {start} to {end}")
            new = self.provider.fetch(key, start, end)
            logger.debug(f"Downloaded {len(new)} records for {key}")
            self.store.write(key, new)
            return new

        missing = find_missing_ranges(df.index, start, end)
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

        df = (
            df.sort_index()
              .loc[start:end]
              .loc[~df.index.duplicated(keep="last")]
        )

        reused_count = len(df) - downloaded_count
        logger.debug(f"Final result for {key}: {len(df)} total records (reused: {reused_count}, downloaded: {downloaded_count})")
        
        self.store.write(key, df)
        return df
