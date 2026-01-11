import pandas as pd
from datetime import timedelta, date

def find_missing_ranges(
    dates: pd.Series,
    start: pd.Timestamp | date,
    end: pd.Timestamp | date,
    freq: str = "1D",
    min_gap_days: int = 5,  # Minimum gap to consider as missing (avoids weekends/holidays)
):
    # Convert timestamps to dates for comparison
    if hasattr(start, 'date'):
        start = start.date()
    if hasattr(end, 'date'):
        end = end.date()
    
    if dates.empty:
        return [(start, end)]

    # Convert dates to date objects if they're timestamps for consistent comparison
    date_values = dates.dt.date if hasattr(dates.iloc[0], 'date') else dates
    date_values = pd.Series(date_values).sort_values().reset_index(drop=True)
    missing = []

    # Before cached data
    first_date = date_values.iloc[0]
    if start < first_date:
        gap_end = first_date - timedelta(days=1)
        if (gap_end - start).days + 1 >= min_gap_days:
            missing.append((start, gap_end))

    # Gaps inside cached data
    for i in range(len(date_values) - 1):
        current_date = date_values.iloc[i]
        next_date = date_values.iloc[i + 1]
        
        # Calculate the gap between consecutive dates
        gap_start = current_date + timedelta(days=1)
        gap_end = next_date - timedelta(days=1)
        
        # Only consider it a missing range if the gap is significant
        if gap_start <= gap_end and (gap_end - gap_start).days + 1 >= min_gap_days:
            missing.append((gap_start, gap_end))

    # After cached data
    last_date = date_values.iloc[-1]
    if end > last_date:
        gap_start = last_date + timedelta(days=1)
        if (end - gap_start).days + 1 >= min_gap_days:
            missing.append((gap_start, end))

    return [(s, e) for s, e in missing if s <= e]
