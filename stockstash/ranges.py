import pandas as pd
from datetime import timedelta

def find_missing_ranges(
    index: pd.DatetimeIndex,
    start: pd.Timestamp,
    end: pd.Timestamp,
    freq: str = "1D",
):
    if index.empty:
        return [(start, end)]

    index = index.sort_values()
    missing = []

    # Before cached data
    if start < index[0]:
        missing.append((start, index[0] - timedelta(days=1)))

    # Gaps inside cached data
    diffs = index.to_series().diff()
    gaps = diffs[diffs > pd.Timedelta(freq)]

    for gap_end in gaps.index:
        gap_start = index[index.get_loc(gap_end) - 1] + timedelta(days=1)
        missing.append((gap_start, gap_end - timedelta(days=1)))

    # After cached data
    if end > index[-1]:
        missing.append((index[-1] + timedelta(days=1), end))

    return [(s, e) for s, e in missing if s <= e]
