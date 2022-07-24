import pandas as pd


def convert_pandas_datetime_to_timestamps(series: pd.Series) -> pd.Series:
    series = pd.to_datetime(series, dayfirst=True).apply(lambda ts: ts.timestamp())
    return series.map(int)
