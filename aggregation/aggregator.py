# TODO: finish / fix this file
import pandas as pd


def aggregate(df: pd.DataFrame, by: list) -> pd.DataFrame:
    running = df.loc[df.state == 'RUNNING'].copy()
    running['cost'] = running.dbu * running.interval
    return df.groupby(by).agg({'cost': 'sum'}).rename(columns={'sum': 'dbu'})
