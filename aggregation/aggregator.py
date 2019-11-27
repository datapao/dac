from datetime import datetime, timedelta

import pandas as pd


def since(days: int) -> datetime:
    return datetime.today() - timedelta(days=days)


def aggregate(df: pd.DataFrame,
              col: str = 'dbu',
              by: list = None,
              aggfunc: str = 'sum',
              since_days: int = None):

    filter = df.state == 'RUNNING'
    if since_days is not None:
        filter &= df.timestamp > since(since_days)
    running = df.loc[filter].copy()

    # No grouping: return the value
    if by is None:
        return running[col].agg(aggfunc)

    # Grouping is specified: return the resulting df
    return running.groupby(by).agg({col: aggfunc})


def sum_dbu(df: pd.DataFrame, by: list = None, since_days: int = None):
    """Returns the sum of dbus.
    If by parameter is specified, group the result by the specified columns,
    if since_days is present the results will be limited to the specified
    interval. If no grouping columns are specified a number will be returned
    otherwise a pandas DataFrame.

    Parameters:
    -----------
    df : pd.DataFrame
        DF containing the parsed states
    by : list or str
        Column name(s) to use as grouping cols
    since_days : int
        Time limit: the number of days from today to include

    Returns:
    --------
    dbu : float or pd.DataFrame
        Summed up DBU usage.
    """
    result = aggregate(df=df.assign(cost=df.dbu * df.interval),
                       col='cost',
                       by=by,
                       since_days=since_days)

    if isinstance(result, pd.DataFrame):
        result = result.rename(columns={'cost': 'dbu'})

    return result


def aggregate_over_time(states: pd.DataFrame) -> pd.DataFrame:
    grouper = pd.Grouper(key="timestamp", freq="1D", label="right")
    aggregations = {"dbu": "max",
                    "interval": "sum",
                    "interval_dbu": "sum",
                    "num_workers": ["min", "max", "median"],
                    "worker_hours": "sum"}
    index = pd.date_range(since(days=30),
                          datetime.today(),
                          freq="1D",
                          normalize=True)

    time_stats = (states
                  .groupby(grouper)
                  .agg(aggregations)
                  .reindex(index)
                  .fillna(0))
    time_stats.columns = ['_'.join(col) for col in time_stats.columns]
    time_stats["ts"] = time_stats.index.format()

    return time_stats


def aggregate_for_entity(states: pd.DataFrame):
    states = (states
              .assign(worker_hours=states["interval"] * states["num_workers"])
              .fillna(0))

    time_stats = aggregate_over_time(states)

    weekly_cost_stats = (time_stats
                         .loc[:since(days=7),
                              ["interval_dbu_sum", "interval_sum"]]
                         .sum())
    weekly_cost_stats.index = [f"weekly_{p}" for p in weekly_cost_stats.index]

    cost_summary = states[["interval_dbu", "interval"]].sum()
    cost_summary = pd.concat([cost_summary, weekly_cost_stats])

    return cost_summary, time_stats
