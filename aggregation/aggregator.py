from datetime import datetime, timedelta

import pandas as pd


def since(days: int) -> datetime:
    return datetime.today() - timedelta(days=days)


def get_time_grouper(col: str, freq='1D') -> pd.Grouper:
    return pd.Grouper(key=col, freq=freq, label="right")


def get_time_index(since_days=30):
    start = since(days=since_days)
    end = datetime.today()
    return pd.date_range(start, end, freq="1D", normalize=True)


def concat_dfs(dfs):
    dfs = list(dfs)
    return pd.concat(dfs) if len(dfs) else pd.DataFrame()


def aggregate(df: pd.DataFrame,
              col: str = 'dbu',
              by: list = None,
              aggfunc: str = 'sum',
              since_days: int = None):

    filtered = df.loc[df.state.isin(['RUNNING'])]
    if since_days is not None:
        filtered = df.loc[df.timestamp >= since(since_days)]
    running = filtered.copy()

    # No grouping: return the value
    if by is None:
        return running[col].agg(aggfunc)

    # Grouping is specified: return the resulting df
    return running.groupby(by).agg({col: aggfunc})


def get_cluster_dbus(clusters: pd.DataFrame, ids=None):
    if ids is not None:
        clusters = clusters.loc[clusters.cluster_id.isin(ids)]

    last_setups = (clusters
                   .loc[clusters.state.isin(['RUNNING'])]
                   .sort_values('timestamp')
                   .groupby('cluster_id')
                   .tail(1))

    return last_setups.dbu.sum()


def aggregate_over_time(states: pd.DataFrame) -> pd.DataFrame:
    grouper = get_time_grouper("timestamp")
    aggregations = {"cluster_id": "nunique",
                    "dbu": "max",
                    "interval": "sum",
                    "interval_dbu": ["sum", "mean"],
                    "num_workers": ["min", "max", "median"],
                    "worker_hours": "sum"}
    index = get_time_index(since_days=30)

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
              .fillna(0)
              .loc[states['state'].isin(['RUNNING'])])

    time_stats = aggregate_over_time(states)

    weekly_cost_stats = (time_stats
                         .loc[since(days=7):,
                              ["interval_dbu_sum", "interval_sum"]]
                         .sum())
    weekly_cost_stats.index = [f"weekly_{p}" for p in weekly_cost_stats.index]

    cost_summary = states[["interval_dbu", "interval"]].sum()
    cost_summary = pd.concat([cost_summary, weekly_cost_stats])

    return cost_summary, time_stats
