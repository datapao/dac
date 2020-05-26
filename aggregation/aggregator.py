from datetime import datetime, timedelta
from types import GeneratorType

import pandas as pd


def since(days: int) -> datetime:
    return datetime.today() - timedelta(days=days)


def get_time_grouper(col: str, freq='1D') -> pd.Grouper:
    return pd.Grouper(key=col, freq=freq, label="right")


def get_time_index(since_days=30):
    start = since(days=since_days)
    # since end parameter is not inclusive, we need to add one day
    end = datetime.today() + timedelta(days=1)
    return pd.date_range(start, end, freq="1D", normalize=True)


def concat_dfs(dfs):
    if isinstance(dfs, GeneratorType):
        dfs = list(dfs)
    return pd.concat(dfs, sort=False) if len(dfs) else pd.DataFrame()


def empty_timeseries(columns=None, days=30, as_df=False):
    if columns is None:
        columns = ['run_id', 'dbu', 'duration']

    time_stats = (pd.DataFrame(columns=columns)
                  .reindex(get_time_index(days), fill_value=0.))
    time_stats['ts'] = time_stats.index.format()

    if not as_df:
        time_stats = time_stats.to_dict('records')

    return time_stats


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


def get_cluster_dbus(clusters: pd.DataFrame, ids: list = None) -> pd.Series:
    if clusters.empty:
        return pd.Series({'interactive': 0.0, 'job': 0.0})

    if ids is not None:
        clusters = clusters.loc[clusters.cluster_id.isin(ids)]

    last_setups = (clusters
                   .loc[clusters.state.isin(['RUNNING'])]
                   .sort_values('timestamp')
                   .groupby('cluster_id')
                   .tail(1))

    return last_setups.groupby('cluster_type').dbu.sum()


def get_running_jobs(jobs):
    jobs = pd.DataFrame([{'job_id': job.job_id,
                          'workspace_id': job.workspace_id,
                          'user_id': job.creator_user_name,
                          'name': job.run_name,
                          'timestamp': job.start_time}
                         for job in jobs])
    numbjobs_dict = []
    if not jobs.empty:
        numjobs = (jobs
                   .groupby(get_time_grouper('timestamp'))
                   [['job_id']]
                   .count()
                   .reindex(get_time_index(30), fill_value=0))
        numjobs['ts'] = numjobs.index.format()
        numbjobs_dict = numjobs.to_dict('records')
    else:
        numjobs = empty_timeseries(columns=['job_id'])

    return numbjobs_dict


def get_last_7_days_dbu(states: pd.DataFrame) -> dict:
    last7dbu_dict = {}
    if not states.empty:
        last7dbu = (aggregate(df=states,
                              col='interval_dbu',
                              by=get_time_grouper('timestamp'),
                              aggfunc='sum',
                              since_days=7)
                    .reindex(get_time_index(7), fill_value=0))
        last7dbu['ts'] = last7dbu.index.format()
        last7dbu_dict = last7dbu.to_dict('records')
    else:
        last7dbu_dict = empty_timeseries(columns=['interval_dbu'], days=7)

    return last7dbu_dict


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
    cost_summary = pd.concat([cost_summary, weekly_cost_stats], sort=False)

    return cost_summary, time_stats


def aggregate_by_types(states: pd.DataFrame, aggregation_func):
    interactive_states = states.loc[states.cluster_type == 'interactive']
    job_states = states.loc[states.cluster_type == 'job']

    results = {'interactive': aggregation_func(interactive_states),
               'job': aggregation_func(job_states)}

    return results
