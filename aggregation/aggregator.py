from datetime import datetime, timedelta
from types import GeneratorType

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
    if isinstance(dfs, GeneratorType):
        dfs = list(dfs)
    return pd.concat(dfs, sort=False) if len(dfs) else pd.DataFrame()


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
    last_setups['type'] = get_cluster_type(last_setups)

    return last_setups.groupby('type').dbu.sum()


def get_cluster_type(clusters: pd.DataFrame) -> pd.Series:
    names = clusters[['cluster_name']].copy()
    isjob = names.cluster_name.str.startswith('job')
    names['type'] = 'interactive'
    names.loc[isjob, 'type'] = 'job'
    return names['type']


def get_running_jobs(jobs):
    jobs = pd.DataFrame([{'job_id': job.job_id,
                          'workspace_id': job.workspace_id,
                          'user_id': job.creator_user_name,
                          'name': job.run_name,
                          'timestamp': job.start_time}
                         for job in jobs])
    numbjobs_dict = {}
    if not jobs.empty:
        numjobs = (jobs
                   .groupby(get_time_grouper('timestamp'))
                   [['job_id']]
                   .count()
                   .reindex(get_time_index(30), fill_value=0))
        numjobs['ts'] = numjobs.index.format()
        numbjobs_dict = numjobs.to_dict('records')

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
    cost_summary = pd.concat([cost_summary, weekly_cost_stats])

    return cost_summary, time_stats


def aggregate_by_types(states: pd.DataFrame, aggregation_func):
    interactive_states = states.loc[states.cluster_type == 'interactive']
    job_states = states.loc[states.cluster_type == 'job']

    return {
        'interactive': aggregation_func(interactive_states),
        'job': aggregation_func(job_states)
    }
