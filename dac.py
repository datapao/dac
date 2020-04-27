import functools
import logging
import json
import os

from datetime import datetime

import pandas as pd

from flask import Flask, render_template
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, scoped_session

from aggregation import concat_dfs, get_time_index, get_time_grouper
from aggregation import aggregate, get_cluster_dbus, get_running_jobs
from aggregation import get_last_7_days_dbu, aggregate_for_entity
from aggregation import aggregate_by_types
from db import engine_url, Base
from db import Workspace, Cluster, Job, JobRun, User, ScraperRun


logformat = "%(asctime)-15s %(name)-12s %(levelname)-8s %(message)s"
logging.basicConfig(level=logging.DEBUG, format=logformat)
log = logging.getLogger("dac")
logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)


app = Flask(__name__, static_folder='templates/static/')
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY') or 'whoops'
app.config['CONFIG_PATH'] = os.getenv('DAC_CONFIG_PATH')
engine = create_engine(engine_url)
Base.metadata.bind = engine


def create_session():
    DBSession = scoped_session(sessionmaker())
    DBSession.bind = engine
    session = DBSession()
    return session


def format_datetime(value):
    return value.strftime("%Y-%m-%d %H:%M:%S")


app.jinja_env.filters['datetime'] = format_datetime


@functools.lru_cache(maxsize=None)
def get_settings(path=None):
    if path is None:
        path = app.config.get('CONFIG_PATH')

    with open(path, 'r') as config_file:
        settings = json.load(config_file)
    return settings


@functools.lru_cache(maxsize=None)
def get_price_settings(path=None):
    settings = get_settings(path)
    price = settings.get('prices')
    if isinstance(price, list):
        log.warning('Price config is depricated. Please refer to the sample '
                    'config file (configs/config.json.default).')
        price_config = {setting['type']: setting['value']
                        for setting in price}

    elif isinstance(price, dict):
        price_config = price

    else:
        log.error('Price config is invalid. Falling back to default value '
                  '(interactive: 1, job: 1)')
        price_config = {'interactive': 1, 'job': 1}

    return price_config


def get_level_info_data():
    session = create_session()
    workspaces = session.query(Workspace)

    price_settings = get_price_settings()
    interactive_dbu_price = price_settings['interactive']
    job_dbu_price = price_settings['job']

    workspace_count = workspaces.count()
    cluster_count = sum([len(workspace.active_clusters())
                         for workspace in workspaces])
    user_count = sum([len(workspace.users()) for workspace in workspaces])
    actives = concat_dfs(workspace.state_df(active_only=True)
                         for workspace in workspaces)

    dbu_counts = get_cluster_dbus(actives)
    dbu_count_dict = dbu_counts.to_dict()
    dbu_cost = (dbu_count_dict.get('interactive', 0.0) * interactive_dbu_price
                + dbu_count_dict.get('job', 0.0) * job_dbu_price)

    return {
        "clusters": cluster_count,
        "workspaces": workspace_count,
        "user_count": user_count,
        "daily_dbu": dbu_counts.sum(),
        "daily_dbu_cost": dbu_cost,
        "interactive_dbu_price": interactive_dbu_price,
        "job_dbu_price": job_dbu_price
    }


# ======= MISSING =======
@app.route('/missing/<string:type>/<string:id>')
def view_missing(type, id):
    return render_template('missing.html', type=type, id=id)


# ======= DASHBOARD =======
@app.route('/')
def view_dashboard():
    session = create_session()
    clusters = session.query(Cluster).all()
    jobs = session.query(JobRun).all()
    states = concat_dfs(cluster.state_df() for cluster in clusters)

    level_info_data = get_level_info_data()
    numbjobs_dict = get_running_jobs(jobs)
    last7dbu_dict = aggregate_by_types(states, get_last_7_days_dbu)

    time_stats_dict = {}
    if not states.empty:
        results = aggregate_by_types(states, aggregate_for_entity)
        time_stats_dict = {}
        cost_summary_dict = {}
        for key, (cost_summary, time_stats) in results.items():
            time_stats['dbu_cumsum'] = time_stats['interval_dbu_sum'].cumsum()
            time_stats_dict[key] = time_stats.to_dict("records")
            cost_summary_dict[key] = cost_summary.to_dict()

    return render_template('dashboard.html',
                           time_stats=time_stats_dict,
                           last7dbu=last7dbu_dict,
                           numjobs=numbjobs_dict,
                           data=level_info_data)


# ======= WORKSPACE =======
@app.route('/workspaces/<string:workspace_id>')
def view_workspace(workspace_id):
    session = create_session()
    try:
        workspace = (session
                    .query(Workspace)
                    .filter(Workspace.id == workspace_id)
                    .one())
    except Exception:
        return render_template('missing.html',
                               type="workspace",
                               id=workspace_id)
    states = workspace.state_df()
    numbjobs_dict = get_running_jobs(workspace.jobruns)
    price_settings = get_price_settings()

    if not states.empty:
        results = aggregate_by_types(states, aggregate_for_entity)
        time_stats_dict = {}
        cost_summary_dict = {}
        for key, (cost_summary, time_stats) in results.items():
            time_stats_dict[key] = time_stats.to_dict("records")
            cost_summary = cost_summary.to_dict()
            cost = cost_summary['interval_dbu'] * price_settings[key]
            weekly_cost = (cost_summary['weekly_interval_dbu_sum']
                           * price_settings[key])
            cost_summary['cost'] = cost
            cost_summary['weekly_cost'] = weekly_cost
            cost_summary_dict[key] = cost_summary

        # We aren't sure if we have both interactive and job
        present_key = list(cost_summary_dict.keys())[0]
        cost_summary_dict = {key: sum([cost_summary_dict[type][key]
                                       for type in results.keys()])
                             for key in cost_summary_dict[present_key]}

        top_users = (aggregate(df=states,
                               col='interval_dbu',
                               by='user_id',
                               since_days=7)
                     .reset_index()
                     .rename(columns={'interval_dbu': 'dbu'})
                     .sort_values('dbu', ascending=False))
        top_users_dict = (top_users
                          .loc[~top_users.user_id.isin(['UNKONWN'])]
                          .to_dict("records")
                          [:3])
    else:
        cost_summary_dict = {
            "interval": 0.0,
            "interval_dbu": 0.0,
            "weekly_interval_sum": 0.0,
            "weekly_interval_dbu_sum": 0.0,
            "cost": 0.0,
            "weekly_cost": 0.0
        }
        time_stats_dict = {'interactive': {}, 'job': {}}
        top_users_dict = {}

    clusters_by_type = {}
    for cluster in workspace.clusters:
        clusters_by_type.setdefault(cluster.cluster_type(), []).append(cluster)

    return render_template('workspace.html',
                           workspace=workspace,
                           clusters_by_type=clusters_by_type,
                           cost=cost_summary_dict,
                           time_stats=time_stats_dict,
                           top_users=top_users_dict,
                           numjobs=numbjobs_dict,
                           empty=states.empty)


@app.route('/workspaces')
def view_workspaces():
    session = create_session()
    workspaces = session.query(Workspace).all()
    level_info_data = get_level_info_data()
    price_settings = get_price_settings()

    return render_template('workspaces.html',
                           workspaces=workspaces,
                           data=level_info_data,
                           price_settings=price_settings)


#  ======= CLUSTER =======
@app.route('/clusters/<string:cluster_id>')
def view_cluster(cluster_id):
    session = create_session()
    try:
        cluster = (session
                   .query(Cluster)
                   .filter(Cluster.cluster_id == cluster_id)
                   .one())
    except Exception:
        return render_template('missing.html', type="cluster", id=cluster_id)
    states = cluster.state_df()
    cluster_type = cluster.cluster_type()
    price_settings = get_price_settings()

    cost_summary, time_stats = aggregate_for_entity(states)
    cost_summary = cost_summary.to_dict()
    cost = cost_summary['interval_dbu'] * price_settings[cluster_type]
    weekly_cost = (cost_summary['weekly_interval_dbu_sum']
                   * price_settings[cluster_type])
    cost_summary['cost'] = cost
    cost_summary['weekly_cost'] = weekly_cost

    return render_template('cluster.html',
                           cluster=cluster,
                           cost=cost_summary,
                           time_stats=time_stats.to_dict("records"))


@app.route('/clusters')
def view_clusters():
    session = create_session()
    clusters = session.query(Cluster).all()
    states = concat_dfs(cluster.state_df() for cluster in clusters)
    level_info_data = get_level_info_data()
    price_settings = get_price_settings()

    if not states.empty:
        results = aggregate_by_types(states, aggregate_for_entity)
        time_stats_dict = {}
        for key, (_, time_stats) in results.items():
            time_stats_dict[key] = time_stats.to_dict("records")

        cluster_dbus = (aggregate(df=states,
                                  col="interval_dbu",
                                  by="cluster_id",
                                  since_days=7)
                        .rename(columns={'interval_dbu': 'dbu'})
                        .dbu.to_dict())
    else:
        cluster_dbus = {cluster.cluster_id: 0.0 for cluster in clusters}
        time_stats_dict = {'interactive': {}, 'job': {}}

    clusters_by_type = {}
    for cluster in clusters:
        clusters_by_type.setdefault(cluster.cluster_type(), []).append(cluster)

    return render_template('clusters.html',
                           clusters_by_type=clusters_by_type,
                           price_settings=price_settings,
                           data=level_info_data,
                           cluster_dbus=cluster_dbus,
                           time_stats=time_stats_dict)


#  ======= USER =======
@app.route('/users/<string:username>')
def view_user(username):
    session = create_session()
    try:
        user = (session
                .query(User)
                .filter(User.username == username)
                .one())
    except Exception:
        return render_template('missing.html', type="user", id=username)
    states = user.state_df()
    if not states.empty:
        workspaces = (
            concat_dfs({(w.workspace.id, w.workspace.name): w.workspace.state_df()
                        for w in user.user_workspaces})
            .reset_index([0, 1])
            .rename(columns={'level_0': 'workspace_id',
                             'level_1': 'workspace_name'}))

        last7_workspaces = (aggregate(df=workspaces,
                                      col='interval_dbu',
                                      by=['workspace_id', 'workspace_name'],
                                      since_days=7)
                            .rename(columns={'interval_dbu': 'last7dbu'}))

        all_workspaces = (aggregate(df=workspaces,
                                    col='interval_dbu',
                                    by=['workspace_id', 'workspace_name'])
                          .rename(columns={'interval_dbu': 'alltimedbu'}))

        workspaces_dict = (
            pd.merge(all_workspaces,
                     last7_workspaces,
                     how='left',
                     left_index=True,
                     right_index=True)
            .fillna(0.0)
            .reset_index()
            .sort_values('last7dbu')
            .to_dict('records')
        )

        price_settings = get_price_settings()
        results = aggregate_by_types(states, aggregate_for_entity)
        time_stats_dict = {}
        cost_summary_dict = {}
        for key, (cost_summary, time_stats) in results.items():
            time_stats_dict[key] = time_stats.to_dict("records")
            cost_summary = cost_summary.to_dict()
            cost = cost_summary['interval_dbu'] * price_settings[key]
            weekly_cost = (cost_summary['weekly_interval_dbu_sum']
                           * price_settings[key])
            cost_summary['cost'] = cost
            cost_summary['weekly_cost'] = weekly_cost
            cost_summary_dict[key] = cost_summary

        # We aren't sure if we have both interactive and job
        present_key = list(cost_summary_dict.keys())[0]
        cost_summary_dict = {key: sum([cost_summary_dict[type][key]
                                       for type in results.keys()])
                             for key in cost_summary_dict[present_key]}
    else:
        workspaces_dict = [{'workspace_id': w.workspace.id,
                            'workspace_name': w.workspace.name,
                            'last7dbu': 0.0,
                            'alltimedbu': 0.0}
                           for w in user.user_workspaces]
        cost_summary_dict = {
            "interval": 0.0,
            "interval_dbu": 0.0,
            "weekly_interval_sum": 0.0,
            "weekly_interval_dbu_sum": 0.0,
            "cost": 0.0,
            "weekly_cost": 0.0
        }
        time_stats_dict = {'interactive': {}, 'job': {}}

    return render_template('user.html',
                           user=user,
                           workspaces=workspaces_dict,
                           cost=cost_summary_dict,
                           time_stats=time_stats_dict)


@app.route('/users')
def view_users():
    session = create_session()
    users = session.query(User).all()

    level_info_data = get_level_info_data()

    for user in users:
        user.dbu = aggregate(df=user.state_df(),
                             col='interval_dbu',
                             since_days=7)
    users = sorted(users, key=lambda user: user.dbu, reverse=True)
    states = concat_dfs(user.state_df() for user in users)

    # Average active users
    active_users = (aggregate(df=states,
                              col='user_id',
                              by=get_time_grouper('timestamp'),
                              aggfunc='nunique',
                              since_days=7)
                    .reindex(get_time_index(7), fill_value=0))
    active_users['ts'] = active_users.index.format()

    # Average used DBU
    dbus = (aggregate(df=states,
                      col='interval_dbu',
                      by=get_time_grouper('timestamp'),
                      aggfunc='sum',
                      since_days=7)
            .reindex(get_time_index(7), fill_value=0))
    active_users['sum_dbus'] = dbus.interval_dbu
    active_users['average_dbu'] = ((active_users.sum_dbus
                                    / active_users.user_id)
                                   .fillna(0.))

    return render_template('users.html',
                           users=users,
                           active_users=active_users.to_dict('records'),
                           data=level_info_data)


#  ======= JOBS =======
@app.route('/jobs/<string:job_id>')
def view_job(job_id):
    session = create_session()
    try:
        job = (session
               .query(Job)
               .filter(Job.job_id == job_id)
               .one())
    except Exception:
        return render_template('missing.html', type="job", id=job_id)
    price_settings = get_price_settings()

    aggregations = {'duration': ['min', 'median', 'max', 'sum'],
                    'cost': ['min', 'median', 'max', 'sum']}
    last7_stats = (job
                   .runs(as_df=True, price_config=price_settings, last=7)
                   .agg(aggregations))
    if last7_stats.empty:
        last7_stats = pd.DataFrame({column: {agg: 0 for agg in aggregation}
                                    for column, aggregation in aggregations.items()})

    since30 = job.runs(as_df=True, price_config=price_settings, since_days=30)
    if not since30.empty:
        time_stats = (since30
                      .groupby(get_time_grouper('start_time'))
                      .agg({'run_id': 'count',
                            'dbu': 'sum',
                            'duration': 'median'})
                      .fillna(0.)
                      .reindex(get_time_index(30), fill_value=0.))
        time_stats['ts'] = time_stats.index.format()
    else:
        time_stats = (pd.DataFrame(columns=['run_id', 'dbu', 'duration'])
                      .reindex(get_time_index(30), fill_value=0.))
        time_stats['ts'] = time_stats.index.format()

    return render_template('job.html',
                           job=job,
                           price_settings=price_settings,
                           last7_stats=last7_stats,
                           time_stats=time_stats.to_dict("records"))


@app.route('/jobs')
def view_jobs():
    session = create_session()
    jobs = session.query(Job).all()
    level_info_data = get_level_info_data()
    price_settings = get_price_settings()

    aggregations = {'cost': ['median'],
                    'dbu': ['median'],
                    'duration': ['median']}

    extra_stats = {}
    for job in jobs:
        aggregated = (job
                      .runs(as_df=True, price_config=price_settings, last=7)
                      .agg(aggregations))
        if aggregated.empty:
            aggregated = pd.DataFrame({column: {agg: 0 for agg in aggregation}
                                       for column, aggregation in aggregations.items()})
        extra_stats[job.job_id] = aggregated

    run_df = concat_dfs(job.runs(as_df=True, price_config=price_settings, since_days=30)
                        for job in jobs)
    if not run_df.empty:
        time_stats = (run_df
                      .groupby(get_time_grouper('start_time'))
                      .agg({'run_id': 'count',
                            'dbu': 'sum',
                            'duration': 'sum'})
                      .fillna(0.)
                      .reindex(get_time_index(30), fill_value=0))
        time_stats['ts'] = time_stats.index.format()
    else:
        time_stats = (pd.DataFrame(columns=['run_id', 'dbu', 'duration'])
                      .reindex(get_time_index(30), fill_value=0))
        time_stats['ts'] = time_stats.index.format()

    return render_template('jobs.html',
                           jobs=jobs,
                           price_settings=price_settings,
                           data=level_info_data,
                           time_stats=time_stats.to_dict("records"),
                           extra_stats=extra_stats)


#  ======= SCRAPE RUNS =======
@app.route('/scrape_runs')
def view_scrape_runs():
    session = create_session()
    runs = session.query(ScraperRun).order_by(
        desc(ScraperRun.start_time)).limit(15).all()
    last_scrape = "no scrape run yet"
    if len(runs) > 0:
        last_scrape = datetime.now() - runs[0].end_time
    return render_template('scrape_runs.html',
                           runs=runs,
                           last_scrape=last_scrape)


#  ======= ALERTS =======
@app.route('/alerts')
def view_alerts():
    return render_template('alerts.html')


#  ======= SETTINGS =======
def format_workspace_configs(configs):
    if not isinstance(configs, list):
        configs = [configs]

    formatted = []
    for config in configs:
        key_value = '\n\t'.join([f"'{k}': '{v}'" for k, v in config.items()])
        formatted.append(f'{{\n\t{key_value}\n}}')

    return ',\n'.join(formatted)


@app.route('/settings')
def view_settings():
    settings = get_settings()
    # obfuscation
    settings['workspaces'] = [{key: (value
                                     if key != 'token'
                                     else '*' * 6 + value[-4:])
                               for key, value in setting.items()}
                              for setting in settings['workspaces']]
    settings = {key: json.dumps(value, indent=4, sort_keys=True)
                for key, value in settings.items()}

    return render_template('settings.html', settings=settings)
