import argparse
import configparser
import functools
import logging

from datetime import datetime, timedelta

import pandas as pd

from flask import Flask, render_template
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, scoped_session

from aggregation import concat_dfs, get_time_index, get_time_grouper
from aggregation import aggregate, get_cluster_dbus, aggregate_for_entity
from db import engine_url, create_db, Base
from db import Workspace, Cluster, Job, User, ScraperRun
from scraping import scrape, start_scheduled_scraping


logformat = "%(asctime)-15s %(name)-12s %(levelname)-8s %(message)s"
logging.basicConfig(level=logging.DEBUG, format=logformat)
log = logging.getLogger("dac")
logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)


app = Flask(__name__, static_folder='templates/static/')
engine = create_engine(engine_url)
Base.metadata.bind = engine


def create_session():
    DBSession = scoped_session(sessionmaker())
    DBSession.bind = engine
    session = DBSession()
    return session


def get_level_info_data():
    session = create_session()
    workspaces = session.query(Workspace)

    workspace_count = workspaces.count()
    cluster_count = sum([len(workspace.active_clusters())
                         for workspace in workspaces])
    user_count = sum([len(workspace.users()) for workspace in workspaces])
    actives = concat_dfs(workspace.state_df(active_only=True)
                         for workspace in workspaces)

    dbu_count = get_cluster_dbus(actives)
    # TODO: use price from settings
    # related todo @ db.py: implement user settings
    dbu_cost = dbu_count * 10

    return {
        "clusters": cluster_count,
        "workspaces": workspace_count,
        "user_count": user_count,
        "daily_dbu": dbu_count,
        "daily_dbu_cost": dbu_cost
    }


def get_running_jobs(session):
    jobs = pd.DataFrame([{'job_id': job.job_id,
                          'workspace_id': job.workspace_id,
                          'user_id': job.creator_user_name,
                          'name': job.name,
                          'timestamp': job.created_time}
                         for job in session.query(Job).all()])
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


# ======= DASHBOARD =======
@app.route('/')
def view_dashboard():
    session = create_session()
    clusters = session.query(Cluster).all()
    states = concat_dfs(cluster.state_df() for cluster in clusters)

    clusters_by_type = {}
    for cluster in clusters:
        clusters_by_type.setdefault(cluster.worker_type, []).append(cluster)

    level_info_data = get_level_info_data()
    numbjobs_dict = get_running_jobs(session)
    last7dbu_dict = get_last_7_days_dbu(states)

    time_stats_dict = {}
    if not states.empty:
        cost_summary, time_stats = aggregate_for_entity(states)
        time_stats['interval_cumsum'] = time_stats['interval_sum'].cumsum()
        time_stats_dict = time_stats.to_dict("records")

    return render_template('dashboard.html',
                           clusters=clusters_by_type,
                           time_stats=time_stats_dict,
                           last7dbu=last7dbu_dict,
                           numjobs=numbjobs_dict,
                           data=level_info_data)


# ======= WORKSPACE =======
@app.route('/workspaces/<string:workspace_id>')
def view_workspace(workspace_id):
    session = create_session()
    workspace = (session
                 .query(Workspace)
                 .filter(Workspace.id == workspace_id)
                 .one())
    states = workspace.state_df()

    if not states.empty:
        cost_summary, time_stats = aggregate_for_entity(states)
        cost_summary_dict = cost_summary.to_dict()
        time_stats_dict = time_stats.to_dict("records")
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
        }
        time_stats_dict = {}
        top_users_dict = {}

    return render_template('workspace.html',
                           workspace=workspace,
                           cost=cost_summary_dict,
                           time_stats=time_stats_dict,
                           top_users=top_users_dict)


@app.route('/workspaces')
def view_workspaces():
    session = create_session()
    workspaces = session.query(Workspace).all()

    level_info_data = get_level_info_data()

    users_by_workspaces = {}
    for workspace in workspaces:
        users = [uw.user for uw in workspace.user_workspaces]
        users_by_workspaces.setdefault(workspace.name, []).extend(users)

    return render_template('workspaces.html',
                           workspaces=workspaces,
                           users_by_workspaces=users_by_workspaces,
                           data=level_info_data)


#  ======= CLUSTER =======
@app.route('/clusters/<string:cluster_id>')
def view_cluster(cluster_id):
    session = create_session()
    cluster = (session
               .query(Cluster)
               .filter(Cluster.cluster_id == cluster_id)
               .one())
    states = cluster.state_df()

    cost_summary, time_stats = aggregate_for_entity(states)

    return render_template('cluster.html',
                           cluster=cluster,
                           cost=cost_summary.to_dict(),
                           time_stats=time_stats.to_dict("records"))


@app.route('/clusters')
def view_clusters():
    session = create_session()
    clusters = session.query(Cluster).all()

    level_info_data = get_level_info_data()
    states = concat_dfs(cluster.state_df() for cluster in clusters)
    cost_summary, time_stats = aggregate_for_entity(states)
    cluster_dbus = (aggregate(df=states,
                              col="interval_dbu",
                              by="cluster_id",
                              since_days=7)
                    .rename(columns={'interval_dbu': 'dbu'})
                    .dbu.to_dict())

    return render_template('clusters.html',
                           clusters=clusters,
                           data=level_info_data,
                           cluster_dbus=cluster_dbus,
                           time_stats=time_stats.to_dict("records"))


#  ======= USER =======
@app.route('/users/<string:username>')
def view_user(username):
    session = create_session()
    user = (session
            .query(User)
            .filter(User.username == username)
            .one())
    states = user.state_df()

    cost_summary, time_stats = aggregate_for_entity(states)

    return render_template('user.html',
                           user=user,
                           cost=cost_summary.to_dict(),
                           time_stats=time_stats.to_dict("records"))


@app.route('/users')
def view_users():
    session = create_session()
    users = session.query(User).all()
    level_info_data = get_level_info_data()

    for user in users:
        user.dbu = aggregate(df=user.state_df(),
                             col='interval_dbu',
                             since_days=7)
    users = sorted(users, key=lambda x: x.dbu, reverse=True)
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

    print(active_users)
    print(dbus)

    return render_template('users.html',
                           users=users,
                           active_users=active_users.to_dict('records'),
                           data=level_info_data)


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


def format_datetime(value):
    return value.strftime("%Y-%m-%d %H:%M:%S")


#  ======= ALERTS =======
@app.route('/alerts')
def view_alerts():
    return render_template('alerts.html')


app.jinja_env.filters['datetime'] = format_datetime


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('command', type=str, help='command to run',
                        choices=["create_db", "scrape", "scrape_once"])
    parser.add_argument('-c', '--config', type=str,
                        help='path to config file to use', default="config.ini")
    args = parser.parse_args()
    command = args.command
    config = configparser.ConfigParser()
    config.read(args.config)
    log.info("Command: %s", command)
    log.debug("config path: %s", args.config)

    if command == "scrape":
        start_scheduled_scraping(config["scraper"].getfloat("interval"))
    elif command == "create_db":
        create_db()
    elif command == "scrape_once":
        scrape()
