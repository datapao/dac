import argparse
import configparser
import logging
import functools

from datetime import datetime, timedelta

import pandas as pd

from flask import Flask, render_template
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, scoped_session

from aggregation import concat_dfs, get_time_index, get_time_grouper
from aggregation import aggregate, get_cluster_dbus, aggregate_for_entity
from db import engine_url, create_db, Base, Workspace, Cluster, Job, ScraperRun
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
    return render_template('workspaces.html',
                           workspaces=workspaces,
                           data=level_info_data)


@app.route('/alerts')
def view_alerts():
    return render_template('alerts.html')


@app.route('/users')
def view_users():
    return render_template('users.html')


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
