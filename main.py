import argparse
import configparser
import logging

from datetime import datetime, timedelta

from flask import Flask, render_template
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, scoped_session

from db import Cluster, Workspace, create_db, Base, engine_url, ScraperRun
from scraping import scrape, start_scheduled_scraping
import pandas as pd


logformat = "%(asctime)-15s %(name)-12s %(levelname)-8s %(message)s"
logging.basicConfig(level=logging.DEBUG, format=logformat)
log = logging.getLogger("dac")
logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)


app = Flask(__name__)
engine = create_engine(engine_url)
Base.metadata.bind = engine


def create_session():
    DBSession = scoped_session(sessionmaker())
    DBSession.bind = engine
    session = DBSession()
    return session


@app.route('/')
def view_dashboard():
    data = {
        "clusters": 45,
        "workspaces": 3,
        "daily_dbu": 56.214,
        "daily_vm": 433.963
    }
    return render_template('dashboard.html', data=data)


@app.route('/workspaces/<string:workspace_id>')
def view_workspace(workspace_id):
    session = create_session()
    workspace = session.query(Workspace).filter(
        Workspace.id == workspace_id).one()
    return render_template('workspace.html', workspace=workspace)


@app.route('/workspaces')
def view_workspaces():
    session = create_session()
    workspaces = session.query(Workspace).all()
    return render_template('workspaces.html', workspaces=workspaces)


@app.route('/alerts')
def view_alerts():
    return render_template('alerts.html')


@app.route('/users')
def view_users():
    return render_template('users.html')


@app.route('/clusters/<string:cluster_id>')
def view_cluster(cluster_id):
    session = create_session()
    cluster = session.query(Cluster).filter(
        Cluster.cluster_id == cluster_id).one()
    states = cluster.state_df()
    states["worker_hours"] = states["interval"] * states["num_workers"]
    states.fillna(0, inplace=True)

    cost_summary = states.agg({
        "interval_dbu": "sum",
        "interval": "sum"
    })

    time_stats = states.groupby(pd.Grouper(key="timestamp", freq="1D", label="right")).agg({
        "dbu": "max",
        "interval": "sum",
        "interval_dbu": "sum",
        "num_workers": ["min", "max", "median"],
        "worker_hours": "sum"
    })
    full_index = pd.date_range(datetime.today()-timedelta(days=30), datetime.today(), freq="1D", normalize=True)
    time_stats = time_stats.reindex(full_index)
    time_stats.fillna(0, inplace=True)
    time_stats.columns = ['_'.join(col) for col in time_stats.columns.values]

    weekly_cost_stats = time_stats[time_stats.index >= datetime.today() - timedelta(days=7)].agg({
        "interval_dbu_sum": "sum",
        "interval_sum": "sum"
    })
    weekly_cost_stats.index = [f"weekly_{p}" for p in weekly_cost_stats.index.values]
    cost_summary = pd.concat([cost_summary, weekly_cost_stats])
    
    time_stats.index = time_stats.index.format()
    time_stats["ts"] = time_stats.index
    return render_template('cluster.html',
                           cluster=cluster,
                           cost=cost_summary.to_dict(),
                           time_stats=time_stats.to_dict("records"))


@app.route('/clusters')
def view_clusters():
    session = create_session()
    clusters = session.query(Cluster).all()
    for cluster in clusters:
        cluster.dbu_cost_per_hour = "$6.78"
        cluster.hw_cost_per_hour = "$19.12"
        cluster.cost_per_hour = "$25.9"
    return render_template('clusters.html', clusters=clusters)


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
    parser.add_argument('command', type=str, help='command to run', choices=[
                        "create_db", "scrape", "scrape_once"])
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
