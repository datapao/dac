from db import Cluster, Workspace, create_db, Base, engine_url
from flask import Flask, render_template
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from scraping import scrape
import argparse
import logging
import configparser

logformat = "%(asctime)-15s %(name)-12s %(levelname)-8s %(message)s"
logging.basicConfig(level=logging.DEBUG, format=logformat)
log = logging.getLogger("dac-ui")
logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)

app = Flask(__name__)
engine = create_engine(engine_url)
Base.metadata.bind = engine


def create_session():
    DBSession = sessionmaker()
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
    return render_template('cluster.html', cluster=cluster)


@app.route('/clusters')
def view_clusters():
    session = create_session()
    clusters = session.query(Cluster).all()
    for cluster in clusters:
        cluster.dbu_cost_per_hour = "$6.78"
        cluster.hw_cost_per_hour = "$19.12"
        cluster.cost_per_hour = "$25.9"
    return render_template('clusters.html', clusters=clusters)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('command', type=str, help='command to run', choices=[
                        "create_db", "ui", "scrape"])
    parser.add_argument('-c', '--config', type=str,
                        help='path to config file to use', default="config.ini")
    args = parser.parse_args()
    command = args.command
    config = configparser.ConfigParser()
    config.read(args.config)
    log.info("Command: %s", command)
    log.debug("config path: %s", args.config)
    if command == "ui":
        app.run(debug=config["web"].getboolean("development"))
    elif command == "create_db":
        create_db()
    elif command == "scrape":
        scrape()
