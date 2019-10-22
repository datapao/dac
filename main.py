import os
from db import Cluster, Workspace, create_db, Base, engine_url
from databricks_api import DatabricksAPI
from flask import Flask, render_template
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import argparse


def scrape():
    print("Scraping started...")
    engine = create_engine(engine_url)
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    workspaces = [
        Workspace(
            url="dbc-b5882a77-2f55.cloud.databricks.com",
            id="dbc-b5882a77-2f55",
            type="AWS",
            name="Datapao Main",
            token=os.getenv("DATABRICKS_TOKEN_MAIN_AWS")
        ),
        Workspace(
            url="westeurope.azuredatabricks.net/?o=1950971732059748",
            id="1950971732059748",
            type="AZURE",
            name="Datapao Azure Main",
            token=os.getenv("DATABRICKS_TOKEN_MAIN_AZURE")
        ),
        Workspace(
            url="https://westeurope.azuredatabricks.net/?o=2381314298301659",
            id="2381314298301659",
            type="AZURE",
            name="Lidl",
            token=os.getenv("DATABRICKS_TOKEN_MAIN_LIDL")
        )
    ]
    for workspace in workspaces:
        session.merge(workspace)

        api = DatabricksAPI(host=workspace.url, token=workspace.token)
        clusters = api.cluster.list_clusters()
        for c in clusters["clusters"]:
            cluster = Cluster(
                id=c["cluster_id"],
                name=c["cluster_name"],
                state=c["state"],
                driver_type=c["driver_node_type_id"],
                worker_type=c["node_type_id"],
                num_workers=c["num_workers"],
                spark_version=c["spark_version"],
                creator_user_name=c["creator_user_name"],
                workspace_id=workspace.id
            )
            cluster.workspace_id = workspace.id
            session.merge(cluster)
            events = api.cluster.get_events(cluster_id=cluster.id)
            for event in events["events"]:
                pass
    session.commit()
    print("Scraping done")


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
    print(cluster_id)
    cluster = session.query(Cluster).filter(Cluster.id == cluster_id).one()
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
    args = parser.parse_args()
    command = args.command
    if command == "ui":
        app.run(debug=True)
    elif command == "create_db":
        create_db()
    elif command == "scrape":
        scrape()
