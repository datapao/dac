import os
import logging
from databricks_api import DatabricksAPI
from db import Cluster, Workspace, Base, engine_url
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import datetime

log = logging.getLogger("dac-scraper")


def write_to_file(cluster):
    import json
    with open("api-data.txt", "a") as f:
        f.write(json.dumps(cluster) + "\n")


def scrape_event(cluster, event, session, api):
    pass


def to_time(t):
    if t is not None:
        return datetime.datetime.utcfromtimestamp(t/1000)
    else:
        return None


def scrape_cluster(workspace, cluster_dict, session, api):
    logging.debug("Scraping cluster: %s (%s)",
                  cluster_dict["cluster_name"], cluster_dict["state"])
    cluster = Cluster(
        cluster_id=cluster_dict["cluster_id"],
        cluster_name=cluster_dict["cluster_name"],
        state=cluster_dict["state"],
        state_message=cluster_dict["state_message"],
        driver_type=cluster_dict["driver_node_type_id"],
        worker_type=cluster_dict["node_type_id"],
        num_workers=cluster_dict["num_workers"],
        spark_version=cluster_dict["spark_version"],
        creator_user_name=cluster_dict["creator_user_name"],
        autotermination_minutes=cluster_dict["autotermination_minutes"],
        cluster_source=cluster_dict["cluster_source"],
        enable_elastic_disk=cluster_dict["enable_elastic_disk"],
        last_activity_time=to_time(
            cluster_dict.get("last_activity_time", None)),
        last_state_loss_time=to_time(cluster_dict["last_state_loss_time"]),
        pinned_by_user_name=cluster_dict.get("pinned_by_user_name", None),
        spark_context_id=cluster_dict["spark_context_id"],
        start_time=to_time(cluster_dict["start_time"]),
        terminated_time=to_time(cluster_dict.get("terminated_time", None)),
        workspace_id=workspace.id
    )
    if "termination_reason" in cluster_dict:
        cluster.termination_reason_code = cluster_dict["termination_reason"]["code"]
        cluster.termination_reason_inactivity_min = cluster_dict[
            "termination_reason"]["parameters"].get("inactivity_duration_min", None)
        cluster.termination_reason_username = cluster_dict[
            "termination_reason"]["parameters"].get("username", None)

    session.merge(cluster)
    events = api.cluster.get_events(cluster_id=cluster.cluster_id)
    for event in events["events"]:
        scrape_event(cluster, event, session, api)


def scrape_workspace(workspace, session):
    logging.info("Scraping workspace %s", workspace.name)
    session.merge(workspace)

    api = DatabricksAPI(host=workspace.url, token=workspace.token)
    clusters = api.cluster.list_clusters()

    for cluster in clusters["clusters"]:
        scrape_cluster(workspace, cluster, session, api)


def get_workspaces():
    return [
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
            url="westeurope.azuredatabricks.net/?o=2381314298301659",
            id="2381314298301659",
            type="AZURE",
            name="Lidl",
            token=os.getenv("DATABRICKS_TOKEN_MAIN_LIDL")
        )
    ]


def scrape():
    log.info("Scraping started...")

    engine = create_engine(engine_url)
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    for workspace in get_workspaces():
        scrape_workspace(workspace, session)

    session.commit()
    logging.info("Scraping done")
