import datetime
import functools
import logging
import os
import threading
import time
import json

import requests
import pandas as pd

from databricks_api import DatabricksAPI
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from db import engine_url
from db import Base, Cluster, Workspace, Event, Job, JobRun, User
from db import ScraperRun, ClusterStates
from scraping.parser import parse_events, query_instance_types


log = logging.getLogger("dac-scraper")


def scrape_event(cluster, event_dict, session, api, result):
    event = Event(
        cluster_id=event_dict["cluster_id"],
        timestamp=to_time(event_dict["timestamp"]),
        type=event_dict["type"],
        details=event_dict["details"]
    )
    session.merge(event)
    result.num_events += 1


def upsert_states(session: "Session", df: pd.DataFrame) -> int:
    for counter, row in enumerate(df.to_dict(orient='records')):
        row['timestamp'] = pd.to_datetime(row['timestamp'], unit='ms')
        session.merge(ClusterStates(**row))
    return counter


def to_time(t):
    if t is not None:
        return datetime.datetime.utcfromtimestamp(t / 1000)
    else:
        return None


def scrape_cluster(workspace, cluster_dict, instance_types, session, api, result):
    log.debug(f"Scraping cluster: {cluster_dict['cluster_name']} "
              f"({cluster_dict['state']})")
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
        autotermination_minutes=cluster_dict.get("autotermination_minutes"),
        cluster_source=cluster_dict.get("cluster_source"),
        enable_elastic_disk=cluster_dict.get("enable_elastic_disk"),
        last_activity_time=to_time(
            cluster_dict.get("last_activity_time")),
        last_state_loss_time=to_time(
            cluster_dict.get("last_state_loss_time")),
        pinned_by_user_name=cluster_dict.get("pinned_by_user_name"),
        spark_context_id=cluster_dict.get("spark_context_id"),
        start_time=to_time(cluster_dict["start_time"]),
        terminated_time=to_time(cluster_dict.get("terminated_time")),
        workspace_id=workspace.id,
        default_tags=cluster_dict["default_tags"],
        aws_attributes=cluster_dict.get("aws_attributes"),
        spark_conf=cluster_dict.get("spark_conf"),
        spark_env_vars=cluster_dict.get("spark_env_vars")
    )
    if "termination_reason" in cluster_dict:
        reason = cluster_dict["termination_reason"]
        cluster.termination_reason_code = reason["code"]
        if "parameters" in reason:
            params = reason["parameters"]
            cluster.termination_reason_inactivity_min = params.get("inactivity_duration_min")
            cluster.termination_reason_username = params.get("username")

    session.merge(cluster)
    result.num_clusters += 1
    log.debug(f"Started scraping events for cluster {cluster.cluster_name}")
    events = (api.cluster
              .get_events(cluster_id=cluster.cluster_id)
              .get('events', []))
    for event in events:
        scrape_event(cluster, event, session, api, result)

    log.debug(f"Finished scraping events for cluster {cluster.cluster_name}. "
              f"Events: {len(events)}")

    # CLUSTER STATE PARSING
    # TODO: Events are not commited yet, so we cannot parse them.
    # Possible workarounds:
    # - move this functionality after events are in the db.
    #   problem: requires two commits
    # - change ClusterState to use raw events instead of querying it from the
    #   db. affected functions: parser.py/parse_events, parser.py/query_events
    #   this option is implemented currently, we should consider other options.
    parsed_events = parse_events(session, events, instance_types)
    affected_rows = upsert_states(session, parsed_events)

    log.debug(f"Finished parsing events for cluster {cluster.cluster_name}. "
              f"Affected rows: {affected_rows}")


def scrape_job_run(workspace, job_run_dict, session, result):
    log.debug(f"Scraping job run in workspace: {workspace.name} "
              f"job ({job_run_dict['job_id']}) "
              f"run id: {job_run_dict['run_id']}")
    instance = job_run_dict["cluster_instance"]
    state = job_run_dict["state"]
    job_run = JobRun(
        job_id=job_run_dict["job_id"],
        run_id=job_run_dict["run_id"],
        number_in_job=job_run_dict["number_in_job"],
        original_attempt_run_id=job_run_dict["original_attempt_run_id"],
        cluster_spec=job_run_dict["cluster_spec"],
        workspace_id=workspace.id,
        cluster_instance_id=instance["cluster_id"],
        spark_context_id=instance.get("spark_context_id"),
        state_life_cycle_state=state["life_cycle_state"],
        state_result_state=state["result_state"],
        state_state_message=state["state_message"],
        task=job_run_dict["task"],
        start_time=to_time(job_run_dict["start_time"]),
        setup_duration=job_run_dict["setup_duration"],
        execution_duration=job_run_dict["execution_duration"],
        cleanup_duration=job_run_dict["cleanup_duration"],
        trigger=job_run_dict["trigger"],
        creator_user_name=job_run_dict["creator_user_name"],
        run_name=job_run_dict["run_name"],
        run_page_url=job_run_dict["run_page_url"],
        run_type=job_run_dict["run_type"]
    )
    session.merge(job_run)
    result.num_job_runs += 1


def scrape_jobs(workspace, job_dict, session, api, result):
    log.debug(f"Scraping job, id: {job_dict['job_id']}")
    job = Job(
        job_id=job_dict["job_id"],
        created_time=to_time(job_dict["created_time"]),
        creator_user_name=job_dict["creator_user_name"],
        name=job_dict["settings"]["name"],
        workspace_id=workspace.id,
        max_concurrent_runs=job_dict["settings"]["max_concurrent_runs"],
        timeout_seconds=job_dict["settings"]["timeout_seconds"],
        email_notifications=job_dict["settings"]["email_notifications"],
        new_cluster=job_dict["settings"]["new_cluster"],
        schedule_quartz_cron_expression=job_dict["settings"].get(
            "schedule", {}).get("quartz_cron_expression", None),
        schedule_timezone_id=job_dict["settings"].get(
            "schedule", {}).get("timezone_id", None),
        task_type="NOTEBOOK_TASK",
        notebook_path=job_dict["settings"].get(
            "notebook_task", {}).get("notebook_path", None),
        notebook_revision_timestamp=job_dict["settings"].get(
            "notebook_task", {}).get("revision_timestamp", None),
    )
    session.merge(job)
    result.num_jobs += 1
    job_runs_response = api.jobs.list_runs(
        job_id=job_dict["job_id"], limit=120)
    job_runs = job_runs_response.get("runs", [])
    log.debug(f"Scraping job runs for job_id: {job_dict['job_id']}")
    for job_run in job_runs:
        scrape_job_run(workspace, job_run, session, result)
    log.debug(f"Finished job_run scraping for job_id: {job_dict['job_id']}. "
              f"Runs scraped: {len(job_runs)}")


def scrape_user(user_dict, session, result):
    log.debug(f"Scraping user, id: {user_dict['id']}")
    name_dict = user_dict.get('name', {})
    user = User(
        user_id=user_dict['id'],
        username=user_dict.get('userName', 'UNKOWN'),
        name=' '.join([name_dict.get('givenName', ''),
                       name_dict.get('familyName', '')]),
        is_active=user_dict.get('active'),
        groups=', '.join(group.get('$ref', '')
                         for group in user_dict['groups']),
        primary_email=list({email.get('value', '')
                            for email in user_dict['emails']
                            if email['primary']})[0],
        emails=', '.join(email.get('value', '')
                         for email in user_dict['emails'])
    )

    session.merge(user)
    result.num_users += 1


def scrape_users(workspace, session, result):
    log.debug(f"Scraping users for {workspace.name} workspace.")

    url, *_ = workspace.url.rsplit('/', 1)
    api_path = f"https://{url}/api/2.0/preview/scim/v2/Users"
    headers = {
        "Authorization": f"Bearer {workspace.token}",
        "Content-Type": "application/scim+json",
        "Accept": "application/scim+json"
    }
    resp = requests.get(api_path, headers=headers)
    raw = json.loads(resp.text) if resp.text else {}
    users = raw.get('Resources', [])

    uniq_users = {user['userName']: user for user in users}
    for username, user in uniq_users:
        scrape_user(user, session, result)
        UserWorkspace(username=username, workspace_id=user[workspace.id])

    log.debug(f"Finished users scraping for workspace: {workspace.name}. "
              f"Users scraped: {len(users)}")


def scrape_workspace(workspace, session, instance_types):
    log.info(f"Scraping workspace {workspace.name}, {workspace.url}")
    result = ScraperRun.empty()
    result.start()
    session.merge(workspace)
    result.num_workspaces += 1

    api = DatabricksAPI(host=workspace.url, token=workspace.token)

    # CLUSTERS
    log.info(f"Started scraping clusters in workspace {workspace.name}.")
    clusters = api.cluster.list_clusters()
    for cluster in clusters.get("clusters", []):
        scrape_cluster(workspace, cluster, instance_types, session, api, result)
    log.info(f"Finished scraping clusters in workspace {workspace.name}.")

    # JOBS
    log.info(f"Started scraping jobs in workspace {workspace.name}.")
    jobs = api.jobs.list_jobs()
    for job in jobs.get("jobs", []):
        scrape_jobs(workspace, job, session, api, result)
    log.info(f"Finished scraping jobs in workspace {workspace.name}. "
             f"Jobs scraped: {len(jobs)}")

    # USERS
    log.info(f"Started scraping users in workspace {workspace.name}.")
    scrape_users(workspace, session, result)
    log.info(f"Finished scraping users in workspace {workspace.name}. "
             f"Users scraped: {result.num_users}")

    result.finish(ScraperRun.SUCCESSFUL)
    return result


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


def scraping_loop(interval: int):
    while True:
        log.info("loop will go into another scraping")
        result = scrape()
        log.info(f"Scraping {result.scraper_run_id[:8]} finished.")
        log.debug(f"Going to sleep for {interval} seconds")
        time.sleep(interval)


def start_scheduled_scraping(interval: int) -> threading.Thread:
    thread = threading.Thread(target=scraping_loop,
                              name="scraping-loop-Thread", args=[interval])
    thread.start()
    return thread


def scrape():
    log.info("Scraping started...")
    start_time = time.time()

    engine = create_engine(engine_url)
    Base.metadata.bind = engine
    DBSession = scoped_session(sessionmaker(bind=engine, autoflush=False))
    session = DBSession()

    instance_types = query_instance_types()

    scraping_results = []
    for workspace in get_workspaces():
        result = scrape_workspace(workspace, session, instance_types)
        scraping_results.append(result)

    final_result = functools.reduce(
        ScraperRun.merge, scraping_results, ScraperRun.empty())

    session.add(final_result)
    session.commit()
    log.info(f"Scraping done. Duration: {time.time() - start_time:.2f}", )
    return result
