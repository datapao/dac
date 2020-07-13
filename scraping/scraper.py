import datetime
import functools
import json
import logging
import re
import threading
import time
import traceback

import requests
import pandas as pd

from databricks_api import DatabricksAPI
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from db import engine_url
from db import Base, Cluster, Workspace, Event, Job, JobRun
from db import User, UserWorkspace, ScraperRun, ClusterStates, ClusterType
from scraping.parser import parse_events, query_instance_types


log = logging.getLogger("dac-scraper")


def query_paginated(query_func, query_params, key):
    items = [query_func(**query_params)]
    while 'next_page' in items[-1].keys():
        offset = items[-1]['next_page']
        items.append(query_func(**offset))

    items = [i for item in items for i in item.get(key, [])]
    return items


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
    return None


def scrape_aws_instance_types(regex, columns):
    aws_url = "https://databricks.com/product/aws-pricing/instance-types"
    aws = pd.read_html(aws_url)[0].drop(columns=[0])
    aws.columns = columns
    aws['type'] = aws.type.str.extract(regex)
    return aws


def scrape_azure_instance_types(regex):
    column_mapping = {
        'Instance': 'type',
        'vCPU(s)': 'cpu',
        'Ram': 'mem',
        'Dbu Count': 'dbu_light'
    }
    azure_url = "https://azure.microsoft.com/en-us/pricing/details/databricks/"
    azure_dfs = [df for df in pd.read_html(azure_url)
                 if 'Dbu Count' in df.columns]
    azure = (pd.concat(azure_dfs, sort=False)
             [column_mapping.keys()]
             .rename(columns=column_mapping)
             .drop_duplicates()
             .assign(dbu_job=lambda df: df['dbu_light'],
                     dbu_analysis=lambda df: df['dbu_light']))
    azure['type'] = 'Standard_' + azure.type.str.replace(' ', '_')
    azure['type'] = azure.type.str.extract(regex)
    # TODO: find an elegant solution to this hotfix:
    # Azure site has some misspelled instance names:
    # Standard_F4, F8, F16 instead of Standard_F4s, F8s, F16s
    affected_rows = azure.type.str.match(r'Standard_F\d((?!_v\d).)*$')
    azure.loc[affected_rows, 'type'] = azure.loc[affected_rows, 'type'] + 's'
    return azure


def scrape_instance_types():
    regex = re.compile(r'(([a-z]\d[a-z]{0,2}.[\d]*[x]?large)|'
                       r'((Standard_|Premium_)'
                       r'[a-zA-Z]{1,2}\d+[a-zA-Z]?(_v\d*)?))')

    columns = ['type', 'cpu', 'mem',
               'dbu_light', 'dbu_job', 'dbu_analysis']

    # AWS parse
    try:
        aws = scrape_aws_instance_types(regex, columns)
    except Exception as e:
        aws = pd.DataFrame(columns=columns)
        log.exception(f'AWS instance type scraping failed with the following '
                      f'Exception:\n{e}\nTraceback:\n{traceback.format_exc()}')

    # AZURE parse
    try:
        azure = scrape_azure_instance_types(regex)
    except Exception as e:
        azure = pd.DataFrame(columns=columns)
        log.exception(f'Azure instance type scraping '
                      f'failed with the following Exception:\n{e}\n'
                      f'Traceback:\n{traceback.format_exc()}')

    # MERGE
    df = pd.concat([aws, azure]).reset_index(drop=True)
    df['scrape_time'] = datetime.datetime.today()

    print("AWS NULL ROWS:", aws.loc[aws.isnull().any(axis=1)])
    print("AZURE NULL ROWS:", azure.loc[azure.isnull().any(axis=1)])

    return df


def upsert_instance_types(session: "Session") -> pd.DataFrame:
    """Upserts instance types and returns latest data as a pandas DataFrame."""
    log.debug('Instance type scraping started...')
    today = datetime.datetime.today()
    one_month = datetime.timedelta(days=30)
    latest_instance_types = query_instance_types(session, as_df=False)

    if latest_instance_types is None:
        log.debug('Empty DB, scraping instance types...')
        instance_types = scrape_instance_types()
        for instance_type in instance_types.to_dict(orient='records'):
            session.merge(ClusterType(**instance_type))
        session.commit()
        log.debug('Instance type scraping done.')
        return instance_types
    else:
        log.debug('Existing records found in DB.')
        latest_instance_types_df = query_instance_types(session, as_df=True)
        latest_scrape_date = latest_instance_types_df.scrape_time.max()
        if latest_scrape_date < today - one_month:
            log.debug('Outdated DB, scraping instance types...')
            instance_types = scrape_instance_types()
            same_results = (
                instance_types
                .sort_values(by=['type', 'scrape_time'], ignore_index=True)
                .drop(columns='scrape_time')
                .equals(latest_instance_types_df
                        .sort_values(by=['type', 'scrape_time'],
                                     ignore_index=True)
                        .drop(columns='scrape_time'))
            )
            if same_results:
                log.debug('No updates found, updating timestamps...')
                for instance_type in latest_instance_types:
                    instance_type.scrape_time = today
            else:
                log.debug('New updates found, adding them to DB...')
                for instance_type in instance_types.to_dict(orient='records'):
                    session.merge(ClusterType(**instance_type))

            log.debug('Instance type scraping done.')
            session.commit()
            return instance_types

        log.debug('Instance type scraping done.')
        return latest_instance_types_df


def scrape_cluster(workspace, cluster_dict, instance_types,
                   session, api, result):
    log.debug(f"Scraping cluster: {cluster_dict['cluster_name']} "
              f"({cluster_dict['state']})")
    cluster = Cluster(
        cluster_id=cluster_dict["cluster_id"],
        cluster_name=cluster_dict["cluster_name"],
        state=cluster_dict["state"],
        state_message=cluster_dict["state_message"],
        driver_type=cluster_dict["driver_node_type_id"],
        worker_type=cluster_dict["node_type_id"],
        num_workers=cluster_dict.get("num_workers"),
        autoscale_min_workers=(cluster_dict
                               .get('autoscale', {})
                               .get('min_workers')),
        autoscale_max_workers=(cluster_dict
                               .get('autoscale', {})
                               .get('max_workers')),
        spark_version=cluster_dict["spark_version"],
        autotermination_minutes=cluster_dict.get("autotermination_minutes"),
        cluster_source=cluster_dict.get("cluster_source"),
        enable_elastic_disk=cluster_dict.get("enable_elastic_disk"),
        last_activity_time=to_time(cluster_dict.get("last_activity_time")),
        last_state_loss_time=to_time(cluster_dict.get("last_state_loss_time")),
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

    if "creator_user_name" in cluster_dict:
        cluster.creator_user_name = cluster_dict.get("creator_user_name")

    session.merge(cluster)
    result.num_clusters += 1
    log.debug(f"Started scraping events for cluster {cluster.cluster_name}")

    events = query_paginated(query_func=api.cluster.get_events,
                             query_params={'cluster_id': cluster.cluster_id},
                             key='events')
    for event in events:
        scrape_event(cluster, event, session, api, result)

    log.debug(f"Finished scraping events for cluster {cluster.cluster_name}. "
              f"Events: {len(events)}")

    # CLUSTER STATE PARSING
    # TODO: Events are not commited yet, so we cannot parse them.
    # Possible workarounds:
    # - move this functionality after events are in the db.
    #   problem: requires two db commits
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
    instance = job_run_dict.get("cluster_instance", {})
    state = job_run_dict.get("state", {})

    job_run = JobRun(
        job_id=job_run_dict["job_id"],
        run_id=job_run_dict["run_id"],
        number_in_job=job_run_dict["number_in_job"],
        original_attempt_run_id=job_run_dict["original_attempt_run_id"],
        cluster_spec=job_run_dict["cluster_spec"],
        cluster_type_id=(job_run_dict["cluster_spec"]
                         .get("new_cluster", {})
                         .get("node_type_id")),
        workspace_id=workspace.id,
        cluster_instance_id=instance.get("cluster_id"),
        spark_context_id=instance.get("spark_context_id"),
        state_life_cycle_state=state.get('life_cycle_state'),
        state_result_state=state.get('result_state'),
        state_state_message=state["state_message"],
        task=job_run_dict.get("task"),
        start_time=to_time(job_run_dict.get("start_time")),
        setup_duration=job_run_dict.get("setup_duration"),
        execution_duration=job_run_dict.get("execution_duration"),
        cleanup_duration=job_run_dict.get("cleanup_duration"),
        trigger=job_run_dict.get("trigger"),
        run_name=job_run_dict.get("run_name"),
        run_page_url=job_run_dict.get("run_page_url"),
        run_type=job_run_dict.get("run_type")
    )

    if "creator_user_name" in job_run_dict:
        job_run.creator_user_name = job_run_dict.get("creator_user_name")

    session.merge(job_run)
    result.num_job_runs += 1


def get_task_type(settings):
    task_types = ['notebook_task', 'spark_jar_task',
                  'spark_python_task', 'spark_submit_task']
    for task_type in task_types:
        if settings.get(task_type) is not None:
            return task_type.upper()

    log.warning("Couldn't determine job task type, falling back to 'UNKNOWN'.")
    return 'UNKNOWN'


def scrape_jobs(workspace, job_dict, session, api, result):
    log.debug(f"Scraping job, id: {job_dict['job_id']}")
    settings = job_dict.get("settings", {})
    job = Job(
        job_id=job_dict["job_id"],
        created_time=to_time(job_dict["created_time"]),
        name=settings.get("name", "Untitled"),
        workspace_id=workspace.id,
        max_concurrent_runs=settings.get("max_concurrent_runs", 1),
        timeout_seconds=settings.get("timeout_seconds"),
        email_notifications=settings.get("email_notifications", []),
        new_cluster=settings.get("new_cluster"),
        existing_cluster_id=settings.get("existing_cluster_id"),
        task_type=get_task_type(settings),
        task_parameters=settings.get(get_task_type(settings).lower(), {})
    )

    if "creator_user_name" in job_dict:
        job.creator_user_name = job_dict.get("creator_user_name")

    if "schedule" in settings:
        schedule = settings.get("schedule", {})
        job.schedule_quartz_cron_expression = schedule.get("quartz_cron_expression")
        job.schedule_timezone_id = schedule.get("timezone_id")

    if job.task_type == 'notebook':
        task = settings.get("notebook_task", {})
        job.notebook_path = task.get("notebook_path")
        job.notebook_revision_timestamp = task.get("revision_timestamp")

    session.merge(job)
    result.num_jobs += 1
    job_runs = query_paginated(api.jobs.list_runs,
                               {'job_id': job_dict["job_id"]},
                               'runs')

    log.debug(f"Scraping job runs for job_id: {job_dict['job_id']}")
    for job_run in job_runs:
        scrape_job_run(workspace, job_run, session, result)
    log.debug(f"Finished job_run scraping for job_id: {job_dict['job_id']}. "
              f"Runs scraped: {len(job_runs)}")


def scrape_user(workspace, user_dict, session, result):
    log.debug(f"Scraping user, id: {user_dict['id']}")
    name_dict = user_dict.get('name', {})
    user = User(
        username=user_dict.get('userName', 'UNKOWN'),
        name=' '.join([name_dict.get('givenName', ''),
                       name_dict.get('familyName', '')]),
        is_active=user_dict.get('active'),
        primary_email=list({email.get('value', '')
                            for email in user_dict['emails']
                            if email['primary']})[0],
    )
    session.merge(user)
    result.num_users += 1

    user_workspace = UserWorkspace(user_id=user_dict['id'],
                                   username=user.username,
                                   workspace_id=workspace.id)
    session.merge(user_workspace)


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
    for username, user in uniq_users.items():
        scrape_user(workspace, user, session, result)

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
    clusters = query_paginated(api.cluster.list_clusters, {}, 'clusters')
    for cluster in clusters:
        scrape_cluster(workspace, cluster, instance_types,
                       session, api, result)
    log.info(f"Finished scraping clusters in workspace {workspace.name}.")

    # JOBS
    log.info(f"Started scraping jobs in workspace {workspace.name}.")
    jobs = query_paginated(api.jobs.list_jobs, {}, 'jobs')
    for job in jobs:
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


def load_workspaces(json_path):
    with open(json_path, 'r') as json_file:
        workspaces = json.load(json_file)['workspaces']
    return workspaces


def get_workspaces(json_path):
    return [Workspace(**workspace) for workspace in load_workspaces(json_path)]


def create_session():
    engine = create_engine(engine_url)
    Base.metadata.bind = engine
    DBSession = scoped_session(sessionmaker(bind=engine, autoflush=False))
    session = DBSession()
    return session


def scraping_loop(interval: int, json_path: str, session: "Session"):
    while True:
        log.info("loop will go into another scraping")
        result = scrape(json_path, session)
        log.info(f"Scraping {result.scraper_run_id[:8]} finished.")
        log.debug(f"Going to sleep for {interval} seconds")
        time.sleep(interval)


def start_scheduled_scraping(interval: int, json_path: str) -> threading.Thread:
    session = create_session()
    thread = threading.Thread(target=scraping_loop,
                              name="scraping-loop-Thread",
                              args=[interval, json_path, session])
    thread.start()
    return thread


def scrape(json_path, session=None):
    log.info("Scraping started...")
    start_time = time.time()

    if session is None:
        session = create_session()

    instance_types = upsert_instance_types(session)

    scraping_results = []
    for workspace in get_workspaces(json_path):
        result = scrape_workspace(workspace, session, instance_types)
        scraping_results.append(result)
        session.commit()

    final_result = functools.reduce(
        ScraperRun.merge, scraping_results, ScraperRun.empty())

    session.add(final_result)
    session.commit()

    log.info(f"Scraping done. Duration: {time.time() - start_time:.2f}", )
    return final_result
