import re
import time
import logging
import functools

from datetime import timedelta

import pandas as pd

from db import engine_url
from db import Base, Cluster, Event


log = logging.getLogger("dac-parser")


class EventParser:

    events = [
        # Custom starting event
        "INIT",
        # Indicates that the cluster is being created.
        "CREATING",
        # Indicates that a disk is low on space, but adding disks would
        # put it over the max capacity.
        "DID_NOT_EXPAND_DISK",
        # Indicates that a disk was low on space and the disks were expanded.
        "EXPANDED_DISK",
        # Indicates that a disk was low on space and disk space
        # could not be expanded.
        "FAILED_TO_EXPAND_DISK",
        # Indicates that the cluster scoped init script has started.
        "INIT_SCRIPTS_STARTING",
        # Indicates that the cluster scoped init script has finished.
        "INIT_SCRIPTS_FINISHED",
        # Indicates that the cluster is being started.
        "STARTING",
        # Indicates that the cluster is being started.
        "RESTARTING",
        # Indicates that the cluster is being terminated.
        "TERMINATING",
        # Indicates that the cluster has been edited.
        "EDITED",
        # Indicates the cluster has finished being created.
        # Includes the number of nodes in the cluster and a failure reason
        # if some nodes could not be acquired.
        "RUNNING",
        # Indicates a change in the target size of the cluster
        # (upsize or downsize).
        "RESIZING",
        # Indicates that nodes finished being added to the cluster.
        # Includes the number of nodes in the cluster and a failure reason
        # if some nodes could not be acquired.
        "UPSIZE_COMPLETED",
        # Indicates that some nodes were lost from the cluster.
        "NODES_LOST",
        # Indicates that the driver is healthy and the cluster is ready for use.
        "DRIVER_HEALTHY",
        # Indicates that the driver is unavailable.
        "DRIVER_UNAVAILABLE",
        # Indicates that a Spark exception was thrown from the driver.
        "SPARK_EXCEPTION",
        # Indicates that the driver is up but is not responsive,
        # likely due to GC.
        "DRIVER_NOT_RESPONDING",
        # Indicates that the driver is up but DBFS is down.
        "DBFS_DOWN",
        # Indicates that the driver is up but the metastore is down.
        "METASTORE_DOWN",
        # Usage report containing the total and unused instance minutes
        # of the autoscaling cluster over the last hour.
        "AUTOSCALING_STATS_REPORT",
        # Indicates that a node has been blacklisted by Spark.
        "NODE_BLACKLISTED",
        # Indicates that the cluster was pinned.
        "PINNED",
        # Indicates that the cluster was unpinned.
        "UNPINNED",
    ]
    states = [
        'UNKNOWN',
        'RUNNING',
        'STOPPED'
    ]
    transitions = {
        "CREATING": 'RUNNING',
        "STARTING": 'RUNNING',
        "RESTARTING": 'RUNNING',
        "TERMINATING": 'STOPPED',
        "RUNNING": 'RUNNING',
    }
    instance_type_regex = re.compile(r'([a-z]\d[a-z]?.[\d]*[x]?large)')

    def __init__(self, instance_types: pd.DataFrame) -> None:
        self.instance_type_map = instance_types

    def parse(self, events: list, clusters: dict) -> pd.DataFrame:
        timeline = self.process_events(events)
        result = self.process_timelines(timeline, clusters)
        return result

    def process_events(self, events: list) -> list:
        timeline = []
        for event in events:
            timeline.append(self.process_event(event))
        return timeline

    def process_event(self, event: dict) -> dict:
        etype = event.get('type')
        if etype not in self.events:
            raise ValueError(f'Unkown event: {event}\n'
                             f'Recognized events are: {self.events}')

        details = event.get('details', {})
        user = details.get('user')
        num_workers = details.get('current_num_workers')

        # CREATED / EDITED event only
        attributes = details.get('attributes', {})
        cluster_name = attributes.get('cluster_name')
        driver_type = attributes.get('driver_node_type_id')
        worker_type = attributes.get('node_type_id')

        return {'timestamp': event.get('timestamp', 0),
                'cluster_id': event.get('cluster_id'),
                'user': user,
                'event': etype,
                'driver_type': driver_type,
                'worker_type': worker_type,
                'num_workers': num_workers}

    def process_timelines(self,
                          raw_timeline: list,
                          clusters: dict) -> pd.DataFrame:
        timelines = {}
        for event in raw_timeline:
            cluster = event['cluster_id']
            if cluster not in timelines:
                timelines[cluster] = []
            timelines[cluster].append(event)

        dfs = []
        for cluster_id, timeline in timelines.items():
            cluster_name = clusters.get(cluster_id, 'UNKNOWN')
            dfs.append(self.process_timeline(timeline, cluster_name))

        return pd.concat(dfs)

    def process_timeline(self,
                         timeline: list,
                         cluster_name: str) -> pd.DataFrame:
        # Empty timeline
        if not len(timeline):
            return pd.DataFrame()

        sorted_timeline = sorted(timeline, key=lambda x: x['timestamp'])

        # initial event
        first = sorted_timeline[0]
        init = {key: None for key in first.keys()}
        init['timestamp'] = first['timestamp']
        init['event'] = 'INIT'

        timeline = [init] + sorted_timeline

        frm = timeline[:-1]
        to = timeline[1:]

        rows = []
        status = {
            'timestamp': first['timestamp'],
            'cluster_id': first['cluster_id'],
            'state': 'UNKNOWN',
            'user_id': 'UNKONWN',
            'driver_type': first['driver_type'],
            'worker_type': first['worker_type'],
            'num_workers': 0,
            'interval': 0
        }
        for frm_event, to_event in zip(frm, to):
            delta = to_event['timestamp'] - frm_event['timestamp']
            delta = timedelta(milliseconds=delta)
            delta = delta.seconds / 3600

            row = status.copy()
            row['interval'] = delta
            rows.append(row)

            status = self.get_new_status(status, to_event)

        cluster_type = self.determine_cluster_type(cluster_name)
        df = pd.DataFrame(rows)
        df['dbu'] = self.calculate_dbu(df, cluster_type)

        return df

    def get_new_state(self, current_state: dict, event: dict) -> str:
        return self.transitions.get(event, current_state)

    def get_new_status(self, actual: dict, event: dict) -> dict:
        """
        Update status with the new values from the event.
        If a new event doesn't have a new value, use the actual values
        """
        status = {}
        for key, value in actual.items():
            if key == 'state':
                status[key] = self.get_new_state(actual['state'],
                                                 event['event'])
            else:
                status[key] = event.get(key, value) or actual[key]

        return status

    def determine_cluster_type(self, cluster_name: str) -> str:
        if cluster_name.startswith('job-'):
            return 'job'

        if cluster_name.startswith('light-'):
            return 'light'

        return 'analysis'

    def calculate_dbu(self,
                      df: pd.DataFrame,
                      cluster_type: str = 'analysis') -> pd.Series:
        if cluster_type not in ['light', 'job', 'analysis']:
            raise ValueError(f'Unrecognized cluster type {cluster_type} '
                             f'during DBU computation.')

        clusters = df[['driver_type', 'worker_type', 'num_workers']].copy()
        clusters['driver_type'] = (clusters
                                   .driver_type
                                   .str
                                   .extract(self.instance_type_regex))
        clusters['worker_type'] = (clusters
                                   .worker_type
                                   .str
                                   .extract(self.instance_type_regex))

        mapping = self.instance_type_map[['type', 'cpu', 'mem', cluster_type]]

        joined = (
            clusters
            .merge(mapping
                   .rename(columns={col: f'driver_{col}'
                                    for col in mapping.columns}),
                   on=['driver_type'])
            .merge(mapping
                   .rename(columns={col: f'worker_{col}'
                                    for col in mapping.columns}),
                   on='worker_type')
        )

        # TODO: Decide wether we need to set the driver count to 0 when
        # there are no workers or not?
        return (joined[f'driver_{cluster_type}']
                # * (joined['num_workers'] > 0).astype('int')
                + joined[f'worker_{cluster_type}']
                * joined['num_workers'])

@functools.lru_cache(maxsize=None)
def query_instance_types() -> pd.DataFrame:
    # TODO 1: save locally and check if we can parse the actual page
    # TODO 2: parse azure and databricks specific machines as well
    # (eg. Standard_DS3_v2) - related todo in db/db.py:224
    regex = r'([a-z]\d[a-z]?.[\d]*[x]?large)'

    url = "https://databricks.com/product/aws-pricing/instance-types"
    df = pd.read_html(url)[0].drop(columns=[6])
    df.columns = ['type', 'cpu', 'mem', 'light', 'job', 'analysis']
    df['type'] = df.type.str.extract(regex)

    return df


def query_events(session):
    return [event.__dict__ for event in session.query(Event).all()]


def query_cluster_names(session: "Session") -> dict:
    clusters = (session
                .query(Cluster.cluster_id, Cluster.cluster_name)
                .distinct()
                .all())
    return {cluster.cluster_id: cluster.cluster_name for cluster in clusters}


def parse_events(session: "Session", events: list) -> pd.DataFrame:
    log.info("Parsing started...")
    start_time = time.time()

    # Querying required info from db / web
    # events = query_events(session)  # related todo in scraping/scraper.py:102
    cluster_names = query_cluster_names(session)
    instance_types = query_instance_types()

    # Parsing
    parser = EventParser(instance_types)
    result = parser.parse(events, cluster_names)

    log.info(f"Parsing done. Duration: {time.time() - start_time:.2f}")
    return result
