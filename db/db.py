import logging

from uuid import uuid4
from datetime import datetime

import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy import Column, ForeignKey
from sqlalchemy import String, Integer, BigInteger, Float
from sqlalchemy import DateTime, Boolean, JSON
from sqlalchemy.schema import ForeignKeyConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from aggregation import since


log = logging.getLogger("dac-db")


engine_url = 'sqlite:///dac.db'
Base = declarative_base()


class User(Base):
    __tablename__ = 'users'

    username = Column(String, primary_key=True)
    name = Column(String)
    is_active = Column(Boolean)
    primary_email = Column(String)

    user_workspaces = relationship("UserWorkspace", back_populates='user')

    def state_df(self):
        df = (pd.concat([workspace.workspace.state_df()
                         for workspace in self.user_workspaces], sort=False)
              .sort_values('timestamp'))
        return df.loc[df.user_id == self.username] if not df.empty else df

    def active(self, since_days=7):
        states = self.state_df()
        states = states.loc[states.timestamp >= since(since_days)]
        return not states.empty

    def dbu(self, since_days=7):
        states = self.state_df()
        return (states
                .loc[states.timestamp >= since(since_days)]
                .interval_dbu
                .sum())


class UserWorkspace(Base):
    __tablename__ = 'user_workspaces'

    user_id = Column(String, nullable=False)
    username = Column(String, ForeignKey("users.username"),
                      primary_key=True)
    workspace_id = Column(String, ForeignKey("workspaces.id"),
                          primary_key=True)

    user = relationship('User', back_populates='user_workspaces')
    workspace = relationship('Workspace', back_populates='user_workspaces')


class Cluster(Base):
    __tablename__ = "clusters"

    cluster_id = Column(String, primary_key=True)
    cluster_name = Column(String, nullable=False)
    state = Column(String, nullable=False)
    state_message = Column(String, nullable=False)
    driver_type = Column(String, ForeignKey("cluster_types.type"))
    worker_type = Column(String, ForeignKey("cluster_types.type"))
    num_workers = Column(Integer)
    autoscale_min_workers = Column(Integer)
    autoscale_max_workers = Column(Integer)
    spark_version = Column(String, nullable=False)
    creator_user_name = Column(String, ForeignKey("users.username"),
                               nullable=True)
    workspace_id = Column(String, ForeignKey("workspaces.id"), nullable=False)
    autotermination_minutes = Column(Integer)
    cluster_source = Column(String)
    enable_elastic_disk = Column(Boolean)
    last_activity_time = Column(DateTime)
    last_state_loss_time = Column(DateTime)
    pinned_by_user_name = Column(String, ForeignKey("users.username"))
    spark_context_id = Column(BigInteger)
    spark_version = Column(String)
    start_time = Column(DateTime, nullable=False)
    terminated_time = Column(DateTime)
    termination_reason_code = Column(String)
    termination_reason_inactivity_min = Column(String)
    termination_reason_username = Column(String)
    default_tags = Column(JSON, nullable=False)
    aws_attributes = Column(JSON)
    spark_conf = Column(JSON)
    spark_env_vars = Column(JSON)

    workspace = relationship("Workspace")
    events = relationship("Event")
    cluster_states = relationship("ClusterStates")

    def eventFilterNot(self, types):
        sorted_events = sorted(self.events, key=lambda x: x.timestamp)
        return [e for e in sorted_events if e.type not in types]

    def state_df(self):
        df = (pd.DataFrame([state.to_dict() for state in self.cluster_states])
              .sort_values('timestamp'))
        df['cluster_type'] = self.cluster_type()
        df["interval_dbu"] = df["dbu"] * df["interval"]

        return df

    def users(self, active_only=False):
        return self.workspaces.users(active_only)

    def dbu_per_hour(self):
        df = self.state_df()
        return df.loc[df.state.isin(['RUNNING']), 'dbu'].iloc[-1] or 0.0

    def cluster_type(self):
        return 'job' if self.cluster_name.startswith('job') else 'interactive'

    def cost_per_hour(self, price_config):
        return self.dbu_per_hour() * price_config[self.cluster_type()]


class Workspace(Base):
    __tablename__ = "workspaces"
    __attributes__ = ['id', 'name', 'url', 'type', 'token']

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    url = Column(String, nullable=False)
    # AWS or AZURE
    type = Column(String, nullable=False)
    token = Column(String, nullable=False)

    clusters = relationship(Cluster)
    user_workspaces = relationship('UserWorkspace', back_populates='workspace')
    jobruns = relationship('JobRun')

    def active_clusters(self):
        return [cluster for cluster in self.clusters
                if cluster.state in ["RUNNING", "PENDING"]]

    def state_df(self, active_only=False):
        clusters = self.clusters if not active_only else self.active_clusters()

        if not clusters:
            columns = ClusterStates.__attributes__ + ['interval_dbu']
            return pd.DataFrame(columns=columns)

        df = (pd.concat([cluster.state_df() for cluster in clusters],
                        sort=False)
              .sort_values('timestamp')
              .reset_index(drop=True))

        return df

    def users(self, with_id=False, active_only=False):
        users = [uw.user for uw in self.user_workspaces]

        if active_only:
            users = [user for user in users if user.active()]

        if with_id:
            usernames = [user.username for user in users]
            users = [(uw.user_id, uw.user) for uw in self.user_workspaces
                     if uw.user.username in usernames]

        return users

    def dbu(self, since_days=7):
        states = self.state_df()
        return (states
                .loc[states.timestamp >= since(since_days)]
                .interval_dbu
                .sum())

    def dbu_per_hour(self):
        return sum([cluster.dbu_per_hour() for cluster in self.clusters])

    def cost_per_hour(self, price_config):
        return sum([cluster.cost_per_hour(price_config)
                    for cluster in self.clusters])

    def to_dict(self):
        return {attr: getattr(self, attr) for attr in self.__attributes__}


class Event(Base):
    __tablename__ = "events"

    cluster_id = Column(String,
                        ForeignKey("clusters.cluster_id"),
                        primary_key=True,
                        nullable=False)
    timestamp = Column(DateTime, primary_key=True, nullable=False)
    details = Column(JSON, nullable=False)
    type = Column(String, primary_key=True, nullable=False)

    cluster = relationship(Cluster)

    def human_details(self):
        patterns = {
            "CREATING": lambda x: "Cluster is created by: {user}".format(**x),
            "EXPANDED_DISK": lambda x: ("Disk size is changed "
                                        "from {previous_disk_size:,} "
                                        "to {disk_size:,}"
                                        .format(**x)),
            "STARTING": lambda x: "Cluster is started by {user}".format(**x),
            "RESTARTING": lambda x: ("Cluster is restarted by {user}"
                                     .format(**x)),
            "TERMINATING": lambda x: ("Cluster is terminated due to {}"
                                      .format(x["reason"]["code"]
                                              .capitalize())),
            "EDITED": lambda x: ("Cluster is edited by {user}:\n{changes}"
                                 .format(user=x['user'],
                                         changes=self.parse_config_edits(x))),
            "RUNNING": lambda x: ("Cluster is running with "
                                  "{current_num_workers} workers "
                                  "(target: {target_num_workers})"
                                  .format(**x)),
            "RESIZING": lambda x: ("Cluster resize from {current_num_workers} "
                                   "to {target_num_workers}.".format(**x)
                                   + ("Resize is initiated by {user}."
                                      .format(user=x.get('user'))
                                      if 'user' in x.keys() else "")),
            "UPSIZE_COMPLETED": lambda x: ("Cluster is now running with "
                                           "{current_num_workers} workers "
                                           "(target: {target_num_workers})"
                                           .format(**x)),
            "DRIVER_HEALTHY": lambda _: "Driver is healthy",
            "DRIVER_UNAVAILABLE": lambda _: "Driver is not available.",
        }
        if self.type not in patterns:
            return self.details
        return patterns[self.type](self.details)

    def difference(self, first, second, parent_key=None):
        diff = []
        if isinstance(first, dict):
            for key in first:
                if key not in second:
                    diff.append(f"{key} removed.")
                else:
                    changes = self.difference(first[key], second[key],
                                              parent_key)
                    if len(changes):
                        diff.append(f'{key} {", ".join(changes)}')

        elif isinstance(first, list):
            diff = [self.difference(first_item, second_item, parent_key)
                    for first_item, second_item in zip(first, second)]
        else:
            if first != second:
                diff.append(f'{parent_key if parent_key else ""} '
                            f'changed from {first} to {second}')
        return diff

    def parse_config_edits(self, config):
        prev = config.get('previous_attributes')
        act = config.get('attributes')

        if prev is None or act is None:
            return 'No changes made.'

        diff = self.difference(prev, act)

        return '\n- '.join(diff)


class Job(Base):
    __tablename__ = "jobs"

    job_id = Column(BigInteger, primary_key=True)
    workspace_id = Column(String, ForeignKey("workspaces.id"), primary_key=True)
    created_time = Column(DateTime, primary_key=False)  # still debating if necessary
    creator_user_name = Column(String, ForeignKey("users.username"),
                               nullable=True)
    name = Column(String, nullable=False)
    timeout_seconds = Column(Integer, nullable=False)
    email_notifications = Column(JSON, nullable=False)
    new_cluster = Column(JSON)
    existing_cluster_id = Column(String)
    schedule_quartz_cron_expression = Column(String)
    schedule_timezone_id = Column(String)
    task_type = Column(String)
    task_parameters = Column(JSON)
    max_concurrent_runs = Column(Integer)

    workspace = relationship(Workspace)
    jobruns = relationship("JobRun",
                           backref="job",
                           foreign_keys="[JobRun.job_id, JobRun.workspace_id]",
                           order_by="JobRun.start_time")

    def runs(self, as_df=False, price_config=None, last=None, since_days=None):
        """Returns the job's run.

        Parameters:
        -----------
        - as_df : boolean (default: False)
            Convert the runs to pandas dataframe
        - price_config : dict (default: None)
            Price config used to compute cluster cost
        - last : int (default: None)
            Filters the last N results (no filtering if set to None)
        - since_days : int (default: None)
            Filters to last N days (no filtering if set to None)

        Returns:
        --------
        runs : list of JobRun objects or pd.DataFrame
            The job's runs as list of sqlalchemy objects
            or as a pandas DataFrame
        """
        runs = self.jobruns

        if since_days is not None:
            runs = [run for run in runs if run.start_time >= since(since_days)]

        if last is not None:
            runs = runs[-last:]

        if as_df:
            runs = pd.DataFrame([run.to_dict(price_config) for run in runs])

        return runs

    def num_runs(self, last=None, since_days=None):
        return len(self.runs(last, since_days))

    def duration(self, last=None, since_days=None):
        return sum([run.duration() for run in self.runs(last, since_days)])

    def dbu(self, last=None, since_days=None):
        return sum([run.dbu() for run in self.runs(last, since_days)])

    def cost(self, price_config, last=None, since_days=None):
        return sum([run.cost(price_config)
                    for run in self.runs(last, since_days)])


class JobRun(Base):
    __tablename__ = "jobruns"
    __attributes__ = ['job_id', 'run_id', 'workspace_id',
                      'cluster_instance_id', 'cluster_type_id',
                      'start_time', 'creator_user_name', 'run_name',
                      'duration', 'dbu']
    __table_args__ = (
        ForeignKeyConstraint(["job_id", "workspace_id"],
                             ["jobs.job_id", 'jobs.workspace_id']),
    )

    job_id = Column(BigInteger, primary_key=True)
    run_id = Column(BigInteger, primary_key=True)
    number_in_job = Column(Integer)
    original_attempt_run_id = Column(Integer)
    workspace_id = Column(String, ForeignKey("workspaces.id"),
                          primary_key=True)
    cluster_spec = Column(JSON, nullable=False)
    cluster_type_id = Column(String, ForeignKey("cluster_types.type"))
    cluster_instance_id = Column(String, ForeignKey("clusters.cluster_id"),
                                 nullable=True)
    spark_context_id = Column(BigInteger)
    state_life_cycle_state = Column(String)
    state_result_state = Column(String)
    state_state_message = Column(String)
    task = Column(JSON, nullable=False)
    start_time = Column(DateTime, nullable=False)
    setup_duration = Column(Integer)
    execution_duration = Column(Integer)
    cleanup_duration = Column(Integer)
    trigger = Column(String, nullable=False)
    creator_user_name = Column(String, ForeignKey("users.username"),
                               nullable=True)
    run_name = Column(String, nullable=False)
    run_page_url = Column(String)
    run_type = Column(String, nullable=False)

    workspace = relationship(Workspace, viewonly=True)
    cluster = relationship(Cluster)
    cluster_type = relationship("ClusterType")

    def start_date(self):
        return datetime.fromtimestamp(self.start_time)

    def duration(self):
        millisec = ((self.setup_duration or 0.0)
                    + (self.execution_duration or 0.0)
                    + (self.cleanup_duration or 0.0))
        hours = millisec / 3_600_000
        return hours

    def dbu(self):
        if self.cluster is None:
            log.warning("[JOBRUN DBU Calculation] Cluster no longer "
                        "available, falling back to cluster specification.")
            dbu_per_hour = self.cluster_type.dbu_job
            if dbu_per_hour is None:
                log.warning("[JOBRUN DBU Calculation] Instance type is "
                            "missing, returning 0.")
                return 0
            return dbu_per_hour * self.duration()

        return self.cluster.dbu_per_hour() * self.duration()

    def cost(self, price_config):
        if self.cluster is None:
            log.warning("[JOBRUN Cost Calculation] Cluster no longer "
                        "available, falling back to cluster specification.")
            dbu_per_hour = self.cluster_type.dbu_job
            if dbu_per_hour is None:
                log.warning("[JOBRUN Cost Calculation] Instance type "
                            "is missing, returning 0.")
                return 0
            price_per_hour = price_config['job']
            return dbu_per_hour * price_per_hour * self.duration()

        return self.cluster.cost_per_hour(price_config) * self.duration()

    def to_dict(self, price_config=None):
        mapping = {'cluster_instance_id': 'cluster_id',
                   'creator_user_name': 'username',
                   'run_name': 'name'}

        attributes = {}
        for attr in self.__attributes__:
            name = mapping.get(attr, attr)
            attribute = getattr(self, attr)
            if callable(attribute):
                attribute = attribute()
            attributes[name] = attribute

        if price_config is not None:
            attributes['cost'] = self.cost(price_config)

        return attributes


class ScraperRun(Base):

    SUCCESSFUL = "SUCCESSFUL"
    FAILED = "FAILED"
    IN_PROGRESS = "IN_PROGRESS"

    __tablename__ = "scraper-run"

    scraper_run_id = Column(String, primary_key=True)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    status = Column(String, nullable=False)
    num_workspaces = Column(Integer, nullable=False)
    num_clusters = Column(Integer, nullable=False)
    num_events = Column(Integer, nullable=False)
    num_jobs = Column(Integer, nullable=False)
    num_job_runs = Column(Integer, nullable=False)
    num_users = Column(Integer, nullable=False)

    def start(self):
        self.start_time = datetime.now()
        self.scraper_run_id = str(uuid4())
        self.status = ScraperRun.IN_PROGRESS

    def finish(self, status):
        if self.start_time is None:
            raise RuntimeError(
                "ScraperRun that was not started cannot be finished")
        self.end_time = datetime.now()
        self.status = status

    def duration(self):
        return self.end_time - self.start_time

    def duration_str(self):
        return str(self.duration())

    def __str__(self):
        return "ScraperRun[id={}, status={}, duration={:.2f}s]" \
            .format(self.scraper_run_id, self.status, self.duration())

    def __repr__(self):
        return self.__str__()

    def merge(left: "ScraperRun", right: "ScraperRun") -> "ScraperRun":
        start_time = None
        if left.start_time is None:
            start_time = right.start_time
        elif right.start_time is None:
            start_time = left.start_time
        else:
            start_time = (left.start_time
                          if left.start_time < right.start_time
                          else right.start_time)
        end_time = None
        if left.end_time is None:
            end_time = right.end_time
        elif right.end_time is None:
            end_time = left.end_time
        else:
            end_time = (left.end_time
                        if left.end_time > right.end_time
                        else right.end_time)

        return ScraperRun(
            start_time=start_time,
            end_time=end_time,
            status=left.status if left.status == "FAILED" else right.status,
            scraper_run_id=str(uuid4()),
            num_workspaces=left.num_workspaces + right.num_workspaces,
            num_clusters=left.num_clusters + right.num_clusters,
            num_events=left.num_events + right.num_events,
            num_jobs=left.num_jobs + right.num_jobs,
            num_job_runs=left.num_job_runs + right.num_job_runs,
            num_users=left.num_users + right.num_users,
        )

    def empty() -> "ScraperRun":
        return ScraperRun(
            num_workspaces=0,
            num_clusters=0,
            num_events=0,
            num_jobs=0,
            num_job_runs=0,
            num_users=0,
        )


class ClusterStates(Base):
    __tablename__ = "cluster_states"
    __attributes__ = ["user_id", "cluster_id", "timestamp", "state",
                      "driver_type", "worker_type", "num_workers",
                      "dbu", "interval"]

    user_id = Column(String, ForeignKey("users.username"), primary_key=True)
    cluster_id = Column(String, ForeignKey("clusters.cluster_id"),
                        primary_key=True)
    timestamp = Column(DateTime, primary_key=True)
    state = Column(String, primary_key=True)
    driver_type = Column(String)
    worker_type = Column(String)
    num_workers = Column(Integer)
    dbu = Column(Float)
    interval = Column(Float, nullable=False)

    cluster = relationship(Cluster)

    def __str__(self):
        return (f"ClusterState["
                f"cluster={self.cluster}, "
                f"state={self.state}, "
                f"interval={self.interval}, "
                f"dbu={self.dbu}"
                f"]")

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        return {attr: getattr(self, attr) for attr in self.__attributes__}


class ClusterType(Base):
    __tablename__ = "cluster_types"
    __attributes__ = ['scrape_time', 'type', 'cpu', 'mem',
                      'dbu_light', 'dbu_job', 'dbu_analysis']

    scrape_time = Column(DateTime, primary_key=True)
    type = Column(String, primary_key=True)
    cpu = Column(Integer)
    mem = Column(Integer)
    dbu_light = Column(Float)
    dbu_job = Column(Float)
    dbu_analysis = Column(Float)

    def to_dict(self):
        return {attr: getattr(self, attr) for attr in self.__attributes__}


def create_db():
    engine = create_engine(engine_url)
    Base.metadata.create_all(engine)
