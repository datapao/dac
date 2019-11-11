from uuid import uuid4
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import Column, ForeignKey
from sqlalchemy import String, Integer, BigInteger, Float
from sqlalchemy import DateTime, Boolean, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

engine_url = 'sqlite:///dac.db'
Base = declarative_base()


class Cluster(Base):
    __tablename__ = "clusters"
    cluster_id = Column(String, primary_key=True)
    cluster_name = Column(String, nullable=False)
    state = Column(String, nullable=False)
    state_message = Column(String, nullable=False)
    # TODO(gulyasm): This must be a foreign key and a seperate table
    driver_type = Column(String, nullable=False)
    worker_type = Column(String, nullable=False)
    num_workers = Column(Integer, nullable=False)
    spark_version = Column(String, nullable=False)
    creator_user_name = Column(String, nullable=False)
    workspace_id = Column(String, ForeignKey("workspaces.id"), nullable=False)
    workspace = relationship("Workspace")
    autotermination_minutes = Column(Integer)
    cluster_source = Column(String)
    creator_user_name = Column(String)
    enable_elastic_disk = Column(Boolean)
    last_activity_time = Column(DateTime)
    last_state_loss_time = Column(DateTime)
    pinned_by_user_name = Column(String)
    spark_context_id = Column(BigInteger, nullable=False)
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
    events = relationship("Event")

    def eventFilterNot(self, types):
        return [e for e in self.events if e.type not in types]


class Workspace(Base):
    __tablename__ = "workspaces"
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    url = Column(String, nullable=False)
    # AWS or AZURE
    type = Column(String, nullable=False)
    token = Column(String, nullable=False)
    clusters = relationship(Cluster)

    def active_clusters(self):
        return [c for c in self.clusters if c.state in ["RUNNING", "PENDING"]]


class Event(Base):
    __tablename__ = "events"
    cluster_id = Column(String, ForeignKey(
        "clusters.cluster_id"), primary_key=True)
    timestamp = Column(DateTime, primary_key=True)
    details = Column(JSON, nullable=False)
    type = Column(String, nullable=False)
    cluster = relationship(Cluster)

    def human_details(self):
        patterns = {
            "TERMINATING": lambda x: "Cluster is terminated due to {}".format(x["reason"]["code"].capitalize()),
            "DRIVER_HEALTHY": lambda _: "Driver is healthy",
            "RUNNING": lambda x: "Cluster is running with {current_num_workers} workers (target: {target_num_workers})".format(**x),
            "STARTING": lambda x: "Cluster is started by {user}".format(**x),
            "CREATING": lambda x: "Cluster is created by: {user}".format(**x)
        }
        if self.type not in patterns:
            return self.details
        return patterns[self.type](self.details)


class Job(Base):
    __tablename__ = "jobs"
    job_id = Column(BigInteger, primary_key=True)
    created_time = Column(DateTime, primary_key=True)
    creator_user_name = Column(String, nullable=False)
    name = Column(String, nullable=False)
    timeout_seconds = Column(Integer, nullable=False)
    email_notifications = Column(JSON, nullable=False)
    workspace_id = Column(String, ForeignKey("workspaces.id"), nullable=False)
    workspace = relationship(Workspace)
    new_cluster = Column(JSON)
    schedule_quartz_cron_expression = Column(String)
    schedule_timezone_id = Column(String)
    task_type = Column(String)
    notebook_path = Column(String)
    notebook_revision_timestamp = Column(Integer)
    max_concurrent_runs = Column(Integer)


class JobRun(Base):
    __tablename__ = "jobruns"
    job_id = Column(BigInteger, primary_key=True)
    run_id = Column(BigInteger, primary_key=True)
    number_in_job = Column(Integer)
    original_attempt_run_id = Column(Integer)
    workspace_id = Column(String, ForeignKey(
        "workspaces.id"), primary_key=True)
    workspace = relationship(Workspace)
    cluster_spec = Column(JSON, nullable=False)
    cluster_instance_id = Column(String, nullable=False)
    spark_context_id = Column(BigInteger, nullable=False)
    state_life_cycle_state = Column(String)
    state_result_state = Column(String)
    state_state_message = Column(String)
    task = Column(JSON, nullable=False)
    start_time = Column(DateTime, nullable=False)
    setup_duration = Column(Integer)
    execution_duration = Column(Integer)
    cleanup_duration = Column(Integer)
    trigger = Column(String, nullable=False)
    creator_user_name = Column(String, nullable=False)
    run_name = Column(String, nullable=False)
    run_page_url = Column(String)
    run_type = Column(String, nullable=False)


class ScraperRun(Base):

    SUCCESSFUL = "SUCCESSFUL"
    FAILED = "FAILED"
    IN_PROGRESS = "IN_PROGRESS"

    __tablename__ = "screper-run"
    scraper_run_id = Column(String, primary_key=True)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    status = Column(String, nullable=False)
    num_workspaces = Column(Integer, nullable=False)
    num_clusters = Column(Integer, nullable=False)
    num_events = Column(Integer, nullable=False)
    num_jobs = Column(Integer, nullable=False)
    num_job_runs = Column(Integer, nullable=False)

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
            start_time = left.start_time if left.start_time < right.start_time else right.start_time
        end_time = None
        if left.end_time is None:
            end_time = right.end_time
        elif right.end_time is None:
            end_time = left.end_time
        else:
            end_time = left.end_time if left.end_time > right.end_time else right.end_time

        return ScraperRun(
            start_time=start_time,
            end_time=end_time,
            status=left.status if left.status == "FAILED" else right.status,
            scraper_run_id=str(uuid4()),
            num_workspaces=left.num_workspaces + right.num_workspaces,
            num_clusters=left.num_clusters + right.num_clusters,
            num_events=left.num_events + right.num_events,
            num_jobs=left.num_jobs + right.num_jobs,
            num_job_runs=left.num_job_runs + right.num_job_runs
        )

    def empty() -> "ScraperRun":
        return ScraperRun(
            num_workspaces=0,
            num_clusters=0,
            num_events=0,
            num_jobs=0,
            num_job_runs=0
        )


class ClusterStates(Base):
    __tablename__ = "cluster_states"
    cluster_id = Column(String, ForeignKey(
        "clusters.cluster_id"), primary_key=True)
    # should be foreign key to users table
    user_id = Column(String, primary_key=True)
    timestamp = Column(DateTime, primary_key=True)
    state = Column(String, primary_key=True)
    driver_type = Column(String, nullable=False)
    worker_type = Column(String, nullable=False)
    num_workers = Column(Integer, nullable=False)
    # TODO: set to not nullable once proper instance
    # type scraping is implemented.
    dbu = Column(Float, nullable=True)
    interval = Column(Float, nullable=False)


def create_db():
    engine = create_engine(engine_url)
    Base.metadata.create_all(engine)
