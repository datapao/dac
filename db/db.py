from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Integer, String, BigInteger, DateTime, Boolean, JSON
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship

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
    terminated_time = Column(DateTime, nullable=False)
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
        return patterns[self.type](self.details)


def create_db():
    engine = create_engine(engine_url)
    Base.metadata.create_all(engine)
