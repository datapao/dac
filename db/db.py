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


class Events(Base):
    __tablename__ = "events"
    id = Column(BigInteger, primary_key=True)
    cluster_id = Column(String, primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    # This will be the events details, which is a different JSON for every type of events. Ouch.
    # TODO(gulyasm): Fix this monstrosity
    details = Column(String)


def create_db():
    engine = create_engine(engine_url)
    Base.metadata.create_all(engine)
