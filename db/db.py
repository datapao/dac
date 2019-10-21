from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Integer, String, BigInteger, Time
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship

engine_url = 'sqlite:///dac.db'
Base = declarative_base()

class Cluster(Base):
    __tablename__ = "clusters"
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    state = Column(String, nullable=False)
    # TODO(gulyasm): This must be a foreign key and a seperate table
    driver_type = Column(String, nullable=False)
    worker_type = Column(String, nullable=False)
    num_workers = Column(Integer, nullable=False)
    spark_version = Column(String, nullable=False)
    creator_user_name = Column(String, nullable=False)
    workspace_id = Column(String, ForeignKey("workspaces.id"))
    workspace = relationship("Workspace")
    


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
    timestamp = Column(Time, nullable=False)
    # This will be the events details, which is a different JSON for every type of events. Ouch.
    # TODO(gulyasm): Fix this monstrosity
    details = Column(String)


def create_db():
    engine = create_engine(engine_url)
    Base.metadata.create_all(engine)
    