from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, scoped_session

from db import Cluster, Workspace, create_db, Base, engine_url
from db import ScraperRun, ClusterStates
from scraping import scrape, start_scheduled_scraping
import pandas as pd

engine = create_engine(engine_url)
Base.metadata.bind = engine
DBSession = scoped_session(sessionmaker())
DBSession.bind = engine
session = DBSession()
states = session.query(ClusterStates).all()
df = pd.DataFrame.from_records([s.to_dict() for s in states])
df["interval_dbu"] = df["dbu"] * df["interval"]

print(df.user_id.value_counts())
