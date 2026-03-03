
from sqlalchemy import Column, String, Float, Integer
from sqlalchemy.orm import declarative_base

from pathlib import Path
from sqlalchemy import create_engine

#----------------------------------------------
# 1 Create a table transformers in the database
#----------------------------------------------

Base = declarative_base() 

class Transformer(Base):
    __tablename__ = "transformers" #Table transformers to append data

    id = Column(Integer, primary_key=True, autoincrement=True)
    Sensor_ID = Column(String)
    Timestamp = Column(String)
    Voltage = Column(Float)
    Current = Column(Float)
    Temperature = Column(Float)
    Power = Column(Float)
    Humidity = Column(Float)
    Vibration = Column(Float)
    Equipment_ID = Column(String)
    Operational_Status = Column(String)
    Fault_Status = Column(String)
    Failure_Type = Column(String)
    Last_Maintenance_Date = Column(String)
    Maintenance_Type = Column(String)
    Failure_History = Column(String)
    Repair_Time = Column(Integer)
    Maintenance_Costs = Column(Float)
    Ambient_Temperature = Column(Float)
    Ambient_Humidity = Column(Integer)
    External_Factors = Column(String)
    X = Column(Integer)
    Y = Column(Integer)
    Z = Column(Integer)
    Equipment_Relationship = Column(String)
    Equipment_Criticality = Column(String)
    Fault_Detected = Column(Integer)
    Predictive_Maintenance_Trigger = Column(Integer)


def get_engine(db_path="../database/sensor_database.db"):
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{db_path}")
    return engine


def create_tables(engine):
    Base.metadata.create_all(engine)