from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Boolean, Time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import os
from datetime import datetime

# Create a sqlite database engine
DATABASE_URL = "sqlite:///./store_monitoring.db"
engine = create_engine(DATABASE_URL)

# Create a session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create a base class for models
Base = declarative_base()

# Define models
class StoreStatus(Base):
    __tablename__ = "store_status"
    
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, index=True)
    timestamp_utc = Column(DateTime)
    status = Column(String)  # 'active' or 'inactive'

class BusinessHours(Base):
    __tablename__ = "business_hours"
    
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, index=True)
    day_of_week = Column(Integer)  # 0=Monday, 6=Sunday
    start_time_local = Column(Time)
    end_time_local = Column(Time)

class Timezone(Base):
    __tablename__ = "timezone"
    
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, unique=True, index=True)
    timezone_str = Column(String)

class ReportStatus(Base):
    __tablename__ = "report_status"
    
    id = Column(Integer, primary_key=True, index=True)
    report_id = Column(String, unique=True, index=True)
    status = Column(String)  # 'Running' or 'Complete'
    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)

def init_db():
    # Create tables
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()