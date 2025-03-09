import os
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import func
import csv
from datetime import datetime, time
from database import StoreStatus, BusinessHours, Timezone

def load_store_status(db: Session, file_path: str):
    """Load store status data from CSV"""
    # Check if data is already loaded
    count = db.query(func.count(StoreStatus.id)).scalar()
    if count > 0:
        print("Store status data already loaded. Skipping...")
        return
    
    # Read CSV file
    df = pd.read_csv(file_path)
    
    # Insert data into database
    for _, row in df.iterrows():
        store_status = StoreStatus(
            store_id=str(row['store_id']),
            timestamp_utc=pd.to_datetime(row['timestamp_utc']),
            status=row['status']
        )
        db.add(store_status)
    
    db.commit()
    print(f"Loaded {len(df)} store status records")

def load_business_hours(db: Session, file_path: str):
    """Load business hours data from CSV"""
    # Check if data is already loaded
    count = db.query(func.count(BusinessHours.id)).scalar()
    if count > 0:
        print("Business hours data already loaded. Skipping...")
        return
    
    # Read CSV file
    df = pd.read_csv(file_path)
    
    # Insert data into database
    for _, row in df.iterrows():
        # Parse time strings into time objects
        try:
            start_time_local = datetime.strptime(row['start_time_local'], '%H:%M:%S').time()
            end_time_local = datetime.strptime(row['end_time_local'], '%H:%M:%S').time()
        except ValueError:
            print(f"Invalid time format for store {row['store_id']}, day {row['day']}")
            continue
        
        business_hours = BusinessHours(
            store_id=str(row['store_id']),
            day_of_week=int(row['day']),
            start_time_local=start_time_local,
            end_time_local=end_time_local
        )
        db.add(business_hours)
    
    db.commit()
    print(f"Loaded {len(df)} business hours records")

def load_timezone(db: Session, file_path: str):
    """Load timezone data from CSV"""
    # Check if data is already loaded
    count = db.query(func.count(Timezone.id)).scalar()
    if count > 0:
        print("Timezone data already loaded. Skipping...")
        return
    
    # Read CSV file
    df = pd.read_csv(file_path)
    
    # Insert data into database
    for _, row in df.iterrows():
        timezone = Timezone(
            store_id=str(row['store_id']),
            timezone_str=row['timezone_str']
        )
        db.add(timezone)
    
    db.commit()
    print(f"Loaded {len(df)} timezone records")

def load_all_data(db: Session, data_dir: str):
    """Load all data from CSV files"""
    load_store_status(db, os.path.join(data_dir, 'store_status.csv'))
    load_business_hours(db, os.path.join(data_dir, 'menu_hours.csv'))
    load_timezone(db, os.path.join(data_dir, 'timezones.csv'))