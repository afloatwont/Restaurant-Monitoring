from sqlalchemy.orm import Session
from sqlalchemy import func
import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta, time
import os
from database import StoreStatus, BusinessHours, Timezone, ReportStatus
import csv
from typing import List, Dict, Tuple, Optional

def get_report_status(report_id: str, db: Session):
    """Get the status of a report from the database"""
    return db.query(ReportStatus).filter(ReportStatus.report_id == report_id).first()

def get_max_timestamp(db: Session) -> datetime:
    """Get the maximum timestamp from the store_status table to simulate current time"""
    max_timestamp = db.query(func.max(StoreStatus.timestamp_utc)).scalar()
    if not max_timestamp:
        max_timestamp = datetime.utcnow()
    return max_timestamp

def get_store_timezone(store_id: str, db: Session) -> str:
    """Get the timezone for a store, default to 'America/Chicago' if not found"""
    timezone_record = db.query(Timezone).filter(Timezone.store_id == store_id).first()
    if timezone_record:
        return timezone_record.timezone_str
    return 'America/Chicago'

def get_business_hours(store_id: str, db: Session) -> List[Dict]:
    """Get business hours for a store, assume 24/7 if not found"""
    hours = db.query(BusinessHours).filter(BusinessHours.store_id == store_id).all()
    if hours:
        return [
            {
                'day_of_week': hour.day_of_week,
                'start_time_local': hour.start_time_local,
                'end_time_local': hour.end_time_local
            }
            for hour in hours
        ]
    # Default to 24/7 if no business hours are found
    return [
        {
            'day_of_week': day,
            'start_time_local': time(0, 0),
            'end_time_local': time(23, 59, 59)
        }
        for day in range(7)
    ]

def is_store_open(timestamp_utc: datetime, store_id: str, business_hours: List[Dict], tz_str: str) -> bool:
    """Check if a store is open at a given UTC timestamp based on business hours"""
    # Convert UTC timestamp to local time
    local_tz = pytz.timezone(tz_str)
    local_time = timestamp_utc.replace(tzinfo=pytz.utc).astimezone(local_tz)
    
    # Get day of week (0=Monday, 6=Sunday)
    day_of_week = local_time.weekday()
    
    # Find matching business hours
    for hours in business_hours:
        if hours['day_of_week'] == day_of_week:
            # Convert datetime.time to datetime.datetime for comparison
            start_dt = datetime.combine(local_time.date(), hours['start_time_local'])
            end_dt = datetime.combine(local_time.date(), hours['end_time_local'])
            
            # Check if current time is within business hours
            if start_dt <= local_time <= end_dt:
                return True
    
    return False

def calculate_uptime_downtime(store_id: str, current_time: datetime, db: Session) -> Dict:
    """Calculate uptime and downtime for a store"""
    # Get store timezone
    tz_str = get_store_timezone(store_id, db)
    local_tz = pytz.timezone(tz_str)
    
    # Get business hours
    business_hours = get_business_hours(store_id, db)
    
    # Define time ranges
    hour_ago = current_time - timedelta(hours=1)
    day_ago = current_time - timedelta(days=1)
    week_ago = current_time - timedelta(days=7)
    
    # Get store status observations within the last week
    status_records = db.query(StoreStatus).filter(
        StoreStatus.store_id == store_id,
        StoreStatus.timestamp_utc >= week_ago,
        StoreStatus.timestamp_utc <= current_time
    ).order_by(StoreStatus.timestamp_utc).all()
    
    if not status_records:
        # No data for this store in the time range
        return {
            'store_id': store_id,
            'uptime_last_hour': 0,
            'uptime_last_day': 0,
            'uptime_last_week': 0,
            'downtime_last_hour': 0,
            'downtime_last_day': 0,
            'downtime_last_week': 0
        }
    
    # Process observations
    records_df = pd.DataFrame([
        {
            'timestamp_utc': record.timestamp_utc,
            'status': record.status
        }
        for record in status_records
    ])
    
    # Add a column to indicate if timestamp is within business hours
    records_df['is_business_hours'] = records_df['timestamp_utc'].apply(
        lambda ts: is_store_open(ts, store_id, business_hours, tz_str)
    )
    
    # Filter to only include observations during business hours
    business_hours_df = records_df[records_df['is_business_hours']]
    
    if business_hours_df.empty:
        # No observations during business hours
        return {
            'store_id': store_id,
            'uptime_last_hour': 0,
            'uptime_last_day': 0,
            'uptime_last_week': 0,
            'downtime_last_hour': 0,
            'downtime_last_day': 0,
            'downtime_last_week': 0
        }
    
    # Calculate uptime and downtime for each time range
    result = {
        'store_id': store_id,
        'uptime_last_hour': 0,
        'uptime_last_day': 0,
        'uptime_last_week': 0,
        'downtime_last_hour': 0,
        'downtime_last_day': 0,
        'downtime_last_week': 0
    }
    
    # For last hour
    hour_df = business_hours_df[business_hours_df['timestamp_utc'] >= hour_ago]
    if not hour_df.empty:
        active_ratio = hour_df[hour_df['status'] == 'active'].shape[0] / hour_df.shape[0]
        result['uptime_last_hour'] = active_ratio * 60  # Convert to minutes
        result['downtime_last_hour'] = 60 - result['uptime_last_hour']
    
    # For last day
    day_df = business_hours_df[business_hours_df['timestamp_utc'] >= day_ago]
    if not day_df.empty:
        active_ratio = day_df[day_df['status'] == 'active'].shape[0] / day_df.shape[0]
        
        # Calculate total business hours in the last day
        total_business_hours = 0
        current_local_time = current_time.replace(tzinfo=pytz.utc).astimezone(local_tz)
        day_ago_local = day_ago.replace(tzinfo=pytz.utc).astimezone(local_tz)
        
        # Loop through each hour in the last day
        for i in range(24):
            check_time = current_local_time - timedelta(hours=i)
            if is_store_open(check_time.astimezone(pytz.utc), store_id, business_hours, tz_str):
                total_business_hours += 1
        
        result['uptime_last_day'] = active_ratio * total_business_hours
        result['downtime_last_day'] = total_business_hours - result['uptime_last_day']
    
    # For last week
    week_df = business_hours_df
    if not week_df.empty:
        active_ratio = week_df[week_df['status'] == 'active'].shape[0] / week_df.shape[0]
        
        # Calculate total business hours in the last week
        total_business_hours = 0
        for i in range(7*24):  # 7 days * 24 hours
            check_time = current_time - timedelta(hours=i)
            if is_store_open(check_time, store_id, business_hours, tz_str):
                total_business_hours += 1
        
        result['uptime_last_week'] = active_ratio * total_business_hours
        result['downtime_last_week'] = total_business_hours - result['uptime_last_week']
    
    return result

async def trigger_report_generation(report_id: str, db: Session):
    """Generate a report in the background"""
    try:
        # Get current time (max timestamp in the database)
        current_time = get_max_timestamp(db)
        
        # Get all unique store IDs
        store_ids = [row[0] for row in db.query(StoreStatus.store_id).distinct().all()]
        
        # Calculate uptime and downtime for each store
        results = []
        for store_id in store_ids:
            result = calculate_uptime_downtime(store_id, current_time, db)
            results.append(result)
            
        # Create the CSV file
        output_file = f"reports/{report_id}.csv"
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = [
                'store_id', 
                'uptime_last_hour', 
                'uptime_last_day', 
                'uptime_last_week', 
                'downtime_last_hour', 
                'downtime_last_day', 
                'downtime_last_week'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for result in results:
                writer.writerow(result)
        
        # Update report status
        report = db.query(ReportStatus).filter(ReportStatus.report_id == report_id).first()
        if report:
            report.status = "Complete"
            report.completed_at = datetime.utcnow()
            db.commit()
            
    except Exception as e:
        print(f"Error generating report: {str(e)}")
        # In case of error, update report status
        report = db.query(ReportStatus).filter(ReportStatus.report_id == report_id).first()
        if report:
            report.status = "Error"
            db.commit()