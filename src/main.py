from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.responses import FileResponse
import uuid
import os
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import Session
from typing import Dict, Optional

from database import get_db, init_db, ReportStatus, StoreStatus, BusinessHours, Timezone
from services import trigger_report_generation, get_report_status

# Create data directory if it doesn't exist
os.makedirs("reports", exist_ok=True)

app = FastAPI(
    title="Restaurant Monitoring API",
    description="API for monitoring restaurant uptime and downtime",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    init_db()
    
@app.get("/")
def read_root():
    return {"message": "Welcome to Restaurant Monitoring API"}

@app.post("/trigger_report")
async def trigger_report(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Trigger a new report generation
    
    Returns:
        dict: A dictionary containing the report_id
    """
    report_id = str(uuid.uuid4())
    
    # Create a new report status entry
    report_status = ReportStatus(report_id=report_id, status="Running")
    db.add(report_status)
    db.commit()
    
    # Trigger background report generation
    background_tasks.add_task(trigger_report_generation, report_id, db)
    
    return {"report_id": report_id}

@app.get("/get_report")
async def get_report(report_id: str, db: Session = Depends(get_db)):
    """
    Get the status of a report or the CSV file if it's complete
    
    Args:
        report_id (str): The ID of the report to check
        
    Returns:
        dict or FileResponse: Report status or the CSV file
    """
    status = get_report_status(report_id, db)
    
    if not status:
        raise HTTPException(status_code=404, detail=f"Report with ID {report_id} not found")
        
    if status.status == "Running":
        return {"status": "Running"}
    
    if status.status == "Complete":
        file_path = f"reports/{report_id}.csv"
        if os.path.exists(file_path):
            return FileResponse(
                path=file_path, 
                filename=f"report_{report_id}.csv",
                media_type="text/csv"
            )
        else:
            raise HTTPException(status_code=404, detail="Report file not found")
    
    raise HTTPException(status_code=500, detail="Unknown report status")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)