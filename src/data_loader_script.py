import os
from sqlalchemy.orm import Session
from database import init_db, get_db
from data_loader import load_all_data

def main():
    # Initialize database
    init_db()
    
    # Get database session
    db = next(get_db())
    
    # Load data from parent directory
    # Navigate one level up from the src directory to the root
    current_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(current_dir)
    data_dir = os.path.join(root_dir, "data")
    
    # Verify data directory exists
    if not os.path.exists(data_dir):
        print(f"Error: Data directory not found at {data_dir}")
        return
        
    load_all_data(db, data_dir)
    
    print("Data loading complete")

if __name__ == "__main__":
    main()