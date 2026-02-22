import json 
from src.utils.commons import hash, ingest_at, read_json, get_event_type
from src.database.upsert import bulk_upsert_events
from dotenv import load_dotenv
import os 
from datetime import datetime, timezone
from pymongo import MongoClient
import glob

load_dotenv()

paths = "data/bootstrap/*.json" 


def read_event(file_path):
    """
    Reads a JSON file and wraps every record as a synthetic event.
    """
    data = read_json(file_path)
    
    # Ensure data is a list so we can wrap "every record"
    records = data if isinstance(data, list) else [data]
    event_type = get_event_type(file_path)
    
    events = []
    for record in records:
        # Generate ID based on the specific record content
        event_id = hash(record)
        
        events.append({
            "event_id": event_id,
            "event_type": event_type,
            "event_time": record.get("created_at") or 
                          record.get("paid_at") or 
                          record.get("refundedAt") or 
                          record.get("timestamp") or 
                          datetime.now(timezone.utc).isoformat(),
            "vendor": record.get("carrier", "unknown"),
            "payload": record,
            "ingested_at": ingest_at()
        })
    
    return events

def main():
    file_list = glob.glob(paths)
    
    if not file_list:
        print(f"No files found matching: {paths}")
    
    for file_path in file_list:
        print(f"Processing: {file_path}")
        try:
            # Generate the list of synthetic events for this file
            synthetic_events = read_event(file_path)
            
            for event_data in synthetic_events:
                result = bulk_upsert_events(event_data)
                
                status = "Updated" if result.matched_count > 0 else "Inserted"
                
            print(f"Finished {file_path}: {len(synthetic_events)} records processed.")
            
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

if __name__ == "__main__":
    main()