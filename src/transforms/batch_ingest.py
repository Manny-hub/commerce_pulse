import json 
from src.utils.commons import hash, ingest_at, read_json
from dotenv import load_dotenv
import os 
from datetime import datetime, timezone
from pymongo import MongoClient
import glob

load_dotenv()

paths = "data/bootstrap/*.json" 

client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DBNAME")]
collection = db[os.getenv("MONGO_COLLECTION")]

def get_event_type(file_path):
    """Maps filename (e.g., orders_2023.json) to event_type (historical_order)."""
    filename = os.path.basename(file_path).lower()
    if "order" in filename: return "historical_order"
    if "payment" in filename: return "historical_payment"
    if "shipment" in filename: return "historical_shipment"
    if "refund" in filename: return "historical_refund"
    return "historical_event"

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
            "event_time": record.get("created_at") or record.get("paid_at") or record.get("refundedAt") or record.get("timestamp") or datetime.now(timezone.utc).isoformat(),
            "vendor": record.get("vendor", "unknown"),
            "payload": record,
            "ingested_at": ingest_at()
        })
    
    return events

def upsert_event(raw: dict):
    """
    Insert or update raw deterministically.
    """
    result = collection.update_one(
        {"event_id": raw["event_id"]},
        {"$set": raw},
        upsert=True
    )
    return result

if __name__ == "__main__":
    file_list = glob.glob(paths)
    
    if not file_list:
        print(f"No files found matching: {paths}")
    
    for file_path in file_list:
        print(f"Processing: {file_path}")
        try:
            # Generate the list of synthetic events for this file
            synthetic_events = read_event(file_path)
            
            for event_data in synthetic_events:
                result = upsert_event(event_data)
                status = "Updated" if result.matched_count > 0 else "Inserted"
                # Optional: print status for every record if file is small, 
                # or just a summary for large files.
            
            print(f"Finished {file_path}: {len(synthetic_events)} records processed.")
            
        except Exception as e:
            print(f"Error processing {file_path}: {e}")