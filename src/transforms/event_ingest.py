import json 
from src.utils.commons import hash, ingest_at, read_json
from dotenv import load_dotenv
import os 
from datetime import datetime, timezone
from pymongo import MongoClient

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DBNAME")]
collection = db[os.getenv("MONGO_COLLECTION")]

url ="https://commerce-pulse-api.onrender.com/events?events=2000"

def fetch_data(url):
    data = []
    request = requests.get(url)
    if request.status_code == 200:
        data = request.json()
    return data 


def get_data(data: list) -> list:
    records = data if isinstance(data, list) else [data]
    events = []
    
    for record in records:
        events.append({
            "event_id": record.get("event_id"),
            "event_type": record.get("event_type"),
            "event_time": record.get("event_time"),
            "vendor": record.get("vendor"),
            "payload": record.get("payload"),
            "ingested_at": record.get("ingested_at")
        })
        
    return events 
        
def upsert_events(events: Dict):
    """
    Insert or update raw deterministically.
    """
    try: 
        result = collection.update_one(
            {"event_id": events["event_id"]},
            {"$set": events},
            upsert=True
        )
        return result
    
    except Exception as e: 
        print(f"Error: {e}")
        
        
def main():
    file_path = "data/events/events.jsonl"
    data = fetch_data(url)
    events = get_data(data)
    
    for event in events:
        upsert_events(event)