import json 
from src.utils.commons import hash, ingest_at, read_json
from dotenv import load_dotenv
import os 
from datetime import datetime, timezone
from pymongo import MongoClient
import requests    
from typing import Dict, List, Any 


load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DBNAME")]
collection = db[os.getenv("MONGO_COLLECTION")]

url ="https://commerce-pulse-api.onrender.com/events"

import requests

def fetch_data(**context):
    # Get the Airflow execution interval
    start = context["data_interval_start"].isoformat()
    end = context["data_interval_end"].isoformat()

    url = "https://api.commercepulse.com/events"

    events = []
    offset = 0
    limit = 1000  # or max allowed per API request

    while True:
        params = {
            "start_date": start,
            "end_date": end,
            "limit": limit,
            "offset": offset
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        events = response.json()

        if not events:  # no more events
            break

        events.extend(events)
        offset += limit

    return events


def get_data(event: list) -> list:
    records = event if isinstance(event, list) else [event]
    all_events = []
    
    for record in records:
        events.append({
            "event_id": record.get("event_id"),
            "event_type": record.get("event_type"),
            "event_time": record.get("event_time"),
            "vendor": record.get("vendor"),
            "payload": record.get("payload"),
            "ingested_at": record.get("ingested_at")
        })
        
    return all_events 
        
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
    data = fetch_data()
    events = get_data(data.get("events", []))
    
    for event in events:
        upsert_events(event)