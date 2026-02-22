import os
from dotenv import load_dotenv
from pymongo import MongoClient
import requests
from typing import Dict
from live_event_generator import main as fetch_data 
from tqdm.auto import tqdm
load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DBNAME")]
collection = db[os.getenv("MONGO_COLLECTION")]


# def fetch_data():
    
#     events = []
#     live_data = main()
#     offset = 0
#     limit = 100
    
#     for raw in tqdm(data):
        

#         if not page_events:
#             break

#         events.extend(page_events)
#         offset += limit

#     return events

def get_data(events: list) -> list:
    all_events = []

    for record in events:
        all_events.append({
            "event_id": record.get("event_id"),
            "event_type": record.get("event_type"),
            "event_time": record.get("event_time"),
            "vendor": record.get("vendor"),
            "payload": record.get("payload"),
            "ingested_at": record.get("ingested_at")
        })

    return all_events


def upsert_events(event: Dict):
    try:
        return collection.update_one(
            {"event_id": event["event_id"]},
            {"$set": event},
            upsert=True
        )
    except Exception as e:
        print(f"Error: {e}")


def main():
    events = []

    data = fetch_data()
    for raw in tqdm(data):
        events.append(raw)
    
    data = fetch_data()
    events = get_data(data)
    for event in events:
        upsert_events(event)