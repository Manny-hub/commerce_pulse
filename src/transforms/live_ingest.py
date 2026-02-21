import os
from dotenv import load_dotenv
from pymongo import MongoClient
import requests
from typing import Dict

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DBNAME")]
collection = db[os.getenv("MONGO_COLLECTION")]


def fetch_data():
    
    limit = 1000
    offset = 0
    events = []

    url = "https://commerce-pulse-api.onrender.com/events"
    
    while True:
        params = {
            "limit": limit,
            "offset": offset
        }

        response = requests.get(url, params=params)
        response.raise_for_status()

        page_events = response.json()

        if not page_events:
            break

        events.extend(page_events)
        offset += limit

    return events

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
    data = fetch_data()
    events = get_data(data)
    for event in events:
        upsert_events(event)