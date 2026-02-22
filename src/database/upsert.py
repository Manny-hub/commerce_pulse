from pymongo import MongoClient, UpdateOne
from typing import List, Dict
import os

def get_collection():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("MONGO_DBNAME")]
    return db[os.getenv("MONGO_COLLECTION")]


def bulk_upsert_events(events: List[Dict]):
    """
    Perform bulk upsert into MongoDB.
    """
    collection = get_collection()

    operations = [
        UpdateOne(
            {"event_id": event["event_id"]},
            {"$set": event},
            upsert=True
        )
        for event in events
        if event.get("event_id")
    ]

    if operations:
        collection.bulk_write(operations)