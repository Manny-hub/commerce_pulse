from pymongo import MongoClient, UpdateOne
from pymongo.results import BulkWriteResult
from typing import List, Dict, Optional
import os


def get_collection():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("MONGO_DBNAME")]
    return client, db[os.getenv("MONGO_COLLECTION")]


def bulk_upsert_events(events: List[Dict]) -> Optional[BulkWriteResult]:
    """
    Perform bulk upsert into MongoDB.
    Returns the BulkWriteResult or None if there were no valid operations.
    """
    client, collection = get_collection()
    try:
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
            return collection.bulk_write(operations)
        return None
    finally:
        client.close()
