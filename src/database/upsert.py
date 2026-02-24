from pymongo import MongoClient, UpdateOne
from pymongo.results import BulkWriteResult
from typing import List, Dict, Optional
import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

def get_collection():
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGO_DBNAME") or os.getenv("MONGO_DB")
    collection_name = os.getenv("MONGO_COLLECTION", "events_raw")

    if not db_name:
        raise ValueError("Missing Mongo DB name: set MONGO_DBNAME or MONGO_DB in environment.")

    client = MongoClient(mongo_uri)
    db = client[db_name]
    return client, db[collection_name]


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
