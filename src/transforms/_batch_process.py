import json 
from src.utils.commons import hash, ingest_at, read_json
from dotenv import load_dotenv
import os 
from bson import ObjectId as object_id
from pymongo import MongoClient

load_dotenv()


paths ="data/bootstrap/*.json" 

data = read_json(paths)
time = ingest_at()

client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DBNAME")]
collection = db[os.getenv("MONGO_COLLECTION")]

def read_event(file_path):
    
    data = read_json(paths)
    raw = {
        "event_id": object_id(),
        "payload": data,
        "metadata": {
            "source_file": file_path,
            "ingest_at": ingest_at(),        
            },
    }
    return raw
    
    return raw

def upsert_event(event: dict):
    """
    Insert or update event deterministically.
    """

    result = collection.update_one(
        {"event_id": event["event_id"]},
        {"$set": event},
        upsert=True
    )

    return result



if __name__ == "__main__":
    upsert_event(read_event(paths))