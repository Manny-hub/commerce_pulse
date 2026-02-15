import json 
from utils.commons import hash, ingest_at, 
from dotenv import load_dotenv
import os 


load_dotenv()


paths ="data/bootstrap/*.json" 

def get_config():
    
    MONGO_URL = os.getenv("MONGO_URI")
    MONGO_DBNAME = os.getenv("MONGO_DBNAME")
    MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

def read_event(file_path):
    
    data = read_json(paths)
    raw = {
        "event_id": hash([data["event_name"], data["timestamp"]]),
        "event_name": data["event_name"],
        "timestamp": data["timestamp"],
        "ingest_at": ingest_at(),
    }
    
    