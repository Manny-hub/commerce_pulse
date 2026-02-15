import json 
import haslib 
from datetime import datetime
from pathlib import Path
import os



def read_json(file_path):
    
    # a level to check the file extension before trying to read it as JSON
    if not file_path.endswith(".json"):
        raise ValueError("File must be a JSON file.")
    
    # Check if the file exists before processing (optional but recommended)
    if not path.is_file():
        print(f"Error: File not found at {file_path}")
        return

    # Open and process the file
    try:
        with open(path, 'r') as f:
            content = f.read()
            print(f"Successfully read from {file_path}")
    except IOError as e:
        print(f"Error reading file: {e}")

def hash(parts):
    dt = "|".join(parts).encode("utf-8")
    return hashlib.sha256(dt).hexdigest()


def ingest_at():
    dt = datetime.now(datetime.timezone.utc)
    return dt.isoformat()



    