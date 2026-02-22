import os
import json
import glob
from datetime import datetime
import hashlib
import time
from pathlib import Path
import requests
from typing import List, Dict

def hash(data):
    """Generate a stable MD5 hash of a dictionary or list."""
    # Ensure keys are always in the same order before hashing
    encoded_data = json.dumps(data, sort_keys=True, default=str).encode('utf-8')
    return hashlib.md5(encoded_data).hexdigest()

def ingest_at():
    """Return current timestamp for ingestion."""
    return datetime.utcnow().isoformat()

def read_json(file_pattern):
    """Read JSON files matching a glob pattern."""
    files = glob.glob(file_pattern)
    data = []
    for file in files:
        with open(file, 'r') as f:
            data.append(json.load(f))
    return data if len(data) > 1 else data[0] if data else None


BASE_DIR = Path("data")


def get_latest_jsonl_file():
    # Get all date folders
    date_folders = [f for f in BASE_DIR.iterdir() if f.is_dir()]
    
    if not date_folders:
        return None
    
    # Sort folders by name (assuming YYYY-MM-DD format)
    latest_folder = sorted(date_folders)[-1]
    
    # Find jsonl file inside
    jsonl_files = list(latest_folder.glob("*.jsonl"))
    
    if not jsonl_files:
        return None
    
    return jsonl_files[0]


def read_jsonl(file):
    
    for line in file.read().splitlines():
        line = line.strip()
        data = yield json.loads(line)
    return data


def fetch_events(api_url: str) -> List[Dict]:
    """
    Fetch events from external API.
    """
    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, list):
            return data

        # If API wraps response
        return data.get("events", [])

    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")
        return []
    
    
def get_event_type(file_path):
    """Maps filename (e.g., orders_2023.json) to event_type (historical_order)."""
    filename = os.path.basename(file_path).lower()
    if "order" in filename: return "historical_order"
    if "payment" in filename: return "historical_payment"
    if "shipment" in filename: return "historical_shipment"
    if "refund" in filename: return "historical_refund"
    return "historical_event"