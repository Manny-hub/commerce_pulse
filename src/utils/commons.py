import os
import json
import glob
import hashlib
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional, Generator

BASE_DIR = Path("data")

def hash(data):
    """Generate a stable MD5 hex digest of a dictionary or list."""
    encoded = json.dumps(data, sort_keys=True, default=str).encode('utf-8')
    return hashlib.md5(encoded).hexdigest()

def ingest_at():
    """Return current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()

def read_json(file_pattern):
    """Read JSON files matching a pattern; returns list, single object, or None."""
    files = glob.glob(file_pattern)
    if not files:
        return None
    
    data = []
    for file in files:
        with open(file, 'r') as f:
            data.append(json.load(f))
    return data[0] if len(data) == 1 else data

def get_latest_jsonl_file():
    """Find the first .jsonl file in the most recent date-named directory."""
    if not BASE_DIR.exists():
        return None
        
    date_folders = sorted([f for f in BASE_DIR.iterdir() if f.is_dir()])
    if not date_folders:
        return None
    
    return next(date_folders[-1].glob("*.jsonl"), None)

def read_jsonl(file):
    """Generator to yield parsed JSON objects line-by-line from a file object."""
    for line in file:
        if clean_line := line.strip():
            yield json.loads(clean_line)

def fetch_events(api_url: str) -> List[Dict]:
    """Fetch events from API, handling both list and wrapped 'events' keys."""
    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else data.get("events", [])
    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")
        return []

def get_event_type(file_path):
    """Map filename keywords to specific historical event types."""
    name = os.path.basename(file_path).lower()
    mapping = {
        "order": "historical_order",
        "payment": "historical_payment",
        "shipment": "historical_shipment",
        "refund": "historical_refund"
    }
    # Return the first match found, otherwise default
    for key, event_type in mapping.items():
        if key in name:
            return event_type
    return "historical_event"