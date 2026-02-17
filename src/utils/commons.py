import json
import glob
from datetime import datetime
import hashlib

def hash(data):
    """Generate hash of data."""
    return hashlib.md5(str(data).encode()).hexdigest()

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
