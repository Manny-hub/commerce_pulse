import json
import glob
from datetime import datetime
import hashlib


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
