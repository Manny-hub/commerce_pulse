from __future__ import annotations
import hashlib
from typing import Optional, Tuple
import pandas as pd

def fetch_mongo_collection(
    mongo_uri: str,
    db_name: str,
    collection: str,
) -> List[Dict[str, Any]]:
    """
    Fetch documents from MongoDB.
    Requires: pip install pymongo
    """
    from pymongo import MongoClient  # local import so script still runs without pymongo if using JSON files

    client = MongoClient(mongo_uri)
    col = client[db_name][collection]

    cursor = col.find(query or {}, projection)
    if limit:
        cursor = cursor.limit(limit)

    docs = list(cursor)
    # Optional: remove Mongo ObjectId field if you don't need it
    for d in docs:
        d.pop("_id", None)
    return docs