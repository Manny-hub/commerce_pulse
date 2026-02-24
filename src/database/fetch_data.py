from __future__ import annotations
from typing import Any, Dict, List, Optional


def fetch_mongo_collection(
    mongo_uri: str,
    db_name: str,
    collection: str,
    query: Optional[Dict] = None,
    projection: Optional[Dict] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch full event documents from MongoDB.
    Returns a list of event dicts (with event_type, payload, etc.) —
    the normalize_* functions each filter to their own event types.
    """
    from pymongo import MongoClient

    client = MongoClient(mongo_uri)
    try:
        col = client[db_name][collection]
        cursor = col.find(query or {}, projection)
        if limit:
            cursor = cursor.limit(limit)
        docs = list(cursor)
    finally:
        client.close()

    # Strip Mongo's ObjectId — everything else is passed through as-is
    for d in docs:
        d.pop("_id", None)
    return docs
