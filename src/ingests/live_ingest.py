import os
from dotenv import load_dotenv
from src.utils.commons import fetch_events
from src.database.upsert import bulk_upsert_events

load_dotenv()

api_url = "https://commerce-pulse-api.onrender.com/events?events=2000"


def transform_events(events):
    """
    Normalize API response into MongoDB schema.
    """
    all_events = []

    for record in events:
        event_id = record.get("event_id") or hash(record)
        
        all_events.append({
            "event_id": event_id,
            "event_type": record.get("event_type"),
            "event_time": record.get("event_time"),
            "vendor": record.get("vendor"),
            "payload": record.get("payload"),
            "ingested_at": record.get("ingested_at")
        })

    return all_events


def main():
    print("Fetching events from API...")
    raw_events = fetch_events(api_url)

    if not raw_events:
        print("No events fetched.")
        return

    print("Transforming events...")
    transformed_events = transform_events(raw_events)

    print("Upserting into MongoDB...")
    bulk_upsert_events(transformed_events)

    print("Ingestion completed successfully.")


if __name__ == "__main__":
    main()