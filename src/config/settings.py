from __future__ import annotations
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class MongoConfig:
    uri: str
    db: str
    col: str = "events_raw"

@dataclass(frozen=True)
class BigQueryConfig:
    project_id: str
    dataset_id: str
    location: str = "EU"  # you asked to use EU

def load_mongo_config() -> MongoConfig:
    return MongoConfig(
        uri=os.environ["MONGO_URI"],
        db=os.environ["MONGO_DBNAME"],
        col=os.getenv("MONGO_COLLECTION"),  
    )

def load_bq_config() -> BigQueryConfig:
    return BigQueryConfig(
        project_id=os.environ["BQ_PROJECT_ID"],
        dataset_id=os.environ["BQ_DATASET_ID"],
        location=os.getenv("BQ_LOCATION", "EU"),
    )