from __future__ import annotations
import os
from dataclasses import dataclass
from dotenv import load_dotenv


load_dotenv()  # Load environment variables from .env file

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
        uri=os.getenv("MONGO_URI"),
        db=os.getenv("MONGO_DBNAME"),
        col=os.getenv("MONGO_COLLECTION"),  
    )

def load_bq_config() -> BigQueryConfig:
    return BigQueryConfig(
        project_id=os.getenv("BQ_PROJECT_ID"),
        dataset_id=os.getenv("BQ_DATASET_ID"),
        location=os.getenv("BQ_LOCATION", "EU"),
    )