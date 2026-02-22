from pymongo import MongoClient
from google.cloud import bigquery
from src.database.upsert import get_collection


collection = get_collection()
bq_client = bigquery.Client()