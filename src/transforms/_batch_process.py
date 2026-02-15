import json 
from utils.commons import hash, ingest_at, read_json


paths ="data/bootstrap/*.json" 

def read_event(file_path):
    
    data = read_json(paths)
    