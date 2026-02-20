import os 
import sys
# Add the path where your script lives so Airflow can find it
sys.path.insert(0, '/home/dell/commercepulse/src')

current_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.transforms.live_ingest import main as run_ingest