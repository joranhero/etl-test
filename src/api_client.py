import json
import time

def fetch_orders(file_path, max_retries=3):
    for attempt in range(max_retries):
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error fetching: {e}. Retry {attempt+1}/{max_retries}")
            time.sleep(1)
    raise Exception("Failed to fetch orders after retries")
