import requests
import json
import os

METADATA_FILE = 'metadata.json'

def should_run_sync(source_id, url):
    """Checks the ETag/Last-Modified header against local JSON."""
    try:
        response = requests.head(url, timeout=10)
        current_etag = response.headers.get('ETag') or response.headers.get('Last-Modified')
    except Exception as e:
        print(f"Connection error: {e}")
        return False, None

    if not os.path.exists(METADATA_FILE):
        return True, current_etag

    with open(METADATA_FILE, 'r') as f:
        metadata = json.load(f)
    
    last_etag = metadata.get(source_id, {}).get('etag')
    if current_etag == last_etag:
        return False, None
    
    return True, current_etag

def update_sync_metadata(source_id, etag):
    metadata = {}
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)
    
    metadata[source_id] = {'etag': etag, 'status': 'success'}
    with open(METADATA_FILE, 'w') as f:
        json.dump(metadata, f, indent=4)