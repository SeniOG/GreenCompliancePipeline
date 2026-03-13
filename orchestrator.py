import requests
import json
import os

METADATA_FILE = 'metadata.json'

def should_run_sync(source_id, url):
    """Checks the ETag/Last-Modified header against local JSON."""
    try:
        # allow_redirects=True is critical to reach the actual file headers
        response = requests.head(url, timeout=10, allow_redirects=True)
        current_etag = response.headers.get('ETag') or response.headers.get('Last-Modified')
    except Exception as e:
        print(f"Connection error: {e}")
        return False, None

    # If metadata is missing, we definitely need to run
    if not os.path.exists(METADATA_FILE):
        return True, current_etag

    with open(METADATA_FILE, 'r') as f:
        metadata = json.load(f)
    
    last_etag = metadata.get(source_id, {}).get('etag')
    
    # FIX: If the server doesn't give us an ETag, we can't prove it's "the same".
    # We should sync if current_etag is None OR if it doesn't match the last one.
    if current_etag is None or current_etag != last_etag:
        return True, current_etag
    
    return False, None


def update_sync_metadata(source_id, etag):
    metadata = {}
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)
    
    metadata[source_id] = {'etag': etag, 'status': 'success'}
    with open(METADATA_FILE, 'w') as f:
        json.dump(metadata, f, indent=4)