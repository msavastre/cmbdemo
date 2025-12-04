from google.cloud import storage
from cmbdemo.config import GCSConfig
import json
from typing import List, Dict, Any

def read_exported_metadata(project_id: str, config: GCSConfig) -> List[Dict[str, Any]]:
    """Reads exported metadata JSONL files from GCS."""
    client = storage.Client(project=project_id)
    bucket = client.bucket(config.bucket_name)
    
    # The export path might contain multiple files.
    # The path structure is: gs://BUCKET/PREFIX/year=YYYY/month=MM/day=DD/...
    # We need to list all files under the output_path and parse them.
    
    blobs = bucket.list_blobs(prefix=config.output_path)
    
    metadata = []
    for blob in blobs:
        if blob.name.endswith('.jsonl'):
            print(f"Reading {blob.name}...")
            content = blob.download_as_text()
            for line in content.splitlines():
                if line.strip():
                    metadata.append(json.loads(line))
                    
    return metadata
