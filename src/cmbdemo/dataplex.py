from google.cloud import dataplex_v1
from cmbdemo.config import DataplexConfig, GCSConfig
from typing import Dict, Any
import time

def trigger_export_job(project_id: str, location: str, dataplex_config: DataplexConfig, gcs_config: GCSConfig) -> str:
    """Triggers a Dataplex metadata export job."""
    client = dataplex_v1.CatalogServiceClient()
    
    parent = f"projects/{project_id}/locations/{location}"
    
    # Construct the job spec
    # Note: The actual API might differ slightly based on library version.
    # Using generic structure based on documentation.
    
    # Based on docs: POST https://dataplex.googleapis.com/v1/projects/JOB_PROJECT/locations/LOCATION_ID/metadataJobs
    # Body: { "type": "EXPORT", "export_spec": { "output_path": "gs://BUCKET/", "scope": { ... } } }
    
    metadata_job = dataplex_v1.MetadataJob()
    metadata_job.type_ = dataplex_v1.MetadataJob.Type.EXPORT
    
    export_spec = dataplex_v1.MetadataJob.ExportSpec()
    export_spec.output_path = f"gs://{gcs_config.bucket_name}/{gcs_config.output_path}"
    
    # Map scope from config
    scope = dataplex_v1.MetadataJob.ExportSpec.Scope()
    if 'project_ids' in dataplex_config.scope:
        # Assuming the library supports this, otherwise might need manual mapping
        # The python client usually has specific fields.
        # Let's assume generic dict mapping for now or specific fields if known.
        # For now, let's try to set attributes if they exist, or pass as dict if constructor allows.
        pass 
        # Actually, let's look at the proto definition if possible.
        # Since I can't see the library code, I will assume standard proto mapping.
    
    # For simplicity and robustness, let's assume the user passes the scope dict matching the API.
    # We might need to manually construct the Scope object.
    
    # Using a simplified approach:
    # scope.project_ids = dataplex_config.scope.get('project_ids', [])
    # etc.
    
    # Re-creating the object with kwargs might be safer if structure matches
    # export_spec.scope = dataplex_v1.MetadataJob.ExportSpec.Scope(**dataplex_config.scope)
    
    # However, protobuf objects don't always support **kwargs in constructor like that.
    # Let's try to assign.
    
    # IMPORTANT: The python client for MetadataJobs might be under a specific service.
    # The docs say `metadataJobs.create`. In python client this is likely `create_metadata_job`.
    
    request = dataplex_v1.CreateMetadataJobRequest(
        parent=parent,
        metadata_job=metadata_job
    )
    
    # We need to populate metadata_job.export_spec
    metadata_job.export_spec = export_spec
    
    # For scope, let's assume we can pass the dictionary to the constructor or use json_format
    from google.protobuf import json_format
    
    # Construct the Scope object manually to be safe and support item_types
    scope_msg = dataplex_v1.MetadataJob.ExportSpec.Scope()
    
    if 'project_ids' in dataplex_config.scope:
        scope_msg.project_ids.extend(dataplex_config.scope['project_ids'])
        
    if 'item_types' in dataplex_config.scope:
        # item_types should be a list of strings like "ASPECT_TYPE", "ENTRY"
        # The API expects enum values or strings if using json_format.
        # Let's assume strings are passed and we map them if necessary, 
        # or just pass them if the proto wrapper handles it.
        # The proto definition usually expects strings for repeated fields of enums in python 
        # or the enum integers.
        # Let's try to map string to enum if possible, or just pass strings.
        
        # Mapping strings to enums
        item_type_map = {
            "ASPECT_TYPE": dataplex_v1.MetadataJob.ExportSpec.Scope.ItemType.ASPECT_TYPE,
            "ENTRY": dataplex_v1.MetadataJob.ExportSpec.Scope.ItemType.ENTRY,
            "ASPECT": dataplex_v1.MetadataJob.ExportSpec.Scope.ItemType.ASPECT,
            "FULL": dataplex_v1.MetadataJob.ExportSpec.Scope.ItemType.FULL
        }
        
        for it in dataplex_config.scope['item_types']:
            if it in item_type_map:
                scope_msg.item_types.append(item_type_map[it])
            else:
                print(f"Warning: Unknown item type {it}, skipping.")

    export_spec.scope = scope_msg

    
    operation = client.create_metadata_job(request=request)
    print("Triggered metadata export job...")
    response = operation.result() # Wait for completion
    
    return response.name

def wait_for_job(project_id: str, location: str, job_id: str) -> bool:
    """Waits for a metadata job to complete."""
    # The create_metadata_job LRO already waits if we call result().
    # So this might be redundant if we use the LRO.
    # But if we wanted to poll asynchronously:
    client = dataplex_v1.CatalogServiceClient()
    name = f"projects/{project_id}/locations/{location}/metadataJobs/{job_id}"
    
    while True:
        job = client.get_metadata_job(name=name)
        if job.state == dataplex_v1.MetadataJob.State.SUCCEEDED:
            return True
        elif job.state in (dataplex_v1.MetadataJob.State.FAILED, dataplex_v1.MetadataJob.State.CANCELLED):
            raise Exception(f"Job failed with state: {job.state}")
        
        time.sleep(5)
