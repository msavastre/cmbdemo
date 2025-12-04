from google.cloud import bigquery
from cmbdemo.config import BigQueryConfig, GCSConfig
from typing import List, Dict, Any

def create_metadata_reporting_table(project_id: str, location: str, bq_config: BigQueryConfig, gcs_config: GCSConfig):
    """Creates a BigLake table for reporting on exported Dataplex metadata."""
    client = bigquery.Client(project=project_id, location=location)
    
    dataset_ref = bigquery.DatasetReference(project_id, bq_config.dataset_id)
    
    # Ensure dataset exists
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)
        print(f"Created dataset {bq_config.dataset_id}")

    table_name = "dataplex_metadata_export"
    table_ref = dataset_ref.table(table_name)
    
    # Construct ExternalConfig for BigLake
    # Source URI should point to the root of the export path
    source_uri = f"gs://{gcs_config.bucket_name}/{gcs_config.output_path.rstrip('/')}/*"
    
    external_config = bigquery.ExternalConfig("HIVE_PARTITIONING")
    external_config.source_uris = [source_uri]
    external_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    
    # Configure Hive partitioning
    # The path format is: .../year=YYYY/month=MM/day=DD/consumer_project=.../job=.../project=.../entry_group=.../FILE.jsonl
    # We need to define the hive partitioning options.
    # Auto-detection might work if the layout is standard.
    # But explicitly setting mode to AUTO or STRINGS is safer.
    external_config.hive_partitioning = bigquery.HivePartitioningOptions()
    external_config.hive_partitioning.mode = "AUTO"
    external_config.hive_partitioning.source_uri_prefix = f"gs://{gcs_config.bucket_name}/{gcs_config.output_path.rstrip('/')}"

    table = bigquery.Table(table_ref)
    table.external_data_configuration = external_config
    
    # Try to create (or update)
    try:
        client.create_table(table, exists_ok=True)
        print(f"Created/Updated BigLake table {bq_config.dataset_id}.{table_name}")
    except Exception as e:
        print(f"Failed to create table {table_name}: {e}")
