import sys
import argparse
from cmbdemo.config import load_config
from cmbdemo.dataplex import trigger_export_job
from cmbdemo.gcs import read_exported_metadata
from cmbdemo.bigquery import create_metadata_reporting_table

def main():
    """Main entry point for the application."""
    parser = argparse.ArgumentParser(description="Dataplex Export Tool")
    parser.add_argument("--config", help="Path to configuration file", required=True)
    args = parser.parse_args()

    try:
        config = load_config(args.config)
        print(f"Loaded configuration for project: {config.project_id}")

        print("Triggering Dataplex metadata export job...")
        job_name = trigger_export_job(config.project_id, config.location, config.dataplex, config.gcs)
        print(f"Job triggered: {job_name}")
        
        # In a real scenario, we might want to poll for completion here or let the user know to check later.
        # For this demo, since trigger_export_job waits for the LRO, we proceed.
        
        # We don't necessarily need to read the metadata in python anymore if we just want the BQ table.
        # But keeping it for verification or other processing is fine.
        # If the user only wants the BQ table, we can skip reading.
        # However, let's keep it as per previous instructions unless told otherwise, 
        # but the BQ table creation doesn't depend on the read metadata anymore.
        
        print("Creating BigLake table for metadata reporting...")
        create_metadata_reporting_table(config.project_id, config.location, config.bigquery, config.gcs)
        print("Done.")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())
