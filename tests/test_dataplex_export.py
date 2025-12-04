import sys
from unittest.mock import MagicMock

# Mock google.cloud modules before importing app code
sys.modules['google.cloud'] = MagicMock()
sys.modules['google.cloud.dataplex_v1'] = MagicMock()
sys.modules['google.cloud.storage'] = MagicMock()
sys.modules['google.cloud.bigquery'] = MagicMock()
sys.modules['google.protobuf.json_format'] = MagicMock()

import unittest
from unittest.mock import patch, MagicMock
from cmbdemo.dataplex import trigger_export_job
from cmbdemo.gcs import read_exported_metadata
from cmbdemo.config import DataplexConfig, GCSConfig

class TestDataplexExportJob(unittest.TestCase):
    @patch('cmbdemo.dataplex.dataplex_v1.CatalogServiceClient')
    def test_trigger_export_job(self, mock_client):
        mock_instance = mock_client.return_value
        mock_operation = MagicMock()
        mock_operation.result.return_value.name = "projects/p/locations/l/metadataJobs/j"
        mock_instance.create_metadata_job.return_value = mock_operation
        
        config = DataplexConfig(scope={"project_ids": ["p"]})
        gcs_config = GCSConfig(bucket_name="b", output_path="o")
        job_name = trigger_export_job("project", "location", config, gcs_config)
        
        self.assertEqual(job_name, "projects/p/locations/l/metadataJobs/j")

    @patch('cmbdemo.gcs.storage.Client')
    def test_read_exported_metadata(self, mock_client):
        mock_bucket = mock_client.return_value.bucket.return_value
        mock_blob = MagicMock()
        mock_blob.name = "test.jsonl"
        mock_blob.download_as_text.return_value = '{"entry": {"name": "e1"}}\n{"entry": {"name": "e2"}}'
        mock_bucket.list_blobs.return_value = [mock_blob]
        
        config = GCSConfig(bucket_name="bucket", output_path="path")
        metadata = read_exported_metadata("project", config)
        
        self.assertEqual(len(metadata), 2)
        self.assertEqual(metadata[0]['entry']['name'], 'e1')

if __name__ == '__main__':
    unittest.main()
