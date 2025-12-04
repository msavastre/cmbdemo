import sys
from unittest.mock import MagicMock

# Mock google.cloud modules before importing app code
sys.modules['google.cloud'] = MagicMock()
sys.modules['google.cloud.dataplex_v1'] = MagicMock()
sys.modules['google.cloud.storage'] = MagicMock()
sys.modules['google.cloud.bigquery'] = MagicMock()
sys.modules['google.protobuf.json_format'] = MagicMock()

import unittest
from cmbdemo.main import main
from io import StringIO
import sys
from unittest.mock import patch

class TestMain(unittest.TestCase):
    @patch('cmbdemo.main.load_config')
    @patch('cmbdemo.main.trigger_export_job')
    @patch('cmbdemo.main.read_exported_metadata')
    @patch('cmbdemo.main.create_metadata_reporting_table')
    def test_main(self, mock_create_table, mock_read_metadata, mock_trigger_job, mock_load_config):
        # Setup mocks
        mock_load_config.return_value = MagicMock()
        mock_trigger_job.return_value = "job_id"
        mock_read_metadata.return_value = []

        with patch('sys.stdout', new=StringIO()) as fake_out:
            with patch('sys.argv', ['cmbdemo', '--config', 'config.yaml']):
                result = main()
                self.assertEqual(result, 0)
                self.assertIn("Done.", fake_out.getvalue())

if __name__ == '__main__':
    unittest.main()
