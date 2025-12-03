import unittest
from cmbdemo.main import main
from io import StringIO
import sys
from unittest.mock import patch

class TestMain(unittest.TestCase):
    def test_main(self):
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = main()
            self.assertEqual(result, 0)
            self.assertEqual(fake_out.getvalue().strip(), "Hello from cmbdemo!")

if __name__ == '__main__':
    unittest.main()
