import os
import subprocess
import sys
import unittest
from datetime import datetime

from lmdb_sensor_storage import Sensor
from lmdb_sensor_storage.import_csv import read_csv
from tests import EmptyDatabaseMixin


class TestImportCSV(EmptyDatabaseMixin, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.csv_filename = os.path.join(self.tempfolder, 'data.csv')
        with open(self.csv_filename, 'w') as f:
            for minute in range(5):
                f.write(f'2024-01-01T00:0{minute}:00,ignored,{minute + 0.5}\n')

    def import_csv(self, *args):
        return subprocess.run([sys.executable, '-m', 'lmdb_sensor_storage.import_csv',
                               '--csv-filename', self.csv_filename, '--mdb-filename', self.mdb_filename, *args],
                              env={**os.environ, 'PYTHONPATH': os.pathsep.join(sys.path)},
                              capture_output=True, timeout=60).returncode

    def test_import(self):
        self.assertEqual(0, self.import_csv('--sensor-name', 'imported'))
        s = Sensor(self.mdb_filename, 'imported')
        self.assertEqual('f', s.data_format)
        self.assertEqual([(datetime(2024, 1, 1, 0, minute).astimezone(), minute + 0.5) for minute in range(5)], s.items())

    def test_sensor_name_required(self):
        self.assertNotEqual(0, self.import_csv())

    def test_read_csv_since_until(self):
        _, dates, values = read_csv(self.csv_filename, since='2024-01-01T00:01', until='2024-01-01T00:03')
        self.assertEqual([datetime(2024, 1, 1, 0, minute).astimezone() for minute in (1, 2, 3)], dates)
        self.assertEqual([1.5, 2.5, 3.5], values)
