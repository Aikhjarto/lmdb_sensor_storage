import os
import subprocess
import sys
import unittest
from datetime import datetime

from lmdb_sensor_storage import Sensor
from tests import EmptyDatabaseMixin


class TestCopySensorData(EmptyDatabaseMixin, unittest.TestCase):

    def test_copy(self):
        s = Sensor(self.mdb_filename, 'source')
        s.write_value(datetime(2024, 1, 1), 1.0)
        s.close_file()

        export_mdb_filename = os.path.join(self.tempfolder, 'export.mdb')
        subprocess.run([sys.executable, '-m', 'lmdb_sensor_storage.copy_sensor_data',
                        '--import-mdb-filename', self.mdb_filename, '--import-sensor-name', 'source',
                        '--export-mdb-filename', export_mdb_filename, '--export-sensor-name', 'dest'],
                       env={**os.environ, 'PYTHONPATH': os.pathsep.join(sys.path)},
                       cwd=self.tempfolder, check=True, timeout=60)

        self.assertEqual(Sensor(export_mdb_filename, 'dest').items(), [(datetime(2024, 1, 1), 1.0)])
        self.assertEqual(sorted(os.listdir(self.tempfolder)),
                         ['export.mdb', 'export.mdb-lock', 'unittest.mdb', 'unittest.mdb-lock'])
