import os
import subprocess
import sys
import unittest
from datetime import datetime

from lmdb_sensor_storage import Sensor, LMDBSensorStorage
from tests import EmptyDatabaseMixin


class TestCopySensorData(EmptyDatabaseMixin, unittest.TestCase):

    def setUp(self):
        super().setUp()
        s = Sensor(self.mdb_filename, 'source')
        s.write_value(datetime(2024, 1, 1), 1.0)
        s.metadata['unit'] = 'V'
        s.notes.add_note('note', timestamp=datetime(2024, 1, 1))
        s.close_file()

    def copy_sensor_data(self, export_mdb_filename, export_sensor_name, *args, import_sensor_name='source'):
        return subprocess.run([sys.executable, '-m', 'lmdb_sensor_storage.copy_sensor_data',
                               '--import-mdb-filename', self.mdb_filename,
                               '--import-sensor-name', import_sensor_name,
                               '--export-mdb-filename', export_mdb_filename,
                               '--export-sensor-name', export_sensor_name, *args],
                              env={**os.environ, 'PYTHONPATH': os.pathsep.join(sys.path)},
                              cwd=self.tempfolder, timeout=60).returncode

    def test_copy(self):
        export_mdb_filename = os.path.join(self.tempfolder, 'export.mdb')
        self.assertEqual(0, self.copy_sensor_data(export_mdb_filename, 'dest'))

        self.assertEqual(Sensor(export_mdb_filename, 'dest').items(), [(datetime(2024, 1, 1), 1.0)])
        self.assertEqual(sorted(os.listdir(self.tempfolder)),
                         ['export.mdb', 'export.mdb-lock', 'unittest.mdb', 'unittest.mdb-lock'])
        self.assertIn('source', LMDBSensorStorage(self.mdb_filename).keys())

    def test_move(self):
        export_mdb_filename = os.path.join(self.tempfolder, 'export.mdb')
        self.assertEqual(0, self.copy_sensor_data(export_mdb_filename, 'dest', '--move'))

        dest = Sensor(export_mdb_filename, 'dest')
        self.assertEqual(dest.items(), [(datetime(2024, 1, 1), 1.0)])
        self.assertEqual(dest.metadata['unit'], 'V')
        self.assertEqual(len(dest.notes), 1)
        self.assertEqual([], LMDBSensorStorage(self.mdb_filename).keys())

    def test_move_within_file(self):
        self.assertEqual(0, self.copy_sensor_data(self.mdb_filename, 'dest', '--move'))
        self.assertEqual(['dest'], LMDBSensorStorage(self.mdb_filename).keys())
        self.assertEqual(Sensor(self.mdb_filename, 'dest').items(), [(datetime(2024, 1, 1), 1.0)])

    def test_missing_source(self):
        export_mdb_filename = os.path.join(self.tempfolder, 'export.mdb')
        self.assertNotEqual(0, self.copy_sensor_data(export_mdb_filename, 'dest', '--move',
                                                     import_sensor_name='misspelled'))
        self.assertFalse(os.path.exists(export_mdb_filename))
