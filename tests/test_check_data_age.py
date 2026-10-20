import os
import subprocess
import sys
import unittest
from datetime import datetime, timedelta

from lmdb_sensor_storage import Sensor, LMDBSensorStorage
from tests import EmptyDatabaseMixin

SCRIPT = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                      'scripts', 'check_lmdb_sensor_data_age.py')


class TestCheckDataAge(EmptyDatabaseMixin, unittest.TestCase):

    def setUp(self):
        super().setUp()
        now = datetime.now()
        Sensor(self.mdb_filename, 'new').write_value(now - timedelta(seconds=10), 1.0)
        Sensor(self.mdb_filename, 'old').write_value(now - timedelta(days=2, seconds=10), 1.0)
        Sensor(self.mdb_filename, 'nan').write_value(now - timedelta(seconds=10), float('nan'))
        Sensor(self.mdb_filename, 'Temperatur Kühlschrank').write_value(now - timedelta(seconds=10), 1.0)

    def check(self, *args):
        cmd = [sys.executable, SCRIPT, '-w', '60', '-c', '3600', '--mdb-filename', self.mdb_filename]
        for arg in args:
            cmd.append(arg)
        p = subprocess.run(cmd, capture_output=True, timeout=60,
                           env={**os.environ, 'PYTHONPATH': os.pathsep.join(sys.path)})
        return p.returncode, p.stdout.decode()

    def test_ok(self):
        self.assertEqual(0, self.check('--sensor-name', 'new')[0])

    def test_critical(self):
        code, out = self.check('--sensor-name', 'old')
        self.assertEqual(2, code)
        # days are included in the age
        self.assertIn(f'age={2 * 86400 + 10}s', out)

    def test_most_recent_and_oldest_of(self):
        self.assertEqual(0, self.check('--sensor-name', 'new', '--sensor-name', 'old')[0])
        self.assertEqual(2, self.check('--sensor-name', 'new', '--sensor-name', 'old', '--oldest-of')[0])
        self.assertEqual(0, self.check('--sensor-name', 'new', '--sensor-name', 'nan', '--oldest-of',
                                       '--no-error-if-nan')[0])

    def test_nan(self):
        code, out = self.check('--sensor-name', 'new', '--sensor-name', 'nan')
        self.assertEqual(2, code)
        self.assertIn('Sensors with nan: nan|', out)
        self.assertEqual(0, self.check('--sensor-name', 'nan', '--no-error-if-nan')[0])

    def test_unknown_sensor_is_not_created(self):
        code, out = self.check('--sensor-name', 'misspelled')
        self.assertEqual(3, code)
        self.assertNotIn('misspelled', LMDBSensorStorage(self.mdb_filename).keys())

    def test_sensor_name_encoding(self):
        # utf-8 as well as latin-1 encoded names, as sent e.g. by check_by_ssh
        for encoding in ('utf-8', 'latin-1'):
            name = os.fsdecode('Temperatur Kühlschrank'.encode(encoding))
            self.assertEqual(0, self.check('--sensor-name', name)[0], encoding)
