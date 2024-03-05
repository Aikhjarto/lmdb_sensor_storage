import unittest
from datetime import datetime
from lmdb_sensor_storage import Sensor, LMDBSensorStorage
# noinspection PyProtectedMember
from lmdb_sensor_storage.db._manager import manager
from tests import EmptyDatabaseMixin


class TestcaseLMDBSensorStorage(EmptyDatabaseMixin, unittest.TestCase):

    def test_get_on_empty_dB(self):
        m = LMDBSensorStorage(self.mdb_filename)
        self.assertIsInstance(m.statistics, dict)
        self.assertIsNotNone(m._environment)
        self.assertEqual(m.keys(), [])
        self.assertEqual(m.get_non_empty_sensor_names(), [])
        self.assertEqual(m.get_non_empty_sensors(), [])
        m.close()
        self.assertEqual(manager.handles, {})

    def test_delete(self):
        s1 = Sensor(self.mdb_filename, 'test')
        s2 = Sensor(self.mdb_filename, 'test2')
        file = LMDBSensorStorage(self.mdb_filename)

        self.assertFalse(file.keys())

        s1[datetime.now()] = 1.0
        s2[datetime.now()] = 2.0
        self.assertEqual(file.keys(), ['test', 'test2'])

        del file['test']
        self.assertEqual(file.keys(), ['test2'])

        del file['test2']
        self.assertFalse(file.keys())
