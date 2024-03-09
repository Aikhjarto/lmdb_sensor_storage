import json
import unittest
from datetime import datetime, timedelta
from lmdb_sensor_storage import Sensor, LMDBSensorStorage
# noinspection PyProtectedMember
from lmdb_sensor_storage.db._manager import manager
# noinspection PyProtectedMember
from lmdb_sensor_storage._parser import as_datetime
from tests import EmptyDatabaseMixin


class TestcaseLMDBSensorStorage(EmptyDatabaseMixin, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.reference_date = as_datetime("2000-01-01")

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

    def test_get_json(self):
        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='s1', data_format='f')
        s[self.reference_date] = 1
        s[self.reference_date + timedelta(seconds=5)] = 2
        s[self.reference_date + timedelta(seconds=10.1)] = 3
        s[self.reference_date + timedelta(seconds=15)] = 4

        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='s2', data_format='f')
        s[self.reference_date] = 10
        s[self.reference_date + timedelta(seconds=5)] = 20
        s[self.reference_date + timedelta(seconds=6.5)] = 30
        s[self.reference_date + timedelta(seconds=15)] = 40

        db = LMDBSensorStorage(self.mdb_filename)

        # test without limits
        json_bytes = db.get_json(['s1', 's2'])
        data = json.loads(json_bytes)
        ref = {'Time': ['2000-01-01T00:00:00',
                        '2000-01-01T00:00:05',
                        '2000-01-01T00:00:06.500000',
                        '2000-01-01T00:00:10.100000',
                        '2000-01-01T00:00:15'],
               's1': [1.0, 2.0, 2.0, 3.0, 4.0],
               's2': [10.0, 20.0, 30.0, 30.0, 40.0]}
        self.assertEqual(ref, data)

        # test with since and until
        json_bytes = db.get_json(['s1', 's2'],
                                 since=self.reference_date + timedelta(seconds=2),
                                 until=self.reference_date + timedelta(seconds=11))

        data = json.loads(json_bytes)
        ref = {'Time': [(self.reference_date + timedelta(seconds=5)).isoformat(),
                        (self.reference_date + timedelta(seconds=6.5)).isoformat(),
                        (self.reference_date + timedelta(seconds=10.1)).isoformat()],
               's1': [2.0, 2.0, 3.0],
               's2': [20.0, 30.0, 30.0]}
        self.assertEqual(ref, data)


