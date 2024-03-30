import unittest
from . import EmptyDatabaseMixin
from lmdb_sensor_storage import LMDBSensorStorage
from datetime import datetime


class TestManager(EmptyDatabaseMixin, unittest.TestCase):
    def test_number_subdatabases(self):
        storage = LMDBSensorStorage(self.mdb_filename)
        ref_date = datetime.now()
        ref_data = []
        for i in range(4097):
            ref_data.append((ref_date, i))
            storage[f"{i:05}"][ref_date] = i

        data = [sensor.items()[0] for sensor in storage.get_non_empty_sensors()]
        self.assertEqual(ref_data, data)


if __name__ == '__main__':
    unittest.main()
