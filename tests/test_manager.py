import unittest
from unittest import mock
from . import EmptyDatabaseMixin
from lmdb_sensor_storage import LMDBSensorStorage
# noinspection PyProtectedMember
from lmdb_sensor_storage.db._manager import manager
from datetime import datetime, timezone


class TestManager(EmptyDatabaseMixin, unittest.TestCase):
    def test_number_subdatabases(self):
        # Exceed the maximum number of sub-databases several times. Small limits make this fast, since the cost of
        # transactions in lmdb grows with the limit.
        storage = LMDBSensorStorage(self.mdb_filename)
        ref_date = datetime.now(timezone.utc)
        ref_data = []
        with mock.patch.object(manager, 'DEFAULT_MAXDBS', 8), mock.patch.object(manager, 'MAX_DBS_STEP', 8), \
                self.assertLogs('lmdb_sensor_storage.manager', level='WARNING') as logs:
            # each sensor has 4 sub-databases
            for i in range(10):
                ref_data.append((ref_date, i))
                storage[f"{i:05}"][ref_date] = i

            data = [sensor.items()[0] for sensor in storage.get_non_empty_sensors()]
        self.assertEqual(ref_data, data)
        self.assertGreaterEqual(len([line for line in logs.output if 'Reopening' in line]), 2)


if __name__ == '__main__':
    unittest.main()
