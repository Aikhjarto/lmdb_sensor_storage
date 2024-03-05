import os
import unittest
from datetime import datetime
import numpy as np

from lmdb_sensor_storage import Sensor
# noinspection PyProtectedMember
from lmdb_sensor_storage.db._manager import manager
from lmdb_sensor_storage.db.chunker import timestamp_chunker_center, value_chunker_mean, value_chunker_minmeanmax, \
    timestamp_chunker_minmeanmax
from tests import EmptyDatabaseMixin


class TestcaseSensor(EmptyDatabaseMixin, unittest.TestCase):

    def test_guessing(self):
        s = Sensor(self.mdb_filename, 'k')
        date = datetime.now()
        data = 0.0
        self.assertEqual(None, s.data_format)
        s.write_value(date, data)
        self.assertEqual('f', s.data_format)
        self.assertEqual([0.0, ], s.values())

    def test_packed_write_read(self):
        s = Sensor(self.mdb_filename, 'a', data_format='fhf')
        data = (1.0, 4, -3)
        date = datetime.now()
        s.write_value(date, data)
        self.assertEqual(s.values(), [data, ])
        self.assertEqual(s.keys(), [date, ])
        self.assertEqual(s.items(), [(date, data), ])

        # close and reopen to see if pack_str is correctly read from file
        s.close_file()
        s = Sensor(self.mdb_filename, 'a')
        self.assertEqual(s.values(), [data, ])
        self.assertEqual(s.keys(), [date, ])
        self.assertEqual(s.items(), [(date, data), ])

    def test_write_read_float(self):
        s = Sensor(self.mdb_filename, 'a')
        data = 1.0
        date = datetime.now()
        self.assertTrue(s.write_value(date, data))
        self.assertEqual([date, ], s.keys())
        self.assertEqual([data, ], s.values())
        self.assertEqual(s.statistics['entries'], 1)

        date = date.now()
        self.assertTrue(s.write_value(date, 2))
        self.assertEqual(s.statistics['entries'], 2)

        date = date.now()
        self.assertTrue(s.write_value(date, 3))
        self.assertEqual(s.statistics['entries'], 3)

        # test for only_if_changed
        date = datetime.now()
        self.assertFalse(s.write_value(date, 3, only_if_value_changed=True))
        self.assertEqual(s.statistics['entries'], 3)

    def test_write_read_float_decimate(self):
        s = Sensor(self.mdb_filename, 'b')

        dates = [datetime.fromtimestamp(i) for i in range(100)]
        values = list(range(100))
        s.write_values(dates, values)
        self.assertEqual(s.items(), list(zip(dates, values)))

        self.assertEqual(
            s.items(decimate_to_s=7, timestamp_chunker=timestamp_chunker_center, value_chunker=value_chunker_mean),
            [(dates[i] + (dates[i + 6] - dates[i]) / 2, np.mean(values[i:i + 7])) for i in range(0, 100 - 7, 7)])

        self.assertEqual(s.items(limit=7), list(zip(dates[:8], values[:8])))
        d = []
        v = []
        for i in range(0, 100 - 7, 7):
            d.extend([dates[i], dates[i] + (dates[i + 6] - dates[i]) / 2, dates[i + 6]])
            v.extend([np.min(values[i:i + 7]), np.mean(values[i:i + 7]), np.max(values[i:i + 7])])

        self.assertEqual(
            s.items(decimate_to_s=7, timestamp_chunker=timestamp_chunker_minmeanmax,
                    value_chunker=value_chunker_minmeanmax),
            list(zip(d, v)))

    def test_write_read_samples_float(self):
        s = Sensor(self.mdb_filename, 'c')
        dates = [datetime.fromtimestamp(1),
                 datetime.fromtimestamp(2),
                 datetime.fromtimestamp(3),
                 datetime.fromtimestamp(4)]
        values = [18, 17, 16, 15]

        s.write_values(dates, values)
        self.assertEqual(s.items(), list(zip(dates, values)))

    def test_write_read_samples_packed(self):
        s = Sensor(self.mdb_filename, 'e', data_format='fh')
        dates = [datetime.fromtimestamp(1),
                 datetime.fromtimestamp(2),
                 datetime.fromtimestamp(3),
                 datetime.fromtimestamp(4)]
        values = [(18., 18), (17., 17), (16., 16), (15.0, 15)]

        s.write_values(dates, values)
        self.assertEqual(s.items(), list(zip(dates, values)))

    def test_empty_sensor(self):
        s = Sensor(self.mdb_filename, 'f')
        self.assertEqual(0, len(s.values()))
        self.assertEqual(0, len(s.keys()))
        self.assertEqual(0, len(s.items()))
        self.assertEqual(s.statistics, {'entries': 0})

        self.assertIsNone(s.get_first_timestamp())
        self.assertIsNone(s.get_last_timestamp())
        s.close_file()
        self.assertEqual(manager.handles, {})

        self.assertEqual([], s.items(decimate_to_s=80, since=datetime.now()))
        self.assertEqual([], s.items(limit=3))

    def test_copy_to(self):
        s = Sensor(self.mdb_filename, 'j1', data_format='fh')
        dates = [datetime.fromtimestamp(1),
                 datetime.fromtimestamp(2),
                 datetime.fromtimestamp(3),
                 datetime.fromtimestamp(4)]
        values = [(18., 18), (17., 17), (16., 16), (15.0, 15)]

        s.write_values(dates, values)
        self.assertEqual(s.items(), list(zip(dates, values)))

        # copy Sensor to new name in new file
        s.copy_to('j2')
        s2 = Sensor(self.mdb_filename, 'j2')
        self.assertEqual(s.metadata, s2.metadata)
        self.assertEqual(s.data_format, s2.data_format)
        self.assertEqual(s.items(), s2.items())

        # error is raised if copied to already existing location is same file
        with self.assertRaises(RuntimeError):
            s.copy_to('j2')

        # test copy sensor to new location in new file
        new_mdb_filename = os.path.join(self.tempfolder, 'unittest2.mdb')
        s.copy_to('j', new_mdb_filename)
        s2 = Sensor(new_mdb_filename, 'j')
        self.assertEqual(s.metadata, s2.metadata)
        self.assertEqual(s.data_format, s2.data_format)
        self.assertEqual(s.items(), s2.items())

        # error is raised if copied to already existing location is new file
        with self.assertRaises(RuntimeError):
            s.copy_to('j', new_mdb_filename)


if __name__ == "__main__":
    unittest.main()
