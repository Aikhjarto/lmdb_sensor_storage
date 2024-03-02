import os.path
import unittest
from lmdb_sensor_storage.db import *
from lmdb_sensor_storage.sensor_db import *
import tempfile
import shutil


# noinspection PyPep8Naming
class EmptyDatabaseMixin:
    # noinspection PyAttributeOutsideInit
    def setUp(self):
        # logger = logging.getLogger('lmdb_sensor_storage.db')
        # logger.setLevel(logging.DEBUG)

        self.tempfolder = tempfile.mkdtemp()
        self.mdb_filename = os.path.join(self.tempfolder, 'unittest.mdb')

    def tearDown(self) -> None:
        manager.close_all()
        shutil.rmtree(self.tempfolder)


class TestcaseChunker(unittest.TestCase):

    def test_minmeamax(self):
        val = (1, 2, 3, 4)
        self.assertEqual((1, 2.5, 4), value_chunker_minmeanmax(val))
        self.assertEqual([2.5, ], value_chunker_mean(val))

        val = ((1, 2, 3, 4),
               (2, 3, 4, 5))
        self.assertEqual(([1, 2, 3, 4], [1.5, 2.5, 3.5, 4.5], [2, 3, 4, 5]), value_chunker_minmeanmax(val))
        self.assertEqual([[1.5, 2.5, 3.5, 4.5, ]], value_chunker_mean(val))


class TestcaseNotes(EmptyDatabaseMixin, unittest.TestCase):
    def testNotes(self):
        n = Notes(self.mdb_filename)
        # empty
        self.assertEqual(len(n), 0)

        n[datetime.now()] = 'test'

        stamp = datetime.now()
        n.add_note('short', 'long', stamp)
        note = n[stamp]
        self.assertEqual(note['short'], 'short')
        self.assertEqual(note['long'], 'long')


class TestcaseLMDBDict(EmptyDatabaseMixin, unittest.TestCase):

    def test_eq(self):
        db1 = LMDBDict(self.mdb_filename, 'db1')
        db2 = LMDBDict(self.mdb_filename, 'bd2')
        self.assertTrue(db1 == db2)

    def test_copy(self):
        db = LMDBDict(self.mdb_filename, 'source')
        db[b'x'] = b'3.0'

        # copy to new file
        db.copy_to('dest', self.mdb_filename + '2')
        db2 = LMDBDict(self.mdb_filename + '2', 'dest')
        self.assertEqual(db.items(), db2.items())

        # copy to same file
        db.copy_to('dest', self.mdb_filename)
        db2 = LMDBDict(self.mdb_filename, 'dest')
        self.assertEqual(db.items(), db2.items())


class TestStringYamlDB(EmptyDatabaseMixin, unittest.TestCase):
    def test(self):
        db = StringYamlDB(self.mdb_filename, 'yaml')
        data = {'aB': 3.0,
                3: 'xy'}
        db['metadata'] = data
        self.assertEqual(data, db['metadata'])


class TestcaseTimestampDB(EmptyDatabaseMixin, unittest.TestCase):

    def test_only_if_changed(self):
        db = TimestampBytesDB(self.mdb_filename, 'g')
        val = b'1'
        date = datetime.now()
        db.write_value(date, val)
        self.assertEqual(len(db), 1)

        # skip adding entry if vallue is not changed
        dt = timedelta(seconds=60)
        db.write_value(date+dt, val, only_if_value_changed=True)
        self.assertEqual(len(db), 1)

        # add entry which regardless of valuechange
        db.write_value(date+dt, val, only_if_value_changed=False)
        self.assertEqual(len(db), 2)

        # add entry which has no value change but is delayed by max_age_seconds
        db.write_value(date+timedelta(days=1), val, only_if_value_changed=True, max_age_seconds=3600)
        self.assertEqual(len(db), 3)

    def test_TimeStampBytesDB(self):
        db = TimestampBytesDB(self.mdb_filename, 'g')
        db._get_lmdb_stats()

        # test on empty db
        self.assertFalse(datetime.now() in db)
        self.assertEqual(len(db), 0)
        db.__repr__()
        db.keys(since=datetime.now(), until=datetime.now(), endpoint=True)
        db.keys(since=datetime.now(), until=datetime.now(), endpoint=False)

        key = datetime.now()

        # test for setting first value
        val = b'0'
        db[key] = val
        self.assertEqual(db[key], val)
        self.assertEqual(len(db), 1)
        db.__repr__()

        self.assertEqual([key, ], db.keys(since=key, until=key, endpoint=True))
        self.assertEqual([], db.keys(since=key, until=key, endpoint=False))

        tmp = list(db.items())
        self.assertEqual(tmp[0][0], key)
        self.assertEqual(tmp[0][1], val)

        # test overwrite
        val = b'0'
        db[key] = val
        self.assertEqual(db[key], val)
        self.assertEqual(len(db), 1)
        self.assertEqual(db.items(), [(key, val), ])

        # test two value
        val2 = b'1'
        key2 = datetime.now()
        db[key2] = val2
        self.assertEqual(len(db), 2)
        self.assertEqual(db[key], val)
        self.assertEqual(db[key2], val2)
        self.assertEqual(db.values(), [val, val2])
        self.assertEqual(db.keys(), [key, key2])
        self.assertEqual(db.items(), [(key, val), (key2, val2)])

        self.assertEqual([key, key2], db.keys(since=key, until=key2, endpoint=True))
        self.assertEqual([key, ], db.keys(since=key, until=key2, endpoint=False))

        db.__repr__()

        # only if value changed
        key3 = datetime.now()
        db.write_value(key3, val2, only_if_value_changed=True)
        self.assertEqual(len(db), 2)

        key_intermediate = key + (key2 - key) / 2
        self.assertFalse(key_intermediate in db)
        db.write_value(key_intermediate, val, only_if_value_changed=True)
        self.assertEqual(len(db), 2)

        key_before = key - (key2 - key) / 2
        self.assertFalse(key_before in db)
        db.write_value(key_intermediate, val, only_if_value_changed=True)
        self.assertEqual(len(db), 2)

        del db[key]
        self.assertEqual(db.items(), [(key2, val2)])
        self.assertEqual(len(db), 1)

    def test_last_changed(self):
        db = TimestampBytesDB(self.mdb_filename, 'h')
        db._get_lmdb_stats()

        self.assertEqual(None, db.get_last_changed())

        date = datetime.now()
        data = b'1'
        db.write_value(date, data)
        self.assertEqual(None, db.get_last_changed())

        db.write_value(date+timedelta(seconds=1), data)
        self.assertEqual(None, db.get_last_changed())

        new_date = date+timedelta(seconds=10)
        new_data = b'0'
        db.write_value(new_date, new_data)
        self.assertEqual(new_date, db.get_last_changed())

        db.write_value(new_date+timedelta(seconds=1), new_data)
        self.assertEqual(new_date, db.get_last_changed())


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

    def test_get_on_empty_dB(self):
        m = LMDBSensorStorage(self.mdb_filename)
        self.assertIsInstance(m.statistics, dict)
        self.assertIsNotNone(m._environment)
        self.assertEqual(m.keys(), [])
        self.assertEqual(m.get_non_empty_sensor_names(), [])
        self.assertEqual(m.get_non_empty_sensors(), [])
        m.close()
        self.assertEqual(manager.handles, {})

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


class TestcaseLMDB(EmptyDatabaseMixin, unittest.TestCase):

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


if __name__ == "__main__":
    unittest.main()
