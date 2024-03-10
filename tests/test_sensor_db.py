import unittest
from datetime import datetime, timedelta
from lmdb_sensor_storage.db.sensor_db import TimestampBytesDB
from lmdb_sensor_storage.db.dict_db import StringYamlDB
# noinspection PyProtectedMember
from lmdb_sensor_storage._parser import as_datetime

from tests import EmptyDatabaseMixin


class TestStringYamlDB(EmptyDatabaseMixin, unittest.TestCase):
    def test(self):
        db = StringYamlDB(self.mdb_filename, 'yaml')
        data = {'aB': 3.0,
                3: 'xy'}
        db['metadata'] = data
        self.assertEqual(data, db['metadata'])


class TestcaseTimestampBytesDB(EmptyDatabaseMixin, unittest.TestCase):

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


class TestcaseTimestampBytesDB_get_at_timestamps(EmptyDatabaseMixin, unittest.TestCase):
    
    def setUp(self):
        super().setUp()
        self.db = TimestampBytesDB(self.mdb_filename, 'i')
        self.reference_date = as_datetime("2000-01-01")
        self.db[self.reference_date] = b'1'
        self.db[self.reference_date + timedelta(seconds=5)] = b'2'
        self.db[self.reference_date + timedelta(seconds=10.1)] = b'3'
        self.db[self.reference_date + timedelta(seconds=15)] = b'4'

    def test_get_at_timestamp_on_empty(self):
        db = TimestampBytesDB(self.mdb_filename, 'j')
        data = db.items(at_timestamps=iter([self.reference_date]))
        self.assertEqual([], list(data))

    def test_get_at_timestamp_all_before_first_element(self):
        data = self.db.items(at_timestamps=iter([self.reference_date + timedelta(seconds=i) for i in range(-4, -1)]))
        self.assertEqual([], list(data))

    def test_get_at_timestamps(self):
        # test how at_timestamps return previous sensor value in times between measurements
        
        # full time-range, but additionally at extra points. Points in DB and in at_timestamps (like at seconds 5 and
        # 15) are not reported only once
        data = self.db.items(at_timestamps=iter([self.reference_date + timedelta(seconds=i) for i in range(15)]))
        dataref = [(self.reference_date + timedelta(seconds=0), b'1'),
                   (self.reference_date + timedelta(seconds=1), b'1'),
                   (self.reference_date + timedelta(seconds=2), b'1'),
                   (self.reference_date + timedelta(seconds=3), b'1'),
                   (self.reference_date + timedelta(seconds=4), b'1'),
                   (self.reference_date + timedelta(seconds=5), b'2'),
                   (self.reference_date + timedelta(seconds=6), b'2'),
                   (self.reference_date + timedelta(seconds=7), b'2'),
                   (self.reference_date + timedelta(seconds=8), b'2'),
                   (self.reference_date + timedelta(seconds=9), b'2'),
                   (self.reference_date + timedelta(seconds=10), b'2'),
                   (self.reference_date + timedelta(seconds=10.1), b'3'),
                   (self.reference_date + timedelta(seconds=11), b'3'),
                   (self.reference_date + timedelta(seconds=12), b'3'),
                   (self.reference_date + timedelta(seconds=13), b'3'),
                   (self.reference_date + timedelta(seconds=14), b'3'),
                   (self.reference_date + timedelta(seconds=15), b'4')
                   ]
        self.assertEqual(dataref, data)

    def test_get_at_timestamps_only(self):

        # at_timestamps and at_timestamps_only
        data = self.db.items(at_timestamps=iter([self.reference_date + timedelta(seconds=i) for i in range(15)]),
                             at_timestamps_only=True)
        dataref = [(self.reference_date + timedelta(seconds=0), b'1'),
                   (self.reference_date + timedelta(seconds=1), b'1'),
                   (self.reference_date + timedelta(seconds=2), b'1'),
                   (self.reference_date + timedelta(seconds=3), b'1'),
                   (self.reference_date + timedelta(seconds=4), b'1'),
                   (self.reference_date + timedelta(seconds=5), b'2'),
                   (self.reference_date + timedelta(seconds=6), b'2'),
                   (self.reference_date + timedelta(seconds=7), b'2'),
                   (self.reference_date + timedelta(seconds=8), b'2'),
                   (self.reference_date + timedelta(seconds=9), b'2'),
                   (self.reference_date + timedelta(seconds=10), b'2'),
                   (self.reference_date + timedelta(seconds=11), b'3'),
                   (self.reference_date + timedelta(seconds=12), b'3'),
                   (self.reference_date + timedelta(seconds=13), b'3'),
                   (self.reference_date + timedelta(seconds=14), b'3')
                   ]
        self.assertEqual(dataref, data)

    def test_get_at_timestamps_later(self):
        # same as test_get_at_timestamps, but at_timestamps starts later
        data = self.db.items(at_timestamps=iter([self.reference_date+timedelta(seconds=i) for i in range(3, 15)]))
        dataref = [(self.reference_date + timedelta(seconds=0), b'1'),  # included as not limited with since
                   (self.reference_date + timedelta(seconds=3), b'1'),
                   (self.reference_date + timedelta(seconds=4), b'1'),
                   (self.reference_date + timedelta(seconds=5), b'2'),
                   (self.reference_date + timedelta(seconds=6), b'2'),
                   (self.reference_date + timedelta(seconds=7), b'2'),
                   (self.reference_date + timedelta(seconds=8), b'2'),
                   (self.reference_date + timedelta(seconds=9), b'2'),
                   (self.reference_date + timedelta(seconds=10), b'2'),
                   (self.reference_date + timedelta(seconds=10.1), b'3'),
                   (self.reference_date + timedelta(seconds=11), b'3'),
                   (self.reference_date + timedelta(seconds=12), b'3'),
                   (self.reference_date + timedelta(seconds=13), b'3'),
                   (self.reference_date + timedelta(seconds=14), b'3'),
                   (self.reference_date + timedelta(seconds=15), b'4')
                   ]
        self.assertEqual(dataref, data)

    def test_get_at_timestamps_only_later(self):
        # same as above but first element is excluded by using at_timestamps_only=True
        data = self.db.items(at_timestamps=iter([self.reference_date+timedelta(seconds=i) for i in range(3, 15)]),
                             at_timestamps_only=True)
        dataref = [(self.reference_date + timedelta(seconds=3), b'1'),
                   (self.reference_date + timedelta(seconds=4), b'1'),
                   (self.reference_date + timedelta(seconds=5), b'2'),
                   (self.reference_date + timedelta(seconds=6), b'2'),
                   (self.reference_date + timedelta(seconds=7), b'2'),
                   (self.reference_date + timedelta(seconds=8), b'2'),
                   (self.reference_date + timedelta(seconds=9), b'2'),
                   (self.reference_date + timedelta(seconds=10), b'2'),
                   (self.reference_date + timedelta(seconds=11), b'3'),
                   (self.reference_date + timedelta(seconds=12), b'3'),
                   (self.reference_date + timedelta(seconds=13), b'3'),
                   (self.reference_date + timedelta(seconds=14), b'3'),
                   ]
        self.assertEqual(dataref, data)

    def test_get_at_timestamps_only_since_is_before_first_timestamp(self):
        # same as two above but first element is excluded by using a propper since value
        data = self.db.items(since=self.reference_date + timedelta(seconds=1.1),
                             at_timestamps=iter([self.reference_date+timedelta(seconds=i) for i in range(3, 15)]),
                             at_timestamps_only=True)
        dataref = [(self.reference_date + timedelta(seconds=3), b'1'),
                   (self.reference_date + timedelta(seconds=4), b'1'),
                   (self.reference_date + timedelta(seconds=5), b'2'),
                   (self.reference_date + timedelta(seconds=6), b'2'),
                   (self.reference_date + timedelta(seconds=7), b'2'),
                   (self.reference_date + timedelta(seconds=8), b'2'),
                   (self.reference_date + timedelta(seconds=9), b'2'),
                   (self.reference_date + timedelta(seconds=10), b'2'),
                   (self.reference_date + timedelta(seconds=11), b'3'),
                   (self.reference_date + timedelta(seconds=12), b'3'),
                   (self.reference_date + timedelta(seconds=13), b'3'),
                   (self.reference_date + timedelta(seconds=14), b'3')
                   ]
        self.assertEqual(dataref, data)

    def test_get_at_timestamps_only_since_is_after_first_timestamp(self):
        data = self.db.items(since=self.reference_date + timedelta(seconds=4),
                             at_timestamps=iter([self.reference_date + timedelta(seconds=i) for i in range(3, 15)]),
                             at_timestamps_only=True)
        dataref = [(self.reference_date + timedelta(seconds=4), b'1'),
                   (self.reference_date + timedelta(seconds=5), b'2'),
                   (self.reference_date + timedelta(seconds=6), b'2'),
                   (self.reference_date + timedelta(seconds=7), b'2'),
                   (self.reference_date + timedelta(seconds=8), b'2'),
                   (self.reference_date + timedelta(seconds=9), b'2'),
                   (self.reference_date + timedelta(seconds=10), b'2'),
                   (self.reference_date + timedelta(seconds=11), b'3'),
                   (self.reference_date + timedelta(seconds=12), b'3'),
                   (self.reference_date + timedelta(seconds=13), b'3'),
                   (self.reference_date + timedelta(seconds=14), b'3')
                   ]
        self.assertEqual(dataref, data)

    def test_get_at_timestamps_since_is_before_first_timestamp(self):
        # at_timestamp is limited further by since, at_timestamps querying before since does not produce anything
        data = self.db.items(since=self.reference_date + timedelta(seconds=1.1),
                             at_timestamps=iter([self.reference_date+timedelta(seconds=i) for i in range(8, 15)]))
        dataref = [(self.reference_date + timedelta(seconds=5), b'2'),
                   (self.reference_date + timedelta(seconds=8), b'2'),
                   (self.reference_date + timedelta(seconds=9), b'2'),
                   (self.reference_date + timedelta(seconds=10), b'2'),
                   (self.reference_date + timedelta(seconds=10.1), b'3'),
                   (self.reference_date + timedelta(seconds=11), b'3'),
                   (self.reference_date + timedelta(seconds=12), b'3'),
                   (self.reference_date + timedelta(seconds=13), b'3'),
                   (self.reference_date + timedelta(seconds=14), b'3'),
                   (self.reference_date + timedelta(seconds=15), b'4')
                   ]
        self.assertEqual(dataref, data)

    def test_get_at_timestamps_since_is_after_first_timestamp(self):
        # at_timestamp is limited further by since, at_timestamps querying before since does not produce anything
        data = self.db.items(since=self.reference_date + timedelta(seconds=5),
                             at_timestamps=iter([self.reference_date+timedelta(seconds=i) for i in range(15)]))
        dataref = [(self.reference_date + timedelta(seconds=5), b'2'),
                   (self.reference_date + timedelta(seconds=6), b'2'),
                   (self.reference_date + timedelta(seconds=7), b'2'),
                   (self.reference_date + timedelta(seconds=8), b'2'),
                   (self.reference_date + timedelta(seconds=9), b'2'),
                   (self.reference_date + timedelta(seconds=10), b'2'),
                   (self.reference_date + timedelta(seconds=10.1), b'3'),
                   (self.reference_date + timedelta(seconds=11), b'3'),
                   (self.reference_date + timedelta(seconds=12), b'3'),
                   (self.reference_date + timedelta(seconds=13), b'3'),
                   (self.reference_date + timedelta(seconds=14), b'3'),
                   (self.reference_date + timedelta(seconds=15), b'4')
                   ]
        self.assertEqual(dataref, data)

    def test_get_at_timestamps_until_before_last_timestamp(self):
        # at_timestamp is limited by until
        data = self.db.items(until=self.reference_date + timedelta(seconds=11),
                             at_timestamps=iter([self.reference_date+timedelta(seconds=i) for i in range(15)]))
        dataref = [(self.reference_date + timedelta(seconds=0), b'1'),
                   (self.reference_date + timedelta(seconds=1), b'1'),
                   (self.reference_date + timedelta(seconds=2), b'1'),
                   (self.reference_date + timedelta(seconds=3), b'1'),
                   (self.reference_date + timedelta(seconds=4), b'1'),
                   (self.reference_date + timedelta(seconds=5), b'2'),
                   (self.reference_date + timedelta(seconds=6), b'2'),
                   (self.reference_date + timedelta(seconds=7), b'2'),
                   (self.reference_date + timedelta(seconds=8), b'2'),
                   (self.reference_date + timedelta(seconds=9), b'2'),
                   (self.reference_date + timedelta(seconds=10), b'2'),
                   (self.reference_date + timedelta(seconds=10.1), b'3'),
                   ]
        self.assertEqual(dataref, data)

    def test_get_at_timestamps_until_with_endpoint_before_last_timestamp(self):
        # at_timestamp is limited by until
        data = self.db.items(until=self.reference_date + timedelta(seconds=11), endpoint=True,
                             at_timestamps=iter([self.reference_date+timedelta(seconds=i) for i in range(15)]))
        dataref = [(self.reference_date + timedelta(seconds=0), b'1'),
                   (self.reference_date + timedelta(seconds=1), b'1'),
                   (self.reference_date + timedelta(seconds=2), b'1'),
                   (self.reference_date + timedelta(seconds=3), b'1'),
                   (self.reference_date + timedelta(seconds=4), b'1'),
                   (self.reference_date + timedelta(seconds=5), b'2'),
                   (self.reference_date + timedelta(seconds=6), b'2'),
                   (self.reference_date + timedelta(seconds=7), b'2'),
                   (self.reference_date + timedelta(seconds=8), b'2'),
                   (self.reference_date + timedelta(seconds=9), b'2'),
                   (self.reference_date + timedelta(seconds=10), b'2'),
                   (self.reference_date + timedelta(seconds=10.1), b'3'),
                   (self.reference_date + timedelta(seconds=11), b'3'),
                   ]
        self.assertEqual(dataref, data)

    def test_get_at_timestamps_only_since_after_first_timestamp(self):
        # at_timestamps and at_timestamps_only limited by since
        data = self.db.items(since=self.reference_date + timedelta(seconds=3),
                             at_timestamps=iter([self.reference_date + timedelta(seconds=i) for i in range(15)]),
                             at_timestamps_only=True)
        dataref = [(self.reference_date + timedelta(seconds=3), b'1'),
                   (self.reference_date + timedelta(seconds=4), b'1'),
                   (self.reference_date + timedelta(seconds=5), b'2'),
                   (self.reference_date + timedelta(seconds=6), b'2'),
                   (self.reference_date + timedelta(seconds=7), b'2'),
                   (self.reference_date + timedelta(seconds=8), b'2'),
                   (self.reference_date + timedelta(seconds=9), b'2'),
                   (self.reference_date + timedelta(seconds=10), b'2'),
                   (self.reference_date + timedelta(seconds=11), b'3'),
                   (self.reference_date + timedelta(seconds=12), b'3'),
                   (self.reference_date + timedelta(seconds=13), b'3'),
                   (self.reference_date + timedelta(seconds=14), b'3')
                   ]
        self.assertEqual(dataref, data)

    def test_get_at_timestamps_only_until_with_endpoint_before_last_timestamp(self):
        # at_timestamps and at_timestamps_only limited by until and endpoint
        data = self.db.items(until=self.reference_date + timedelta(seconds=11), endpoint=True,
                             at_timestamps=iter([self.reference_date + timedelta(seconds=i) for i in range(15)]),
                             at_timestamps_only=True)
        dataref = [(self.reference_date + timedelta(seconds=0), b'1'),
                   (self.reference_date + timedelta(seconds=1), b'1'),
                   (self.reference_date + timedelta(seconds=2), b'1'),
                   (self.reference_date + timedelta(seconds=3), b'1'),
                   (self.reference_date + timedelta(seconds=4), b'1'),
                   (self.reference_date + timedelta(seconds=5), b'2'),
                   (self.reference_date + timedelta(seconds=6), b'2'),
                   (self.reference_date + timedelta(seconds=7), b'2'),
                   (self.reference_date + timedelta(seconds=8), b'2'),
                   (self.reference_date + timedelta(seconds=9), b'2'),
                   (self.reference_date + timedelta(seconds=10), b'2'),
                   (self.reference_date + timedelta(seconds=11), b'3')
                   ]
        self.assertEqual(dataref, data)

    def test_get_at_timestamps_until_with_endpoint_after_last_timestamp(self):
        # at_timestamps and at_timestamps_only limited by until and endpoint
        data = self.db.items(until=self.reference_date + timedelta(seconds=15), endpoint=True,
                             at_timestamps=iter([self.reference_date + timedelta(seconds=i) for i in range(11)]),)
        dataref = [(self.reference_date + timedelta(seconds=0), b'1'),
                   (self.reference_date + timedelta(seconds=1), b'1'),
                   (self.reference_date + timedelta(seconds=2), b'1'),
                   (self.reference_date + timedelta(seconds=3), b'1'),
                   (self.reference_date + timedelta(seconds=4), b'1'),
                   (self.reference_date + timedelta(seconds=5), b'2'),
                   (self.reference_date + timedelta(seconds=6), b'2'),
                   (self.reference_date + timedelta(seconds=7), b'2'),
                   (self.reference_date + timedelta(seconds=8), b'2'),
                   (self.reference_date + timedelta(seconds=9), b'2'),
                   (self.reference_date + timedelta(seconds=10), b'2'),
                   (self.reference_date + timedelta(seconds=10.1), b'3'),
                   (self.reference_date + timedelta(seconds=15), b'4')
                   ]
        self.assertEqual(dataref, data)

    def test_get_at_timestamps_only_until_with_endpoint_afterlast_timestamp(self):
        # at_timestamps and at_timestamps_only limited by until and endpoint
        data = self.db.items(until=self.reference_date + timedelta(seconds=15), endpoint=True,
                             at_timestamps=iter([self.reference_date + timedelta(seconds=i) for i in range(11)]),
                             at_timestamps_only=True)
        dataref = [(self.reference_date + timedelta(seconds=0), b'1'),
                   (self.reference_date + timedelta(seconds=1), b'1'),
                   (self.reference_date + timedelta(seconds=2), b'1'),
                   (self.reference_date + timedelta(seconds=3), b'1'),
                   (self.reference_date + timedelta(seconds=4), b'1'),
                   (self.reference_date + timedelta(seconds=5), b'2'),
                   (self.reference_date + timedelta(seconds=6), b'2'),
                   (self.reference_date + timedelta(seconds=7), b'2'),
                   (self.reference_date + timedelta(seconds=8), b'2'),
                   (self.reference_date + timedelta(seconds=9), b'2'),
                   (self.reference_date + timedelta(seconds=10), b'2')
                   ]
        self.assertEqual(dataref, data)


if __name__ == "__main__":
    unittest.main()
