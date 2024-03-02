import unittest
from datetime import datetime, timedelta
from lmdb_sensor_storage.sensor_db import TimestampBytesDB, StringYamlDB

from tests import EmptyDatabaseMixin


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


if __name__ == "__main__":
    unittest.main()
