import unittest

from lmdb_sensor_storage.db.dict_db import LMDBDict
from tests import EmptyDatabaseMixin


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

    def test_update(self):
        db = LMDBDict(self.mdb_filename, 'source')
        db[b'x'] = b'3.0'
        db[b'y'] = b'y'

        # test update to empty DB
        db2 = LMDBDict(self.mdb_filename, 'dest')
        db2.update(db)
        self.assertEqual(len(db), len(db2))
        for key in db.keys():
            self.assertEqual(db[key], db2[key])

        # test update to non-empty db (existing keys may be overwritten)
        db2[b'x'] = b'4.0'
        db2[b'z'] = b'xzy'
        db2.update(db)
        self.assertEqual(len(db), len(db2)-1)
        for key in db.keys():
            self.assertEqual(db[key], db2[key])
        self.assertEqual(b'xzy', db2[b'z'])

    def test_clear(self):
        db = LMDBDict(self.mdb_filename, 'source')
        db[b'x'] = b'3.0'
        db[b'y'] = b'y'

        db.clear()
        self.assertEqual(0, len(db))
        self.assertEqual(0, len(db.keys()))

    def test_pop(self):
        db = LMDBDict(self.mdb_filename, 'source')
        db[b'x'] = b'3.0'
        db[b'y'] = b'y'

        val = db.pop(b'x')
        self.assertEqual(b'3.0', val)
        self.assertEqual(1, len(db))
        self.assertEqual(1, len(db.keys()))
        self.assertEqual(b'y', db[b'y'])

        # pop non-exiting item
        with self.assertRaises(KeyError):
            db.pop(b'z')

    def test_popitem(self):
        db = LMDBDict(self.mdb_filename, 'source')
        db[b'x'] = b'3.0'
        db[b'y'] = b'z'

        (key, val) = db.popitem()
        self.assertEqual(b'y', key)
        self.assertEqual(b'z', val)
        self.assertEqual(1, len(db))
        self.assertEqual(1, len(db.keys()))
        self.assertEqual(b'3.0', db[b'x'])

    def test_setdefault(self):
        db = LMDBDict(self.mdb_filename, 'source')
        db.setdefault(b'x', b'3.0')
        self.assertEqual(1, len(db))
        self.assertEqual(b'3.0', db[b'x'])

        db.setdefault(b'x', b'4.0')
        self.assertEqual(1, len(db))
        self.assertEqual(b'3.0', db[b'x'])

    def test_eq_neq(self):
        db = LMDBDict(self.mdb_filename, 'source')
        db[b'x'] = b'3.0'
        db[b'y'] = b'z'

        db2 = LMDBDict(self.mdb_filename, 'source2')
        db2[b'x'] = b'3.0'
        db2[b'y'] = b'z'

        self.assertEqual(db, db2)

        db2[b'x'] = b'4'
        self.assertNotEqual(db, db2)

        db2[b'x'] = b'3.0'
        self.assertEqual(db, db2)

        db2[b'z'] = b'x'
        self.assertNotEqual(db, db2)

    def test_repr(self):
        db = LMDBDict(self.mdb_filename, 'source')
        print(db.__repr__())
        print(db.__str__())

        db[b'x'] = b'3.0'
        db[b'y'] = b'z'

        print(db.__repr__())
        print(db.__str__())

    def test_double_open(self):
        db = LMDBDict(self.mdb_filename, 'source')
        db2 = LMDBDict(self.mdb_filename, 'source')

        # no paralell transactions
        db[b'x'] = b'3'
        db[b'y'] = b'4'
        self.assertEqual(db[b'x'], db2[b'x'])

        # access same db while another transaction is open
        for key_db, val_db in db._get(what='items'):
            print('DB:', key_db, val_db)
            for key_db2, val_db2 in db2._get(what='items'):
                print('DB2:', key_db2, val_db2)


if __name__ == "__main__":
    unittest.main()
