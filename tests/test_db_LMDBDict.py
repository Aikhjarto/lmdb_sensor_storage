import unittest

from lmdb_sensor_storage.db import LMDBDict
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


if __name__ == "__main__":
    unittest.main()
