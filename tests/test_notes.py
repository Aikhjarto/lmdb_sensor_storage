import unittest
from datetime import datetime

from lmdb_sensor_storage.db.sensor_db import Notes
from tests import EmptyDatabaseMixin


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


if __name__ == "__main__":
    unittest.main()
