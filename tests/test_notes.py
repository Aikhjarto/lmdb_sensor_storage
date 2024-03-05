import unittest
from datetime import datetime, timedelta

from lmdb_sensor_storage.db.sensor_db import Notes
from tests import EmptyDatabaseMixin


class TestcaseNotes(EmptyDatabaseMixin, unittest.TestCase):
    def testNotes(self):
        n = Notes(self.mdb_filename)
        # empty
        self.assertEqual(len(n), 0)
        reference_date = datetime.now()

        n[reference_date+timedelta(seconds=1)] = 'test'

        n[reference_date+timedelta(seconds=2)] = {'short': 'test1'}

        with self.assertRaises(TypeError):
            n[reference_date+timedelta(seconds=3)] = 3

        with self.assertRaises(AssertionError):
            n[reference_date+timedelta(seconds=4)] = {}

        n.add_note('short', 'long', reference_date+timedelta(seconds=4))

        note = n[reference_date+timedelta(seconds=1)]
        self.assertEqual(note['short'], 'test')

        note = n[reference_date + timedelta(seconds=2)]
        self.assertEqual(note['short'], 'test1')

        note = n[reference_date+timedelta(seconds=4)]
        self.assertEqual(note['short'], 'short')
        self.assertEqual(note['long'], 'long')


if __name__ == "__main__":
    unittest.main()
