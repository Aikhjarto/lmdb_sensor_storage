import math

from lmdb_sensor_storage._parser import as_datetime
import unittest
from datetime import datetime

from lmdb_sensor_storage.sensor_db import guess_format_string


class TestDatetime(unittest.TestCase):
    def test1(self):
        self.assertEqual(datetime(year=2023, month=1, day=2),
                         as_datetime('2023-01-02'))

        self.assertEqual(None, as_datetime(None, none_ok=True))

        with self.assertRaises(ValueError):
            as_datetime(None, none_ok=False)

        self.assertEqual(datetime(year=2023, month=1, day=2),
                         datetime(year=2023, month=1, day=2))


if __name__ == "__main__":
    unittest.main()


class TestcaseGuesstype(unittest.TestCase):
    def test_single_float(self):
        self.assertEqual(guess_format_string('3.0'), 'f')
        self.assertEqual(guess_format_string(b'3.0'), 'f')
        self.assertEqual(guess_format_string(b'3'), 'f')
        self.assertEqual(guess_format_string('3'), 'f')
        self.assertEqual(guess_format_string(3), 'f')
        self.assertEqual(guess_format_string(3.0), 'f')
        self.assertEqual(guess_format_string('-1e-3'), 'f')
        self.assertEqual(guess_format_string(math.nan), 'f')

        self.assertEqual(guess_format_string(0), 'f')

        self.assertEqual(guess_format_string(b'xyz'), 'bytes')
        self.assertEqual(guess_format_string('xyz'), 'str')
        self.assertEqual(guess_format_string('B'), 'str')

    def test_lists(self):
        self.assertEqual(guess_format_string([3.0,3]), '2f')
        self.assertEqual(guess_format_string((3.0, 'xc')), 'json')
