import math

from lmdb_sensor_storage._parser import as_datetime
import unittest
from datetime import datetime

from lmdb_sensor_storage.db.sensor_db import guess_format_string


class TestDatetime(unittest.TestCase):
    def test_asdatetime_isoformat(self):
        self.assertEqual(datetime(year=2023, month=1, day=2),
                         as_datetime('2023-01-02'))

        self.assertEqual(datetime(year=2023, month=1, day=2, hour=6, minute=5),
                         as_datetime('2023-01-02T06:05'))

        self.assertEqual(datetime(year=2023, month=1, day=2, hour=6, minute=5, second=54),
                         as_datetime('2023-01-02T06:05:54'))

        self.assertEqual(datetime(year=2023, month=1, day=2, hour=6, minute=5, second=54, microsecond=1234),
                         as_datetime('2023-01-02T06:05:54.001234'))

    def test_asdatetime_none(self):
        self.assertEqual(None, as_datetime(None, none_ok=True))

        with self.assertRaises(ValueError):
            as_datetime(None, none_ok=False)

    def test_asdatetime_timestamp_seconds(self):
        self.assertEqual(datetime(year=2023, month=1, day=2, hour=6, minute=5, second=54),
                         as_datetime(1672635954))

        self.assertEqual(datetime(year=2023, month=1, day=2, hour=6, minute=5, second=54),
                         as_datetime(1672635954.0))

        self.assertEqual(datetime(year=2023, month=1, day=2, hour=6, minute=5, second=54),
                         as_datetime("1672635954s"))

        self.assertEqual(datetime(year=2023, month=1, day=2, hour=6, minute=5, second=54),
                         as_datetime("1672635954.0s"))

    def test_asdatetime_timestamp_milliseconds(self):
        self.assertEqual(datetime(year=2023, month=1, day=2, hour=6, minute=5, second=54),
                         as_datetime("1672635954000ms"))

        self.assertEqual(datetime(year=2023, month=1, day=2, hour=6, minute=5, second=54),
                         as_datetime("1672635954000.0ms"))

    def test_asdatetime_timestamp_microseconds(self):
        self.assertEqual(datetime(year=2023, month=1, day=2, hour=6, minute=5, second=54),
                         as_datetime("1672635954000000us"))

        self.assertEqual(datetime(year=2023, month=1, day=2, hour=6, minute=5, second=54),
                         as_datetime("1672635954000000.0us"))

    def test_asdatetime_timestamp_nanoseconds(self):
        self.assertEqual(datetime(year=2023, month=1, day=2, hour=6, minute=5, second=54),
                         as_datetime("1672635954000000000ns"))

        self.assertEqual(datetime(year=2023, month=1, day=2, hour=6, minute=5, second=54),
                         as_datetime("1672635954000000000.0ns"))

    def test_wrong_suffix(self):
        with self.assertRaises(ValueError):
            as_datetime("1xs")

    # noinspection PyTypeChecker
    def test_wrong_type(self):
        with self.assertRaises(ValueError):
            as_datetime("1xs".encode())
            as_datetime("1672635954s".encode())


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
        self.assertEqual(guess_format_string([3.0, 3]), '2f')
        self.assertEqual(guess_format_string((3.0, 'xc')), 'json')
