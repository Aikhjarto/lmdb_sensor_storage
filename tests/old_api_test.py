import unittest
from lmdb_sensor_storage._old_api import unpack, pack
from lmdb_sensor_storage.sensor_db import guess_format_string


class TestcasePackUnpack(unittest.TestCase):
    def test_pack_float(self):
        d = 3
        fmt = guess_format_string(d)
        self.assertAlmostEqual(unpack(pack(d, fmt), fmt)[0], d)

    def test_pack_iterable_of_float(self):
        d = (3, 4, 3.2, 7, 1)
        fmt = guess_format_string(d)
        e = unpack(pack(d, fmt), fmt)
        [self.assertAlmostEqual(x, y) for x, y in zip(d, e)]
