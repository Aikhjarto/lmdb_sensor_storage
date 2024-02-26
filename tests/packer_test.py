import unittest

from lmdb_sensor_storage._old_api import unpack, pack
from lmdb_sensor_storage._packer import FloatPacker, IntPacker, StringPacker, JSONPacker
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


class TestcasePacker(unittest.TestCase):

    def testFloat(self):

        p = FloatPacker()
        self.assertAlmostEqual(3.0, p.unpack(p.pack(3)))
        self.assertAlmostEqual(3.0, p.unpack(p.pack(3.0)))
        self.assertAlmostEqual(3.0, p.unpack(p.pack("3.0")))
        self.assertAlmostEqual(3.0, p.unpack(p.pack(b"3.0")))

    def testInt(self):
        p = IntPacker()
        self.assertEqual(3, p.unpack(p.pack(3)))
        self.assertEqual(3, p.unpack(p.pack("3")))
        self.assertEqual(3, p.unpack(p.pack(b"3")))

    def testString(self):
        p = StringPacker()

        self.assertEqual("3", p.unpack(p.pack("3")))
        self.assertEqual("3", p.unpack(p.pack(3)))

    def testJson(self):

        p = JSONPacker()
        self.assertEqual([3, ], p.unpack(p.pack([3, ])))
        self.assertEqual([3, 4], p.unpack(p.pack([3, 4])))
        self.assertEqual({"a": 3, "4": 4}, p.unpack(p.pack({"a": 3, "4": 4})))
        with self.assertWarns(RuntimeWarning):
            self.assertEqual({"a": 3, "4": 4}, p.unpack(p.pack({"a": 3, 4: 4})))
        self.assertEqual([1, 2, 3], p.unpack(p.pack('[1,2,3]')))
        self.assertEqual([1, 2, 3], p.unpack(p.pack(' [ 1, 2, 3 ] ')))


if __name__ == "__main__":
    unittest.main()
