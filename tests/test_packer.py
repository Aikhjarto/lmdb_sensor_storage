import random
import struct
import unittest
from datetime import datetime, timedelta, timezone
from lmdb_sensor_storage.db.packer import FloatPacker, IntPacker, StringPacker, JSONPacker, StructPacker, \
    DatetimePacker


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

    def testStruct(self):
        p = StructPacker('HH')
        self.assertEqual((3, 4), p.unpack(p.pack([3, 4])))

    def testDatetime(self):
        p = DatetimePacker()
        d = datetime(2024, 1, 2, 3, 4, 5, 678901, tzinfo=timezone.utc)
        self.assertEqual(d, p.unpack(p.pack(d)))
        self.assertEqual(timezone.utc, p.unpack(p.pack(d)).tzinfo)

        # same instant in other timezone, or as naive local time
        self.assertEqual(p.pack(d), p.pack(d.astimezone(timezone(timedelta(hours=-5)))))
        self.assertEqual(p.pack(d), p.pack(d.astimezone().replace(tzinfo=None)))

        # keys are sorted in time
        self.assertLess(p.pack(d), p.pack(d + timedelta(microseconds=1)))

    def testDatetimeExact(self):
        # packing an unpacked key results in the same key
        p = DatetimePacker()
        for _ in range(10000):
            key = struct.pack('!Q', random.randrange(2**52))
            self.assertEqual(key, p.pack(p.unpack(key)))

    def testStruct2(self):
        p = StructPacker('HH')
        with self.assertRaises(struct.error):
            p.pack([3, 4.0])


if __name__ == "__main__":
    unittest.main()
