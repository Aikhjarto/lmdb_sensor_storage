import struct
import unittest
from lmdb_sensor_storage.db.packer import FloatPacker, IntPacker, StringPacker, JSONPacker, StructPacker


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

    def testStruct2(self):
        p = StructPacker('HH')
        with self.assertRaises(struct.error):
            p.pack([3, 4.0])


if __name__ == "__main__":
    unittest.main()
