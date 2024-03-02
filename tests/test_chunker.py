import unittest

from lmdb_sensor_storage.db import value_chunker_minmeanmax, value_chunker_mean


class TestcaseChunker(unittest.TestCase):

    def test_minmeamax(self):
        val = (1, 2, 3, 4)
        self.assertEqual((1, 2.5, 4), value_chunker_minmeanmax(val))
        self.assertEqual([2.5, ], value_chunker_mean(val))

        val = ((1, 2, 3, 4),
               (2, 3, 4, 5))
        self.assertEqual(([1, 2, 3, 4], [1.5, 2.5, 3.5, 4.5], [2, 3, 4, 5]), value_chunker_minmeanmax(val))
        self.assertEqual([[1.5, 2.5, 3.5, 4.5, ]], value_chunker_mean(val))


if __name__ == "__main__":
    unittest.main()
