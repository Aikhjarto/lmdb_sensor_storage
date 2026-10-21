import unittest
from datetime import datetime, timedelta

from lmdb_sensor_storage import Sensor
from lmdb_sensor_storage.generate_history_html import generate_history_div
from tests import EmptyDatabaseMixin


class TestGenerateHistoryDiv(EmptyDatabaseMixin, unittest.TestCase):

    def test_keep_local_extrema(self):
        start = datetime(2024, 1, 1)
        Sensor(self.mdb_filename, 's').write_values([start + timedelta(seconds=i) for i in range(100)],
                                                    [float(i % 7) for i in range(100)])
        div = generate_history_div(self.mdb_filename, decimate_to_s=10, keep_local_extrema=True)
        self.assertTrue(div.startswith('<div>'))
        self.assertIn('Plotly.newPlot', div)
