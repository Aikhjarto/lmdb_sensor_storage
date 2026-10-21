import json
import os
import unittest
from datetime import datetime, timezone
from unittest import mock

import requests

from lmdb_sensor_storage import Sensor, LMDBSensorStorage
from lmdb_sensor_storage.import_wunderground import import_wunderground, import_wunderground_station
from tests import EmptyDatabaseMixin

OBSERVATIONS = {'observations': [
    {'stationID': 'XY1', 'epoch': 1700000000, 'metric': {'tempAvg': 5.5, 'windspeedAvg': 1.5, 'windgustHigh': 4.0}},
    {'stationID': 'XY1', 'epoch': 1700000300, 'metric': {'tempAvg': None, 'windspeedAvg': 2.0, 'windgustHigh': 6.0}},
]}


class TestImportWunderground(EmptyDatabaseMixin, unittest.TestCase):

    def import_json(self, data):
        json_filename = os.path.join(self.tempfolder, 'data.json')
        with open(json_filename, 'w') as f:
            json.dump(data, f)
        import_wunderground(self.mdb_filename, json_filename)

    def test_import(self):
        self.import_json(OBSERVATIONS)

        dates = [datetime.fromtimestamp(1700000000, tz=timezone.utc), datetime.fromtimestamp(1700000300, tz=timezone.utc)]
        temperature = Sensor(self.mdb_filename, 'outside_temperature_XY1')
        self.assertEqual(dates, temperature.keys())
        self.assertEqual(5.5, temperature.values()[0])

        speed = Sensor(self.mdb_filename, 'wind_speed_XY1')
        self.assertEqual([1.5, 2.0], speed.values())
        self.assertEqual('Windgeschwindigkeit XY1', speed.metadata['label'])

        gust = Sensor(self.mdb_filename, 'wind_gust_XY1')
        self.assertEqual([4.0, 6.0], gust.values())
        self.assertEqual('Windböen XY1', gust.metadata['label'])

    def test_no_observations(self):
        self.import_json({'observations': []})
        self.assertEqual([], LMDBSensorStorage(self.mdb_filename).keys())

    def test_request(self):
        response = mock.Mock(ok=True, content=json.dumps(OBSERVATIONS).encode())
        with mock.patch('requests.get', return_value=response) as get, \
                self.assertLogs('lmdb_sensor_storage.wunderground', level='DEBUG') as logs:
            import_wunderground_station(self.mdb_filename, 'XY1', 'secret-key', date='2024-01-02')

        self.assertEqual('secret-key', get.call_args.kwargs['params']['apiKey'])
        self.assertEqual('20240102', get.call_args.kwargs['params']['date'])
        self.assertIsNotNone(get.call_args.kwargs['timeout'])
        self.assertNotIn('secret-key', '\n'.join(logs.output))
        self.assertEqual(2, len(Sensor(self.mdb_filename, 'wind_gust_XY1')))

    def test_request_error_does_not_leak_key(self):
        error = requests.ConnectionError('https://api.weather.com/...&apiKey=secret-key')
        with mock.patch('requests.get', side_effect=error):
            with self.assertRaises(RuntimeError) as cm:
                import_wunderground_station(self.mdb_filename, 'XY1', 'secret-key')
        self.assertNotIn('secret-key', str(cm.exception))
