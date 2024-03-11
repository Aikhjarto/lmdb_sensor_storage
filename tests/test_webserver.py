import datetime
import math
import os
import requests
import shutil
import tempfile
import threading
import unittest
# noinspection PyProtectedMember
from lmdb_sensor_storage.db._manager import manager
from lmdb_sensor_storage.db.sensor_db import Sensor, Notes
# noinspection PyProtectedMember
from lmdb_sensor_storage.mdb_server._mdb_server import MDBServer, MDBRequestHandler


class UnitTests(unittest.TestCase):

    # noinspection PyAttributeOutsideInit
    def setUp(self):
        # logger = logging.getLogger('lmdb_sensor_storage.db')
        # logger.setLevel(logging.DEBUG)

        super(UnitTests, self).setUp()
        self.tempfolder = tempfile.mkdtemp()
        self.mdb_filename = os.path.join(self.tempfolder, 'unittest.mdb')

        self.server = MDBServer(self.mdb_filename, ("", 0), MDBRequestHandler)
        self.port = self.server.server_address[1]
        self.base_url = f'http://localhost:{self.port}'
        
        self.server.daemon_threads = True
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()

        self.reference_date = datetime.datetime.now()

        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='sensor1', data_format='f')
        s.metadata['unit'] = 'V'
        for i in range(150):
            s.write_value(date=self.reference_date-datetime.timedelta(seconds=i), value=i)

        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='sensor2', data_format='f')
        s.metadata['unit'] = 'V'
        for i in range(100):
            s.write_value(date=self.reference_date - datetime.timedelta(seconds=i), value=math.sqrt(i))

        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='sensor_RGBC', data_format='ffff')
        s.metadata['field_names'] = ('R', 'G', 'B', 'I')
        for i in range(100):
            s.write_value(date=self.reference_date - datetime.timedelta(seconds=i),
                          value=[math.sqrt(i), math.log10(i+1), i, math.log2(i+1)])

        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='s1', data_format='f')
        s[self.reference_date] = 1
        s[self.reference_date + datetime.timedelta(seconds=5)] = 2
        s[self.reference_date + datetime.timedelta(seconds=10.1)] = 3
        s[self.reference_date + datetime.timedelta(seconds=15)] = 4

        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='s2', data_format='f')
        s[self.reference_date] = 10
        s[self.reference_date + datetime.timedelta(seconds=5)] = 20
        s[self.reference_date + datetime.timedelta(seconds=6.5)] = 30
        s[self.reference_date + datetime.timedelta(seconds=15)] = 40

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join()
        manager.close_all()
        shutil.rmtree(self.tempfolder)

    def test_get_stat_json(self):
        req = requests.request('GET', f'{self.base_url}/stat.json', timeout=1)
        self.assertEqual(200, req.status_code)

        data = req.json()
        self.assertEqual(self.mdb_filename, data['filename'])
        self.assertEqual(5, len(data['sensors']))
        self.assertEqual(150, data['sensors']['sensor1']['entries'])
        self.assertEqual(100, data['sensors']['sensor2']['entries'])
        self.assertEqual('V', data['sensors']['sensor1']['meta']['unit'])
        self.assertEqual('V', data['sensors']['sensor2']['meta']['unit'])
        self.assertEqual('f', data['sensors']['sensor2']['data_format'])
        self.assertEqual('f', data['sensors']['sensor2']['data_format'])

    def test_get_plotly_js(self):
        req = requests.request('GET', f'{self.base_url}/plotly.min.js', timeout=1)
        self.assertEqual(200, req.status_code)

    def test_get_favicon(self):
        req = requests.request('GET', f'{self.base_url}/favicon.ico', timeout=1)
        self.assertEqual(200, req.status_code)

    def test_get_sensor_data_wrong_sensor(self):

        # request data from non-existing sensor
        req = requests.request('GET', f'{self.base_url}/data?sensor_name=sensor3', timeout=1)
        self.assertEqual(422, req.status_code)

    def test_get_sensor_data_sensor1(self):
        req = requests.request('GET', f'{self.base_url}/data?sensor_name=sensor1', timeout=1)
        self.assertEqual(200, req.status_code)
        self.assertEqual(len(req.text.split('\n')), 151)

    def test_get_sensor_data_sensor2(self):
        req = requests.request('GET', f'{self.base_url}/data?sensor_name=sensor2', timeout=1)
        self.assertEqual(200, req.status_code)
        self.assertEqual(len(req.text.split('\n')), 101)

    def test_get_sensor_data_struct(self):
        req = requests.request('GET', f'{self.base_url}/data',
                               params={'sensor_name': 'sensor_RGBC',
                                       'include_header': 'true'},
                               timeout=1)
        self.assertEqual(200, req.status_code)
        self.assertEqual(len(req.text.split('\n')), 102)

    def test_get_sensor_data_without_sensor_name(self):
        # error message due to request data with no sensor name
        req = requests.request('GET', f'{self.base_url}/data', timeout=1)
        self.assertEqual(422, req.status_code)

    def test_get_sensor_data_since(self):
        # get most recent value
        req = requests.request('GET', f'{self.base_url}/data', timeout=1000,
                               params={'sensor_name': "sensor1",
                                       'since': self.reference_date})
        self.assertEqual(200, req.status_code)
        self.assertEqual(2, len(req.text.split('\n')))

    def test_get_sensor_data_since_with_header(self):
        # get most recent value with header
        req = requests.request('GET', f'{self.base_url}/data', timeout=1,
                               params={'sensor_name': "sensor1",
                                       'include_header': True,
                                       'since': self.reference_date})
        self.assertEqual(200, req.status_code)
        self.assertEqual(3, len(req.text.split('\n')))

    def test_get_sensor_data_since_in_isoformat(self):
        # get last 10 seconds requested in isoformat
        req = requests.request('GET', f'{self.base_url}/data', timeout=1,
                               params={'sensor_name': "sensor1",
                                       'since': self.reference_date-datetime.timedelta(seconds=10)})
        self.assertEqual(200, req.status_code)
        self.assertEqual(12, len(req.text.split('\n')))

    def test_get_sensor_data_since_as_timestamp(self):
        # get last 10 seconds requested as timestamp
        req = requests.request('GET', f'{self.base_url}/data', timeout=1,
                               params={'sensor_name': "sensor1",
                                       'since': (self.reference_date-datetime.timedelta(seconds=10)).timestamp()})
        self.assertEqual(200, req.status_code)
        self.assertEqual(12, len(req.text.split('\n')))

    def test_get_nodered_chart(self):
        req = requests.request('GET', f'{self.base_url}/nodered_chart', timeout=1)
        self.assertEqual(422, req.status_code)

        req = requests.request('GET', f'{self.base_url}/nodered_chart',
                               params={'sensor_name': ["sensor1", "sensor2"]}, timeout=1)
        self.assertEqual(200, req.status_code)
        self.assertEqual(1, len(req.json()))

        req = requests.request('GET', f'{self.base_url}/nodered_chart',
                               params={'sensor_name': ["sensor1", "sensor2"]}, timeout=1)
        self.assertEqual(200, req.status_code)
        self.assertEqual(1, len(req.json()))

    def test_get_notes(self):
        # get emtpy notes
        req = requests.request('GET', f'{self.base_url}/notes', timeout=1)
        self.assertEqual(200, req.status_code)
        self.assertEqual(0, len(req.json()),)

        Notes(self.mdb_filename).add_note('brief note', 'long message text', self.reference_date)

        req = requests.request('GET', f'{self.base_url}/notes', timeout=1)
        self.assertEqual(200, req.status_code)
        data = req.json()
        self.assertEqual(data,
                         [[self.reference_date.timestamp(), {'long': 'long message text', 'short': 'brief note'}]])

    def test_get_root(self):
        req = requests.request('GET', f'{self.base_url}/', timeout=1)
        self.assertEqual(307, req.history[0].status_code)
        self.assertEqual(307, req.history[1].status_code)
        self.assertEqual(200, req.status_code)

    def test_get_now(self):
        req = requests.request('GET', f'{self.base_url}/now', timeout=1)
        self.assertEqual(307, req.history[0].status_code)
        self.assertEqual(200, req.status_code)

    def test_get_today(self):
        req = requests.request('GET', f'{self.base_url}/today', timeout=1)
        self.assertEqual(307, req.history[0].status_code)
        self.assertEqual(200, req.status_code)

    def test_get_week(self):
        req = requests.request('GET', f'{self.base_url}/week', timeout=1)
        self.assertEqual(307, req.history[0].status_code)
        self.assertEqual(200, req.status_code)

    def test_get_month(self):
        req = requests.request('GET', f'{self.base_url}/month', timeout=1)
        self.assertEqual(307, req.history[0].status_code)
        self.assertEqual(200, req.status_code)

    def test_get_time(self):
        req = requests.request('GET', f'{self.base_url}/time', timeout=1)
        self.assertEqual(200, req.status_code)

        req = requests.request('GET', f'{self.base_url}/time?&sensor_name=sensor1&sensor_name=sensor2', timeout=1)
        self.assertEqual(200, req.status_code)

        req = requests.request('GET', f'{self.base_url}/time?&sensor_name=sensor3', timeout=1)
        self.assertEqual(422, req.status_code)

    def test_get_json_no_sensor_name(self):
        req = requests.request('GET', f'{self.base_url}/json', timeout=1)
        self.assertEqual(422, req.status_code)

    def test_get_json_single_sensor(self):
        req = requests.request('GET', f'{self.base_url}/json', timeout=1,
                               params={'sensor_name': 's1'})
        j = req.json()
        self.assertEqual(4, len(j['Time']))
        self.assertEqual(4, len(j['s1']['values']))

    def test_get_json_two_sensor_with_unequal_timestamps(self):
        req = requests.request('GET', f'{self.base_url}/json', timeout=1,
                               params={'sensor_name': ['s1', 's2']})
        j = req.json()
        self.assertEqual(5, len(j['Time']))
        self.assertEqual(5, len(j['s1']['values']))
        self.assertEqual(5, len(j['s2']['values']))

    def test_get_json_two_sensor_with_unequal_timestamps_with_since_and_until(self):
        req = requests.request('GET', f'{self.base_url}/json', timeout=1000,
                               params={'sensor_name': ['s1', 's2'],
                                       'since': self.reference_date + datetime.timedelta(seconds=2),
                                       'until': self.reference_date + datetime.timedelta(seconds=11)})
        j = req.json()
        ref = {'Time': [(self.reference_date + datetime.timedelta(seconds=5)).isoformat(),
                        (self.reference_date + datetime.timedelta(seconds=6.5)).isoformat(),
                        (self.reference_date + datetime.timedelta(seconds=10.1)).isoformat()],
               's1': {'values': [2.0, 2.0, 3.0]},
               's2': {'values': [20.0, 30.0, 30.0]}
               }
        self.assertEqual(ref, j)


if __name__ == '__main__':
    unittest.main()
