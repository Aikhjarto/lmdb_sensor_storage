import datetime
from html import escape
import math
import os
import requests
import shutil
import tempfile
import threading
import unittest
from unittest import mock
import urllib.parse
# noinspection PyProtectedMember
from lmdb_sensor_storage.db._manager import manager
from lmdb_sensor_storage.db.sensor_db import Sensor, Notes
from lmdb_sensor_storage.db.timestamp_db import TimestampBytesDB
# noinspection PyProtectedMember
from lmdb_sensor_storage.mdb_server._mdb_server import MDBServer, MDBRequestHandler
from tests import assert_no_logs


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
        # short poll interval, since shutdown in tearDown waits for it
        self.server_thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': 0.05},
                                              daemon=True)
        self.server_thread.start()

        self.reference_date = datetime.datetime.now()

        # Note: write each sensor in one transaction, since each transaction is synced to disk, which is slow
        def seconds_before(n):
            return [self.reference_date - datetime.timedelta(seconds=i) for i in range(n)]

        def seconds_after(seconds):
            return [self.reference_date + datetime.timedelta(seconds=i) for i in seconds]

        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='sensor1', data_format='f')
        s.metadata['unit'] = 'V'
        s.write_values(seconds_before(150), range(150))

        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='sensor2', data_format='f')
        s.metadata['unit'] = 'V'
        s.write_values(seconds_before(100), [math.sqrt(i) for i in range(100)])

        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='sensor_RGBC', data_format='ffff')
        s.metadata['field_names'] = ('R', 'G', 'B', 'I')
        s.write_values(seconds_before(100),
                       [[math.sqrt(i), math.log10(i+1), i, math.log2(i+1)] for i in range(100)])

        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='s1', data_format='f')
        s.write_values(seconds_after((0, 5, 10.1, 15)), [1, 2, 3, 4])

        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='s2', data_format='f')
        s.write_values(seconds_after((0, 5, 6.5, 15)), [10, 20, 30, 40])

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

    def get_json(self, **params):
        return requests.request('GET', f'{self.base_url}/json', params={'sensor_name': 'sensor1', **params},
                                timeout=10)

    def test_no_state_between_requests(self):
        req = self.get_json()
        self.assertEqual(200, req.status_code)
        self.assertNotIn('Set-Cookie', req.headers)
        # sensor_name of the previous request is not kept
        self.assertEqual(422, requests.request('GET', f'{self.base_url}/json', timeout=10).status_code)

    def test_query_validation(self):
        self.assertEqual(422, self.get_json(limit=0).status_code)
        self.assertEqual(422, self.get_json(decimate_to_s=0).status_code)
        self.assertEqual(422, self.get_json(decimate_to_s='nan').status_code)

        requested = {}

        def get_json(sensor_names, write, **kwargs):
            requested.update(kwargs)
            write(b'{}')
        self.server.sensor_storage.get_json = get_json
        self.assertEqual(200, self.get_json(limit=10**9).status_code)
        self.assertEqual(self.server.MAX_LIMIT, requested['limit'])

    def test_time_page_keeps_state_in_url(self):
        params = {'since': (self.reference_date - datetime.timedelta(hours=1)).isoformat(),
                  'decimate_to_s': 60, 'sensor_name': 'sensor1'}
        req = requests.request('GET', f'{self.base_url}/time', params=params, timeout=10)
        self.assertEqual(200, req.status_code)

        # selected sensors, and decimation, which is not part of the form
        self.assertIn('<option value="sensor1" selected>sensor1</option>', req.text)
        self.assertIn('<option value="sensor2">sensor2</option>', req.text)
        self.assertIn('<input type="hidden" name="decimate_to_s" value="60.0">', req.text)

        # note forms return to the current view
        query = escape(urllib.parse.urlencode(params))
        self.assertIn(f'<form id="note_add" action="time?{query}" method="POST">', req.text)
        self.assertIn(f'<form id="note_delete" action="time?delete=yes&amp;{query}" method="POST">', req.text)

        # without selection, all sensors are shown
        req = requests.request('GET', f'{self.base_url}/time', params={'since': params['since']}, timeout=10)
        self.assertIn('<option value="sensor2" selected>sensor2</option>', req.text)

    def test_note_returns_to_view(self):
        req = self.post('/time?since=2024-01-01T00%3A00&sensor_name=sensor1', {'short': 'note'})
        self.assertEqual(200, req.status_code)
        self.assertIn('url=/time?since=2024-01-01T00%3A00&amp;sensor_name=sensor1"', req.text)

        timestamp = str(Notes(self.mdb_filename).keys()[0])
        req = self.post('/time?delete=yes&since=2024-01-01T00%3A00', {'timestamp': timestamp})
        self.assertEqual(200, req.status_code)
        self.assertIn('url=/time?since=2024-01-01T00%3A00"', req.text)

    def test_redirect_urls(self):
        for path in ('/now', '/today', '/week', '/month'):
            req = requests.request('GET', f'{self.base_url}{path}', allow_redirects=False, timeout=10)
            self.assertEqual(307, req.status_code)
            location = req.headers['Location']
            self.assertNotIn(' ', location)
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(location).query)
            self.assertLess(datetime.datetime.fromisoformat(query['since'][0]),
                            datetime.datetime.fromisoformat(query['until'][0]))

    def post(self, path, data):
        return requests.request('POST', f'{self.base_url}{path}', data=data, timeout=10)

    def test_post_add_and_delete_note(self):
        notes = Notes(self.mdb_filename)
        timestamp = datetime.datetime(2024, 1, 2, 3, 4, 5, 678901)
        req = self.post('/time', {'timestamp': timestamp.isoformat(), 'short': 'brief', 'long': 'longer'})
        self.assertEqual(200, req.status_code)
        # the form sends local time
        self.assertEqual([(timestamp.astimezone(), {'short': 'brief', 'long': 'longer'})], notes.items())

        # delete with the timestamp as shown in the form
        req = self.post('/time?delete=yes', {'timestamp': str(notes.keys()[0])})
        self.assertEqual(200, req.status_code)
        self.assertEqual([], notes.items())

        # deleting again fails
        self.assertEqual(404, self.post('/time?delete=yes', {'timestamp': str(timestamp)}).status_code)

    def test_post_note_without_timestamp(self):
        before = datetime.datetime.now(datetime.timezone.utc)
        self.assertEqual(200, self.post('/time', {'timestamp': '', 'short': 'now'}).status_code)
        timestamp, note = Notes(self.mdb_filename).items()[0]
        self.assertGreaterEqual(timestamp, before)
        self.assertEqual({'short': 'now'}, note)

    def test_post_note_invalid(self):
        self.assertEqual(400, self.post('/time', {'timestamp': 'yesterday', 'short': 'x'}).status_code)
        self.assertEqual(400, self.post('/time', {'timestamp': '2024-01-01T00:00'}).status_code)
        self.assertEqual(400, self.post('/time?delete=yes', {}).status_code)
        self.assertEqual([], Notes(self.mdb_filename).items())

    def test_post_add_sample(self):
        timestamp = self.reference_date + datetime.timedelta(seconds=1)
        req = self.post('/add_sample', f'{timestamp.isoformat()};42.5;sensor1')
        self.assertEqual(204, req.status_code)
        self.assertEqual(42.5, Sensor(self.mdb_filename, 'sensor1')[timestamp])

        self.assertEqual(400, self.post('/add_sample', 'garbage').status_code)
        self.assertEqual(400, self.post('/add_sample', f'{timestamp.isoformat()};abc;sensor1').status_code)

    def test_json_errors(self):
        req = requests.request('GET', f'{self.base_url}/json', timeout=10)
        self.assertEqual(422, req.status_code)
        self.assertIn('sensor_name', req.json()['error'])

        req = self.get_json(limit=0)
        self.assertEqual(422, req.status_code)
        self.assertIn('limit', req.json()['error'])

        def fail(*args, **kwargs):
            raise RuntimeError('broken')
        self.server.sensor_storage.get_json = fail
        req = self.get_json()
        self.assertEqual(500, req.status_code)
        self.assertEqual('RuntimeError: broken', req.json()['error'])
        self.assertIn('Traceback', req.json()['explain'])

    def test_current_minute(self):
        # a note in the current minute is listed
        now = datetime.datetime.now()
        Notes(self.mdb_filename).add_note('current', timestamp=now)
        req = requests.request('GET', f'{self.base_url}/now', timeout=10)
        self.assertEqual(200, req.status_code)
        self.assertIn(f'{now}; current</option>', req.text)

        # the time range form keeps seconds, so re-submitting it does not cut off the current minute
        self.assertRegex(req.text, r'id="until" name="until" value="\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d"')

    def test_gzip_only_for_remote_clients(self):
        headers = {'Accept-Encoding': 'gzip'}
        for path in ('/json?sensor_name=sensor1', '/plotly.min.js'):
            with self.subTest(path=path):
                req = requests.request('GET', f'{self.base_url}{path}', headers=headers, timeout=10)
                self.assertEqual(200, req.status_code)
                self.assertNotIn('Content-Encoding', req.headers)

                with mock.patch.object(MDBRequestHandler, 'is_localhost', return_value=False):
                    req = requests.request('GET', f'{self.base_url}{path}', headers=headers, timeout=10)
                self.assertEqual(200, req.status_code)
                self.assertEqual('gzip', req.headers['Content-Encoding'])

    def test_request_logging(self):
        # requests from localhost are only logged with debug level
        with self.assertLogs('lmdb_sensor_storage.httpd', level='DEBUG') as logs:
            requests.request('GET', f'{self.base_url}/stat.json', timeout=10)
        self.assertIn('GET /stat.json', '\n'.join(logs.output))

        with assert_no_logs(self, 'lmdb_sensor_storage.httpd', level='INFO'):
            requests.request('GET', f'{self.base_url}/stat.json', timeout=10)

    def test_streaming_errors(self):
        def fail_after(n):
            def generate(sensor_names, write, **kwargs):
                write(b'x' * n)
                raise RuntimeError('broken')
            return generate

        for path, method in (('/json', 'get_json'), ('/data', 'get_csv')):
            with self.subTest(path=path):
                # error within the buffered beginning: error response
                with mock.patch.object(self.server.sensor_storage, method, fail_after(10)):
                    req = requests.request('GET', f'{self.base_url}{path}', params={'sensor_name': 'sensor1'},
                                           timeout=10)
                self.assertEqual(500, req.status_code)
                if path == '/json':
                    self.assertEqual('RuntimeError: broken', req.json()['error'])

                # error after the response started: incomplete response
                with mock.patch.object(self.server.sensor_storage, method, fail_after(100)), \
                        mock.patch.object(MDBRequestHandler, 'STREAM_BUFFER_SIZE', 10):
                    with self.assertRaises(requests.exceptions.ChunkedEncodingError):
                        requests.request('GET', f'{self.base_url}{path}', params={'sensor_name': 'sensor1'},
                                         timeout=10)

    def test_streaming_larger_than_buffer(self):
        expected = requests.request('GET', f'{self.base_url}/data', params={'sensor_name': 'sensor1'},
                                    timeout=10).content
        with mock.patch.object(MDBRequestHandler, 'STREAM_BUFFER_SIZE', 10):
            req = requests.request('GET', f'{self.base_url}/data', params={'sensor_name': 'sensor1'}, timeout=10)
        self.assertEqual(200, req.status_code)
        self.assertEqual(expected, req.content)
        self.assertGreater(len(expected), 10)

    def test_sensor_without_format(self):
        # data without entry in format_, e.g. written by an old version of import_csv
        TimestampBytesDB(self.mdb_filename, 'data_raw').write_value(self.reference_date, b'1234')
        for path in ('/json', '/data'):
            with self.subTest(path=path):
                req = requests.request('GET', f'{self.base_url}{path}', params={'sensor_name': 'raw'}, timeout=10)
                self.assertEqual(422, req.status_code)

    def test_html_escaping(self):
        payload = '</script><img src=x onerror=alert(1)>'
        s = Sensor(mdb_filename=self.mdb_filename, sensor_name=payload, data_format='f')
        s.metadata['unit'] = payload
        s[self.reference_date] = 1
        Notes(self.mdb_filename).add_note(payload, payload, self.reference_date)

        # time range must include the note
        params = {'since': (self.reference_date - datetime.timedelta(hours=1)).isoformat(),
                  'until': (self.reference_date + datetime.timedelta(hours=1)).isoformat()}
        req = requests.request('GET', f'{self.base_url}/time', params=params, timeout=10)
        self.assertEqual(200, req.status_code)
        # escaped note in the form for deleting notes
        self.assertIn('; &lt;/script&gt;&lt;img src=x onerror=alert(1)&gt;</option>', req.text)
        self.assertIn('alert(1)', req.text)
        self.assertNotIn('<img', req.text)

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
        # times in local timezone with offset
        ref = {'Time': [(self.reference_date + datetime.timedelta(seconds=5)).astimezone().isoformat(),
                        (self.reference_date + datetime.timedelta(seconds=6.5)).astimezone().isoformat(),
                        (self.reference_date + datetime.timedelta(seconds=10.1)).astimezone().isoformat()],
               's1': {'values': [2.0, 2.0, 3.0]},
               's2': {'values': [20.0, 30.0, 30.0]}
               }
        self.assertEqual(ref, j)



class TestListenAddress(unittest.TestCase):

    def setUp(self):
        self.tempfolder = tempfile.mkdtemp()
        self.mdb_filename = os.path.join(self.tempfolder, 'unittest.mdb')

    def tearDown(self):
        manager.close_all()
        shutil.rmtree(self.tempfolder)

    def get_status(self, url):
        try:
            return requests.request('GET', url, timeout=10).status_code
        except requests.ConnectionError:
            return None

    def test_listen_address(self):
        # address to listen on, URL host which must be reachable, URL host which must not be reachable
        for address, reachable, unreachable in (('127.0.0.1', '127.0.0.1', '[::1]'),
                                                ('::1', '[::1]', '127.0.0.1'),
                                                ('', '127.0.0.1', None),
                                                ('', '[::1]', None)):
            with self.subTest(address=address, reachable=reachable):
                server = MDBServer(self.mdb_filename, (address, 0), MDBRequestHandler)
                thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.05}, daemon=True)
                thread.start()
                try:
                    port = server.server_address[1]
                    self.assertEqual(200, self.get_status(f'http://{reachable}:{port}/stat.json'))
                    if unreachable:
                        self.assertIsNone(self.get_status(f'http://{unreachable}:{port}/stat.json'))
                finally:
                    server.shutdown()
                    server.server_close()
                    thread.join()


if __name__ == '__main__':
    unittest.main()
