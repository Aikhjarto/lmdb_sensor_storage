import json
import unittest
from unittest import mock
from datetime import datetime, timedelta
from lmdb_sensor_storage import Sensor, LMDBSensorStorage
# noinspection PyProtectedMember
from lmdb_sensor_storage.db._manager import manager
# noinspection PyProtectedMember
from lmdb_sensor_storage._parser import as_datetime, as_local_datetime
from tests import EmptyDatabaseMixin


class DataMixin(EmptyDatabaseMixin):
    # noinspection PyAttributeOutsideInit
    def setUp(self):
        super().setUp()

        self.reference_date = as_datetime("2000-01-01")
        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='s1', data_format='f')
        s[self.reference_date] = 1
        s[self.reference_date + timedelta(seconds=5)] = 2
        s[self.reference_date + timedelta(seconds=10.1)] = 3
        s[self.reference_date + timedelta(seconds=15)] = 4

        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='s2', data_format='f')
        s[self.reference_date] = 10
        s[self.reference_date + timedelta(seconds=5)] = 20
        s[self.reference_date + timedelta(seconds=6.5)] = 30
        s[self.reference_date + timedelta(seconds=15)] = 40

        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='s3', data_format='HH')
        s[self.reference_date] = (100, 101)
        s[self.reference_date + timedelta(seconds=3)] = (200, 201)
        s[self.reference_date + timedelta(seconds=4.5)] = (300, 301)
        s[self.reference_date + timedelta(seconds=11)] = (400, 401)

        s.copy_to("s4")
        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='s4')
        s.metadata['field_names'] = ['A', 'B']
        s.notes.add_note(short='Sensorlevel Note', long='Very long description of sensorlevel note',
                         timestamp=self.reference_date + timedelta(seconds=7))

        s = Sensor(mdb_filename=self.mdb_filename, sensor_name='s5', data_format='str')
        s[self.reference_date] = 'test with space'
        s[self.reference_date + timedelta(seconds=3)] = 'test with , '
        s[self.reference_date + timedelta(seconds=4.5)] = 'test'
        s[self.reference_date + timedelta(seconds=11)] = 'test with ;'

        self.db = LMDBSensorStorage(self.mdb_filename)
        self.db.notes.add_note(short='Toplevel Note', long='Very long description of toplevel note',
                               timestamp=self.reference_date + timedelta(seconds=4))


class TestcaseLMDBSensorStorage(EmptyDatabaseMixin, unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.reference_date = as_datetime("2000-01-01")

    def test_get_on_empty_dB(self):
        m = LMDBSensorStorage(self.mdb_filename)
        self.assertIsInstance(m.statistics, dict)
        self.assertIsNotNone(m._environment)
        self.assertEqual(m.keys(), [])
        self.assertEqual(m.get_non_empty_sensor_names(), [])
        self.assertEqual(m.get_non_empty_sensors(), [])
        m.close()
        self.assertEqual(manager.handles, {})

    def test_delete(self):
        s1 = Sensor(self.mdb_filename, 'test')
        s2 = Sensor(self.mdb_filename, 'test2')
        file = LMDBSensorStorage(self.mdb_filename)

        self.assertFalse(file.keys())

        s1[datetime.now()] = 1.0
        s2[datetime.now()] = 2.0
        self.assertEqual(file.keys(), ['test', 'test2'])

        del file['test']
        self.assertEqual(file.keys(), ['test2'])

        del file['test2']
        self.assertFalse(file.keys())


class TestcaseLMDBSensorStorageGetJSON(DataMixin, unittest.TestCase):

    def setUp(self):
        super().setUp()

    def test_get_json_missing_sensor(self):
        with self.assertRaises(KeyError):
            self.db.get_json(['s1', 's0'])

    def test_get_json(self):

        # test without limits
        json_bytes = self.db.get_json(['s1', 's2'])
        data = json.loads(json_bytes)
        ref = {'Time': ['2000-01-01T00:00:00+01:00',
                        '2000-01-01T00:00:05+01:00',
                        '2000-01-01T00:00:06.500000+01:00',
                        '2000-01-01T00:00:10.100000+01:00',
                        '2000-01-01T00:00:15+01:00'],
               'notes': [{'2000-01-01T00:00:04+01:00': {'long': 'Very long description of toplevel note',
                                                        'short': 'Toplevel Note'}}],
               's1': {"values": [1.0, 2.0, 2.0, 3.0, 4.0]},
               's2': {"values": [10.0, 20.0, 30.0, 30.0, 40.0]}}
        self.assertEqual(ref, data)

        # test with since and until
        json_bytes = self.db.get_json(['s1', 's2'],
                                      since=self.reference_date + timedelta(seconds=2),
                                      until=self.reference_date + timedelta(seconds=11))

        data = json.loads(json_bytes)
        ref = {'Time': [as_local_datetime(self.reference_date + timedelta(seconds=5)).isoformat(),
                        as_local_datetime(self.reference_date + timedelta(seconds=6.5)).isoformat(),
                        as_local_datetime(self.reference_date + timedelta(seconds=10.1)).isoformat()],
               'notes': [{'2000-01-01T00:00:04+01:00': {'long': 'Very long description of toplevel note',
                                                        'short': 'Toplevel Note'}}],
               's1': {"values": [2.0, 2.0, 3.0]},
               's2': {"values": [20.0, 30.0, 30.0]}}
        self.assertEqual(ref, data)

    def test_get_json_with_notes(self):
        json_bytes = self.db.get_json(['s1', 's2', 's4'],
                                      since=self.reference_date + timedelta(seconds=2),
                                      until=self.reference_date + timedelta(seconds=11))

        data = json.loads(json_bytes)
        ref = {'Time': ['2000-01-01T00:00:03+01:00',
                        '2000-01-01T00:00:04.500000+01:00',
                        '2000-01-01T00:00:05+01:00',
                        '2000-01-01T00:00:06.500000+01:00',
                        '2000-01-01T00:00:10.100000+01:00'],
               'notes': [{'2000-01-01T00:00:04+01:00': {'long': 'Very long description of toplevel note',
                                                        'short': 'Toplevel Note'}}],
               's1': {'values': [1.0, 1.0, 2.0, 2.0, 3.0]},
               's2': {'values': [10.0, 10.0, 20.0, 30.0, 30.0]},
               's4': {'metadata': {'field_names': ['A', 'B']},
                      'notes': [{'2000-01-01T00:00:07+01:00': {'long': 'Very long description of sensorlevel note',
                                                         'short': 'Sensorlevel Note'}}],
                      'values': [[200, 201], [300, 301], [300, 301], [300, 301], [300, 301]]}}
        self.assertEqual(ref, data)

    def test_get_json_with_str(self):
        json_bytes = self.db.get_json(['s1', 's5'])
        data = json.loads(json_bytes)
        data_ref = {'Time': ['2000-01-01T00:00:00+01:00',
                             '2000-01-01T00:00:03+01:00',
                             '2000-01-01T00:00:04.500000+01:00',
                             '2000-01-01T00:00:05+01:00',
                             '2000-01-01T00:00:10.100000+01:00',
                             '2000-01-01T00:00:11+01:00',
                             '2000-01-01T00:00:15+01:00'],
                    's1': {'values': [1.0, 1.0, 1.0, 2.0, 3.0, 3.0, 4.0]},
                    's5': {'values': ['test with space',
                                      'test with , ',
                                      'test',
                                      'test',
                                      'test',
                                      'test with ;',
                                      'test with ;']},
                    'notes': [{'2000-01-01T00:00:04+01:00': {'long': 'Very long description of toplevel note',
                                                       'short': 'Toplevel Note'}}]}
        self.assertEqual(data_ref, data)


class TestcaseLMDBSensorStorageGetCSV(DataMixin, unittest.TestCase):

    def setUp(self):
        super().setUp()

    def test_get_csv_missing_sensor(self):
        with self.assertRaises(KeyError):
            self.db.get_csv(['s0'])
        with self.assertRaises(KeyError):
            self.db.get_csv(['s1', 's0'])

    def test_get_csv_with_float(self):
        # test without limits and without header
        data = self.db.get_csv(['s1',])
        ref = (b'2000-01-01T00:00:00+01:00;1.0\n'
               b'2000-01-01T00:00:05+01:00;2.0\n'
               b'2000-01-01T00:00:10.100000+01:00;3.0\n'
               b'2000-01-01T00:00:15+01:00;4.0\n')
        self.assertEqual(ref, data)

        # test without limits and without header
        data = self.db.get_csv(['s1',], include_header=True)
        ref = (b'"Time";"s1"\n'
               b'2000-01-01T00:00:00+01:00;1.0\n'
               b'2000-01-01T00:00:05+01:00;2.0\n'
               b'2000-01-01T00:00:10.100000+01:00;3.0\n'
               b'2000-01-01T00:00:15+01:00;4.0\n')
        self.assertEqual(ref, data)

        # test without limits and without header
        data = self.db.get_csv(['s1',], include_header=True)
        ref = (b'"Time";"s1"\n'
               b'2000-01-01T00:00:00+01:00;1.0\n'
               b'2000-01-01T00:00:05+01:00;2.0\n'
               b'2000-01-01T00:00:10.100000+01:00;3.0\n'
               b'2000-01-01T00:00:15+01:00;4.0\n')
        self.assertEqual(ref, data)

    def test_get_csv_with_struct_no_fieldname(self):

        data = self.db.get_csv(['s3',], include_header=True)
        ref = (b'"Time";"s3 Field 0";"s3 Field 1"\n'
               b'2000-01-01T00:00:00+01:00;100;101\n'
               b'2000-01-01T00:00:03+01:00;200;201\n'
               b'2000-01-01T00:00:04.500000+01:00;300;301\n'
               b'2000-01-01T00:00:11+01:00;400;401\n')
        self.assertEqual(ref, data)

    def test_get_csv_with_struct_and_fieldname(self):
        data = self.db.get_csv(['s4',], include_header=True)
        ref = (b'"Time";"s4 A";"s4 B"\n'
               b'2000-01-01T00:00:00+01:00;100;101\n'
               b'2000-01-01T00:00:03+01:00;200;201\n'
               b'2000-01-01T00:00:04.500000+01:00;300;301\n'
               b'2000-01-01T00:00:11+01:00;400;401\n')
        self.assertEqual(ref, data)

    def test_get_csv_two_float_sensors(self):
        data = self.db.get_csv(['s1', 's2'])
        ref = (b'2000-01-01T00:00:00+01:00;1.0;10.0\n'
               b'2000-01-01T00:00:05+01:00;2.0;20.0\n'
               b'2000-01-01T00:00:06.500000+01:00;2.0;30.0\n'
               b'2000-01-01T00:00:10.100000+01:00;3.0;30.0\n'
               b'2000-01-01T00:00:15+01:00;4.0;40.0\n')
        self.assertEqual(ref, data)

    def test_get_csv_two_float_sensors_header(self):
        data = self.db.get_csv(['s1', 's2'], include_header=True)
        ref = (b'"Time";"s1";"s2"\n'
               b'2000-01-01T00:00:00+01:00;1.0;10.0\n'
               b'2000-01-01T00:00:05+01:00;2.0;20.0\n'
               b'2000-01-01T00:00:06.500000+01:00;2.0;30.0\n'
               b'2000-01-01T00:00:10.100000+01:00;3.0;30.0\n'
               b'2000-01-01T00:00:15+01:00;4.0;40.0\n')
        self.assertEqual(ref, data)

    def test_get_csv_float_and_struct_sensors(self):
        data = self.db.get_csv(['s1', 's2', 's3'])
        ref = (b'2000-01-01T00:00:00+01:00;1.0;10.0;100;101\n'
               b'2000-01-01T00:00:03+01:00;1.0;10.0;200;201\n'
               b'2000-01-01T00:00:04.500000+01:00;1.0;10.0;300;301\n'
               b'2000-01-01T00:00:05+01:00;2.0;20.0;300;301\n'
               b'2000-01-01T00:00:06.500000+01:00;2.0;30.0;300;301\n'
               b'2000-01-01T00:00:10.100000+01:00;3.0;30.0;300;301\n'
               b'2000-01-01T00:00:11+01:00;3.0;30.0;400;401\n'
               b'2000-01-01T00:00:15+01:00;4.0;40.0;400;401\n')
        self.assertEqual(ref, data)

    def test_get_csv_float_and_struct_sensors_with_headers(self):
        data = self.db.get_csv(['s1', 's2', 's3'], include_header=True)
        ref = (b'"Time";"s1";"s2";"s3 Field 0";"s3 Field 1"\n'
               b'2000-01-01T00:00:00+01:00;1.0;10.0;100;101\n'
               b'2000-01-01T00:00:03+01:00;1.0;10.0;200;201\n'
               b'2000-01-01T00:00:04.500000+01:00;1.0;10.0;300;301\n'
               b'2000-01-01T00:00:05+01:00;2.0;20.0;300;301\n'
               b'2000-01-01T00:00:06.500000+01:00;2.0;30.0;300;301\n'
               b'2000-01-01T00:00:10.100000+01:00;3.0;30.0;300;301\n'
               b'2000-01-01T00:00:11+01:00;3.0;30.0;400;401\n'
               b'2000-01-01T00:00:15+01:00;4.0;40.0;400;401\n')
        self.assertEqual(ref, data)

    def test_get_csv_float_and_struct_sensors_with_headers_and_named_fields(self):
        data = self.db.get_csv(['s1', 's2', 's4'], include_header=True)
        ref = (b'"Time";"s1";"s2";"s4 A";"s4 B"\n'
               b'2000-01-01T00:00:00+01:00;1.0;10.0;100;101\n'
               b'2000-01-01T00:00:03+01:00;1.0;10.0;200;201\n'
               b'2000-01-01T00:00:04.500000+01:00;1.0;10.0;300;301\n'
               b'2000-01-01T00:00:05+01:00;2.0;20.0;300;301\n'
               b'2000-01-01T00:00:06.500000+01:00;2.0;30.0;300;301\n'
               b'2000-01-01T00:00:10.100000+01:00;3.0;30.0;300;301\n'
               b'2000-01-01T00:00:11+01:00;3.0;30.0;400;401\n'
               b'2000-01-01T00:00:15+01:00;4.0;40.0;400;401\n')
        self.assertEqual(ref, data)

    def test_get_csv_float_and_string(self):
        data = self.db.get_csv(['s1', 's5'], include_header=True)
        data_ref = (b'"Time";"s1";"s5"\n'
                    b'2000-01-01T00:00:00+01:00;1.0;"test with space"\n'
                    b'2000-01-01T00:00:03+01:00;1.0;"test with , "\n'
                    b'2000-01-01T00:00:04.500000+01:00;1.0;"test"\n'
                    b'2000-01-01T00:00:05+01:00;2.0;"test"\n'
                    b'2000-01-01T00:00:10.100000+01:00;3.0;"test"\n'
                    b'2000-01-01T00:00:11+01:00;3.0;"test with ;"\n'
                    b'2000-01-01T00:00:15+01:00;4.0;"test with ;"\n')
        self.assertEqual(data_ref, data)



class TestcaseExport(EmptyDatabaseMixin, unittest.TestCase):
    """Exports of sensors with different times, read in windows of different sizes"""

    def setUp(self):
        super().setUp()
        self.t0 = datetime(2024, 1, 1).astimezone()
        self.db = LMDBSensorStorage(self.mdb_filename)
        self.write('a', range(0, 41, 2), lambda i: float(i))
        self.write('b', range(5, 31, 3), lambda i: float(100 + i))  # starts and ends inside of a
        self.write('c', (33, 34), lambda i: float(200 + i))  # only at the end
        Sensor(self.mdb_filename, 's', data_format='ff').write_values([self.t(1), self.t(20)], [(1., 2.), (3., 4.)])

    def t(self, seconds):
        return self.t0 + timedelta(seconds=seconds)

    def write(self, name, seconds, value):
        Sensor(self.mdb_filename, name).write_values([self.t(i) for i in seconds], [value(i) for i in seconds])

    def reference(self, names, since=None, until=None, limit=None):
        """rows with the most recent value of each sensor at or before each timestamp of any sensor"""
        items = [self.db[name].items() for name in names]
        timestamps = sorted({t for sensor_items in items for t, _ in sensor_items
                             if (since is None or t >= since) and (until is None or t < until)})
        rows = []
        for t in timestamps[:limit]:
            rows.append((t, [([v for ts, v in sensor_items if ts <= t] or [None])[-1] for sensor_items in items]))
        return rows

    def test_rows(self):
        names = ['a', 'b', 'c', 's']
        for window in (1, 2, 3, 7, 50000):
            for since, until, limit in ((None, None, None), (self.t(6), None, None), (None, self.t(30), None),
                                        (self.t(7), self.t(33), None), (self.t(3), None, 5), (self.t(41), None, None)):
                with self.subTest(window=window, since=since, until=until, limit=limit), \
                        mock.patch.object(self.db, 'EXPORT_WINDOW', window):
                    sensors = [self.db[name] for name in names]
                    rows = list(self.db._export_rows(sensors, since=since, until=until, limit=limit))
                    self.assertEqual(self.reference(names, since, until, limit), rows)

    def test_csv_with_missing_values(self):
        with mock.patch.object(self.db, 'EXPORT_WINDOW', 2):
            csv = self.db.get_csv(['c', 's', 'b'], include_header=True, until=self.t(6)).decode().splitlines()
        self.assertEqual(['"Time";"c";"s Field 0";"s Field 1";"b"',
                          f'{self.t(1).isoformat()};;1.0;2.0;',
                          f'{self.t(5).isoformat()};;1.0;2.0;105.0'], csv)

    def test_json_with_missing_values(self):
        with mock.patch.object(self.db, 'EXPORT_WINDOW', 2), mock.patch.object(self.db, 'EXPORT_WRITE_ROWS', 2):
            data = json.loads(self.db.get_json(['c', 'a', 's'], since=self.t(30), until=self.t(35)))
        self.assertEqual([self.t(i).isoformat() for i in (30, 32, 33, 34)], data['Time'])
        self.assertEqual([None, None, 233.0, 234.0], data['c']['values'])
        self.assertEqual([30.0, 32.0, 32.0, 34.0], data['a']['values'])
        self.assertEqual([[3.0, 4.0]] * 4, data['s']['values'])

    def test_unsupported_arguments(self):
        with self.assertRaises(ValueError):
            self.db.get_csv(['a'], decimate_to_s=60)


if __name__ == "__main__":
    unittest.main()
