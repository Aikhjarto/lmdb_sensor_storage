import json
import unittest
from datetime import datetime, timedelta
from lmdb_sensor_storage import Sensor, LMDBSensorStorage
# noinspection PyProtectedMember
from lmdb_sensor_storage.db._manager import manager
# noinspection PyProtectedMember
from lmdb_sensor_storage._parser import as_datetime
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

    def test_get_json(self):

        # test without limits
        json_bytes = self.db.get_json(['s1', 's2'])
        data = json.loads(json_bytes)
        ref = {'Time': ['2000-01-01T00:00:00',
                        '2000-01-01T00:00:05',
                        '2000-01-01T00:00:06.500000',
                        '2000-01-01T00:00:10.100000',
                        '2000-01-01T00:00:15'],
               'notes': [{'2000-01-01T00:00:04': {'long': 'Very long description of toplevel note',
                                                  'short': 'Toplevel Note'}}],
               's1': {"values": [1.0, 2.0, 2.0, 3.0, 4.0]},
               's2': {"values": [10.0, 20.0, 30.0, 30.0, 40.0]}}
        self.assertEqual(ref, data)

        # test with since and until
        json_bytes = self.db.get_json(['s1', 's2'],
                                      since=self.reference_date + timedelta(seconds=2),
                                      until=self.reference_date + timedelta(seconds=11))

        data = json.loads(json_bytes)
        ref = {'Time': [(self.reference_date + timedelta(seconds=5)).isoformat(),
                        (self.reference_date + timedelta(seconds=6.5)).isoformat(),
                        (self.reference_date + timedelta(seconds=10.1)).isoformat()],
               'notes': [{'2000-01-01T00:00:04': {'long': 'Very long description of toplevel note',
                                                  'short': 'Toplevel Note'}}],
               's1': {"values": [2.0, 2.0, 3.0]},
               's2': {"values": [20.0, 30.0, 30.0]}}
        self.assertEqual(ref, data)

    def test_get_json_with_notes(self):
        json_bytes = self.db.get_json(['s1', 's2', 's4'],
                                      since=self.reference_date + timedelta(seconds=2),
                                      until=self.reference_date + timedelta(seconds=11))

        data = json.loads(json_bytes)
        ref = {'Time': ['2000-01-01T00:00:03',
                        '2000-01-01T00:00:04.500000',
                        '2000-01-01T00:00:05',
                        '2000-01-01T00:00:06.500000',
                        '2000-01-01T00:00:10.100000'],
               'notes': [{'2000-01-01T00:00:04': {'long': 'Very long description of toplevel note',
                                                  'short': 'Toplevel Note'}}],
               's1': {'values': [1.0, 1.0, 2.0, 2.0, 3.0]},
               's2': {'values': [10.0, 10.0, 20.0, 30.0, 30.0]},
               's4': {'metadata': {'field_names': ['A', 'B']},
                      'notes': [{'2000-01-01T00:00:07': {'long': 'Very long description of sensorlevel note',
                                                         'short': 'Sensorlevel Note'}}],
                      'values': [[200, 201], [300, 301], [300, 301], [300, 301], [300, 301]]}}
        self.assertEqual(ref, data)


class TestcaseLMDBSensorStorageGetCSV(DataMixin, unittest.TestCase):

    def setUp(self):
        super().setUp()

    def get_csv_empty(self):
        db = LMDBSensorStorage(self.mdb_filename)
        with self.assertRaises(KeyError):
            db.get_csv('s0')

    def test_get_csv_with_float(self):
        # test without limits and without header
        data = self.db.get_csv(['s1',])
        ref = (b'2000-01-01T00:00:00;1.0\n'
               b'2000-01-01T00:00:05;2.0\n'
               b'2000-01-01T00:00:10.100000;3.0\n'
               b'2000-01-01T00:00:15;4.0\n')
        self.assertEqual(ref, data)

        # test without limits and without header
        data = self.db.get_csv(['s1',], include_header=True)
        ref = (b'"Time";"s1"\n'
               b'2000-01-01T00:00:00;1.0\n'
               b'2000-01-01T00:00:05;2.0\n'
               b'2000-01-01T00:00:10.100000;3.0\n'
               b'2000-01-01T00:00:15;4.0\n')
        self.assertEqual(ref, data)

        # test without limits and without header
        data = self.db.get_csv(['s1',], include_header=True)
        ref = (b'"Time";"s1"\n'
               b'2000-01-01T00:00:00;1.0\n'
               b'2000-01-01T00:00:05;2.0\n'
               b'2000-01-01T00:00:10.100000;3.0\n'
               b'2000-01-01T00:00:15;4.0\n')
        self.assertEqual(ref, data)

    def test_get_csv_with_struct_no_fieldname(self):

        data = self.db.get_csv(['s3',], include_header=True)
        ref = (b'"Time";"s3 Field 0";"s3 Field 1"\n'
               b'2000-01-01T00:00:00;100;101\n'
               b'2000-01-01T00:00:03;200;201\n'
               b'2000-01-01T00:00:04.500000;300;301\n'
               b'2000-01-01T00:00:11;400;401\n')
        self.assertEqual(ref, data)

    def test_get_csv_with_struct_and_fieldname(self):
        data = self.db.get_csv(['s4',], include_header=True)
        ref = (b'"Time";"s4 A";"s4 B"\n'
               b'2000-01-01T00:00:00;100;101\n'
               b'2000-01-01T00:00:03;200;201\n'
               b'2000-01-01T00:00:04.500000;300;301\n'
               b'2000-01-01T00:00:11;400;401\n')
        self.assertEqual(ref, data)

    def test_get_csv_two_float_sensors(self):
        data = self.db.get_csv(['s1', 's2'])
        ref = (b'2000-01-01T00:00:00;1.0;10.0\n'
               b'2000-01-01T00:00:05;2.0;20.0\n'
               b'2000-01-01T00:00:06.500000;2.0;30.0\n'
               b'2000-01-01T00:00:10.100000;3.0;30.0\n'
               b'2000-01-01T00:00:15;4.0;40.0\n')
        self.assertEqual(ref, data)

    def test_get_csv_two_float_sensors_header(self):
        data = self.db.get_csv(['s1', 's2'], include_header=True)
        ref = (b'"Time";"s1";"s2"\n'
               b'2000-01-01T00:00:00;1.0;10.0\n'
               b'2000-01-01T00:00:05;2.0;20.0\n'
               b'2000-01-01T00:00:06.500000;2.0;30.0\n'
               b'2000-01-01T00:00:10.100000;3.0;30.0\n'
               b'2000-01-01T00:00:15;4.0;40.0\n')
        self.assertEqual(ref, data)

    def test_get_csv_float_and_struct_sensors(self):
        data = self.db.get_csv(['s1', 's2', 's3'])
        ref = (b'2000-01-01T00:00:00;1.0;10.0;100;101\n'
               b'2000-01-01T00:00:03;1.0;10.0;200;201\n'
               b'2000-01-01T00:00:04.500000;1.0;10.0;300;301\n'
               b'2000-01-01T00:00:05;2.0;20.0;300;301\n'
               b'2000-01-01T00:00:06.500000;2.0;30.0;300;301\n'
               b'2000-01-01T00:00:10.100000;3.0;30.0;300;301\n'
               b'2000-01-01T00:00:11;3.0;30.0;400;401\n'
               b'2000-01-01T00:00:15;4.0;40.0;400;401\n')
        self.assertEqual(ref, data)

    def test_get_csv_float_and_struct_sensors_with_headers(self):
        data = self.db.get_csv(['s1', 's2', 's3'], include_header=True)
        ref = (b'"Time";"s1";"s2";"s3 Field 0";"s3 Field 1"\n'
               b'2000-01-01T00:00:00;1.0;10.0;100;101\n'
               b'2000-01-01T00:00:03;1.0;10.0;200;201\n'
               b'2000-01-01T00:00:04.500000;1.0;10.0;300;301\n'
               b'2000-01-01T00:00:05;2.0;20.0;300;301\n'
               b'2000-01-01T00:00:06.500000;2.0;30.0;300;301\n'
               b'2000-01-01T00:00:10.100000;3.0;30.0;300;301\n'
               b'2000-01-01T00:00:11;3.0;30.0;400;401\n'
               b'2000-01-01T00:00:15;4.0;40.0;400;401\n')
        self.assertEqual(ref, data)

    def test_get_csv_float_and_struct_sensors_with_headers_and_named_fields(self):
        data = self.db.get_csv(['s1', 's2', 's4'], include_header=True)
        ref = (b'"Time";"s1";"s2";"s4 A";"s4 B"\n'
               b'2000-01-01T00:00:00;1.0;10.0;100;101\n'
               b'2000-01-01T00:00:03;1.0;10.0;200;201\n'
               b'2000-01-01T00:00:04.500000;1.0;10.0;300;301\n'
               b'2000-01-01T00:00:05;2.0;20.0;300;301\n'
               b'2000-01-01T00:00:06.500000;2.0;30.0;300;301\n'
               b'2000-01-01T00:00:10.100000;3.0;30.0;300;301\n'
               b'2000-01-01T00:00:11;3.0;30.0;400;401\n'
               b'2000-01-01T00:00:15;4.0;40.0;400;401\n')
        self.assertEqual(ref, data)


if __name__ == "__main__":
    unittest.main()
