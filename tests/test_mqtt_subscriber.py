import unittest
import subprocess
import shutil
import os
import time
import paho.mqtt.client as mqtt
import json
from lmdb_sensor_storage import LMDBSensorStorage
from lmdb_sensor_storage.mqtt_subscriber import on_message
# noinspection PyProtectedMember
from lmdb_sensor_storage.db._manager import manager
import logging
import threading
import pprint
import tempfile
import sys
import datetime
import numpy as np

# noinspection PyPep8Naming
class LocalMQTTServerMixin:
    mosquitto_process: subprocess.Popen

    @classmethod
    def setUpClass(cls):
        mosquitto_exe = shutil.which('mosquitto')
        if mosquitto_exe is None:
            mosquitto_exe = '/usr/sbin/mosquitto'

        mosquitto_conf_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                           'mosquitto.conf')
        cls.mosquitto_process = subprocess.Popen([mosquitto_exe, '-v', '-c', mosquitto_conf_file],
                                                 stdin=subprocess.PIPE,
                                                 stdout=subprocess.PIPE,
                                                 stderr=subprocess.PIPE)

    @classmethod
    def get_client(cls, client_id=None):
        # some versions of paho-mqtt do not have CallbackAPIVersion, some version require it to be set
        if hasattr(mqtt, "CallbackAPIVersion"):
            # noinspection PyArgumentList
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        else:
            client = mqtt.Client(client_id=client_id)
        start_time = time.time()
        res = None
        while time.time() < (start_time + 60):
            try:
                res = client.connect(host='localhost', port=1884)
            except ConnectionRefusedError:
                pass
            if res == mqtt.MQTT_ERR_SUCCESS:
                break
            cls._check_mosquitto_process()

        if res != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f'MQTT connection error {res}')

        return client

    @classmethod
    def _check_mosquitto_process(cls):
        ret = cls.mosquitto_process.poll()
        if ret is not None:
            stdout = cls.mosquitto_process.stdout.read()
            stderr = cls.mosquitto_process.stderr.read()
            raise RuntimeError(f'mosquitto stopped with error code {ret}\n'
                               f'stdout={stdout.decode()}\n'
                               f'stderr={stderr.decode()}\n')

    def setUp(self):
        if hasattr(super(), 'setUp'):
            super().setUp()

        self._check_mosquitto_process()

    @classmethod
    def tearDownClass(cls):
        cls.mosquitto_process.terminate()
        stdout = cls.mosquitto_process.stdout.read()
        stderr = cls.mosquitto_process.stderr.read()
        ret = cls.mosquitto_process.returncode
        if ret != 0:
            print(f'mosquitto stopped\n'
                  f'stdout={stdout.decode()}\n'
                  f'stderr={stderr.decode()}\n')
        else:
            raise RuntimeError(f'mosquitto stopped with error code {ret}\n'
                               f'stdout={stdout.decode()}\n'
                               f'stderr={stderr.decode()}\n')


def publish_function(client: mqtt.Client):

    publish_kwargs = {'qos': 2}

    client.publish('test_float', 1.0, **publish_kwargs)
    client.publish('test_float_as_str', "1.0", **publish_kwargs)
    client.publish('test_csv', "1.0,2,3,4.1", **publish_kwargs)
    client.publish('test_float_list', "[1.0,2,3,4.1]", **publish_kwargs)
    client.publish('test_float_list_of_1', "[1.0]", **publish_kwargs)
    client.publish('test_string', "ON", **publish_kwargs)

    client.publish('test_compatible_datatype_change', 1.0, **publish_kwargs)
    client.publish('test_compatible_datatype_change', "2.0", **publish_kwargs)  # compatible type
    client.publish('test_compatible_datatype_change', 3, **publish_kwargs)  # compatible type
    client.publish('test_incompatible_datatypes', 1, **publish_kwargs)
    # client.publish('test_incompatible_datatypes', "ON", **publish_kwargs)  # incompatible type

    # epoch/value from ESP32_datalogger
    client.publish('test_json_epoch_value', json.dumps({'epoch': 0, 'value': 1.0}), **publish_kwargs)
    client.publish('test_json_epoch_value_list', json.dumps({'epoch': 0, 'value': [1.0, 2, 4.0]}), **publish_kwargs)
    client.publish('test_json_epoch_value_list_of_1', json.dumps({'epoch': 0, 'value': [1.0,]}), **publish_kwargs)
    client.publish('heizung/sensors/U1A_current',
                   '{"epoch":1712040040,"bins":[50],"value":[0.164681]}', **publish_kwargs)
    client.publish('test_json_nan', '{"epoch":1728727628,"value": NaN}', **publish_kwargs)
    client.publish('test_json_nan_on_float', '{"epoch":1728727628,"value": NaN}', **publish_kwargs)
    client.publish('test_json_str_nan', '{"epoch":1728727628,"value": "nan"}', **publish_kwargs)
    client.publish('test_json_nan_array', '{"epoch":1728727628,"value": [1.0, 2.0, NaN, 4.0]}', **publish_kwargs)

    # esphome state message
    client.publish('<TOPIC_PREFIX>/<COMPONENT_TYPE>/<COMONENT_NAME>/state', '1.0', **publish_kwargs)

    # tasmota SENSOR message
    client.publish('tele/tasmota_B56CA8/SENSOR',
                   json.dumps(
                       {"Time": "2022-12-13T16:45:52",
                        "DS18B20-1": {"Id": "01212F95C31B", "Temperature": 0.9},
                        "DS18B20-2": {"Id": "01212F9704FD", "Temperature": -1.8},
                        "DS18B20-3": {"Id": "01212FA56810", "Temperature": 22.8},
                        "BME280": {"Temperature": 23.6, "Humidity": 34.5,
                                   "DewPoint": 7.0, "Pressure": 967.2},
                        "MHZ19B": {"Model": "B", "CarbonDioxide": 645, "Temperature": 27.0},
                        "AS3935": {"Event": 0, "Distance": 0, "Energy": 0, "Stage": 9},
                        "PressureUnit": "hPa",
                        "TempUnit": "C",
                        }),
                   **publish_kwargs
                   )

    # history message from ESP32_datalogger
    client.publish('heizung/c8c9a3c7c50c/history',
                   json.dumps({"len": 9,
                               "epoch": [1670946115, 1670946131, 1670946147, 1670946163, 1670946179, 1670946198,
                                         1670946214, 1670946230, 1670946246],
                               "Vorlauftemperatur Kreis 1": [40.8125, 40.8125, 40.8125, 40.8125, 40.8125, 40.8125,
                                                             40.8125, 40.8125, 40.8125],
                               "Rücklauftemperatur Kreis 1": [33.8125, 33.8125, 33.8125, 33.8125, 33.8125, 33.875,
                                                              33.875, 33.8125, 33.8125],
                               "Vorlauftemperatur Kreis 2": [40.0625, 40.625, 41.3125, 42.0625, 42.6875, 42.9375,
                                                             43, 43, 42.9375],
                               "Rücklauftemperatur Kreis 2": [36.9375, 37, 37.0625, 37.0625, 37, 36.875, 36.75,
                                                              36.625, 36.5625],
                               "Buffertemperatur": [66.75, 66.75, 66.75, 66.75, 66.75, 66.75, 66.8125, 66.8125,
                                                    66.8125],
                               "Vorlaufpumpe Kreis 1": [0, 0, 0, 0, 0, 0, 0, 0, 0]}),
                   **publish_kwargs)

    client.publish('test_history_with_nan/history', 
                   '{"len":4,"epoch":[1728809579,1728809614,1728809649,1728809684],'
                   '"test_history_some_nan":[1.4,NaN,2.0,NaN]}',
                   **publish_kwargs)


    # homeassistant
    client.publish('homeassistant/sensor/sofar_grid_frequency/state', 49.97, **publish_kwargs)
    client.publish('homeassistant/sensor/sofar_grid_frequency/last_updated',
                   "2024-03-30T12:33:24.173890+00:00", **publish_kwargs)
    client.publish('homeassistant/sensor/sofar_grid_frequency/last_changed',
                   "2024-03-30T12:33:24.173890+00:00", **publish_kwargs)
    client.publish('homeassistant/sensor/sofar_grid_frequency/unit_of_measurement', "Hz", **publish_kwargs)
    client.publish('homeassistant/sensor/sofar_grid_frequency/device_class', "frequency", **publish_kwargs)
    client.publish('homeassistant/sensor/sofar_grid_frequency/friendly_name', "sofar Grid Frequency",
                   **publish_kwargs)
    client.publish('homeassistant/binary_sensor/esphome-wohnzimmer/display_on_button/config',
                   '{"name":"Display On Button","stat_t":"esphome-wohnzimmer/binary_sensor/display_on_button/state","avty_t":"esphome-wohnzimmer/status","uniq_id":"ESPbinary_sensordisplay_on_button","dev":{"ids":"943cc607ede0","name":"Wohnzimmer Sensor","sw":"esphome v2024.3.1 Mar 27 2024, 13:49:30","mdl":"esp32dev","mf":"espressif","sa":""}}',
                   **publish_kwargs)
    client.publish('homeassistant/binary_sensor/esphome-wohnzimmer/display_off_button/config',
                   '{"name":"Display Off Button","stat_t":"esphome-wohnzimmer/binary_sensor/display_off_button/state","avty_t":"esphome-wohnzimmer/status","uniq_id":"ESPbinary_sensordisplay_off_button","dev":{"ids":"943cc607ede0","name":"Wohnzimmer Sensor","sw":"esphome v2024.3.1 Mar 27 2024, 13:49:30","mdl":"esp32dev","mf":"espressif","sa":""}}',
                   **publish_kwargs)
    client.publish('homeassistant/binary_sensor/heizungsrelais/k9/config',
                   '{"name":"K9","stat_t":"heizung/binary_sensor/k9/state","avty_t":"heizung/status","uniq_id":"ESPbinary_sensork9","dev":{"ids":"b0b21ca70010","name":"heizungsrelais","sw":"esphome v2024.3.1 Mar 30 2024, 11:19:01","mdl":"esp32dev","mf":"espressif","sa":""}}',
                   **publish_kwargs)


class TestMQTTSubscriber(LocalMQTTServerMixin, unittest.TestCase):
    tempfolder: str
    mdb_filename: str
    db: LMDBSensorStorage

    @classmethod
    def setUpClass(cls):

        super().setUpClass()

        cls.tempfolder = tempfile.mkdtemp()
        cls.mdb_filename = os.path.join(cls.tempfolder, 'unittest.mdb')

        client = cls.get_client(client_id='unittest')

        # subscribe to all topics
        res, mid = client.subscribe('#', qos=2)
        if res != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError

        # setup message handler
        client.user_data_set({'mdb_filename': cls.mdb_filename,
                              'write_value_kwargs': {'only_if_value_changed': True,
                                                     'max_age_seconds': 60,
                                                     }
                              }
                             )
        client.on_message = on_message

        # setup logging
        logger = logging.getLogger('lmdb_sensor_storage.mqtt_subscriber')
        logger.setLevel('DEBUG')
        #client.logger = logger
        
        # manually add stdout handler, otherwise unittest does not caputer logger's output
        logger.addHandler(logging.StreamHandler(sys.stdout))

        # start publishing in background
        thd = threading.Thread(target=publish_function,
                               args=(client,))
        thd.start()

        # start loop
        start_time = time.time()
        while time.time() < start_time + 5:  # len(db) < 40:
            res = client.loop()
            if res != mqtt.MQTT_ERR_SUCCESS:
                print(f'ret={res}')
                break

        thd.join()

        cls.db = LMDBSensorStorage(cls.mdb_filename)
        stats = cls.db.statistics
        cls.db['test_json_nan_on_float'].write_value(datetime.datetime.fromtimestamp(0.0), 1.0)
        pprint.pprint(stats)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        manager.close_all()
        shutil.rmtree(cls.tempfolder)

    def test_compatible_datatype_change(self):
        self.assertEqual(3, len(self.db['test_compatible_datatype_change']))
        self.assertEqual('f', self.db['test_compatible_datatype_change'].data_format)

    def test_string(self):
        self.assertEqual('str', self.db['test_string'].data_format)

    def test_csv(self):
        self.assertEqual('str', self.db['test_csv'].data_format)

    def test_float_list(self):
        self.assertEqual('4f', self.db['test_float_list'].data_format)

    def test_float_list_of_1(self):
        self.assertEqual('1f', self.db['test_float_list_of_1'].data_format)

    def test_float_as_str(self):
        self.assertEqual('f', self.db['test_float_as_str'].data_format)

    def test_homeassistant_last_changed(self):
        self.assertEqual('str', self.db['homeassistant/sensor/sofar_grid_frequency/last_changed'].data_format)

    def test_json_epoch_value(self):
        self.assertEqual('f', self.db['test_json_epoch_value'].data_format)
        self.assertEqual('1f', self.db['heizung/sensors/U1A_current'].data_format)
    
    def test_json_epoch_value_list(self):
        self.assertEqual('3f', self.db['test_json_epoch_value_list'].data_format)
    
    def test_json_epoch_value_list_of_1(self):
        self.assertEqual('1f', self.db['test_json_epoch_value_list_of_1'].data_format)

    def test_json_nan(self):
        self.assertTrue('test_json_nan' in self.db)
        self.assertEqual('f', self.db['test_json_nan'].data_format)
        self.assertTrue(np.isnan(self.db['test_json_nan'].values()[0]))

    def test_json_nan_on_float(self):
        self.assertEqual(2, len(self.db['test_json_nan_on_float']))

    def test_json_string_nan(self):
        self.assertEqual('f', self.db['test_json_str_nan'].data_format)
        self.assertTrue(np.isnan(self.db['test_json_str_nan'].get_last_value()))

    def test_json_nan_array(self):
        vals = self.db['test_json_nan_array'].get_last_value()
        self.assertAlmostEqual(1.0, vals[0])
        self.assertAlmostEqual(2.0, vals[1])
        self.assertTrue(np.isnan(vals[2]))
        self.assertAlmostEqual(4.0, vals[3])
        self.assertEqual('4f', self.db['test_json_nan_array'].data_format)

    def test_history_some_nan(self):
        self.assertEqual(4, len(self.db['test_history_some_nan']))
        self.assertEqual('f',self.db['test_history_some_nan'].data_format)
        vals = self.db['test_history_some_nan'].values()
        self.assertAlmostEqual(1.4, vals[0])
        self.assertTrue(np.isnan(vals[1]))
        self.assertAlmostEqual(2.0, vals[2])
        self.assertTrue(np.isnan(vals[3]))

    def test_incompatible_datatypes(self):
        self.assertEqual(1, len(self.db['test_incompatible_datatypes']))


if __name__ == '__main__':
    unittest.main()
