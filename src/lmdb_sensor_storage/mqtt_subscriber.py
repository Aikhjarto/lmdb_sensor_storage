#!/bin/env python3
import paho.mqtt.client as mqtt
from datetime import datetime
from lmdb_sensor_storage.db.sensor_db import Sensor
import json
import argparse
import logging
import numpy as np
from lmdb_sensor_storage._parser import add_logging, setup_logging, add_mqtt, LoadFromFile, fromisoformat
import regex

logger = logging.getLogger('lmdb_sensor_storage.mqtt_subscriber')


re_esphome = regex.compile(r'^.+\/.+\/.+\/state$')


def setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument('--settings-filename', type=open, action=LoadFromFile,
                        help='Read parameters from a file')

    parser.add_argument('--mdb-filename', type=str)

    add_mqtt(parser)

    parser.add_argument('--mqtt-subscribe-topic', type=str, action='append')

    parser.add_argument('--max-age-seconds', type=float, default=None)

    add_logging(parser)

    return parser


def on_message(mqtt_client, userdata, message):
    """
    Parameters
    ----------
    mqtt_client: mqtt.Client
        the client instance for this callback
    userdata: dict
        the private user data as set in Client() or userdata_set()
    message: mqtt.MQTTMessage
        This is a class with members topic, payload, qos, retain.

    """
    logger.info("message topic='%s', payload '%s'",
                message.topic, str(message.payload.decode("utf-8")))
    # print("message qos=", message.qos)
    # print("message retain flag=", message.retain)

    if message.topic.endswith('/history'):
        # History message, e.g. from https://github.com/Aikhjarto/ESP32_datalogger
        # heizung/c8c9a3c7c50c/history {"len":9,"epoch":[1670946115,1670946131,1670946147,1670946163,1670946179,1670946198,1670946214,1670946230,1670946246],"Vorlauftemperatur Kreis 1":[40.8125,40.8125,40.8125,40.8125,40.8125,40.8125,40.8125,40.8125,40.8125],"Rücklauftemperatur Kreis 1":[33.8125,33.8125,33.8125,33.8125,33.8125,33.875,33.875,33.8125,33.8125],"Vorlauftemperatur Kreis 2":[40.0625,40.625,41.3125,42.0625,42.6875,42.9375,43,43,42.9375],"Rücklauftemperatur Kreis 2":[36.9375,37,37.0625,37.0625,37,36.875,36.75,36.625,36.5625],"Buffertemperatur":[66.75,66.75,66.75,66.75,66.75,66.75,66.8125,66.8125,66.8125],"Vorlaufpumpe Kreis 1":[0,0,0,0,0,0,0,0,0]}
        # heizung/b0b21ca893f8/history {"len":1,"epoch":[1709279498],"Current Sensors Temperature":[27.75],"spectrum":{"len":1,"frequencies":[50],"U1A_current":[[0.0281364]],"U1B_current":[[0.282819]],"U2_current":[[0.00199494]],"U3_current":[[0.000936895]],"U4_current":[[0.000550473]],"U5_current":[[0.000337011]]}}
        # heizung/34865d44bcd8/history {"len":9,"epoch":[1709534436,1709534452,1709534469,1709534486,1709534502,1709534521,1709534537,1709534554,1709534571],"Photoresistor":[0,0,0,0,0,0,0,0,0],"RGBC":[[1,1,1,1],[1,1,1,1],[1,1,1,1],[1,1,1,1],[1,1,1,1],[1,1,1,1],[1,1,1,1],[1,1,1,1],[1,1,1,1]]}
        data = json.loads(message.payload.decode())
        dates = [datetime.fromtimestamp(epoch) for epoch in data['epoch']]
        del data['len']
        del data['epoch']
        for sensor_name in data.keys():
            for date, value in zip(dates, data[sensor_name]):
                if date.year > 2016:
                    try:
                        if len(value) == 1:
                            value = value[0]
                    except TypeError:
                        # value has no length, so nothing to do
                        pass

                    s = Sensor(userdata['mdb_filename'], sensor_name)
                    s.write_value(date, value, **userdata['write_value_kwargs'])
    elif re_esphome.fullmatch(message.topic):
        # <TOPIC_PREFIX>/<COMPONENT_TYPE>/<COMONENT_NAME>/state
        # TOPIC_PREFIX is usally the hostname
        sensor_name, _ = message.topic.rsplit('/', 1)
        date = datetime.now()
        s = Sensor(userdata['mdb_filename'], sensor_name)
        data = message.payload.decode()
        s.write_value(date, data, **userdata['write_value_kwargs'])

    elif regex.match(r'tele\/.+\/SENSOR', message.topic):
        # tasmaota sensor data, see https://tasmota.github.io/docs/MQTT/#examples
        # tele/tasmota_B56CA8/SENSOR
        # {"Time": "2022-12-13T16:45:52",
        #  "DS18B20-1": {"Id": "01212F95C31B", "Temperature": 0.9},
        #  "DS18B20-2": {"Id": "01212F9704FD", "Temperature": -1.8},
        #  "DS18B20-3": {"Id": "01212FA56810", "Temperature": 22.8},
        #  "BME280": {"Temperature": 23.6, "Humidity": 34.5, "DewPoint": 7.0, "Pressure": 967.2},
        #  "MHZ19B": {"Model": "B", "CarbonDioxide": 645, "Temperature": 27.0},
        #  "AS3935": {"Event": 0, "Distance": 0, "Energy": 0, "Stage": 9},
        #  "PressureUnit": "hPa",
        #  "TempUnit": "C"
        # }

        try:
            data = json.loads(message.payload.decode())
        except json.decoder.JSONDecodeError:
            logger.error('Cannot decode message {message.payload.decode()} as json')
            return

        date = fromisoformat(data.pop('Time'))
        if date.year < 2016:
            date = datetime.now()

        for sensor in data.keys():
            if sensor.startswith('DS18B20'):
                # tasmato names for DS18B20-X are not consistent, thus use unique sensor id sensor name
                sensor_name = "DS18B20-" + data[sensor]['Id']
                value = data[sensor]['Temperature']
                s = Sensor(userdata['mdb_filename'], sensor_name)
                s.write_value(date, float(value),  **userdata['write_value_kwargs'])
            elif sensor.startswith('AS3935') and int(data[sensor]['Event']) > 0:
                # don't log no-lightning-events
                sensor_name = message.topic.split('/')[1] + '_' + sensor
                s = Sensor(userdata['mdb_filename'], sensor_name)
                s.write_value(date,
                              (float(data[sensor]['Event']),
                               float(data[sensor]['Distance']),
                               float(data[sensor]['Energy']),
                               float(data[sensor]['Stage'])),
                              **userdata['write_value_kwargs'])

            elif callable(getattr(data[sensor], 'items', None)):
                # TODO: don't split up values to multiple sensors, since a timestamp is then generated for each value
                #   Better: store as struct/json and use Sensor.metadata['field_names']
                sensor_name = message.topic.split('/')[1] + '_' + sensor + '_'
                for key, value in data[sensor].items():
                    s = Sensor(userdata['mdb_filename'], sensor_name + key)
                    s.write_value(date, value, **userdata['write_value_kwargs'])
            else:
                sensor_name = message.topic.split('/')[1] + '_' + sensor
                s = Sensor(userdata['mdb_filename'], sensor_name)
                s. write_value(date, data[sensor], **userdata['write_value_kwargs'])

    else:
        msg = message.payload.decode()
        date = data = None

        try:
            json_dict = json.loads(msg)
        except json.decoder.JSONDecodeError:
            # non-json
            json_dict = None

        if isinstance(json_dict, dict):
            # json
            if "epoch" in json_dict and "value" in json_dict:
                # {"epoch":1670946356,"value":62.875}
                date = datetime.fromtimestamp(float(json_dict['epoch']))
                if date.year < 2016:
                    # sensor could not sync it's time
                    date = datetime.now()
                data = json_dict['value']
        else:
            date = datetime.now()
            try:
                data = float(msg)
            except ValueError:
                data = np.fromstring(msg[1:-1], dtype=float, sep=',')

        if date is not None and data is not None:

            try:
                if len(data) == 1:
                    data = data[0]
            except TypeError:
                # value has no length, so nothing to do
                pass

            s = Sensor(userdata['mdb_filename'], message.topic)
            s.write_value(date, data, **userdata['write_value_kwargs'])


if __name__ == '__main__':

    p = setup_parser()
    args = p.parse_args()
    setup_logging(logger, syslog=args.syslog, loglevel=args.loglevel)
    setup_logging(logging.getLogger('lmdb_sensor_storage.db'), syslog=args.syslog, loglevel=args.loglevel)

    logger.debug('Starting with parameters %s', args.__dict__)

    client = mqtt.Client()
    client.username_pw_set(args.mqtt_username, args.mqtt_password)
    res = client.connect(host=args.mqtt_broker, port=args.mqtt_broker_port)
    if res != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError(f'MQTT connection error {res}')

    # subscribe to topics
    for topic in args.mqtt_subscribe_topic:
        res, mid = client.subscribe(topic)
        if res != mqtt.MQTT_ERR_SUCCESS:
            logger.error(f'Subscribe to topic "{topic}" failed with error {res}')
    client.user_data_set({'mdb_filename': args.mdb_filename,
                          'write_value_kwargs': {'only_if_value_changed': True,
                                                 'max_age_seconds': args.max_age_seconds,
                                                 }
                          }
                         )
    client.on_message = on_message
    client.loop_forever()
