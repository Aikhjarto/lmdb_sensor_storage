#!/bin/env python3
import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from lmdb_sensor_storage.db.sensor_db import Sensor
import json
import argparse
import lmdb
import logging
import os
import re
import socket
from lmdb_sensor_storage._parser import add_logging, setup_logging, add_mqtt, LoadFromFile, as_datetime

logger = logging.getLogger('lmdb_sensor_storage.mqtt_subscriber')

# The broker keeps the session, i.e., subscriptions and messages with QoS > 0, for this time after a disconnect, so
# messages published while the subscriber was disconnected are delivered after reconnecting.
SESSION_EXPIRY_SECONDS = 24 * 3600


# <TOPIC_PREFIX>/<COMPONENT_TYPE>/<COMPONENT_NAME>/state, where TOPIC_PREFIX may contain slashes
re_esphome = re.compile(r'.+/.+/.+/state')
# tele/<topic>/SENSOR, for the default FullTopic of tasmota only, since sensor names are derived from <topic>
re_tasmota = re.compile(r'tele/[^/]+/SENSOR')


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


def _on_message(mqtt_client, userdata, message):
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
        try:
            data = json.loads(message.payload.decode(), parse_constant=float)
        except json.decoder.JSONDecodeError:
            logger.error(f'Cannot decode message {message.payload.decode()} as json')
            return

        dates = [datetime.fromtimestamp(epoch, tz=timezone.utc) for epoch in data['epoch']]
        del data['len']
        del data['epoch']
        for sensor_name in data.keys():
            if isinstance(data[sensor_name], dict):
                # e.g. "spectrum" in the example above, iterating it would store its keys as values
                logger.warning("skipping '%s' of message on topic '%s': nested dicts in history messages are not "
                               "implemented", sensor_name, message.topic)
                continue
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
        # MQTT Message from https://esphome.io/components/mqtt
        # <TOPIC_PREFIX>/<COMPONENT_TYPE>/<COMPONENT_NAME>/state
        # TOPIC_PREFIX is usually the hostname
        sensor_name, _ = message.topic.rsplit('/', 1)
        date = datetime.now(timezone.utc)
        s = Sensor(userdata['mdb_filename'], sensor_name)
        data = message.payload.decode()
        s.write_value(date, data, **userdata['write_value_kwargs'])

    elif re_tasmota.fullmatch(message.topic):
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
            data = json.loads(message.payload.decode(), parse_constant=float)
        except json.decoder.JSONDecodeError:
            logger.error('Cannot decode message {message.payload.decode()} as json')
            return

        # tasmota sends local time without timezone
        date = as_datetime(data.pop('Time'))
        if date.year < 2016:
            date = datetime.now(timezone.utc)

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
        date = datetime.now(timezone.utc)

        # Note: don't use np.fromstring to decode payload data
        # There are many bugreports that, in some versions of numpy, it does not raise a ValueError on inappropriate
        # data, but returns something wrong or incomplete, e.g. https://github.com/numpy/numpy/issues/18435
        # np.fromstring("2024-03-30T12:33:24.173890+00:00", dtype=float, sep=',') returns array([2024.])
        # np.fromstring('"Hz"', dtype=float, sep=',') returns an array with zero elements.

        try:
            # json.loads can decode single values of floats or strings, lists or dictionaries
            data = json.loads(msg, parse_constant=float)
        except json.decoder.JSONDecodeError:
            # non-json
            data = msg

        if isinstance(data, dict):
            # decode epoch/value from ESP32_datalogger
            if "epoch" in data and "value" in data:
                # {"epoch":1670946356,"value":62.875}
                date = datetime.fromtimestamp(float(data['epoch']), tz=timezone.utc)
                if date.year < 2016:
                    # sensor could not sync it's time
                    date = datetime.now(timezone.utc)
                data = data['value']

        s = Sensor(userdata['mdb_filename'], message.topic)
        s.write_value(date, data, **userdata['write_value_kwargs'])


def on_message(mqtt_client, userdata, message):
    """Store a message; skip a malformed message or a value that does not fit the sensor's type, e.g.
    'unavailable' or 'unknown' from Home Assistant, instead of crashing.
    Database errors are raised, since they will not go away by skipping messages."""
    try:
        _on_message(mqtt_client, userdata, message)
    except lmdb.Error:
        raise
    except Exception:
        logger.exception("skipping message on topic '%s' with payload '%s'",
                         message.topic, message.payload[:100])


def on_connect(mqtt_client, userdata, flags, reason_code, properties):
    """(Re-)subscribe on every connect, since a reconnect with a clean session drops all subscriptions."""
    if reason_code.is_failure:
        logger.error('MQTT connect failed: %s', reason_code)
        return
    logger.warning('Connected to MQTT broker as %s, session present: %s, subscribing to %s',
                   userdata['client_id'], flags.session_present, userdata['topics'])
    for topic in userdata['topics']:
        res, mid = mqtt_client.subscribe(topic, qos=userdata['qos'])
        if res != mqtt.MQTT_ERR_SUCCESS:
            logger.error(f'Subscribe to topic "{topic}" failed with error {res}')


def on_disconnect(mqtt_client, userdata, flags, reason_code, properties):
    logger.warning('Disconnected from MQTT broker: %s', reason_code)


def use_tls(args: argparse.Namespace) -> bool:
    return bool(args.mqtt_tls or args.mqtt_ca_file or args.mqtt_cert_file)


def client_id(args: argparse.Namespace) -> str:
    if args.mqtt_client_id:
        return args.mqtt_client_id
    name = args.settings_filename if args.settings_filename else args.mdb_filename
    return f'lmdb_sensor_storage-{socket.gethostname()}-{os.path.basename(name)}'


def create_client(args: argparse.Namespace) -> mqtt.Client:
    """Create a client according to the command line arguments, ready to connect to the broker."""
    _client_id = client_id(args)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=_client_id, protocol=mqtt.MQTTv5)
    client.username_pw_set(args.mqtt_username, args.mqtt_password)
    if use_tls(args):
        client.tls_set(ca_certs=args.mqtt_ca_file, certfile=args.mqtt_cert_file, keyfile=args.mqtt_key_file)
        if args.mqtt_tls_insecure:
            client.tls_insecure_set(True)
    client.user_data_set({'mdb_filename': args.mdb_filename,
                          'client_id': _client_id,
                          'topics': args.mqtt_subscribe_topic,
                          'qos': args.mqtt_qos,
                          'write_value_kwargs': {'only_if_value_changed': True,
                                                 'max_age_seconds': args.max_age_seconds,
                                                 }
                          }
                         )
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    return client


def broker_port(args: argparse.Namespace) -> int:
    if args.mqtt_broker_port is not None:
        return args.mqtt_broker_port
    return 8883 if use_tls(args) else 1883


def connect(client: mqtt.Client, args: argparse.Namespace):
    """
    Connect to the broker. The first connect starts a new session, which removes subscriptions of a previous run,
    e.g., of topics no longer configured. Reconnects resume the session.
    """
    properties = mqtt.Properties(mqtt.PacketTypes.CONNECT)
    properties.SessionExpiryInterval = SESSION_EXPIRY_SECONDS
    res = client.connect(host=args.mqtt_broker, port=broker_port(args),
                         clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY, properties=properties)
    if res != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError(f'MQTT connection error {res}')


if __name__ == '__main__':

    p = setup_parser()
    args = p.parse_args()
    setup_logging(logger, syslog=args.syslog, loglevel=args.loglevel)
    setup_logging(logging.getLogger('lmdb_sensor_storage.db'), syslog=args.syslog, loglevel=args.loglevel)

    logger.debug('Starting with parameters %s',
                 {key: '***' if key == 'mqtt_password' and val else val for key, val in args.__dict__.items()})

    client = create_client(args)
    connect(client, args)
    client.loop_forever()
