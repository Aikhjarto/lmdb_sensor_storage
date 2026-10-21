"""
Tests running the MQTT subscriber as process against its own mosquitto broker.
"""
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import paho.mqtt.client as mqtt
import paho.mqtt.publish

from lmdb_sensor_storage.db.sensor_db import Sensors
from lmdb_sensor_storage.mqtt_subscriber import client_id, create_client, on_connect, setup_parser
from tests import EmptyDatabaseMixin

MOSQUITTO = shutil.which('mosquitto') or '/usr/sbin/mosquitto'

# ports of the brokers started by these tests, different to the one of test_mqtt_subscriber
PORT = 1885
TLS_PORT = 8885


def wait_for(condition, timeout=30, interval=0.2):
    """Wait until `condition()` returns True, or `timeout` seconds passed. Returns the last result."""
    end = time.time() + timeout
    while True:
        result = condition()
        if result or time.time() > end:
            return result
        time.sleep(interval)


def port_open(port):
    try:
        with socket.create_connection(('localhost', port), timeout=1):
            return True
    except OSError:
        return False


@unittest.skipUnless(os.path.exists(MOSQUITTO), 'mosquitto is not installed')
class SubscriberProcessMixin(EmptyDatabaseMixin):

    def setUp(self):
        super().setUp()
        self.broker = None
        self.subscriber = None
        self.subscriber_log = os.path.join(self.tempfolder, 'subscriber.log')

    def tearDown(self):
        for process in (self.subscriber, self.broker):
            if process is not None and process.poll() is None:
                process.send_signal(signal.SIGCONT)  # in case it was stopped
                process.terminate()
                process.wait(timeout=10)
        super().tearDown()

    def start_broker(self, port, config=''):
        conf_filename = os.path.join(self.tempfolder, 'mosquitto.conf')
        with open(conf_filename, 'w') as f:
            f.write(f'listener {port}\nallow_anonymous true\n{config}')
        self.broker = subprocess.Popen([MOSQUITTO, '-c', conf_filename],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.assertTrue(wait_for(lambda: port_open(port)), 'mosquitto did not start')

    def stop_broker(self):
        self.broker.terminate()
        self.broker.wait(timeout=10)

    def start_subscriber(self, *args):
        with open(self.subscriber_log, 'w') as log:
            self.subscriber = subprocess.Popen(
                [sys.executable, '-u', '-m', 'lmdb_sensor_storage.mqtt_subscriber',
                 '--mdb-filename', self.mdb_filename, '--mqtt-broker', 'localhost',
                 '--mqtt-subscribe-topic', 'test/#', *args],
                env={**os.environ, 'PYTHONPATH': os.pathsep.join(sys.path)},
                stdout=log, stderr=subprocess.STDOUT)

    def values(self, sensor_name):
        sensors = Sensors(self.mdb_filename)
        if sensor_name not in sensors:
            return []
        return sensors[sensor_name].values()

    def publish_until_stored(self, value, port, timeout=60, **publish_kwargs):
        """
        Publish `value` repeatedly to topic test/value until the subscriber stored it, since the subscriber
        might not be (re-)connected yet.
        """
        def stored():
            try:
                paho.mqtt.publish.single('test/value', value, qos=1, hostname='localhost', port=port,
                                         **publish_kwargs)
            except OSError:
                return False
            return wait_for(lambda: value in self.values('test/value'), timeout=1)

        if not wait_for(stored, timeout=timeout, interval=0):
            with open(self.subscriber_log) as f:
                self.fail(f'{value} was not stored, subscriber log:\n{f.read()}')


class TestSubscriberReconnect(SubscriberProcessMixin, unittest.TestCase):

    def test_resubscribe_after_broker_restart(self):
        self.start_broker(PORT)
        self.start_subscriber('--mqtt-broker-port', str(PORT))
        self.publish_until_stored(1.0, PORT)

        # broker restart, e.g. during a reboot or network outage, drops all subscriptions
        self.stop_broker()
        self.start_broker(PORT)

        self.publish_until_stored(2.0, PORT)
        self.assertIsNone(self.subscriber.poll(), 'subscriber stopped')
        with open(self.subscriber_log) as f:
            self.assertIn('Disconnected from MQTT broker', f.read())

    def test_messages_while_disconnected_are_delivered(self):
        self.start_broker(PORT)
        self.start_subscriber('--mqtt-broker-port', str(PORT), '--mqtt-client-id', 'test-subscriber')
        self.publish_until_stored(1.0, PORT)

        # Disconnect the subscriber while the broker keeps running: freeze it, so it cannot reconnect, and let another
        # client take over its session, which closes the connection of the subscriber. Then disconnect the other
        # client, which keeps the session, but leaves it without connection.
        self.subscriber.send_signal(signal.SIGSTOP)
        properties = mqtt.Properties(mqtt.PacketTypes.CONNECT)
        properties.SessionExpiryInterval = 3600
        other = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id='test-subscriber', protocol=mqtt.MQTTv5)
        other.connect('localhost', PORT, clean_start=False, properties=properties)
        other.loop_start()
        self.assertTrue(wait_for(other.is_connected))
        other.disconnect()
        other.loop_stop()

        paho.mqtt.publish.single('test/value', 2.0, qos=1, hostname='localhost', port=PORT)
        self.subscriber.send_signal(signal.SIGCONT)

        # the message is not retained, thus it is only delivered via the session of the subscriber
        if not wait_for(lambda: 2.0 in self.values('test/value')):
            with open(self.subscriber_log) as f:
                self.fail(f'message published while disconnected was not delivered, subscriber log:\n{f.read()}')


class TestSubscriberTLS(SubscriberProcessMixin, unittest.TestCase):

    def openssl(self, *args):
        subprocess.run(['openssl', *args], cwd=self.tempfolder, check=True, capture_output=True)

    def create_certificates(self):
        """CA, and server and client certificates signed by the CA"""
        self.openssl('req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', 'ca.key', '-out', 'ca.crt',
                     '-days', '1', '-subj', '/CN=test CA')
        with open(os.path.join(self.tempfolder, 'server.ext'), 'w') as f:
            f.write('subjectAltName=DNS:localhost\n')
        for name, ext in (('server', ['-extfile', 'server.ext']), ('client', [])):
            self.openssl('req', '-newkey', 'rsa:2048', '-nodes', '-keyout', f'{name}.key', '-out', f'{name}.csr',
                         '-subj', f'/CN={"localhost" if name == "server" else "client"}')
            self.openssl('x509', '-req', '-in', f'{name}.csr', '-CA', 'ca.crt', '-CAkey', 'ca.key',
                         '-CAcreateserial', '-out', f'{name}.crt', '-days', '1', *ext)
        return {name: os.path.join(self.tempfolder, name) for name in
                ('ca.crt', 'server.crt', 'server.key', 'client.crt', 'client.key')}

    @unittest.skipUnless(shutil.which('openssl'), 'openssl is not installed')
    def test_tls_with_client_certificate(self):
        files = self.create_certificates()
        self.start_broker(TLS_PORT, f'cafile {files["ca.crt"]}\n'
                                    f'certfile {files["server.crt"]}\n'
                                    f'keyfile {files["server.key"]}\n'
                                    f'require_certificate true\n')
        self.start_subscriber('--mqtt-broker-port', str(TLS_PORT), '--mqtt-ca-file', files['ca.crt'],
                              '--mqtt-cert-file', files['client.crt'], '--mqtt-key-file', files['client.key'])

        self.publish_until_stored(1.0, TLS_PORT, tls={'ca_certs': files['ca.crt'],
                                                           'certfile': files['client.crt'],
                                                           'keyfile': files['client.key']})


class TestCreateClient(unittest.TestCase):

    def parse(self, *args):
        return setup_parser().parse_args(['--mdb-filename', 'x.mdb', '--mqtt-subscribe-topic', 'a/#', *args])

    def test_qos(self):
        for args, qos in (((), 1), (('--mqtt-qos', '2'), 2), (('--mqtt-qos', '0'), 0)):
            client = create_client(self.parse(*args))
            mock_client = mock.Mock()
            mock_client.subscribe.return_value = (mqtt.MQTT_ERR_SUCCESS, 1)
            on_connect(mock_client, client.user_data_get(), mqtt.ConnectFlags(session_present=False),
                       mqtt.ReasonCode(mqtt.PacketTypes.CONNACK, 'Success'), None)
            mock_client.subscribe.assert_called_once_with('a/#', qos=qos)

    def test_client_id(self):
        hostname = socket.gethostname()
        self.assertEqual(f'lmdb_sensor_storage-{hostname}-x.mdb', client_id(self.parse()))
        self.assertEqual('my-id', client_id(self.parse('--mqtt-client-id', 'my-id')))

        with tempfile.TemporaryDirectory() as folder:
            settings_filename = os.path.join(folder, 'settings.txt')
            with open(settings_filename, 'w') as f:
                f.write('--mqtt-broker localhost\n')
            self.assertEqual(f'lmdb_sensor_storage-{hostname}-settings.txt',
                             client_id(self.parse('--settings-filename', settings_filename)))

    def test_tls(self):
        with mock.patch.object(mqtt.Client, 'tls_set') as tls_set:
            create_client(self.parse())
            tls_set.assert_not_called()
            create_client(self.parse('--mqtt-tls'))
            tls_set.assert_called_once_with(ca_certs=None, certfile=None, keyfile=None)


if __name__ == '__main__':
    unittest.main()
