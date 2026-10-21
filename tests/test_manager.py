import os
import subprocess
import sys
import threading
import time
import unittest
from unittest import mock
import lmdb
from . import EmptyDatabaseMixin
from lmdb_sensor_storage import LMDBSensorStorage
# noinspection PyProtectedMember
from lmdb_sensor_storage.db._manager import manager
from datetime import datetime, timezone


class TestManager(EmptyDatabaseMixin, unittest.TestCase):
    def test_number_subdatabases(self):
        # Exceed the maximum number of sub-databases several times. Small limits make this fast, since the cost of
        # transactions in lmdb grows with the limit.
        storage = LMDBSensorStorage(self.mdb_filename)
        ref_date = datetime.now(timezone.utc)
        ref_data = []
        with mock.patch.object(manager, 'DEFAULT_MAXDBS', 8), mock.patch.object(manager, 'MAX_DBS_STEP', 8), \
                self.assertLogs('lmdb_sensor_storage.manager', level='WARNING') as logs:
            # each sensor has 4 sub-databases
            for i in range(10):
                ref_data.append((ref_date, i))
                storage[f"{i:05}"][ref_date] = i

            data = [sensor.items()[0] for sensor in storage.get_non_empty_sensors()]
        self.assertEqual(ref_data, data)
        self.assertGreaterEqual(len([line for line in logs.output if 'Reopening' in line]), 2)


    def create_dbs(self, n):
        for i in range(n):
            with manager.get_transaction(self.mdb_filename, f'db{i}', write=True) as txn:
                txn.put(b'k', b'v')

    def test_max_dbs_sized_for_file(self):
        # databases of an existing file fit without reopening
        self.create_dbs(30)
        manager.close_all()
        # forget max_dbs of the file, as in a new process
        with mock.patch.object(manager, 'DEFAULT_MAXDBS', 8), mock.patch.object(manager, '_max_dbs', {}), \
                self.assertNoLogs('lmdb_sensor_storage.manager', level='WARNING'):
            for i in range(30):
                manager.get_db(self.mdb_filename, f'db{i}', create=False)

    def test_reopen_waits_for_transactions(self):
        with mock.patch.object(manager, 'DEFAULT_MAXDBS', 8), mock.patch.object(manager, 'MAX_DBS_STEP', 8):
            self.create_dbs(4)
            reading = threading.Event()
            finish = threading.Event()
            result = []

            def read():
                with manager.get_transaction(self.mdb_filename, 'db0') as txn:
                    reading.set()
                    finish.wait()
                    # still valid, since reopening waited
                    result.append(txn.get(b'k'))

            reader = threading.Thread(target=read, daemon=True)
            reader.start()
            reading.wait()

            # exceed max_dbs in another thread
            creator = threading.Thread(target=self.create_dbs, args=(20,), daemon=True)
            creator.start()
            try:
                time.sleep(0.5)
                self.assertTrue(creator.is_alive(), 'reopening did not wait for the transaction')
            finally:
                finish.set()
            reader.join(timeout=10)
            creator.join(timeout=10)
            self.assertFalse(creator.is_alive())
            self.assertEqual([b'v'], result)
            self.assertIn('db19', manager.get_db_names(self.mdb_filename))

    def test_no_reopen_during_own_transaction(self):
        with mock.patch.object(manager, 'DEFAULT_MAXDBS', 8), mock.patch.object(manager, 'MAX_DBS_STEP', 8):
            self.create_dbs(4)
            with manager.get_transaction(self.mdb_filename, 'db0') as txn:
                with self.assertRaises(lmdb.DbsFullError), self.assertLogs('lmdb_sensor_storage.manager', 'ERROR'):
                    self.create_dbs(20)
                # the transaction is still valid
                self.assertEqual(b'v', txn.get(b'k'))

    def test_stale_readers_are_cleared(self):
        self.create_dbs(1)
        manager.close_all()

        # Stale readers remain only while another process has the file open, e.g. another MQTT subscriber, since
        # lmdb resets the reader table when opening a file which no other process has open.
        keep_open = subprocess.Popen([sys.executable, '-c', 'import sys, time, lmdb; '
                                      'env = lmdb.open(sys.argv[1], subdir=False, max_dbs=8); '
                                      'print("open", flush=True); time.sleep(60)', self.mdb_filename],
                                     stdout=subprocess.PIPE)
        try:
            self.assertEqual(b'open\n', keep_open.stdout.readline())

            # a process killed during a read transaction leaves a stale reader
            code = ('import os, signal, sys, lmdb; '
                    'env = lmdb.open(sys.argv[1], subdir=False, max_dbs=8); txn = env.begin(); '
                    'os.kill(os.getpid(), signal.SIGKILL)')
            subprocess.run([sys.executable, '-c', code, self.mdb_filename], timeout=60)

            with self.assertLogs('lmdb_sensor_storage.manager', level='WARNING') as logs:
                manager.get_environment(self.mdb_filename)
            self.assertIn('Cleared 1 stale readers', '\n'.join(logs.output))
        finally:
            keep_open.kill()
            keep_open.wait()

if __name__ == '__main__':
    unittest.main()
