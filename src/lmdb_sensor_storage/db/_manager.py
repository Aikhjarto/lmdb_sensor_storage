import logging
import os
import threading
import time
from typing import Dict, List, Tuple
import lmdb


logger = logging.getLogger('lmdb_sensor_storage.manager')


class _ReopenLock:
    """
    Readers-writer lock, which is held shared by transactions and exclusively when reopening an environment, which
    invalidates all its transactions.
    A thread may acquire it shared several times, also while another thread waits for exclusive access.
    """

    def __init__(self):
        self._condition = threading.Condition()
        self._shared = 0
        self._exclusive = False
        self._local = threading.local()

    def _own_shared(self) -> int:
        return getattr(self._local, 'count', 0)

    def acquire_shared(self):
        with self._condition:
            # nested transactions of a thread must not wait, since the thread would wait for itself
            while self._exclusive and not self._own_shared():
                self._condition.wait()
            self._shared += 1
        self._local.count = self._own_shared() + 1

    def release_shared(self):
        self._local.count = self._own_shared() - 1
        with self._condition:
            self._shared -= 1
            self._condition.notify_all()

    def acquire_exclusive(self, timeout: float) -> bool:
        """
        Wait until no other transaction is active. Returns False, if the current thread has an active transaction or
        if other transactions did not finish within `timeout` seconds.
        """
        if self._own_shared():
            return False
        with self._condition:
            while self._exclusive:  # another thread reopens
                self._condition.wait()
            # block new transactions while waiting for active ones
            self._exclusive = True
            if not self._condition.wait_for(lambda: self._shared == 0, timeout):
                self._exclusive = False
                self._condition.notify_all()
                return False
            return True

    def release_exclusive(self):
        with self._condition:
            self._exclusive = False
            self._condition.notify_all()


class _Transaction:
    """Wraps an lmdb.Transaction to hold the _ReopenLock shared until the transaction ends."""

    def __init__(self, txn: lmdb.Transaction, lock: _ReopenLock):
        self._txn = txn
        self._lock = lock
        self._released = False

    def _release(self):
        if not self._released:
            self._released = True
            self._lock.release_shared()

    def __enter__(self) -> lmdb.Transaction:
        return self._txn.__enter__()

    def __exit__(self, *exc_info):
        try:
            return self._txn.__exit__(*exc_info)
        finally:
            self._release()

    def __getattr__(self, name):
        return getattr(self._txn, name)

    def __del__(self):
        self._release()


class Manager:
    """
    Some functionality of lmdb is not thread-safe [1], e.g, and env.open_dB should be called only by one thread and
    a lmdb file must only be opened once per process!

    This class provides a more thread-safe interface to lmdb.

    Notes
    -----
    .. [1] https://lmdb.readthedocs.io/en/release/#environment-class
    """

    DEFAULT_MAXDBS = 1024
    MAX_DBS_STEP = 1024

    # maximum time to wait for other transactions to finish, before the environment is reopened
    REOPEN_TIMEOUT = 10

    # interval in seconds to clear stale readers of killed processes
    READER_CHECK_INTERVAL = 3600

    # options for lmdb.open, which can be overridden with env_kwargs
    ENV_OPTIONS = {
        # Databases are often larger than RAM, where reading ahead reads mostly pages which are not needed.
        'readahead': False,
        # Sync only data pages on commit, but not the meta page. A system crash may undo the last transaction, but
        # does not corrupt the database. This saves one of the two syncs per commit.
        'metasync': False,
    }

    def __init__(self):
        self.handles = {}  # type: Dict[str, lmdb.Environment]
        self._max_dbs = {}  # type: Dict[str, int]
        self._dbs = {}  # type: Dict[Tuple[str, str], Tuple[lmdb.Environment, lmdb._Database]]
        # additional arguments for lmdb.open, e.g. sync=False for tests
        self.env_kwargs = {}
        self._last_reader_check = {}  # type: Dict[str, float]
        # guards the dicts above and env.open_db, which must not be called concurrently
        self._lock = threading.RLock()
        # held by transactions, which become invalid when their environment is reopened
        self._reopen_lock = _ReopenLock()

    def get_environment(self, mdb_filename, **kwargs):
        """
        Thread-safe function returning the Environment.

        Parameters
        ----------
        mdb_filename : str

        Returns
        -------
        env : lmdb.Environment
        """
        mdb_filename = os.path.realpath(mdb_filename)
        env = self.handles.get(mdb_filename)
        if env is not None:
            return env
        with self._lock:
            if mdb_filename not in self._max_dbs:
                self._max_dbs[mdb_filename] = self.DEFAULT_MAXDBS

            if mdb_filename not in self.handles:
                env = self._open_environment(mdb_filename, **kwargs)

                # Size max_dbs for the databases in the file, so reopening because of exceeding it, which invalidates
                # all transactions, is rarely needed.
                required_max_dbs = 2 * env.stat()['entries']
                if required_max_dbs > self._max_dbs[mdb_filename]:
                    env.close()
                    self._max_dbs[mdb_filename] = required_max_dbs
                    env = self._open_environment(mdb_filename, **kwargs)

                self._check_readers(mdb_filename, env)
                self.handles[mdb_filename] = env
            return self.handles[mdb_filename]

    def _open_environment(self, mdb_filename, **kwargs) -> lmdb.Environment:
        logger.debug('Opening database %s with max_dbs=%i', mdb_filename, self._max_dbs[mdb_filename])
        options = {'map_size': 1024 * 1024 * 1024 * 1024,
                   'subdir': False,
                   'max_dbs': self._max_dbs[mdb_filename],
                   **self.ENV_OPTIONS,
                   **self.env_kwargs,
                   **kwargs}
        return lmdb.open(mdb_filename, **options)

    def _check_readers(self, mdb_filename, env: lmdb.Environment):
        """
        Clear reader slots of processes, which were killed during a read transaction. Such stale readers prevent
        lmdb from reusing pages freed after their snapshot, so the file grows.
        """
        self._last_reader_check[mdb_filename] = time.monotonic()
        cleared = env.reader_check()
        if cleared:
            logger.warning('Cleared %i stale readers of %s', cleared, mdb_filename)

    def _open_db(self, mdb_filename, db_name, create=True):
        """
        Returns environment and handle of a database. If `create` is False, lmdb.NotFoundError is raised for a
        database which does not exist, instead of creating it.
        """
        mdb_filename = os.path.realpath(mdb_filename)
        # Fast path without lock for already opened databases. This avoids waiting for the lock while another thread
        # opens a new database, which requires a write transaction and thus might wait for the writer lock of lmdb.
        cached = self._dbs.get((mdb_filename, db_name))
        if cached is not None:
            return cached
        with self._lock:
            env = self.get_environment(mdb_filename)
            while True:
                try:
                    self._dbs[(mdb_filename, db_name)] = env, env.open_db(db_name.encode(), create=create)
                    return self._dbs[(mdb_filename, db_name)]
                except lmdb.DbsFullError:
                    # Reopen with more slots for named databases. Since this invalidates all transactions of the
                    # environment, wait until no transaction is active.
                    if not self._reopen_lock.acquire_exclusive(self.REOPEN_TIMEOUT):
                        logger.error('Cannot reopen %s to increase max_dbs=%i, since transactions are active',
                                     mdb_filename, self._max_dbs[mdb_filename])
                        raise
                    try:
                        logger.warning('Reopening %s since max_dbs=%i is exceeded',
                                       mdb_filename, self._max_dbs[mdb_filename])
                        self._max_dbs[mdb_filename] = env.stat()['entries'] + self.MAX_DBS_STEP
                        del env
                        self.close(mdb_filename)
                        env = self.get_environment(mdb_filename)
                    finally:
                        self._reopen_lock.release_exclusive()

    def get_db(self, mdb_filename, db_name, create=True):
        """
        Parameters
        ----------
        mdb_filename : str
        db_name : str
        create : bool
            If False, lmdb.NotFoundError is raised for a database which does not exist, instead of creating it.

        Returns
        -------
        lmdb._Database

        """
        env, db = self._open_db(mdb_filename, db_name, create=create)

        return db

    def get_transaction(self, mdb_filename, db_name, write=False, create=None, **kwargs):
        """
        Thread safe function creating a Transaction handle.

        Note: env.open_db will block when another transaction is in progress.

        Parameters
        ----------
        mdb_filename : str
        db_name : str
        write : bool
            Transaction for writing
        create : bool or None
            Create the database if it does not exist, default is to create it only for writing, so reading e.g. a
            misspelled sensor does not create empty databases.
            If the database does not exist and is not created, lmdb.NotFoundError is raised.

        Returns
        -------
        txn : lmdb.Transaction

        """
        if create is None:
            create = write
        while True:
            env, db = self._open_db(mdb_filename, db_name, create=create)
            txn = self._begin(mdb_filename, env, db=db, write=write, **kwargs)
            if txn is not None:
                return txn

    def begin(self, mdb_filename, **kwargs):
        """
        Like lmdb.Environment.begin, but the environment is not reopened while the transaction is active.
        Database handles in `kwargs` must be obtained by `get_db` before.
        """
        while True:
            txn = self._begin(mdb_filename, self.get_environment(mdb_filename), **kwargs)
            if txn is not None:
                return txn

    def _begin(self, mdb_filename, env: lmdb.Environment, **kwargs):
        """Returns a transaction of `env`, or None, if `env` was reopened meanwhile."""
        mdb_filename = os.path.realpath(mdb_filename)
        self._reopen_lock.acquire_shared()
        try:
            if self.handles.get(mdb_filename) is not env:
                self._reopen_lock.release_shared()
                return None
            if time.monotonic() - self._last_reader_check.get(mdb_filename, 0) > self.READER_CHECK_INTERVAL:
                self._check_readers(mdb_filename, env)
            return _Transaction(env.begin(**kwargs), self._reopen_lock)
        except BaseException:
            self._reopen_lock.release_shared()
            raise

    def db_exists(self, mdb_filename, db_name):
        """
        Check if database exists since access (also read-only) to a non-existing database creates it.
        Misspelling sensor_names should not create databases.

        Parameters
        ----------
        mdb_filename : str
        db_name : str

        Returns
        -------
        ret : bool
            True, if database exists in file, False otherwise.
        """

        with self.begin(mdb_filename) as txn:
            if txn.get(db_name.encode()) is None:
                logger.info('Requested db %s does not exist in %s',
                            db_name, mdb_filename)
                return False
        return True

    def delete_db(self, mdb_filename, db_name):
        if self.db_exists(mdb_filename, db_name):
            logger.info('deleting db %s from %s', db_name, mdb_filename)
            db = self.get_db(mdb_filename, db_name)
            with self._lock:
                self._dbs.pop((os.path.realpath(mdb_filename), db_name), None)
            with self.begin(mdb_filename, db=db, write=True) as txn:
                txn.drop(db=db)

    def get_db_names(self, mdb_filename: str) -> List[str]:
        dbs = []
        with self.begin(mdb_filename) as txn:
            for key, val in txn.cursor():
                dbs.append(key.decode())
        return dbs

    def close(self, mdb_filename):
        mdb_filename = os.path.realpath(mdb_filename)
        with self._lock:
            self._dbs = {key: val for key, val in self._dbs.items() if key[0] != mdb_filename}
            if mdb_filename in self.handles:
                self.handles.pop(mdb_filename).close()

    def close_all(self):
        with self._lock:
            self._dbs = {}
            for env in self.handles.values():
                env.close()
            self.handles = {}

    def __del__(self):
        self.close_all()


manager = Manager()
