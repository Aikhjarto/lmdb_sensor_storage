import logging
import os
from typing import Dict, List
import lmdb


logger = logging.getLogger('lmdb_sensor_storage.manager')


class Manager:
    """
    Some functionality of lmdb is not thread-safe [1], e.g, and env.open_dB should be called only by one thread and
    a lmdb file must only be opened once per process!

    This class provides a more thread-safe interface to lmdb.

    Notes
    -----
    .. [1] https://lmdb.readthedocs.io/en/release/#environment-class
    """

    def __init__(self):
        self.handles = {}  # type: Dict[str, lmdb.Environment]

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
        if mdb_filename not in self.handles:
            logger.debug('Opening database %s', mdb_filename)
            self.handles[mdb_filename] = lmdb.open(mdb_filename,
                                                   map_size=1024 * 1024 * 1024 * 1024,
                                                   subdir=False,
                                                   max_dbs=1024,
                                                   **kwargs)
        # else:
        #     logger.debug('Reusing handle for database %s', mdb_filename)
        return self.handles[mdb_filename]

    def get_db(self, mdb_filename, db_name):
        """
        Parameters
        ----------
        mdb_filename : str
        db_name : str

        Returns
        -------
        lmdb._Database

        """
        env = self.get_environment(mdb_filename)
        db = env.open_db(db_name.encode())
        return db

    def get_transaction(self, mdb_filename, db_name, **kwargs):
        """
        Thread safe function creating a Transaction handle.

        Note: env.open_db will block when another transaction is in progress.

        Parameters
        ----------
        mdb_filename : str
        db_name : str

        Returns
        -------
        txn : lmdb.Transaction

        """
        env = self.get_environment(mdb_filename)
        db = env.open_db(db_name.encode())
        return env.begin(db=db, **kwargs)

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

        with self.get_environment(mdb_filename).begin() as txn:
            if txn.get(db_name.encode()) is None:
                logger.info('Requested db %s does not exist in %s',
                            db_name, mdb_filename)
                return False
        return True

    def delete_db(self, mdb_filename, db_name):
        if self.db_exists(mdb_filename, db_name):
            logger.info('deleting db %s from %s', db_name, mdb_filename)
            e = manager.get_environment(mdb_filename)
            db = e.open_db(db_name.encode())
            with e.begin(db=db, write=True) as txn:
                txn.drop(db=db)

    def get_db_names(self, mdb_filename: str) -> List[str]:
        dbs = []
        with self.get_environment(mdb_filename).begin() as txn:
            for key, val in txn.cursor():
                dbs.append(key.decode())
        return dbs

    def close(self, mdb_filename):
        if mdb_filename in self.handles:
            self.handles.pop(mdb_filename).close()

    def close_all(self):
        for env in self.handles.values():
            env.close()
        self.handles = {}

    def __del__(self):
        self.close_all()


manager = Manager()
