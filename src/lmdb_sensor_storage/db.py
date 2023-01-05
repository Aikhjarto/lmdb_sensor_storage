import logging
import os
from datetime import datetime, timedelta
from typing import MutableMapping, Dict, Mapping, Iterable, Sequence, TypeVar, Union, Tuple, Any, List
_T = TypeVar('_T')
import lmdb
import numpy as np

from lmdb_sensor_storage._packer import BytesPacker, StringPacker, JSONPacker, DatetimePacker, YamlPacker, FloatPacker, \
    IntPacker, StructPacker, RegexPacker
from lmdb_sensor_storage._parser import as_datetime

logger = logging.getLogger('lmdb_sensor_storage.db')


class Manager:
    """
    Some functionality of lmdb is not thread-safe [1], e.g, and env.open_dB should be called only by one thread and
    a lmdb file must only be opened once per process!

    This class provides a more thread-safe interface to lmdb.

    References
    ----------
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

    def get_db(self,mdb_filename, db_name):
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
                if db_name.startswith('data_'):
                    log_fct = logger.warning
                else:
                    log_fct = logger.info
                log_fct('Requested db %s does not exist in %s',
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


class LMDBDict(MutableMapping):
    """
    Key-value database based on LMDB where both are bytes-objects.

    Keys and values can be accessed similar to a dictionary.
    """

    def __init__(self, mdb_filename: str, db_name: str):
        self._mdb_filename = os.path.realpath(mdb_filename)
        self._db_name = db_name

        self._key_packer = BytesPacker()
        self._value_packer = BytesPacker()

    def __repr__(self):
        return f'{self._mdb_filename}:{self._db_name} with {len(self)} keys'

    @property
    def mdb_filename(self):
        return self._mdb_filename

    @property
    def db_name(self):
        return self._db_name

    def _get_lmdb_stats(self):
        with manager.get_transaction(self._mdb_filename, self._db_name) as txn:
            # noinspection PyArgumentList
            stat = txn.stat()
            return stat

    def __getitem__(self, key):
        if manager.db_exists(self._mdb_filename, self._db_name):

            if isinstance(key, slice):
                if key.start is not None or key.stop is not None or key.step is not None:
                    raise KeyError(f'slicing in undorderd data is not supported')

                result = []
                with manager.get_transaction(self._mdb_filename, self._db_name) as txn:
                    for _, val in txn.cursor():
                        result.append(self._value_packer.unpack(val))
                return result

            else:
                key_packed = self._key_packer.pack(key)
                with manager.get_transaction(self._mdb_filename, self._db_name) as txn:
                    cur = txn.cursor()
                    if cur.set_range(key_packed) and cur.key() == key_packed:
                        return self._value_packer.unpack(cur.value())
        raise KeyError(f'{key} is not in DB {self._db_name}')

    def keys(self):
        return list(self._get(what='keys'))

    def values(self):
        return list(self._get(what='values'))

    def items(self):
        return list(self._get(what='items'))

    def _get(self, what='keys'):
        if manager.db_exists(self._mdb_filename, self._db_name):
            with manager.get_transaction(self._mdb_filename, self._db_name) as txn:
                c = txn.cursor()
                if c.first():
                    if what=='keys':
                        yield self._key_packer.unpack(c.key())
                        while c.next():
                            yield self._key_packer.unpack(c.key())
                    elif what=='values':
                        yield self._value_packer.unpack(c.value())
                        while c.next():
                            yield self._value_packer.unpack(c.value())
                    elif what=='items':
                        yield self._key_packer.unpack(c.key()), self._value_packer.unpack(c.value())
                        while c.next():
                            yield self._key_packer.unpack(c.key()), self._value_packer.unpack(c.value())

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def pop(self, key):
        value = self[key]
        del self[key]
        return value

    def clear(self):
        for key in self.keys():
            del [key]

    def setdefault(self, key, default=None):
        if key in self:
            return self[key]
        else:
            self[key] = default
            return default

    def as_dict(self):
        d = {}
        for key, val in self.items():
            d[key]=val
        return d

    def update(self, other: Union[Mapping, Sequence[Tuple[Any, Any]]], **kwargs: int) -> None:

        # https://docs.python.org/3/library/stdtypes.html#dict.update
        if isinstance(other, Mapping):
            _iter = other.items()
        elif isinstance(other, Iterable):
            _iter = other
        else:
            _iter = kwargs.items()
        
        result = True
        with manager.get_transaction(self._mdb_filename, self._db_name, write=True) as txn:
            for date, value in _iter:
                ret = txn.put(self._key_packer.pack(date), self._value_packer.pack(value))
                result = result and ret
        return result

    def __iter__(self):
        return self.keys()

    def __setitem__(self, key: bytes, value: bytes):
        key_packed = self._key_packer.pack(key)
        value_packed = self._value_packer.pack(value)

        if manager.db_exists(self._mdb_filename, self._db_name):
            with manager.get_transaction(self._mdb_filename, self._db_name) as txn:
                c = txn.cursor()
                if c.set_range(key_packed) and c.key() == key_packed and c.value() == value_packed:
                    # don't overwrite value when nothing changed
                    logger.debug(f'Write ({key},{value}) to DB "{self._db_name} skiped, since is entrie already there"')
                    return

        with manager.get_transaction(self._mdb_filename, self._db_name, write=True) as txn:
            logger.debug(f'Write ({key},{value}) to DB "{self._db_name}"')
            result = txn.put(key_packed, self._value_packer.pack(value))

        # documentation says that sync is done every Transaction.commit, however it does not seem to happen when
        # mdb_filename is created with said Transaction. Thus sync manually.
        manager.get_environment(self._mdb_filename).sync()

        return result

    def __len__(self):
        stats = self._get_lmdb_stats()
        if stats:
            return stats['entries']
        else:
            return 0

    def __contains__(self, key):
        if manager.db_exists(self._mdb_filename, self._db_name):
            key_packed = self._key_packer.pack(key)
            with manager.get_transaction(self._mdb_filename, self._db_name) as txn:
                c = txn.cursor()
                if c.set_range(key_packed) and c.key() == key_packed:
                    return True
        return False

    def __delitem__(self, key):
        if manager.db_exists(self._mdb_filename, self._db_name):
            with manager.get_transaction(self._mdb_filename, self._db_name, write=True) as txn:
                c = txn.cursor()
                key_packed = self._key_packer.pack(key)
                if c.set_range(key_packed) and c.key() == key_packed:
                    if not c.delete():
                        raise IOError
                else:
                    raise KeyError


    def copy_to(self, new_db_name, export_mdb_filename = None):
        if not export_mdb_filename:
            export_mdb_filename = self.mdb_filename
        export_mdb_filename = os.path.realpath(export_mdb_filename)

        if manager.db_exists(self.mdb_filename, new_db_name):
            raise RuntimeError(f'DB "{new_db_name}" already exists in "{export_mdb_filename}"')
        else:
            # Note: all db handles must be genereated befor a transaction is created, thus manager.get_transaction cannot be used here
            db_out = manager.get_db(export_mdb_filename, new_db_name)
            db_in = manager.get_db(self.mdb_filename, self._db_name)
            with lmdb.Transaction(manager.get_environment(export_mdb_filename),db=db_out, write=True) as txn_out:
                with lmdb.Transaction(manager.get_environment(self._mdb_filename),db=db_in) as txn_in:
                    for key, val in txn_in.cursor():
                        txn_out.put(key, val, db=db_out)

    def close_file(self):
        manager.close(self._mdb_filename)


class StringStringDB(LMDBDict):
    """
    Key-value database based on LMDB where both are str-objects.

    Keys and values can be accessed similar to a dictionary.
    """

    def __init__(self, mdb_filename: str, db_name: str):
        super().__init__(mdb_filename, db_name)
        self._key_packer = StringPacker()
        self._value_packer = StringPacker()


class StringRegexpDB(LMDBDict):
    def __init__(self, mdb_filename: str, db_name: str):
        super().__init__(mdb_filename, db_name)
        self._key_packer = StringPacker()
        self._value_packer = RegexPacker()


class StringJsonDB(LMDBDict):
    """
    Key-value database based on LMDB where keys are str-objects and values is anything that can be converted to json.

    Keys and values can be accessed similar to a dictionary.
    """

    def __init__(self, mdb_filename: str, db_name: str):
        super().__init__(mdb_filename, db_name)
        self._key_packer = StringPacker()
        self._value_packer = JSONPacker()


class StringYamlDB(LMDBDict):
    """
    Key-value database based on LMDB where keys are str-objects and values is anything that can be converted to YAML.

    Keys and values can be accessed similar to a dictionary.
    """

    def __init__(self, mdb_filename: str, db_name: str):
        super().__init__(mdb_filename, db_name)
        self._key_packer = StringPacker()
        self._value_packer = YamlPacker()


def timestamp_chunker_left(x: Sequence[datetime]) -> Sequence[datetime]:
    return [x[0],]

def timestamp_chunker_right(x: Sequence[datetime]) -> Sequence[datetime]:
    return [x[-1],]

def timestamp_chunker_center(x: Sequence[datetime]) -> Sequence[datetime]:
    if len(x)>1:
        return [x[0]+(x[-1]-x[0])/2,]
    else:
        return [x[0],]

def value_chunker_min(x: Sequence[_T]) -> Sequence[_T]:
    return [min(*x),]

def value_chunker_max(x: Sequence[_T]) -> Sequence[_T]:
    return [max(*x),]

def value_chunker_median(x: Sequence[_T]) -> Sequence[_T]:
    return [np.median(x),]

def value_chunker_mean(x: Sequence[_T]) -> Sequence[_T]:
    return [np.mean(x,axis=0).tolist(),]

def value_chunker_minmeanmax(x):
    if len(x)>1:
        return np.min(x, axis=0).tolist(), np.mean(x, axis=0).tolist(), np.max(x, axis=0).tolist()
    else:
        return x

def timestamp_chunker_minmeanmax(x):
    if len(x)>1:
        return [x[0], x[0]+(x[-1]-x[0])/2, x[-1]]
    else:
        return x


class TimestampBytesDB(LMDBDict):
    """
    Key-value database based on LMDB where the keys are datetime-object and always stored in ascending order and values
    are bytes-objects.

    Keys and values can be accessed similar to a dictionary.
    """

    def __init__(self, mdb_filename: str, db_name: str):
        super().__init__(mdb_filename, db_name)
        self._key_packer = DatetimePacker()

        self._timestamp_chunker = lambda x: x
        self._value_chunker = lambda x: x

    def __repr__(self):
        return super().__repr__() + f' from {self.get_first_timestamp()} to {self.get_last_timestamp()}'

    def write_value(self, date: datetime, value, only_if_value_changed=False):
        """
        Write a new value at date.
        If date already existed, the value is updated.
        If date did not exist previously, it is added, unless only_if_value_changed was True and the previous value was the same as the new value.
        """
        with manager.get_transaction(self._mdb_filename, self._db_name, write=True) as txn:
            value_packed = self._value_packer.pack(value)
            if only_if_value_changed:
                cur = txn.cursor()
                if cur.set_range(self._key_packer.pack(date)):  # seek to first timestamp greater or equal to date
                    if cur.prev():  # move one back (if not already on first entry)
                        # assert: cur points to the place right before sample will be added in
                        last_value = cur.value()

                        if last_value == value_packed:
                            logger.debug(
                                f'Skip ({date},{value}) for db "{self._db_name}," since value did not change')
                            return False
                else:
                    if cur.last():
                        last_value = cur.value()
                        if last_value == value_packed:
                            logger.debug(
                                f'Skip ({date},{value}) for db "{self._db_name}," since value did not change')
                            return False

            logger.debug(f'Write ({date},{value}) to DB "{self._db_name}"')

            result = txn.put(self._key_packer.pack(date), value_packed)

        # documentation says that sync is done every Transaction.commit, however it does not seem to happen when
        # mdb_filename is created with said Transaction. Thus sync manually.
        manager.get_environment(self._mdb_filename).sync()

        return result

    def delete_entries(self, since=None, until=None):

        since = as_datetime(since, none_ok=True)
        until = as_datetime(until, none_ok=True)

        if since is None:
            since = self.get_first_timestamp()
        elif since > self.get_last_timestamp():
            return False

        if until is None:
            until = self.get_last_timestamp()
        elif until < self.get_first_timestamp():
            return False

        if not since <= until:
            raise RuntimeError(f'{since} is not before {until}')

        logger.info(
            f'Deleting values of DB {self._db_name} from {self._mdb_filename} since {since.isoformat()} until {until.isoformat()}.')

        result = True
        if manager.db_exists(self._mdb_filename, self._db_name):
            with manager.get_transaction(self._mdb_filename, self._db_name, write=True) as txn:
                c = txn.cursor()
                c.first()
                if c.set_range(self._key_packer.pack(since)):
                    # Hint: c.delete() return True on success and iterates one item further
                    # End is deteced when 'key' becomes an empty bytearray.
                    key = c.key()
                    while key and self._key_packer.unpack(key) <= until:
                        # loop until no more key before 'until' exists or deletion fails
                        if not c.delete():
                            raise IOError
                        if not c.next():
                            break

                else:
                    return False
        return result

    def _get_timespan(self, since=None, until=None, endpoint=False, limit=None, what=None):

        if not len(self):
            return

        since = as_datetime(since, none_ok=True)
        until = as_datetime(until, none_ok=True)

        if since is None:
            since = self.get_first_timestamp()
        elif since > self.get_last_timestamp():
            return

        if until is None:
            until = self.get_last_timestamp()
            endpoint = True
        elif until < self.get_first_timestamp():
            return

        if since is None and until is None:
            return

        if not since <= until:
            raise RuntimeError(f'{since} is not before {until}')

        if manager.db_exists(self._mdb_filename, self._db_name):
            with manager.get_transaction(self._mdb_filename, self._db_name, write=True) as txn:
                c = txn.cursor()
                if c.first() and c.set_range(self._key_packer.pack(since)):
                    key = c.key()
                    key_unpacked = self._key_packer.unpack(key)
                    count = 0
                    while key and (key_unpacked < until or (endpoint and key_unpacked == until)):
                        # loop until no more key before 'until' exists or deletion fails
                        if what=='keys':
                            yield key_unpacked
                        elif what=='values':
                            yield self._value_packer.unpack(c.value())
                        elif what=='items':
                            yield key_unpacked, self._value_packer.unpack(c.value())
                        else:
                            raise NotImplementedError
                        count += 1
                        if c.next() and (limit is None or count <= limit):
                            key = c.key()
                            key_unpacked = self._key_packer.unpack(key)
                        else:
                            break
                else:
                    return


    def _get_timespan_decimated(self, decimate_to_s, since=None, until=None, limit=None, timestamp_chunker=None, value_chunker=None):
        for timestamp_list, value_list in self._get_timespan_chunked(decimate_to_s=decimate_to_s,
                                                                    since=since, until=until,
                                                                    limit = limit):
            if timestamp_chunker is None:
                timestamp_chunker = lambda x: x

            if value_chunker is None:
                value_chunker = lambda x: x

            for d, v in zip(timestamp_chunker(timestamp_list), value_chunker(value_list)):
                yield d, v

    def _get_timespan_chunked(self, decimate_to_s, since=None, until=None, limit=None):


        if decimate_to_s == 'auto':
            assert isinstance(limit, int) and limit > 0
            if since is None:
                since = self.get_first_timestamp()
            if until is None:
                until = self.get_last_timestamp()
            duration = until - since
            decimate_to = duration / limit
        else:
            decimate_to = timedelta(seconds=float(decimate_to_s))

        n = 0  # number of values in accumulator
        values = None
        dates = None
        stop_date = None
        for row_date, value in self._get_timespan(since=since, until=until, endpoint=True, what='items'):

            if until is not None and row_date > until:
                yield dates, values

            if n > 0:
                if row_date >= stop_date:
                    yield dates, values
                    n = 0
                else:  # accumulate values
                    values.append(value)
                    dates.append(row_date)
                    n += 1

            if n == 0:  # new segment
                start_date = row_date
                stop_date = start_date + decimate_to
                values = [value,]
                dates = [row_date, ]
                n = 1

    def values(self, since=None, until=None, endpoint=False, limit=None):
        return list(self._get_timespan(since=since, until=until, endpoint=endpoint, limit=limit, what='values'))

    def items(self, since=None, until=None, endpoint=False, decimate_to_s=None, limit=None, timestamp_chunker=None, value_chunker=None):
        if decimate_to_s:
            return list(self._get_timespan_decimated(since=since, until=until, decimate_to_s=decimate_to_s, limit=limit, timestamp_chunker=timestamp_chunker, value_chunker=value_chunker))
        return list(self._get_timespan(since=since, until=until, endpoint=endpoint, limit=limit, what='items'))

    def keys_values(self, **kwargs):
        keys = []
        values = []
        for key, value in self.items(**kwargs):
            keys.append(key)
            values.append(value)
        return keys, values

    def keys(self, since=None, until=None, endpoint=False, limit=None):
        return list(self._get_timespan(since=since, until=until, endpoint=endpoint, limit=limit, what='keys'))

    def get_first_timestamp(self):
        return self._get_sample(last=False)

    def get_last_timestamp(self):
        return self._get_sample(last=True)

    def get_first_value(self):
        return self._get_sample(last=False, get_value=True)

    def get_last_value(self):
        return self._get_sample(last=True, get_value=True)

    @property
    def statistics(self):
        d_s = {}
        with manager.get_transaction(self._mdb_filename, self._db_name) as txn:
            # noinspection PyArgumentList
            stat = txn.stat()
            d_s['entries'] = stat['entries']
            if stat['entries'] > 0:
                cursor = txn.cursor()
                if cursor.first():
                    since = self._key_packer.unpack(cursor.key())
                    d_s['since'] = since.isoformat()
                    d_s['since_epoch'] = since.strftime('%s')
                    #d_s['first_value'] = self._value_packer.unpack(cursor.value())
                if cursor.last():
                    until = self._key_packer.unpack(cursor.key())
                    d_s['until'] = until.isoformat()
                    d_s['until_epoch'] = until.strftime('%s')
                    #d_s['last_value'] = self._value_packer.unpack(cursor.value())

        return d_s

    def _get_sample(self, last=False, get_value=False):
        if manager.db_exists(self._mdb_filename, self._db_name):
            with manager.get_transaction(self._mdb_filename, self._db_name) as txn:
                c = txn.cursor()
                if last:
                    cursor_move_successful = c.last()
                else:
                    cursor_move_successful = c.first()
                if cursor_move_successful:
                    if get_value:
                        return self._value_packer.unpack(c.value())
                    else:
                        return self._key_packer.unpack(c.key())

    def __setitem__(self, date: datetime, value):
        self.write_value(date, value, only_if_value_changed=False)

    def __getitem__(self, key: datetime):
        if isinstance(key, slice):
            if key.start is None and key.stop is None and key.step is None:
                # use faster (unconditioned) loop from super class
                return super().__getitem__(key)
            else:
                if key.step is None:
                    raise NotImplementedError
                return self.values(since=key.start, until=key.stop)
        else:
            return super().__getitem__(key)

    def __delitem__(self, key: datetime):
        if isinstance(key, slice):
            if key.step is None:
                raise NotImplementedError
            return self.delete_entries(since=key.start, until=key.stop)
        else:
            return super().__delitem__(key)


class TimestampStringDB(TimestampBytesDB):
    def __init__(self, mdb_filename, db_name):
        super().__init__(mdb_filename, db_name)

        self._value_packer = StringPacker()


class TimestampJSONDB(TimestampBytesDB):
    """
    Values are json encoded before writing and decoded
    """

    def __init__(self, mdb_filename, db_name):
        super().__init__(mdb_filename, db_name)

        self._value_packer = JSONPacker()


class TimestampYAMLDB(TimestampBytesDB):
    def __init__(self, mdb_filename, db_name):
        super().__init__(mdb_filename, db_name)

        self._value_packer = YamlPacker()


class TimestampFloatDB(TimestampBytesDB):
    def __init__(self, mdb_filename, db_name):
        super().__init__(mdb_filename, db_name)

        self._value_packer = FloatPacker


class TimestampIntDB(TimestampBytesDB):
    def __init__(self, mdb_filename, db_name):
        super().__init__(mdb_filename, db_name)

        self._value_packer = IntPacker()


class TimestampStructDB(TimestampBytesDB):
    def __init__(self, mdb_filename, db_name, pack_str=None):
        super().__init__(mdb_filename, db_name)

        self._value_packer = StructPacker(pack_str)
