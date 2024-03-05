import logging
from datetime import datetime, timedelta
from typing import (Dict, Sequence, Union, Tuple, Any, List, SupportsFloat, Callable)
from typing_extensions import Literal
from lmdb_sensor_storage.db.chunker import _T, non_chunker
from lmdb_sensor_storage.db._manager import manager
from lmdb_sensor_storage.db.packer import (StringPacker, JSONPacker, DatetimePacker, YamlPacker, FloatPacker,
                                           IntPacker, StructPacker)
from lmdb_sensor_storage._parser import as_datetime
from lmdb_sensor_storage.db.dict_db import LMDBDict

logger = logging.getLogger('lmdb_sensor_storage.db')


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

    def write_value(self, date: datetime, value: Any,
                    only_if_value_changed: bool = False,
                    max_age_seconds: float = None):
        """
        Write a new value at date.
        If date already existed, the value is updated.
        If date did not exist previously, it is added, unless only_if_value_changed was True and the previous value was
        the same as the new value.
        If only_if_value_changed was True and max_age_seconds was given, an entry will be made if time difference
        between the most recent value before `date` and `date` exceeds  max_age_seconds.
        """
        with manager.get_transaction(self._mdb_filename, self._db_name, write=True) as txn:
            value_packed = self._value_packer.pack(value)
            if only_if_value_changed:
                cur = txn.cursor()
                if cur.set_range(self._key_packer.pack(date)):  # seek to first timestamp greater or equal to date
                    # assert: cur is not at end of database but pointing to first value with timestamp >=date
                    cur_valid = cur.prev()  # move one back (if not already on first entry)
                else:
                    # assert: cur is pointing somewhere after the end of database
                    cur_valid = cur.last()  # move to last entry, unless database is empty

                if cur_valid:
                    # assert: cur points to the place right before sample will be added in
                    last_time = self._key_packer.unpack(cur.key())
                    check_value_changed_is_required = True
                    if max_age_seconds is not None:
                        assert (max_age_seconds > 0)
                        check_value_changed_is_required = (date - last_time) < timedelta(seconds=max_age_seconds)

                    if check_value_changed_is_required:
                        last_value = cur.value()
                        if last_value == value_packed:
                            logger.debug(
                                f'Skip ({date},{value}) for db "{self._db_name}," since value did not change from '
                                f'since {last_time}')
                            return False

            logger.debug(f'Write ({date},{value}) to DB "{self._db_name}"')

            result = txn.put(self._key_packer.pack(date), value_packed)

        # documentation says that sync is done every Transaction.commit, however it does not seem to happen when
        # mdb_filename is created with said Transaction. Thus sync manually.
        manager.get_environment(self._mdb_filename).sync()

        return result

    def delete_entries(self,
                       since: Union[str, datetime, None] = None,
                       until: Union[str, datetime, None] = None) -> bool:

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

        logger.info(f'Deleting values of DB {self._db_name} from {self._mdb_filename} '
                    f'since {since.isoformat()} until {until.isoformat()}.')

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

    def _get_timespan(self, since: datetime = None, until: datetime = None, endpoint: bool = False, limit: int = None,
                      what: Literal['keys', 'values', 'items'] = None):

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
                        if what == 'keys':
                            yield key_unpacked
                        elif what == 'values':
                            yield self._value_packer.unpack(c.value())
                        elif what == 'items':
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

    def _get_timespan_decimated(self, decimate_to_s: Union[Literal['auto'], SupportsFloat],
                                since: datetime = None, until: datetime = None, limit: int = None,
                                timestamp_chunker: Callable[[Sequence[datetime]], Sequence[datetime]] = None,
                                value_chunker: Callable[[Sequence[_T]], Sequence[_T]] = None):
        for timestamp_list, value_list in self._get_timespan_chunked(decimate_to_s=decimate_to_s,
                                                                     since=since, until=until,
                                                                     limit=limit):
            if timestamp_chunker is None:
                timestamp_chunker = non_chunker

            if value_chunker is None:
                value_chunker = non_chunker

            for d, v in zip(timestamp_chunker(timestamp_list), value_chunker(value_list)):
                yield d, v

    def _get_timespan_chunked(self, decimate_to_s: Union[Literal['auto'], SupportsFloat],
                              since: datetime = None, until: datetime = None, limit: int = None):

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
                values = [value, ]
                dates = [row_date, ]
                n = 1

    def values(self, since: datetime = None, until: datetime = None,
               endpoint: bool = False, limit: int = None) -> List[Any]:
        return list(self._get_timespan(since=since, until=until, endpoint=endpoint, limit=limit, what='values'))

    def items(self, first: bool = None, last: bool = None,
              since: datetime = None, until: datetime = None,
              endpoint: bool = False, decimate_to_s: float = None, limit: int = None,
              timestamp_chunker: Callable[[Sequence[datetime]], Sequence[datetime]] = None,
              value_chunker: Callable[[Sequence[datetime]], Sequence[datetime]] = None) -> List[Tuple[datetime, Any]]:
        if decimate_to_s:
            return list(self._get_timespan_decimated(since=since, until=until, decimate_to_s=decimate_to_s, limit=limit,
                                                     timestamp_chunker=timestamp_chunker, value_chunker=value_chunker))
        elif first:
            return [(self.get_first_timestamp(), self.get_first_value())]
        elif last:
            return [(self.get_last_timestamp(), self.get_last_value())]
        else:
            return list(self._get_timespan(since=since, until=until, endpoint=endpoint, limit=limit, what='items'))

    def keys_values(self, **kwargs) -> Tuple[List[datetime], List[Any]]:
        keys = []
        values = []
        for key, value in self.items(**kwargs):
            keys.append(key)
            values.append(value)
        return keys, values

    def keys(self, since: datetime = None, until: datetime = None,
             endpoint: bool = False, limit: int = None) -> List[datetime]:
        return list(self._get_timespan(since=since, until=until, endpoint=endpoint, limit=limit, what='keys'))

    def get_first_timestamp(self) -> datetime:
        return self._get_sample(last=False)

    def get_last_timestamp(self) -> datetime:
        return self._get_sample(last=True)

    def get_last_changed(self) -> Union[datetime, None]:
        if manager.db_exists(self._mdb_filename, self._db_name):
            with manager.get_transaction(self._mdb_filename, self._db_name) as txn:
                c = txn.cursor()
                if c.last():
                    # assert: db is not empy
                    last_value = c.value()
                    while c.prev():  # move backwards in time
                        if last_value != c.value():
                            c.next()
                            return self._key_packer.unpack(c.key())

    def get_first_value(self):
        return self._get_sample(last=False, get_value=True)

    def get_last_value(self):
        return self._get_sample(last=True, get_value=True)

    @property
    def statistics(self) -> Dict[str, Union[str, Dict]]:
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
                    # d_s['first_value'] = self._value_packer.unpack(cursor.value())
                if cursor.last():
                    until = self._key_packer.unpack(cursor.key())
                    d_s['until'] = until.isoformat()
                    d_s['until_epoch'] = until.strftime('%s')
                    # d_s['last_value'] = self._value_packer.unpack(cursor.value())

        return d_s

    def _get_sample(self, last: bool = False, get_value: bool = False):
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

        self._value_packer = FloatPacker()


class TimestampIntDB(TimestampBytesDB):
    def __init__(self, mdb_filename, db_name):
        super().__init__(mdb_filename, db_name)

        self._value_packer = IntPacker()


class TimestampStructDB(TimestampBytesDB):
    def __init__(self, mdb_filename, db_name, pack_str=None):
        super().__init__(mdb_filename, db_name)

        self._value_packer = StructPacker(pack_str)
