from datetime import datetime, timedelta
import numpy as np
import struct
import os
import yaml

from lmdb_sensor_storage.db import manager, logger
from lmdb_sensor_storage._parser import as_datetime
from typing import Iterable, List, Union, Tuple, Any


def _datetime2key(d: datetime) -> bytes:
    # return d.isoformat().encode()
    return struct.pack('!Q', int(d.timestamp() * 1e6))


def _key2datetime(key: bytes) -> datetime:
    # return fromisoformat(key.decode())
    return datetime.fromtimestamp(struct.unpack('!Q', key)[0] / 1e6)


# def float2bytes(f) -> bytes:
#     """
#
#     Parameters
#     ----------
#     f : float | List[float]
#
#     Returns
#     -------
#
#     """
#     try:
#         _ = iter(f)
#         iterable = True
#     except TypeError:
#         iterable = False
#     if iterable:
#         try:
#             n = len(f)
#             return struct.pack(f'{n}f', *f)
#         except TypeError:
#             # iterable without a length, i.e. len(f) is not defined
#             buf = bytes()
#             for val in f:
#                 buf += struct.pack('f', val)
#             return buf
#     else:
#         return struct.pack('f', f)
#
#
# def bytes2float(b: bytes) -> tuple[Any, ...] | Any:
#     if len(b) == 4:
#         return struct.unpack('f', b)[0]
#     else:
#         return struct.unpack(f'{len(b)//4}f', b)


def pack(data: Union[Tuple, List, bytes, float, int], pack_str=None) -> bytes:
    if not pack_str:
        pack_str = f'{len(data)}f'
    if isinstance(data, list) or isinstance(data, tuple):
        return struct.pack(pack_str, *data)
    else:
        return struct.pack(pack_str, data)


def unpack(data: bytes, pack_str=None) -> Tuple[Any]:
    if not pack_str:
        pack_str = f'{len(data) // 4}f'
    return struct.unpack(pack_str, data)


def _db_exists(mdb_filename, db_name):
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

    with manager.get_environment(mdb_filename).begin() as txn:
        if txn.get(db_name.encode()) is None:
            if db_name.startswith('data_'):
                log_fct = logger.warning
            else:
                log_fct = logger.info
            log_fct('Requested db %s does not exist in %s',
                    db_name, mdb_filename)
            return False
    return True


def get_timestamp_of_first_sample(mdb_filename: str, sensor_name: str, last=False, get_value=False, pack_str=None):
    """
    Get the timestamp as datetime object of the first data-sample of `sensor_name` in `mdb_filename`, if any,
    otherwise None is returned.

    Parameters
    ----------
    mdb_filename : str
    sensor_name : str
    last : bool
        If True, timestamp of last sample is returned.
    get_value : bool
        If True, return value instead of timestamp.
    pack_str: str
        Passed to the `struct` package for values with multiple fields.

    Returns
    -------
    result : datetime or None

    """
    if _db_exists(mdb_filename, 'data_' + sensor_name):
        with manager.get_transaction(mdb_filename, 'data_' + sensor_name) as txn:
            c = txn.cursor()
            if last:
                cursor_move_successful = c.last()
            else:
                cursor_move_successful = c.first()

            if cursor_move_successful:
                if get_value:
                    if pack_str:
                        return struct.unpack(pack_str, c.value())
                    else:
                        return unpack(c.value(), pack_str=pack_str)[0]
                else:
                    return _key2datetime(c.key())


def get_sensor_stats(mdb_filename: str, sensor_name: str):
    d_s = {}
    with manager.get_transaction(mdb_filename, 'data_' + sensor_name) as txn:
        # noinspection PyArgumentList
        stat = txn.stat()
        d_s['entries'] = stat['entries']
        if stat['entries'] > 0:
            cursor = txn.cursor()
            if cursor.first():
                d_s['since'] = _key2datetime(cursor.key()).isoformat()
            if cursor.last():
                d_s['until'] = _key2datetime(cursor.key()).isoformat()
                d_s['last_value'] = get_timestamp_of_first_sample(mdb_filename, sensor_name,
                                                                  last=True, get_value=True)

    return d_s


def get_db_statistic(mdb_filename: str) -> dict:
    d = {}
    for sensor_name in get_sensor_names(mdb_filename):
        d_s = get_sensor_stats(mdb_filename, sensor_name)
        meta = get_metadata(mdb_filename, sensor_name)
        if meta:
            d_s['meta'] = meta
        d[sensor_name] = d_s

    try:
        filesize = os.stat(os.path.realpath(mdb_filename)).st_size
    except FileNotFoundError:
        filesize = -1

    return {'filename': os.path.realpath(mdb_filename),
            'filesize': filesize,
            'sensors': d}


# %% managing sensors
def get_sensor_names(mdb_filename):
    """
    Database names are stored as the keys in the root database.
    Return sensor_names that can be found in the root database.

    Parameters
    ----------
    mdb_filename : str
        mdb-file to use

    Returns
    -------
    sensor_names : List[str]
    """
    sensor_names = []
    with manager.get_environment(mdb_filename).begin() as txn:
        for key, val in txn.cursor():
            key_str = key.decode()
            if key_str.startswith('data_'):
                sensor_names.append(key_str[5:])
    return sensor_names


def get_dbs(mdb_filename: str) -> List[str]:
    dbs = []
    with manager.get_environment(mdb_filename).begin() as txn:
        for key, val in txn.cursor():
            dbs.append(key.decode())
    return dbs


def get_non_empty_sensor_names(mdb_filename: str) -> List[str]:
    stat = get_db_statistic(mdb_filename)

    result = []
    for key, val in stat['sensors'].items():
        if val['entries'] > 0:
            result.append(key)
    return result


def delete_sensor_from_db(mdb_filename: str, sensor_name: str):
    for prefix in ('data_', 'meta_'):
        db_name = prefix + sensor_name
        if _db_exists(mdb_filename, prefix + sensor_name):
            logger.info('deleting db %s from %s', db_name, mdb_filename)
            e = manager.get_environment(mdb_filename)
            db = e.open_db(db_name.encode())
            with e.begin(db=db, write=True) as txn:
                txn.drop(db=db)


# %% timeseries data
def write_sample(mdb_filename, sensor_name, date, value, only_if_changed=False, pack_str=None):
    """
    Writes a date/value pair for a given sensor to the mdb file.

    Parameters
    ----------
    mdb_filename : str
        Name of the mdb-file+
    sensor_name : str
        The database to use
    date : datetime
        Timestamp when the value was recorded
    value : float or Iterable[number]
        The value to store
    only_if_changed : bool
        Only write sample if it's value is different from the value in front of it (by timestamp).
    pack_str: str
        Passed to the `struct` package for values with multiple fields.

    Returns
    -------
    result : bool
        True if value was stored, False otherwise.
    """

    with manager.get_transaction(mdb_filename, 'data_' + sensor_name, write=True) as txn:
        if only_if_changed:
            cur = txn.cursor()
            if cur.set_range(_datetime2key(date)):  # seek to first timestamp greater or equal to date
                if cur.prev():  # move one back (if not already on first entry)
                    # assert: cur points to the place right before sample will be added in
                    last_value = unpack(cur.value(), pack_str=pack_str)[0]
                    if np.all(np.isclose(last_value, value)):
                        logger.debug(f'Skip ({date},{value}) for sensor "{sensor_name}," since value did not change')
                        return False
            else:
                if cur.last():
                    last_value = unpack(cur.value(), pack_str=pack_str)[0]
                    if np.all(np.isclose(last_value, value)):
                        logger.debug(f'Skip ({date},{value}) for sensor "{sensor_name}", since value did not change')
                        return False

        logger.debug(f'Write ({date},{value}) to sensor "{sensor_name}"')

        if isinstance(value, list) or isinstance(value, tuple):
            data = pack(value, pack_str=pack_str)
        else:
            data = pack((value,), pack_str=pack_str)
        result = txn.put(_datetime2key(date), data)

    # documentation says that sync is done every Transaction.commit, however it does not seem to happen when
    # mdb_filename is created with said Transaction. Thus sync manually.
    manager.get_environment(mdb_filename).sync()

    return result


def write_samples(mdb_filename, sensor_name, dates, values, pack_str=None):
    """
    Writes date/value pairs for a given sensor to the mdb file.

    Parameters
    ----------
    mdb_filename : str
        Name of the mdb-file
    sensor_name : str
        The database to use
    dates : Iterable[datetime]
        Timestamp when the value was recorded
    values : Iterable[float]
        The value to store
    pack_str: str
        Passed to the `struct` package for values with multiple fields.

    Returns
    -------
    result : bool
        True if all date/value-pairs were stored, False otherwise.
    """

    try:
        # noinspection PyTypeChecker
        n = len(dates)  # TypeError is raised when dates was an iterator
        logger.debug('Write %d samples to DB %s', n, sensor_name)
    except TypeError:
        logger.debug('Write samples to DB %s', sensor_name)

    result = True
    with manager.get_transaction(mdb_filename, 'data_' + sensor_name, write=True) as txn:
        for date, value in zip(dates, values):
            if isinstance(value, tuple) or isinstance(value, list):
                data = pack(value, pack_str=pack_str)
            else:
                data = pack((value,), pack_str=pack_str)
            ret = txn.put(_datetime2key(date), data)
            result = result and ret

    # documentation says that sync is done every Transaction.commit, however it does not seem to happen when
    # mdb_filename is created with said Transaction. Thus sync manually.
    manager.get_environment(mdb_filename).sync()

    return result


def get_samples(mdb_filename, sensor_name, since=None, until=None, decimate_to_s=None, limit=100000,
                keep_local_extrema=True, pack_str=None):
    """
    Reading a CSV file produced by this module and optionally down sample during reading.

    Down-sampling allows generating low-resolution plots of data that would not fit in memory as a whole.

    Parameters
    ----------
    mdb_filename : str
        File to read
    since : None or datetime or str
        If not None, data-points before `since` are ignored.
    until : None or datetime or str
        If not None, data-points after `until` are ignored.
    decimate_to_s : float or string
        Positive number denoting the desired minimum distance between samples.
        If 'auto', decimation will be set to exactly hit `limit` if there were more points than
        `limit` between `since` and `until` or no decimation will happen if there were fewer points.
    limit : int
        Abort when more than `limit` segments are about to be returned to avoid out-of-memory-problems.
        If `decimate_to_s` was an integer, data will be cropped.
        If `decimate_to_s` was 'auto', at most `limit` number of samples are returned by using decimation instead of
        cropping.
    sensor_name : str
        Defines the database to use
    keep_local_extrema : bool
        If true, three datapoints per segment are returned, the average, the local minimum and the local maximum.
    pack_str: str
        Passed to the `struct` package for values with multiple fields.

    Returns
    -------
    status : bool
        True if `limit` was not reached, False otherwise.

    date : list[datetime]
        List of datetime objects

    val : list[float] or list[list[float]]
        List of values
    """
    logger.debug('import %s from %s since %s until %s in steps of %s seconds and maximum of %s values.',
                 sensor_name,
                 mdb_filename,
                 'beginning of time' if since is None else since,
                 'now' if until is None else until,
                 np.NaN if decimate_to_s is None else decimate_to_s,
                 limit)

    if not _db_exists(mdb_filename, 'data_' + sensor_name):
        # shortcut for the case the sensor does not exist in the database
        return False, [], []

    # ensure since and until are datetime object if not None
    since = as_datetime(since, none_ok=True)
    until = as_datetime(until, none_ok=True)

    dates = []
    values = []

    with manager.get_transaction(mdb_filename, 'data_' + sensor_name) as txn:
        c = txn.cursor()
        if since is not None:
            status = c.set_range(_datetime2key(since))
        else:
            status = c.first()
        if not status:
            # no elements after `since` or no elements at all
            logger.debug('no elements after %s for sensor %s in %s',
                         since, sensor_name, mdb_filename)
            return status, [], []

        if decimate_to_s is not None and len(c.value()) == 4:
            if decimate_to_s == 'auto':
                assert isinstance(limit, int) and limit > 0
                if since is None:
                    since = get_timestamp_of_first_sample(mdb_filename, sensor_name)
                if until is None:
                    until = get_timestamp_of_first_sample(mdb_filename, sensor_name, last=True)
                duration = until - since
                decimate_to = duration / limit
            else:
                decimate_to = timedelta(seconds=float(decimate_to_s))

            n = 0  # number of values in accumulator
            for key, val in c:
                row_date = _key2datetime(key)

                if until is not None and row_date > until:
                    break

                if limit and len(dates) > limit + 2 * keep_local_extrema * limit:
                    status = False
                    break

                if n > 0:
                    if row_date >= stop_date:  # store
                        if keep_local_extrema:
                            times_tmp = (local_min_time, local_max_time, start_date + (row_date - start_date) * 0.5)
                            values_tmp = (local_min, local_max, acc / n)
                            indices = np.argsort(times_tmp)
                            dates.append(times_tmp[indices[0]])
                            dates.append(times_tmp[indices[1]])
                            dates.append(times_tmp[indices[2]])
                            values.append((values_tmp[indices[0]],))
                            values.append((values_tmp[indices[1]],))
                            values.append((values_tmp[indices[2]],))
                        else:
                            dates.append(start_date + (row_date - start_date) * 0.5)
                            values.append((acc / n,))
                        n = 0
                    else:  # accumulate values
                        value = unpack(val, pack_str=pack_str)[0]
                        if keep_local_extrema:
                            if value < local_min:
                                local_min = value
                                local_min_time = row_date
                            if value > local_max:
                                local_max = value
                                local_max_time = row_date
                            local_min = min(local_min, value)
                            local_max = max(local_max, value)
                        acc += value
                        n += 1

                if n == 0:  # new segment
                    start_date = row_date
                    stop_date = start_date + decimate_to
                    acc = unpack(val, pack_str=pack_str)[0]
                    if keep_local_extrema:
                        local_min = acc
                        local_max = acc
                        local_min_time = row_date
                        local_max_time = row_date
                    n = 1

        else:
            # faster loop when decimation is not needed
            for key, val in c:
                row_date = _key2datetime(key)

                if until is not None and row_date > until:
                    break

                if limit and len(dates) > limit:
                    status = False
                    break

                dates.append(row_date)
                values.append(unpack(val, pack_str=pack_str))

    logger.info('read %d data points from %s', len(dates), mdb_filename)

    return status, dates, values


def delete_sensor_values(mdb_filename: str, sensor_name: str, since=None, until=None):
    """
    Deletes sensor values which's timestamp fall in between 'since' (included) and 'until' (included).

    Parameters
    ----------
    mdb_filename : str
    sensor_name : str
    since : str or datetime
        If None, since it will be set to first sample.
    until : str or datetime
        If None, since it will be set to last sample.
    """
    since = as_datetime(since, none_ok=True)
    until = as_datetime(until, none_ok=True)

    if since is None:
        since = get_timestamp_of_first_sample(mdb_filename, sensor_name)
    elif since > get_timestamp_of_first_sample(mdb_filename, sensor_name, last=True):
        return False

    if until is None:
        until = get_timestamp_of_first_sample(mdb_filename, sensor_name, last=True)
    elif until < get_timestamp_of_first_sample(mdb_filename, sensor_name):
        return False

    if not since <= until:
        raise RuntimeError(f'{since} is not before {until}')

    logger.info('Delete values of sensor %s from file %s since %s until %s.',
                sensor_name, mdb_filename, since.isoformat(), until.isoformat())

    db_name = 'data_' + sensor_name
    result = True
    if _db_exists(mdb_filename, db_name):
        with manager.get_transaction(mdb_filename, db_name, write=True) as txn:
            c = txn.cursor()
            c.first()
            if c.set_range(_datetime2key(since)):
                # Hint: c.delete() return True on success and iterates one item further
                # End is deteced when 'key' becomes an empty bytearray.
                key = c.key()
                while key and _key2datetime(c.key()) <= until:
                    # loop until no more key before 'until' exists or deletion fails
                    result = c.delete()
                    if not result:
                        break
                    key = c.key()

            else:
                return False
    return result


def copy_sensor_data(import_mdb_filename: str, export_mdb_filename: str,
                     import_sensor_name: str, export_sensor_name: str,
                     move: bool = False):
    """
    Copy or move sensor data of a given sensor from one database file to another.
    """
    for prefix in ('data_', 'meta_'):
        if not _db_exists(import_mdb_filename, prefix + import_sensor_name):
            if prefix == 'data_':
                logger.error('Sensor %s has no record in %s',
                             import_sensor_name, import_mdb_filename)
                # no warning since for meta-data presence is not required
        else:
            # todo: check memory usage of this nested with-construct
            with manager.get_transaction(export_mdb_filename, prefix + export_sensor_name, write=True) as txn_out:
                with manager.get_transaction(import_mdb_filename, prefix + import_sensor_name) as txn_in:
                    c = txn_in.cursor()
                    for key, val in c:
                        txn_out.put(key, val)

    if move:
        delete_sensor_from_db(import_mdb_filename, import_sensor_name)


# %% metadata

def write_metadata(mdb_filename: str, sensor_name: str, d: dict):
    """Save a dictionary of auxiliary sensor data, like "label" or "unit" to the given database"""
    ret = []
    with manager.get_transaction(mdb_filename, 'meta_' + sensor_name, write=True) as txn:
        for key, val in d.items():
            ret.append(txn.put(key.encode(), yaml.safe_dump(val).encode()))
    return ret


def delete_metadata(mdb_filename: str, sensor_name: str = None, key: str = None):
    if key is None:
        # delete metadata completely
        db_name = 'meta_' + sensor_name
        if _db_exists(mdb_filename, 'meta_' + sensor_name):
            logger.info('deleting db %s from %s', db_name, mdb_filename)
            e = manager.get_environment(mdb_filename)
            db = e.open_db(db_name.encode())
            with e.begin(db=db, write=True) as txn:
                txn.drop(db=db)
    else:
        # delete a key
        with manager.get_transaction(mdb_filename, 'meta_' + sensor_name, write=True) as txn:
            txn.delete(key.encode())


def get_metadata(mdb_filename: str, sensor_name: str) -> dict:
    """
    Reads metadata from file, inverse to `write_metadata`.
    Return None, if no metadata for `sensor_name` exists.
    """
    if _db_exists(mdb_filename, 'meta_' + sensor_name):
        # access database
        with manager.get_transaction(mdb_filename, 'meta_' + sensor_name, write=True) as txn:
            d = {}
            for key, val in txn.cursor():
                d[key.decode()] = yaml.safe_load(val)
            logger.debug('Got Metadata for %s: %s', sensor_name, d)
            return d


# %% Notes
def write_note(mdb_filename: str, short: str, long: str = None, timestamp: datetime = None) -> bool:
    if timestamp is None:
        timestamp = datetime.now()

    data = {'short': short}
    if long:
        data['long'] = long

    with manager.get_transaction(mdb_filename, 'notes', write=True) as txn:
        status = txn.put(_datetime2key(timestamp),
                         yaml.safe_dump(data).encode())

    return status


def get_notes(mdb_filename: str, since: datetime = None, until: datetime = None) -> dict:
    """
    Get notes from a given timespan as dict with key as string in isoformat timestamp.
    """

    until = as_datetime(until, none_ok=True)

    d = {}
    if _db_exists(mdb_filename, 'notes'):
        with manager.get_transaction(mdb_filename, 'notes') as txn:
            c = txn.cursor()
            if since is not None:
                since = as_datetime(since)
                status = c.set_range(_datetime2key(since))
            else:
                status = c.first()
            if status:
                for key, val in c:
                    timestamp = _key2datetime(key)
                    if until is not None and timestamp > until:
                        break
                    else:
                        d[timestamp.isoformat()] = yaml.safe_load(val)

    return d


def delete_note(mdb_filename, timestamp):
    """

    Parameters
    ----------
    mdb_filename : str
    timestamp : str or datetime

    Returns
    -------
    status : bool
        True if delete was successful, False otherwise.

    """
    timestamp = as_datetime(timestamp, none_ok=False)
    with manager.get_transaction(mdb_filename, 'notes', write=True) as txn:
        status = txn.delete(_datetime2key(timestamp))
    return status

