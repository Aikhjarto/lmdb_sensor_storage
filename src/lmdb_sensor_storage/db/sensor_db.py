from array import array
from datetime import datetime, timezone
import math
from io import BytesIO
import json
import struct
import os
import simplejson
from lmdb_sensor_storage.db.timestamp_db import TimestampBytesDB, TimestampStringDB, TimestampYAMLDB
from lmdb_sensor_storage.db.dict_db import StringRegexpDB, StringYamlDB
from lmdb_sensor_storage.db._manager import manager
from lmdb_sensor_storage._parser import as_datetime, as_local_datetime
from lmdb_sensor_storage.db.packer import BytesPacker, StringPacker, JSONPacker, FloatPacker, \
    StructPacker
from typing import Mapping, List, Sequence, Union, Any, Dict, Callable, Iterable, Iterator, Tuple
from typing_extensions import Literal
import logging

logger = logging.getLogger('lmdb_sensor_storage.db')


class Sensor(TimestampBytesDB):
    """
    A Sensor consists of four databases in `mdb_filename`
    'data_' + sensor_name is a TimestampStringDB containing timeseries data
    'meta_' + sensor_name is a StringYAMLDB
    'format_' + sensor_name is a TimestampStringDB containing information on how to decode the timeseries data.
    'notes_' + sensor_name is a TimestampNotesDB

    The format for decoding the timeseries data is either set in the constructor as data_format, read from the
    format-database or guessed from the next  write access to the timeseries data.

    Properties
    ----------
    metadata : lmdb_sensor_storage.db.LMDBDict
        Stores user-defined metadata. Keys 'label', 'unit', 'plot_min_val', 'plot_max_val', and 'group' are used for
        plotting.

    sensor_name : str

    data_format : str

    notes : lmdb_sensor_storage.db.TimestampNotesDB
    """

    def __init__(self, mdb_filename, sensor_name, data_format: str = None):
        self._mdb_filename = os.path.realpath(mdb_filename)

        super().__init__(mdb_filename, 'data_' + sensor_name)
        self._sensor_name = sensor_name

        self._format_db = TimestampStringDB(mdb_filename, 'format_' + sensor_name)

        if data_format:
            self.data_format = data_format
        else:
            # format is already stored, thus only select the packer
            self._set_value_packer(self._format_db.get_last_value())

        self._metadata = StringYamlDB(mdb_filename, 'meta_' + sensor_name)

        self._notes_db = TimestampNotesDB(self._mdb_filename, 'notes_' + sensor_name)

    def __hash__(self):
        return hash((self._mdb_filename, self._sensor_name))

    def __repr__(self):
        return f'{self._mdb_filename}:{self._sensor_name}, ' \
               f'data_format: {self.data_format}, ' \
               f'{self.statistics}'

    @property
    def metadata(self):
        return self._metadata

    @property
    def notes(self):
        return self._notes_db

    @property
    def sensor_name(self):
        return self._sensor_name

    @property
    def data_format(self):
        return self._data_format

    @data_format.setter
    def data_format(self, data_format: Union[str, None]):
        self._set_value_packer(data_format)
        if data_format is not None:
            # store backend-selection
            self._format_db.write_value(datetime.now(timezone.utc), data_format, only_if_value_changed=True)

    def _set_value_packer(self, data_format: Union[str, None]):

        if data_format is None:
            self._value_packer = None
            self._data_format = None
            return

        # select backend pedending on data-type
        if data_format == 'json':
            self._value_packer = JSONPacker()
        elif data_format == 'str':
            self._value_packer = StringPacker()
        elif data_format == 'bytes':
            self._value_packer = BytesPacker()
        elif data_format == 'f':
            self._value_packer = FloatPacker()
        elif isinstance(data_format, str):
            assert struct.calcsize(data_format) > 0
            self._value_packer = StructPacker(data_format)
        else:
            raise NotImplementedError(f'No backend for data_format {data_format}')

        self._data_format = data_format

    def __setitem__(self, key: datetime, value):
        if not self.data_format:
            self.data_format = guess_format_string(value)
        return super().__setitem__(key, value)

    def write_value(self, date: datetime, value, only_if_value_changed: bool = False, max_age_seconds: float = None):
        if not self.data_format:
            self.data_format = guess_format_string(value)
        return super().write_value(date, value,
                                   only_if_value_changed=only_if_value_changed,
                                   max_age_seconds=max_age_seconds)

    def write_values(self, dates: Sequence[datetime], values: Sequence[Any]):
        if not self.data_format:
            self.data_format = guess_format_string(values[0])
        return super().update([(x, y) for x, y in zip(dates, values)])

    def copy_to(self, new_sensor_name: str, export_mdb_filename: str = None):
        """
        Copies a Sensor to be a new location.
        If the desitnation already exists, a RuntimeError is raised.
        """
        if not export_mdb_filename:
            export_mdb_filename = self.mdb_filename

        for prefix in ('data_', 'meta_', 'format_', 'notes_'):
            if manager.db_exists(export_mdb_filename, prefix + new_sensor_name):
                raise RuntimeError('Destination sensor already exists!')

        super().copy_to('data_' + new_sensor_name, export_mdb_filename)
        self._metadata.copy_to('meta_' + new_sensor_name, export_mdb_filename)
        self._format_db.copy_to('format_' + new_sensor_name, export_mdb_filename)
        self._notes_db.copy_to('notes_' + new_sensor_name, export_mdb_filename)


class TimestampNotesDB(TimestampYAMLDB):
    def __init__(self, mdb_filename, db_name):
        super().__init__(mdb_filename, db_name)

    def add_note(self, short: str, long: str = None, timestamp: Union[str, datetime, int, float] = None):
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        else:
            timestamp = as_datetime(timestamp)

        data = {'short': short}
        if long:
            data['long'] = long

        self[timestamp] = data

    def __setitem__(self, key, value: Union[str, Dict[str, str]]):
        if isinstance(value, str):
            data = {'short': value}
            super().__setitem__(key, data)
        elif isinstance(value, dict):
            assert 'short' in value, 'Notes dictionary must have key "short"'
            super().__setitem__(key, value)
        else:
            raise TypeError('only string or dictionary are allowed')


class Notes(TimestampNotesDB):
    def __init__(self, mdb_filename: str):
        super().__init__(mdb_filename, 'notes')


class GroupDefinitions(StringRegexpDB):
    def __init__(self, mdb_filename: str):
        super().__init__(mdb_filename, 'plot_groups')


class Sensors(Mapping):

    def __init__(self, mdb_filename):
        super().__init__()
        self._mdb_filename = os.path.realpath(mdb_filename)

    def __repr__(self):
        return f'Mapping of {len(self.keys())} sensors of file {self._mdb_filename}'

    @property
    def mdb_filename(self):
        return self._mdb_filename

    def _get(self, what: Literal['values', 'keys', 'items'] = 'keys'):
        # Note: get the names before creating Sensor objects, which open databases, since opening databases might
        # require to reopen the environment, which is not possible while a transaction is active
        with manager.begin(self._mdb_filename) as txn:
            names = [key.decode()[5:] for key, val in txn.cursor() if key.startswith(b'data_')]
        for name in names:
            if what == 'values':
                yield Sensor(self._mdb_filename, name)
            elif what == 'keys':
                yield name
            elif what == 'items':
                yield name, Sensor(self._mdb_filename, name)
            else:
                raise NotImplementedError

    def __getitem__(self, name: str) -> Sensor:
        if not isinstance(name, str):
            raise TypeError
        return Sensor(self._mdb_filename, name)

    def __iter__(self):
        return self._get(what='keys')

    def keys(self) -> List[str]:
        return list(self.__iter__())

    def values(self) -> List[Sensor]:
        return list(self._get(what='values'))

    def items(self):
        return list(self._get(what='items'))

    def __contains__(self, name: str):
        for key in self.__iter__():
            if name == key:
                return True
        return False

    def __len__(self):
        return len(tuple(self.__iter__()))

    def __delitem__(self, name: str):
        for prefix in ('data_', 'meta_', 'format_', 'notes_'):
            db_name = prefix + name
            manager.delete_db(self._mdb_filename, db_name)

    @property
    def statistics(self):
        d = {}
        for sensor_name, sensor in self.items():
            d_s = sensor.statistics
            meta = sensor.metadata
            if meta:
                d_s['meta'] = meta.as_dict()
            d_s['data_format'] = sensor.data_format
            d[sensor_name] = d_s
        return d


class LMDBSensorStorage(Sensors):

    # samples per sensor read at once for exports, which bounds memory and the duration of read transactions
    EXPORT_WINDOW = 50000
    # rows of exports passed at once to the buffer function
    EXPORT_WRITE_ROWS = 10000

    def __init__(self, mdb_filename):
        super().__init__(mdb_filename)
        self._notes_db = Notes(self._mdb_filename)
        self._plot_groups_db = GroupDefinitions(self.mdb_filename)

    @property
    def notes(self):
        return self._notes_db

    @property
    def plot_groups(self):
        return self._plot_groups_db

    @property
    def _environment(self):
        return manager.handles[os.path.realpath(self._mdb_filename)]

    @property
    def statistics(self):
        try:
            filesize = os.stat(self._mdb_filename).st_size
        except FileNotFoundError:
            filesize = -1

        return {'filename': self._mdb_filename,
                'filesize': filesize,
                'sensors': super().statistics}

    def get_non_empty_sensor_names(self) -> List[str]:
        stat = self.statistics

        result = []
        for key, val in stat['sensors'].items():
            if val['entries'] > 0:
                result.append(key)
        return result

    def get_non_empty_sensors(self) -> List[Sensor]:
        return [Sensor(self._mdb_filename, sensor_name) for sensor_name in self.get_non_empty_sensor_names()]

    def get_node_red_graph_data(self, sensor_names: Sequence[str], **kwargs):
        """
        Return sensor data in a format that can be imported to NodeRed's charts.
        https://github.com/node-red/node-red-dashboard/blob/master/Charts.md

        **kwargs are forwarded to Sensor.items()
        """
        series = list()
        data = list()
        for sensor_name in sensor_names:
            sensor = Sensor(self._mdb_filename, sensor_name)
            tmp = []
            for stamp, value in sensor.items(**kwargs):
                tmp.append({'x': int(stamp.timestamp()*1000), 'y': value})
            series.append(sensor.sensor_name)
            data.append(tmp)
        return {'series': series, 'data': data, 'labels': [""]}

    def _check_sensor_names(self, sensor_names: Iterable[str]):
        """Raise KeyError, if a sensor does not exist, e.g. because of a misspelled name."""
        existing = set(self.keys())
        missing = [name for name in sensor_names if name not in existing]
        if missing:
            raise KeyError(f'Sensors {missing} do not exist in {self._mdb_filename}')

    @staticmethod
    def _export_kwargs(timespan_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Arguments since, until and limit for exports; other arguments, e.g. decimate_to_s, are not supported."""
        unsupported = set(timespan_kwargs) - {'since', 'until', 'limit'}
        if unsupported:
            raise ValueError(f'{sorted(unsupported)} are not supported for exports')
        return timespan_kwargs

    def _export_rows(self, sensors: Sequence[Sensor], since=None, until=None, limit: int = None) \
            -> Iterator[Tuple[datetime, List[Any]]]:
        """
        Yields a row (timestamp, values) for each timestamp of any of `sensors` from `since` until `until`, which is
        excluded, at most `limit` rows.
        The values are the most recent value of each sensor at or before the timestamp, or None before its first
        sample.

        Data is read in windows of EXPORT_WINDOW samples per sensor, each in its own read transaction, so memory and
        the duration of read transactions are bounded, independent of the number of samples exported.
        """
        since = as_datetime(since, none_ok=True)
        until = as_datetime(until, none_ok=True)
        # values held from before the first exported timestamp
        values = [None if since is None else s.get_value_before(since) for s in sensors]
        cursor = since  # read from here, including
        done = None  # all timestamps up to here, including, were exported
        count = 0
        while True:
            batches = []
            window_end = None
            # the first sample read after the first window is the last one exported, which is skipped
            read_limit = self.EXPORT_WINDOW if done is None else self.EXPORT_WINDOW + 1
            for s in sensors:
                batch = s.items(since=cursor, until=until, limit=read_limit)
                truncated = len(batch) == read_limit
                if done is not None:
                    batch = [item for item in batch if item[0] > done]
                if truncated:
                    # the sensor has more samples, thus timestamps after its last one are not complete in this window
                    if window_end is None or batch[-1][0] < window_end:
                        window_end = batch[-1][0]
                batches.append(batch)
            if window_end is not None:
                batches = [[item for item in batch if item[0] <= window_end] for batch in batches]

            positions = [0] * len(sensors)
            for timestamp in sorted({item[0] for batch in batches for item in batch}):
                for i, batch in enumerate(batches):
                    if positions[i] < len(batch) and batch[positions[i]][0] == timestamp:
                        values[i] = batch[positions[i]][1]
                        positions[i] += 1
                yield timestamp, list(values)
                count += 1
                if limit is not None and count >= limit:
                    return

            if window_end is None:
                return
            cursor = done = window_end

    # noinspection PyProtectedMember
    def get_csv(self, sensor_names: Sequence[str], buffer_function: Callable = None,
                include_header: bool = False, **timespan_kwargs):
        """
        CSV with a row for each timestamp of any of the sensors, see `_export_rows`. If `buffer_function` is given,
        the CSV is passed to it in parts, otherwise it is returned.
        Supported arguments for the time span are `since`, `until` (excluded), and `limit`.
        """

        if buffer_function is None:
            buffer = BytesIO()
            self.get_csv(sensor_names, buffer.write, include_header=include_header, **timespan_kwargs)
            buffer.seek(0)
            return buffer.read()
        else:
            timespan_kwargs = self._export_kwargs(timespan_kwargs)
            self._check_sensor_names(sensor_names)
            sensors = [self[s] for s in sensor_names]
            if include_header:
                tmp = ['"Time"',]
                for s in sensors:
                    if isinstance(s._value_packer, StructPacker):
                        num_fields = s._value_packer._num_fields
                        field_names = s.metadata.get('field_names', ())
                        if len(field_names) != num_fields:
                            tmp.extend([f'"{s.sensor_name} Field {i}"' for i in range(num_fields)])
                        else:
                            tmp.extend([f'"{s.sensor_name} {i}"' for i in field_names])
                    else:
                        tmp.append(f'"{s.sensor_name}"')
                header_line = f"{';'.join(tmp)}\n".encode()
                buffer_function(header_line)

            # fields of sensors without value yet, struct values have several fields
            empty = [';' * (s._value_packer._num_fields - 1) if isinstance(s._value_packer, StructPacker) else ''
                     for s in sensors]
            lines = []
            for key, values in self._export_rows(sensors, **timespan_kwargs):
                tmp = [as_local_datetime(key).isoformat(), ]
                for k, value in enumerate(values):
                    if value is None:
                        tmp.append(empty[k])
                    elif isinstance(sensors[k]._value_packer, StructPacker):
                        tmp.append(';'.join([str(j) for j in value]))
                    elif isinstance(sensors[k]._value_packer, StringPacker):
                        tmp.append(f'"{value}"')
                    else:
                        tmp.append(str(value))
                lines.append(f"{';'.join(tmp)}\n")
                if len(lines) >= self.EXPORT_WRITE_ROWS:
                    buffer_function(''.join(lines).encode())
                    lines = []
            if lines:
                buffer_function(''.join(lines).encode())

    def get_json(self, sensor_names: Iterable[str],
                 buffer_function: Callable = None,
                 **timespan_kwargs) -> Union[str, None]:
        """
        JSON with the timestamps of any of the sensors and the values of each sensor at these timestamps, see
        `_export_rows`, missing values are null. If `buffer_function` is given, the JSON is passed to it in parts,
        otherwise it is returned.
        Supported arguments for the time span are `since`, `until` (excluded), and `limit`.
        """

        if not buffer_function:
            buffer = BytesIO()
            self.get_json(sensor_names, buffer.write, **timespan_kwargs)
            buffer.seek(0)
            return buffer.read()
        else:
            timespan_kwargs = self._export_kwargs(timespan_kwargs)
            self._check_sensor_names(sensor_names)
            sensors = [self[s] for s in sensor_names]

            # The timestamps are streamed, while the values are collected, since they follow all timestamps.
            # Floats are collected as array of doubles, which needs much less memory than a list of floats,
            # missing values as NaN, which are written as null like NaN values.
            columns = [array('d') if s.data_format == 'f' else [] for s in sensors]
            buffer_function('{"Time":['.encode())
            times = []
            separator = ''
            for key, values in self._export_rows(sensors, **timespan_kwargs):
                times.append(f'"{as_local_datetime(key).isoformat()}"')
                for column, value in zip(columns, values):
                    column.append(math.nan if value is None and isinstance(column, array) else value)
                if len(times) >= self.EXPORT_WRITE_ROWS:
                    buffer_function((separator + ','.join(times)).encode())
                    separator = ','
                    times = []
            if times:
                buffer_function((separator + ','.join(times)).encode())
            buffer_function(']'.encode())

            for sensor_name, column in zip(sensor_names, columns):
                buffer_function(f',{json.dumps(sensor_name)}:{{"values":['.encode())
                separator = ''
                for i in range(0, len(column), self.EXPORT_WRITE_ROWS):
                    part = simplejson.dumps(list(column[i:i + self.EXPORT_WRITE_ROWS]), ignore_nan=True)[1:-1]
                    buffer_function((separator + part).encode())
                    separator = ', '
                buffer_function(']'.encode())
                metadata = self[sensor_name].metadata.as_dict()
                if metadata:
                    buffer_function(',"metadata":'.encode())
                    buffer_function(json.dumps(metadata).encode())

                notes = self[sensor_name].notes.items(**timespan_kwargs)
                if notes:
                    buffer_function(',"notes":['.encode())
                    buffer_function(','.join([f'{{"{as_local_datetime(note[0]).isoformat()}": {json.dumps(note[1])}}}'
                                              for note in notes]).encode())
                    buffer_function(']'.encode())
                buffer_function("}".encode())

            notes = self.notes.items(**timespan_kwargs)
            if notes:
                buffer_function(',"notes":['.encode())
                buffer_function(','.join([f'{{"{as_local_datetime(note[0]).isoformat()}": {json.dumps(note[1])}}}'
                                          for note in notes]).encode())
                buffer_function(']'.encode())

            buffer_function('}'.encode())

    def close(self):
        manager.close(self._mdb_filename)


def try_float(x) -> bool:
    try:
        float(x)
        return True
    except (TypeError, ValueError):
        return False


def guess_format_string(d) -> str:
    if try_float(d):
        return 'f'

    if isinstance(d, dict):
        return 'json'

    if isinstance(d, str):
        if try_float(d):
            return 'f'
        return 'str'

    if isinstance(d, bytes):
        if try_float(d):
            return 'f'
        return 'bytes'

    if hasattr(d, '__iter__'):
        try:
            fmts = [float(x) for x in d]
            return f'{len(fmts)}f'
        except ValueError:
            pass

    return 'json'
