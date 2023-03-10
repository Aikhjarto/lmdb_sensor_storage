from datetime import datetime
import struct
import os
from lmdb_sensor_storage.db import manager, StringYamlDB, TimestampBytesDB, TimestampStringDB, TimestampYAMLDB, \
    StringRegexpDB
from lmdb_sensor_storage._parser import as_datetime
from lmdb_sensor_storage._packer import BytesPacker, StringPacker, JSONPacker, FloatPacker, \
    StructPacker
from typing import Mapping, List, Sequence
import logging

logger = logging.getLogger('lmdb_sensor_storage.storage')


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
        Stores user-defined metadata. Keys 'label', 'unit', 'plot_min_val', 'plot_max_val', and 'group' are used for plotting.

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
            self.data_format = self._format_db.get_last_value()

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
    def data_format(self, data_format):

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

        # store backend-selection
        self._format_db.write_value(datetime.now(), data_format, only_if_value_changed=True)
        self._data_format = data_format

    def __setitem__(self, key, value):
        if not self.data_format:
            self.data_format = guess_format_string(value)
        return super().__setitem__(key, value)

    def write_value(self, date, value, only_if_value_changed=False):
        if not self.data_format:
            self.data_format = guess_format_string(value)
        return super().write_value(date, value, only_if_value_changed=only_if_value_changed)

    def write_values(self, dates, values):
        if not self.data_format:
            self.data_format = guess_format_string(values[0])
        return super().update([(x, y) for x, y in zip(dates, values)])

    def copy_to(self, new_sensor_name, export_mdb_filename=None):
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

    def add_note(self, short, long=None, timestamp=None):
        if timestamp is None:
            timestamp = datetime.now()
        else:
            timestamp = as_datetime(timestamp)

        data = {'short': short}
        if long:
            data['long'] = long

        self[timestamp] = data


class Notes(TimestampNotesDB):
    def __init__(self, mdb_filename):
        super().__init__(mdb_filename, 'notes')


class GroupDefinitions(StringRegexpDB):
    def __init__(self, mdb_filename):
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

    def _get(self, what='keys'):
        with manager.get_environment(self._mdb_filename).begin() as txn:
            for key, val in txn.cursor():
                key_str = key.decode()
                if key_str.startswith('data_'):
                    if what == 'values':
                        yield Sensor(self._mdb_filename, key_str[5:])
                    elif what == 'keys':
                        yield key_str[5:]
                    elif what == 'items':
                        yield key_str[5:], Sensor(self._mdb_filename, key_str[5:])
                    else:
                        raise NotImplementedError

    def __getitem__(self, name) -> Sensor:
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

    def __contains__(self, name):
        for key in self.__iter__():
            if name == key:
                return True
        return False

    def __len__(self):
        return len(tuple(self.__iter__()))

    def __delitem__(self, name):
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
            d[sensor_name] = d_s
        return d


class LMDBSensorStorage(Sensors):

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

    def get_non_empty_sensor_names(self):
        stat = self.statistics

        result = []
        for key, val in stat['sensors'].items():
            if val['entries'] > 0:
                result.append(key)
        return result

    def get_non_empty_sensors(self):
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

    def close(self):
        manager.close(self._mdb_filename)


def try_float(x):
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
