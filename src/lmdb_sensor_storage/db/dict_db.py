import logging
import os
from typing import MutableMapping, Union, Mapping, Sequence, Tuple, Any, Iterable

import lmdb
from typing_extensions import Literal

from lmdb_sensor_storage.db._manager import manager
from lmdb_sensor_storage.db.packer import Packer, BytesPacker, StringPacker, RegexPacker, JSONPacker, YamlPacker

logger = logging.getLogger('lmdb_sensor_storage.db')


class LMDBDict(MutableMapping):
    """
    Key-value database based on LMDB where both are bytes-objects.

    Keys and values can be accessed similar to a dictionary.
    """
    _value_packer: Packer

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

    def _get(self, what: Literal['keys', 'values', 'items'] = 'keys'):
        if manager.db_exists(self._mdb_filename, self._db_name):
            with manager.get_transaction(self._mdb_filename, self._db_name) as txn:
                c = txn.cursor()
                if c.first():
                    if what == 'keys':
                        yield self._key_packer.unpack(c.key())
                        while c.next():
                            yield self._key_packer.unpack(c.key())
                    elif what == 'values':
                        yield self._value_packer.unpack(c.value())
                        while c.next():
                            yield self._value_packer.unpack(c.value())
                    elif what == 'items':
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

    def popitem(self):
        """Return key/value tuple of last item inserted"""

        if len(self) == 0:
            raise KeyError('`popitem(): DB is empty')
        key = None
        value = None
        if manager.db_exists(self._mdb_filename, self._db_name):
            with manager.get_transaction(self._mdb_filename, self._db_name, write=True) as txn:
                c = txn.cursor()
                if c.last():
                    key = self._key_packer.unpack(c.key())
                    value = self._value_packer.unpack(c.value())
                    if not c.delete():
                        raise IOError

        if key is None:
            raise KeyError('`popitem(): DB is empty')

        return key, value

    def clear(self):
        for key in self.keys():
            del self[key]

    def setdefault(self, key, default=None):
        """
        Sets key to value "default", if key does not yet exist.
        """
        if key in self:
            return self[key]
        else:
            self[key] = default
            return default

    def __eq__(self, other):
        if len(self) != len(other):
            return False
        for key, val in self.items():
            if val != other[key]:
                return False
        return True

    def __ne__(self, other):
        if len(self) != len(other):
            return True
        for key, val in self.items():
            if val != other[key]:
                return True

        return False

    def as_dict(self):
        d = {}
        for key, val in self.items():
            d[key] = val
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

    def copy_to(self, new_db_name, export_mdb_filename=None):
        """
        Copies a Sensor to be a new location.
        If the desitnation already exists, a RuntimeError is raised.
        """
        if not export_mdb_filename:
            export_mdb_filename = self.mdb_filename
        export_mdb_filename = os.path.realpath(export_mdb_filename)

        if manager.db_exists(export_mdb_filename, new_db_name):
            raise RuntimeError(f'DB "{new_db_name}" already exists in "{export_mdb_filename}"')
        else:
            # Note: all db handles must be genereated befor a transaction is created,
            # thus manager.get_transaction cannot be used here
            db_out = manager.get_db(export_mdb_filename, new_db_name)
            db_in = manager.get_db(self.mdb_filename, self._db_name)
            with lmdb.Transaction(manager.get_environment(export_mdb_filename), db=db_out, write=True) as txn_out:
                with lmdb.Transaction(manager.get_environment(self._mdb_filename), db=db_in) as txn_in:
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
