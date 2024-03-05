from datetime import datetime
import json
import re
import struct
from typing import Union, Iterable, Mapping, SupportsFloat, SupportsInt, Tuple, Any
import warnings
import yaml
from lmdb_sensor_storage._parser import as_datetime


class Packer:
    def __init__(self):
        pass

    def pack(self, x) -> bytes:
        raise NotImplementedError

    def unpack(self, x: bytes):
        raise NotImplementedError


class BytesPacker(Packer):

    def pack(self, x: bytes) -> bytes:
        return x

    def unpack(self, x: bytes):
        return x


class StringPacker(Packer):

    def pack(self, x: Any) -> bytes:
        data = str(x)
        return data.encode()

    def unpack(self, x: bytes) -> str:
        return x.decode()


class DatetimePacker(Packer):

    def pack(self, d: datetime) -> bytes:
        return struct.pack('!Q', int(as_datetime(d).timestamp() * 1e6))

    def unpack(self, key: bytes) -> datetime:
        return datetime.fromtimestamp(struct.unpack('!Q', key)[0] / 1e6)


class JSONPacker(Packer):

    def pack(self, x: Union[bytes, str, Iterable, dict]) -> bytes:

        if isinstance(x, bytes):
            x = x.decode()
        if isinstance(x, str):
            try:
                data = json.loads(x)
            except json.JSONDecodeError:
                raise ValueError(f'Data {x} is no valid json')
            else:
                return json.dumps(data).encode()

        if isinstance(x, Mapping):
            non_string = []
            for key in x.keys():
                if not isinstance(key, str):
                    non_string.append(key)
            if non_string:
                warnings.warn(f'keys {non_string} will be converted to string keys', RuntimeWarning)
        return json.dumps(x).encode()

    def unpack(self, x) -> Any:
        return json.loads(x.decode())


class YamlPacker(Packer):
    def pack(self, x: Any) -> bytes:

        if isinstance(x, bytes):
            x = x.decode()
        if isinstance(x, str):
            try:
                data = yaml.safe_load(x)
            except yaml.YAMLError:
                raise ValueError(f'Data {x} is no valid json')
            else:
                return yaml.safe_dump(data).encode()

        return yaml.safe_dump(x).encode()

    def unpack(self, x: bytes) -> Any:
        return yaml.safe_load(x.decode())


class FloatPacker(Packer):

    def pack(self, x: Union[bytes, str, SupportsFloat]) -> bytes:
        return struct.pack('f', float(x))

    def unpack(self, x: bytes) -> float:
        return struct.unpack('f', x)[0]


class IntPacker(Packer):

    def pack(self, x: Union[bytes, str, SupportsInt]) -> bytes:
        return struct.pack('h', int(x))

    def unpack(self, x: bytes) -> int:
        return struct.unpack('h', x)[0]


class StructPacker(Packer):

    def __init__(self, fmt_str: str):
        super().__init__()
        self._fmt_str = str(fmt_str)
        self._num_fields = len(struct.unpack(self._fmt_str, b'0' * struct.calcsize(self._fmt_str)))

    def pack(self, x: Iterable) -> bytes:
        return struct.pack(self._fmt_str, *x)

    def unpack(self, x: bytes) -> Tuple[Any, ...]:
        return struct.unpack(self._fmt_str, x)


class FloatsPacker(Packer):

    def pack(self, x: Iterable):
        data = [float(d) for d in x]
        return struct.pack(f'{len(data)}f', *data)

    def unpack(self, x: bytes) -> Tuple[float, ...]:
        return struct.unpack(f'{len(x)//4}f', x)


class RegexPacker(StringPacker):

    def pack(self, x: str) -> bytes:
        try:
            re.compile(x)
        except re.error as e:
            raise e
        return super().pack(x)
