#!/usr/bin/env python3
import argparse
import os
from datetime import datetime, timedelta

# NAGIOS return codes :
# https://nagios-plugins.org/doc/guidelines.html#AEN78
OK = 0
WARNING = 1
CRITICAL = 2
UNKNOWN = 3

try:
    from lmdb_sensor_storage.db.sensor_db import Sensor
except ImportError:
    print('UNKNOWN - cannot find lmdb_sensor_storage module')
    exit(UNKNOWN)


def setup_parser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser()

    parser.add_argument('-c', type=str, required=True,
                        help='If age of the last sample in seconds is higher than `C`, state is CRITICAL')

    parser.add_argument('-w', type=str, required=True,
                        help='If age of the last sample in seconds is higher than `W`, state is WARNING')

    parser.add_argument('--last-change', action='store_true', default=False)

    parser.add_argument('--mdb-filename', type=str, required=True)

    parser.add_argument('--sensor-name', type=str, required=True, action='append')

    return parser


def main():

    p = setup_parser()
    args = p.parse_args()

    timedelta_warning = abs(timedelta(seconds=float(args.w)))
    timedelta_critical = abs(timedelta(seconds=float(args.c)))

    # ugly hack to correct wrongfully encoded character when using this script with the nagios plugin check_by_ssh
    sensor_names = []
    for sensor_name in args.sensor_name:
        tmp = []
        for char in sensor_name:
            if ord(char) & 0xdc00:
                tmp.append(chr(ord(char)-0xdc00))
            else:
                tmp.append(char)
        sensor_names.append(''.join(tmp))

    if timedelta_warning >= timedelta_critical:
        raise RuntimeError('Warning age must be smaller than critical age')

    if not os.path.isfile(args.mdb_filename):
        print(f'UNKNOWN - File {args.mdb_filename} does not exist')
        exit(UNKNOWN)
    else:
        if not os.access(args.mdb_filename, os.R_OK):
            print(f'UNKNOWN - File {args.mdb_filename} is not readable')
            exit(UNKNOWN)

    timestamp = datetime.min
    for sensor_name in sensor_names:
        sensor = Sensor(args.mdb_filename, sensor_name)
        if args.last_change:
            _timestamp = sensor.get_last_changed()
        else:
            _timestamp = sensor.get_last_timestamp()
        if not _timestamp:
            print(f'UNKNOWN - No data found for sensor {sensor_name} in file {args.mdb_filename}')
            exit(UNKNOWN)
        else:
            timestamp = max(timestamp, _timestamp)

    # https://assets.nagios.com/downloads/nagioscore/docs/nagioscore/3/en/pluginapi.html
    # noinspection PyPep8Naming
    SERVICEPERFDATA = f'|age={(datetime.now()-timestamp).seconds}s;' \
                      f'{timedelta_warning.seconds};{timedelta_critical.seconds}'
    # noinspection PyPep8Naming
    SERVICEOUTPUT = f'Last sensor update: {timestamp}'
    if timestamp < datetime.now()-abs(timedelta_critical):
        print(f'CRITICAL - ' + SERVICEOUTPUT + SERVICEPERFDATA)
        exit(CRITICAL)
    elif timestamp < datetime.now()-abs(timedelta_warning):
        print(f'WARNING - ' + SERVICEOUTPUT + SERVICEPERFDATA)
        print('WARNING - ')
        exit(WARNING)
    else:
        print(f'OK - ' + SERVICEOUTPUT + SERVICEPERFDATA)
        exit(OK)


if __name__ == '__main__':
    main()
