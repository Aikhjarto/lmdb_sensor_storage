#!/usr/bin/env python3
import argparse
import os
from datetime import datetime, timedelta, timezone
import numpy as np

# NAGIOS return codes :
# https://nagios-plugins.org/doc/guidelines.html#AEN78
OK = 0
WARNING = 1
CRITICAL = 2
UNKNOWN = 3

try:
    from lmdb_sensor_storage.db.sensor_db import Sensors
except ImportError:
    print('UNKNOWN - cannot import lmdb_sensor_storage module')
    exit(UNKNOWN)


def setup_parser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser()

    parser.add_argument('-c', type=str, required=True,
                        help='If age of the last sample in seconds is higher than `C`, state is CRITICAL')

    parser.add_argument('-w', type=str, required=True,
                        help='If age of the last sample in seconds is higher than `W`, state is WARNING')

    parser.add_argument('--last-change', action='store_true', default=False,
                        help='If give, check for time last value change instead of time of last entry.')

    parser.add_argument('--mdb-filename', type=str, required=True,
                        metavar='file')

    parser.add_argument('--sensor-name', type=str, required=True, action='append',
                        metavar='name')
    
    parser.add_argument('--no-error-if-nan', action='store_true', default=False,
                        help='Do not report state critical, when a sensor has not-a-number as last value.')

    parser.add_argument('--oldest-of', action='store_true', default=False,
                        help='If given, the oldest entry among all given "--sensor_name name" is checked '
                             'against -c and -w, '
                             'otherwise the most recent one is checked.')

    return parser


def main():

    p = setup_parser()
    args = p.parse_args()

    timedelta_warning = abs(timedelta(seconds=float(args.w)))
    timedelta_critical = abs(timedelta(seconds=float(args.c)))

    # When called via the nagios plugin check_by_ssh, sensor names may arrive in an encoding not matching the locale.
    # Undecodable bytes are then surrogate-escaped by Python, thus recover the raw bytes and decode them as
    # UTF-8, or as latin-1 if they are not valid UTF-8.
    sensor_names = []
    for sensor_name in args.sensor_name:
        raw = os.fsencode(sensor_name)
        try:
            sensor_names.append(raw.decode('utf-8'))
        except UnicodeDecodeError:
            sensor_names.append(raw.decode('latin-1'))

    if timedelta_warning >= timedelta_critical:
        print('UNKNOWN - Warning age must be smaller than critical age')
        exit(UNKNOWN)

    if not os.path.isfile(args.mdb_filename):
        print(f'UNKNOWN - File {args.mdb_filename} does not exist')
        exit(UNKNOWN)
    else:
        if not os.access(args.mdb_filename, os.R_OK):
            print(f'UNKNOWN - File {args.mdb_filename} is not readable')
            exit(UNKNOWN)

    sensors = Sensors(args.mdb_filename)
    timestamps = []
    sensors_with_nan = []
    for sensor_name in sensor_names:
        # check before accessing the sensor, which would create it
        if sensor_name not in sensors:
            print(f'UNKNOWN - No sensor {sensor_name} in file {args.mdb_filename}')
            exit(UNKNOWN)
        sensor = sensors[sensor_name]

        if not args.no_error_if_nan:
            last_value = sensor.get_last_value()
            is_nan = False
            try: 
                is_nan = np.isnan(last_value)
            except TypeError:
                pass
            if is_nan:
                sensors_with_nan.append(sensor_name)
        
        if args.last_change:
            _timestamp = sensor.get_last_changed()
        else:
            _timestamp = sensor.get_last_timestamp()
        if not _timestamp:
            print(f'UNKNOWN - No data found for sensor {sensor_name} in file {args.mdb_filename}')
            exit(UNKNOWN)
        else:
            timestamps.append(_timestamp)

    if args.oldest_of:
        timestamp = min(timestamps)
    else:
        timestamp = max(timestamps)

    # https://assets.nagios.com/downloads/nagioscore/docs/nagioscore/3/en/pluginapi.html
    # noinspection PyPep8Naming
    SERVICEPERFDATA = f'|age={int((datetime.now(timezone.utc)-timestamp).total_seconds())}s;' \
                      f'{int(timedelta_warning.total_seconds())};{int(timedelta_critical.total_seconds())}'

    if sensors_with_nan:
        print(f'CRITICAL - Sensors with nan: {", ".join(sensors_with_nan)}{SERVICEPERFDATA}')
        exit(CRITICAL)

    # noinspection PyPep8Naming
    SERVICEOUTPUT = f'Last sensor update: {timestamp.astimezone().replace(tzinfo=None)}'
    if timestamp < datetime.now(timezone.utc)-abs(timedelta_critical):
        print(f'CRITICAL - ' + SERVICEOUTPUT + SERVICEPERFDATA)
        exit(CRITICAL)
    elif timestamp < datetime.now(timezone.utc)-abs(timedelta_warning):
        print(f'WARNING - ' + SERVICEOUTPUT + SERVICEPERFDATA)
        exit(WARNING)
    else:
        print(f'OK - ' + SERVICEOUTPUT + SERVICEPERFDATA)
        exit(OK)


if __name__ == '__main__':
    main()
