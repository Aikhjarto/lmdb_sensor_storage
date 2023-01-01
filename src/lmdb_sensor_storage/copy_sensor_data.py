#!/bin/env python3
import argparse
import logging
from lmdb_sensor_storage.sensor_db import Sensor
from lmdb_sensor_storage._parser import add_logging, setup_logging
logger = logging.getLogger('lmdb_sensor_storage')


def setup_parser():

    parser = argparse.ArgumentParser()
    parser.add_argument('--import-mdb-filename', type=str, required=True)
    parser.add_argument('--import-sensor-name', type=str, required=True)

    parser.add_argument('--export-mdb-filename', type=str, required=True)
    parser.add_argument('--export-sensor-name', type=str, required=True)

    parser.add_argument('--move', default=False, action='store_true',
                        help='Drop source database after copying.')

    add_logging(parser)

    return parser


if __name__ == '__main__':
    p = setup_parser()
    args = p.parse_args()

    setup_logging(logger, syslog=args.syslog, loglevel=args.loglevel)
    sensor = Sensor(args.import_mdb_filename, args.import_sensor_name)
    sensor.copy_to(args.export_mdb_filename, args.export_sensor_name)