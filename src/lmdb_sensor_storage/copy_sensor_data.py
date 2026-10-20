#!/bin/env python3
import argparse
import logging
from lmdb_sensor_storage.db.sensor_db import Sensor, Sensors
from lmdb_sensor_storage._parser import add_logging, setup_logging
logger = logging.getLogger('lmdb_sensor_storage')


def setup_parser():

    parser = argparse.ArgumentParser()
    parser.add_argument('--import-mdb-filename', type=str, required=True)
    parser.add_argument('--import-sensor-name', type=str, required=True)

    parser.add_argument('--export-mdb-filename', type=str, required=True)
    parser.add_argument('--export-sensor-name', type=str, required=True)

    parser.add_argument('--move', default=False, action='store_true',
                        help='Delete the source sensor after copying.')

    add_logging(parser)

    return parser


if __name__ == '__main__':
    p = setup_parser()
    args = p.parse_args()

    setup_logging(logger, syslog=args.syslog, loglevel=args.loglevel)
    if args.import_sensor_name not in Sensors(args.import_mdb_filename):
        logger.error('Sensor %s does not exist in %s', args.import_sensor_name, args.import_mdb_filename)
        exit(1)
    sensor = Sensor(args.import_mdb_filename, args.import_sensor_name)
    sensor.copy_to(args.export_sensor_name, args.export_mdb_filename)

    if args.move:
        # only delete the source when the copy is complete
        copy = Sensor(args.export_mdb_filename, args.export_sensor_name)
        if (len(copy) != len(sensor) or
                copy.metadata.as_dict() != sensor.metadata.as_dict() or
                len(copy.notes) != len(sensor.notes)):
            logger.error('Copy of %s differs from source, thus source is not deleted', args.import_sensor_name)
            exit(1)
        del Sensors(args.import_mdb_filename)[args.import_sensor_name]
        logger.info('Deleted %s from %s', args.import_sensor_name, args.import_mdb_filename)
