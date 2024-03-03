#!/bin/env python3
import argparse
import logging
import socket
import yaml
from lmdb_sensor_storage.mdb_server._mdb_server import MDBServer, MDBRequestHandler
from lmdb_sensor_storage.db.sensor_db import LMDBSensorStorage
from lmdb_sensor_storage._parser import add_logging, setup_logging, LoadFromFile

logger = logging.getLogger('lmdb_sensor_storage')


def setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lmdb_sensor_storage.mdb_server")

    parser.add_argument('--settings-filename', type=open, action=LoadFromFile,
                        help='Read parameters from a file')

    parser.add_argument('--mdb-filename', type=str)

    parser.add_argument('--port', type=int, default=8001)

    parser.add_argument('--wunderground-station-id', type=str, action='append', dest='wunderground_station_ids',)

    parser.add_argument('--wunderground-api-key', type=str)

    parser.add_argument('--group-sensors-regexp', type=str, action='append', dest='group_sensors_regexps')

    add_logging(parser)

    return parser


def start_db_viewer(mdb_filename, port=8000, **kwargs):
    logger.info('Start DB-viewer HTTP server on port %s', port)

    # start server
    with MDBServer(mdb_filename, ("", port), MDBRequestHandler, **kwargs) as httpd:
        httpd.serve_forever()


def main():
    p = setup_parser()
    args = p.parse_args()
    del args.settings_filename

    setup_logging(logger, syslog=args.syslog, loglevel=args.loglevel)
    del args.loglevel
    del args.syslog

    if args.wunderground_station_ids and not args.wunderground_api_key:
        raise RuntimeError('--wunderground-api-key is required to download weather data '
                           'using --wunderground-station-id')

    d = LMDBSensorStorage(args.mdb_filename).statistics
    print(yaml.safe_dump(d))

    print(f'Go to http://{socket.gethostname()}:{args.port}/week for visualisation')

    start_db_viewer(**args.__dict__)


if __name__ == '__main__':
    main()
