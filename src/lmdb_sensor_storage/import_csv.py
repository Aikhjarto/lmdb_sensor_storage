import argparse
import csv
from datetime import datetime
import logging
import struct
from lmdb_sensor_storage._parser import add_logging, setup_logging, fromisoformat
from lmdb_sensor_storage.db._manager import manager

logger = logging.getLogger('lmdb_sensor_storage')


def read_csv(csv_filename='export.csv', since=None, until=None,
             datetime_column=0, data_column=2):
    """
    Reading a CSV file produced by this module and simultaneously down sample for future plotting.
    This allows to generate low-resolution plot of data that would not fit in memory as a whole.

    Parameters
    ----------
    csv_filename : str
        File to read
    since : None, datetime, or str
        If not None, data-points before `since` are ignored.
    until : None, datetime, or str
        If not None, data-points after `until` are ignored.
    datetime_column : int
    data_column: int

    Returns
    -------
    status : bool
        True if `limit` was not reached, False otherwise.

    date : list[datetime]
        List of datetime objects

    val : list[float]
        List of values
    """

    status = True
    dates = []
    values = []

    # ensure since and until are datetime object if not None
    if since is not None and isinstance(since, str):
        since = fromisoformat(since)
    if until is not None and isinstance(until, str):
        until = fromisoformat(until)

    logger.debug('import %s from %s until %s',
                 csv_filename,
                 'beginning of time' if since is None else since,
                 'now' if until is None else until)

    with open(csv_filename) as csvfile:
        reader = csv.reader(csvfile)

        # simpler and faster loop for reading without decimation
        for row in reader:
            row_date = fromisoformat(row[datetime_column])
            if since is None or since <= row_date:
                dates.append(row_date)
                values.append(float(row[data_column]))
            if until is not None and row_date > until:
                break

    logger.info('read %d data points from %s', len(dates), csv_filename)

    return status, dates, values


def setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Read a plain CSV file (no header) with one column having timestamps '
                                                 'in ISO-format and another column scalar values.')

    parser.add_argument('--csv-filename', type=str, required=True)
    parser.add_argument('--mdb-filename', type=str, required=True)
    parser.add_argument('--sensor-name', type=str, default=None)
    parser.add_argument('--csv-data-column', type=int, default=2)
    parser.add_argument('--csv-datetime-column', type=int, default=0)

    add_logging(parser)

    # TODO: support metadata like unit, plot_min_val, plot_max_val and label

    return parser


def csv_to_lmdb(csv_filename, mdb_filename, sensor_name=None, csv_data_column=2, csv_datetime_column=0):
    status, dates, values = read_csv(csv_filename=csv_filename,
                                     data_column=csv_data_column,
                                     datetime_column=csv_datetime_column)
    with manager.get_transaction(mdb_filename, 'data_' + sensor_name, write=True) as txn:
        for date, value in zip(dates, values):
            txn.put(date.isoformat().encode(), struct.pack('d', float(value)))


if __name__ == '__main__':
    p = setup_parser()
    args = p.parse_args()

    setup_logging(logger, syslog=args.syslog, loglevel=args.loglevel)
    del args.loglevel
    del args.syslog

    csv_to_lmdb(args.csv_filename,
                args.mdb_filename,
                sensor_name=args.sensor_name,
                csv_data_column=args.csv_data_column,
                csv_datetime_column=args.csv_datetime_column)
