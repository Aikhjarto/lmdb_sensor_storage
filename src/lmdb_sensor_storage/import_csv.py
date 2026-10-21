import argparse
import csv
from datetime import datetime
import logging
from lmdb_sensor_storage._parser import add_logging, setup_logging, as_datetime
from lmdb_sensor_storage.db.sensor_db import Sensor

logger = logging.getLogger('lmdb_sensor_storage')


def read_csv(csv_filename='export.csv', since=None, until=None,
             datetime_column=0, data_column=2):
    """
    Read timestamps and values from a CSV file without header, with timestamps in ISO format in one column and
    scalar values in another column. Rows are expected to be sorted by timestamp.

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
        Always True.

    date : list[datetime]
        List of datetime objects

    val : list[float]
        List of values
    """

    status = True
    dates = []
    values = []

    # ensure since and until are datetime object if not None
    since = as_datetime(since, none_ok=True)
    until = as_datetime(until, none_ok=True)

    logger.debug('import %s from %s until %s',
                 csv_filename,
                 'beginning of time' if since is None else since,
                 'now' if until is None else until)

    with open(csv_filename) as csvfile:
        reader = csv.reader(csvfile)

        # simpler and faster loop for reading without decimation
        for row in reader:
            # timestamps without timezone are local time
            row_date = as_datetime(row[datetime_column])
            if until is not None and row_date > until:
                break
            if since is None or since <= row_date:
                dates.append(row_date)
                values.append(float(row[data_column]))

    logger.info('read %d data points from %s', len(dates), csv_filename)

    return status, dates, values


def setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Read a plain CSV file (no header) with one column having timestamps '
                                                 'in ISO-format and another column scalar values.')

    parser.add_argument('--csv-filename', type=str, required=True)
    parser.add_argument('--mdb-filename', type=str, required=True)
    parser.add_argument('--sensor-name', type=str, required=True)
    parser.add_argument('--csv-data-column', type=int, default=2)
    parser.add_argument('--csv-datetime-column', type=int, default=0)

    add_logging(parser)

    # TODO: support metadata like unit, plot_min_val, plot_max_val and label

    return parser


def csv_to_lmdb(csv_filename, mdb_filename, sensor_name, csv_data_column=2, csv_datetime_column=0):
    status, dates, values = read_csv(csv_filename=csv_filename,
                                     data_column=csv_data_column,
                                     datetime_column=csv_datetime_column)
    if dates:
        Sensor(mdb_filename, sensor_name).write_values(dates, values)


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
