import argparse
import logging
import logging.handlers
import numpy as np
import shlex
import sys
import datetime


def add_mqtt(g):
    g.add_argument('--mqtt-broker', type=str, default='localhost',
                   help="Hostname or IP of the MQTT broker")

    g.add_argument('--mqtt-broker-port', type=int, default=1883)

    g.add_argument('--mqtt-username', type=str, default=None)

    g.add_argument('--mqtt-password', type=str, default=None)


def add_logging(g):
    """
    Adds verbose, debug, and syslog arguments to a ArgumentParser.

    Parameters
    ----------
    g : argparse.ArgumentParser or argparse._ArgumentGroup

    """
    g.add_argument('-d', '--debug', action="store_const", dest="loglevel", const=logging.DEBUG,
                   help="Print lots of debugging statements",
                   default=logging.WARNING)
    g.add_argument('-v', '--verbose', action="store_const", dest="loglevel", const=logging.INFO,
                   help="Be verbose")
    g.add_argument('--syslog', action='store_true', default=False,
                   help='If given, logging goes to local syslog facility instead of stdout/stderr')


def setup_logging(logger, syslog=False, loglevel=logging.WARNING):
    """
    Set a logger to log to syslog or or stdout with a given `loglevel`
    Parameters
    ----------
    logger : logging.Logger
    syslog : bool
    loglevel : int
    """
    # set up logging
    if syslog:
        handler = logging.handlers.SysLogHandler(address='/dev/log')
        formatter = logging.Formatter('%(message)s')
    else:
        handler = logging.StreamHandler(sys.stdout)
        # noinspection SpellCheckingInspection
        formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.setLevel(loglevel)
    logger.addHandler(handler)


class LoadFromFile(argparse.Action):
    """
    Action for argparse to read parameters from a file.

    Only lines starting with a '-' as first non-whitespace character are processed.

    https://stackoverflow.com/a/27434050/17603877

    TODO: this implementation currently breaks the support for required arguments if
        said argument is given in the imported file.
    TODO: mutually exclusive arguments  are not detected if one is given in file, and
        the other one via CLI.
        Maybe is would be better to use
        https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser.convert_arg_line_to_args
    """

    # noinspection PyShadowingNames
    def __call__(self, parser, namespace, values, option_string=None):
        with values as f:
            # parse arguments in the file and store them in the target namespace
            for line in f:
                line = line.strip()  # remove leading and trailing whitespaces
                if line.startswith('-'):
                    # use shlex.split instead of line.split to preserve quoting of arguments containing spaces
                    parser.parse_args(shlex.split(line), namespace)


def fromisoformat(s):
    """
    Converts a ISO formatted datetime string to a datetime object.

    Parameters
    ----------
    s : str
        Datetime string in ISO format

    Returns
    -------
    datetime.datetime

    """
    if hasattr(datetime, 'fromisoformat'):
        return datetime.datetime.fromisoformat(s)
    else:
        # python 3.6 des not have built-in fromisoformat
        # thus, go over datetime64 from numpy
        tmp = np.datetime64(s).item()

        # TODO: np.datetime64 does not store timezone information, thus switch to differen library
        # https://numpy.org/devdocs/reference/arrays.datetime.html

        # np.datetime64 can return datetime.date instead of datetime.datetime
        if type(tmp) is datetime.date:
            tmp = datetime.datetime.combine(tmp, datetime.time.min)
        return tmp


def as_timedelta(d, none_ok=False):
    if d is None and none_ok:
        return None
    elif isinstance(d, datetime.timedelta):
        return d
    else:
        return datetime.timedelta(seconds=float(d))


def as_datetime(d, none_ok=False):
    """
    Converts 'd' to a datetime object and returns said object.

    Parameters
    ----------
    d : str or datetime.datetime or int or float or None
        `d` can be either a number, which is treated as timestamp, or a string, which is treated as isoformat.

    none_ok : boolisinstance
        If True, None is valid for 'd', otherwise a ValueError is raised.
    Returns
    -------
    datetime.datetime
    """

    if d is None and none_ok:
        return None
    elif isinstance(d, int) or isinstance(d, float):
        # timestamp in seconds
        return datetime.datetime.fromtimestamp(d)
    elif isinstance(d, datetime.datetime):
        return d
    elif isinstance(d, str):
        # isoformat string, timestamp number as string (with or without suffix)
        scale = 1
        timestamp_str = d

        if d.endswith('ms'):
            timestamp_str = d[:-2]
            scale = 1000
        elif d.endswith('us'):
            timestamp_str = d[:-2]
            scale = 1000000
        elif d.endswith('ns'):
            timestamp_str = d[:-2]
            scale = 1000000000
        elif d.endswith('s'):
            timestamp_str = d[:-1]
            scale = 1

        # try to convert to int or float
        timestamp_val = None
        try:
            timestamp_val = int(timestamp_str)
        except ValueError:
            try:
                timestamp_val = float(timestamp_str)
            except ValueError:
                pass

        if timestamp_val is not None:
            return datetime.datetime.fromtimestamp(timestamp_val/scale)

        return fromisoformat(d)
    elif isinstance(d, datetime.date):
        return datetime.datetime.combine(d, datetime.time.min)
    else:
        raise ValueError(f'{d} of type {type(d)} cannot be processed as timestamp')
