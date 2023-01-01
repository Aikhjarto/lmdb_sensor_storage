import argparse
from datetime import datetime
import logging
import os
import json
import requests
import math
from lmdb_sensor_storage.sensor_db import Sensor
from lmdb_sensor_storage._parser import add_logging, setup_logging, fromisoformat

logger = logging.getLogger('lmdb_sensor_storage.wunderground')


def setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument('--mdb-filename', type=str, required=True)

    parser.add_argument('--json-data', type=str, default=None)

    parser.add_argument('--api-key', type=str,
                        help='--api-key shows the key via `top` for all user. '
                             'Better read it from environment variable WUNDERGROUND_API_KEY')

    parser.add_argument('--station-id', type=str)

    parser.add_argument('--date', type=str,
                        help='YYYYMMDD or YYYY-MM-DD format.')

    add_logging(parser)

    return parser

def float_nan(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return math.nan

def import_wunderground_data(mdb_filename: str, data: dict):
    logger.info('Importing %d samples to %s', len(data['observations']), mdb_filename)

    timestamps = [datetime.fromtimestamp(d['epoch']) for d in data['observations']]
    station_id = data['observations'][0]['stationID']

    sensor = Sensor(mdb_filename, f'outside_temperature_{station_id}')
    sensor.write_values(timestamps, [float_nan(d['metric']['tempAvg']) for d in data['observations']])
    sensor.metadata.update({'label': f'Außentemperatur {station_id}',
                            'unit': '°C'})

    sensor = Sensor(mdb_filename, f'wind_speed_{station_id}')
    sensor.write_values(timestamps, [float_nan(d['metric']['windspeedAvg']) for d in data['observations']])
    sensor.metadata.update({'label': f'Windböen {station_id}',
                            'unit': 'm/s'})

    sensor = Sensor(mdb_filename, f'wind_gust_{station_id}')
    sensor.write_values(timestamps, [float_nan(d['metric']['windgustHigh']) for d in data['observations']])
    sensor.metadata.update({'label': f'Windgeschwindigkeit {station_id}',
                            'unit': 'm/s'})


wunderground_url_template = 'https://api.weather.com/v2/pws/observations/all/1day?' \
                            'stationId={station_id}&format=json&units=m&numericPrecision=decimal&apiKey={api_key}'

template_history = 'https://api.weather.com/v2/pws/history/all?' \
                   'stationId={station_id}&format=json&units=m&numericPrecision=decimal&date={date}&apiKey={api_key}'


def import_wunderground_station(mdb_filename, station_id, api_key, date=None):
    """
    Download weather data of a given station from wunderground.com.

    Parameters
    ----------
    mdb_filename : str
    station_id : str
    api_key : str
    date : Union[datetime, str, None]
        Either in YYYYMMDD, isoformat or as datetime object.
        If not given, 5-minute date for the current day is fetched.
    """

    if date:
        if isinstance(date, str) and len(date) != 8:
            d = fromisoformat(date)
            date = d.strftime('%Y%m%d')
        src = template_history.format(station_id=station_id, api_key=api_key, date=date)
    else:
        src = wunderground_url_template.format(station_id=station_id, api_key=api_key)
    return import_wunderground(mdb_filename, src)


def import_wunderground(mdb_filename, src):
    """Import wunderground data from a pre-downloaded *.json file or from a URL"""

    if src.startswith('http'):
        logger.info('Requesting URL %s', src)
        resp = requests.get(src)
        if not resp.ok:
            msg = f'Request fails with {resp.status_code}, {resp.reason}; {resp.content.decode()}'
            raise RuntimeError(msg)
        else:
            if len(resp.content) == 0:
                msg = f'Request failed with "{resp.reason}", no data received'
                raise RuntimeError(msg)

        logger.debug('reading json')
        json_data = json.loads(resp.content)

        if 'observations' not in json_data:
            raise RuntimeError('Did not received ')

    else:
        logger.info('Opening file %s for imports', src)
        with open(src, 'r') as f:
            json_data = json.load(f)

    return import_wunderground_data(mdb_filename, json_data)


if __name__ == '__main__':

    p = setup_parser()
    args = p.parse_args()

    setup_logging(logger, syslog=args.syslog, loglevel=args.loglevel)

    if not args.json_data:
        # assembly URL from other arguments
        if not args.station_id:
            raise RuntimeError('--station-id must be given, since --json-data was not given')
        if not args.api_key:
            API_KEY = os.getenv('WUNDERGROUND_API_KEY')
            if not API_KEY:
                raise RuntimeError('--api_key or environment variable WUNDERGROUND_API_KEY must be given, '
                                   'since --json-data was not given')
            else:
                args.api_key = API_KEY
        import_wunderground_station(args.mdb_filename,
                                    args.station_id,
                                    args.api_key,
                                    date=args.date)
    else:
        import_wunderground(args.mdb_filename, args.json_data)
