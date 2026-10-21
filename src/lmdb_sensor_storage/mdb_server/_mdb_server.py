"""
Parameters passed in URI:
Once: 'since', 'until', 'decimate_to_s', 'limit', 'first', 'last'
Multiple times: sensor_name
"""

from collections import OrderedDict
import datetime
from html import escape
import json
import math
import secrets
import socket
import socketserver
import struct
import urllib.parse
import traceback
from threading import Lock
from typing import List, Union, Any, Dict
from typing_extensions import Literal
import urllib3
from lmdb_sensor_storage.db.sensor_db import LMDBSensorStorage
from lmdb_sensor_storage.generate_history_html import generate_history_div
from lmdb_sensor_storage.mdb_server._http_request_handler import HTTPRequestHandler, logger
from lmdb_sensor_storage.mdb_server import html_template
from lmdb_sensor_storage.import_wunderground import import_wunderground_station
from lmdb_sensor_storage._parser import as_datetime


def strtobool(val: str) -> bool:
    """Convert a string representation of truth to its Boolean counterpart.

    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return True
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return False
    else:
        raise ValueError(f'invalid bool representation {val}')


class DatetimeTimestampEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.timestamp()
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class MDBServer(socketserver.ThreadingTCPServer):
    # allow for rapid stop/start cycles during debugging
    # Assumption is, that no other process will start listening on `port` during restart of this script
    allow_reuse_address = True

    # allow IPv4 and IPv6
    address_family = socket.AF_INET6

    # sessions are dropped when unused for SESSION_TIMEOUT, or least recently used ones when exceeding MAX_SESSIONS
    MAX_SESSIONS = 1000
    SESSION_TIMEOUT = datetime.timedelta(days=1)

    # upper bound for the number of samples requested via `limit`
    MAX_LIMIT = 100000

    def __init__(self, mdb_filename, *args,
                 wunderground_station_ids=(),
                 wunderground_api_key=None,
                 group_sensors_regexps=(),
                 **kwargs):
        self.sensor_storage = LMDBSensorStorage(mdb_filename)
        self.wunderground_station_ids = wunderground_station_ids
        self.wunderground_api_key = wunderground_api_key
        self.group_sensors_regexps = group_sensors_regexps
        self.sessions = OrderedDict()  # ordered by last access
        self.my_lock = Lock()
        super().__init__(*args, **kwargs)


class MDBRequestHandler(HTTPRequestHandler):
    # maximum size of the body of POST requests
    MAX_BODY_LENGTH = 1024 * 1024
    # typehints
    query_dict: Dict[str, Any]
    server: MDBServer

    protocol_version = "HTTP/1.1"  # default is HTTP/1.0, but this doesn't support Transfer-Encoding: chunked

    html_range_select_form_template = """
<form id="range_select" action="time" method="GET">

<div id="div_date">
    <div id="div_since">
    <label for="since">Start date:</label>
    <input type="datetime-local" id="since" name="since" value="{since}">
    </div>

    <div id="div_until">
    <label for="until">End date:</label>
    <input type="datetime-local" id="until" name="until" value="{until}">
    </div>

    <div id="div_samples">
    <label for="limit">Maximum number of samples</label>
    <select name="limit" id="limit">
    {options}
    </select>
    </div>
    
    <div id="id_div_download_wunderground">
    <input type="checkbox" id="id_download_wunderground" name="download_wunderground" />
    <label id="id_label_download_wunderground" for="id_download_wunderground">Update today's weather data</label>
    </div>
</div>

<div>
    <label for="id_sensor_name">Select Sensors</label>
    <select name="sensor_name" id="id_sensor_name" multiple size="{multiselect_height}">
    {sensor_select_options}
    </select>
</div>

<button>Submit</button>
</form>
"""

    html_delete_note_form_template = """
<form id="note_delete" action="time?delete=yes" method="POST">
<div id="div_id_note_del">
<select name="timestamp" id="del_note">
{options}
</select>
<br>
<button>Delete Note</button>
</div>
</form>
"""

    html_add_note_form = """
<form id="note_add" action="time" method="POST">
<div id="div_id_note_add">
<input id="add_note_timestamp" name="timestamp" type="datetime-local" step="0.001"></input>
<input type="text" id="short" name="short" placeholder="Short description"></input>
<textarea id="long" name="long" placeholder="Here goes the longer explanation..."></textarea>
<br>
<button>Add Note</button>
<div>
</form>                
"""

    style = """
<style>
    form {
            display: flex;
            flex-wrap: wrap;
            outline: 2px solid;
        }

    input, label {
        display:block;
        margin:3;
    }

    select {
        margin:3;
    }

    button {
        margin: 3;
    }

    textarea {
        margin: 3;
    }

    div#id_download_wunderground, label#id_label_download_wunderground, input#id_download_wunderground {
        display:inline;
    }

    div#id_div_inputs {
        display:flex;
        margin:3px;
    }
    
    div#id_div_notes {
        margin-left: 3;
    }

    form#range_select{
        display: flex;
    }

    .js-plotly-plot .plotly .modebar {
        left: 0%;
        transform: translateY(100%);
        transform: translateX(-100%);
    }
    </style>"""

    def __init__(self, *args, **kwargs):
        self.sid = None
        self.session = {'query_dict': {}}  # set by update_session
        self.query_dict = {}

        super().__init__(*args, **kwargs)

    def ensure_single_sensor_name(self) -> Union[str, Literal[False]]:
        sensor_names = self.get_sensor_names_from_session()
        if len(sensor_names) != 1:
            logger.info('Request "%s" does not have a single sensor_name but %s',
                        self.path, sensor_names)
            msg = f'Request "{self.path}" does not have a single sensor_name but {sensor_names}'
            self.send_error(422, message=msg)
            return False
        return sensor_names[0]

    @staticmethod
    def generate_sid(n: int = 32) -> str:
        """
        Generate a session id from `n` random bytes.
        """
        return secrets.token_urlsafe(n)

    def parse_cookies(self, cookie_list='') -> dict:
        if not cookie_list:
            cookie_list = self.headers["Cookie"]

        # assumption: no "=" in cookie name
        cookies = {}
        for cookie in (cookie_list or '').split(';'):
            name, sep, value = cookie.strip().partition('=')
            if sep:
                cookies[name] = value
        return cookies

    def parse_query(self) -> Union[Exception, Literal[True]]:
        """
        Parses query string, converts to desired number and time datatypes and checks if requested sensor names
        are in the database.
        If everything works, result are stored self.query_dict, and self.server.sessions[self.sid] is updated.
        If anything goes wrong, HTTP 422 is sent with an appropriate errormessage.
        """

        try:
            self.query_dict = urllib.parse.parse_qs(urllib3.util.parse_url(self.path).query)
            # at this point self. query_dict is of type Dict[str, list[str]]

            for key in ('since', 'until'):  # parse to datetime
                if key in self.query_dict:
                    self.query_dict[key] = as_datetime(self.query_dict[key][0])

            for key in ('decimate_to_s',):  # parse to float
                if key in self.query_dict:
                    self.query_dict[key] = float(self.query_dict[key][0])
                    if not (math.isfinite(self.query_dict[key]) and self.query_dict[key] > 0):
                        raise ValueError(f'{key} must be a positive number')

            for key in ('limit',):  # parse to int
                if key in self.query_dict:
                    self.query_dict[key] = int(self.query_dict[key][0])
                    if self.query_dict[key] <= 0:
                        raise ValueError(f'{key} must be positive')
                    self.query_dict[key] = min(self.query_dict[key], self.server.MAX_LIMIT)

            for key in ('include_header',):  # parse to bool
                if key in self.query_dict:
                    self.query_dict[key] = strtobool(self.query_dict[key][0])

            # check if requested sensor names are actually in file
            with self.server.my_lock:
                sensor_names = self.server.sensor_storage.keys()
            if 'sensor_name' in self.query_dict:
                for sensor_name in self.query_dict['sensor_name']:
                    if sensor_name not in sensor_names:
                        raise ValueError(f'Sensor name {sensor_name} is not in list '
                                         f'of available sensor names {sensor_names}')

            with self.server.my_lock:
                self.session['query_dict'].update(self.query_dict)

        except Exception as error:

            self.send_error_response(422, message=str(error),
                                     explain=f'Malformed request {self.path} resulting in error {error}\n '
                                             f'{traceback.format_exc()}')
            logger.info('Malformed request "%s" resulting in %s',
                        self.path, error)
            return error

        return True

    @property
    def cookie(self):
        return f"sid={self.sid}"

    def update_session(self):

        sid = self.parse_cookies().get('sid')
        now = datetime.datetime.now()
        with self.server.my_lock:
            sessions = self.server.sessions

            # drop expired sessions and least recently used ones, if there are too many
            while sessions:
                oldest_sid, oldest_session = next(iter(sessions.items()))
                if (len(sessions) >= self.server.MAX_SESSIONS or
                        now - oldest_session['last_access'] > self.server.SESSION_TIMEOUT):
                    del sessions[oldest_sid]
                else:
                    break

            # only accept session ids generated by the server
            if sid not in sessions:
                sid = self.generate_sid()
                sessions[sid] = {'query_dict': {},
                                 'session_start': now}

            sessions[sid]['last_access'] = now
            sessions.move_to_end(sid)
            self.sid = sid
            # keep a reference, since the session might be dropped from sessions while handling the request
            self.session = sessions[sid]

    def do_GET(self):

        self.update_session()

        if self.parse_query() is not True:
            return

        if self.path == '/':
            self.send_response(307)
            self.send_header("Location", '/now')
            self.send_header("Content-length", "0")
            self.end_headers()

        elif self.path.startswith('/json'):
            timespan_kwargs = self.get_timespan_dict_from_session()
            sensor_names = self.get_sensor_names_from_session()
            if not sensor_names:
                logger.info('Request "%s" does not have a single sensor_name', self.path)
                msg = f'Request "{self.path}" does not have a single sensor_name.'
                self.send_error_response(422, message=msg)
                return

            # generate the whole response before sending, so errors can still be sent as error response
            try:
                data = self.server.sensor_storage.get_json(sensor_names, **timespan_kwargs)
            except Exception as error:
                logger.exception('Request "%s" failed', self.path)
                self.send_error_response(500, message=f'{type(error).__name__}: {error}',
                                         explain=traceback.format_exc())
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header('Set-Cookie', self.cookie)
            self.send_chunked_header()
            self.end_headers()
            self.write_chunked(data)
            self.end_write_chunked()

        elif self.path.startswith('/data'):
            # get data (timestamps and values) for a specific time-range
            timestamp_kwargs = self.get_timespan_dict_from_session()
            sensor_names = self.get_sensor_names_from_session()
            if not sensor_names:
                logger.info('Request "%s" does not have a single sensor_name')
                msg = f'Request "{self.path}" does not have a single sensor_name.'
                self.send_error(422, message=msg)
                return

            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            self.send_header('Set-Cookie', self.cookie)
            self.send_chunked_header()
            self.end_headers()

            try:
                include_header = self.session['query_dict'].get('include_header', False)
                self.server.sensor_storage.get_csv(sensor_names,
                                                   self.write_chunked,
                                                   include_header=include_header,
                                                   **timestamp_kwargs)
            except Exception:
                self.write_chunked(traceback.format_exc().encode())
            self.end_write_chunked()

        elif self.path.startswith('/nodered_chart'):
            # https://github.com/node-red/node-red-dashboard/blob/master/Charts.md#line-charts-1

            kwargs = self.get_timespan_dict_from_session()

            sensor_names = self.get_sensor_names_from_session()
            if not sensor_names:
                logger.info('Request "%s" does not have a list called sensor_name but %s',
                            self.path, sensor_names)
                msg = f'Request "{self.path}" does not have a single sensor_names but {sensor_names}'
                self.send_error(422, message=msg)
                return

            data = [self.server.sensor_storage.get_node_red_graph_data(sensor_names, **kwargs)]
            j = json.dumps(data).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/json")
            self.send_header("Cache-Control", "public")
            self.send_chunked_header()
            self.end_headers()

            self.write_chunked(j)
            self.end_write_chunked()

        elif self.path.startswith('/time'):

            kwargs = self.get_timespan_dict_from_session()

            if 'download_wunderground' in self.query_dict:
                with self.server.my_lock:
                    for station_id in self.server.wunderground_station_ids:
                        try:
                            import_wunderground_station(self.server.sensor_storage.mdb_filename,
                                                        station_id,
                                                        self.server.wunderground_api_key)
                        except RuntimeError as e:
                            self.send_error(500, message=f'Could not download data for {station_id} from wunderground',
                                            explain=str(e))
                            return

            if 'since' not in kwargs:
                kwargs['since'] = (datetime.datetime.now() - datetime.timedelta(days=1))

            if 'decimate_to_s' not in kwargs:
                kwargs['decimate_to_s'] = 'auto'

            kwargs['limit'] = 10000 if 'limit' not in kwargs else int(kwargs['limit'])

            # generate plotly.js figure as an HTML <div>
            sensor_names = self.get_sensor_names_from_session()
            try:
                div_plot = generate_history_div(self.server.sensor_storage.mdb_filename,
                                                sensor_names=sensor_names,
                                                group_sensors_regexps=self.server.group_sensors_regexps,
                                                include_plotlyjs='/plotly.min.js',
                                                **kwargs)
            except Exception as e:
                tb = traceback.format_exc()
                div_plot = f'<div>{escape(f"{e} {tb}")}</div>'
            # update HTML form data
            since = kwargs['since'].isoformat()[:16]

            if 'until' in kwargs:
                until = kwargs['until'].isoformat()[:16]
            else:
                until = datetime.datetime.now().isoformat()[:16]

            # range select form
            options = []
            for limit in (100000, 50000, 20000, 10000, 5000, 2000, 1000):
                if kwargs['limit'] == limit:
                    options.append(f'<option value="{limit}" selected="selected">{limit}</option>')
                else:
                    options.append(f'<option value="{limit}">{limit}</option>')
            options = '\n'.join(options)

            # select sensor form
            options_sensors = []
            for sensor_name in self.server.sensor_storage.keys():
                sensor_name_html = escape(sensor_name)
                if not sensor_names or sensor_name in sensor_names:
                    options_sensors.append(f'<option name="{sensor_name_html}" selected>{sensor_name_html}</option>')
                else:
                    options_sensors.append(f'<option name="{sensor_name_html}">{sensor_name_html}</option>')

            range_select_form = \
                self.html_range_select_form_template.format(since=since[:16],
                                                            until=until[:16],
                                                            options=options,
                                                            sensor_select_options='\n'.join(options_sensors),
                                                            multiselect_height=min(10, len(options_sensors)))
            # delete note form
            options = []
            for t_isoformat, val in self.server.sensor_storage.notes.items(since=since, until=until):
                options.append(f'<option value="{escape(str(t_isoformat))}">'
                               f'{escape(str(t_isoformat))}; {escape(val["short"])}</option>')
            delete_note_form = self.html_delete_note_form_template.format(options='\n'.join(options))

            # add note form
            add_note_form = self.html_add_note_form

            div_inputs = '<div id="id_div_inputs">{}</div>'.format('\n'.join((
                range_select_form,
                '<div id="id_div_notes">' + delete_note_form,
                add_note_form + '</div>',
            )))

            # link to stats.json
            div_links = '<div><a target="blank" href="stat.json">Database overview</a></div>'

            # join to full html page
            html = html_template.format(head='\n'.join(('<title>Sensor data viewer</title>',
                                                        self.style)),
                                        body='\n'.join((div_plot,
                                                        '<br>',
                                                        div_inputs,
                                                        '<br>',
                                                        div_links)))

            html = html.encode()

            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header('Cache-Control', 'no-store, must-revalidate')
            self.send_header('Expires', '0')
            self.send_header('Link', '</plotly.min.js>; rel=preload; as=script')
            self.send_header('Set-Cookie', self.cookie)
            self.send_chunked_header()
            self.end_headers()

            self.write_chunked(html)
            self.end_write_chunked()

        elif self.path == '/month':
            # serve graph of values over last week in 15 min intervals
            self.send_response(307)
            since = (datetime.datetime.now() - datetime.timedelta(days=30)).isoformat()
            self.send_header("Location", f'/time?decimate_to_s=900&since={since}&until={datetime.datetime.now()}')
            self.send_header("Content-length", "0")
            self.end_headers()

        elif self.path == '/week' or self.path == '/weekly':
            # serve graph of values over last week in 1 min intervals
            self.send_response(307)
            since = (datetime.datetime.now() - datetime.timedelta(days=7)).isoformat()
            self.send_header("Location", f'/time?decimate_to_s=60&since={since}&until={datetime.datetime.now()}')
            self.send_header("Content-length", "0")
            self.end_headers()

        elif self.path == '/today' or self.path == '/day':
            # serve graph of values from last 24h in 1 seconds intervals
            self.send_response(307)
            since = (datetime.datetime.now() - datetime.timedelta(days=1)).isoformat()
            self.send_header("Location", f'/time?decimate_to_s=1&since={since}&until={datetime.datetime.now()}')
            self.send_header("Content-length", "0")
            self.end_headers()

        elif self.path == '/now' or self.path == '/hour':
            # serve graph of values from last 4 hour
            self.send_response(307)
            since = (datetime.datetime.now() - datetime.timedelta(hours=4)).isoformat()
            self.send_header("Location", f'/time?since={since}&until={datetime.datetime.now()}')
            self.send_header("Content-length", "0")
            self.end_headers()

        elif self.path.startswith('/notes'):

            d = self.get_timespan_dict_from_session()
            notes = self.server.sensor_storage.notes.items(since=d.get('since'), until=d.get('until'))
            data = json.dumps(notes, cls=DatetimeTimestampEncoder)

            self.send_response(200)
            self.send_header('Content-Type', 'text/json')
            self.send_chunked_header()
            self.end_headers()
            self.write_chunked(data.encode())
            self.end_write_chunked()

        elif self.path == '/stat.json':

            stat = self.server.sensor_storage.statistics
            j = json.dumps(stat, indent=4).encode()

            self.send_response(200)
            self.send_header("Content-Type", "text/json")
            self.send_header("Cache-Control", "public")
            self.send_header('Content-Length', str(len(j)))
            self.end_headers()

            self.wfile.write(j)

        else:  # any other path
            super().do_GET()

    # noinspection PyPep8Naming
    def send_error_response(self, code: int, message: str, explain: str = None):
        """
        Like `send_error`, but for requests to /json, the error is sent as JSON object with the keys
        "error" and "explain".
        """
        if not urllib.parse.urlsplit(self.path).path.startswith('/json'):
            self.send_error(code, message=message, explain=explain)
            return

        body = json.dumps({'error': message, 'explain': explain}).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.close_connection = True
        self.wfile.write(body)

    def read_body(self) -> Union[str, None]:
        """
        Read the request body according to Content-Length.
        If the body is invalid or too large, an error is sent and None is returned.
        """
        try:
            length = int(self.headers.get('Content-Length', 0))
        except ValueError:
            self.send_error(400, message='Invalid Content-Length')
            return None
        if length < 0:
            self.send_error(400, message='Invalid Content-Length')
            return None
        if length > self.MAX_BODY_LENGTH:
            self.send_error(413, message=f'Request body exceeds {self.MAX_BODY_LENGTH} bytes')
            return None
        try:
            return self.rfile.read(length).decode()
        except UnicodeDecodeError:
            self.send_error(400, message='Request body is not UTF-8')
            return None

    def send_message_page(self, message: str):
        """Send a short HTML page showing `message`, which reloads the current URL after one second."""
        html = html_template.format(head='<title>Sensor data viewer</title>\n<meta http-equiv="refresh" content="1" />',
                                    body=escape(message)).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.send_header('Content-Length', str(len(html)))
        self.send_header('Set-Cookie', self.cookie)
        self.end_headers()
        self.wfile.write(html)

    def do_POST(self):
        self.update_session()

        body = self.read_body()
        if body is None:
            return

        url = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(url.query)

        if url.path == '/add_sample':
            # body is "timestamp;value;sensor_name"
            try:
                timestamp, value, sensor_name = body.split(';', 2)
                self.server.sensor_storage[sensor_name][as_datetime(timestamp)] = float(value)
            except (ValueError, TypeError, struct.error) as e:
                self.send_error(400, message=f'Invalid sample "{body[:100]}": {e}')
                return
            self.send_response(204)
            self.send_header('Set-Cookie', self.cookie)
            self.end_headers()

        elif url.path == '/time':
            form = urllib.parse.parse_qs(body)
            timestamp = form.get('timestamp', [''])[0]
            try:
                timestamp = as_datetime(timestamp) if timestamp else None
            except ValueError:
                self.send_error(400, message=f'parameter "timestamp"={timestamp} is not a valid ISO formatted timestamp')
                return

            if 'delete' in query:
                if timestamp is None:
                    self.send_error(400, message='parameter "timestamp" is required to delete a note')
                    return
                try:
                    del self.server.sensor_storage.notes[timestamp]
                except KeyError:
                    self.send_error(404, message=f'No note at {timestamp}')
                    return
                self.send_message_page('Note was deleted')

            elif form.get('short'):
                # without timestamp, the note is added at the current time
                self.server.sensor_storage.notes.add_note(form['short'][0],
                                                          long=form.get('long', [None])[0],
                                                          timestamp=timestamp)
                self.send_message_page('Note was written')

            else:
                self.send_error(400, message='parameter "short" or "delete" must be given')

        else:
            self.send_error(404)

    def get_sensor_names_from_session(self) -> List[str]:
        return self.session['query_dict'].get('sensor_name', [])

    def get_timespan_dict_from_session(self) -> dict:
        """
        Get dict for `since`, `until`, and `decimate_to_s`.
        """
        kwargs = {}
        for key in ('since', 'until', 'decimate_to_s', 'limit', 'first', 'last'):
            if key in self.session['query_dict']:
                kwargs[key] = self.session['query_dict'][key]

        return kwargs
