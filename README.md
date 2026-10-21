# LMDB Sensor Storage
This project provides a timeseries storage for sensor data based on the
[Lightning Memory-Mapped Database](https://www.symas.com/lmdb). 
LMDB was originally developed for the [OpenLDAP project](https://git.openldap.org/openldap/openldap/tree/mdb.master) as
a fast and efficient database with small footprint.
On top of this fast key/value database, a timeseries storage with annotations implemented by this project and easy 
access is provided via `Python` classes.

An [MQTT](https://de.wikipedia.org/wiki/MQTT) subscriber is included to collect and store data.
Sensors based on 
* [Tasmota](https://www.tasmota.info/), 
* [ESPHome](https://esphome.io/), or
* [ESP32_datalogger](https://github.com/Aikhjarto/ESP32_datalogger)

are directly supported. 

A webserver is included, providing a simple browser-based visualization via `plotly`, as well as an 
HTTP-based API to get sensor data, e.g., to visualize using [Grafana](https://grafana.com/).

A Nagios/Icinga check plugin for sensor data age is also included.

## Installation
Python 3.7 or newer is required. To install the latest version use
```
pip install git+https://github.com/Aikhjarto/lmdb_sensor_storage
```
The version is derived from git tags by [setuptools-scm](https://setuptools-scm.readthedocs.io), e.g. tag `v0.2` for
version 0.2. Commits after a tag get a development version like `0.3.dev4+g1234abc`.


## Tools
### Import CSV to MDB
To import csv-data from another monitoring tool use the provided import tool. See
```commandline
python3 -m lmdb_sensor_storage.import_csv -h
```
for help.

### Wunderground
When monitoring a heating system, weather conditions are often required.
Therefor an import function from weather-sensors from <https://wunderground.com> is provided.
```commandline
python3 -m lmdb_sensor_storage.import_wunderground
```

Auto-import via systemd-timer
```
systemctl enable wunderground_import@$STATION_ID.timer
systemctl start wunderground_import@$STATION_ID.timer
```

### Subscribe to MQTT
```commandline
python3 -m lmdb_sensor_storage.mqtt_subscriber --mdb-filename sensors.mdb \
    --mqtt-broker localhost --mqtt-subscribe-topic 'tele/+/SENSOR'
```
`--mqtt-subscribe-topic` can be given multiple times.
Parameters can also be read from a settings file via `--settings-filename`, with one parameter per line:
```
--mdb-filename /path/to/sensors.mdb
--mqtt-broker broker.lan
--mqtt-subscribe-topic heizung/sensors/#
--mqtt-subscribe-topic tele/+/SENSOR
--max-age-seconds 3600
```
Only lines starting with `-` are processed, so other lines, e.g. starting with `#`, are comments.

Subscriptions use QoS 1 by default, i.e., messages are delivered at least once.
Use `--mqtt-qos` to change it.

The subscriber connects via MQTT 5 with a fixed client id, by default
`lmdb_sensor_storage-<hostname>-<name of settings file>` (or of the mdb file without settings file).
Thus, the broker keeps the session for one day after a disconnect, and delivers messages with QoS > 0 published while
the subscriber was disconnected after it reconnects.
Each start of the subscriber begins with a new session, so changes of the subscribed topics take effect.
Client ids must be unique, thus set `--mqtt-client-id` when running multiple subscribers with the same settings file
name on one host.

To connect via TLS, use `--mqtt-tls`, which verifies the broker against the CAs of the system and uses port 8883 by
default. `--mqtt-ca-file` sets other CA certificates, and `--mqtt-cert-file` and `--mqtt-key-file` set a client
certificate. Both imply `--mqtt-tls`.

Topics are subscribed again on every reconnect to the broker, e.g., after a network outage or a restart of the broker.
Connects and disconnects are logged as warnings.
Messages which cannot be decoded, or whose values don't match the data format of the sensor, are logged and skipped.

History messages of [ESP32_datalogger](https://github.com/Aikhjarto/ESP32_datalogger) with nested dicts, e.g.
`"spectrum": {"len": 1, "frequencies": [50], "U1A_current": [[0.028]]}`, are not supported. Such entries are skipped
with a warning, while the other entries of the message are stored.

When running as systemd service, start it after the network is configured and the broker is running:
```ini
[Unit]
After=network-online.target mosquitto.service
Wants=network-online.target
```

### Compress database
Note, a mdb-file does not shrink when data is moved away or deleted.
However, you can make a use `mdb_copy` from `lmdb`
```commandline
mdb_copy -n -c export.mdb compact.mdb
```
to compress, i.e. free deleted space, an mdb-file.

### Export to CSV
Start `lmdb_sensor_storage.mdb_server` and query 
`http://{hostname}:8001/data?since={from}ms&until={to}ms&sensor_name=xyz`

The port defaults to 8001 and can be changed with `--port`.
`limit` is capped at 100000 samples per request.

### Webserver security
The webserver has **no authentication**. Everybody who can reach it can read all sensor data, add samples via
`/add_sample`, add and delete notes, and trigger downloads of weather data with the configured Wunderground API key.

By default, it listens on all network interfaces (IPv4 and IPv6). Use `--listen-address` to restrict this, e.g.
`--listen-address 127.0.0.1` to accept only local connections.

If the webserver must be reachable from other hosts, a reverse proxy with authentication and TLS is advised, with the
webserver listening on `127.0.0.1` only. Example for nginx with HTTP basic authentication, where the password file is
created with `htpasswd -c /etc/nginx/lmdb_sensor_storage.htpasswd <user>`:
```nginx
server {
    listen 443 ssl;
    server_name sensors.example.lan;
    ssl_certificate     /etc/nginx/ssl/sensors.crt;
    ssl_certificate_key /etc/nginx/ssl/sensors.key;

    location / {
        auth_basic "Sensor data";
        auth_basic_user_file /etc/nginx/lmdb_sensor_storage.htpasswd;
        proxy_pass http://127.0.0.1:8001;
    }
}
```

## Project structure

### Classes
Submodule `db` provides key-value storage for various data-types.

Submodule `sensor_db` provides classes for easy storage of time-series data.

### Callable Modules
`python3 -m lmdb_sensor_storage.mdb_server` provides a web-interface and an HTTP API.

`python3 -m lmdb_sensor_storage.copy_sensor_data` can copy sensor data from one mdb-file to another, or within one
mdb-file to a new name. With `--move`, the source sensor is deleted after the copy was verified.

`python3 -m lmdb_sensor_storage.import_wunderground`

`python3 -m lmdb_sensor_storage.mqtt_subscriber`

`python3 -m lmdb_sensor_storage.generate_history_html`

### Nagios/Icinga plugin
With `scripts/check_lmdb_sensor_data_age.py`, a plugin for checking freshness of values using Nagios/Icinga is provided.
```commandline
check_lmdb_sensor_data_age.py -w 600 -c 3600 --mdb-filename sensors.mdb --sensor-name a --sensor-name b
```
* `-w`/`-c`: age in seconds of the most recent sample for state WARNING/CRITICAL.
* `--sensor-name` can be given multiple times. By default, the most recently updated sensor is checked,
  with `--oldest-of` the least recently updated one.
* `--last-change` checks the time of the last change of the value instead of the last sample.
* A not-a-number as last value is reported as CRITICAL, unless `--no-error-if-nan` is given.
* A sensor which does not exist results in UNKNOWN.


### Systemd services
Systemd service and timer files for the Wunderground import, the MQTT subscriber, and the viewer are included in
`src/lmdb_sensor_storage/etc/systemd/system`.
Adapt `User=`, the Python interpreter, and the settings file before installing them.


## Timestamps and timezones
Timestamps are stored as microseconds since epoch, i.e. in UTC, and returned as timezone-aware `datetime` objects in
UTC. Timestamps without timezone, e.g. naive `datetime` objects, ISO formatted strings without offset, or the `Time` of
Tasmota, are interpreted as local time.
The webserver shows local time, and CSV and JSON output contain local time with offset, e.g.
`2024-10-27T02:30:00+01:00`, so times during the switch from daylight saving time are unambiguous.

Timestamps before 1970-01-01T00:00:00 UTC cannot be stored, since keys are unsigned integers, which keeps their byte
order equal to their time order. Writing such a timestamp raises `struct.error`. Supporting them would require another
key format and a migration of existing files, which is not worth it for sensor data.

## DB layout
* One mdb-file for all sensors.
* Sub-DB:
  * `f'data_{sensor_name}'` for time-series data
    * Keys are timestamps as microseconds since epoch, packed as 8-byte unsigned big-endian integers
    * Values are packed according to the data format of the sensor stored in `f'format_{sensor_name}'`, e.g.
      `f` for a 4-byte float, a `struct` format string like `4f`, `str`, `bytes`, or `json`.
      If not given, the format is guessed from the first value.
  * `f'meta_{sensor_name}'` for plot layouts and annotation
    * keys are strings denoting the plot property, values are yaml formatted.
    * Currently used:
      label: Label
      plot_max_val: 1.8
      plot_min_val: 1.0
      unit: bar
  * `f'notes_{sensor_name}'` for annotations of a sensor
    * Keys are timestamps as in `data_`, values are yaml formatted dicts with keys `short` and optionally `long`
  * `f'format_{sensor_name}'` for the data format of `data_`
    * Keys are timestamps as in `data_`, values are the format strings
  * `notes` for annotations when something auxiliary has changed
    * Same layout as `f'notes_{sensor_name}'`
  * `plot_groups` for grouping sensors in plots
    * Keys are group names, values are regular expressions matching sensor names

## Install Requirements for development
Python 3.7+ is supported. Packaging is defined in `pyproject.toml`.
`requirements.txt` for pip and `environment.yml` for conda/mamba are provided, or install the package in editable mode
with its dependencies via
```commandline
pip install -e .
```

Run the unittests with
```commandline
python3 -m unittest discover -s tests -t .
```
The MQTT tests start their own `mosquitto` broker on port 1884, thus `mosquitto` must be installed.

Parallel running unittests for different version of `Python` via tox 4.11 are supported via `tox.ini`.
One way to get different python versions, which is supported by `tox`, is to use 
[pyenv](https://github.com/pyenv/pyenv).

Install pyenv (if not already installed):
```bash
git clone https://github.com/pyenv/pyenv.git ~/.pyenv
cd ~/.pyenv && src/configure && make -C src
```
Install Python versions which are tests by tox:
```
pyenv install -s 3.7 3.8 3.9 3.10 3.11 3.12 3.13
pyenv local 3.7 3.8 3.9 3.10 3.11 3.12 3.13
```
