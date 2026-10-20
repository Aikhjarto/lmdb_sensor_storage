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
To install the latest version use
```
pip install git+https://github.com/Aikhjarto/lmdb_sensor_storage
```


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

Topics are subscribed again on every reconnect to the broker, e.g., after a network outage or a restart of the broker.
Connects and disconnects are logged as warnings.
Messages which cannot be decoded, or whose values don't match the data format of the sensor, are logged and skipped.

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

Note: the webserver has no authentication and allows adding samples and notes via HTTP POST.
Don't expose it to untrusted networks, or put it behind a reverse proxy with authentication.

## Project structure

### Classes
Submodule `db` provides key-value storage for various data-types.

Submodule `sensor_db` provides classes for easy storage of time-series data.

### Callable Modules
`python3 -m lmdb_sensor_storage.mdb_server` provides a web-interface and an HTTP API.

`python3 -m lmdb_sensor_storage.copy_sensor_data` can copy sensor data from one mdb-file to another.

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
Python 3.6+ is supported.
`requirements.txt` for pip and `environment.yml` for conda/mamba are provided.

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
pyenv install -s 3.6 3.7 3.8 3.9 3.10 3.11 3.12
pyenv local 3.6 3.7 3.8 3.9 3.10 3.11 3.12
```

For openSUSE Leap 15.5, all dependencies for python3.6 can be installed via zypper
```bash
zypper in python3-lmdb \
    python3-numpy \
    python3-plotly \
    python3-urllib3 \
    python3-paho-mqtt \
    python3-PyYAML \
    python3-requests \
    python3-regex \
    python3-typing_extensions
```
