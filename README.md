# LMDB Sensor Storage
This project provides a timeseries storage for sensor-date based on the
[Lightning Memory-Mapped Database](https://www.symas.com/lmdb). 
LMDB was originally developed for the [OpenLDAP project](https://git.openldap.org/openldap/openldap/tree/mdb.master) as
a fast an efficient database with small footprint.
On top of this fast key/value database, a timeseries storage with annotations implemented by this project and easy 
asccess is provided via `Python` classes.

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
python3 -m lmdb_sensor_storage.import_wunderground.py
```

Auto-import via systemd-timer
```
systemctl enable wunderground_import@$STATION_ID.timer
systemctl start wunderground_import@$STATION_ID.timer
```

### Subscribe to MQTT
```commandline
python3 -m mqtt_subscriber
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
`http://{hostname}:8000/data?since={from}ms&until={to}ms&sensor_name=xyz`

## Project structure

### Classes
Submodule `db` provides key-value storage for various data-types.

Submodule `sensor_db` provides classes for easy storage of time-series data.

### Callable Modules
`python3 -m lmdb_sensor_storage.mdb_server` provides a web-interface and an HTTP API.

`python3 -m lmdb_sensor_storage.copy_sensor_data` can copy/move sensor data from one mdb-file to another.

`python3 -m lmdb_sensor_storage.import_wunderground`

`python3 -m lmdb_sensor_storage.mqtt_subscriber`

`python3 -m lmdb_sensor_storage.generate_history_html`

### Nagios/Icinga plugin
With `scripts/check_lmdb_sensor_data_age.py`, a plugin for checking freshness of values using Nagios/Inciga is provided. 


### Systemd services
Systemd service and timer files underground import, mqtt subscriber, and the viewer are included.


## DB layout
* One mdb-file for all sensors.
* Sub-DB:
  * `f'data_{sensor_name}'` for time-series data
    * Keys are timestamps in ISO-format
    * values is an 8-byte IEEE floating point number as 
  * `f'meta_{sensor_name}'` for plot layouts and annotation
    * keys are strings denoting the plot property, values are yaml formatted.
    * Currently used:
      label: Label
      plot_max_val: 1.8
      plot_min_val: 1.0
      unit: bar
  * `f'{notes_{sensor_name}`
    * Keys are timestamps in ISO-format 
  * `f'{format_{sensor_name}`
     * Keys are timestamps in ISO-format
  * `notes` for annotations when something auxiliar has changed
    * Keys are timestamps in ISO-format
  * `plot_groups`

## Install Requirements for development
Python 3.6+ is supported.
`requirements.txt` for pip and `environment.yml` for conda/mamba are provided.

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
