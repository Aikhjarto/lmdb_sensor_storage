# LMDB Sensor Storage
This project provides a timeseries storage for sensor-date based on the
[Lightning Memory-Mapped Database](https://www.symas.com/lmdb). 
LMDB was originally developed for the [OpenLDAP project](https://git.openldap.org/openldap/openldap/tree/mdb.master) as
a fast an efficient database with small footprint.

An MQTT subscriber is included to easily store data from a broker using LMDB Sensor Storage.


## Installation
To install the latest version use
```
pip install git+https://github.com/Aikhjarto/lmdb_sensor_storage
```

## Requirements
Python 3.6+and packages from `requirements.txt`


## Tools
### Import CSV to MDB
To import csv-data from another monitoring tool use the provided import tool. See
```commandline
python3 -m analog_gauge_reader.import_csv -h
```
for help.

### Generate standalone HTML
```commandline
python3 -m analog_gauge_reader.generate_html
```

### Visualize database content interactively with possibility to add notes
```commandline
python3 -m analog_gauge_reader.show_mdb
```

### Move sensor data between MDB files
To copy or move sensor data from one mdb-file to another, `analog_gauge_reader.copy_sensor_data` is provided.


### Wunderground
When monitoring a heating system, weather conditions are often required.
Therefor an import function from weather-sensors from <https://wunderground.com> is provided.
```commandline
python3 -m analog_gauge_reader.import_wunderground.py
```

Auto-import via systemd-timer
```
systemctl enable wunderground_import@$STATION_ID.timer
systemctl start wunderground_import@$STATION_ID.timer
```

### Compress database
Note, a mdb-file does not shrink when data is moved away or deleted.
However, you can make a use `mdb_copy` from `lmdb`
```commandline
mdb_copy -n -c export.mdb compact.mdb
```
to compress, i.e. free deleted space, an mdb-file.

### Export to CSV
Start `show_mdb` and query `/data?`

## Project structure

### Classes
Submodule `db` provides key-value storage for various data-types.

Submodule `sensor_db` provides classes for easy storage of time-series data.

### Scripts
`lmdb_sensor_storage` provieds a web-interface.

`copy_sensor_data` can copy/move sensor data from one mdb-file to another.

`lmdb_sensor_storage.import_wunderground`

`lmdb_sensor_storage.mqtt_subscriber`

`lmdb_sensor_storage.generate_history_html`

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

  * `notes` for annotations when something auxiliar has changed
    * Keys are timestamps in ISO-format

# Requirements
A requirements.txt for pip and an environment.yml for conda/mamba is provided. 

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
