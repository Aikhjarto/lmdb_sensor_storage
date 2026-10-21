# TODO

## Storage

## Webserver

## MQTT subscriber
* History messages with nested dicts, e.g. `"spectrum": {"len": 1, "frequencies": [50], ...}`, are not implemented;
  they are skipped with a warning.

## Deployment
* The systemd units in `src/lmdb_sensor_storage/etc/systemd/system` differ from the installed ones: the MQTT
  subscriber unit is no template, units lack `User=` and `After=network-online.target`, and the Wunderground unit
  contains the API key instead of using `EnvironmentFile=`.

## Development
* No CI. Tests run only with Python 3.13 so far, tox covers 3.7 to 3.13.
* `requirements.txt` duplicates the dependencies of `pyproject.toml`.
* Use the SPDX string for the license in `pyproject.toml` when requiring Python >= 3.9.
* Dead code: `_old_api.py`, `generate_history_plotly`, `plot_plotly`, and the hand-copied plotly template.

# Ideas

## Maybe bokeh provides plots with dynamically loaded data
https://pauliacomi.com/2020/06/07/plotly-v-bokeh.html

https://docs.bokeh.org/en/latest/docs/first_steps.html
https://stackoverflow.com/questions/31392319/bokeh-apps-with-dynamic-updates-of-data-sources

https://docs.bokeh.org/en/latest/docs/reference/models/sources.html#bokeh.models.AjaxDataSource

https://docs.bokeh.org/en/2.4.0/docs/user_guide/data.html
https://realpython.com/python-data-visualization-bokeh/
https://github.com/bokeh/bokeh/issues/3441

https://www.kaggle.com/code/innerfire/sql-and-python-primer-bokeh-plotly
https://davidhamann.de/2018/02/11/integrate-bokeh-plots-in-flask-ajax/
https://discourse.bokeh.org/t/very-big-data-plotting/7491/5
https://notebook.community/DistrictDataLabs/ceb-training/02%20-%20Interactive%20Data%20Visualization%20with%20Bokeh


## Grafana
How to use lmdb_sensor_storage as Grafana datasource


## Plot in holoviews
https://holoviews.org/getting_started/Customization.html
https://holoviews.org/reference_manual/holoviews.streams.html
https://holoviews.org/reference/elements/bokeh/Segments.html
https://holoviews.org/user_guide/index.html

https://stackoverflow.com/questions/55939775/succint-way-to-add-line-segments-to-plotly-graph-with-python-jupyter-notebook