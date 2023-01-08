import argparse
import datetime
import json
import logging
import numpy as np
import plotly
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import os
import re
import requests
import uuid
from lmdb_sensor_storage.sensor_db import LMDBSensorStorage
from lmdb_sensor_storage.db import timestamp_chunker_center, value_chunker_mean, timestamp_chunker_minmeanmax, \
    value_chunker_minmeanmax
from lmdb_sensor_storage._http_request_handler import html_template
from lmdb_sensor_storage._parser import add_logging, setup_logging, fromisoformat

logger = logging.getLogger('lmdb_sensor_storage.history')

# noinspection SpellCheckingInspection
template = {'data': {'scatter': [{'marker': {'colorbar': {'outlinewidth': 0, 'ticks': ''}},
                                  'type': 'scatter'}], },
            'layout': {
                'font': {'color': '#2a3f5f'},
                'hoverlabel': {'align': 'left'},
                'hovermode': 'closest',
                'mapbox': {'style': 'light'},
                'paper_bgcolor': 'white',
                'plot_bgcolor': '#E5ECF6',
                'showlegend': True,
                'scene': {'xaxis': {'backgroundcolor': '#E5ECF6',
                                    'gridcolor': 'white',
                                    'gridwidth': 2,
                                    'linecolor': 'white',
                                    'showbackground': True,
                                    'ticks': '',
                                    'zerolinecolor': 'white'},
                          'yaxis': {'backgroundcolor': '#E5ECF6',
                                    'gridcolor': 'white',
                                    'gridwidth': 2,
                                    'linecolor': 'white',
                                    'showbackground': True,
                                    'ticks': '',
                                    'zerolinecolor': 'white'},
                          'zaxis': {'backgroundcolor': '#E5ECF6',
                                    'gridcolor': 'white',
                                    'gridwidth': 2,
                                    'linecolor': 'white',
                                    'showbackground': True,
                                    'ticks': '',
                                    'zerolinecolor': 'white'}},
                'shapedefaults': {'line': {'color': '#2a3f5f'}},
                'title': {'x': 0.05},
                'xaxis': {'automargin': True,
                          'gridcolor': 'white',
                          'linecolor': 'white',
                          'ticks': '',
                          'title': {'standoff': 15},
                          'zerolinecolor': 'white',
                          'zerolinewidth': 2},
                'yaxis': {'automargin': True,
                          'gridcolor': 'white',
                          'linecolor': 'white',
                          'ticks': '',
                          'title': {'standoff': 15},
                          'zerolinecolor': 'white',
                          'zerolinewidth': 2}}}


def plot_plotly(date, val,  plot_min_val=None, plot_max_val=None, unit=None):
    import plotly.express as px
    ylabel = 'Value'
    if unit:
        ylabel = ylabel + f' [{unit}]'

    fig = px.scatter(x=date, y=val,
                     labels={'x': 'Date', 'y': ylabel},
                     range_y=(plot_min_val, plot_max_val))
    return fig


def generate_history_div(mdb_filename,
                         sensor_names=None,
                         include_plotlyjs=None,
                         group_sensors_regexps=(),
                         **kwargs):
    """
    Faster version of generate_history_div if only the HTML <div> is required and not a whole plotly figure.

    Parameters
    ----------
    mdb_filename : str
    sensor_names : list[str]
    include_plotlyjs : str
        If given, it must point to a location were plotly.min.js can be downloaded.
        plotly.min.js will not be embedded in the HTMl code.
    group_sensors_regexps : list[str]
        Regular expression matching against the same sensor names result in grouping the sensors in plots.
    kwargs : dict

    Returns
    -------
    div : str
    """
    # TODO: x-axis ticks are wrong if bottom-most plot has now data in specified time range

    storage = LMDBSensorStorage(mdb_filename)
    # read data
    if sensor_names is None or len(sensor_names) == 0:
        sensor_names = storage.get_non_empty_sensor_names()
    logger.debug('generate history plots for sensors %s', sensor_names)

    n_axis = len(sensor_names)
    axis_suffices = ['' if k == 0 else f'{k + 1}' for k in range(n_axis)]

    if not group_sensors_regexps:
        # read grouping information from file
        group_sensors_regexps = storage.plot_groups.values()

    if len(sensor_names) > 1 and group_sensors_regexps:
        # check which sensors readings can be grouped in one plot
        x = []
        for regexp in group_sensors_regexps:
            logger.debug('Matching sensor name against regexp %s', regexp)
            r = re.compile(regexp)
            x.append([bool(r.match(sensor_name)) for sensor_name in sensor_names])

        if x:
            x = np.atleast_2d(x)

            # empty row in x: no data for this type of sensor, thus no need for an axis
            # empty column: sensordata is not combinable
            n_non_combinable_yaxis = np.sum(np.sum(np.asarray(x, dtype=np.int), axis=0) == 0)
            n_combinable_yaxis = np.sum(np.sum(np.asarray(x, dtype=np.int), axis=1) != 0)
            n_axis = n_non_combinable_yaxis + n_combinable_yaxis

            # remove empty rows, i.e. non-required axis
            x = x[~np.all(x == 0, axis=1)]
            # assign yaxis numbers for combinable axis
            x = x * np.atleast_2d(np.arange(n_non_combinable_yaxis, n_axis)).T
            x = np.sum(x, axis=0)

            # assign yaxis number for non-combinable axis
            if n_non_combinable_yaxis > 0:
                x[np.flatnonzero(x == 0)] = np.arange(n_non_combinable_yaxis)

            axis_suffices = ['' if k == 0 else f'{k + 1}' for k in x]

            del x  # don't accidentally reuse temporary variable

    data = []
    layout = {'template': template,
              'modebar': {'orientation': 'v'},
              'margin': {'t': 0},
              'title': False}

    if not n_axis:
        return f'<div>No data in {mdb_filename}</div>'

    # define subplot arrangement
    d = 0.02  # distance in percent between consecutive plots
    n_rows = n_axis
    h = (1 - (n_rows - 1) * d) / n_rows  # height of a plot
    for i in range(n_axis):
        suffix = str(i + 1) if i > 0 else ''
        j = n_rows - 1 - i  # index of plot from the bottom
        layout['yaxis' + suffix] = {'anchor': 'x' + suffix,
                                    'domain': [j * (h + d), (j + 1) * h + j * d],
                                    }

        layout['xaxis' + suffix] = {'anchor': 'y' + suffix,
                                    'domain': [0.0, 1.0],
                                    'showticklabels': False}
        if i > 0:
            # link all x-axis
            layout['xaxis' + suffix]['matches'] = 'x'

        if i == n_rows - 1:
            # show x-ticks only on bottom most plot
            layout['xaxis' + suffix]["showticklabels"] = True

    if 'keep_local_extrema' in kwargs:
        del kwargs['keep_local_extrema']
        timestamp_chunker = timestamp_chunker_minmeanmax,
        value_chunker = value_chunker_minmeanmax
    else:
        timestamp_chunker = timestamp_chunker_center
        value_chunker = value_chunker_mean

    for i, sensor_name in enumerate(sensor_names):

        suffix = axis_suffices[i]
        print(sensor_name)
        dates, values = storage[sensor_name].keys_values(timestamp_chunker=timestamp_chunker,
                                                         value_chunker=value_chunker,
                                                         **kwargs)
        if len(dates) == 0:
            logger.info('Skipped history generation since no data is in %s for database %s the requests time period',
                        mdb_filename, sensor_name)
            data.append({'type': 'scatter',
                         'x': [],
                         'xaxis': 'x' + suffix,
                         'y': [],
                         'yaxis': 'y' + suffix,
                         })
        else:
            dates = [d.isoformat() for d in dates]
            if storage[sensor_name].data_format == 'f':
                data.append({'type': 'scatter',
                             'x': dates,
                             'xaxis': 'x' + suffix,
                             'y': values,
                             'yaxis': 'y' + suffix,
                             })
                data[-1]['name'] = sensor_name

            elif storage[sensor_name].data_format == 'ffff':
                v = np.asarray(values)
                y = [list(v[:, i].flatten()) for i in range(v.shape[1])]
                field_names = storage[sensor_name].metadata.get('field_names', None)
                if not field_names:
                    field_names = map(str, range(v.shape[1]))
                for label, val in zip(field_names, y):
                    data.append({'type': 'scatter',
                                 'x': dates,
                                 'xaxis': 'x' + suffix,
                                 'y': val,
                                 'yaxis': 'y' + suffix,
                                 'name': f'{sensor_name} {label}',
                                 'line': {'color': label},
                                 })

        meta_dict = storage[sensor_name].metadata.as_dict()
        if meta_dict is not None:
            if 'unit' in meta_dict:
                layout['yaxis' + suffix]['title'] = {'text': meta_dict['unit']}
                # layout['yaxis' + suffix]['ticksuffix'] = f' {meta_dict['unit']}

            plot_min_val = meta_dict.get('plot_min_val')
            plot_max_val = meta_dict.get('plot_max_val')
            if plot_min_val is not None and plot_max_val is not None:
                try:
                    yrange = layout['yaxis' + suffix]['range']
                    layout['yaxis' + suffix]['range'] = [min(yrange[0], plot_min_val),
                                                         max(yrange[1], plot_max_val)]
                except KeyError:
                    layout['yaxis' + suffix]['range'] = [plot_min_val, plot_max_val]

            label = meta_dict.get('label')
            if label and 'name' not in data[-1]:
                data[-1]['name'] = label

        logger.debug('Data appended')

    shapes = []
    annotations = []
    for timestamp, notes in storage.notes.items(since=kwargs.get('since'), until=kwargs.get('until')):
        shapes.append({"type": 'line',
                       "x0": timestamp.isoformat(),
                       "y0": 0,
                       "x1": timestamp.isoformat(),
                       "yref": 'paper',
                       "y1": 1,
                       "line": {
                           "color": 'grey',
                           "width": 1.5,
                           "dash": 'dot',
                       }})

        hovertext = timestamp.isoformat()
        if notes.get('long', ''):
            hovertext = f'{hovertext}<br>{notes.get("long")}'

        # noinspection SpellCheckingInspection
        annotations.append({
            "yref": 'paper',
            "x": timestamp.isoformat(),
            "xanchor": 'center',
            "y": 0,
            "yanchor": 'top',
            "text": notes['short'],
            "showarrow": True,
            "ax": 0,
            "ay": -0.035,
            "ayref": 'paper',
            "hovertext": hovertext,
        })

    layout['shapes'] = shapes
    layout['annotations'] = annotations

    if include_plotlyjs:
        script = f'<script src="{include_plotlyjs}"></script>'
    else:
        with open(os.path.join(os.path.dirname(plotly.__file__),
                               'package_data', 'plotly.min.js')) as f:
            script = '\n'.join(('<script type="text/javascript">',
                                f.read(),
                                '</script>'))
    # noinspection SpellCheckingInspection
    # noinspection PyPep8Naming
    UUID = str(uuid.uuid4())
    div = f"""<div>
<script type="text/javascript">window.PlotlyConfig = {{MathJaxConfig: 'local'}};</script>
{script}
<div id="{UUID}" class="plotly-graph-div" style="height:100%; width:100%;"></div>
<script type="text/javascript">
        window.PLOTLYENV=window.PLOTLYENV || {{}};
        if (document.getElementById("{UUID}")) {{
            Plotly.newPlot("{UUID}", 
                           {json.dumps(data)},
                           {json.dumps(layout)},
                           {{"responsive": true}})
        }};
</script>
</div>"""

    logger.debug('div generation done')

    return div


def generate_history_plotly(mdb_filename,
                            sensor_names=None,
                            **kwargs):
    """
    Produces time-series plots of sensors from data from `mdb_filename`.

    This function produces and returns a plotly Figure. If you only want the HTML <div>, use `generate_history_div`,
    which is much faster.


    Parameters
    ----------
    mdb_filename : str
    sensor_names : list[str]
        If given, only sensor_names from this list will be plotted. Otherwise, all sensors from `mdb_filename` will
         be plotted.

    kwargs : dict
        Forwarded to read_samples() to limit number of samples (avoid OOM) and select timespan.

    Returns
    -------
    fig : go.Figure
    """
    # read data
    storage = LMDBSensorStorage(mdb_filename)

    if sensor_names is None or len(sensor_names) == 0:
        sensor_names = storage.get_non_empty_sensor_names()
    logger.debug('generate history plots for sensors %s', sensor_names)

    fig = make_subplots(rows=len(sensor_names), cols=1,
                        shared_xaxes=True,
                        vertical_spacing=0.02,
                        )

    for i, sensor_name in enumerate(sensor_names):

        dates, values = storage[sensor_name].keys_values(**kwargs)
        if len(dates) == 0:
            logger.info('Skipped history generation since no data is in %s for database %s the requests time period',
                        mdb_filename, sensor_name)
        else:
            ylabel = ''
            trace_name = sensor_name
            plot_min_val = None
            plot_max_val = None

            meta_dict = storage[sensor_name].metadata.as_dict()
            if meta_dict is not None:
                if 'unit' in meta_dict:
                    ylabel = meta_dict['unit']

                if 'label' in meta_dict and meta_dict['label'] is not None:
                    trace_name = meta_dict['label']

                if 'plot_min_val' in meta_dict:
                    plot_min_val = meta_dict['plot_min_val']

                if 'plot_max_val' in meta_dict:
                    plot_max_val = meta_dict['plot_max_val']

            fig.add_trace(go.Scatter(x=dates, y=values,
                                     name=trace_name),  # legend entry
                          row=i + 1, col=1, )

            yaxis = 'yaxis' + str(i + 1) if i > 0 else 'yaxis'
            # set yrange
            if plot_min_val is not None or plot_max_val is not None:
                getattr(fig.layout, yaxis).range = [plot_min_val, plot_max_val]

            # y-axis label
            getattr(fig.layout, yaxis).title = ylabel

            logger.debug('Trace built')


def generate_history_html(mdb_filename,
                          export_filename=None,
                          **kwargs):
    """
    Parameters
    ----------
    mdb_filename : str
    export_filename : str
        If given, an HTML file with this name will be produced.
        If it starts with 'http', a <div> containing the plot will be uploaded via POST to the given URL.

    kwargs :dict
        Forwarded to `generate_history_div`

    Returns
    -------
    html_content : str
    """

    div = generate_history_div(mdb_filename, **kwargs)
    html_content = html_template.format(head='<title>Gauge reader</title>',
                                        body=div)

    if export_filename is not None:
        if export_filename.startswith('http'):
            # upload to webserver
            requests.post(export_filename, div.encode())
        else:
            # write to file
            with open(export_filename, 'w') as f:
                f.write(html_content)
    return html_content


def setup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument('--mdb-filename', type=str, required=True)
    parser.add_argument('--export-filename', type=str, default=None)
    parser.add_argument('--group-sensors-regexp', type=str, action='append', dest='group_sensors_regexps')

    parser.add_argument('--since', type=lambda s: fromisoformat(s),
                        help='Timestamp in isoformat')
    parser.add_argument('--until', type=lambda s: fromisoformat(s),
                        help='Timestamp in isoformat')
    parser.add_argument('--decimate-to-s')
    parser.add_argument('--limit', type=int)
    parser.add_argument('--keep-local-extrema', type=bool, default=True)

    parser.add_argument('--sensor-name', action='append', dest='sensor_names')

    add_logging(parser)

    return parser


if __name__ == '__main__':

    p = setup_parser()
    args = p.parse_args()

    setup_logging(logger, syslog=args.syslog, loglevel=args.loglevel)
    del args.loglevel
    del args.syslog

    html = generate_history_html(**args.__dict__)

    if not args.export_filename:
        # write to stderr instead of file
        print(html)
