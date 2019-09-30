import json
import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import pandas

from scbapi.scbcontroller import DataController, SimpleQuery, Queries
from scbapi.scbutils import MappingTools
from scbapi.scbconfig import Config

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# Colors for default color scale
DEFAULT_COLORS = ["#edf8fb", "#bfd3e6", "#9ebcda", "#8c96c6", "#8c6bb1", "#88419d", "#6e016b"]

# TODO Serialize query collection and add to local store?
# Initialize data controller and get data frames
data_controller = DataController(local_path=Config.fixtures('FOLDER'))


def get_dataframe_cols(data_dict=None):
    return [{"name": i, "id": i} for i in data_dict["COLUMN_NAMES"]]


def get_dataframe(data_dict=None):
    df_data: pandas.DataFrame = pandas.DataFrame.from_dict(data_dict["DATAFRAME"]).astype({"region": str})
    return df_data.to_dict('records')


def get_line_chart(data_dict: dict):
    df_data: pandas.DataFrame = pandas.DataFrame.from_dict(data_dict["DATAFRAME"])
    df = df_data.groupby(by='year', as_index=False).sum()

    return \
        dict(
            data=[dict(
                x=df["year"],
                y=df[data_dict["VALUE_COLUMN"]],
                mode='lines+markers',
                type='scatter'
            )]
        )


def get_map_chart(data_dict: dict, map_dict: dict, classifier=None):
    df_data: pandas.DataFrame = pandas.DataFrame.from_dict(data_dict["DATAFRAME"]).astype({"region": str})
    df = df_data.groupby(by='region', as_index=False).sum()

    # Calculate colorscale using classifier
    color_scale = MappingTools.get_colorscale(df=df, column=data_dict["VALUE_COLUMN"],
                                              colors=DEFAULT_COLORS, classifier=classifier)
    return \
        dict(
            data=[dict(
                geojson=map_dict,
                locations=df.region,
                z=df[data_dict["VALUE_COLUMN"]],
                colorscale=color_scale,
                zauto=True,
                marker_opacity=0.8,
                marker_line_width=0,
                visible=True,
                type='choroplethmapbox'
            )],
            layout=dict(
                mapbox=dict(
                    layers=[],
                    style="carto-positron",
                    zoom=5,
                    center={"lat": 57.78145, "lon": 14.15618},
                ),
                margin={"r": 0, "t": 5, "l": 0, "b": 0},
                clickmode='event+select'
            )
        )


app.layout = html.Div(children=[
    # In memory store to persist and share query result between callbacks
    dcc.Store(id='query_data'),

    # In memory store to persist and share query collection between callbacks
    dcc.Store(id='query_collection'),

    dcc.Tabs(id="tabs",
             parent_className='custom-tabs',
             className='custom-tabs-container',
             children=[
                 # Tab for mapping data
                 dcc.Tab(label='Map',
                         className='custom-tab',
                         selected_className='custom-tab--selected',
                         children=[
                             html.Div([
                                 html.Div([
                                     html.P('Data sources:'),
                                     dcc.Dropdown(
                                         options=[{'label': v["query"]["name"], 'value': k} for (k, v) in
                                                  data_controller.queries.items()],
                                         id='stat-dropdown'
                                     )
                                 ], style={'width': '30%', 'display': 'inline-block'}),
                                 html.Div([
                                     html.P('Classifiers: '),
                                     dcc.Dropdown(
                                         options=[{'label': v, 'value': k} for (k, v) in
                                                  MappingTools.CLASSIFIERS.items()],
                                         value=list(MappingTools.CLASSIFIERS.keys())[0],
                                         clearable=False,
                                         id='classifier-dropdown'
                                     )
                                 ], style={'width': '30%', 'display': 'inline-block'}),
                             ], style={'width': '50%'}),
                             html.Div([
                                 dcc.Graph(
                                     id='sweden-choropleth',
                                     figure=dict(
                                         data=[dict(
                                             zauto=True,
                                             marker_opacity=0.8,
                                             marker_line_width=0,
                                             visible=True,
                                             type='choroplethmapbox'
                                         )],
                                         layout=dict(
                                             mapbox=dict(
                                                 layers=[],
                                                 style="carto-positron",
                                                 zoom=5,
                                                 center={"lat": 57.78145, "lon": 14.15618},
                                             ),
                                             margin={"r": 0, "t": 5, "l": 0, "b": 0},
                                             clickmode='event+select'
                                         )
                                     )
                                 )
                             ], style={'display': 'inline-block', 'width': '49%'}),
                             html.Div([
                                 dcc.Graph(
                                     id='sweden-timeseries',
                                     figure=dict(
                                         data=[dict(
                                             mode='lines+markers',
                                             type='scatter'
                                         )]
                                     )
                                 )
                             ], style={'display': 'inline-block', 'width': '49%'})
                         ]),

                 # Tab for data table
                 dcc.Tab(label='Data',
                         className='custom-tab',
                         selected_className='custom-tab--selected',
                         children=[
                             html.Div([
                                 dash_table.DataTable(
                                     id='table-data',
                                     style_cell={'minWidth': '0px', 'maxWidth': '80px', },
                                     fixed_rows={'headers': True, 'data': 0},
                                     virtualization=True,
                                     page_action='none'
                                 )
                             ], style={'padding': '10px'})
                         ]),

                 # Tab for query details
                 dcc.Tab(label='Query',
                         className='custom-tab',
                         selected_className='custom-tab--selected',
                         children=[
                             html.Div([
                                 html.P('Query path:'),
                                 dcc.Input(
                                     id="query-path",
                                     type="text",
                                     style={'width': '100%'}
                                 ),
                                 html.P('Query key:'),
                                 dcc.Input(
                                     id="query-key",
                                     type="text",
                                     style={'width': '100%'}
                                 ),
                                 html.P('Query name:'),
                                 dcc.Input(
                                     id="query-name",
                                     type="text",
                                     style={'width': '100%'}
                                 ),
                                 html.P('Query (simplified):'),
                                 dcc.Textarea(
                                     id="query-simplified",
                                     rows=12,
                                     style={'width': '100%'}
                                 ),
                                 html.Button('Save', id='query-save-button'),
                                 html.Div(
                                     id="query-save-result", style={'color': 'red'}
                                 ),
                                 html.Br(),
                                 html.P('Query (final):'),
                                 dcc.Markdown(
                                     id="query-final",
                                     children='''> No data''',
                                 ),
                                 html.P('Query variables:'),
                                 dcc.Markdown(
                                     id="query-variables",
                                     children='''> No data''',
                                 )
                             ], style={'padding': '10px', 'width': '60%'})
                         ])
             ]),
])


@app.callback(
    [Output('query_collection', 'data'),
     Output('query-save-result', 'children')],
    [Input('query-save-button', 'n_clicks')],
    [State('query_collection', 'data'),
     State('query-path', 'value'),
     State('query-key', 'value'),
     State('query-name', 'value'),
     State('query-simplified', 'value')]
)
def save_query_to_collection(n_clicks, stored_queries, path, key, name, simple_query):
    if n_clicks is None:
        raise PreventUpdate

    # Initialize parameters
    status = ''

    # Get queries from store or read defaults
    if stored_queries is None:
        queries = data_controller.queries
    else:
        queries = stored_queries

    # Create new query and add it the collection
    try:
        query: SimpleQuery = SimpleQuery(path=path, name=name, simple_query=json.loads(simple_query))
        queries = data_controller.add_query(query_key=key, query=query, query_dict=queries)
    except Exception as e:
        status = 'Error: {error}'.format(error=e)

    # Get queries collection and store it
    return queries, status


@app.callback(
    [Output('query-path', 'value'),
     Output('query-name', 'value'),
     Output('query-key', 'value'),
     Output('query-simplified', 'value'),
     Output('query-final', 'children'),
     Output('query-variables', 'children')],
    [Input('stat-dropdown', 'value')],
    [State('query_collection', 'data')])
def load_query_details(statistic, stored_queries):
    if statistic is None:
        raise PreventUpdate

    # JSON format template
    json_template = '''```json {code} ```'''

    # TODO Show status in a separate DIV
    status = ''

    # Get queries from store or read defaults
    if stored_queries is None:
        queries: Queries = data_controller.queries
    else:
        queries: Queries = stored_queries

    # Read single query
    try:
        query: SimpleQuery = data_controller.get_query(query_key=statistic, query_dict=queries)
        return \
            query.path, \
            query.name, \
            statistic, \
            json.dumps(query.simple_query), \
            json_template.format(code=json.dumps([a.dict() for a in query.query], indent=4)), \
            json_template.format(code=query.info.json(skip_defaults=True, ensure_ascii=False, indent=4))
    except Exception as e:
        status = 'Error: {error}'.format(error=e)

    return '', '', '', '', '> No data', '> No data'


@app.callback(
    [Output('stat-dropdown', 'value'),
     Output('stat-dropdown', 'options')],
    [Input('query_collection', 'modified_timestamp')],
    [State('query_collection', 'data')])
def reset_after_change(ts, queries):
    if ts is None:
        raise PreventUpdate

    # update select options too
    options = [{'label': v["query"]['name'], 'value': k} for (k, v) in queries.items()]
    return None, options


@app.callback(
    Output('query_data', 'data'),
    [Input('stat-dropdown', 'value')],
    [State('query_collection', 'data')])
def clean_data(statistic, stored_queries):
    if statistic is None:
        raise PreventUpdate

    # Get queries from store or read defaults
    if stored_queries is None:
        queries: Queries = data_controller.queries
    else:
        queries: Queries = stored_queries

    # get data
    data_dict = data_controller.data_dict(query_key=statistic, query_dict=queries)
    return data_dict


@app.callback(
    [Output('table-data', 'columns'),
     Output('table-data', 'data')],
    [Input('query_data', 'modified_timestamp')],
    [State('query_data', 'data')])
def display_columsn(ts, data_dict):
    if ts is None:
        raise PreventUpdate

    return get_dataframe_cols(data_dict), get_dataframe(data_dict)


@app.callback(
    Output('sweden-choropleth', 'figure'),
    [Input('query_data', 'modified_timestamp'),
     Input('classifier-dropdown', 'value')],
    [State('query_data', 'data')])
def display_selected_data(ts, classifier, data_dict):
    if ts is None:
        raise PreventUpdate

    print(data_dict)
    fig = get_map_chart(data_dict, data_controller.map_dict(), classifier)
    return fig


@app.callback(
    Output('sweden-timeseries', 'figure'),
    [Input('query_data', 'modified_timestamp')],
    [State('query_data', 'data')])
def display_selected_data(ts, data_dict):
    if ts is None:
        raise PreventUpdate

    print(data_dict)
    fig = get_line_chart(data_dict)
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
