import boto3
import json
import pathlib
import pandas
from botocore.handlers import disable_signing
from typing import Any, Dict, Union

from scbapi.scbmap import Region, MapHandler
from scbapi.scbstat import SimpleQuery, QueryTypesEnum, CalcQuery
from scbapi.scbconfig import Config

Query = Union[CalcQuery, SimpleQuery]
Maps = Dict[str, MapHandler]
Queries = Dict[str, Dict[str, Any]]
QueryData = Dict[str, Any]

QueryDataTemplate: 'QueryData' = {
    "VALUE_COLUMN": '',
    "COLUMN_NAMES": [],
    "DATAFRAME": {},
}


class DataController(object):
    _regions: 'Maps' = None
    _queries: 'Queries' = None
    _s3: object = None
    _path: pathlib.Path = None

    def __init__(self, local_path: str = None):
        if local_path is not None:
            self._path = pathlib.Path(local_path)
        else:
            # Initialize S3 connection
            self._s3 = boto3.resource('s3', region_name=Config.s3('REGION'))
            self._s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)

        # Initialize maps and queries
        self._regions = {}
        self._queries = {}

        # load content for maps and queries
        self._regions = self._load_maps()
        self._queries = self._load_queries()

    @staticmethod
    def add_query(query_key: str, query: Query, query_dict: 'Queries') -> 'Queries':
        """
        Adding query to query collection
        """

        # check input dictionary and initialize if needed
        if query_dict is None:
            query_dict = {}

        # raise an error if key already exists in collection
        if query_key in query_dict:
            raise KeyError('key already exists')

        # initialize member
        query_dict[query_key] = {}

        # instantiate query according to query type
        if isinstance(query, SimpleQuery):
            query_dict[query_key]["type"] = QueryTypesEnum.SIMPLE.value
            query_dict[query_key]["query"] = query.query_dict
        elif isinstance(query, CalcQuery):
            query_dict[query_key]["type"] = QueryTypesEnum.CALCULATED.value
            query_dict[query_key]["query"] = query.query_dict
        else:
            # unsupported query type, raise error
            raise NameError('unknown query type')

        return query_dict

    def get_query(self, query_key: str, query_dict: 'Queries', map_key: str = None) -> 'Query':
        """
        Returns selected query object from query dictionary
        """

        # raise error if key not provided
        if query_key is None:
            raise ValueError('invalid query key')

        # Get map data for regions
        maphandler: MapHandler = self._get_map(map_key=map_key)
        region_keys = maphandler.get_keys()

        # Load selected query from collection
        query_itm: dict = query_dict[query_key]
        if query_itm["type"] == QueryTypesEnum.SIMPLE.value:
            query: 'Query' = SimpleQuery(**query_itm["query"])
        elif query_itm["type"] == QueryTypesEnum.CALCULATED.value:
            query: 'Query' = CalcQuery(**query_itm["query"])

        query.region_keys = region_keys
        return query

    def _load_maps(self) -> 'Maps':
        """
        Loads map shape files from S3 store or local path and initialize dictionary for map objects
        """

        regions: 'Maps' = {}

        if self._path is not None:
            reg_path: pathlib.Path = self._path / Config.s3('BUCKET_FOLDER_REGION')
            file_content = reg_path.read_text(encoding='utf-8-sig')
        elif self._s3 is not None:
            content_object = self._s3.Object(Config.s3('BUCKET_NAME'), Config.s3('BUCKET_FOLDER_REGION'))
            file_content = content_object.get()['Body'].read().decode('utf-8')

        map_dict = json.loads(file_content)
        for key, value in map_dict.items():
            region = Region(**value)
            maphandler = MapHandler(region)
            regions[key]: MapHandler = maphandler

        return regions

    def _load_queries(self) -> 'Queries':
        """
        Loads query definitions from JSON and initialize dictionary for queries
        """

        queries: 'Queries' = {}
        query: 'Query'

        # Otherwise load content from file
        if self._path is not None:
            reg_path: pathlib.Path = self._path / Config.s3('BUCKET_FOLDER_QUERY')
            file_content = reg_path.read_text(encoding='utf-8-sig')
        elif self._s3 is not None:
            content_object = self._s3.Object(Config.s3('BUCKET_NAME'), Config.s3('BUCKET_FOLDER_QUERY'))
            file_content = content_object.get()['Body'].read().decode('utf-8')

        qry_dict: 'Queries' = json.loads(file_content)

        # Initialize query objects and add them to output dictionary
        if qry_dict is not None and len(qry_dict) > 0:
            for key, value in qry_dict.items():
                if value["type"] == QueryTypesEnum.SIMPLE.value:
                    query = SimpleQuery(**value["query"])
                elif value["type"] == QueryTypesEnum.CALCULATED.value:
                    query = CalcQuery(**value["query"])

                # Add query to ouput
                queries: 'Queries' = DataController.add_query(query_key=key, query=query, query_dict=queries)

        return queries

    @property
    def maps(self) -> 'Maps':
        return self._regions

    @property
    def queries(self) -> 'Queries':
        return self._queries

    def _get_map(self, map_key: str = None) -> MapHandler:
        """
        Load selected map object
        """

        # Load default if no map parameter provided
        if map_key is None and len(self._regions) > 0:
            # Just get the first item from the list as default
            key = list(self._regions.keys())[0]
        else:
            key = map_key

        if key in self._regions.keys():
            return self._regions[key]

    def map_dict(self, map_key: str = None) -> dict:
        """
        Get map data in dictionary format
        """
        maphandler: MapHandler = self._get_map(map_key=map_key)

        if maphandler is not None:
            df_map = maphandler.get_dataframe()
            map_dict = json.loads(df_map.to_json())
            return map_dict

    def data_dict(self, query_key: str, map_key: str = None, query_dict: 'Queries' = None) -> dict:
        """
        Get query result in JSON format
        """

        data_dict: 'QueryData' = QueryDataTemplate

        query: 'Query' = self.get_query(query_key=query_key, map_key=map_key,
                                        query_dict=query_dict)

        df_data: pandas.DataFrame = query.get_dataframe()

        # Assign values to output structure
        data_dict['VALUE_COLUMN'] = query.value_col
        data_dict['COLUMN_NAMES'] = list(df_data.columns)
        data_dict['DATAFRAME'] = df_data.to_dict()

        print(query.info.json(skip_defaults=True, ensure_ascii=False))
        print(query.json(skip_defaults=True, ensure_ascii=False))

        return data_dict
