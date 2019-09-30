from typing import List, Dict, Any
import pandas
import requests
import json
from enum import Enum
from pydantic import BaseModel, validator, UrlStr
from scbapi.scbconfig import Config

session = requests.Session()


class QueryTypesEnum(Enum):
    SIMPLE = "SIMPLE"
    CALCULATED = "CALCULATED"


class ResultColumn(BaseModel):
    # TODO Support additional variables
    code: str
    text: str
    type: str


class FilterSelection(BaseModel):
    # TODO Review and implement other supported filter types
    filter: str = "item"
    values: List[str]


class FilterItem(BaseModel):
    code: str
    selection: FilterSelection


class QueryVariable(BaseModel):
    # TODO Handle elimination and time correctly
    code: str
    text: str
    values: List[str]
    valueTexts: List[str]
    elimination: bool = None
    time: bool = None


class QueryInfo(BaseModel):
    title: str
    variables: List[QueryVariable]

    def get_codes(self, texts: List[str] = None) -> List[str]:
        """
        Converts variable text values to codes
        """
        return [var.code for var in self.variables if (texts is None) or (var.text in texts)]

    def get_texts(self, codes: List[str] = None) -> List[str]:
        """
        Converts codes to variable texts
        """
        return [var.text for var in self.variables if (codes is None) or (var.code in codes)]

    def get_values(self, value_texts: List[str] = None, code: str = None, text: str = None) -> List[str]:
        """
        Get variable values for selected variable. Selection is based on code or variable text.
        """
        val_list: List[str] = []
        if text is not None or code is not None:
            for var in self.variables:
                if var.code == code or var.text == text:
                    # Get list of indexes with matching variable texts
                    val_ind: List[int] = [ind for ind, text in enumerate(var.valueTexts) if
                                          (value_texts is None) or (text in value_texts)]
                    # Get list of values matching indexes
                    val_list = [var.values[i] for i in val_ind]

        return val_list

    def get_value_texts(self, values: List[str] = None, code: str = None, text: str = None) -> List[str]:
        """
        Get variable value texts for selected variable. Selection is based on code or variable text.
        """
        val_list: List[str] = []
        if text is not None or code is not None:
            for var in self.variables:
                if var.code == code or var.text == text:
                    # Get list of indexes with matching variables
                    val_ind: List[int] = [ind for ind, val in enumerate(var.values) if
                                          (values is None) or (val in values)]
                    # Get list of values matching indexes
                    val_list = [var.valueTexts[i] for i in val_ind]

        return val_list

    def check_codes(self, codes: List[str]) -> List[str]:
        """
        Compare input with metadata and return missing code values
        """
        if codes is None:
            return []

        codes_found: List[str] = []
        for code in codes:
            for var in self.variables:
                if var.code == code:
                    codes_found.append(code)

        missing_codes: List[str] = [code for code in codes if code not in codes_found and code != '']
        return missing_codes

    def check_texts(self, texts: List[str]) -> List[str]:
        """
        Compare input with metadata and return missing text values
        """
        if texts is None:
            return []

        texts_found: List[str] = []
        for text in texts:
            for var in self.variables:
                if var.text == text:
                    texts_found.append(text)

        missing_texts: List[str] = [text for text in texts if text not in texts_found and text != '']
        return missing_texts

    def check_values(self, values: List[str], code: str = None, text: str = None) -> List[str]:
        """
        Compare input with metadata and return missing parameter values
        """
        values_missing: List[str] = []
        if text is not None or code is not None:
            for var in self.variables:
                if var.code == code or var.text == text:
                    values_missing = [val for val in values if val not in var.values]

        return values_missing

    def check_valuetexts(self, value_texts: List[str], code: str = None, text: str = None) -> List[str]:
        """
        Compare input with metadata and return missing parameter value texts
        """
        value_texts_missing: List[str] = []
        if text is not None or code is not None:
            for var in self.variables:
                if var.code == code or var.text == text:
                    value_texts_missing = \
                        [val_text for val_text in value_texts if val_text not in var.valueTexts]

        return value_texts_missing


class BaseQuery(BaseModel):
    name: str
    url: UrlStr = Config.api('URL')

    def __init__(__query_self__, **values: Any) -> None:
        super().__init__(**values)
        __query_self__._set_metadata()
        __query_self__._validate_query()

    @validator('name')
    def check_name(cls, v):
        if v is None or v == '':
            raise ValueError('cannot be empty')
        return v

    def get_dataframe(self) -> pandas.DataFrame:
        pass

    @property
    def query_dict(self) -> dict:
        return {}

    @property
    def value_col(self) -> str:
        return ''

    def _validate_query(self):
        pass

    def _set_metadata(self):
        pass


class CalcQuery(BaseQuery):
    output: str
    data_sources: List[str]
    calculation: str

    def get_dataframe(self):
        # TODO Implement calculated data source handling
        pass


class Query(BaseQuery):
    filter: List[FilterItem] = None
    path: str
    info: QueryInfo = None
    result_cols: List[ResultColumn] = None
    region_keys: List = None

    @validator('path')
    def check_path(cls, v, values):
        response = session.get(values['url'] + v)
        if response.status_code != 200:
            raise ValueError('cannot reach url, invalid path')
        return v

    def _set_metadata(self):
        if self.info is None:
            response = session.get(self.url + self.path)
            result = json.loads(response.content.decode('utf-8-sig'))
            self.info = QueryInfo(**result)

    def _validate_query(self):
        """
        Check codes and values against variables
        """
        # Check code values
        codes: List[str] = [itm.code for itm in self.query]
        missing_codes: List[str] = self.info.check_codes(codes)
        if len(missing_codes) > 0:
            raise ValueError('{} codes is missing.'.format(missing_codes))

        # Check value parameters
        # TODO wildcards are supported values.
        for itm in self.query:
            missing_values: List[str] = self.info.check_values(values=itm.selection.values, code=itm.code)
            if len(missing_values) > 0:
                raise ValueError('{} values are missing.'.format(missing_values))

    @property
    def query(self) -> List[FilterItem]:
        return self.filter

    @property
    def query_dict(self) -> dict:
        query_dict: dict = {
            'name': self.name,
            'path': self.path,
            'query': [item.dict() for item in self.query],
            'info': self.info.dict()
        }
        return query_dict

    @property
    def value_col(self) -> str:
        return [col.text for col in self.result_cols if col.type == 'c'][0]

    def get_dataframe(self) -> pandas.DataFrame:
        """
        Get data from API and create data frame
        """
        # Construct query for API call
        selection = [a.dict() for a in self.query]
        query: Dict = {"query": selection, "response": {"format": "json"}}

        # Post query
        response = session.post(self.url + self.path, json=query)
        response_json = json.loads(response.content.decode('utf-8-sig'))
        scb_data: Dict[str, List[Any]] = response_json['data']
        scb_columns = response_json['columns']

        # Create dictionary for data frame
        df_dict: Dict[str, List[Any]] = {}

        # Prepare columns and initialize lists in dictionary
        self.result_cols = []
        for i in range(len(scb_columns)):
            res_column = ResultColumn(**scb_columns[i])
            self.result_cols.append(res_column)
            df_dict[res_column.text] = []

        # Get data into dictionary
        for i in range(len(scb_data)):
            for j in range(len(self.result_cols)):
                col_name: str = self.result_cols[j].text
                col_type: str = self.result_cols[j].type

                if col_type != "c":
                    df_dict[col_name].append(scb_data[i]['key'][j])
                else:
                    try:
                        df_dict[col_name].append(float(scb_data[i]['values'][0]))
                    except ValueError:
                        df_dict[col_name].append(float('Nan'))

        df: pandas.DataFrame = pandas.DataFrame.from_dict(df_dict)

        # Drop record with missing values
        values: List = [col.text for col in self.result_cols if col.type == "c"]
        df.dropna(subset=values)

        return df


class SimpleQuery(Query):
    simple_query: Dict[str, List[str]] = None

    def _prepare_simple_query(self):
        # TODO Implement query to simple query conversion
        pass

    def _validate_query(self):
        # Check text values
        texts = self.simple_query.keys()
        missing_texts: List[str] = self.info.check_texts(texts)
        if len(missing_texts) > 0:
            raise ValueError('{} texts are missing.'.format(missing_texts))

        # Check value text parameters
        for key, val in self.simple_query.items():
            # Wildcard. No need for check, values will come from metadata
            if val != ['*']:
                missing_value_texts: List[str] = self.info.check_valuetexts(value_texts=val, text=key)
                if len(missing_value_texts) > 0:
                    raise ValueError('{} values are missing.'.format(missing_value_texts))

    @property
    def query_dict(self) -> dict:
        query_dict: dict = {
            'name': self.name,
            'path': self.path,
            'simple_query': self.simple_query,
            'info': self.info.dict()
        }

        return query_dict

    @property
    def query(self) -> List[FilterItem]:
        self._transform_query()
        return self.filter

    def _transform_query(self):
        """
        Transform the simplified query into executable format
        """
        self.filter = []
        for par, par_val in self.simple_query.items():
            values: List[str]
            if par_val == ["*"]:
                if par == 'region':
                    values = self.region_keys
                else:
                    values = self.info.get_values(text=par)
            else:
                values = self.info.get_values(value_texts=par_val, text=par)

            selection = FilterSelection(filter="item", values=values)
            code: List[str] = self.info.get_codes([par])
            filteritem: FilterItem = FilterItem(code=code[0], selection=selection)

            self.filter.append(filteritem)
