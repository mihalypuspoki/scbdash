import geopandas
import fiona
from fiona.session import AWSSession
from typing import List
from enum import Enum
from pydantic import BaseModel

# TODO Remove this
class RegionEnum(Enum):
    MUNICIPALITIES = "MUNICIPALITIES"
    COUNTIES = "COUNTIES"


class Region(BaseModel):
    key_col: str
    name_col: str
    s3_bucket: str = None
    s3_key: str = None
    url: str = None

    @property
    def is_s3(self) -> bool:
        """
        Checks if the data source is S3 or not
        """
        if self.s3_bucket is not None and self.s3_key is not None:
            return True

    @property
    def zip_url(self) -> str:
        """
        Prepares URL for zip file read
        """
        if self.url is not None:
            # Set URL to provided value
            return '/'.join(['zip:/', self.url])
        elif self.is_s3:
            # URL for default data source files from S3 storage
            return '/'.join(['zip+s3:/', self.s3_bucket, self.s3_key])


class MapHandler(object):
    region: Region
    gdf: geopandas.geodataframe

    def __init__(self, region: Region):
        self.region: Region = region
        self.__load_map()

    def __load_map(self):
        """
        Download/open Sweden regional shape files and prepare geo dataframe
        """

        if self.region.is_s3:
            with fiona.Env(session=AWSSession(aws_unsigned=True)):
                gdf: geopandas.geodataframe = geopandas.read_file(self.region.zip_url)
        else:
            gdf: geopandas.geodataframe = geopandas.read_file(self.region.zip_url)

        gdf = gdf.to_crs({'init': 'epsg:4326'})
        self.gdf = gdf

    def get_dataframe(self, indexed: bool = True) -> geopandas.geodataframe:
        gdf: geopandas.GeoDataFrame = self.gdf
        if indexed:
            gdf: geopandas.GeoDataFrame = self.gdf.set_index(self.region.key_col)

        return gdf

    def get_keys(self) -> List[str]:
        """
        Get key values from geo dataframe as list
        """
        return self.gdf[self.region.key_col].tolist()

    def get_names(self) -> List[str]:
        """
        Get name values from geo dataframe as list
        """
        return self.gdf[self.region.name_col].tolist()
