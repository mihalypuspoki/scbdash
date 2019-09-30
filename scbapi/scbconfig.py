from configparser import ConfigParser
import pathlib


class Config:

    configParser = ConfigParser()
    configFilePath = pathlib.Path.cwd() / 'scbapi/config.ini'

    @classmethod
    def initialize(cls):
        cls.configParser.read(cls.configFilePath)

    @classmethod
    def s3(cls, key):
        return cls.configParser.get('S3', key)

    @classmethod
    def fixtures(cls, key):
        return cls.configParser.get('FIXTURES', key)

    @classmethod
    def api(cls, key):
        return cls.configParser.get('API', key)