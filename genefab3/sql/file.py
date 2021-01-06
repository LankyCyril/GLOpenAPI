from genefab3.common.utils import HashableEnough
from genefab3.sql.blob import SQLiteBlob
from genefab3.common.exceptions import GeneLabJSONException
from genefab3.common.exceptions import GeneLabDataManagerException


class CacheableFile(HashableEnough, SQLiteBlob):
    """Represents an SQLiteObject that stores up-to-date file contents as a binary blob"""
    __identity_fields = "name", "url", "timestamp", "sqlite_db"
 
    def __ingest_arguments__(self, *, json, name, url, timestamp, json_timestamp_extractor):
        """Initialize either from `json` or from `name, url, timestamp`"""
        if json is not None:
            try:
                self.name, self.url = json["name"], json["remote_url"]
                if json_timestamp_extractor is None:
                    self.timestamp = None
                elif isinstance(json_timestamp_extractor, str):
                    self.timestamp = json[json_timestamp_extractor]
                else:
                    self.timestamp = json_timestamp_extractor(json)
            except KeyError as e:
                raise GeneLabJSONException(
                    "CacheableFile: missing key in file JSON", key=str(e),
                )
        if name is not None:
            self.name = name
        if url is not None:
            self.url = url
        if timestamp is not None:
            self.timestamp = timestamp
        if (not hasattr(self, "url")) or (self.url is None):
            raise GeneLabDataManagerException("No URL for file", name=self.name)
 
    def __init__(self, *, json=None, name=None, url=None, timestamp=None, sqlite_db=None, json_timestamp_extractor=None, compressor=None, decompressor=None):
        """Interpret file descriptors; inherit functionality from SQLiteBlob; define equality (hashableness) of self"""
        self.__ingest_arguments__(
            json=json, name=name, url=url, timestamp=timestamp,
            json_timestamp_extractor=json_timestamp_extractor,
        )
        SQLiteBlob.__init__(
            self, sqlite_db=sqlite_db, table="blobs",
            identifier=self.url, timestamp=self.timestamp,
            url=self.url, compressor=compressor, decompressor=decompressor,
        )
        HashableEnough.__init__(
            self, self.__identity_fields,
        )
