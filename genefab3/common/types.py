from collections import namedtuple
from genefab3.common.exceptions import GeneLabDataManagerException
from memoized_property import memoized_property


class DatasetBaseClass():
    """Placeholder for identifying classes representing datasets"""
    pass


DatasetJSONs = namedtuple("DatasetJSONs", ("glds", "fileurls", "filedates"))
DatasetJSONs.__new__.__defaults__ = (None,) * 3


class FileDescriptor():
    """Holds name, url, timestamp; raises delayed error on url==None, returns negative timestamps for malformed/absent timestamps"""
    def __init__(self, name, url, timestamp):
        self.name, self._url, self._timestamp = name, url, timestamp
    @property
    def url(self):
        if self._url is None:
            raise GeneLabDataManagerException("No URL for file", name=self.name)
        else:
            return self._url
    @memoized_property
    def timestamp(self):
        if isinstance(self._timestamp, int):
            return self._timestamp
        elif isinstance(self._timestamp, str) and self._timestamp.isdigit():
            return int(self._timestamp)
        else:
            return -1
    def __hash__(self):
        return hash((self.name, self._url, self._timestamp))


class AssayBaseClass():
    """Placeholder for identifying classes representing assays"""
    pass
