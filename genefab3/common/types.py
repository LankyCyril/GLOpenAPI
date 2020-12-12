from genefab3.common.exceptions import GeneLabDataManagerException
from types import SimpleNamespace


def PlaceholderLogger():
    """Placeholder that masquerades as a logger but does not do anything"""
    return SimpleNamespace(
        info=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
    )


class UniversalSet(set):
    """Naive universal set"""
    def __and__(self, x): return x
    def __iand__(self, x): return x
    def __rand__(self, x): return x
    def __or__(self, x): return self
    def __ior__(self, x): return self
    def __ror__(self, x): return self
    def __contains__(self, x): return True


class DatasetBaseClass():
    """Placeholder for identifying classes representing datasets"""
    pass


class AssayBaseClass():
    """Placeholder for identifying classes representing assays"""
    pass


class IterableNamespace(SimpleNamespace):
    """SimpleNamespace that iterates its values (can be used for tests with all(), any(), etc)"""
    def __iter__(self):
        yield from self.__dict__.values()


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
    @property
    def timestamp(self):
        if isinstance(self._timestamp, int):
            return self._timestamp
        elif isinstance(self._timestamp, str) and self._timestamp.isdigit():
            return int(self._timestamp)
        else:
            return -1
    def __eq__(self, other):
        return (
            (self.name == other.name) and (self._url == other._url) and
            (self._timestamp == other._timestamp)
        )
    def __hash__(self):
        return hash((self.name, self._url, self._timestamp))
