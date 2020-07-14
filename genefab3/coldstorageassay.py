from genefab3.exceptions import GeneLabJSONException
from genefab3.utils import INDEX_BY, DEFAULT_NAME_DELIMITER
from collections import defaultdict
from pandas import concat, Series
from re import search, fullmatch, sub, IGNORECASE


def parse_assay_json_fields(json):
    """Parse fields and titles from assay json 'header'"""
    try:
        header = json["header"]
    except KeyError:
        raise GeneLabJSONException("Malformed assay JSON: header")
    field2title = {entry["field"]: entry["title"] for entry in header}
    if len(field2title) != len(header):
        raise GeneLabJSONException("Conflicting IDs of data fields")
    fields = defaultdict(set)
    for field, title in field2title.items():
        fields[title].add(field)
    return dict(fields), field2title


def parse_metadata_json(self, json):
    """Convert assay JSON to metadata DataFrame"""
    try:
        raw = json["raw"]
    except KeyError:
        raise GeneLabJSONException("Malformed assay JSON: raw")
    # convert raw JSON to raw DataFrame:
    unindexed_metadata = concat(map(Series, raw), axis=1).T
    # find field title to index metadata by:
    matching_indexer_titles = self.match_field_titles(INDEX_BY)
    if len(matching_indexer_titles) == 0:
        raise IndexError("Nonexistent '{}'".format(INDEX_BY))
    elif len(matching_indexer_titles) > 1:
        raise IndexError("Ambiguous '{}'".format(INDEX_BY))
    else:
        matching_title = matching_indexer_titles.pop()
        if matching_title == self.indexed_by:
            matching_fields = {self.field_indexed_by}
        else:
            matching_fields = self.fields[matching_title]
    if len(matching_fields) == 0:
        raise IndexError("Nonexistent '{}'".format(INDEX_BY))
    elif len(matching_fields) > 1:
        raise IndexError("Ambiguous '{}'".format(INDEX_BY))
    else:
        field_indexed_by = list(matching_fields)[0]
    # make sure found field is unambiguous:
    indexed_by = self.match_field_titles(INDEX_BY, method=fullmatch)
    if len(indexed_by) != 1:
        msg = "Nonexistent or ambiguous index_by value: '{}'".format(INDEX_BY)
        raise IndexError(msg)
    # reindex raw DataFrame:
    raw_metadata = unindexed_metadata.set_index(field_indexed_by)
    raw_metadata.index = raw_metadata.index.map(
        lambda f: sub(r'[._-]', DEFAULT_NAME_DELIMITER, f)
    )
    return raw_metadata, indexed_by.pop(), field_indexed_by


class AssayMetadata():
    """Stores assay fields and metadata in raw and processed form"""
    indexed_by, field_indexed_by = None, None
 
    def __init__(self, json):
        """Convert assay JSON to metadata object"""
        self.fields, self.field2title = parse_assay_json_fields(json)
        self.raw_metadata, self.indexed_by, self.field_indexed_by = (
            parse_metadata_json(self, json)
        )
        del self.fields[self.indexed_by]
 
    def match_field_titles(self, pattern, flags=IGNORECASE, method=search):
        """Find fields matching pattern"""
        if self.indexed_by:
            field_pool = set(self.fields) | {self.indexed_by}
        else:
            field_pool = self.fields
        return {
            title for title in field_pool if method(pattern, title, flags=flags)
        }


class ColdStorageAssay():
    """Stores individual assay information and metadata"""
 
    def __init__(self, dataset, name, json):
        """Parse assay JSON reported by cold storage"""
        from genefab3.coldstoragedataset import ColdStorageDataset
        if isinstance(dataset, ColdStorageDataset):
            self.dataset = dataset
        else:
            self.dataset = ColdStorageDataset(dataset)
        self.name = name
        self.fileurls = self.dataset.fileurls
        self.filedates = self.dataset.filedates
        self.metadata = AssayMetadata(json)
