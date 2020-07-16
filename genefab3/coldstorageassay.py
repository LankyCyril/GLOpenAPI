from genefab3.exceptions import GeneLabJSONException, GeneLabException
from genefab3.utils import INDEX_BY, force_default_name_delimiter
from collections import defaultdict
from pandas import Series, DataFrame, concat, merge
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


def parse_metadata_json(metadata_object, json):
    """Convert assay JSON to metadata DataFrame"""
    try:
        raw = json["raw"]
    except KeyError:
        raise GeneLabJSONException("Malformed assay JSON: raw")
    # convert raw JSON to raw DataFrame:
    unindexed_dataframe = concat(map(Series, raw), axis=1).T
    # find field title to index metadata by:
    matching_indexer_titles = metadata_object.match_field_titles(INDEX_BY)
    if len(matching_indexer_titles) == 0:
        raise IndexError("Nonexistent '{}'".format(INDEX_BY))
    elif len(matching_indexer_titles) > 1:
        raise IndexError("Ambiguous '{}'".format(INDEX_BY))
    else:
        matching_title = matching_indexer_titles.pop()
        if matching_title == metadata_object.indexed_by:
            matching_fields = {metadata_object.field_indexed_by}
        else:
            matching_fields = metadata_object.fields[matching_title]
    if len(matching_fields) == 0:
        raise IndexError("Nonexistent '{}'".format(INDEX_BY))
    elif len(matching_fields) > 1:
        raise IndexError("Ambiguous '{}'".format(INDEX_BY))
    else:
        field_indexed_by = list(matching_fields)[0]
    # make sure found field is unambiguous:
    indexed_by = metadata_object.match_field_titles(INDEX_BY, method=fullmatch)
    if len(indexed_by) != 1:
        msg = "Nonexistent or ambiguous index_by value: '{}'".format(INDEX_BY)
        raise IndexError(msg)
    # reindex raw DataFrame:
    raw_dataframe = unindexed_dataframe.set_index(field_indexed_by)
    raw_dataframe.index = raw_dataframe.index.map(force_default_name_delimiter)
    return raw_dataframe, indexed_by.pop(), field_indexed_by


def make_multiindex_metadata_dataframe(metadata_object):
    """Convert raw dataframe into human-accessible dataframe"""
    multicols = ["field", "internal_field"]
    columns_dataframe = DataFrame(
        data=metadata_object.raw_dataframe.columns, columns=["internal_field"]
    )
    fields_dataframe = DataFrame(
        data=[[k, v] for k, vv in metadata_object.fields.items() for v in vv],
        columns=multicols
    )
    multiindex_dataframe = (
        merge(columns_dataframe, fields_dataframe, sort=False, how="outer")
        .fillna("Unknown")
    )
    mdv = multiindex_dataframe["internal_field"].values
    rmv = metadata_object.raw_dataframe.columns.values
    if (mdv != rmv).any():
        raise GeneLabException(
            "Inconsistent internal and human-readable fields in assay metadata"
        )
    else:
        multiindex_dataframe = multiindex_dataframe.sort_values(by="field")
        internal_field_order = multiindex_dataframe["internal_field"]
        as_frame = metadata_object.raw_dataframe[internal_field_order].copy()
        as_frame.columns = multiindex_dataframe.set_index(multicols).index
        return as_frame


class AssayMetadata():
    """Stores assay fields and metadata in raw and processed form"""
    indexed_by, field_indexed_by = None, None
 
    def __init__(self, json):
        """Convert assay JSON to metadata object"""
        self.fields, self.field2title = parse_assay_json_fields(json)
        self.raw_dataframe, self.indexed_by, self.field_indexed_by = (
            parse_metadata_json(self, json)
        )
        del self.fields[self.indexed_by]
        self.dataframe = make_multiindex_metadata_dataframe(self)
 
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
 
    @property
    def sample_key(self):
        """Look up sample key in dataset metadata"""
        sample_keys = set(self.dataset.samples.keys())
        if len(sample_keys) == 1:
            return sample_keys.pop()
        else:
            sample_key = sub(r'^a', "s", self.name)
            if sample_key not in self.dataset.samples:
                raise GeneLabJSONException(
                    "Could not find an unambiguous samples key"
                )
            else:
                return sample_key
 
    @property
    def annotation(self):
        """Property alias to get_annotation() with default settings"""
        return self.get_annotation()
 
    def get_annotation(self, variable_only=True, named_only=True):
        """Get annotation of samples: entries that are named only and differ, by default"""
        try:
            samples_field2title = {
                entry["field"]: entry["title"]
                for entry in self.dataset.samples[self.sample_key]["header"]
            }
            untransposed_annotation = concat([
                Series(raw_sample_annotation) for raw_sample_annotation
                in self.dataset.samples[self.sample_key]["raw"]
            ], axis=1)
        except KeyError:
            raise GeneLabJSONException("Malformed samples JSON")
        if named_only:
            untransposed_annotation = untransposed_annotation.loc[[
                field for field in untransposed_annotation.index
                if field in samples_field2title
            ]]
        untransposed_annotation.index = untransposed_annotation.index.map(
            lambda field: samples_field2title.get(field, field)
        )
        if variable_only:
            variable_rows = untransposed_annotation.apply(
                lambda r: len(set(r.values))>1, axis=1,
            )
            untransposed_annotation = untransposed_annotation[variable_rows]
        untransposed_annotation = untransposed_annotation.T.set_index(INDEX_BY).T
        untransposed_annotation.columns = untransposed_annotation.columns.map(
            force_default_name_delimiter
        )
        untransposed_annotation.columns.name = INDEX_BY
        return untransposed_annotation.T
        # TODO: consider multiindex here, to mirror AssayMetadata
 
    @property
    def factors(self):
        """Get subset of annotation that represents factors"""
        annotation = self.get_annotation()
        factor_fields = [
            field for field in annotation.columns
            if search(r'^factor value', field, flags=IGNORECASE)
        ]
        factors = annotation[factor_fields]
        factors.index.name, factors.columns.name = (
            self.metadata.indexed_by, "Factor"
        )
        return factors
