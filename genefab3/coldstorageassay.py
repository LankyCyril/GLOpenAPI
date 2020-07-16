from genefab3.exceptions import GeneLabJSONException, GeneLabException
from genefab3.utils import INDEX_BY, force_default_name_delimiter
from collections import defaultdict
from pandas import Series, DataFrame, concat, merge
from re import search, fullmatch, split, IGNORECASE
from copy import deepcopy


def filter_json(json, field_mask):
    """Reduce metadata-like JSON to 'header' fields matching `field_mask`"""
    try:
        return {
            "raw": deepcopy(json["raw"]),
            "header": [
                deepcopy(e) for e in json["header"]
                if search(field_mask, e["title"]) or (e["title"] == INDEX_BY)
            ]
        }
    except KeyError:
        raise GeneLabJSONException("Malformed assay JSON: header and/or raw")


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


def parse_metadatalike_json(metadata_object, json):
    """Convert assay JSON to metadata DataFrame"""
    try:
        raw = json["raw"]
    except KeyError:
        raise GeneLabJSONException("Malformed assay JSON: raw")
    # convert raw JSON to raw DataFrame:
    unindexed_dataframe = concat(map(Series, raw), axis=1).T
    # find field title to index metadata by:
    matching_indexer_titles = metadata_object.match_field_titles(
        INDEX_BY, method=fullmatch,
    )
    if len(matching_indexer_titles) == 0:
        raise IndexError("Nonexistent '{}'".format(INDEX_BY))
    elif len(matching_indexer_titles) > 1:
        raise IndexError("Ambiguous '{}'".format(INDEX_BY))
    else:
        indexed_by = matching_indexer_titles.pop()
        if indexed_by == metadata_object.indexed_by:
            matching_fields = {metadata_object.internally_indexed_by}
        else:
            matching_fields = metadata_object.fields[indexed_by]
    if len(matching_fields) == 0:
        raise IndexError("Nonexistent '{}'".format(INDEX_BY))
    elif len(matching_fields) > 1:
        raise IndexError("Ambiguous '{}'".format(INDEX_BY))
    else:
        internally_indexed_by = list(matching_fields)[0]
    # reindex raw DataFrame:
    raw_dataframe = unindexed_dataframe.set_index(internally_indexed_by)
    raw_dataframe.index = raw_dataframe.index.map(force_default_name_delimiter)
    return raw_dataframe, indexed_by, internally_indexed_by


def make_metadatalike_dataframe(metadata_object):
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


def get_variable_subset_of_dataframe(df):
    """Subset `df` to columns that have variable values"""
    return df.loc[:, df.apply(lambda r: len(set(r.values))>1)]


def get_minimal_dataframe(df, index_name=None):
    """Keep only named (known) variable columns, drop internal field names"""
    fields = df.columns.get_level_values(0)
    minimal_df = df.loc[:, [f != "Unknown" for f in fields]]
    minimal_df.columns = minimal_df.columns.droplevel(1)
    if index_name:
        minimal_df.index.name = index_name
    return minimal_df


class MetadataLike():
    """Stores assay fields and metadata in raw and processed form"""
    indexed_by, internally_indexed_by = None, None
 
    def __init__(self, json, field_mask=None):
        """Convert assay JSON to metadata object"""
        if field_mask is None:
            filtered_json = json
        else:
            filtered_json = filter_json(json, field_mask)
        self.fields, self.field2title = parse_assay_json_fields(filtered_json)
        self.raw_dataframe, self.indexed_by, self.internally_indexed_by = (
            parse_metadatalike_json(self, filtered_json)
        )
        del self.fields[self.indexed_by]
        self.full = make_metadatalike_dataframe(self)
        self.differential = get_variable_subset_of_dataframe(self.full)
        self.minimal = get_minimal_dataframe(self.differential, self.indexed_by)
 
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
 
    def __init__(self, dataset, name, assay_json, sample_json):
        """Parse assay JSON reported by cold storage"""
        try:
            _ = dataset.assays[name]
        except (AttributeError, KeyError):
            msg = "Attempt to associate an assay with a wrong dataset"
            raise GeneLabException(msg)
        self.name = name
        self.dataset = dataset
        self.metadata = MetadataLike(assay_json)
        self.annotation = MetadataLike(sample_json)
        self.factors = MetadataLike(sample_json, field_mask=r'^Factor Value')
 
    def resolve_filename(self, mask, sample_mask=".*", field_mask=".*"):
        """Given masks, find filenames, urls, and datestamps"""
        dataset_level_files = self.dataset.resolve_filename(mask)
        if len(dataset_level_files) == 0:
            return {}
        else:
            metadata_df = self.metadata.full
            sample_names = [
                sn for sn in metadata_df.index if search(sample_mask, sn)
            ]
            fields = [
                fn for fn in metadata_df.columns.get_level_values(0)
                if search(field_mask, fn)
            ]
            metadata_subset_filenames = set.union(set(), *[
                split(r'\s*,\s*', cell_value) for cell_value
                in metadata_df.loc[sample_names, fields].values.flatten()
            ])
            return {
                filename: fileinfo
                for filename, fileinfo in dataset_level_files.items()
                if filename in metadata_subset_filenames
            }
