from genefab3.exceptions import GeneLabJSONException, GeneLabException
from genefab3.config import INDEX_BY
from genefab3.utils import force_default_name_delimiter
from collections import defaultdict
from pandas import Series, DataFrame, concat, merge, read_csv
from re import search, fullmatch, split, IGNORECASE
from copy import copy, deepcopy
from urllib.request import urlopen


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


def INPLACE_merge_duplicate_fields(dataframe, fields):
    first_field, *other_fields = fields
    for other_field in other_fields:
        if (dataframe[first_field] != dataframe[other_field]).any():
            raise ValueError("Ambiguous and differing duplicate fields")
    else:
        dataframe.drop(columns=other_fields, inplace=True)
        return first_field


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
        try:
            internally_indexed_by = INPLACE_merge_duplicate_fields(
                unindexed_dataframe, matching_fields,
            )
        except ValueError:
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
        em = "Inconsistent internal and human-readable fields in assay metadata"
        raise GeneLabException(em)
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
    minimal_df = df.loc[:, [f != "Unknown" for f in fields]].copy()
    minimal_df.columns = minimal_df.columns.droplevel(1)
    minimal_df.columns.name = None
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


def INPLACE_force_default_name_delimiter_in_file_data(filedata, metadata_indexed_by, metadata_name_set):
    """In data read from file, find names and force default name delimiter"""
    if metadata_indexed_by in filedata.columns:
        filedata[metadata_indexed_by] = filedata[metadata_indexed_by].apply(
            force_default_name_delimiter,
        )
    filedata.columns = [
        force_default_name_delimiter(column)
        if force_default_name_delimiter(column) in metadata_name_set
        else column
        for column in filedata.columns[:]
    ]


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
        try:
            self.metadata = MetadataLike(assay_json)
            self.annotation = MetadataLike(sample_json)
            self.factors = MetadataLike(sample_json, r'^Factor Value')
        except IndexError as e:
            msg = "{}, {}: {}".format(dataset.accession, name, e)
            raise GeneLabJSONException(msg)
 
    def resolve_filename(self, mask, sample_mask=".*", field_mask=".*"):
        """Given masks, find filenames, urls, and datestamps"""
        dataset_level_files = self.dataset.resolve_filename(mask)
        metadata_filters_present = (sample_mask != ".*") or (field_mask != ".*")
        if len(dataset_level_files) == 0:
            return {}
        elif (len(dataset_level_files) == 1) and (not metadata_filters_present):
            return dataset_level_files
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
 
    def get_file(self, mask, sample_mask=".*", field_mask=".*", astype=None, sep=None):
        """Given masks, read file data directly from cold storage"""
        fileinfos = self.resolve_filename(mask, sample_mask, field_mask)
        if len(fileinfos) == 0:
            raise GeneLabException("File not found")
        elif len(fileinfos) > 1:
            raise GeneLabException("Ambiguous file lookup")
        else:
            fileinfo = copy(next(iter(fileinfos.values())))
        if astype is None:
            with urlopen(fileinfo.url) as response:
                fileinfo.filedata = response.read()
        elif astype is DataFrame:
            fileinfo.filedata = read_csv(fileinfo.url, sep=sep)
            if fileinfo.filedata.columns[0] == "Unnamed: 0":
                fileinfo.filedata.columns = (
                    [self.metadata.indexed_by] +
                    list(fileinfo.filedata.columns[1:])
                )
            INPLACE_force_default_name_delimiter_in_file_data(
                fileinfo.filedata,
                metadata_indexed_by=self.metadata.indexed_by,
                metadata_name_set=set(self.metadata.full.index),
            )
        else:
            raise NotImplementedError("Unsupported astype in get_file()")
        return fileinfo
