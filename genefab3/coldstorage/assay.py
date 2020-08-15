from genefab3.exceptions import GeneLabJSONException, GeneLabException
from genefab3.config import INDEX_BY
from genefab3.utils import force_default_name_delimiter
from pandas import DataFrame, read_csv, isnull, MultiIndex
from re import compile, search, split, sub, IGNORECASE
from copy import copy
from urllib.request import urlopen
from itertools import count


def filter_table(sparse_table, use=None, discard=None, index_by=INDEX_BY):
    """Reduce metadata-like to columns matching `use` XOR not matching `discard`"""
    if use:
        expression = r'^{}:\s*'.format(use)
        matches = compile(expression, flags=IGNORECASE).search
        filtered_columns = [
            (l0, l1) for l0, l1 in sparse_table.columns
            if (not isnull(l0)) and (matches(l0) or (l0 == index_by))
        ]
    elif discard:
        expression = r'^({}):\s*'.format("|".join(discard))
        matches = compile(expression, flags=IGNORECASE).search
        filtered_columns = [
            (l0, l1) for l0, l1 in sparse_table.columns
            if (not isnull(l0)) and (matches(l0) or (l0 == index_by))
        ]
    filtered_table = sparse_table[filtered_columns].copy()
    return filtered_table


def strip_prefixes(dataframe, use):
    """Remove prefixes used to filter original table"""
    expression = r'^{}:\s*'.format(use)
    return DataFrame(
        data=dataframe.values,
        index=dataframe.index,
        columns=MultiIndex.from_tuples([
            (sub(expression, "", l0, flags=IGNORECASE), l1)
            for l0, l1 in dataframe.columns
        ]),
    )


def make_metadatalike_dataframe(raw_dataframe, index_by=INDEX_BY, use=None):
    """Index dataframe by index_by"""
    index_columns = raw_dataframe[index_by]
    if index_columns.shape[1] == 0:
        raise GeneLabException("Nonexistent field: " + index_by)
    elif index_columns.shape[1] > 1:
        if index_columns.T.drop_duplicates().shape[0] > 1:
            msg = "Multiple fields with differing values: " + index_by
            raise GeneLabException(msg)
        else: # will remove duplicated index columns regardless of second level
            c = count()
            keep = [
                not (col==index_by and next(c) or 0)
                for col, _ in raw_dataframe.columns
            ]
    else: # make sure `keep` exists but does not remove anything
        keep = [True] * raw_dataframe.shape[1]
    index = (index_by, index_columns.columns[0])
    full = raw_dataframe.loc[:,keep].set_index(index)
    if use:
        full = strip_prefixes(full, use)
    full.index.name, full.columns.names = None, index
    return full


def make_named_metadatalike_dataframe(df, index_by=INDEX_BY):
    """Keep only named (known) variable columns, drop internal field names"""
    fields = df.columns.get_level_values(0)
    named_df = df.loc[:, [not isnull(f) for f in fields]].copy()
    named_df.columns = named_df.columns.droplevel(1)
    named_df.columns.name = None
    if index_by:
        named_df.index.name = index_by
    return named_df


class MetadataLike():
    """Stores assay fields and metadata in raw and processed form"""
 
    def __init__(self, sparse_table, use=None, discard=None, index_by=INDEX_BY, harmonize=lambda f: sub(r'_', " ", f).lower()):
        """Convert assay JSON to metadata object"""
        if (use is None) and (discard is None):
            raw_dataframe = sparse_table
        elif use:
            raw_dataframe = filter_table(
                sparse_table, use=use, index_by=index_by,
            )
        elif discard:
            raw_dataframe = filter_table(
                sparse_table, discard=discard, index_by=index_by,
            )
        else:
            raise GeneLabException("MetadataLike can only 'use' XOR 'discard'")
        if harmonize:
            raw_dataframe.columns = MultiIndex.from_tuples([
                (l0, l1) if isnull(l0) else (harmonize(l0), l1)
                for l0, l1 in raw_dataframe.columns
            ])
            index_by = harmonize(index_by)
        self.full = make_metadatalike_dataframe(raw_dataframe, index_by, use)
        self.named = make_named_metadatalike_dataframe(self.full, index_by)
        self.indexed_by = index_by


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
 
    def __init__(self, dataset, name, sample_key):
        """Parse assay-related entries from ISA"""
        try:
            _ = dataset.assays[name]
        except (AttributeError, KeyError):
            msg = "Attempt to associate an assay with the wrong dataset"
            raise GeneLabException(msg)
        self.name = name
        self.dataset = dataset
        try:
            assays_isa = dataset.isa.assays[name]
            samples_isa = dataset.isa.samples[sample_key]
            ML = MetadataLike
            self.metadata = ML(assays_isa)
            self.factors = ML(samples_isa, use="factor value")
            self.parameters = ML(samples_isa, use="parameter value")
            self.characteristics = ML(samples_isa, use="characteristics")
            self.comments = ML(samples_isa, use="comment")
            self.properties = ML(samples_isa, discard={
                "factor value", "parameter value", "characteristics", "comment",
            })
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
