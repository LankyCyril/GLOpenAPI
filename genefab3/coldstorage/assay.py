from genefab3.exceptions import GeneLabJSONException, GeneLabException
from genefab3.config import INDEX_BY, ASSAY_TYPES
from genefab3.utils import force_default_name_delimiter
from pandas import DataFrame, read_csv, isnull, MultiIndex, concat
from re import compile, search, split, sub, IGNORECASE
from copy import copy
from urllib.request import urlopen
from itertools import count
from numpy import vstack


def filter_table(isa_table, use=None, discard=None, index_by=INDEX_BY):
    """Reduce metadata-like to columns matching `use` XOR not matching `discard`"""
    if use:
        expression = r'^{}:\s*'.format(use)
        matches = compile(expression, flags=IGNORECASE).search
        filtered_columns = [
            (l0, l1) for l0, l1 in isa_table.columns
            if (not isnull(l0)) and (matches(l0) or (l0 == index_by))
        ]
    elif discard:
        expression = r'^({}):\s*'.format("|".join(discard))
        matches = compile(expression, flags=IGNORECASE).search
        filtered_columns = [
            (l0, l1) for l0, l1 in isa_table.columns
            if (not isnull(l0)) and ((not matches(l0)) or (l0 == index_by))
        ]
    filtered_table = isa_table[filtered_columns].copy()
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


def make_metadatalike_dataframe(isa_table, index_by=INDEX_BY, use=None):
    """Index dataframe by index_by"""
    index_columns = isa_table[index_by]
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
                for col, _ in isa_table.columns
            ]
    else: # make sure `keep` exists but does not remove anything
        keep = [True] * isa_table.shape[1]
    index = (index_by, index_columns.columns[0])
    full = isa_table.loc[:,keep].set_index(index)
    if (full.shape[1] > 0) and use:
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


def ISATableLike(data, like):
    """Make ISATable from arbitrary dict with shape and INDEX_BY like that of `like`"""
    c = count(200000)
    values = [v for vv in data.values() for v in vv]
    titles = [k for k, v in data.items() for _ in range(len(v))]
    fields = ["a{}{}".format(next(c), sub(" ", "", t)) for t in titles]
    return concat([
        DataFrame(
            data=vstack([values]*like.full.shape[0]),
            columns=MultiIndex.from_arrays([titles, fields]),
        ),
        DataFrame(
            data=like.full.index,
            columns=MultiIndex.from_tuples([like.full.columns.names]),
        ),
    ], axis=1)


class MetadataLike():
    """Stores assay fields and metadata in raw and processed form"""
 
    def __init__(self, data, like=None, use=None, discard=None, index_by=INDEX_BY, harmonize=lambda f: sub(r'_', " ", f).lower()):
        """Convert assay ISATable to metadata object, or make metadata object similar to `like`"""
        if isinstance(data, dict) and isinstance(like, MetadataLike):
            isa_table = ISATableLike(data, like)
        elif isinstance(data, DataFrame):
            isa_table = data
        else:
            raise GeneLabException("MetadataLike from unsupported object type")
        if use:
            isa_table = filter_table(
                isa_table, use=use, index_by=index_by,
            )
        elif discard:
            isa_table = filter_table(
                isa_table, discard=discard, index_by=index_by,
            )
        elif use and discard:
            raise GeneLabException("MetadataLike can only 'use' XOR 'discard'")
        if harmonize:
            isa_table.columns = MultiIndex.from_tuples([
                (l0, l1) if isnull(l0) else (harmonize(l0), l1)
                for l0, l1 in isa_table.columns
            ])
            index_by = harmonize(index_by)
        self.full = make_metadatalike_dataframe(isa_table, index_by, use)
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


def infer_assay_types(assay_name):
    """Infer assay types from curated assay name"""
    return {
        "type": {
            assay_type.split("|")[0] for assay_type in ASSAY_TYPES if search(
                r'(-|_|^)' + assay_type.replace(" ", "_") + r'(-|_|$)',
                assay_name, flags=IGNORECASE,
            )
        }
    }


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
            self.assay_types = ML(infer_assay_types(name), like=self.comments)
        except IndexError as e:
            msg = "{}, {}: {}".format(dataset.accession, name, e)
            raise GeneLabJSONException(msg)
 
    def __getattr__(self, attribute):
        """Allow asking for metas with spaces: e.g., getattr(self, "assay types")"""
        if " " in attribute:
            return getattr(self, sub(r'\s', "_", attribute))
        else:
            raise AttributeError("'{}' object has no attribute '{}'".format(
                "ColdStorageAssay", attribute,
            ))
 
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
