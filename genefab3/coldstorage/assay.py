from genefab3.exceptions import GeneLabException, GeneLabISAException
from genefab3.utils import force_default_name_delimiter, copy_and_drop
from pandas import DataFrame, read_csv
from re import search, split
from urllib.request import urlopen


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


WRONG_DATASET_ERROR = "Attempt to associate an assay with the wrong dataset"
NO_SAMPLE_NAME_ERROR = "Could not retrieve Sample Name from Assay entry"
AMBIGUOUS_SAMPLE_NAME_ERROR = "Ambiguous Sample Names for one Assay entry"


class ColdStorageAssay():
    """Stores individual assay information and metadata"""
    name = None
 
    def __init__(self, dataset, assay_name, isa_assay_entries):
        """Combine and re-parse entries from dataset ISA"""
        self.name = assay_name
        self._assert_correct_dataset(dataset, assay_name)
        self.meta = {}
        for isa_assay_entry in isa_assay_entries:
            try: # check validity / uniqueness of Sample Name entries
                entry_sample_names = {
                    ee[""] for ee in isa_assay_entry["Sample Name"]
                }
            except (KeyError, IndexError, TypeError):
                raise GeneLabISAException(NO_SAMPLE_NAME_ERROR)
            if len(entry_sample_names) != 1:
                raise GeneLabISAException(AMBIGUOUS_SAMPLE_NAME_ERROR)
            else: # populate metadata from Assay, general Investigation entries
                sample_name = entry_sample_names.pop()
                self.meta[sample_name] = self._init_sample_entry_with_assay(
                    dataset, isa_assay_entry, assay_name, sample_name,
                )
        # populate annotation from Study and Investigation entries:
        for sample_name in self.meta:
            if sample_name in dataset.isa.studies._by_sample_name:
                # populate annotation from Study entries matching Sample Names:
                self._extend_sample_entry_with_study(
                    self.meta[sample_name], dataset, sample_name,
                )
 
    def _assert_correct_dataset(self, dataset, assay_name):
        """Check if being associated with correct dataset"""
        try:
            _ = dataset.assays[assay_name]
        except (KeyError, IndexError, TypeError):
            raise GeneLabException(WRONG_DATASET_ERROR)
 
    def _init_sample_entry_with_assay(self, dataset, isa_assay_entry, assay_name, sample_name):
        """Create sample entry for `sample_name`, associate with accession, Assay tab annotation, Investigation Study Assays entry"""
        sample_entry = {
            "": {
                "Accession": dataset.accession, "Assay": assay_name,
                "Sample Name": sample_name,
            },
            "Assay": copy_and_drop(isa_assay_entry, {""}),
            "Investigation": {
                k: v for k, v in dataset.isa.investigation.items()
                if (isinstance(v, list) or k == "Investigation")
            },
        }
        sample_entry["Investigation"]["Study Assays"] = (
            dataset.isa.investigation["Study Assays"].get(assay_name, {})
        )
        return sample_entry
 
    def _extend_sample_entry_with_study(self, sample_entry, dataset, sample_name):
        """Add Study tab annotation, Investigation Study entry"""
        isa_study_entry = dataset.isa.studies._by_sample_name[sample_name]
        study_name = isa_study_entry[""]["Study"]
        sample_entry[""]["Study"] = study_name
        sample_entry["Study"] = copy_and_drop(isa_study_entry, {""})
        sample_entry["Investigation"]["Study"] = (
            dataset.isa.investigation["Study"].get(study_name, {})
        )
 
    def resolve_filename(self, mask, sample_mask=".*", field_mask=".*"):
        """Given masks, find filenames, urls, and datestamps"""
        dataset_level_files = self.dataset.resolve_filename(mask)
        metadata_filters_present = (sample_mask != ".*") or (field_mask != ".*")
        if len(dataset_level_files) == 0:
            return {}
        elif (len(dataset_level_files) == 1) and (not metadata_filters_present):
            return dataset_level_files
        else:
            metadata_df = self.metadata.full # TODO new logic
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
            fileinfo = deepcopy(next(iter(fileinfos.values())))
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
