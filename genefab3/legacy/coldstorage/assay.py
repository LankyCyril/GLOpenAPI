from genefab3.common.types import AssayBaseClass
from genefab3.common.exceptions import GeneLabException, GeneLabISAException
from genefab3.common.utils import copy_and_drop


WRONG_DATASET_ERROR = "Attempt to associate an assay with the wrong dataset"
NO_SAMPLE_NAME_ERROR = "Could not retrieve Sample Name from Assay entry"
AMBIGUOUS_SAMPLE_NAME_ERROR = "Ambiguous Sample Names for one Assay entry"


class ColdStorageAssay(AssayBaseClass):
    """Stores individual assay information and metadata"""
    name = None
 
    def __init__(self, dataset, assay_name, isa_assay_entries):
        """Combine and re-parse entries from dataset ISA"""
        self._assert_correct_dataset(dataset, assay_name)
        self.dataset = dataset
        self.name = assay_name
        self.metadata = {}
        for isa_assay_entry in isa_assay_entries:
            try: # check validity / uniqueness of Sample Name entries
                entry_sample_names = {
                    ee[""] for ee in isa_assay_entry["Sample Name"]
                }
            except (KeyError, IndexError, TypeError):
                raise GeneLabISAException(NO_SAMPLE_NAME_ERROR, self)
            if len(entry_sample_names) != 1:
                raise GeneLabISAException(
                    AMBIGUOUS_SAMPLE_NAME_ERROR,
                    self, sample_names=entry_sample_names,
                )
            else: # populate metadata from Assay, general Investigation entries
                sample_name = entry_sample_names.pop()
                self.metadata[sample_name] = self._init_sample_entry_with_assay(
                    dataset, isa_assay_entry, assay_name, sample_name,
                )
        # populate annotation from Study and Investigation entries:
        for sample_name in self.metadata:
            if sample_name in dataset.isa.studies._by_sample_name:
                # populate annotation from Study entries matching Sample Names:
                self._extend_sample_entry_with_study(
                    self.metadata[sample_name], dataset, sample_name,
                )
 
    def _assert_correct_dataset(self, dataset, assay_name):
        """Check if being associated with correct dataset"""
        try:
            _ = dataset.assays[assay_name]
        except (KeyError, IndexError, TypeError):
            raise GeneLabException(WRONG_DATASET_ERROR, dataset, assay_name)
 
    def _init_sample_entry_with_assay(self, dataset, isa_assay_entry, assay_name, sample_name):
        """Create sample entry for `sample_name`, associate with accession, Assay tab annotation, Investigation Study Assays entry"""
        sample_entry = {
            "Info": {
                "Accession": dataset.accession, "Assay": assay_name,
                "Sample Name": sample_name,
            },
            "Assay": copy_and_drop(isa_assay_entry, {"Info"}),
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
        study_name = isa_study_entry["Info"]["Study"]
        sample_entry["Info"]["Study"] = study_name
        sample_entry["Study"] = copy_and_drop(isa_study_entry, {"Info"})
        sample_entry["Investigation"]["Study"] = (
            dataset.isa.investigation["Study"].get(study_name, {})
        )
