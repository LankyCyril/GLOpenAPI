from genefab3.common.exceptions import GeneLabFileException, GeneLabISAException
from genefab3.common.utils import pick_reachable_url
from genefab3.db.sql.file import CachedBinaryFile
from genefab3.isa.parser import IsaZip
from genefab3.common.utils import copy_and_drop


class Dataset():
 
    def __init__(self, accession, files, mongo_db, sqlite_blobs):
        self.accession, self.files = accession, files
        self.mongo_db, self.sqlite_blobs = mongo_db, sqlite_blobs
        isa_files = {
            filename: descriptor for filename, descriptor in files.items()
            if descriptor.get("datatype") == "isa"
        }
        if len(isa_files) != 1:
            raise GeneLabFileException(
                "File entries for Dataset must contain exactly one ISA file",
                accession, filenames=sorted(isa_files),
            )
        else:
            isa_name, isa_desc = next(iter(isa_files.items()))
            with pick_reachable_url(isa_desc["urls"]) as url:
                isa_file = CachedBinaryFile(
                    name=isa_name, sqlite_db=self.sqlite_blobs,
                    url=url, timestamp=isa_desc["timestamp"],
                )
            self.isa = IsaZip(
                data=isa_file.data,
                logger_info=dict(filename=isa_file.name, url=isa_file.url),
            )
            self.isa.changed = isa_file.changed
 
    @property
    def samples(self):
        for assay_entry in self.isa.assays:
            yield Sample(self, assay_entry)


class Assay():
    pass


class Sample(dict):
    """Represents a single Sample entry generated from Assay, Study, general Investigation entries"""
 
    @property
    def name(self): return self.get("Info", {}).get("Sample Name")
    @property
    def sample_name(self): return self.name
    @property
    def study_name(self): return self.get("Info", {}).get("Study")
    @property
    def assay_name(self): return self.get("Info", {}).get("Assay")
    @property
    def accession(self): return self.get("Info", {}).get("Accession")
 
    def __init__(self, dataset, assay_entry):
        """Represents a single Sample entry generated from Assay, Study, general Investigation entries"""
        self.dataset, self["Info"] = dataset, {"Accession": dataset.accession}
        # associate with assay name:
        self["Info"]["Assay"] = self._get_subkey_value(
            assay_entry, "Info", "Assay",
        )
        # associate with sample name:
        self["Info"]["Sample Name"] = self._get_unique_primary_value(
            assay_entry, "Sample Name",
        )
        # associate with assay and study metadata:
        self._INPLACE_extend_with_assay_metadata(assay_entry)
        self._INPLACE_extend_with_study_metadata()
        # associate with files # TODO
 
    def _INPLACE_extend_with_assay_metadata(self, assay_entry):
        """Populate with Assay tab annotation, Investigation Study Assays entry"""
        self["Assay"] = copy_and_drop(assay_entry, {"Info"})
        self["Investigation"] = {
            k: v for k, v in self.dataset.isa.investigation.items()
            if (isinstance(v, list) or k == "Investigation")
        }
        self["Investigation"]["Study Assays"] = (
            self.dataset.isa.investigation["Study Assays"].get(
                self.assay_name, {},
            )
        )
 
    def _INPLACE_extend_with_study_metadata(self):
        """Populate with Study tab annotation for entries matching current Sample Name"""
        if self.name in self.dataset.isa.studies._by_sample_name:
            study_entry = self.dataset.isa.studies._by_sample_name[self.name]
            self["Info"]["Study"] = self._get_subkey_value(
                study_entry, "Info", "Study",
            )
            self["Study"] = copy_and_drop(study_entry, {"Info"})
            self["Investigation"]["Study"] = (
                self.dataset.isa.investigation["Study"].get(self.study_name, {})
            )
 
    def _get_subkey_value(self, entry, key, subkey):
        """Check existence of `key.subkey` in entry and return its value"""
        try:
            return entry[key][subkey]
        except (TypeError, KeyError):
            raise GeneLabISAException(
                "Could not retrieve value of `key.subkey` from Assay entry",
                self.dataset, key=key, subkey=subkey,
            )
 
    def _get_unique_primary_value(self, entry, key):
        """Check validity / uniqueness of `key[*].''` in entry and return its value"""
        values = set()
        for branch in entry.get(key, {}):
            if "" in branch:
                values.add(branch[""])
        if len(values) == 0:
            raise GeneLabISAException(
                "Could not retrieve any value of `key` from Assay entry",
                self.dataset, assay_name=self.assay_name, key=key,
            )
        elif len(values) > 1:
            raise GeneLabISAException(
                f"Ambiguous values of `key` for one Assay entry",
                self.dataset, assay_name=self.assay_name,
                key=key, values=sorted(values),
            )
        else:
            return values.pop()
