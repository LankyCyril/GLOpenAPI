from genefab3.common.types import Adapter
from genefab3.common.exceptions import GeneFabDataManagerException
from genefab3.db.sql.files import CachedBinaryFile
from genefab3.isa.parser import IsaFromZip
from genefab3.common.utils import deepcopy_except, copy_except
from genefab3.common.exceptions import GeneFabISAException
from genefab3.common.utils import iterate_terminal_leaf_elements


class Dataset():
 
    def __init__(self, accession, files, sqlite_blobs, best_sample_name_matches=None, status_kwargs=None):
        self.accession, self.files = accession, files
        self.sqlite_blobs = sqlite_blobs
        self.best_sample_name_matches = (
            best_sample_name_matches or
            (lambda n, N: Adapter.best_sample_name_matches(None, n, N))
        )
        isa_files = {
            filename: descriptor for filename, descriptor in files.items()
            if descriptor.get("datatype") == "isa"
        }
        if len(isa_files) != 1:
            msg = "File entries for Dataset must contain exactly one ISA file"
            _kw = dict(accession=accession, filenames=set(isa_files))
            raise GeneFabDataManagerException(msg, **_kw)
        else:
            isa_name, isa_desc = next(iter(isa_files.items()))
            urls = isa_desc.get("urls", ())
            isa_file = CachedBinaryFile(
                name=isa_name, identifier=f"{accession}/ISA/{isa_name}",
                sqlite_db=self.sqlite_blobs,
                urls=urls, timestamp=isa_desc.get("timestamp", -1),
            )
            self.isa = IsaFromZip(
                data=isa_file.data,
                status_kwargs={
                    **(status_kwargs or {}), "accession": accession,
                    "filename": isa_file.name, "url": isa_file.url,
                },
            )
            self.isa.changed = isa_file.changed
 
    @property
    def samples(self):
        for assay_entry in self.isa.assays:
            yield Sample(self, assay_entry)


class Sample(dict):
    """Represents a single Sample entry generated from Assay, Study, general Investigation entries"""
 
    @property
    def name(self): return self.get("Id", {}).get("Sample Name")
    @property
    def sample_name(self): return self.name
    @property
    def study_name(self): return self.get("Id", {}).get("Study Name")
    @property
    def assay_name(self): return self.get("Id", {}).get("Assay Name")
    @property
    def accession(self): return self.get("Id", {}).get("Accession")
 
    def __init__(self, dataset, assay_entry):
        """Represents a single Sample entry generated from Assay, Study, general Investigation entries"""
        self.dataset, self["Id"] = dataset, {"Accession": dataset.accession}
        # associate with assay name:
        self["Id"]["Assay Name"] = self._get_subkey_value(
            assay_entry, "Id", "Assay Name",
        )
        # associate with sample name:
        self["Id"]["Sample Name"] = self._get_unique_primary_value(
            assay_entry, "Sample Name",
        )
        # validate names:
        for attr in "accession", "assay_name":
            value = getattr(self, attr)
            if isinstance(value, str):
                if {"$", "/"} & set(value):
                    msg = "Forbidden characters ('$', '/') in sample attribute"
                    raise GeneFabISAException(msg, **{f"self.{attr}": value})
        # associate with assay and study metadata:
        self._INPLACE_extend_with_assay_metadata(assay_entry)
        self._INPLACE_extend_with_study_metadata()
        self._INPLACE_extend_with_dataset_files()
 
    def _INPLACE_extend_with_assay_metadata(self, assay_entry):
        """Populate with Assay tab annotation, Investigation Study Assays entry"""
        self["Assay"] = deepcopy_except(assay_entry, "Id")
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
        matching_study_sample_names = set(
            self.dataset.best_sample_name_matches(
                self.name, self.dataset.isa.studies._by_sample_name,
            )
        )
        if len(matching_study_sample_names) == 1:
            study_entry = self.dataset.isa.studies._by_sample_name[
                matching_study_sample_names.pop()
            ]
            self["Id"]["Study Name"] = self._get_subkey_value(
                study_entry, "Id", "Study Name",
            )
            self["Study"] = deepcopy_except(study_entry, "Id")
            self["Investigation"]["Study"] = (
                self.dataset.isa.investigation["Study"].get(self.study_name, {})
            )
        elif len(matching_study_sample_names) > 1:
            raise GeneFabISAException(
                "Multiple Study 'Sample Name' entries match Assay entry",
                accession=self.dataset.accession, assay_sample_name=self.name,
                matching_study_sample_names=matching_study_sample_names,
            )
 
    def _INPLACE_extend_with_dataset_files(self):
        """Populate with File annotation for files that match records for the sample"""
        isa_elements = set(iterate_terminal_leaf_elements(self))
        _sdf = self.dataset.files
        _no_condition = lambda *_: True
        self["File"] = [
            {**copy_except(_sdf[f], "condition"), "filename": f} for f in {
                filename for filename, filedata in _sdf.items() if (
                    (filedata.get("internal") or (filename in isa_elements)) and
                    filedata.get("condition", _no_condition)(self, filename)
                )
            }
        ]
 
    def _get_subkey_value(self, entry, key, subkey):
        """Check existence of `key.subkey` in entry and return its value"""
        try:
            return entry[key][subkey]
        except (TypeError, KeyError):
            msg = "Could not retrieve value of `key.subkey` from Assay entry"
            _kw = dict(accession=self.dataset.accession, key=key, subkey=subkey)
            raise GeneFabISAException(msg, **_kw)
 
    def _get_unique_primary_value(self, entry, key):
        """Check validity / uniqueness of `key[*].''` in entry and return its value"""
        values = {branch[""] for branch in entry.get(key, {}) if ("" in branch)}
        _kw = dict(accession=self.dataset.accession, assay_name=self.assay_name)
        if len(values) == 0:
            msg = "Could not retrieve any value of `key` from Assay entry"
            raise GeneFabISAException(msg, **_kw, key=key)
        elif len(values) > 1:
            msg = "Ambiguous values of `key` for one Assay entry"
            raise GeneFabISAException(msg, **_kw, key=key, values=values)
        else:
            return values.pop()
