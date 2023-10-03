from glopenapi.common.types import Adapter
from glopenapi.common.exceptions import GLOpenAPIDataManagerException
from glopenapi.db.sql.files import CachedBinaryFile
from glopenapi.isa.parser import IsaFromZip
from glopenapi.common.utils import deepcopy_except, copy_except
from glopenapi.common.exceptions import GLOpenAPIISAException, GLOpenAPILogger
from glopenapi.common.utils import iterate_terminal_leaf_elements


class Dataset():
 
    def __init__(self, accession, files, sqlite_dbs, best_sample_name_matches=None, status_kwargs=None):
        self.accession, self.files = accession, files
        self.sqlite_db = sqlite_dbs.blobs["db"]
        self.maxdbsize = sqlite_dbs.blobs["maxsize"]
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
            raise GLOpenAPIDataManagerException(msg, **_kw)
        else:
            isa_name, isa_desc = next(iter(isa_files.items()))
            urls = isa_desc.get("urls", ())
            isa_file = CachedBinaryFile(
                name=isa_name, identifier=f"BLOB:{accession}/ISA/{isa_name}",
                sqlite_db=self.sqlite_db, maxdbsize=self.maxdbsize,
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
                    raise GLOpenAPIISAException(msg, **{f"self.{attr}": value})
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
        _by_sample_name = self.dataset.isa.studies._by_sample_name
        matching_study_sample_names = set(
            self.dataset.best_sample_name_matches(self.name, _by_sample_name)
        )
        if len(matching_study_sample_names) > 1:
            raise GLOpenAPIISAException(
                "Multiple Study 'Sample Name' entries match Assay entry",
                accession=self.dataset.accession, assay_sample_name=self.name,
                matching_study_sample_names=matching_study_sample_names,
            )
        elif len(matching_study_sample_names) == 0: # fall back if single study
            study_names, study_entry = set(), None
            for entry in _by_sample_name.values():
                study_name = entry.get("Id", {}).get("Study Name")
                if study_name is not None:
                    study_entry = entry
                    study_names.add(study_name)
            if (len(study_names) != 1) or (study_entry is None):
                raise GLOpenAPIISAException(
                    "Could not match Assay entry to a single study",
                    accession=self.dataset.accession,
                    assay_sample_name=self.name, study_names=study_names,
                )
        else: # matched to Study entry, get actual info from there
            study_entry = _by_sample_name[matching_study_sample_names.pop()]
        self["Id"]["Study Name"] = self._get_subkey_value(
            study_entry, "Id", "Study Name",
        )
        self["Study"] = deepcopy_except(study_entry, "Id")
        self["Investigation"]["Study"] = (
            self.dataset.isa.investigation["Study"].get(self.study_name, {})
        )
 
    def _INPLACE_extend_with_dataset_files(self, check_isa_elements=True):
        """Populate with File annotation for files that match records for the sample"""
        if check_isa_elements:
            isa_elements = set(iterate_terminal_leaf_elements(self))
        else:
            isa_elements = type("", (set,), {"__contains__": lambda *_: True})()
        def _log(msg, _id=self.dataset.accession):
            _id = "/".join((self.accession, self.assay_name, self.sample_name))
            GLOpenAPILogger.debug(f"Files for {_id}: {msg}")
        def _format_file_entries(_sdf=self.dataset.files):
            _log("Processing entries")
            if check_isa_elements:
                _log(f"Entries checked against ISA: {isa_elements!r}")
            for fn, fd in _sdf.items():
                is_eligible = bool(fd.get("condition", lambda *_: 1)(self, fn))
                _log(f"{fn}: passes assay condition(s)? {is_eligible}")
                if check_isa_elements:
                    is_internal = bool(fd.get("internal"))
                    is_in_isa_elements = bool(fn in isa_elements)
                    _log(f"{fn}: is internal? {is_internal}")
                    _log(f"{fn}: is in isa elements? {is_in_isa_elements}")
                    is_eligible &= (is_internal | is_in_isa_elements)
                _log(f"{fn}: is eligible? {is_eligible}")
                if is_eligible:
                    yield {**copy_except(_sdf[fn], "condition"), "filename": fn}
        self["File"] = list(_format_file_entries())
 
    def _get_subkey_value(self, entry, key, subkey):
        """Check existence of `key.subkey` in entry and return its value"""
        try:
            return entry[key][subkey]
        except (TypeError, KeyError):
            msg = "Could not retrieve value of `key.subkey` from Assay entry"
            _kw = dict(accession=self.dataset.accession, key=key, subkey=subkey)
            raise GLOpenAPIISAException(msg, **_kw)
 
    def _get_unique_primary_value(self, entry, key):
        """Check validity / uniqueness of `key[*].''` in entry and return its value"""
        values = {branch[""] for branch in entry.get(key, {}) if ("" in branch)}
        _kw = dict(accession=self.dataset.accession, assay_name=self.assay_name)
        if len(values) == 0:
            msg = "Could not retrieve any value of `key` from Assay entry"
            raise GLOpenAPIISAException(msg, **_kw, key=key)
        elif len(values) > 1:
            msg = "Ambiguous values of `key` for one Assay entry"
            raise GLOpenAPIISAException(msg, **_kw, key=key, values=values)
        else:
            return values.pop()
