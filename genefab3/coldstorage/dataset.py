from genefab3.common.types_legacy import DatasetBaseClass, IterableNamespace
from re import search
from urllib.parse import quote
from genefab3.coldstorage.json import download_cold_json
from genefab3.common.types_legacy import PlaceholderLogger, FileDescriptor
from genefab3.common.exceptions import GeneLabJSONException
from genefab3.common.exceptions import GeneLabFileException
from memoized_property import memoized_property
from genefab3.config import GENELAB_ROOT, ISA_ZIP_REGEX
from genefab3.coldstorage.json import extract_file_timestamp
from genefab3.coldstorage.isa import IsaZip
from collections import defaultdict
from genefab3.coldstorage.assay import ColdStorageAssay


class ColdStorageDataset(DatasetBaseClass):
    """Contains GLDS metadata associated with an accession number"""
    json, changed = IterableNamespace(), IterableNamespace()
    accession, isa, assays = None, None, None
 
    def __init__(self, accession, init_assays=True, get_json=download_cold_json, logger=None):
        """Request JSONs (either from cold storage or from local cache) and optionally init assays via an ISA ZIP file"""
        self.logger = logger if (logger is not None) else PlaceholderLogger()
        self.json.glds, self.changed.glds = get_json(
            identifier=accession, kind="glds", report_changes=True,
        )
        if not self.json.glds:
            raise GeneLabJSONException("No dataset found", accession)
        try:
            assert len(self.json.glds) == 1
            self._id = self.json.glds[0]["_id"]
        except (AssertionError, IndexError, KeyError):
            raise GeneLabJSONException("Malformed GLDS JSON", accession)
        else:
            j = self.json.glds[0]
            if accession in {j.get("accession"), j.get("legacy_accession")}:
                self.accession = accession
            else:
                error = "Initializing with wrong JSON"
                raise GeneLabJSONException(error, accession)
        # populate file information:
        self.json.fileurls, self.changed.fileurls = get_json(
            identifier=accession, kind="fileurls", report_changes=True,
        )
        self.json.filedates, self.changed.filedates = get_json(
            identifier=self._id, kind="filedates", report_changes=True,
        )
        # initialize assays via ISA ZIP:
        if init_assays:
            self.init_assays()
 
    @memoized_property
    def fileurls(self):
        """Parse file URLs JSON reported by cold storage"""
        try:
            return {
                fd["file_name"]: GENELAB_ROOT + quote(fd["remote_url"])
                for fd in self.json.fileurls
            }
        except KeyError:
            raise GeneLabJSONException("Malformed 'files' JSON", self)
 
    @memoized_property
    def filedates(self):
        """Parse file dates JSON reported by cold storage"""
        try:
            return {
                fd["file_name"]: extract_file_timestamp(fd)
                for fd in self.json.filedates
            }
        except KeyError:
            raise GeneLabJSONException("Malformed 'filelistings' JSON", self)
 
    def get_file_descriptors(self, name=None, regex=None, glob=None):
        """Given mask, find filenames, urls, and datestamps"""
        if sum(arg is not None for arg in (name, regex, glob)) != 1:
            raise ValueError(
                "get_file_descriptors() accepts exactly one "
                "of `filename`, `regex`, `glob`"
            )
        elif name is not None:
            matches = lambda filename: filename == name
        elif regex is not None:
            matches = lambda filename: search(regex, filename)
        elif glob is not None:
            matches = lambda filename: search(glob.replace("*", ".*"), filename)
        return [
            FileDescriptor(
                filename, self.fileurls[filename], self.filedates[filename],
            )
            for filename in set(self.filedates) & set(self.fileurls)
            if matches(filename)
        ]

    def init_assays(self):
        """Initialize assays via ISA ZIP"""
        isa_zip_descriptors = self.get_file_descriptors(regex=ISA_ZIP_REGEX)
        if len(isa_zip_descriptors) == 0:
            raise GeneLabFileException("ISA ZIP not found", self)
        elif len(isa_zip_descriptors) == 1:
            self.isa = IsaZip(isa_zip_descriptors[0].url)
        else:
            raise GeneLabFileException("Multiple ambiguous ISA ZIPs", self)
        # first declare placeholders, then rewrite them with actual assays:
        self.assays = {e["Info"]["Assay"]: None for e in self.isa.assays}
        self.assays = AssayDispatcher(self)


class AssayDispatcher(dict):
    """Contains a dataset's assay objects, indexable by name"""
 
    def __init__(self, dataset):
        """Populate dictionary of assay_name -> Assay()"""
        self.dataset = dataset
        isa_entries_by_assay = defaultdict(list)
        for isa_entry in dataset.isa.assays:
            assay_name = isa_entry["Info"]["Assay"]
            isa_entries_by_assay[assay_name].append(isa_entry)
        for assay_name, isa_assay_entries in isa_entries_by_assay.items():
            super().__setitem__(
                assay_name,
                ColdStorageAssay(dataset, assay_name, isa_assay_entries),
            )
 
    def __getitem__(self, assay_name):
        """Get assay by name or alias"""
        if (assay_name == "assay") and (len(self) == 1):
            return dict.__getitem__(self, next(iter(self)))
        else:
            return dict.__getitem__(self, assay_name)
