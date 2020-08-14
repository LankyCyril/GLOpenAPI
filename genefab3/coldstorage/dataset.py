from sys import stderr
from re import search, sub
from genefab3.exceptions import GeneLabException, GeneLabJSONException
from genefab3.config import GENELAB_ROOT
from genefab3.utils import download_cold_json as dl_json
from genefab3.utils import extract_file_timestamp, levenshtein_distance
from genefab3.coldstorage.assay import ColdStorageAssay
from argparse import Namespace
from genefab3.isa.parser import ISA


def parse_fileurls_json(fileurls_json):
    """Parse file urls JSON reported by cold storage"""
    try:
        return {
            fd["file_name"]: GENELAB_ROOT+fd["remote_url"]
            for fd in fileurls_json
        }
    except KeyError:
        raise GeneLabJSONException("Malformed 'files' JSON")


def parse_filedates_json(filedates_json):
    """Parse file dates JSON reported by cold storage"""
    try:
        return {
            fd["file_name"]: extract_file_timestamp(fd)
            for fd in filedates_json
        }
    except KeyError:
        raise GeneLabJSONException("Malformed 'filelistings' JSON")


class ColdStorageDataset():
    """Contains GLDS metadata associated with an accession number"""
 
    def __init__(self, accession, glds_json=None, fileurls_json=None, filedates_json=None):
        """Request ISA and store fields"""
        self.isa = ISA(glds_json or dl_json(accession, "glds"))
        if accession not in {self.isa.accession, self.isa.legacy_accession}:
            raise GeneLabException("Initializing dataset with wrong JSON")
        else:
            self.accession = accession
        self.fileurls = parse_fileurls_json(
            fileurls_json or dl_json(accession, "fileurls"),
        )
        self.filedates = parse_filedates_json(
            filedates_json or dl_json(self.isa._id, "filedates"),
        )
        try:
            self.assays = {a: None for a in self.isa.assays} # placeholders
            self.assays = ColdStorageAssayDispatcher(self) # actual assays
        except (KeyError, TypeError):
            raise GeneLabJSONException("Invalid JSON (field 'assays')")
 
    def resolve_filename(self, mask):
        """Given mask, find filenames, urls, and datestamps"""
        return {
            filename: Namespace(
                filename=filename, url=url,
                timestamp=self.filedates.get(filename, -1)
            )
            for filename, url in self.fileurls.items() if search(mask, filename)
        }


def infer_sample_key(assay_name, keys):
    """Infer sample key for assay from dataset JSON"""
    expected_key = sub(r'^a', "s", assay_name)
    if expected_key in keys: # first, try key as-is, expected behavior
        return expected_key
    else: # otherwise, match regardless of case
        for key in keys:
            if key.lower() == expected_key.lower():
                return key
        else:
            if len(keys) == 1: # otherwise, the only one must be correct
                return next(iter(keys))
            else: # find longest match starting from head
                max_key_length = max(len(key) for key in keys)
                max_comparison_length = min(len(expected_key), max_key_length)
                for cl in range(max_comparison_length, 0, -1):
                    likely_keys = {
                        key for key in keys
                        if key[:cl].lower() == expected_key[:cl].lower()
                    }
                    if len(likely_keys) == 1:
                        return likely_keys.pop()
                    elif len(likely_keys) > 1: # resolve by edit distance
                        best_key, best_ld = None, -1
                        for key in likely_keys:
                            key_ld = levenshtein_distance(
                                key.lower(), expected_key.lower(),
                            )
                            if key_ld > best_ld:
                                best_key, best_ld = key, key_ld
                        if best_key:
                            return best_key
                else:
                    raise GeneLabException("No matching samples key for assay")


class ColdStorageAssayDispatcher(dict):
    """Contains a dataset's assay objects, indexable by name or by attributes"""
 
    def __init__(self, dataset):
        """Populate dictionary of assay_name -> Assay()"""
        for assay_name in dataset.isa.assays:
            sample_key = infer_sample_key(assay_name, dataset.isa.samples)
            if levenshtein_distance(assay_name, sample_key) > 1:
                msg = "Warning: ld('{}', '{}')".format(assay_name, sample_key)
                print(msg, file=stderr)
            super().__setitem__(
                assay_name, ColdStorageAssay(dataset, assay_name, sample_key)
            )
 
    def __getitem__(self, assay_name):
        """Get assay by name or alias"""
        if (assay_name == "assay") and (len(self) == 1):
            return dict.__getitem__(self, next(iter(self)))
        else:
            return dict.__getitem__(self, assay_name)
