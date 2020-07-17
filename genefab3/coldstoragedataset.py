from urllib.request import urlopen
from json import loads
from re import search, sub
from genefab3.exceptions import GeneLabException, GeneLabJSONException
from genefab3.utils import COLD_API_ROOT, GENELAB_ROOT
from genefab3.utils import date2timestamp, levenshtein_distance
from genefab3.coldstorageassay import ColdStorageAssay
from pandas import DataFrame, concat
from argparse import Namespace


def parse_glds_json(accession):
    """Parse GLDS JSON reported by cold storage"""
    url = "{}/data/study/data/{}/".format(COLD_API_ROOT, accession)
    with urlopen(url) as response:
        data_json = loads(response.read().decode())
    if len(data_json) == 0:
        raise GeneLabJSONException("Invalid JSON (GLDS does not exist?)")
    elif len(data_json) > 1:
        raise GeneLabJSONException("Invalid JSON, too many sections")
    else:
        try:
            json = data_json[0]
            _id, metadata_id = json["_id"], json["metadata_id"]
        except KeyError:
            raise GeneLabJSONException("Invalid JSON, missing ID fields")
        foreign_fields = json.get("foreignFields", [])
        if len(foreign_fields) == 0:
            raise GeneLabJSONException("Invalid JSON, no foreignFields")
        elif len(foreign_fields) > 1:
            raise GeneLabJSONException("Invalid JSON, multiple foreignFields")
        else:
            try:
                info = foreign_fields[0]["isa2json"]["additionalInformation"]
            except KeyError:
                raise GeneLabJSONException("Invalid JSON: isa2json")
            return _id, metadata_id, info


def parse_fileurls_json(accession):
    """Parse file urls JSON reported by cold storage"""
    accession_number_match = search(r'\d+$', accession)
    if accession_number_match:
        accession_number = accession_number_match.group()
    else:
        raise GeneLabException("Malformed accession number")
    url = "{}/data/glds/files/{}".format(COLD_API_ROOT, accession_number)
    with urlopen(url) as response:
        fileurls_json = loads(response.read().decode())
    try:
        return {
            fd["file_name"]: GENELAB_ROOT+fd["remote_url"]
            for fd in fileurls_json["studies"][accession]["study_files"]
        }
    except KeyError:
        raise GeneLabJSONException("Malformed 'files' JSON")


def parse_filedates_json(_id):
    """Parse file dates JSON reported by cold storage"""
    url = "{}/data/study/filelistings/{}".format(COLD_API_ROOT, _id)
    with urlopen(url) as response:
        filedates_json = loads(response.read().decode())
    try:
        return {fd["file_name"]: date2timestamp(fd) for fd in filedates_json}
    except KeyError:
        raise GeneLabJSONException("Malformed 'filelistings' JSON")


class ColdStorageDataset():
    """Contains GLDS metadata associated with an accession number"""
 
    def __init__(self, accession):
        """Request JSON representation of ISA metadata and store fields"""
        self.accession = accession
        self._id, self.metadata_id, info = parse_glds_json(accession)
        try:
            self.json = Namespace(**{
                field: info[field] for field in
                ("description", "samples", "ontologies", "organisms")
            })
        except KeyError:
            raise GeneLabJSONException("Invalid JSON, missing isa2json fields")
        self.fileurls = parse_fileurls_json(accession)
        self.filedates = parse_filedates_json(self._id)
        try:
            self.assays = {name: None for name in info["assays"]} # placeholders
            self.assays = ColdStorageAssayDispatcher( # actual assays
                dataset=self, assays_json=info["assays"],
            )
        except KeyError:
            raise GeneLabJSONException("Invalid JSON, missing 'assays' field")
 
    @property
    def summary(self):
        """List factors, assay names and types"""
        assays_summary = self.assays.summary.copy()
        assays_summary["type"] = "assay"
        factors_dataframe = DataFrame(
            columns=["type", "name", "factors"],
            data=[
                ["dataset", self.accession, fi["factor"]]
                for fi in self.json.description["factors"]
            ]
        )
        return concat([factors_dataframe, assays_summary], axis=0, sort=False)
 
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
 
    def __init__(self, dataset, assays_json):
        """Populate dictionary of assay_name -> Assay()"""
        try:
            for assay_name, assay_json in assays_json.items():
                sample_key = infer_sample_key(
                    assay_name, dataset.json.samples.keys(),
                )
                super().__setitem__(
                    assay_name, ColdStorageAssay(
                        dataset, assay_name, assay_json,
                        sample_json=dataset.json.samples[sample_key],
                    )
                )
        except KeyError:
            raise GeneLabJSONException("Malformed assay JSON")
