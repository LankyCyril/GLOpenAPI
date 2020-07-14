from urllib.request import urlopen
from json import loads
from re import search
from genefab3.exceptions import GeneLabException, GeneLabJSONException
from genefab3.utils import API_ROOT, GENELAB_ROOT
from genefab3.utils import date2stamp
from pandas import DataFrame, concat
from collections import defaultdict


def parse_glds_json(accession):
    """Parse GLDS JSON reported by cold storage"""
    url = "{}/data/study/data/{}/".format(API_ROOT, accession)
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
                isa2json = foreign_fields[0]["isa2json"]
            except KeyError:
                raise GeneLabJSONException("Invalid JSON, no isa2json")
            return _id, metadata_id, isa2json


def parse_fileurls_json(accession):
    """Parse file urls JSON reported by cold storage"""
    accession_number_match = search(r'\d+$', accession)
    if accession_number_match:
        accession_number = accession_number_match.group()
    else:
        raise GeneLabException("Malformed accession number")
    url = "{}/data/glds/files/{}".format(API_ROOT, accession_number)
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
    url = "{}/data/study/filelistings/{}".format(API_ROOT, _id)
    with urlopen(url) as response:
        filedates_json = loads(response.read().decode())
    try:
        return {fd["file_name"]: date2stamp(fd) for fd in filedates_json}
    except KeyError:
        raise GeneLabJSONException("Malformed 'filelistings' JSON")


class ColdStorageDataset():
    """Contains GLDS metadata associated with an accession number"""
 
    def __init__(self, accession):
        """Request JSON representation of ISA metadata and store fields"""
        self.accession = accession
        self._id, self.metadata_id, isa2json = parse_glds_json(accession)
        try:
            info = isa2json["additionalInformation"]
            self.description = info["description"]
            self.samples = info["samples"]
            self.ontologies = info["ontologies"]
            self.organisms = info["organisms"]
        except KeyError:
            raise GeneLabJSONException("Invalid JSON, missing isa2json fields")
        self.fileurls = parse_fileurls_json(accession)
        self.filedates = parse_filedates_json(self._id)
        try:
            self.assays = ColdStorageAssayDispatcher(
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
                for fi in self.description["factors"]
            ]
        )
        return concat([factors_dataframe, assays_summary], axis=0, sort=False)


class ColdStorageAssayDispatcher(dict):
    """Contains a dataset's assay objects, indexable by name or by attributes"""
 
    def __init__(self, dataset, assays_json):
        """Populate dictionary of assay_name -> Assay()"""
        try:
            for name, json in assays_json.items():
                super().__setitem__(name, ColdStorageAssay(dataset, name, json))
        except KeyError:
            raise GeneLabJSONException("Malformed assay JSON")


def parse_assay_json_fields(json):
    """Parse fields and titles from assay json 'header'"""
    try:
        header = json["header"]
        field2title = {entry["field"]: entry["title"] for entry in header}
    except KeyError:
        raise GeneLabJSONException("Malformed assay JSON: header")
    if len(field2title) != len(header):
        raise GeneLabJSONException("Conflicting IDs of data fields")
    fields = defaultdict(set)
    for field, title in field2title.items():
        fields[title].add(field)
    return dict(fields), field2title


class ColdStorageAssay():
    """Stores individual assay information and metadata in raw form"""
 
    def __init__(self, dataset, name, json):
        """Parse assay JSON reported by cold storage"""
        if isinstance(dataset, ColdStorageDataset):
            self.dataset = dataset
        else:
            self.dataset = ColdStorageDataset(dataset)
        self.fileurls = self.dataset.fileurls
        self.filedates = self.dataset.filedates
        self.fields, self.field2title = parse_assay_json_fields(json)
