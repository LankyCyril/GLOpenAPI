from urllib.request import urlopen
from urllib.error import URLError
from json import loads
from natsort import natsorted
from genefab3.common.types import Adapter
from genefab3.common.exceptions import GeneFabJSONException
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.common.exceptions import GeneFabDataManagerException
from pandas import json_normalize, Timestamp
from urllib.parse import quote
from re import search, sub
from warnings import catch_warnings, filterwarnings
from dateutil.parser import UnknownTimezoneWarning
from functools import lru_cache


GENELAB_ROOT = "https://genelab-data.ndc.nasa.gov"
COLD_API_ROOT = GENELAB_ROOT + "/genelab"
COLD_SEARCH_MASK = COLD_API_ROOT + "/data/search/?term=GLDS&type=cgene&size={}"
COLD_GLDS_MASK = COLD_API_ROOT + "/data/study/data/{}/"
COLD_FILELISTINGS_MASK = COLD_API_ROOT + "/data/study/filelistings/{}"
ALT_FILEPATH = "/genelab/static/media/dataset/"


SPECIAL_FILE_TYPES = {
    r'.*_metadata_.*[_-]ISA\.zip$': {
        "cached": True,
        "datatype": "isa",
    },
    r'rna_seq_Unnormalized_Counts\.csv$': {
        "cached": True,
        "type": "table",
        "datatype": "unnormalized counts",
        "joinable": True,
        "index_name": "ENSEMBL",
        "column_subset": "sample name",
    },
    r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_differential_expression\.csv$': {
        "cached": True,
        "type": "table",
        "datatype": "differential expression",
        "index_name": "ENSEMBL",
    },
    r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_output_table\.csv$': {
        "cached": True,
        "type": "table",
        "datatype": "visualization table",
        "index_name": "ENSEMBL",
        "unconditional": True,
    },
    r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_PCA_table.csv$': {
        "cached": True,
        "type": "table",
        "datatype": "pca",
        "index_name": "sample name",
        "unconditional": True,
    },
}


def read_json(url):
    """Get parsed JSON from URL"""
    try:
        with urlopen(url) as response:
            return loads(response.read().decode())
    except URLError:
        raise GeneFabDataManagerException("Not found", url=url)


def as_timestamp(dataframe, column, default=-1):
    """Convert dataframe[column] to Unix timestamp, filling in `default` where not available"""
    if column not in dataframe:
        return default
    else:
        def safe_timestamp(value):
            try:
                return int(Timestamp(value).timestamp())
            except ValueError:
                return default
        return dataframe[column].apply(safe_timestamp)


def format_file_entry(row):
    """Format filelisting dataframe row to include URLs, timestamp, datatype, rules"""
    filename = row["file_name"]
    version_info = "?version={}".format(row["version"])
    entry = {
        "urls": [
            GENELAB_ROOT + quote(row["remote_url"]) + version_info,
            GENELAB_ROOT + ALT_FILEPATH + quote(filename) + version_info,
        ],
        "timestamp": row["timestamp"],
    }
    matched_patterns = set()
    for pattern, metadata in SPECIAL_FILE_TYPES.items():
        if search(pattern, filename):
            entry.update(metadata)
            matched_patterns.add(pattern)
    if len(matched_patterns) > 1:
        raise GeneFabConfigurationException(
            "File name matches more than one predefined pattern",
            filename=filename, patterns=sorted(matched_patterns),
        )
    return entry


class GeneLabAdapter(Adapter):
 
    def get_accessions(self):
        """Return list of dataset accessions available through genelab.nasa.gov/genelabAPIs"""
        try:
            n_datasets = read_json(COLD_SEARCH_MASK.format(0))["hits"]["total"]
            return natsorted(
                entry["_id"] for entry in
                read_json(COLD_SEARCH_MASK.format(n_datasets))["hits"]["hits"]
            )
        except (KeyError, TypeError):
            raise GeneFabJSONException("Malformed GeneLab search JSON")
 
    def get_files_by_accession(self, accession):
        """Get dictionary of files for dataset available through genelab.nasa.gov/genelabAPIs"""
        try:
            url = COLD_GLDS_MASK.format(accession)
            glds_json = read_json(url)
            assert len(glds_json) == 1
            _id = glds_json[0]["_id"]
        except (AssertionError, IndexError, KeyError, TypeError):
            raise GeneFabJSONException(
                "Malformed GLDS JSON", accession,
                url=url, object_type=type(glds_json).__name__,
                length=getattr(glds_json, "__len__", lambda: None)(),
                target="[0]['_id']",
            )
        try:
            url = COLD_FILELISTINGS_MASK.format(_id)
            filelisting_json = read_json(url)
            assert isinstance(filelisting_json, list)
        except AssertionError:
            raise GeneFabJSONException(
                "Malformed 'filelistings' JSON", accession, _id=_id,
                url=url, object_type=type(filelisting_json).__name__,
                expected_type="list",
            )
        else:
            files = json_normalize(filelisting_json)
        with catch_warnings():
            filterwarnings("ignore", category=UnknownTimezoneWarning)
            files["date_created"] = as_timestamp(files, "date_created")
            files["date_modified"] = as_timestamp(files, "date_modified")
        files["timestamp"] = files[["date_created", "date_modified"]].max(axis=1)
        return {
            row["file_name"]: format_file_entry(row)
            for _, row in files.sort_values(by="timestamp").iterrows()
        }
 
    def best_sample_name_matches(self, name, names):
        """Match ISA sample names to their variants in data files (R-like dot-separated, postfixed)"""
        dotted = lru_cache(maxsize=None)(lambda s: sub(r'[._-]', ".", s))
        matches = [ns for ns in names if ns == name]
        if matches:
            return matches
        else:
            matches = [ns for ns in names if dotted(ns) == dotted(name)]
            if matches:
                return matches
            else:
                return [
                    ns for ns in names if
                    dotted(ns).startswith(dotted(name)) or
                    dotted(name).startswith(dotted(ns))
                ]
