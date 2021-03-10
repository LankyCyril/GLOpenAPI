from urllib.request import urlopen
from json import loads
from natsort import natsorted
from genefab3.common.exceptions import GeneLabJSONException
from pandas import json_normalize, Timestamp
from urllib.parse import quote
from re import search


GENELAB_ROOT = "https://genelab-data.ndc.nasa.gov"
COLD_API_ROOT = GENELAB_ROOT + "/genelab"
COLD_SEARCH_MASK = COLD_API_ROOT + "/data/search/?term=GLDS&type=cgene&size={}"
COLD_GLDS_MASK = COLD_API_ROOT + "/data/study/data/{}/"
COLD_FILELISTINGS_MASK = COLD_API_ROOT + "/data/study/filelistings/{}"
ALT_FILEPATH = "/genelab/static/media/dataset/"


FILE_TYPES = {
    r'.*_metadata_.*[_-]ISA\.zip$': {
        "isa": True,
    },
    r'rna_seq_Unnormalized_Counts\.csv$': {
        "datatype": "unnormalized counts",
        "joinable": True,
        "columns": "Sample Name",
    },
    r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_differential_expression\.csv$': {
        "datatype": "differential expression",
    },
    r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_output_table\.csv$': {
        "datatype": "visualization table",
        "unconditional": True,
    },
    r'^GLDS-[0-9]+_(array|rna_seq)(_all-samples)?_visualization_PCA_table.csv$': {
        "datatype": "pca",
        "unconditional": True,
    },
}


def read_json(url):
    """Get parsed JSON from URL"""
    with urlopen(url) as response:
        return loads(response.read().decode())


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
    file_name = row["file_name"]
    version_info = "?version={}".format(row["version"])
    entry = {
        "urls": [
            GENELAB_ROOT + quote(row["remote_url"]) + version_info,
            GENELAB_ROOT + ALT_FILEPATH + quote(file_name) + version_info,
        ],
        "timestamp": row["timestamp"],
    }
    for pattern, metadata in FILE_TYPES.items():
        if search(pattern, file_name):
            entry.update(metadata)
    return entry


def get_dataset_files(accession):
    """Get dictionary of files for dataset available through genelab.nasa.gov/genelabAPIs"""
    try:
        glds_json = read_json(COLD_GLDS_MASK.format(accession))
        assert len(glds_json) == 1
        _id = glds_json[0]["_id"]
    except (AssertionError, IndexError, KeyError, TypeError):
        raise GeneLabJSONException("Malformed GLDS JSON", accession)
    try:
        filelisting_entries = read_json(COLD_FILELISTINGS_MASK.format(_id))
        assert isinstance(filelisting_entries, list)
    except AssertionError:
        raise GeneLabJSONException("Malformed 'filelistings' JSON", _id=_id)
    else:
        files = json_normalize(filelisting_entries)
    files["date_created"] = as_timestamp(files, "date_created")
    files["date_modified"] = as_timestamp(files, "date_modified")
    files["timestamp"] = files[["date_created", "date_modified"]].max(axis=1)
    return {
        row["file_name"]: format_file_entry(row)
        for _, row in files.sort_values(by="timestamp").iterrows()
    }


def get_genelab_accessions():
    """Return list of dataset accessions available through genelab.nasa.gov/genelabAPIs"""
    try:
        n_datasets = read_json(COLD_SEARCH_MASK.format(0))["hits"]["total"]
        return natsorted(
            entry["_id"] for entry in
            read_json(COLD_SEARCH_MASK.format(n_datasets))["hits"]["hits"]
        )
    except (KeyError, TypeError):
        raise GeneLabJSONException("Malformed GeneLab search JSON")
