from urllib.request import urlopen
from json import loads
from genefab3.common.exceptions import GeneLabJSONException, GeneLabFileException
from pandas import json_normalize, isnull, Timestamp
from re import search


COLD_API_ROOT = "https://genelab-data.ndc.nasa.gov/genelab"
COLD_SEARCH_MASK = COLD_API_ROOT + "/data/search/?term=GLDS&type=cgene&size={}"
COLD_GLDS_MASK = COLD_API_ROOT + "/data/study/data/{}/"
COLD_FILELISTINGS_MASK = COLD_API_ROOT + "/data/study/filelistings/{}"
ISA_ZIP_REGEX = r'.*_metadata_.*[_-]ISA\.zip$'


def read_json(url):
    with urlopen(url) as response:
        return loads(response.read().decode())


def get_accessions():
    try:
        n_datasets = read_json(COLD_SEARCH_MASK.format(0))["hits"]["total"]
        return {
            entry["_id"] for entry in
            read_json(COLD_SEARCH_MASK.format(n_datasets))["hits"]["hits"]
        }
    except (KeyError, TypeError):
        raise GeneLabJSONException("Malformed GeneLab search JSON")


def get_files(accession):
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
    if "date_created" in files:
        files["date_created"] = files["date_created"].apply(
            lambda d: -1 if isnull(d) else int(Timestamp(d).timestamp())
        )
    else:
        files["date_created"] = -1
    if "date_modified" in files:
        files["date_modified"] = files["date_modified"].apply(
            lambda d: -1 if isnull(d) else int(Timestamp(d).timestamp())
        )
    else:
        files["date_modified"] = -1
    files["timestamp"] = files[["date_created", "date_modified"]].max(axis=1)
    return {
        row["file_name"]: {
            "url": row["remote_url"], "timestamp": row["timestamp"],
        }
        for _, row in files.sort_values(by="timestamp").iterrows()
    }


def get_isa(accession, files=None):
    files = files or get_files(accession)
    candidates = {
        filename for filename in files if search(ISA_ZIP_REGEX, filename)
    }
    if len(candidates) == 0:
        raise GeneLabFileException("ISA ZIP not found", accession)
    elif len(candidates) == 1:
        filename = candidates.pop()
        return {filename: files[filename]}
    else:
        raise GeneLabFileException(
            "Multiple ambiguous ISA ZIPs", accession,
            filenames=sorted(candidates),
        )
