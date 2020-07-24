from urllib.request import urlopen
from json import loads
from genefab3.config import COLD_API_ROOT
from re import search
from genefab3.exceptions import GeneLabException, GeneLabJSONException


def download_cold_json(identifier, kind="other"):
    """Request and pre-parse cold storage JSONs for datasets, file listings, file dates"""
    if kind == "glds":
        url = "{}/data/study/data/{}/".format(COLD_API_ROOT, identifier)
        with urlopen(url) as response:
            return loads(response.read().decode())
    elif kind == "fileurls":
        accession_number_match = search(r'\d+$', identifier)
        if accession_number_match:
            accession_number = accession_number_match.group()
        else:
            raise GeneLabException("Malformed accession number")
        url = "{}/data/glds/files/{}".format(COLD_API_ROOT, accession_number)
        with urlopen(url) as response:
            raw_json = loads(response.read().decode())
            try:
                return raw_json["studies"][identifier]["study_files"]
            except KeyError:
                raise GeneLabJSONException("Malformed 'files' JSON")
    elif kind == "filedates":
        url = "{}/data/study/filelistings/{}".format(COLD_API_ROOT, identifier)
        with urlopen(url) as response:
            return loads(response.read().decode())
    elif kind == "other":
        url = identifier
        with urlopen(url) as response:
            return loads(response.read().decode())
    else:
        raise GeneLabException("Unknown JSON request: kind='{}'".format(kind))
