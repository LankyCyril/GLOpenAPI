from genefab3.config import TIMESTAMP_FMT
from datetime import datetime
from urllib.request import urlopen
from genefab3.config import COLD_GLDS_MASK
from genefab3.config import COLD_FILEURLS_MASK, COLD_FILEDATES_MASK
from json import loads
from re import search
from genefab3.common.exceptions import GeneLabException, GeneLabJSONException


def extract_timestamp(json, key="date_modified", fallback_key="date_created", fallback_value=-1, fmt=TIMESTAMP_FMT):
    """Convert date like 'Fri Oct 11 22:02:48 EDT 2019' to timestamp"""
    try:
        dt = datetime.strptime(json.get(key, json.get(fallback_key)), fmt)
    except (ValueError, TypeError):
        return fallback_value
    else:
        return int(dt.timestamp())


def download_cold_json(identifier, kind="other", report_changes=True):
    """Request and pre-parse cold storage JSONs for datasets, file listings, file dates"""
    if kind == "glds":
        url = COLD_GLDS_MASK.format(identifier)
        with urlopen(url) as response:
            json = loads(response.read().decode())
    elif kind == "fileurls":
        accession_number_match = search(r'\d+$', identifier)
        if accession_number_match:
            accession_number = accession_number_match.group()
        else:
            raise GeneLabException(
                "Malformed accession number", accession=identifier,
            )
        url = COLD_FILEURLS_MASK.format(accession_number)
        with urlopen(url) as response:
            raw_json = loads(response.read().decode())
            try:
                json = raw_json["studies"][identifier]["study_files"]
            except KeyError:
                raise GeneLabJSONException(
                    "Malformed 'files' JSON", accession=identifier, url=url,
                )
    elif kind == "filedates":
        url = COLD_FILEDATES_MASK.format(identifier)
        with urlopen(url) as response:
            json = loads(response.read().decode())
    elif kind == "other":
        url = identifier
        with urlopen(url) as response:
            json = loads(response.read().decode())
    else:
        raise GeneLabException("Unknown JSON request", kind=kind)
    if report_changes:
        return json, True
    else:
        return json
