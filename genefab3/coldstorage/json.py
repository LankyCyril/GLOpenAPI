from dateutil.parser import parse as dateutil_parse
from urllib.request import urlopen
from genefab3.config import COLD_GLDS_MASK
from genefab3.config import COLD_FILEURLS_MASK, COLD_FILEDATES_MASK
from json import loads
from re import search
from genefab3.common.exceptions import GeneLabException, GeneLabJSONException


def extract_file_timestamp(fd, key="date_modified", fallback_key="date_created", fallback_value=-1):
    """Convert date like 'Fri Oct 11 22:02:48 EDT 2019' to timestamp"""
    strdate = fd.get(key)
    if strdate is None:
        strdate = fd.get(fallback_key)
    if strdate is None:
        return fallback_value
        # TODO: fallback_value should be None when checking freshness
    else:
        try:
            dt = dateutil_parse(strdate) # TODO: more timezones (PDT, PST, ...)
        except ValueError:
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
