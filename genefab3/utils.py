from urllib.request import urlopen
from genefab3.config import COLD_GLDS_MASK, COLD_FILEURLS_MASK
from genefab3.config import COLD_FILEDATES_MASK, TIMESTAMP_FMT
from json import loads
from re import search, sub
from genefab3.exceptions import GeneLabException, GeneLabJSONException
from datetime import datetime
from numpy import zeros
from natsort import natsorted
from functools import lru_cache


class UniversalSet(set):
    """Naive universal set"""
    def __and__(self, x): return x
    def __rand__(self, x): return x
    def __contains__(self, x): return True


def natsorted_dataframe(dataframe, by, ascending=True, sort_trailing_columns=False):
    """See: https://stackoverflow.com/a/29582718/590676"""
    if sort_trailing_columns:
        ns_df = dataframe[by + natsorted(dataframe.columns[len(by):])].copy()
    else:
        ns_df = dataframe.copy()
    for column in by:
        ns_df[column] = ns_df[column].astype("category")
        ns_df[column].cat.reorder_categories(
            natsorted(set(ns_df[column])), inplace=True, ordered=True,
        )
    return ns_df.sort_values(by=by, ascending=ascending)


def download_cold_json(identifier, kind="other"):
    """Request and pre-parse cold storage JSONs for datasets, file listings, file dates"""
    if kind == "glds":
        url = COLD_GLDS_MASK.format(identifier)
        with urlopen(url) as response:
            return loads(response.read().decode())
    elif kind == "fileurls":
        accession_number_match = search(r'\d+$', identifier)
        if accession_number_match:
            accession_number = accession_number_match.group()
        else:
            raise GeneLabException("Malformed accession number")
        url = COLD_FILEURLS_MASK.format(accession_number)
        with urlopen(url) as response:
            raw_json = loads(response.read().decode())
            try:
                return raw_json["studies"][identifier]["study_files"]
            except KeyError:
                raise GeneLabJSONException("Malformed 'files' JSON")
    elif kind == "filedates":
        url = COLD_FILEDATES_MASK.format(identifier)
        with urlopen(url) as response:
            return loads(response.read().decode())
    elif kind == "other":
        url = identifier
        with urlopen(url) as response:
            return loads(response.read().decode())
    else:
        raise GeneLabException("Unknown JSON request: kind='{}'".format(kind))


def extract_file_timestamp(fd, key="date_modified", fallback_key="date_created", fallback_value=-1, fmt=TIMESTAMP_FMT):
    """Convert date like 'Fri Oct 11 22:02:48 EDT 2019' to timestamp"""
    strdate = fd.get(key)
    if strdate is None:
        strdate = fd.get(fallback_key)
    if strdate is None:
        return fallback_value
    else:
        try:
            dt = datetime.strptime(strdate, fmt)
        except ValueError:
            return fallback_value
        else:
            return int(dt.timestamp())


@lru_cache(maxsize=None)
def force_default_name_delimiter(string):
    """Replace variable delimiters (._-) with '-' (default)"""
    return sub(r'[._-]', "-", string)


@lru_cache(maxsize=None)
def levenshtein_distance(v, w):
    """Calculate levenshtein distance between two sequences"""
    m, n = len(v), len(w)
    dp = zeros((m+1, n+1), dtype=int)
    for i in range(m+1):
        for j in range(n+1):
            if i == 0:
                dp[i, j] = j
            elif j == 0:
                dp[i, j] = i
            elif v[i-1] == w[j-1]:
                dp[i, j] = dp[i-1, j-1]
            else:
                dp[i, j] = 1 + min(dp[i, j-1], dp[i-1, j], dp[i-1, j-1])
    return dp[m, n]
