from os import environ
from genefab3.config import TIMESTAMP_FMT, DEBUG_MARKERS
from re import sub, escape
from datetime import datetime
from natsort import natsorted
from functools import lru_cache
from copy import deepcopy


def is_debug():
    """Determine if app is running in debug mode"""
    return (environ.get("FLASK_ENV", None) in DEBUG_MARKERS)


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


def map_replace(string, mappings):
    """Perform multiple replacements in one go"""
    return sub(
        r'|'.join(map(escape, mappings.keys())),
        lambda m: mappings[m.group()],
        string,
    )


class UniversalSet(set):
    """Naive universal set"""
    def __and__(self, x): return x
    def __iand__(self, x): return x
    def __rand__(self, x): return x
    def __or__(self, x): return self
    def __ior__(self, x): return self
    def __ror__(self, x): return self
    def __contains__(self, x): return True


def copy_and_update(d, key, E):
    """Deepcopy dictionary `d`, update `d[key]` with data from `E`"""
    d_copy = deepcopy(d)
    d_copy[key].update(E)
    return d_copy


def copy_and_drop(d, keys):
    """Deepcopy dictionary `d`, delete `d[key] for key in keys`"""
    d_copy = deepcopy(d)
    for key in keys:
        del d_copy[key]
    return d_copy
