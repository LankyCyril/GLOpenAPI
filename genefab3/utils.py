from os import environ
from genefab3.config import TIMESTAMP_FMT, DEBUG_MARKERS
from re import sub, escape, split, search, IGNORECASE
from datetime import datetime
from copy import deepcopy
from genefab3.common.exceptions import GeneLabDatabaseException, GeneLabFileException


def is_flask_reloaded():
    """https://stackoverflow.com/a/9476701/590676"""
    return (environ.get("WERKZEUG_RUN_MAIN", None) == "true")


def is_debug():
    """Determine if app is running in debug mode"""
    return (environ.get("FLASK_ENV", None) in DEBUG_MARKERS)


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


def copy_and_drop(d, keys):
    """Deepcopy dictionary `d`, delete `d[key] for key in keys`"""
    d_copy = deepcopy(d)
    for key in keys:
        del d_copy[key]
    return d_copy


def iterate_terminal_leaves(d, step_tracker=0, max_steps=32):
    """Descend into a non-bifurcating branch and find the terminal leaf"""
    if step_tracker >= max_steps:
        raise GeneLabDatabaseException(
            "Document branch exceeds maximum depth", depth=max_steps,
        )
    else:
        if isinstance(d, dict):
            for i, branch in enumerate(d.values()):
                yield from iterate_terminal_leaves(branch, step_tracker+i)
        else:
            yield d


def iterate_terminal_leaf_filenames(d):
    """Get terminal leaf of document and iterate filenames stored in leaf"""
    for value in iterate_terminal_leaves(d):
        if isinstance(value, str):
            yield from split(r'\s*,\s*', value)


def infer_file_separator(filename):
    """Based on filename, infer whether the file is a CSV or a TSV"""
    if search(r'\.csv(\.gz)?$', filename, flags=IGNORECASE):
        return ","
    elif search(r'\.tsv(\.gz)?$', filename, flags=IGNORECASE):
        return "\t"
    else:
        raise GeneLabFileException("Unknown file format", filename=filename)
