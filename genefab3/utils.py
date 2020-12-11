from os import environ
from genefab3.config import DEBUG_MARKERS
from re import split, search, IGNORECASE
from genefab3.common.exceptions import GeneLabDatabaseException, GeneLabFileException


def is_flask_reloaded():
    """https://stackoverflow.com/a/9476701/590676"""
    return (environ.get("WERKZEUG_RUN_MAIN", None) == "true")


def is_debug():
    """Determine if app is running in debug mode"""
    return (environ.get("FLASK_ENV", None) in DEBUG_MARKERS)


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
