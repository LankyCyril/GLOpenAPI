from genefab3.common.utils import iterate_terminal_leaves
from genefab3.common.exceptions import GeneLabDatabaseException
from genefab3.common.exceptions import GeneLabFileException
from re import split, search, IGNORECASE


def iterate_terminal_leaf_filenames(d):
    """Get terminal leaf of document and iterate filenames stored in leaf"""
    try:
        for value in iterate_terminal_leaves(d):
            if isinstance(value, str):
                yield from split(r'\s*,\s*', value)
    except ValueError as e:
        raise GeneLabDatabaseException(
            "Document branch exceeds nestedness threshold",
            max_steps=e.args[1],
        )


def infer_file_separator(filename):
    """Based on filename, infer whether the file is a CSV or a TSV"""
    if search(r'\.csv(\.gz)?$', filename, flags=IGNORECASE):
        return ","
    elif search(r'\.tsv(\.gz)?$', filename, flags=IGNORECASE):
        return "\t"
    else:
        raise GeneLabFileException("Unknown file format", filename=filename)
