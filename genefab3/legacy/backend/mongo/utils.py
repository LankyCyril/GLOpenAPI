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


REPLACE_ERROR = "run_mongo_transaction('replace') without a query and/or data"
DELETE_MANY_ERROR = "run_mongo_transaction('delete_many') without a query"
INSERT_MANY_ERROR = "run_mongo_transaction('insert_many') without documents"
ACTION_ERROR = "run_mongo_transaction() with an unsupported action"


def run_mongo_transaction(action, collection, query=None, data=None, documents=None):
    """Shortcut to drop all instances and replace with updated instance"""
    with collection.database.client.start_session() as session:
        with session.start_transaction():
            if action == "replace":
                if (query is not None) and (data is not None):
                    collection.delete_many(query)
                    collection.insert_one({**query, **data})
                else:
                    raise GeneLabDatabaseException(REPLACE_ERROR)
            elif action == "delete_many":
                if query is not None:
                    collection.delete_many(query)
                else:
                    raise GeneLabDatabaseException(DELETE_MANY_ERROR)
            elif action == "insert_many":
                if documents is not None:
                    collection.insert_many(documents)
                else:
                    raise GeneLabDatabaseException(INSERT_MANY_ERROR)
            else:
                raise GeneLabDatabaseException(ACTION_ERROR, action=action)
