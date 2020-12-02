from bson.errors import InvalidDocument as InvalidDocumentError
from pandas import isnull
from functools import partial
from collections.abc import ValuesView
from genefab3.exceptions import GeneLabDatabaseException


def isempty(v):
    """Check if terminal leaf value is a null value or an empty string"""
    return isnull(v) or (v == "")


def is_unit_formattable(entry, unit_key):
    """Check if entry contains keys "" and unit_key and that entry[unit_key] is not empty"""
    return (
        ("" in entry) and (unit_key in entry) and
        (not isempty(entry[unit_key]))
    )


def format_units(entry, unit_key, units_format):
    """Replace entry[""] with value with formatted entry[unit_key], discard entry[unit_key]"""
    return {
        k: units_format.format(value=v, unit=entry[unit_key]) if k == "" else v
        for k, v in entry.items()
        if k != unit_key
    }


def harmonize_document(query, lowercase=True, units_format=None, dropna=True, depth_tracker=0, *, max_depth=32):
    """Modify dangerous keys in nested dictionaries ('_id', keys containing '$' and '.'), normalize case, format units, drop terminal NaNs"""
    unit_key = "unit" if lowercase else "Unit"
    harmonizer_function = partial(
        harmonize_document, lowercase=lowercase, units_format=units_format,
        dropna=dropna, depth_tracker=depth_tracker+1,
    )
    if depth_tracker >= max_depth:
        raise InvalidDocumentError("Document exceeds maximum depth", max_depth)
    elif isinstance(query, dict):
        harmonized = {}
        for key, branch in query.items():
            harmonized_key = key.replace("$", "_").replace(".", "_")
            if lowercase:
                harmonized_key = harmonized_key.lower()
            if harmonized_key == "_id":
                harmonized_key = "__id"
            if harmonized_key not in harmonized:
                harmonized_branch = harmonizer_function(branch)
                if harmonized_branch:
                    harmonized[harmonized_key] = harmonized_branch
            else:
                raise InvalidDocumentError("Harmonized keys conflict")
        if units_format and is_unit_formattable(harmonized, unit_key):
            return format_units(harmonized, unit_key, units_format)
        else:
            return harmonized
    elif isinstance(query, (list, ValuesView)):
        return [hq for hq in (harmonizer_function(q) for q in query) if hq]
    elif (not dropna) or (not isempty(query)):
        return query
    else:
        return {}

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
                raise GeneLabDatabaseException(ACTION_ERROR, action)
