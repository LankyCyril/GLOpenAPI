from pandas import isnull
from re import sub, search
from functools import partial
from bson.errors import InvalidDocument as InvalidDocumentError
from collections.abc import ValuesView
from genefab3.common.logger import GeneFabLogger
from genefab3.common.exceptions import GeneFabDatabaseException
from pymongo import ASCENDING


def isempty(v):
    """Check if terminal leaf value is a null value or an empty string"""
    return isnull(v) or (v == "")


def is_safe_token(v, allow_regex=False):
    """Check if value is safe for PyMongo queries"""
    return "$" not in (sub(r'\$\/$', "", v) if allow_regex else v)


def is_regex(v):
    """Check if value is a regex"""
    return search(r'^\/.*\/$', v)


def is_unit_formattable(e, unit_key):
    """Check if entry `e` contains keys "" and unit_key and that `e[unit_key]` is not empty"""
    return ("" in e) and (unit_key in e) and (not isempty(e[unit_key]))


def format_units(e, unit_key, units_formatter):
    """Replace `e[""]` with value with formatted `e[unit_key]`, discard `e[unit_key]`"""
    return {
        k: units_formatter(value=v, unit=e[unit_key]) if k == "" else v
        for k, v in e.items() if k != unit_key
    }


def harmonize_document(query, units_formatter=None, lowercase=True, dropna=True, depth_tracker=0, *, max_depth=32):
    """Modify dangerous keys in nested dictionaries ('_id', keys containing '$' and '.'), normalize case, format units, drop terminal NaNs"""
    unit_key = "unit" if lowercase else "Unit"
    harmonizer_function = partial(
        harmonize_document, lowercase=lowercase, dropna=dropna,
        units_formatter=units_formatter, depth_tracker=depth_tracker+1,
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
        if units_formatter and is_unit_formattable(harmonized, unit_key):
            return format_units(harmonized, unit_key, units_formatter)
        else:
            return harmonized
    elif isinstance(query, (list, ValuesView)):
        return [hq for hq in (harmonizer_function(q) for q in query) if hq]
    elif (not dropna) or (not isempty(query)):
        return query
    else:
        return {}


def run_mongo_transaction(action, collection, *, query=None, data=None, documents=None):
    """Shortcut to replace/delete/insert all matching instances in one transaction"""
    error_message, unused_arguments = None, None
    with collection.database.client.start_session() as session:
        with session.start_transaction():
            if action == "replace":
                if (query is not None) and (data is not None):
                    collection.delete_many(query)
                    collection.insert_one({**query, **data})
                    if documents is not None:
                        unused_arguments = "`documents`"
                else:
                    error_message = "no `query` and/or `data` specified"
            elif action == "delete_many":
                if query is not None:
                    collection.delete_many(query)
                    if (data is not None) or (documents is not None):
                        unused_arguments = "`data`, `documents`"
                else:
                    error_message = "no `query` specified"
            elif action == "insert_many":
                if documents is not None:
                    collection.insert_many(documents)
                    if (query is not None) or (data is not None):
                        unused_arguments = "`query`, `data`"
                else:
                    error_message = "no `documents` specified"
            else:
                error_message = "unsupported action"
    if unused_arguments:
        message = "run_mongo_transaction('%s'): %s unused in this action"
        GeneFabLogger().warning(message, action, unused_arguments)
    if error_message:
        raise GeneFabDatabaseException(
            error_message, action=action, collection=collection,
            query=query, data=data, documents=documents,
        )


def retrieve_by_context(collection, *, locale, context, id_fields=(), postprocess=()):
    """Run .find() or .aggregate() based on query, projection"""
    full_projection = {**context.projection, **{"id."+f: 1 for f in id_fields}}
    sort_by_too = ["id."+f for f in id_fields if "id."+f not in context.sort_by]
    pipeline=[
        {"$sort": {f: ASCENDING for f in (*context.sort_by, *sort_by_too)}},
      *({"$unwind": f"${f}"} for f in context.unwind),
        {"$match": context.query},
        {"$project": {**full_projection, "_id": False}},
        *postprocess,
    ]
    collation={"locale": locale, "numericOrdering": True}
    return collection.aggregate(pipeline, collation=collation), full_projection
