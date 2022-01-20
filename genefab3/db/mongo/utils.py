from functools import partial
from bson.errors import InvalidDocument as InvalidDocumentError
from collections.abc import ValuesView
from genefab3.common.exceptions import GeneFabLogger, GeneFabDatabaseException
from collections import OrderedDict
from pymongo import ASCENDING


def iterate_mongo_connections(mongo_client):
    query = {"$currentOp": {"allUsers": True, "idleConnections": True}}
    projection = {"$project": {"appName": True}}
    for e in mongo_client.admin.aggregate([query, projection]):
        connected_app_name = e.get("appName", "")
        if connected_app_name.startswith("GeneFab3"):
            yield connected_app_name


isempty = lambda v: (v != v) or (v == "") or (v is None)


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


def run_mongo_action(action, collection, *, query=None, data=None, documents=None):
    """Shortcut to replace/delete/insert all matching instances"""
    error_message, unused_arguments = None, None
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
        GeneFabLogger.warning(message, action, unused_arguments)
    if error_message:
        raise GeneFabDatabaseException(
            error_message, action=action, collection=collection,
            query=query, data=data, documents=documents,
        )


def get_preferred_sort_order(collection, context, id_fields):
    """Compose an `OrderedDict` for "$sort" stage of aggregate pipeline based on order preference in index, context, request"""
    pending_sort_by = {f"id.{f}" for f in id_fields}
    def _yield_ordered():
        index_information = collection.index_information()
        if "id_compound" in index_information:
            for id_f, *_ in index_information["id_compound"].get("key", []):
                if id_f in context.sort_by:
                    yield id_f
                    if id_f in pending_sort_by:
                        pending_sort_by.remove(id_f)
                elif id_f in pending_sort_by:
                    yield id_f
                    pending_sort_by.remove(id_f)
        yield from pending_sort_by
    return OrderedDict((id_f, ASCENDING) for id_f in _yield_ordered())


def aggregate_entries_by_context(collection, *, locale, context, id_fields=(), postprocess=(), return_full_projection=False):
    """Run .find() or .aggregate() based on query, projection"""
    full_projection = {**context.projection, **{"id."+f: 1 for f in id_fields}}
    if all(k.startswith("id.") for k in full_projection):
        full_projection = {} # there's no metadata constraints provided by user
    pipeline = [
        {"$sort": get_preferred_sort_order(collection, context, id_fields)},
      *({"$unwind": f"${f}"} for f in context.unwind),
        {"$match": context.query},
        {"$project": {**full_projection, "_id": False}},
        *postprocess,
    ]
    collation = {"locale": locale, "numericOrdering": True}
    cursor = collection.aggregate(
        pipeline, collation=collation,
        allowDiskUse=True, # note: this is for worst-case, large, scenarios
    )
    if return_full_projection:
        return cursor, full_projection
    else:
        return cursor


def aggregate_file_descriptors_by_context(collection, *, locale, context, tech_type_locator="investigation.study assays.study assay technology type"):
    """Return DataFrame of file descriptors that match user query"""
    context.projection["file"] = True
    context.update(tech_type_locator, auto_reduce=True)
    return aggregate_entries_by_context(
        collection, locale=locale, context=context,
        id_fields=["accession", "assay name", "sample name"], postprocess=[
            {"$group": {
                "_id": {
                    "accession": "$id.accession",
                    "assay name": "$id.assay name",
                    "technology type": "${tech_type_locator}",
                    "file": "$file",
                },
                "sample name": {"$push": "$id.sample name"},
            }},
            {"$addFields": {"_id.sample name": "$sample name"}},
            {"$replaceRoot": {"newRoot": "$_id"}},
        ],
    )


def match_sample_names_to_file_descriptor(collection, descriptor):
    """Retrieve all sample names associated with given filename under given accession and assay name"""
    try:
        return {
            entry["id"]["sample name"]
            for entry in collection.aggregate([
                {"$unwind": "$file"},
                {"$match": {
                    "id.accession": descriptor["accession"],
                    "id.assay name": descriptor["assay name"],
                    "file.filename": descriptor["file"]["filename"],
                }},
                {"$project": {"_id": False, "id.sample name": True}},
            ])
        }
    except (KeyError, TypeError, IndexError):
        msg = "File descriptor missing 'accession', 'assay name', or 'filename'"
        raise GeneFabDatabaseException(msg, descriptor=descriptor)
