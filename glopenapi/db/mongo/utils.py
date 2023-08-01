from functools import partial
from bson.errors import InvalidDocument as InvalidDocumentError
from collections.abc import ValuesView
from glopenapi.common.exceptions import GLOpenAPILogger
from glopenapi.common.exceptions import GLOpenAPIDatabaseException
from glopenapi.common.exceptions import GLOpenAPIParserException
from collections import OrderedDict
from pymongo import ASCENDING


def iterate_mongo_connections(mongo_client):
    query = {"$currentOp": {"allUsers": True, "idleConnections": True}}
    projection = {"$project": {"appName": True}}
    for entry in mongo_client.admin.aggregate([query, projection]):
        connected_app_name = entry.get("appName", "")
        if connected_app_name.startswith("GLOpenAPI"):
            yield connected_app_name


isempty = lambda v: (v != v) or (v == "") or (v is None)


def is_unit_formattable(entry, unit_key):
    """Check if entry `entry` contains keys "" and unit_key and that `entry[unit_key]` is not empty"""
    return (
        ("" in entry) and (unit_key in entry) and (not isempty(entry[unit_key]))
    )


def format_units(entry, unit_key, units_formatter):
    """Replace `entry[""]` with value with formatted `entry[unit_key]`, discard `entry[unit_key]`"""
    return {
        k: units_formatter(value=v, unit=entry[unit_key]) if k == "" else v
        for k, v in entry.items() if k != unit_key
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
        GLOpenAPILogger.warning(message, action, unused_arguments)
    if error_message:
        raise GLOpenAPIDatabaseException(
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


def skip_same_file_urls_in_aggregation(cursor):
    """Iterate over `cursor`, only yield entries where `entry["file"]["urls"] is new"""
    urltuples = set()
    for entry in cursor:
        file_urls = entry.get("file", {}).get("urls", "")
        if isinstance(file_urls, (tuple, list)):
            urltuple = tuple(sorted(file_urls))
        else:
            urltuple = (file_urls,)
        if urltuple not in urltuples:
            urltuples.add(urltuple)
            yield entry


def aggregate_entries_by_context(collection, *, locale, context, id_fields=(), postprocess=(), verify_projection=True, _logd=GLOpenAPILogger.debug):
    """Run .find() or .aggregate() based on query, projection"""
    _logd(f"starting aggregate_entries_by_context() for:\n  {context.identity}")
    from glopenapi.db.mongo.index import METADATA_AUX_TEMPLATE
    full_projection = {**context.projection, **{"id."+f: 1 for f in id_fields}}
    no_user_constraints = all(k.startswith("id.") for k in full_projection)
    if no_user_constraints:
        full_projection = {}
        context.unwind.add("file") # `file` must always be unwound
    for cat, fields in METADATA_AUX_TEMPLATE.items():
        allowed_in_projection = {f"{cat}.{f}": 1 for f in fields}
        if no_user_constraints:
            full_projection.update(allowed_in_projection)
        elif verify_projection:
            cat_in_full_projection = {
                ".".join(k.split(".")[:2]) for k in full_projection
                if ((k == cat) or k.startswith(f"{cat}."))
            }
            if not (cat_in_full_projection <= set(allowed_in_projection)):
                m = "Only certain fields are queriable in category"
                raise GLOpenAPIParserException(m, category=cat, fields=fields)
    pipeline = [
        {"$sort": get_preferred_sort_order(collection, context, id_fields)},
      *({"$unwind": f"${f}"} for f in context.unwind),
        {"$match": context.query},
        {"$project": {**full_projection, "_id": False}},
        *postprocess,
    ]
    collation = {"locale": locale, "numericOrdering": True}
    cursor = collection.aggregate(
        pipeline, collation=collation, allowDiskUse=True,
    )
    _logd(f"finished aggregate_entries_by_context() for:\n  {context.identity}")
    return cursor, full_projection


def aggregate_file_descriptors_by_context(collection, *, locale, context, unique_urls=False, tech_type_locator="investigation.study assays.study assay technology type"):
    """Return DataFrame of file descriptors that match user query"""
    context.projection["file"] = True
    context.update(tech_type_locator, auto_reduce=True)
    cursor, _ = aggregate_entries_by_context(
        collection, locale=locale, context=context, verify_projection=False,
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
    return skip_same_file_urls_in_aggregation(cursor) if unique_urls else cursor


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
        raise GLOpenAPIDatabaseException(msg, descriptor=descriptor)
