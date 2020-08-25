from bson import Code
from bson.errors import InvalidDocument as InvalidDocumentError


def make_query_safe(query):
    """Modify dangerous keys in nested dictionaries ('_id', keys containing '$' and '.')"""
    if isinstance(query, dict):
        safe_query = {}
        for k, v in query.items():
            safe_key = k.replace("$", "_").replace(".", "_")
            if safe_key == "_id":
                safe_key = "__id"
            if safe_key not in safe_query:
                if isinstance(v, (list, dict)):
                    safe_query[safe_key] = make_query_safe(v)
                else:
                    safe_query[safe_key] = v
            else:
                raise InvalidDocumentError("Safe keys conflict")
        return safe_query
    elif isinstance(query, list):
        return [make_query_safe(q) for q in query]
    else:
        return query


def replace_doc(collection, query, _make_safe=True, **kwargs):
    """Shortcut to drop all instances and replace with updated instance"""
    collection.delete_many(query)
    if _make_safe:
        insert_query = make_query_safe({**query, **kwargs})
    else:
        insert_query = {**query, **kwargs}
    collection.insert_one(insert_query)


def get_collection_fields(collection, skip=set()):
    """Parse collection for keys, except for `skip`; see: https://stackoverflow.com/a/48117846/590676"""
    reduced = collection.map_reduce(
        Code("function() {for (var key in this) {emit(key, null);}}"),
        Code("function(key, stuff) {return null;}"), "_",
    )
    return set(reduced.distinct("_id")) - {"_id"} - skip
