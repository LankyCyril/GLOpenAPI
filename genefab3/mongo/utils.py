from bson import Code
from bson.errors import InvalidDocument as InvalidDocumentError


def make_query_safe(query):
    """Modify dangerous keys in nested dictionaries ('_id', keys containing '$' and '.')"""
    safe_query = {}
    for k, v in query.items():
        safe_key = k.replace("$", "_").replace(".", "_")
        if safe_key == "_id":
            safe_key = "__id"
        if safe_key not in safe_query:
            if isinstance(v, dict):
                safe_query[safe_key] = make_query_safe(v)
            else:
                safe_query[safe_key] = v
        else:
            raise InvalidDocumentError("Safe keys conflict")
    return safe_query


def insert_one_safe(collection, query):
    """Insert nested key-value pairs, modifying dangerous keys ('_id', keys containing '$' and '.')"""
    collection.insert_one(make_query_safe(query))


def replace_doc(collection, query, **kwargs):
    """Shortcut to drop all instances and replace with updated instance"""
    collection.delete_many(query)
    insert_one_safe(collection, {**query, **kwargs})


def get_collection_fields(collection, skip=set()):
    """Parse collection for keys, except for `skip`; see: https://stackoverflow.com/a/48117846/590676"""
    reduced = collection.map_reduce(
        Code("function() {for (var key in this) {emit(key, null);}}"),
        Code("function(key, stuff) {return null;}"), "_",
    )
    return set(reduced.distinct("_id")) - {"_id"} - skip
