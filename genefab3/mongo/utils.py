from bson import Code
from bson.errors import InvalidDocument as InvalidDocumentError


def harmonize_query(query, lowercase=True):
    """Modify dangerous keys in nested dictionaries ('_id', keys containing '$' and '.')"""
    if isinstance(query, dict):
        harmonized = {}
        for k, v in query.items():
            harmonized_key = k.replace("$", "_").replace(".", "_")
            if lowercase:
                harmonized_key = harmonized_key.lower()
            if harmonized_key == "_id":
                harmonized_key = "__id"
            if harmonized_key not in harmonized:
                if isinstance(v, (list, dict)):
                    harmonized[harmonized_key] = harmonize_query(v)
                else:
                    harmonized[harmonized_key] = v
            else:
                raise InvalidDocumentError("Harmonized keys conflict")
        return harmonized
    elif isinstance(query, list):
        return [harmonize_query(q) for q in query]
    else:
        return query


def replace_doc(collection, query, _harmonize=True, **kwargs):
    """Shortcut to drop all instances and replace with updated instance"""
    collection.delete_many(query)
    if _harmonize:
        insert_query = harmonize_query({**query, **kwargs})
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
