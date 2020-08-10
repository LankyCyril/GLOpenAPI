from bson import Code


def replace_doc(collection, query, **kwargs):
    """Shortcut to drop all instances and replace with updated instance"""
    collection.delete_many(query)
    collection.insert_one({**query, **kwargs})


def get_collection_fields(collection, skip=set()):
    """Parse collection for keys, except for `skip`; see: https://stackoverflow.com/a/48117846/590676"""
    reduced = collection.map_reduce(
        Code("function() {for (var key in this) {emit(key, null);}}"),
        Code("function(key, stuff) {return null;}"), "_",
    )
    return set(reduced.distinct("_id")) - {"_id"} - skip
