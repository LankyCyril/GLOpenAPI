from collections import defaultdict
from pandas import DataFrame
from genefab3.utils import UniversalSet
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


def get_collection_fields_as_dataframe(collection, targets, query={}, skip=set(), constrain_fields=UniversalSet(), store_value=True):
    """Parse collection for keys accompanying targets"""
    skip_downstream = set(skip) | {"_id"} | set(targets)
    unique_descriptors = defaultdict(lambda:defaultdict(set))
    for entry in collection.find(query):
        for key in set(entry.keys()) - skip_downstream:
            if key in constrain_fields:
                unique_descriptors[tuple(entry[t] for t in targets)][key].add(
                    entry[key] if store_value else True
                )
    if unique_descriptors:
        for outerkey, innerdict in unique_descriptors.items():
            for key, value in innerdict.items():
                if len(value) == 1:
                    unique_descriptors[outerkey][key] = value.pop()
                else:
                    unique_descriptors[outerkey][key] = "|".join(sorted(value))
        dataframe_by_metas = DataFrame(unique_descriptors).T
        dataframe_by_metas.index = dataframe_by_metas.index.rename(targets)
        return dataframe_by_metas.reset_index()
    else:
        return None
