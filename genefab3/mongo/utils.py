from collections import defaultdict
from pandas import DataFrame
from genefab3.utils import UniversalSet


def replace_doc(collection, query, **kwargs):
    """Shortcut to drop all instances and replace with updated instance"""
    collection.delete_many(query)
    collection.insert_one({**query, **kwargs})


def get_collection_keys_as_dataframe(collection, targets, skip=set(), constrain_fields=UniversalSet()):
    """Parse collection for keys accompanying targets"""
    skip_downstream = set(skip) | {"_id"} | set(targets)
    unique_descriptors = defaultdict(dict)
    for entry in collection.find():
        for key in set(entry.keys()) - skip_downstream:
            if key in constrain_fields:
                unique_descriptors[tuple(entry[t] for t in targets)][key] = True
    dataframe_by_metas = DataFrame(unique_descriptors).T
    dataframe_by_metas.index = dataframe_by_metas.index.rename(targets)
    return dataframe_by_metas.fillna(False).reset_index()
