from collections import OrderedDict
from glopenapi.api.parser import KEYVALUE_PARSER_DISPATCHER
from glopenapi.common.exceptions import GLOpenAPILogger
from pymongo import ASCENDING
from glopenapi.common.utils import deepcopy_keys
from glopenapi.db.mongo.utils import run_mongo_action


METADATA_AUX_TEMPLATE = {
    category: OrderedDict((f, "true") for f in parser.keywords["constrain_to"])
    for category, parser in KEYVALUE_PARSER_DISPATCHER().items()
    if getattr(parser, "keywords", {}).get("constrain_to")
}


def make_index(collection, field, subfields, collation, sorting, index_information):
    msgmask = "Generating index for metadata collection ('{}'), key '{}'"
    GLOpenAPILogger.info(msgmask.format(collection.name, field))
    def _remake_index(spec, name):
        if name in index_information:
            msg = f"Updating parameters to {collection.name} index {name}"
            GLOpenAPILogger.info(msg)
            collection.drop_index(name)
        collection.create_index(spec, name=name, collation=collation)
    # create top-level index:
    _remake_index([(field, sorting)], field)
    # create individual indices:
    for subfield in (subfields or ()):
        _remake_index(
            [(f"{field}.{subfield}", sorting)], f"{field}.{subfield}",
        )
    # create compound index for good measure:
    if subfields:
        _remake_index(
            [(f"{field}.{subfield}", sorting) for subfield in subfields],
            f"{field}_compound",
        )
    msgmask = "Index generated for metadata collection ('{}'), key '{}'"
    GLOpenAPILogger.info(msgmask.format(collection.name, field))


def ensure_indices(mongo_collections, locale):
    """Index `id.*` for sorting"""
    def _index_ok(index_information, field, collation, sorting):
        if field not in index_information:
            return False
        if sorting == "text":
            if "textIndexVersion" not in index_information[field]:
                return False
        if sorting != "text":
            existing_collation = index_information[field]["collation"]
            for k, v in collation.items():
                if existing_collation[k] != v:
                    return False
        return True
    id_collation = dict(locale=locale, numericOrdering=True, strength=2)
    fieldsets = [
        ("id", METADATA_AUX_TEMPLATE["id"].keys(), id_collation, ASCENDING),
        ("$**", None, {"locale": "simple"}, "text"),
    ]
    index_information = mongo_collections.metadata.index_information()
    for field, subfields, collation, sorting in fieldsets:
        if not _index_ok(index_information, field, collation, sorting):
            make_index(
                mongo_collections.metadata, field, subfields,
                collation, sorting, index_information,
            )


def INPLACE_update_metadata_value_lookup_keys(index, mongo_collections, final_key_blacklist=set()):
    """Populate JSON with all possible metadata keys, also for documentation section 'meta-existence'"""
    for isa_category in index:
        for subkey in index[isa_category]:
            raw_next_level_keyset = set.union(set(), *(
                set(entry[isa_category][subkey].keys()) for entry in
                mongo_collections.metadata.find(
                    {f"{isa_category}.{subkey}": {"$exists": True}},
                    {f"{isa_category}.{subkey}": True},
                )
            ))
            index[isa_category][subkey] = {
                next_level_key: True for next_level_key in
                sorted(raw_next_level_keyset - final_key_blacklist)
            }


def INPLACE_update_metadata_value_lookup_values(index, mongo_collections):
    """Generate JSON with all possible metadata values, also for documentation section 'meta-equals'"""
    for isa_category in index:
        for subkey in index[isa_category]:
            for next_level_key in index[isa_category][subkey]:
                vals = sorted(map(str, mongo_collections.metadata.distinct(
                    f"{isa_category}.{subkey}.{next_level_key}.",
                )))
                if not vals:
                    vals = sorted(map(str, mongo_collections.metadata.distinct(
                        f"{isa_category}.{subkey}.{next_level_key}",
                    )))
                index[isa_category][subkey][next_level_key] = vals


def update_metadata_value_lookup(mongo_collections, cacher_id, keys=("investigation", "study", "assay")):
    """Collect existing keys and values for lookups"""
    mf = "{}:\n  reindexing metadata lookup records ('{}')".format
    GLOpenAPILogger.info(mf(cacher_id, mongo_collections.metadata_aux.name))
    index = deepcopy_keys(METADATA_AUX_TEMPLATE, *keys)
    INPLACE_update_metadata_value_lookup_keys(index, mongo_collections)
    INPLACE_update_metadata_value_lookup_values(index, mongo_collections)
    collection = mongo_collections.metadata_aux
    with collection.database.client.start_session() as session:
        with session.start_transaction():
            for isa_category in index:
                for subkey in index[isa_category]:
                    run_mongo_action(
                        action="replace", collection=collection,
                        query={"isa_category": isa_category, "subkey": subkey},
                        data={"content": index[isa_category][subkey]},
                    )
    mf = "{}:\n  finished reindexing metadata lookup records ('{}')".format
    GLOpenAPILogger.info(mf(cacher_id, mongo_collections.metadata_aux.name))
