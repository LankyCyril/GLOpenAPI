from collections import OrderedDict
from genefab3.api.parser import KEYVALUE_PARSER_DISPATCHER
from genefab3.common.exceptions import GeneFabLogger
from pymongo import ASCENDING
from genefab3.common.utils import deepcopy_keys
from genefab3.db.mongo.utils import run_mongo_action


METADATA_AUX_TEMPLATE = {
    category: OrderedDict((f, "true") for f in parser.keywords["constrain_to"])
    for category, parser in KEYVALUE_PARSER_DISPATCHER().items()
    if getattr(parser, "keywords", {}).get("constrain_to")
}


def ensure_info_index(mongo_collections, locale):
    """Index `id.*` for sorting"""
    if "id" not in mongo_collections.metadata.index_information():
        msgmask = "Generating index for metadata collection ('{}'), key 'id'"
        id_fields = METADATA_AUX_TEMPLATE["id"].keys()
        GeneFabLogger.info(msgmask.format(mongo_collections.metadata.name))
        # create individual indices:
        mongo_collections.metadata.create_index(
            [("id", ASCENDING)], name="id",
            collation={"locale": locale, "numericOrdering": True},
        )
        for f in id_fields:
            mongo_collections.metadata.create_index(
                [(f"id.{f}", ASCENDING)], name=f"id.{f}",
                collation={"locale": locale, "numericOrdering": True},
            )
        # create compound index for good measure:
        mongo_collections.metadata.create_index(
            [(f"id.{f}", ASCENDING) for f in id_fields], name="id_compound",
            collation={"locale": locale, "numericOrdering": True},
        )
        msgmask = "Index generated for metadata collection ('{}'), key 'id'"
        GeneFabLogger.info(msgmask.format(mongo_collections.metadata.name))


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
    m = "{}:\n  reindexing metadata lookup records ('{}')"
    GeneFabLogger.info(m.format(cacher_id, mongo_collections.metadata_aux.name))
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
    m = "{}:\n  finished reindexing metadata lookup records ('{}')"
    GeneFabLogger.info(m.format(cacher_id, mongo_collections.metadata_aux.name))
