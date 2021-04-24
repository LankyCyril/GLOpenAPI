from genefab3.common.logger import GeneFabLogger
from pymongo import ASCENDING
from copy import deepcopy
from genefab3.db.mongo.utils import run_mongo_action


INFO_SUBKEYS = ["accession", "assay", "sample name"]

METADATA_AUX_TEMPLATE = {
# TODO: infer this template from keywords of partials in genefab3.api.parser
    "investigation": {
        "study": "true",
        "study assays": "true",
        "investigation": "true",
    },
    "study": {
        "characteristics": "true",
        "factor value": "true",
        "parameter value": "true",
    },
    "assay": {
        "characteristics": "true",
        "factor value": "true",
        "parameter value": "true",
    },
}

FINAL_INDEX_KEY_BLACKLIST = {"comment"}


def ensure_info_index(mongo_collections, locale, subkeys=INFO_SUBKEYS):
    """Index `id.*` for sorting"""
    if "id" not in mongo_collections.metadata.index_information():
        logger = GeneFabLogger()
        msgmask = "Generating index for metadata collection ('{}'), key 'id'"
        logger.info(msgmask.format(mongo_collections.metadata.name))
        mongo_collections.metadata.create_index(
            name="id", keys=[(f"id.{key}", ASCENDING) for key in subkeys],
            collation={"locale": locale, "numericOrdering": True},
        )
        msgmask = "Index generated for metadata collection ('{}'), key 'id'"
        logger.info(msgmask.format(mongo_collections.metadata.name))


def INPLACE_update_metadata_value_lookup_keys(index, mongo_collections, final_key_blacklist=FINAL_INDEX_KEY_BLACKLIST):
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


def update_metadata_value_lookup(mongo_collections, cacher_id, template=METADATA_AUX_TEMPLATE):
    """Collect existing keys and values for lookups"""
    logger = GeneFabLogger()
    msgmask = "{}: reindexing metadata lookup records ('{}')"
    logger.info(msgmask.format(cacher_id, mongo_collections.metadata_aux.name))
    index = deepcopy(template)
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
    msgmask = "{}: finished reindexing metadata lookup records ('{}')"
    logger.info(msgmask.format(cacher_id, mongo_collections.metadata_aux.name))
