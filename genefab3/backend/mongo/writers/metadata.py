from genefab3.common.exceptions import GeneLabDatabaseException
from genefab3.config import COLLECTION_NAMES, MONGO_DB_LOCALE
from pymongo import ASCENDING
from types import SimpleNamespace
from genefab3.backend.mongo.writers.json import list_available_accessions
from genefab3.backend.mongo.writers.json import list_fresh_and_stale_accessions
from genefab3.backend.mongo.dataset import CachedDataset
from genefab3.config import UNITS_FORMAT
from copy import deepcopy


INDEX_TEMPLATE = {
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

REPLACE_ERROR = "run_mongo_transaction('replace') without a query and/or data"
DELETE_MANY_ERROR = "run_mongo_transaction('delete_many') without a query"
INSERT_MANY_ERROR = "run_mongo_transaction('insert_many') without documents"
ACTION_ERROR = "run_mongo_transaction() with an unsupported action"


def run_mongo_transaction(action, collection, query=None, data=None, documents=None):
    """Shortcut to drop all instances and replace with updated instance"""
    with collection.database.client.start_session() as session:
        with session.start_transaction():
            if action == "replace":
                if (query is not None) and (data is not None):
                    collection.delete_many(query)
                    collection.insert_one({**query, **data})
                else:
                    raise GeneLabDatabaseException(REPLACE_ERROR)
            elif action == "delete_many":
                if query is not None:
                    collection.delete_many(query)
                else:
                    raise GeneLabDatabaseException(DELETE_MANY_ERROR)
            elif action == "insert_many":
                if documents is not None:
                    collection.insert_many(documents)
                else:
                    raise GeneLabDatabaseException(INSERT_MANY_ERROR)
            else:
                raise GeneLabDatabaseException(ACTION_ERROR, action=action)


def ensure_info_index(mongo_db, logger, category="info", keys=["accession", "assay", "sample name"], cname=COLLECTION_NAMES.METADATA):
    """Index `info.*` for sorting"""
    if category not in getattr(mongo_db, cname).index_information():
        logger.info(
            f"Creating index for collection '{cname}', key(s) '{category}'",
        )
        getattr(mongo_db, cname).create_index(
            name=category,
            keys=[(f"{category}.{key}", ASCENDING) for key in keys],
            collation={"locale": MONGO_DB_LOCALE, "numericOrdering": True},
        )


def recache_metadata(mongo_db, logger):
    """Instantiate each available dataset; if contents changed, dataset automatically updates db.metadata"""
    accessions = SimpleNamespace()
    logger.info("CacherThread/metadata: Checking cache")
    try:
        accessions.available = list_available_accessions(mongo_db)
        accessions.fresh, accessions.stale = list_fresh_and_stale_accessions(
            mongo_db,
        )
    except Exception as e:
        logger.error(f"CacherThread/metadata: {repr(e)}")
        return None, False
    else:
        accessions.to_update, accessions.to_drop = (
            accessions.available - accessions.fresh,
            (accessions.fresh | accessions.stale) - accessions.available,
        )
        for accession in accessions.to_update:
            try:
                glds = CachedDataset(
                    mongo_db, accession, logger, init_assays=True,
                    units_format=UNITS_FORMAT,
                )
            except Exception as e:
                logger.error(f"CacherThread/metadata/{accession}: {repr(e)}")
            else:
                chg = "changed" if any(glds.changed) else "up to date"
                logger.info(f"CacherThread/metadata/{accession}: {chg}")
        for accession in accessions.to_drop:
            CachedDataset.drop_cache(
                mongo_db=mongo_db, accession=accession,
            )
        logger.info(
            "CacherThread/metadata, datasets: {}={}, {}={}, {}={}".format(
                "available", len(accessions.available),
                "fresh", len(accessions.fresh),
                "updated", len(accessions.stale),
            ),
        )
        return accessions, True


def INPLACE_update_metadata_index_keys(mongo_db, index, final_key_blacklist=FINAL_INDEX_KEY_BLACKLIST, cname=COLLECTION_NAMES.METADATA):
    """Populate JSON with all possible metadata keys, also for documentation section 'meta-existence'"""
    for isa_category in index:
        for subkey in index[isa_category]:
            raw_next_level_keyset = set.union(*(
                set(entry[isa_category][subkey].keys()) for entry in
                getattr(mongo_db, cname).find(
                    {isa_category+"."+subkey: {"$exists": True}},
                    {isa_category+"."+subkey: True},
                )
            ))
            index[isa_category][subkey] = {
                next_level_key: True for next_level_key in
                sorted(raw_next_level_keyset - final_key_blacklist)
            }


def INPLACE_update_metadata_index_values(mongo_db, index, cname=COLLECTION_NAMES.METADATA):
    """Generate JSON with all possible metadata values, also for documentation section 'meta-equals'"""
    metadata_collection = getattr(mongo_db, cname)
    for isa_category in index:
        for subkey in index[isa_category]:
            for next_level_key in index[isa_category][subkey]:
                values = sorted(map(str, metadata_collection.distinct(
                    f"{isa_category}.{subkey}.{next_level_key}.",
                )))
                if not values:
                    values = sorted(map(str, metadata_collection.distinct(
                        f"{isa_category}.{subkey}.{next_level_key}",
                    )))
                index[isa_category][subkey][next_level_key] = values


def update_metadata_index(mongo_db, logger, template=INDEX_TEMPLATE, cname=COLLECTION_NAMES.METADATA_INDEX):
    """Collect existing keys and values for lookups"""
    logger.info("CacherThread: reindexing metadata")
    index = deepcopy(template)
    INPLACE_update_metadata_index_keys(mongo_db, index)
    INPLACE_update_metadata_index_values(mongo_db, index)
    for isa_category in index:
        for subkey in index[isa_category]:
            run_mongo_transaction(
                action="replace",
                collection=getattr(mongo_db, cname),
                query={"isa_category": isa_category, "subkey": subkey},
                data={"content": index[isa_category][subkey]},
            )
