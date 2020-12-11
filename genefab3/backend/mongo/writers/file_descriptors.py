from genefab3.config import COLLECTION_NAMES
from genefab3.backend.mongo.writers.metadata import run_mongo_transaction


def set_cached_file_descriptor_timestamp(mongo_db, file, cname=COLLECTION_NAMES.FILE_DESCRIPTORS):
    """Set timestamp of cached file descriptor"""
    run_mongo_transaction(
        action="replace", collection=getattr(mongo_db, cname),
        query={"name": file.name, "url": file.url},
        data={"timestamp": file.timestamp},
    )


def drop_cached_file_descriptor_timestamp(mongo_db, file, cname=COLLECTION_NAMES.FILE_DESCRIPTORS):
    """Erase Mongo DB entry for file descriptor associated with a CachedTable"""
    run_mongo_transaction(
        action="delete_many", collection=getattr(mongo_db, cname),
        query={"name": file.name, "url": file.url},
    )
