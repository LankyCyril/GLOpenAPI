from genefab3.config import COLLECTION_NAMES
from pymongo import DESCENDING


def get_cached_file_descriptor_timestamp(mongo_db, file, cname=COLLECTION_NAMES.FILE_DESCRIPTORS):
    """Get timestamp of cached file descriptor"""
    cache_entry = getattr(mongo_db, cname).find_one(
        {"name": file.name, "url": file.url},
        {"_id": False, "timestamp": True}, sort=[("timestamp", DESCENDING)],
    )
    return (cache_entry or {}).get("timestamp", -1)
