from genefab3.common.logger import GeneFabLogger
from pymongo import ASCENDING


def ensure_info_index(mongo_collections, locale, keys=["accession", "assay", "sample name"]):
    """Index `info.*` for sorting"""
    if "info" not in mongo_collections.metadata.index_information():
        logger = GeneFabLogger()
        msg_mask = "Generating index for metadata collection ('{}'), key 'info'"
        logger.info(msg_mask.format(mongo_collections.metadata.name))
        mongo_collections.metadata.create_index(
            name="info", keys=[(f"info.{key}", ASCENDING) for key in keys],
            collation={"locale": locale, "numericOrdering": True},
        )
        msg_mask = "Index generated for metadata collection ('{}'), key 'info'"
        logger.info(msg_mask.format(mongo_collections.metadata.name))
