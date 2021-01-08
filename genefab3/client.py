from genefab3.common.exceptions import GeneLabConfigurationException


class GeneFabClient():
    """Controls caching of metadata, data, and responses"""
 
    def __init__(
        self, *,
        locale, mongo_db,
        sqlite_blobs, sqlite_tables, sqlite_cache,
        get_accessions, get_files, get_isa, target_file_locators,
        cacher_start_condition=lambda: True,
        cacher_interval=1800,
        cacher_recheck_delay=300,
    ):
        """Initialize metadata and response cachers, extend Dataset class with methods targeting given storage"""
        self.locale, self.mongo_db, self.get_accessions = (
            locale, mongo_db, get_accessions,
        )
        if len(set(sqlite_blobs, sqlite_tables, sqlite_cache)) != 3:
            raise GeneLabConfigurationException(
                "SQLite databases must all be distinct to avoid name conflicts",
                sqlite_blobs=sqlite_blobs, sqlite_tables=sqlite_tables,
                sqlite_cache=sqlite_cache,
            )
        else:
            self.sqlite_blobs, self.sqlite_tables, self.sqlite_cache = (
                sqlite_blobs, sqlite_tables, sqlite_cache,
            )
