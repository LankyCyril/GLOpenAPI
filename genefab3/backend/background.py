from threading import Thread
from genefab3.config import CACHER_THREAD_CHECK_INTERVAL
from genefab3.config import CACHER_THREAD_RECHECK_DELAY
from logging import getLogger, DEBUG
from genefab3.backend.mongo.writers.metadata import ensure_info_index
from genefab3.backend.mongo.writers.metadata import recache_metadata
from genefab3.backend.mongo.writers.metadata import update_metadata_index
from genefab3.backend.sql.writers.cache import drop_response_lru_cache
from time import sleep


class CacherThread(Thread):
    """Lives in background and keeps local metadata cache, metadata index, and response cache up to date"""
 
    def __init__(self, mongo_db, check_interval=CACHER_THREAD_CHECK_INTERVAL, recheck_delay=CACHER_THREAD_RECHECK_DELAY):
        """Prepare background thread that iteratively watches for changes to datasets"""
        self.mongo_db, self.check_interval = mongo_db, check_interval
        self.recheck_delay = recheck_delay
        self.logger = getLogger("genefab3")
        self.logger.setLevel(DEBUG)
        super().__init__()
 
    def run(self):
        """""" # TODO: docstring
        while True:
            ensure_info_index(
                mongo_db=self.mongo_db, logger=self.logger,
            )
            accessions, success = recache_metadata(
                mongo_db=self.mongo_db, logger=self.logger,
            )
            if success:
                if accessions.to_update | accessions.to_drop:
                    drop_response_lru_cache(logger=self.logger) # TODO sqlite_db
                delay = self.check_interval
            else:
                delay = self.recheck_delay
            update_metadata_index(self.mongo_db, self.logger)
            self.logger.info(f"CacherThread: Sleeping for {delay} seconds")
            sleep(delay)
