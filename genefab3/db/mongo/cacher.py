from threading import Thread
from genefab3.common.logger import GeneFabLogger
from time import sleep


class CacherThread(Thread):
    """Lives in background and keeps local metadata cache, metadata index, and response cache up to date"""
 
    def __init__(self, *, adapter, mongo_db, response_cache, metadata_update_interval, metadata_retry_delay):
        """Prepare background thread that iteratively watches for changes to datasets"""
        self.adapter = adapter
        self.mongo_db, self.response_cache = mongo_db, response_cache
        self.metadata_update_interval = metadata_update_interval
        self.metadata_retry_delay = metadata_retry_delay
        super().__init__()
 
    def run(self):
        """Continuously run MongoDB and SQLite3 cachers"""
        logger = GeneFabLogger()
        while True:
            # ensure_info_index TODO
            success = True # recache_metadata TODO
            if success:
                # update_metadata_value_lookup TODO
                # drop_cached_responses TODO
                # shrink_response_cache TODO
                delay = self.metadata_update_interval
            else:
                delay = self.metadata_retry_delay
            logger.info(f"CacherThread: Sleeping for {delay} seconds")
            sleep(delay)
