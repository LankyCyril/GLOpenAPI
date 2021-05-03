from flask import request
from datetime import datetime
from functools import lru_cache
from logging import getLogger, DEBUG, Handler


def log_to_mongo_collection(collection, et=None, ev=None, stack=None, is_exception=False, **kwargs):
    try:
        remote_addr, full_path = request.remote_addr, request.full_path
    except RuntimeError:
        remote_addr, full_path = None, None
    document = {
        "is_exception": is_exception, "type": et, "value": ev,
        "remote_addr": remote_addr, "full_path": full_path,
        "timestamp": int(datetime.now().timestamp()),
    }
    try:
        collection.insert_one({**document, "stack": stack, **kwargs})
    except Exception as e:
        collection.insert_one({
            **document, "full_logging_failure_reason": repr(e),
            "elements_not_logged": ["stack", "kwargs"],
        })


@lru_cache(maxsize=None)
def GeneFabLogger():
    logger = getLogger("genefab3")
    logger.setLevel(DEBUG)
    return logger


class MongoDBLogger(Handler):
    def __init__(self, collection):
        self.collection = collection
        super().__init__()
    def emit(self, record):
        if self.collection:
            log_to_mongo_collection(
                self.collection, et=record.levelname, ev=record.getMessage(),
                stack=record.stack_info, is_exception=False,
            )
