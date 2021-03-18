from flask import request
from datetime import datetime
from logging import getLogger, Handler


def log_to_mongo_collection(collection, et=None, ev=None, stack=None, is_exception=False, **kwargs):
    try:
        remote_addr, full_path = request.remote_addr, request.full_path
    except RuntimeError:
        remote_addr, full_path = None, None
    collection.insert_one({
        "is_exception": is_exception, "type": et, "value": ev, "stack": stack,
        "remote_addr": remote_addr, "full_path": full_path,
        "timestamp": int(datetime.now().timestamp()), **kwargs,
    })


def GeneFabLogger():
    return getLogger("genefab3")


class MongoDBLogger(Handler):
    def __init__(self, collection):
        self.collection = collection
        super().__init__()
    def emit(self, record):
        if self.collection:
            log_to_mongo_collection(
                self.collection,
                et=record.levelname, ev=record.getMessage(),
                stack=record.stack_info, is_exception=False,
            )
