from flask import request
from datetime import datetime
from logging import getLogger, DEBUG


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
    logger = getLogger("genefab3")
    logger.setLevel(DEBUG)
    return logger
