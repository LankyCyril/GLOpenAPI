from flask import request
from datetime import datetime
from sys import exc_info
from traceback import format_tb
from logging import Handler


class GeneLabException(Exception): pass
class GeneLabParserException(GeneLabException): pass
class GeneLabMetadataException(GeneLabException): pass
class GeneLabDatabaseException(GeneLabException): pass
class GeneLabJSONException(GeneLabException): pass
class GeneLabISAException(GeneLabException): pass
class GeneLabFileException(GeneLabException): pass
class GeneLabDataManagerException(GeneLabException): pass


def interpret_exc_info(ei):
    exc_type, exc_value, exc_tb = ei
    info = [
        exc_type.__name__, str(exc_value),
        "Traceback (most recent call last): \n" + "".join(format_tb(exc_tb)),
    ]
    return exc_type, exc_value, exc_tb, info


def insert_log_entry(log_collection, et=None, ev=None, stack=None, is_exception=False, **kwargs):
    try:
        remote_addr, full_path = request.remote_addr, request.full_path
    except RuntimeError:
        remote_addr, full_path = None, None
    log_collection.insert_one({
        "is_exception": is_exception, "type": et, "value": ev, "stack": stack,
        "remote_addr": remote_addr, "full_path": full_path,
        "timestamp": int(datetime.now().timestamp()), **kwargs,
    })


def traceback_printer(e, db):
    exc_type, exc_value, exc_tb, info = interpret_exc_info(exc_info())
    insert_log_entry(db.log, *info, is_exception=True)
    error_mask = "<h2>{}: {}</h2><pre>{}</pre><br><b>{}: {}</b>"
    error_message = error_mask.format(*info, exc_type.__name__, str(exc_value))
    return error_message, 400


def exception_catcher(e, db):
    if isinstance(e, FileNotFoundError):
        code, explanation = 404, "Not Found"
    elif isinstance(e, NotImplementedError):
        code, explanation = 501, "Not Implemented"
    elif isinstance(e, GeneLabDataManagerException):
        code, explanation = 500, "GeneLab Data Manager Internal Server Error"
    elif isinstance(e, GeneLabDatabaseException):
        code, explanation = 500, "GeneLab Database Error"
    else:
        code, explanation = 400, "Bad Request"
    *_, info = interpret_exc_info(exc_info())
    insert_log_entry(db.log, *info, is_exception=True, code=code)
    error_mask = "<b>HTTP error</b>: {} ({})<br><b>{}</b>: {}"
    return error_mask.format(code, explanation, type(e).__name__, str(e)), code


class DBLogger(Handler):
    def __init__(self, db):
        self.db = db
        super().__init__()
    def emit(self, record):
        insert_log_entry(
            self.db.log, et=record.levelname, ev=record.getMessage(),
            stack=record.stack_info, is_exception=False,
        )
