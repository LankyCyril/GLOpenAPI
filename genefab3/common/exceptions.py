from genefab3.config import COLLECTION_NAMES
from flask import request
from datetime import datetime
from sys import exc_info
from traceback import format_tb
from logging import Handler


class GeneLabException(Exception):
    def __init__(self, message="Error", accession_or_object=None, explicit_assay_name=None, **kwargs):
        from genefab3.common.types_legacy import DatasetBaseClass, AssayBaseClass
        args = [message]
        if isinstance(accession_or_object, DatasetBaseClass):
            accession, assay_name = accession_or_object.accession, None
        elif isinstance(accession_or_object, AssayBaseClass):
            accession = accession_or_object.dataset.accession
            assay_name = accession_or_object.name
        elif accession_or_object is None:
            accession, assay_name = None, None
        else:
            accession, assay_name = str(accession_or_object), None
        if explicit_assay_name is not None:
            assay_name = explicit_assay_name
        if accession is not None:
            args.append(f'accession="{accession}"')
        if assay_name is not None:
            args.append(f'assay.name="{assay_name}"')
        for k, v in kwargs.items():
            args.append(f'{k}="{v}"')
        super().__init__(*args)
    def __str__(self):
        if len(self.args) == 0:
            return "Error"
        elif len(self.args) == 1:
            return self.args[0]
        else:
            return self.args[0] + ". Happened with: " + ", ".join(self.args[1:])


class GeneLabParserException(GeneLabException): pass
class GeneLabMetadataException(GeneLabException): pass
class GeneLabDatabaseException(GeneLabException): pass
class GeneLabJSONException(GeneLabException): pass
class GeneLabISAException(GeneLabException): pass
class GeneLabFileException(GeneLabException): pass
class GeneLabDataManagerException(GeneLabException): pass
class GeneLabFormatException(GeneLabException): pass


HTTP_ERROR_MASK = """<html>
    <head>
        <style>
            * {{font-size: 12pt; font-family: monospace}}
        </style>
    </head>
    <body>
        <b>HTTP error</b>: <mark>{} ({})</mark><br><br><b>{}</b>: {}
    </body>
</html>"""
HTML_LIST_SEP = "<br>&middot;&nbsp;"
HTTP_DEBUG_ERROR_MASK = "<h2>{}: {}</h2><pre>{}</pre><br><b>{}: {}</b>"


def interpret_exc_info(ei):
    exc_type, exc_value, exc_tb = ei
    info = [
        exc_type.__name__, str(exc_value),
        "Traceback (most recent call last): \n" + "".join(format_tb(exc_tb)),
    ]
    return exc_type, exc_value, exc_tb, info


def insert_log_entry(mongo_db, et=None, ev=None, stack=None, is_exception=False, cname=COLLECTION_NAMES.LOG, **kwargs):
    try:
        remote_addr, full_path = request.remote_addr, request.full_path
    except RuntimeError:
        remote_addr, full_path = None, None
    getattr(mongo_db, cname).insert_one({
        "is_exception": is_exception, "type": et, "value": ev, "stack": stack,
        "remote_addr": remote_addr, "full_path": full_path,
        "timestamp": int(datetime.now().timestamp()), **kwargs,
    })


def traceback_printer(e, mongo_db):
    exc_type, exc_value, exc_tb, info = interpret_exc_info(exc_info())
    insert_log_entry(
        mongo_db, *info, is_exception=True,
        args=getattr(e, "args", []),
    )
    error_message = HTTP_DEBUG_ERROR_MASK.format(
        *info, exc_type.__name__, str(exc_value),
    )
    return error_message, 400


def exception_catcher(e, mongo_db):
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
    insert_log_entry(
        mongo_db, *info, is_exception=True,
        args=getattr(e, "args", []), code=code,
    )
    error_message = HTTP_ERROR_MASK.format(
        code, explanation, type(e).__name__, (
            (HTML_LIST_SEP.join(e.args) if hasattr(e, "args") else str(e))
            or type(e).__name__
        ),
    )
    return error_message, code


class DBLogger(Handler):
    def __init__(self, mongo_db):
        self.mongo_db = mongo_db
        super().__init__()
    def emit(self, record):
        insert_log_entry(
            self.mongo_db, et=record.levelname, ev=record.getMessage(),
            stack=record.stack_info, is_exception=False,
        )
