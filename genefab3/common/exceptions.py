from sys import exc_info, stderr
from traceback import format_tb
from genefab3.common.logger import log_to_mongo_collection
from json import dumps
from functools import partial
from flask import Response


class GeneFabException(Exception):
    def __init__(self, message="Error", accession=None, suggestion=None, **kwargs):
        self.accession, self.suggestion = accession, suggestion
        self.kwargs = kwargs
        args = [message, *(
            f'{k}={repr(v)}' for k, v in kwargs.items() if k != "_debug"
        )]
        super().__init__(*args)
    def __str__(self):
        if len(self.args) == 0:
            return "Error"
        elif len(self.args) == 1:
            return self.args[0]
        else:
            return self.args[0] + ". Happened with: " + ", ".join(self.args[1:])


class GeneFabConfigurationException(GeneFabException):
    code, reason = 500, "GeneFab3 Configuration Error"
class GeneFabDatabaseException(GeneFabException):
    code, reason = 500, "GeneFab3 Database Error"
class GeneFabDataManagerException(GeneFabException):
    code, reason = 500, "Data Manager Internal Server Error"
class GeneFabFileException(GeneFabException):
    code, reason = 500, "Unresolvable Data Request"
class GeneFabFormatException(GeneFabException):
    code, reason = 400, "BAD REQUEST"
class GeneFabISAException(GeneFabException):
    code, reason = 500, "ISA Parser Error"
class GeneFabParserException(GeneFabException):
    code, reason = 400, "BAD REQUEST"


def interpret_exception(e, debug=False):
    exc_type, exc_value, exc_tb = exc_info()
    if isinstance(e, NotImplementedError):
        code, reason = 501, "Not Implemented"
    else:
        code = getattr(e, "code", 400)
        reason = getattr(e, "reason", "BAD REQUEST")
    info = dict(
        code=code, reason=reason,
        exception_type=exc_type.__name__, exception_value=str(exc_value),
        args=[] if isinstance(e, GeneFabException) else getattr(e, "args", []),
        kwargs={
            k: v for k, v in getattr(e, "kwargs", {}).items()
            if (debug or (k != "_debug"))
        },
    )
    if hasattr(e, "accession") and e.accession:
        info["accession"] = e.accession
    if hasattr(e, "suggestion") and e.suggestion:
        info["suggestion"] = e.suggestion
    return info, format_tb(exc_tb)


def exception_catcher(e, collection, debug=False):
    info, traceback_lines = interpret_exception(e, debug=debug)
    if collection:
        log_to_mongo_collection(
            collection, info["exception_type"], info["exception_value"],
            stack=traceback_lines, is_exception=True,
            args=getattr(e, "args", []),
        )
    from genefab3.common.utils import json_permissive_default
    dumps_permissive = partial(dumps, default=json_permissive_default)
    if debug:
        tb_preface = f"Traceback (most recent call last):\n"
        traceback = "".join(traceback_lines)
        print(tb_preface, traceback, repr(e), sep="", file=stderr)
        content = dumps_permissive(info, indent=4) + "\n\n" + traceback
    else:
        content = dumps_permissive(info, indent=4)
    return Response(content, mimetype="application/json"), info["code"]
