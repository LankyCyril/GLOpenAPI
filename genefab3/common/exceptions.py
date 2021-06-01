from logging import getLogger, DEBUG
from sys import exc_info, stderr
from traceback import format_tb
from functools import partial
from json import dumps
from flask import Response


GeneFabLogger = getLogger("genefab3")
GeneFabLogger.setLevel(DEBUG)


class GeneFabException(Exception):
    def __init__(self, message="Error", suggestion=None, **kwargs):
        self.kwargs, self.suggestion = kwargs, suggestion
        super().__init__(message, *(
            f'{k}={repr(v)}' for k, v in kwargs.items() if k != "debug_info"
        ))
    def __str__(self):
        from genefab3.common.utils import repr_quote
        if len(self.args) == 0:
            s =  "Error"
        elif len(self.args) == 1:
            s = self.args[0]
        else:
            s = self.args[0] + ". Happened with: " + ", ".join(self.args[1:])
        return repr_quote(s)


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
    from genefab3.common.utils import repr_quote, space_quote
    exc_type, exc_value, exc_tb = exc_info()
    if isinstance(e, NotImplementedError):
        code, reason = 501, "Not Implemented"
    else:
        code = getattr(e, "code", 400)
        reason = getattr(e, "reason", "BAD REQUEST")
    kwargs = e.kwargs if isinstance(e, GeneFabException) else {}
    info = dict(
        code=code, reason=reason,
        exception_type=exc_type.__name__, exception_value=str(exc_value),
        args=[] if isinstance(e, GeneFabException) else [
            repr_quote(repr(a)) for a in getattr(e, "args", [])
        ],
        kwargs={
            space_quote(k): repr_quote(repr(kwargs[k]))
            for k in kwargs if (debug or (k != "debug_info"))
        },
    )
    if isinstance(e, GeneFabException) and getattr(e, "suggestion", None):
        info["suggestion"] = e.suggestion
    return info, format_tb(exc_tb)


def exception_catcher(e, debug=False):
    from genefab3.common.utils import json_permissive_default
    info, traceback_lines = interpret_exception(e, debug=debug)
    tb_preface = f"Traceback (most recent call last):\n"
    traceback = "".join(traceback_lines)
    print(tb_preface, traceback, repr(e), sep="", file=stderr)
    dumps_permissive = partial(dumps, default=json_permissive_default)
    if debug:
        content = dumps_permissive(info, indent=4) + "\n\n" + traceback
    else:
        content = dumps_permissive(info, indent=4)
    return Response(content, mimetype="application/json"), info["code"]
