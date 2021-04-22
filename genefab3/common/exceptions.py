from sys import exc_info, stderr
from traceback import format_tb
from genefab3.common.logger import log_to_mongo_collection


class GeneFabException(Exception):
    def __init__(self, message="Error", **kwargs):
        args = [message]
        self.accession = kwargs.get("accession")
        self.kwargs = kwargs
        for k, v in self.kwargs.items():
            args.append(f'{k}={repr(v)}')
        super().__init__(*args)
    def __str__(self):
        if len(self.args) == 0:
            return "Error"
        elif len(self.args) == 1:
            return self.args[0]
        else:
            return self.args[0] + ". Happened with: " + ", ".join(self.args[1:])


class GeneFabConfigurationException(GeneFabException): pass
class GeneFabParserException(GeneFabException): pass
class GeneFabDatabaseException(GeneFabException): pass
class GeneFabJSONException(GeneFabException): pass
class GeneFabISAException(GeneFabException): pass
class GeneFabFileException(GeneFabException): pass
class GeneFabDataManagerException(GeneFabException): pass
class GeneFabFormatException(GeneFabException): pass


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


def traceback_printer(e, collection, print_to_stderr=False):
    exc_type, exc_value, _, info = interpret_exc_info(exc_info())
    if collection:
        log_to_mongo_collection(
            collection, *info, is_exception=True, args=getattr(e, "args", []),
        )
    if print_to_stderr:
        print("Exception handled", *info, sep=". ", end="", file=stderr)
    error_message = HTTP_DEBUG_ERROR_MASK.format(
        *info, exc_type.__name__, str(exc_value),
    )
    return error_message, 400


def exception_catcher(e, collection, print_to_stderr=False):
    if isinstance(e, FileNotFoundError):
        code, explanation = 404, "Not Found"
    elif isinstance(e, NotImplementedError):
        code, explanation = 501, "Not Implemented"
    elif isinstance(e, GeneFabDataManagerException):
        code, explanation = 500, "Data Manager Internal Server Error"
    elif isinstance(e, GeneFabFileException):
        code, explanation = 500, "Unresolvable Data Request"
    elif isinstance(e, GeneFabDatabaseException):
        code, explanation = 500, "GeneFab3 Database Error"
    else:
        code, explanation = 400, "Bad Request"
    *_, info = interpret_exc_info(exc_info())
    if collection:
        log_to_mongo_collection(
            collection, *info, is_exception=True,
            args=getattr(e, "args", []), code=code,
        )
    if print_to_stderr:
        print("Exception handled", *info, sep=". ", end="", file=stderr)
    error_message = HTTP_ERROR_MASK.format(
        code, explanation, type(e).__name__, (
            (HTML_LIST_SEP.join(e.args) if hasattr(e, "args") else str(e))
            or type(e).__name__
        ),
    )
    return error_message, code
