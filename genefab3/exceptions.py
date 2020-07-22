from sys import exc_info
from traceback import format_tb


class GeneLabException(Exception): pass
class GeneLabJSONException(GeneLabException): pass
class GeneLabFileException(Exception): pass
class GeneLabDataManagerException(GeneLabException): pass


def traceback_printer(e):
    # log(request, e) # TODO
    exc_type, exc_value, exc_tb = exc_info()
    error_mask = "<h2>{}: {}</h2><b>{}</b>:\n<pre>{}</pre><br><b>{}: {}</b>"
    error_message = error_mask.format(
        exc_type.__name__, str(exc_value),
        "Traceback (most recent call last)",
        "".join(format_tb(exc_tb)), exc_type.__name__, str(exc_value)
    )
    return error_message, 400


def exception_catcher(e):
    # log(request, e) # TODO
    if isinstance(e, FileNotFoundError):
        code, explanation = 404, "Not Found"
    elif isinstance(e, NotImplementedError):
        code, explanation = 501, "Not Implemented"
    elif isinstance(e, GeneLabDataManagerException):
        code, explanation = 500, "GeneLab Data Manager Internal Server Error"
    else:
        code, explanation = 400, "Bad Request"
    error_mask = "<b>HTTP error</b>: {} ({})<br><b>{}</b>: {}"
    return error_mask.format(code, explanation, type(e).__name__, str(e)), code
