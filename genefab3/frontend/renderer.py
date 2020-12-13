from genefab3.common.exceptions import GeneLabException, GeneLabFormatException
from genefab3.frontend.renderers.raw import render_raw
from genefab3.frontend.renderers.cls import render_cls
from genefab3.frontend.renderers.gct import render_gct
from pandas import DataFrame, MultiIndex
from genefab3.frontend.renderers.dataframe import render_dataframe
from genefab3.frontend.parser import parse_request
from genefab3.common.exceptions import GeneLabParserException
from genefab3.frontend.renderers.forms import needs_datatype, render_dropdown
from genefab3.frontend.utils import is_debug
from json import dumps
from genefab3.config import USE_RESPONSE_CACHE, RESPONSE_CACHE, ROW_TYPES
from genefab3.backend.sql.readers.cache import retrieve_cached_response
from genefab3.backend.sql.writers.cache import cache_response
from itertools import cycle


def render_as_format(obj, context):
    """Invoke renderer based on requested format"""
    if obj is None:
        raise GeneLabException("No data")
    elif context.kwargs["format"] == "raw":
        return render_raw(obj, context)
    elif context.kwargs["format"] == "cls":
        return render_cls(obj, context)
    elif context.kwargs["format"] == "gct":
        return render_gct(obj, context)
    elif isinstance(obj, DataFrame):
        return render_dataframe(obj, context)
    else:
        raise GeneLabFormatException(
            "Formatting of unsupported object type",
            object_type=type(obj).__name__, format=context.kwargs.get("format"),
        )


def render(db, getter, kwargs, request):
    """Generate object with `getter` and `**kwargs`, dispatch object and trailing request arguments to renderer"""
    try:
        context = parse_request(request)
    except GeneLabParserException as e:
        if needs_datatype(e) and (request.args.get("format") == "browser"):
            return render_dropdown("datatype", None)
        else:
            raise
    if (context.kwargs["debug"] == "1") and is_debug():
        return "<pre>context={}</pre>".format(dumps(context.__dict__, indent=4))
    else:
        if USE_RESPONSE_CACHE:
            cached_response = retrieve_cached_response(
                context, response_cache=RESPONSE_CACHE,
            )
        else:
            cached_response = None
        if cached_response is not None:
            return cached_response
        else:
            obj = getter(db, **kwargs, context=context)
            response = render_as_format(obj, context)
            cache_response(context, response, response_cache=RESPONSE_CACHE)
            return response


class Placeholders:
    """Defines placeholder objects for situations where empty results need to be displayed"""
 
    def dataframe(*level_values):
        """Return an empty dataframe with specificed column names"""
        maxlen = max(map(len, level_values))
        cyclers = [
            cycle(values) if (len(values) < maxlen) else iter(values)
            for values in level_values
        ]
        return DataFrame(columns=MultiIndex.from_tuples(zip(*cyclers)))
 
    def metadata_dataframe(include=set()):
        """Return an empty dataframe that matches metadata format"""
        return Placeholders.dataframe(
            ["info"], ["accession", "assay", *(c.strip(".") for c in include)],
        )
 
    def data_dataframe():
        """Return an empty dataframe that matches data format"""
        return Placeholders.dataframe(
            ["info"], ["info"], [ROW_TYPES.default_factory()],
        )
