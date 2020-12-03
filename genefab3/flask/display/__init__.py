from genefab3.flask.parser import parse_request
from genefab3.utils import is_debug
from json import dumps
from genefab3.exceptions import GeneLabException, GeneLabParserException
from genefab3.flask.display.forms import needs_datatype, render_dropdown
from pandas import DataFrame, MultiIndex
from genefab3.flask.display.raw import render_raw
from genefab3.flask.display.dataframe import render_dataframe
from itertools import cycle


def display(db, getter, kwargs, request):
    """Generate object with `getter` and `**kwargs`, dispatch object and trailing request arguments to display handler"""
    try:
        context = parse_request(request)
    except GeneLabParserException as e:
        if needs_datatype(e) and (request.args.get("fmt") == "browser"):
            return render_dropdown("datatype", None)
        else:
            raise
    if (context.kwargs["debug"] == "1") and is_debug():
        return "<pre>context={}</pre>".format(dumps(context.__dict__, indent=4))
    else:
        obj = getter(db, **kwargs, context=context)
        if obj is None:
            raise GeneLabException("No data")
        elif context.kwargs["fmt"] == "raw":
            return render_raw(obj, context)
        elif isinstance(obj, DataFrame):
            return render_dataframe(obj, context)
        else:
            raise NotImplementedError("Display of {} with 'fmt={}'".format(
                type(obj).__name__, context.kwargs.get("fmt", "[unspecified]"),
            ))


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
