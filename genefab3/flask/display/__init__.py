from genefab3.flask.parser import parse_request
from genefab3.exceptions import GeneLabException
from pandas import DataFrame
from genefab3.flask.display.raw import render_raw
from genefab3.flask.display.dataframe import render_dataframe


def display(db, getter, kwargs, request):
    """Generate object with `getter` and `**kwargs`, dispatch object and trailing request arguments to display handler"""
    context = parse_request(request)
    obj = getter(db, **kwargs, context=context)
    if obj is None:
        raise GeneLabException("No data")
    elif context.args.get("fmt", "raw") == "raw":
        return render_raw(obj, context)
    elif isinstance(obj, DataFrame):
        return render_dataframe(obj, context)
    else:
        raise NotImplementedError("Display of {} with 'fmt={}'".format(
            type(obj).__name__, context.args.get("fmt", "[unspecified]"),
        ))
