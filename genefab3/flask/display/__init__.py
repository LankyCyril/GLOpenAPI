from genefab3.flask.parser import parse_request
from genefab3.exceptions import GeneLabException
from pandas import DataFrame, MultiIndex
from genefab3.flask.display.raw import render_raw
from genefab3.flask.display.dataframe import render_dataframe


def display(db, getter, kwargs, request):
    """Generate object with `getter` and `**kwargs`, dispatch object and trailing request arguments to display handler"""
    context = parse_request(request)
    obj = getter(db, **kwargs, context=context)
    if obj is None:
        raise GeneLabException("No data")
    elif context.kwargs.get("fmt", "raw") == "raw":
        return render_raw(obj, context)
    elif isinstance(obj, DataFrame):
        context.kwargs["fmt"] = context.kwargs.get("fmt", "tsv")
        return render_dataframe(obj, context)
    else:
        raise NotImplementedError("Display of {} with 'fmt={}'".format(
            type(obj).__name__, context.kwargs.get("fmt", "[unspecified]"),
        ))


class Placeholders:
    """Defines placeholder objects for situations where empty results need to be displayed"""
 
    def dataframe(*args, **kwargs):
        """Return an empty dataframe with specificed column names"""
        if args and kwargs:
            raise NotImplementedError(
                "Placeholders.dataframe() received too many arguments",
            )
        elif args:
            return DataFrame(columns=args)
        elif kwargs:
            return DataFrame(
                columns=MultiIndex.from_tuples(
                    (k, v) for k, vv in kwargs.items() for v in vv
                ),
            )
