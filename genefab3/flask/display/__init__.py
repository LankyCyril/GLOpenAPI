from genefab3.flask.parser import parse_request
from pandas import DataFrame
from genefab3.flask.display.dataframe import render_dataframe


def display(db, getter, kwargs, request):
    """Generate object with `getter` and `**kwargs`, dispatch object and trailing request arguments to display handler"""
    context = parse_request(request)
    obj = getter(db, **kwargs, context=context)
    if isinstance(obj, DataFrame):
        return render_dataframe(obj, context)
    else:
        raise NotImplementedError("Display of {}".format(type(obj).__name__))
