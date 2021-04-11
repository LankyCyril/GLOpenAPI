from genefab3.frontend.parser import parse_request
from genefab3.common.exceptions import GeneFabParserException
from genefab3.frontend.renderers.forms import needs_datatype, render_dropdown
from genefab3.frontend.utils import is_debug
from json import dumps
from genefab3.config import USE_RESPONSE_CACHE, RESPONSE_CACHE
from genefab3.backend.sql.readers.cache import retrieve_cached_response
from genefab3.backend.sql.writers.cache import cache_response


def get_accessions_used(obj, context):
    """Infer which accessions were involved in generating `obj`"""
    accessions_in_object = set()
    return accessions_in_object | set(context.accessions_and_assays)


def render(db, getter, kwargs, request):
    """Generate object with `getter` and `**kwargs`, dispatch object and trailing request arguments to renderer"""
    try:
        context = parse_request(request)
    except GeneFabParserException as e:
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
            accessions_used = get_accessions_used(obj, context)
            if USE_RESPONSE_CACHE and accessions_used:
                cache_response(
                    context, response, accessions=accessions_used,
                    response_cache=RESPONSE_CACHE,
                )
            return response
