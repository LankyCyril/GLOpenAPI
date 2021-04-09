from genefab3.api.views import metadata
from genefab3.common.exceptions import GeneFabFileException
from genefab3.common.utils import pick_reachable_url
from flask import redirect, Response


def get_descriptor_dataframe(mongo_collections, *, locale, context, include=()):
    """Return DataFrame of file descriptors that match user query"""
    context.projection["file"] = True
    for key in map(".".join, include):
        context.update(key)
    for key in set(context.projection):
        if key.startswith("file."):
            del context.projection[key]
    annotation = metadata.get(
        mongo_collections, locale=locale, context=context,
        drop_trailing_fields=False, aggregate=False,
    )
    if ("file", "filename") not in annotation:
        raise FileNotFoundError("No file found matching specified constraints")
    if ("file", "urls") in annotation:
        annotation[("file", "urls")] = (
            annotation[("file", "urls")].apply(sorted).apply(tuple)
        )
    return annotation[["file"]].drop_duplicates()


def get(mongo_collections, *, locale, context):
    """Return file if annotation filters constrain search to unique file entity"""
    descriptor_dataframe = get_descriptor_dataframe(
        mongo_collections, locale=locale, context=context,
    )
    if descriptor_dataframe.empty:
        raise FileNotFoundError("No file found matching specified constraints")
    elif len(descriptor_dataframe) > 1:
        msg = "Multiple files match search criteria"
        _kw = {"filenames": descriptor_dataframe[("file", "filename")].tolist()}
        raise GeneFabFileException(msg, **_kw)
    else:
        descriptor = descriptor_dataframe["file"].iloc[0].to_dict()
    if context.format == "json":
        return descriptor
    else:
        urls = descriptor.get("urls", ())
        with pick_reachable_url(urls, name=descriptor["filename"]) as url:
            return redirect(url, code=303, Response=Response)
